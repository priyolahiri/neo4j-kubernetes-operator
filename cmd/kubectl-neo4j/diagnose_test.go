/*
Copyright 2025 Priyo Lahiri.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// serverPod builds a pod carrying the labels the operator actually stamps on
// server pods — taken from the operator's own selector rather than a literal,
// so that a label-scheme change breaks this test the way it would break the
// command.
func serverPod(cluster, name string) *corev1.Pod {
	labels := map[string]string{}
	for k, v := range resources.ServerPodSelector(cluster) {
		labels[k] = v
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j", Labels: labels},
	}
}

func symptomsFor(t *testing.T, d []diagnosis, kindName string) []symptom {
	t.Helper()
	for _, one := range d {
		if one.kind+"/"+one.name == kindName {
			return one.symptoms
		}
	}
	t.Fatalf("no diagnosis for %s", kindName)
	return nil
}

func joinSymptoms(ss []symptom) string {
	var b strings.Builder
	for _, s := range ss {
		b.WriteString(s.mark + " " + s.subject + " " + s.what + " " + s.detail + " " + s.action + "\n")
	}
	return b.String()
}

// The command's reason for existing: `status` says "Pending", this says which
// pod could not be scheduled and why.
func TestDiagnose_UnschedulablePodIsReportedWithSchedulerMessage(t *testing.T) {
	pod := serverPod("prod", "prod-server-2")
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:    corev1.PodScheduled,
		Status:  corev1.ConditionFalse,
		Reason:  corev1.PodReasonUnschedulable,
		Message: "0/3 nodes are available: 3 Insufficient memory.",
	}}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
			Status:     neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: "Pending"},
		},
		pod,
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"))
	assert.Contains(t, out, "cannot be scheduled")
	assert.Contains(t, out, "0/3 nodes are available")
	assert.Contains(t, out, markProblem)
}

// OOMKilled is the most common Neo4j Enterprise failure on an under-provisioned
// cluster. It must be caught from the exit code alone, because that is the one
// signal that is not a kubelet string this test could be written to match.
func TestDiagnose_OOMKilledDetectedFromExitCodeAlone(t *testing.T) {
	pod := serverPod("prod", "prod-server-0")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "neo4j",
		LastTerminationState: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: ""},
		},
	}}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		},
		pod,
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"))
	assert.Contains(t, out, "OOMKilled")
	assert.Contains(t, out, "1.5Gi", "the action must state the Enterprise memory floor")
}

func TestDiagnose_ImagePullAndCrashLoopAreDistinguished(t *testing.T) {
	pull := serverPod("prod", "prod-server-0")
	pull.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "neo4j",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "pull access denied"}},
	}}
	crash := serverPod("prod", "prod-server-1")
	crash.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "neo4j",
		RestartCount: 7,
		State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	}}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		pull, crash,
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"))
	assert.Contains(t, out, "image cannot be pulled")
	assert.Contains(t, out, "imagePullSecret")
	assert.Contains(t, out, "crash-looping after 7 restart(s)")
	assert.Contains(t, out, "--previous", "the crash-loop action must point at the previous log")
}

func TestDiagnose_UnboundPVCNamesItsStorageClass(t *testing.T) {
	sc := "fast-ssd"
	pvcLabels := map[string]string{}
	for k, v := range resources.PVCSelectorByInstance("prod") {
		pvcLabels[k] = v
	}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-prod-server-0", Namespace: "neo4j", Labels: pvcLabels},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &sc},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
		},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"))
	assert.Contains(t, out, "not Bound")
	assert.Contains(t, out, "fast-ssd")
}

// A bound PVC and a ready container are not findings. Without this, the
// command would cry wolf on every healthy deployment and stop being read.
func TestDiagnose_HealthyDeploymentReportsNothing(t *testing.T) {
	pod := serverPod("prod", "prod-server-0")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "neo4j",
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
			Status:     neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: "Ready", Ready: true},
		},
		pod,
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "prod-server", Namespace: "neo4j"},
			Spec: appsv1.StatefulSetSpec{
				Replicas: ptrInt32(1),
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: pod.Labels}},
			},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	d := results[0]
	assert.Empty(t, d.symptoms)
	assert.False(t, d.problems())
	assert.Equal(t, "1/1 pods ready", d.summary)
}

// A running-but-not-ready container is reported so the user knows where the
// deployment is — but it must NOT set the failure exit code, because it
// commonly resolves on its own while Neo4j starts.
func TestDiagnose_NotReadyYetIsWaitingNotProblem(t *testing.T) {
	pod := serverPod("prod", "prod-server-0")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "neo4j",
		Ready: false,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		pod,
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	d := results[0]
	require.NotEmpty(t, d.symptoms)
	assert.Equal(t, markWaiting, d.symptoms[0].mark)
	assert.False(t, d.problems(), "a readiness probe still settling is not a problem")
}

// The failure mode with no other signal: a CR nothing ever reconciled. This is
// what a user sees when the operator is not running, lacks RBAC for the kind,
// or is namespace-scoped and not watching this namespace (#282).
func TestDiagnose_ResourceWithNoStatusAfterGraceIsReported(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name: "analytics", Namespace: "neo4j",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-30 * time.Minute)),
			},
		},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jDatabase/analytics"))
	assert.Contains(t, out, "has no status")
	assert.Contains(t, out, "watching this namespace")
}

// Same resource, just created: silence is expected, so it must stay silent.
func TestDiagnose_FreshResourceWithNoStatusIsNotReported(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jDatabase{
			ObjectMeta: metav1.ObjectMeta{
				Name: "analytics", Namespace: "neo4j",
				CreationTimestamp: metav1.NewTime(time.Now()),
			},
		},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)
	assert.Empty(t, symptomsFor(t, results, "Neo4jDatabase/analytics"))
}

func TestDiagnose_FailedBackupJobIsFoundThroughTheSharedSelector(t *testing.T) {
	jobLabels := map[string]string{}
	for k, v := range resources.BackupJobSelector("nightly") {
		jobLabels[k] = v
	}

	c := testClient(t,
		&neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
			Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "prod"},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly-1", Namespace: "neo4j", Labels: jobLabels},
			Status: batchv1.JobStatus{
				Failed: 1,
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
					Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
				}},
			},
		},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jBackup/nightly"))
	assert.Contains(t, out, "job nightly-1")
	assert.Contains(t, out, "BackoffLimitExceeded")
	assert.Contains(t, out, "ServiceAccount")
}

// diagnoseNightlyBackup runs diagnose over a Neo4jBackup "nightly" whose only
// Job has the given status, and returns what the user would be told.
func diagnoseNightlyBackup(t *testing.T, status batchv1.JobStatus) diagnosis {
	t.Helper()
	jobLabels := map[string]string{}
	for k, v := range resources.BackupJobSelector("nightly") {
		jobLabels[k] = v
	}
	c := testClient(t,
		&neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
			Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "prod"},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly-1", Namespace: "neo4j", Labels: jobLabels},
			Status:     status,
		},
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)
	for _, d := range results {
		if d.kind+"/"+d.name == "Neo4jBackup/nightly" {
			return d
		}
	}
	t.Fatal("no diagnosis for Neo4jBackup/nightly")
	return diagnosis{}
}

// backoffLimit is 3, so a backup pod that hits a transient error (an object
// store timeout, a slow image pull) is retried and the Job usually finishes.
// Job.status.failed counts those earlier attempts forever, so reading it as
// "the Job failed" reported ✗ on a backup that had in fact succeeded — and made
// diagnose exit 1 over it. The verdict is the Job's own condition.
func TestDiagnose_BackupJobThatRetriedAndSucceededIsNotAFailure(t *testing.T) {
	d := diagnoseNightlyBackup(t, batchv1.JobStatus{
		Failed: 2, Succeeded: 1,
		Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
	})

	assert.False(t, d.problems(), "a completed Job is not a problem:\n%s", joinSymptoms(d.symptoms))
	assert.Equal(t, "1 job(s): 1 succeeded, 0 failed", d.summary)
	// The retries are worth a line, but only a line.
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markWarning+" job nightly-1")
	assert.Contains(t, out, "2 failed attempt(s)")
	assert.Contains(t, out, "succeeded")
}

// Older Jobs, or ones whose Complete condition has not been written yet.
func TestDiagnose_BackupJobWithSucceededPodsButNoConditionIsASuccess(t *testing.T) {
	d := diagnoseNightlyBackup(t, batchv1.JobStatus{Failed: 1, Succeeded: 1})
	assert.False(t, d.problems(), joinSymptoms(d.symptoms))
	assert.Equal(t, "1 job(s): 1 succeeded, 0 failed", d.summary)
}

func TestDiagnose_BackupJobThatSucceededFirstTimeSaysNothing(t *testing.T) {
	d := diagnoseNightlyBackup(t, batchv1.JobStatus{
		Succeeded:  1,
		Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
	})
	assert.Empty(t, d.symptoms)
	assert.Equal(t, "1 job(s): 1 succeeded, 0 failed", d.summary)
}

// Still retrying: not failed yet (the backoff limit has not been reached), so it
// must not be a ✗ or flip the exit code — but the user ran diagnose because
// something is off, so the failing attempts are shown, with the log pointer.
func TestDiagnose_BackupJobStillRetryingIsWaitingNotFailed(t *testing.T) {
	d := diagnoseNightlyBackup(t, batchv1.JobStatus{Failed: 1, Active: 1})

	assert.False(t, d.problems(), joinSymptoms(d.symptoms))
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markWaiting+" job nightly-1")
	assert.Contains(t, out, "retrying")
	assert.Contains(t, out, "kubectl logs job/nightly-1")
	assert.Equal(t, "1 job(s): 0 succeeded, 0 failed, 1 in progress", d.summary)
}

// The Job's own Failed condition is what makes it failed, including the ways it
// fails without a single pod having failed: an exceeded activeDeadlineSeconds
// leaves status.failed at 0 and used to be reported as healthy.
func TestDiagnose_BackupJobFailedConditionIsAFailureEvenWithNoFailedPods(t *testing.T) {
	d := diagnoseNightlyBackup(t, batchv1.JobStatus{
		Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
			Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline",
		}},
	})

	assert.True(t, d.problems())
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markProblem+" job nightly-1 failed")
	assert.Contains(t, out, "DeadlineExceeded")
	assert.NotContains(t, out, "0 pod failure", "no failed pods to count, so none should be claimed")
	assert.Equal(t, "1 job(s): 0 succeeded, 1 failed", d.summary)
}

// A Failed condition that is False (or Unknown) is not a failure.
func TestDiagnose_BackupJobFailedConditionMustBeTrue(t *testing.T) {
	d := diagnoseNightlyBackup(t, batchv1.JobStatus{
		Active: 1,
		Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionFalse,
		}},
	})
	assert.False(t, d.problems(), joinSymptoms(d.symptoms))
}

// finishedPod builds a pod whose single container has FINISHED in its current
// attempt and will not be restarted — the shape a Job attempt leaves behind
// under RestartPolicyNever. The terminated state is the container's CURRENT
// state; LastTerminationState is empty, because nothing ever restarted.
func finishedPod(base *corev1.Pod, container string, t corev1.ContainerStateTerminated) *corev1.Pod {
	base.Spec.RestartPolicy = corev1.RestartPolicyNever
	base.Status.Phase = corev1.PodFailed
	base.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  container,
		State: corev1.ContainerState{Terminated: &t},
	}}
	return base
}

// backupJobPod builds a backup pod labelled the way the operator labels it,
// and the way the Job controller labels every pod it creates.
func backupJobPod(backup, job, name string) *corev1.Pod {
	labels := map[string]string{batchv1.JobNameLabel: job}
	for k, v := range resources.BackupJobSelector(backup) {
		labels[k] = v
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j", Labels: labels}}
}

// nightlyBackupWith diagnoses a Neo4jBackup "nightly" that owns one Job with
// the given status and one pod, and returns what the user would be told.
func nightlyBackupWith(t *testing.T, status batchv1.JobStatus, pod *corev1.Pod) diagnosis {
	t.Helper()
	jobLabels := map[string]string{}
	for k, v := range resources.BackupJobSelector("nightly") {
		jobLabels[k] = v
	}
	c := testClient(t,
		&neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "neo4j"},
			Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "prod"},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly-1", Namespace: "neo4j", Labels: jobLabels},
			Status:     status,
		},
		pod,
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)
	for _, d := range results {
		if d.kind+"/"+d.name == "Neo4jBackup/nightly" {
			return d
		}
	}
	t.Fatal("no diagnosis for Neo4jBackup/nightly")
	return diagnosis{}
}

// A backup attempt OOM-killed under RestartPolicyNever sits in State.Terminated
// forever: there is no restart, so LastTerminationState is empty and the old
// code, which read only that, reported nothing for the pod. The single most
// common cause of a failed backup was invisible at the container level.
func TestDiagnose_NeverRestartPodOOMKilledIsReported(t *testing.T) {
	pod := finishedPod(serverPod("prod", "prod-server-0"), "neo4j",
		corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"})

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		pod,
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	d := results[0]
	assert.True(t, d.problems(), joinSymptoms(d.symptoms))
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markProblem+" container neo4j in prod-server-0")
	assert.Contains(t, out, "was OOMKilled (exit 137)")
	assert.Contains(t, out, "1.5Gi", "a server pod gets the Enterprise memory floor")
	assert.Contains(t, out, "kubectl logs prod-server-0 -c neo4j")
	assert.NotContains(t, out, "--previous", "nothing restarted, so the log of this run IS the log")
}

// A backup pod is sized by spec.options.resources on the Neo4jBackup, not by
// the server's spec.resources — pointing at the wrong field sends the user to
// edit a cluster to fix a backup.
func TestDiagnose_OOMKilledBackupPodPointsAtTheBackupsOwnResources(t *testing.T) {
	pod := finishedPod(backupJobPod("nightly", "nightly-1", "nightly-1-abcde"), "backup",
		corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"})
	d := nightlyBackupWith(t, batchv1.JobStatus{
		Failed: 4,
		Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded",
		}},
	}, pod)

	assert.True(t, d.problems())
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markProblem+" container backup in nightly-1-abcde")
	assert.Contains(t, out, "was OOMKilled (exit 137)")
	assert.Contains(t, out, "spec.options.resources")
	assert.NotContains(t, out, "1.5Gi", "the Enterprise server floor is not a backup Job's limit")
	assert.Contains(t, out, "kubectl logs nightly-1-abcde -c backup")
}

// A container that failed without being OOM-killed: neo4j-admin exited
// non-zero (reason "Error"). Same blind spot, different cause.
func TestDiagnose_NeverRestartPodNonZeroExitIsReported(t *testing.T) {
	pod := finishedPod(serverPod("prod", "prod-server-0"), "neo4j",
		corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: "  permission denied on bucket  "})

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		pod,
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	d := results[0]
	assert.True(t, d.problems(), joinSymptoms(d.symptoms))
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markProblem+" container neo4j in prod-server-0")
	assert.Contains(t, out, "exited 1 (Error)")
	assert.Contains(t, out, "permission denied on bucket")
	assert.Contains(t, out, "kubectl logs prod-server-0 -c neo4j")
	assert.NotContains(t, out, "OOMKilled")
	assert.NotContains(t, out, "--previous")
}

// Exit 0 is a container that did its job. Flagging it would turn every
// successful backup Job pod into a finding.
func TestDiagnose_CompletedContainerIsNotAFinding(t *testing.T) {
	pod := finishedPod(serverPod("prod", "prod-server-0"), "neo4j",
		corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"})
	pod.Status.Phase = corev1.PodSucceeded

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		pod,
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	d := results[0]
	assert.False(t, d.problems(), joinSymptoms(d.symptoms))
	assert.Empty(t, d.symptoms)
}

// A crash-looping container is already told about once: it is waiting in
// CrashLoopBackOff and its previous run is reported with a --previous pointer.
// Its current-attempt state must not add a second finding for the same
// container (kubelet briefly reports both while it flips between restarts).
func TestDiagnose_CrashLoopingContainerIsNotReportedTwice(t *testing.T) {
	loop := serverPod("prod", "prod-server-0")
	loop.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "neo4j",
		RestartCount: 5,
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
		},
		LastTerminationState: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"},
		},
	}}
	// The transitional shape: the current attempt has just died AND the
	// previous one is still recorded.
	transition := serverPod("prod", "prod-server-1")
	transition.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "neo4j",
		RestartCount: 5,
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"},
		},
		LastTerminationState: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"},
		},
	}}

	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		loop, transition,
	)
	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	symptoms := symptomsFor(t, results, "Neo4jEnterpriseCluster/prod")
	perPod := map[string]int{}
	for _, s := range symptoms {
		perPod[s.subject]++
	}
	// prod-server-0: crash-looping + OOMKilled (previous run) — the two
	// findings it always had. prod-server-1: the one OOMKilled, not two.
	assert.Equal(t, 2, perPod["container neo4j in prod-server-0"], joinSymptoms(symptoms))
	assert.Equal(t, 1, perPod["container neo4j in prod-server-1"], joinSymptoms(symptoms))
	out := joinSymptoms(symptoms)
	assert.Contains(t, out, "crash-looping after 5 restart(s)")
	assert.Contains(t, out, "--previous")
}

// A failed backup attempt that the Job then retried to success leaves a Failed
// pod behind (Jobs keep them), and that pod's OOMKilled container is now a
// finding. It must not flip the exit code of a backup that in fact completed —
// the Job's own verdict stays the verdict, exactly as for status.failed.
func TestDiagnose_OOMKilledAttemptOfASucceededBackupIsAWarningNotAProblem(t *testing.T) {
	pod := finishedPod(backupJobPod("nightly", "nightly-1", "nightly-1-abcde"), "backup",
		corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"})
	d := nightlyBackupWith(t, batchv1.JobStatus{
		Failed: 1, Succeeded: 1,
		Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
	}, pod)

	assert.False(t, d.problems(), "a completed Job is not a problem:\n%s", joinSymptoms(d.symptoms))
	out := joinSymptoms(d.symptoms)
	assert.Contains(t, out, markWarning+" container backup in nightly-1-abcde")
	assert.Contains(t, out, "was OOMKilled")
}

// Still retrying: the OOMKilled attempt is worth showing, but the Job has not
// failed yet, so it waits rather than fails (same rule as status.failed).
func TestDiagnose_OOMKilledAttemptOfARetryingBackupIsWaitingNotProblem(t *testing.T) {
	pod := finishedPod(backupJobPod("nightly", "nightly-1", "nightly-1-abcde"), "backup",
		corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"})
	d := nightlyBackupWith(t, batchv1.JobStatus{Failed: 1, Active: 1}, pod)

	assert.False(t, d.problems(), joinSymptoms(d.symptoms))
	assert.Contains(t, joinSymptoms(d.symptoms), markWaiting+" container backup in nightly-1-abcde")
}

// A Pending backup pod that cannot be scheduled is not a finished attempt: it
// is the live cause of an in-progress Job, and stays a problem whatever the
// Job's state. Capping must only soften attempts that are over.
func TestDiagnose_UnschedulableBackupPodStaysAProblemWhileJobInProgress(t *testing.T) {
	pod := backupJobPod("nightly", "nightly-1", "nightly-1-abcde")
	pod.Status.Phase = corev1.PodPending
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
		Reason: corev1.PodReasonUnschedulable, Message: "0/3 nodes are available: 3 Insufficient memory.",
	}}
	d := nightlyBackupWith(t, batchv1.JobStatus{Active: 1}, pod)

	assert.True(t, d.problems(), joinSymptoms(d.symptoms))
	assert.Contains(t, joinSymptoms(d.symptoms), "cannot be scheduled")
}

// Targeting one resource must not diagnose its neighbours.
func TestDiagnose_TargetSelectsASingleResource(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "staging", Namespace: "neo4j"}},
		&neo4jv1beta1.Neo4jDatabase{ObjectMeta: metav1.ObjectMeta{Name: "analytics", Namespace: "neo4j"}},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "Neo4jEnterpriseCluster/prod")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "prod", results[0].name)
}

// Warning events are the operator's own voice; structured events exist in
// internal/controller/events.go precisely so they can be read back.
func TestDiagnose_WarningEventsAreAttachedToTheirResource(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "e1", Namespace: "neo4j"},
			Type:           corev1.EventTypeWarning,
			Reason:         "SplitBrainDetected",
			Message:        "servers disagree on cluster membership",
			LastTimestamp:  metav1.NewTime(time.Now().Add(-time.Minute)),
			InvolvedObject: corev1.ObjectReference{Kind: "Neo4jEnterpriseCluster", Name: "prod"},
		},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "e2", Namespace: "neo4j"},
			Type:           corev1.EventTypeNormal,
			Reason:         "Reconciled",
			Message:        "all good",
			InvolvedObject: corev1.ObjectReference{Kind: "Neo4jEnterpriseCluster", Name: "prod"},
		},
	)

	results, err := diagnoseNamespace(context.Background(), c, "neo4j", "")
	require.NoError(t, err)

	out := joinSymptoms(symptomsFor(t, results, "Neo4jEnterpriseCluster/prod"))
	assert.Contains(t, out, "SplitBrainDetected")
	assert.NotContains(t, out, "all good", "Normal events are noise here")
}

func TestWrapIndent_KeepsShortTextOnOneLine(t *testing.T) {
	assert.Equal(t, "short message", wrapIndent("short   message", 6))
	long := strings.Repeat("word ", 40)
	assert.Contains(t, wrapIndent(long, 8), "\n        word")
}

func ptrInt32(i int32) *int32 { return &i }

// renderDiagnoseTo captures what the user actually sees, and the exit code the
// caller's CI will branch on.
func renderDiagnoseTo(t *testing.T, results []diagnosis, ns string, quiet bool) (string, int) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	code := renderDiagnosis(results, f, ns, quiet)
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(b), code
}

// The exit code is this command's contract with CI, in the same way validate's
// is: 1 means "ran fine, the answer is bad", never "could not run".
func TestRenderDiagnosis_ExitCodeReflectsProblemsOnly(t *testing.T) {
	healthy := diagnosis{kind: "Neo4jDatabase", name: "analytics", phase: "Ready"}
	waiting := diagnosis{kind: "Neo4jEnterpriseCluster", name: "prod", phase: "Pending",
		symptoms: []symptom{{mark: markWaiting, subject: "container neo4j in prod-server-0", what: "running but not ready"}}}
	broken := diagnosis{kind: "Neo4jEnterpriseCluster", name: "bad", phase: "Pending",
		symptoms: []symptom{{mark: markProblem, subject: "pod bad-server-0", what: "cannot be scheduled"}}}

	_, code := renderDiagnoseTo(t, []diagnosis{healthy}, "neo4j", false)
	assert.Equal(t, exitOK, code)

	_, code = renderDiagnoseTo(t, []diagnosis{healthy, waiting}, "neo4j", false)
	assert.Equal(t, exitOK, code, "a resource still settling must not fail the exit code")

	out, code := renderDiagnoseTo(t, []diagnosis{healthy, waiting, broken}, "neo4j", false)
	assert.Equal(t, exitProblems, code)
	assert.Contains(t, out, "1 of 3 resource(s) have problems")
	assert.Contains(t, out, "support-bundle", "the escalation path is named once, at the end")
}

func TestRenderDiagnosis_ShowsSubjectDetailAndAction(t *testing.T) {
	d := diagnosis{
		kind: "Neo4jEnterpriseCluster", name: "prod", phase: "Pending", summary: "2/3 pods ready",
		symptoms: []symptom{{
			mark: markProblem, subject: "pod prod-server-2", what: "cannot be scheduled",
			detail: "0/3 nodes are available: 3 Insufficient memory.",
			action: "Compare spec.resources.requests with node capacity.",
		}},
	}
	out, code := renderDiagnoseTo(t, []diagnosis{d}, "neo4j", false)

	assert.Equal(t, exitProblems, code)
	assert.Contains(t, out, "Neo4jEnterpriseCluster/prod — Pending · 2/3 pods ready")
	assert.Contains(t, out, "✗ pod prod-server-2 — cannot be scheduled")
	assert.Contains(t, out, "0/3 nodes are available")
	assert.Contains(t, out, "→ Compare spec.resources.requests")
}

func TestRenderDiagnosis_QuietHidesResourcesWithNothingToSay(t *testing.T) {
	healthy := diagnosis{kind: "Neo4jDatabase", name: "analytics", phase: "Ready"}
	out, code := renderDiagnoseTo(t, []diagnosis{healthy}, "neo4j", true)
	assert.Equal(t, exitOK, code)
	assert.Contains(t, out, "nothing to report")
	assert.NotContains(t, out, "analytics")
}

func TestRenderDiagnosis_EmptyNamespace(t *testing.T) {
	out, code := renderDiagnoseTo(t, nil, "neo4j", false)
	assert.Equal(t, exitOK, code)
	assert.Contains(t, out, `no Neo4j resources found in namespace "neo4j"`)
}
