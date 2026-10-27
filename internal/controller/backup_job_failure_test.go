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

package controller

// A failed backup Job says why (knowledge id 215): "Backup job failed" alone
// left the reason in a pod the Job's TTL removes, for a one-time backup's
// status and event and for every failed run in status.history.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// backupFailedJob is a backup Job whose retry budget ran out.
func backupFailedJob(name string, labels map[string]string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name + "-uid"), Labels: labels},
		Status: batchv1.JobStatus{
			Failed: 4, StartTime: &start,
			Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
			}},
		},
	}
}

// failedBackupPod is a pod of the Job jobName (UID jobUID) whose backup
// container exited with code at finished, saying message.
func failedBackupPod(name, jobName, jobUID string, code int32, finished time.Time, message string) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns",
			Labels: map[string]string{"batch.kubernetes.io/job-name": jobName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: jobName, UID: types.UID(jobUID), Controller: &controller,
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: backupContainerName,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: code, Reason: "Error", Message: message, FinishedAt: metav1.NewTime(finished),
				}},
			}},
		},
	}
}

const backupFailureLog = "Starting backup of database 'neo4j'\n" +
	"Remote backup failed\n" +
	"org.neo4j.cli.CommandFailedException: Failed to back up: Unable to connect to b-server-0:6362\n"

func TestHandleExistingBackupJob_AFailedJobSaysWhy(t *testing.T) {
	now := time.Now()
	backup := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}
	job := backupFailedJob("b-backup", nil)
	r := newBackupTestReconcilerWithStatus(t, backup, job,
		failedBackupPod("b-backup-a", "b-backup", "b-backup-uid", 1, now.Add(-time.Minute), "an earlier attempt"),
		failedBackupPod("b-backup-b", "b-backup", "b-backup-uid", 1, now, backupFailureLog),
		failedBackupPod("b-backup-x", "b-backup", "an-earlier-jobs-uid", 2, now.Add(time.Minute), "a pod of another Job"))

	_, err := r.handleExistingBackupJob(context.Background(), backup, job)
	require.NoError(t, err)

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"}, got))
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	msg := got.Status.Message
	assert.True(t, strings.HasPrefix(msg,
		"Backup job b-backup failed (BackoffLimitExceeded: Job has reached the specified backoff limit): backup exited with code 1: "), msg)
	assert.Contains(t, msg, "Unable to connect to b-server-0:6362")
	assert.NotContains(t, msg, "an earlier attempt", "the last failed pod")
	assert.NotContains(t, msg, "another Job", "only a pod this Job controls")

	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)
	failed := eventsWithReason(drainEvents(rec), EventReasonBackupFailed)
	require.Len(t, failed, 1)
	assert.Contains(t, failed[0], msg)

	require.Len(t, got.Status.History, 1)
	assert.Equal(t, "Failed", got.Status.History[0].Status)
	assert.Equal(t, msg, got.Status.History[0].Error, "the run's history entry says why")
}

// A scheduled backup has no status message per run; its history entry for a
// failed CronJob child says why the run failed.
func TestReconcileScheduledHistory_AFailedRunSaysWhy(t *testing.T) {
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{Schedule: "0 2 * * *"},
	}
	child := backupFailedJob("b-backup-cron-29000000", map[string]string{"app.kubernetes.io/instance": "b"})
	r := newBackupTestReconcilerWithStatus(t, backup, child,
		failedBackupPod("b-backup-cron-29000000-a", child.Name, string(child.UID), 1, time.Now(), backupFailureLog))

	require.NoError(t, r.reconcileScheduledHistory(context.Background(), backup))

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"}, got))
	require.Len(t, got.Status.History, 1)
	run := got.Status.History[0]
	assert.Equal(t, "Failed", run.Status)
	assert.Contains(t, run.Error, "Backup job b-backup-cron-29000000 failed (BackoffLimitExceeded")
	assert.Contains(t, run.Error, "backup exited with code 1: ")
	assert.Contains(t, run.Error, "Unable to connect to b-server-0:6362")
}

// The backup container's termination message is the tail of its log when it
// writes none — in a one-time backup's Job and a scheduled backup's CronJob.
func TestBackupJobs_TheContainerReportsTheTailOfItsLog(t *testing.T) {
	cluster := minimalClusterForBackup("c", "ns")
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: backupServiceAccountName, Namespace: "ns"}}

	oneTime := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", AllDatabases: true},
	}
	r := newBackupTestReconciler(t, cluster, oneTime, sa)
	job, err := r.createBackupJob(context.Background(), oneTime, cluster)
	require.NoError(t, err)
	got := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: job.Name}, got))
	require.Len(t, got.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, backupContainerName, got.Spec.Template.Spec.Containers[0].Name)
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, got.Spec.Template.Spec.Containers[0].TerminationMessagePolicy)

	scheduled := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b2", Namespace: "ns"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", AllDatabases: true, Schedule: "0 2 * * *"},
	}
	r = newBackupTestReconciler(t, cluster, scheduled, sa)
	cron, err := r.createBackupCronJob(context.Background(), scheduled, cluster)
	require.NoError(t, err)
	gotCron := &batchv1.CronJob{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: cron.Name}, gotCron))
	containers := gotCron.Spec.JobTemplate.Spec.Template.Spec.Containers
	require.Len(t, containers, 1)
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, containers[0].TerminationMessagePolicy)
}
