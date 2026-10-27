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

// What a restore says about how it ended (#594, knowledge id 215): a refused
// spec is a status logged once at info, a failed restore Job says why, an
// online restore announces its start once, and its Ready condition carries a
// restore's reasons.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// onlineRestoreServer is a scripted Neo4j for the single-database online
// restore: the database exists or not, and a created one is online at once.
type onlineRestoreServer struct {
	exists  bool
	created []string
}

func (s *onlineRestoreServer) DatabaseExists(context.Context, string) (bool, error) {
	return s.exists, nil
}

func (s *onlineRestoreServer) RecreateDatabaseWithSeedURI(context.Context, *neo4j.Version, string, string) (bool, error) {
	return true, nil
}

func (s *onlineRestoreServer) CreateDatabaseWithSeedURIOptions(_ context.Context, name, _ string, _ bool) error {
	s.created = append(s.created, name)
	return nil
}

func (s *onlineRestoreServer) CreateDatabaseWithSeedURIUntil(_ context.Context, name, _ string, _ time.Time) error {
	s.created = append(s.created, name)
	return nil
}

func (s *onlineRestoreServer) DatabaseSeedFailureMessage(context.Context, string) (string, error) {
	return "", nil
}

func (s *onlineRestoreServer) DatabaseOnlineState(context.Context, string) (int, int, string, error) {
	return 1, 1, "", nil
}

func (s *onlineRestoreServer) Close() error { return nil }

// onlineStandaloneRestore is a restore from a Neo4jBackup on S3 (credentials
// Secret "creds") into standalone "sa", whose pod already runs with the Secret,
// with stopCluster: true: it restores online, and reaches the server in its
// first pass.
func onlineStandaloneRestore(t *testing.T, server *onlineRestoreServer) (*Neo4jRestoreReconciler, *neo4jv1beta1.Neo4jRestore) {
	t.Helper()
	creds := []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}}}}
	sa := minimalStandaloneForRestore("sa", "ns")
	sa.Spec.ExtraEnvFrom = creds
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "ns"},
		Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "neo4j", EnvFrom: creds}},
		}}},
		Status: appsv1.StatefulSetStatus{UpdatedReplicas: 1, ReadyReplicas: 1},
	}
	restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{
		ArtifactFilename: "orders-1.backup", ArtifactType: "FULL", BackupPath: "nightly",
	})
	restore.Status.ResolvedSource.Storage.Bucket = "bucket"
	restore.Spec.Database = "orders"
	restore.Spec.StopCluster = true // ignored: the standalone restores online
	restore.Finalizers = []string{RestoreFinalizer}
	r := statusRestoreReconciler(t, sa, sts, restore)
	r.newOnlineRestoreClient = func(restoreTarget) (onlineRestoreClient, error) { return server, nil }
	return r, restore
}

// reconcileRestore runs passes of Reconcile on restore "ns/r" with sink as the
// logger, returning each pass's error.
func reconcileRestore(r *Neo4jRestoreReconciler, sink levelCaptureSink, passes int) []error {
	ctx := log.IntoContext(context.Background(), logr.New(sink))
	errs := make([]error, 0, passes)
	for range passes {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "r"}})
		errs = append(errs, err)
	}
	return errs
}

func restoreNamed(t *testing.T, r *Neo4jRestoreReconciler) *neo4jv1beta1.Neo4jRestore {
	t.Helper()
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "r"}, got))
	return got
}

func restoreEvents(t *testing.T, r *Neo4jRestoreReconciler) []string {
	t.Helper()
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)
	return drainEvents(rec)
}

// A refused spec — `database: system`, an offline restore into a running
// standalone without stopCluster (a cloud backup read by pod identity takes
// the offline Job), an online restore over a database that exists without
// replaceExisting — logged an ERROR and returned the refusal as an error, which
// controller-runtime logged again as "Reconciler error" (v1.23.0 journey,
// #594). It is a status: one info line, no ERROR, no returned error.
func TestRestoreReconcile_ARefusedSpecIsLoggedOnceNotAsAnError(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(t *testing.T) *Neo4jRestoreReconciler
		message string
		logLine string
		warned  bool
	}{
		{
			name: "database: system",
			setup: func(t *testing.T) *Neo4jRestoreReconciler {
				restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "system-1.backup"})
				restore.Spec.Database = "system"
				restore.Finalizers = []string{RestoreFinalizer}
				return statusRestoreReconciler(t, minimalStandaloneForRestore("sa", "ns"), restore)
			},
			message: `Validation failed: spec.database "system" is not restorable`,
			logLine: "Restore validation failed",
		},
		{
			name: "pod identity, offline, into a running standalone without stopCluster",
			setup: func(t *testing.T) *Neo4jRestoreReconciler {
				restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-1.backup", BackupPath: "nightly"})
				restore.Status.ResolvedSource.Storage.Bucket = "bucket"
				restore.Status.ResolvedSource.Storage.Cloud.CredentialsSecretRef = "" // pod identity: offline
				restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true}
				restore.Finalizers = []string{RestoreFinalizer}
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sa-0", Namespace: "ns", Labels: map[string]string{"app": "sa"}}}
				return statusRestoreReconciler(t, minimalStandaloneForRestore("sa", "ns"), pod, restore)
			},
			message: "cannot run against a running standalone",
			logLine: "Restore refused: the standalone is running",
		},
		{
			name: "online, over a database that exists, without replaceExisting",
			setup: func(t *testing.T) *Neo4jRestoreReconciler {
				r, _ := onlineStandaloneRestore(t, &onlineRestoreServer{exists: true})
				return r
			},
			message: `database "orders" already exists on the target standalone`,
			logLine: "Restore refused: the database exists",
			warned:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.setup(t)
			sink := newLevelCaptureSink()
			for i, err := range reconcileRestore(r, sink, 3) {
				assert.NoError(t, err, "pass %d: a refusal is a status, not a reconcile error", i+1)
			}
			got := restoreNamed(t, r)
			assert.Equal(t, StatusFailed, got.Status.Phase)
			assert.Contains(t, got.Status.Message, tc.message)
			ready := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeReady)
			require.NotNil(t, ready)
			assert.Equal(t, ConditionReasonRestoreFailed, ready.Reason, "a restore's reason, not ReconciliationFailed")

			assert.Empty(t, sink.errors(), "a refusal is not an ERROR")
			assert.Len(t, sink.mentioning(0, tc.logLine), 1, "logged once, at info")
			failed := eventsWithReason(restoreEvents(t, r), EventReasonRestoreFailed)
			if tc.warned {
				assert.Len(t, failed, 1, "announced once")
			}
		})
	}
}

// pointInTimeIntoStandalone is a point-in-time restore from a PVC path into
// standalone "sa": it takes the offline Job.
func pointInTimeIntoStandalone() *neo4jv1beta1.Neo4jRestore {
	restore := minimalRestore("r", "ns", "sa")
	at := metav1.Now()
	restore.Spec.Source = neo4jv1beta1.RestoreSource{
		Type: "storage", BackupPath: "nightly", PointInTime: &at,
		Storage: &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
	}
	restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true}
	return restore
}

// failedRestorePod is a pod of the Job with UID jobUID whose restore container
// exited with code at finished, saying message.
func failedRestorePod(name, jobUID string, code int32, finished time.Time, message string) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns",
			Labels: map[string]string{"batch.kubernetes.io/job-name": "r-restore"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: "r-restore", UID: types.UID(jobUID), Controller: &controller,
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: restoreContainerName,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: code, Reason: "Error", Message: message, FinishedAt: metav1.NewTime(finished),
				}},
			}},
		},
	}
}

// An offline restore whose Job failed read only "Restore job failed" (v1.23.0
// journey, #594): the reader had to find the pod before its TTL removed it.
// The status and the RestoreFailed event say why: the Job's condition, and the
// tail of what the restore container of its last failed pod said.
func TestCheckRestoreProgress_AFailedJobSaysWhy(t *testing.T) {
	now := time.Now()
	failedJob := func() *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "r-restore", Namespace: "ns", UID: "job-uid"},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
			}}},
		}
	}
	running := func() *neo4jv1beta1.Neo4jRestore {
		restore := pointInTimeIntoStandalone()
		restore.Status.Phase = StatusRunning
		return restore
	}

	t.Run("the pod says why", func(t *testing.T) {
		log := strings.Repeat("restoring…\n", 20) +
			"Restore failed for database 'neo4j'\n" +
			"org.neo4j.cli.CommandFailedException: Failed to restore: no space left on device\n"
		sa := minimalStandaloneForRestore("sa", "ns")
		restore := running()
		r := statusRestoreReconciler(t, sa, restore, failedJob(),
			failedRestorePod("r-restore-a", "job-uid", 1, now.Add(-time.Minute), "an earlier attempt"),
			failedRestorePod("r-restore-b", "job-uid", 1, now, log),
			failedRestorePod("r-restore-x", "other-uid", 2, now.Add(time.Minute), "a pod of another Job"))

		_, err := r.checkRestoreProgress(context.Background(), restore, standaloneAsCluster(sa))
		require.NoError(t, err)

		got := restoreNamed(t, r)
		assert.Equal(t, StatusFailed, got.Status.Phase)
		msg := got.Status.Message
		assert.True(t, strings.HasPrefix(msg,
			"Restore job r-restore failed (BackoffLimitExceeded: Job has reached the specified backoff limit): neo4j-restore exited with code 1: "), msg)
		assert.Contains(t, msg, "Failed to restore: no space left on device")
		assert.NotContains(t, msg, "an earlier attempt", "the last failed pod")
		assert.NotContains(t, msg, "another Job")
		assert.LessOrEqual(t, strings.Count(msg, "restoring…"), jobFailureTailLines-2, "the tail, not the whole log")

		failed := eventsWithReason(restoreEvents(t, r), EventReasonRestoreFailed)
		require.Len(t, failed, 1)
		assert.Contains(t, failed[0], msg)
	})

	t.Run("no pod left: the Job's condition", func(t *testing.T) {
		sa := minimalStandaloneForRestore("sa", "ns")
		restore := running()
		r := statusRestoreReconciler(t, sa, restore, failedJob())

		_, err := r.checkRestoreProgress(context.Background(), restore, standaloneAsCluster(sa))
		require.NoError(t, err)
		assert.Equal(t, "Restore job r-restore failed (BackoffLimitExceeded: Job has reached the specified backoff limit)",
			restoreNamed(t, r).Status.Message)
	})
}

// The restore container's termination message is the tail of its log when it
// writes none, which is what restoreJobFailure reads; its phase while the Job
// runs is Running, with a restore's reason.
func TestStartRestore_TheJobReportsTheTailOfItsLog(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := pointInTimeIntoStandalone()
	r := statusRestoreReconciler(t, sa, restore)

	_, err := r.startRestore(context.Background(), restore, standaloneAsCluster(sa))
	require.NoError(t, err)

	job := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "r-restore"}, job))
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, job.Spec.Template.Spec.Containers[0].TerminationMessagePolicy)

	got := restoreNamed(t, r)
	assert.Equal(t, StatusRunning, got.Status.Phase)
	ready := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, ConditionReasonRestoreRunning, ready.Reason)
}

// An online restore into a standalone with stopCluster: true emitted two
// RestoreStarted — "spec.stopCluster is ignored …" and "Online restore into …"
// (v1.23.0 journey, #594). It is one, which says both. Its Ready condition,
// once restored, reads RestoreCompleted, not the backup's BackupSucceeded.
func TestRestoreReconcile_AnOnlineRestoreWithStopClusterStartsOnce(t *testing.T) {
	server := &onlineRestoreServer{}
	r, _ := onlineStandaloneRestore(t, server)
	sink := newLevelCaptureSink()
	for _, err := range reconcileRestore(r, sink, 3) {
		require.NoError(t, err)
	}
	assert.Equal(t, []string{"orders"}, server.created)

	started := eventsWithReason(restoreEvents(t, r), EventReasonRestoreStarted)
	if assert.Len(t, started, 1, "one start: %q", started) {
		assert.Contains(t, started[0], `Online restore into standalone "sa": database "orders" (create)`)
		assert.Contains(t, started[0], "spec.stopCluster is ignored")
	}

	got := restoreNamed(t, r)
	assert.Equal(t, StatusCompleted, got.Status.Phase)
	ready := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
	assert.Equal(t, ConditionReasonRestoreComplete, ready.Reason)
	assert.Empty(t, sink.errors())
}

func TestRestoreReadyCondition(t *testing.T) {
	for _, tc := range []struct {
		phase  string
		status metav1.ConditionStatus
		reason string
	}{
		{StatusCompleted, metav1.ConditionTrue, "RestoreCompleted"},
		{StatusFailed, metav1.ConditionFalse, "RestoreFailed"},
		{StatusRunning, metav1.ConditionUnknown, "RestoreRunning"},
		{StatusPending, metav1.ConditionUnknown, ConditionReasonPending},
	} {
		status, reason := restoreReadyCondition(tc.phase)
		assert.Equal(t, tc.status, status, tc.phase)
		assert.Equal(t, tc.reason, reason, tc.phase)
	}
}

func TestMessageTail(t *testing.T) {
	assert.Equal(t, "", messageTail(" \n\n"))
	assert.Equal(t, "a | b", messageTail("a\n\n b \n"))
	assert.Equal(t, "3 | 4 | 5 | 6 | 7", messageTail("1\n2\n3\n4\n5\n6\n7\n"))
	long := messageTail(strings.Repeat("x", jobFailureMaxLen+50))
	assert.Equal(t, jobFailureMaxLen+1, len([]rune(long)))
	assert.True(t, strings.HasPrefix(long, "…"))
}
