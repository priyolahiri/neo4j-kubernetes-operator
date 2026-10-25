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

// A fresh restore attempt runs on a Job of its own (#565, the #254 symptom
// through the Pending door), and an offline restore announces RestoreStarted
// once (#573).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// restoreJobOf is the Job a restore attempt created: controlled by the
// restore, annotated with the attempt (omitted for "": an operator that did
// not record it), in the given terminal state.
func restoreJobOf(restore *neo4jv1beta1.Neo4jRestore, attempt string, state batchv1.JobConditionType) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: restore.Name + "-restore", Namespace: restore.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: neo4jv1beta1.GroupVersion.String(), Kind: "Neo4jRestore",
				Name: restore.Name, UID: restore.UID, Controller: ptr.To(true),
			}},
		},
	}
	if attempt != "" {
		job.Annotations = map[string]string{AnnotationRestoreAttempt: attempt}
	}
	if state != "" {
		job.Status.Conditions = []batchv1.JobCondition{{Type: state, Status: corev1.ConditionTrue}}
	}
	return job
}

func restoreJob(t *testing.T, r *Neo4jRestoreReconciler, restore *neo4jv1beta1.Neo4jRestore) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: restore.Name + "-restore", Namespace: restore.Namespace}, job))
	return job
}

func restorePhase(t *testing.T, r *Neo4jRestoreReconciler, restore *neo4jv1beta1.Neo4jRestore) string {
	t.Helper()
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(restore), got))
	return got.Status.Phase
}

// #254 through the Pending door: a fresh attempt deleted the previous
// attempt's failed Job only while the restore read Failed. When the Job had
// not finished terminating in ten seconds the attempt went Pending, and the
// next pass — Pending, not Failed — skipped the deletion, hit AlreadyExists
// and adopted the failed Job, which failed the new attempt at once.
func TestStartRestore_ARetryThroughPendingNeverAdoptsTheEarlierJob(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out createRestoreJob's 10s poll for the terminating Job")
	}
	for name, attempt := range map[string]string{
		"a Job of the earlier attempt":                    "1",
		"a Job from an operator that recorded no attempt": "",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sa := minimalStandaloneForRestore("sa", "ns")
			restore := offlinePointInTimeRestore()
			restore.UID = "restore-uid"
			restore.Generation = 2
			// What the pass that met the terminating Job wrote.
			restore.Status = neo4jv1beta1.Neo4jRestoreStatus{Phase: StatusPending, ObservedGeneration: 2,
				Message: "Waiting for the previous attempt's Job to finish terminating"}
			earlier := restoreJobOf(restore, attempt, batchv1.JobFailed)
			earlier.Finalizers = []string{"test/terminating"}
			r := statusRestoreReconciler(t, sa, restore, earlier)
			// It is being deleted, as the previous pass asked.
			require.NoError(t, r.Delete(ctx, restoreJob(t, r, restore)))

			_, err := r.startRestore(ctx, restore, standaloneAsCluster(sa))
			require.NoError(t, err)
			assert.Equal(t, StatusPending, restorePhase(t, r, restore), "the terminating Job is waited out, not adopted")
			assert.Empty(t, restoreStartedEvents(t, r))
		})
	}
}

// A Completed restore run again (spec edited within the Job's five-minute TTL)
// adopted the completed Job and was Completed at once, restoring nothing.
func TestStartRestore_AFreshAttemptRunsOnAJobOfItsOwn(t *testing.T) {
	ctx := context.Background()
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := offlinePointInTimeRestore()
	restore.UID = "restore-uid"
	restore.Generation = 2
	restore.Status = neo4jv1beta1.Neo4jRestoreStatus{Phase: StatusCompleted, ObservedGeneration: 1, Message: "Restore completed"}
	r := statusRestoreReconciler(t, sa, restore, restoreJobOf(restore, "1", batchv1.JobComplete))

	_, err := r.startRestore(ctx, restore, standaloneAsCluster(sa))
	require.NoError(t, err)
	job := restoreJob(t, r, restore)
	assert.Equal(t, "2", job.Annotations[AnnotationRestoreAttempt], "the attempt runs on a Job it created")
	assert.Empty(t, job.Status.Conditions, "not the earlier, completed Job")
	assert.Equal(t, StatusRunning, restorePhase(t, r, restore))
}

// What Rule 46 keeps: two passes of the same attempt race to create the Job
// (the stopCluster window); the one that loses adopts it. A Job from an
// operator that did not record the attempt, met by a pass of an attempt still
// in progress, is that attempt's.
func TestStartRestore_AdoptsThisAttemptsJob(t *testing.T) {
	for name, attempt := range map[string]string{
		"recorded":     "1",
		"not recorded": "",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sa := minimalStandaloneForRestore("sa", "ns")
			restore := offlinePointInTimeRestore()
			restore.UID = "restore-uid"
			restore.Generation = 1
			ours := restoreJobOf(restore, attempt, "")
			ours.Labels = map[string]string{"created-by": "the other pass"}
			r := statusRestoreReconciler(t, sa, restore, ours)

			_, err := r.startRestore(ctx, restore, standaloneAsCluster(sa))
			require.NoError(t, err)
			assert.Equal(t, "the other pass", restoreJob(t, r, restore).Labels["created-by"], "adopted, not replaced")
			assert.Equal(t, StatusRunning, restorePhase(t, r, restore))
			assert.Len(t, restoreStartedEvents(t, r), 1)
		})
	}
}

// A Job of a Neo4jRestore of the same name that was deleted and created again
// — the documented way to retry — is not the new one's, whatever its attempt.
func TestRestoreJobOfThisAttempt_NotAnotherRestoresJob(t *testing.T) {
	restore := offlinePointInTimeRestore()
	restore.UID = "new-uid"
	restore.Generation = 1
	old := restore.DeepCopy()
	old.UID = "old-uid"
	assert.False(t, restoreJobOfThisAttempt(restore, restoreJobOf(old, "1", "")))
	assert.True(t, restoreJobOfThisAttempt(restore, restoreJobOf(restore, "1", "")))
}

// An offline restore with stopCluster announced RestoreStarted twice in the
// same second: "Restoring offline with neo4j-admin, which stops standalone …"
// at the stop, then "Restore job … started" (2026-10-25 re-walk, #573). One
// restore, one RestoreStarted, saying both.
func TestStartRestore_AnOfflineRestoreWithStopClusterIsAnnouncedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out stopCluster's 10s poll for the pods")
	}
	ctx := context.Background()
	sa := minimalStandaloneForRestore("sa", "ns")
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "ns"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(1))},
	}
	restore := offlinePointInTimeRestore()
	restore.Spec.StopCluster = true
	r := statusRestoreReconciler(t, sa, sts, restore)

	_, err := r.startRestore(ctx, restore, standaloneAsCluster(sa))
	require.NoError(t, err)
	started := restoreStartedEvents(t, r)
	require.Len(t, started, 1, "%q", started)
	assert.Contains(t, started[0], "Restore job r-restore started")
	assert.Contains(t, started[0], `restoring offline with neo4j-admin, which stops standalone "sa": `)
	assert.Contains(t, started[0], standaloneOfflineReason(restore, sa), "says why it is offline")
	assert.Equal(t, StatusRunning, restorePhase(t, r, restore))
}

func TestRestoreJobStartedMessage(t *testing.T) {
	assert.Equal(t, "Restore job j started", restoreJobStartedMessage("j", "sa", "", true))
	assert.Equal(t, `Restore job j started: restoring offline with neo4j-admin, which stops standalone "sa": why`,
		restoreJobStartedMessage("j", "sa", "why", true))
	assert.Equal(t, `Restore job j started: restoring offline with neo4j-admin into standalone "sa", which is stopped: why`,
		restoreJobStartedMessage("j", "sa", "why", false))
}
