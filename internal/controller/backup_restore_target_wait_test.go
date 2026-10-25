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

// A Neo4jBackup or Neo4jRestore applied before its target waited in silence:
// every other kind announces a missing deployment with one ClusterNotFound
// (knowledge id 182). They now do too (id 198), and a restore that has ended
// is not reopened by its target going away.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestBackupReconcile_AMissingTargetIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default", Generation: 1, Finalizers: []string{BackupFinalizer}},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef: "not-yet", Database: "neo4j",
			Storage: neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
		},
	}
	r := newBackupTestReconcilerWithStatus(t, backup)
	rec := record.NewFakeRecorder(50)
	r.Recorder = rec
	key := client.ObjectKeyFromObject(backup)

	reconcileTimes(t, r, key, repeatPasses)
	found := eventsWithReason(drainEvents(rec), EventReasonClusterNotFound)
	require.Len(t, found, 1, "announced when the wait starts, not on every pass")
	assert.Contains(t, found[0], "Warning ClusterNotFound Waiting for target to appear")
	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, neo4jv1beta1.PhaseWaiting, got.Status.Phase, "the documented phase of a backup that waits")

	restarted := &Neo4jBackupReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: rec}
	reconcileTimes(t, restarted, key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), "a restarted operator reads the wait from status")
}

func TestRestoreReconcile_AMissingTargetIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	restore := minimalRestore("r", "ns", "not-yet")
	restore.Generation = 1
	restore.Finalizers = []string{RestoreFinalizer}
	r := statusRestoreReconciler(t, restore)
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)
	key := client.ObjectKeyFromObject(restore)

	reconcileTimes(t, r, key, repeatPasses)
	found := eventsWithReason(drainEvents(rec), EventReasonClusterNotFound)
	require.Len(t, found, 1, "announced when the wait starts, not on every pass")
	assert.Contains(t, found[0], "Warning ClusterNotFound Waiting for target to appear")
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, StatusPending, got.Status.Phase)

	restarted := &Neo4jRestoreReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: rec}
	reconcileTimes(t, restarted, key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), "a restarted operator reads the wait from status")
}

// A restore that ended at its generation is decided before its target is
// looked up. A target deleted afterwards wrote Pending over Completed or
// Failed, and one created again under the same name ran the restore again.
func TestRestoreReconcile_AnEndedRestoreIsNotReopenedByAMissingTarget(t *testing.T) {
	for _, phase := range []string{StatusCompleted, StatusFailed} {
		t.Run(phase, func(t *testing.T) {
			restore := minimalRestore("r", "ns", "deleted-since")
			restore.Generation = 1
			restore.Finalizers = []string{RestoreFinalizer}
			restore.Status = neo4jv1beta1.Neo4jRestoreStatus{Phase: phase, ObservedGeneration: 1, Message: "ended"}
			r := statusRestoreReconciler(t, restore)
			rec, ok := r.Recorder.(*record.FakeRecorder)
			require.True(t, ok)

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)})
			require.NoError(t, err)
			got := &neo4jv1beta1.Neo4jRestore{}
			require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(restore), got))
			assert.Equal(t, phase, got.Status.Phase, "an ended restore stays ended")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound))
		})
	}
}
