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

// Events from the v1.22.0 re-walk and the #545 follow-ups that a second pass
// announced again (#549, #553), and the sharded first pass that skipped a phase
// (knowledge operations.md ids 136 and 145).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// offlinePointInTimeRestore restores a PVC directory to a point in time into
// standalone "sa": offline with neo4j-admin, and without stopCluster.
func offlinePointInTimeRestore() *neo4jv1beta1.Neo4jRestore {
	restore := minimalRestore("r", "ns", "sa")
	at := metav1.Now()
	restore.Spec.Source = neo4jv1beta1.RestoreSource{
		Type: "storage", BackupPath: "nightly", PointInTime: &at,
		Storage: &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
	}
	restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true}
	return restore
}

func restoreStartedEvents(t *testing.T, r *Neo4jRestoreReconciler) []string {
	t.Helper()
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)
	return eventsWithReason(drainEvents(rec), EventReasonRestoreStarted)
}

// "Restore job … started" came twice, 10s apart, on an offline point-in-time
// restore (v1.22.0 re-walk): a pass that read the restore from before the
// Running write re-entered startRestore, found the Job (AlreadyExists) and
// announced it again. With stopCluster the re-entry first waits out the stop's
// 10s poll, hence the gap; the announcement does not depend on the mode.
func TestStartRestore_APassOnAStaleCopyDoesNotAnnounceTheJobAgain(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := offlinePointInTimeRestore()
	r := statusRestoreReconciler(t, sa, restore)
	stale := restore.DeepCopy() // the restore as the second pass's cache still has it

	_, err := r.startRestore(context.Background(), restore, standaloneAsCluster(sa))
	require.NoError(t, err)
	require.Len(t, restoreStartedEvents(t, r), 2, "why it is offline, then the Job")

	_, err = r.startRestore(context.Background(), stale, standaloneAsCluster(sa))
	require.NoError(t, err)
	assert.Empty(t, restoreStartedEvents(t, r), "the Job had started; this pass changed nothing")

	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, StatusRunning, got.Status.Phase)
	jobs := &batchv1.JobList{}
	require.NoError(t, r.List(context.Background(), jobs))
	assert.Len(t, jobs.Items, 1)
}

// With stopCluster: false nothing marks the standalone as this restore's, so an
// offline restore that came back through Pending — here the previous attempt's
// Job still terminating — announced "Restoring offline …" on the pass that
// waited and again on the pass that started the Job (#549). It is announced
// with the Job's start.
func TestStartRestore_WithoutStopClusterARetryThroughPendingAnnouncesOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out createRestoreJob's 10s poll for the stale Job")
	}
	ctx := context.Background()
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := offlinePointInTimeRestore()
	restore.Generation = 2 // the spec was edited after the failed attempt
	restore.Status = neo4jv1beta1.Neo4jRestoreStatus{Phase: StatusFailed, ObservedGeneration: 1, Message: "Restore job failed"}
	staleJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "r-restore", Namespace: "ns", Finalizers: []string{"test/terminating"}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		}}},
	}
	r := statusRestoreReconciler(t, sa, restore, staleJob)

	_, err := r.startRestore(ctx, restore, standaloneAsCluster(sa))
	require.NoError(t, err)
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(restore), got))
	require.Equal(t, StatusPending, got.Status.Phase, got.Status.Message)
	assert.Empty(t, restoreStartedEvents(t, r), "nothing has started yet")

	// The old Job finishes terminating; the requeue starts the new one.
	job := &batchv1.Job{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "r-restore", Namespace: "ns"}, job))
	job.Finalizers = nil
	require.NoError(t, r.Update(ctx, job))

	_, err = r.startRestore(ctx, got, standaloneAsCluster(sa))
	require.NoError(t, err)
	started := restoreStartedEvents(t, r)
	require.Len(t, started, 2, "%q", started)
	assert.Contains(t, started[0], `Restoring offline with neo4j-admin into standalone "sa", which is stopped`)
	assert.Contains(t, started[1], "Restore job r-restore started")
}

// The first pass of a new Neo4jShardedDatabase wrote "Validating" to the API
// and kept its own copy at no phase, so its check for "Validating" never wrote
// "Creating" (#549). Every phase a first pass goes through is written.
func TestShardedReconcile_TheFirstPassWritesEveryPhase(t *testing.T) {
	server := &shardedServer{name: "products"}
	sd := productsShardedDB()
	var phases []string
	scheme := noEffectTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardingCluster(), sd).
		WithStatusSubresource(&neo4jv1beta1.Neo4jShardedDatabase{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if s, ok := obj.(*neo4jv1beta1.Neo4jShardedDatabase); ok && sub == "status" {
					if n := len(phases); n == 0 || phases[n-1] != s.Status.Phase {
						phases = append(phases, s.Status.Phase)
					}
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Neo4jShardedDatabaseReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) { return server, nil },
	}

	require.NoError(t, reconcileSharded(t, r, sd))
	assert.Equal(t, []string{"Validating", "Creating", "Ready"}, phases)
}

// #553: ShardedDatabaseReady was announced once more on the first pass of the
// new operator after an upgrade from v1.21.1. Kept as designed: that operator
// announced Ready on every pass and left the status below; from it, this
// operator's first pass writes nothing new and announces nothing (knowledge
// operations.md id 136). An announcement there means the status had left
// Ready (a pass that found the cluster not Ready writes "Waiting"), which is a
// transition.
func TestShardedReconcile_AReadyDatabaseFromAnEarlierOperatorIsNotAnnounced(t *testing.T) {
	server := &shardedServer{name: "products", exists: true}
	sd := productsShardedDB()
	created := metav1.NewTime(metav1.Now().Add(-24 * 60 * 60 * 1e9))
	idx0, idx1 := int32(0), int32(1)
	sd.Status = neo4jv1beta1.Neo4jShardedDatabaseStatus{ // as v1.21.1's last pass wrote it
		Phase: "Ready", Message: "Sharded database is operational", ObservedGeneration: 1,
		ShardingReady: ptr.To(true), CreationTime: &created,
		GraphShard: &neo4jv1beta1.ShardStatus{Name: "products-g000", Type: shardTypeGraph, State: "online", Ready: true},
		PropertyShards: []neo4jv1beta1.ShardStatus{
			{Name: "products-p000", Type: shardTypeProperty, State: "online", Ready: true, PropertyShardIndex: &idx0},
			{Name: "products-p001", Type: shardTypeProperty, State: "online", Ready: true, PropertyShardIndex: &idx1},
		},
		VirtualDatabase: &neo4jv1beta1.VirtualDatabaseStatus{Name: "products", Ready: true},
	}
	condStatus, condReason := PhaseToConditionStatus("Ready")
	SetReadyCondition(&sd.Status.Conditions, 1, condStatus, condReason, "Sharded database is operational")
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd)

	require.NoError(t, reconcileSharded(t, r, sd))
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonShardedDatabaseReady))
	assert.Zero(t, server.count("CREATE DATABASE"))
}
