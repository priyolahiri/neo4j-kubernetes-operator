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

// What the v1.22.x live walk found in the backup controller: a FULL sharded
// PVC backup of a Neo4jShardedDatabase with `spec.name: Products` stayed
// Running "capturing artifact metadata" for the Job's 30-minute TTL, announced
// BackupCompleted when the Job went, and then went Failed starting a new Job
// whose `Products*` glob matched a restored copy, `productscopy`; and a
// Running backup rewrote status.lastRunTime on every pass.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

func capitalisedShardedBackup() (*neo4jv1beta1.Neo4jShardedDatabase, *neo4jv1beta1.Neo4jBackup) {
	sd := &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "products-sdb", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
			ClusterRef: "ec", Name: "Products",
			PropertySharding: neo4jv1beta1.PropertyShardingConfiguration{PropertyShards: 2},
		},
	}
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bk", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef: "ec", ShardedDatabase: "products-sdb",
			Storage: neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "p"}},
			Options: &neo4jv1beta1.BackupOptions{Validate: ptr.To(true)},
		},
	}
	return sd, backup
}

// neo4j-admin names each shard's artifact after the shard as Neo4j names it,
// lower-cased; the expected stubs carried the spec's capitals, so no stub
// matched its log line and the run never had its file names.
func TestShardedBackup_ACapitalisedNameRecordsItsShards(t *testing.T) {
	sd, backup := capitalisedShardedBackup()
	r := newShardedTestReconciler(t, sd, backup)
	ctx := context.Background()

	expected := r.expectedShardArtifactsForBackup(ctx, backup)
	names := make([]string, 0, len(expected))
	for _, a := range expected {
		names = append(names, a.ShardName)
	}
	assert.Equal(t, []string{"products-g000", "products-p000", "products-p001"}, names)

	// Lines as neo4j-admin 2025.12 prints them (BackupOutputMonitor).
	log := "Start creating artifact 'products-g000-2026-10-24T10-00-00.backup' for database 'products-g000'.\n" +
		"Finished artifact creation 'products-g000-2026-10-24T10-00-00.backup' for database 'products-g000', took 1s.\n" +
		"Finished artifact creation 'products-p000-2026-10-24T10-00-01.backup' for database 'products-p000', took 1s.\n" +
		"Finished artifact creation 'products-p001-2026-10-24T10-00-02.backup' for database 'products-p001', took 1s.\n"
	merged := mergeShardArtifactsFromLog(expected, parseShardArtifactsFromLog(log))
	for _, a := range merged {
		assert.NotEmpty(t, a.Filename, "shard %s has its artifact", a.ShardName)
	}

	// A stub an earlier operator built with the spec's capitals matches too.
	old := mergeShardArtifactsFromLog([]neo4jv1beta1.ShardArtifact{{ShardName: "Products-g000"}}, parseShardArtifactsFromLog(log))
	assert.Equal(t, "products-g000-2026-10-24T10-00-00.backup", old[0].Filename)
}

// The backup names the family's graph and property shards, lower-cased, not
// every database whose name begins with the family's: a copy restored beside
// it as ProductsCopy made `Products*` take `productscopy-g000` along, and the
// glob-safety check refused every later backup of Products.
func TestShardedBackup_NamesOnlyItsOwnShards(t *testing.T) {
	sd, backup := capitalisedShardedBackup()
	r := newShardedTestReconciler(t, sd, backup)

	cmd, err := r.buildBackupCommand(context.Background(), backup, fieldFindingsCluster("2026.08.1-enterprise"))
	require.NoError(t, err)
	assert.Contains(t, cmd, `neo4j-admin database backup`)
	assert.Contains(t, cmd, `"products-g*" "products-p*"`)
	assert.NotContains(t, cmd, `Products`)
	assert.Contains(t, cmd, `--database="products"`, "validate takes the parent name")

	dbs := databaseInfos("products", "products-g000", "products-p000", "products-p001",
		"productscopy", "productscopy-g000", "productscopy-p000", "productscopy-p001", "productsales")
	assert.Empty(t, shardedGlobPoisoning("Products", dbs), "a copy beside it is no concern of this backup")
	assert.Equal(t, []string{"products-g-old"}, shardedGlobPoisoning("Products", append(dbs, databaseInfos("products-g-old")...)),
		"a database the patterns do take is still refused")
}

// staleBackupCache serves the first read of a Neo4jBackup from a copy taken
// before the write that ended the run — what the informer cache can still
// hold when the Job's deletion starts the next pass — and reads through
// afterwards.
type staleBackupCache struct {
	client.Client
	stale *neo4jv1beta1.Neo4jBackup
}

func (c *staleBackupCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if b, ok := obj.(*neo4jv1beta1.Neo4jBackup); ok && c.stale != nil {
		c.stale.DeepCopyInto(b)
		c.stale = nil
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// The walk's backup announced BackupCompleted when its Job went, and a pass
// that read it from before that write found the Job gone, started a new one
// and wrote Failed over Completed. The terminal phase is read from the API
// server when the pass's copy is not terminal.
func TestReconcile_AStalePassDoesNotReopenACompletedBackup(t *testing.T) {
	ctx := context.Background()
	cluster := minimalClusterForRestore("c", "ns")
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "first-backup", Namespace: "ns", Finalizers: []string{BackupFinalizer}},
		Spec: neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", Database: "neo4j",
			Storage: neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups", Size: "1Gi"}}},
		Status: neo4jv1beta1.Neo4jBackupStatus{Phase: neo4jv1beta1.PhaseCompleted, Message: "Backup completed successfully"},
	}
	r := newBackupTestReconcilerWithStatus(t, cluster, backup)
	stale := backup.DeepCopy()
	stale.Status.Phase = neo4jv1beta1.PhaseRunning
	stale.Status.Message = "Backup completed; capturing artifact metadata from the backup Pod"
	apiServer := r.Client
	r.Client = &staleBackupCache{Client: apiServer, stale: stale}
	r.APIReader = apiServer

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "first-backup", Namespace: "ns"}})
	require.NoError(t, err)

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, apiServer.Get(ctx, types.NamespacedName{Name: "first-backup", Namespace: "ns"}, got))
	assert.Equal(t, neo4jv1beta1.PhaseCompleted, got.Status.Phase)
	err = apiServer.Get(ctx, types.NamespacedName{Name: "first-backup-backup", Namespace: "ns"}, &batchv1.Job{})
	assert.True(t, apierrors.IsNotFound(err), "no second backup Job")
}

// status.lastRunTime was set on every write of a Running backup, so every pass
// changed status, and every status write is a reconcile: a write about every
// 27s while it ran. It is stamped when the backup enters Running, and a write
// that changes nothing is not sent.
func TestUpdateBackupStatus_StampsTheRunOnceAndWritesOnlyChanges(t *testing.T) {
	ctx := context.Background()
	started := metav1.NewTime(time.Now().Add(-10 * time.Minute).Truncate(time.Second))
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", Database: "neo4j"},
		Status:     neo4jv1beta1.Neo4jBackupStatus{Phase: neo4jv1beta1.PhaseRunning, Message: "Backup job b-backup created", LastRunTime: &started},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	writes := 0
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(backup).
		WithStatusSubresource(&neo4jv1beta1.Neo4jBackup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				writes++
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Neo4jBackupReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(4)}
	get := func() *neo4jv1beta1.Neo4jBackup {
		got := &neo4jv1beta1.Neo4jBackup{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(backup), got))
		return got
	}

	r.updateBackupStatus(ctx, backup, neo4jv1beta1.PhaseRunning, "Backup job is running")
	require.Equal(t, 1, writes, "a new message is written")
	assert.True(t, get().Status.LastRunTime.Equal(&started), "still Running: the run's start is kept")

	for range 3 {
		r.updateBackupStatus(ctx, backup, neo4jv1beta1.PhaseRunning, "Backup job is running")
	}
	assert.Equal(t, 1, writes, "the same status is not written again")

	r.updateBackupStatus(ctx, backup, neo4jv1beta1.PhaseCompleted, "Backup completed successfully")
	got := get()
	assert.True(t, got.Status.LastRunTime.Equal(&started))
	require.NotNil(t, got.Status.LastSuccessTime, "entering Completed stamps the success")
}

// BackupCompleted is announced by the write that makes the backup Completed:
// a second pass through the finished Job, on a copy from before that write,
// announces nothing.
func TestHandleExistingBackupJob_AnnouncesCompletedOnce(t *testing.T) {
	ctx := context.Background()
	finished := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("uid-1"), Name: "b-backup", Namespace: "ns"},
		Status: batchv1.JobStatus{Succeeded: 1, StartTime: &finished, CompletionTime: &finished,
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}},
	}
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", Database: "neo4j"},
		Status:     neo4jv1beta1.Neo4jBackupStatus{Phase: neo4jv1beta1.PhaseRunning, Message: "Backup job is running"},
	}
	r := newBackupTestReconcilerWithStatus(t, backup, job) // no Pod: the run is recorded at once
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)

	_, err := r.handleExistingBackupJob(ctx, backup.DeepCopy(), job)
	require.NoError(t, err)
	_, err = r.handleExistingBackupJob(ctx, backup.DeepCopy(), job) // the same stale copy
	require.NoError(t, err)

	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonBackupCompleted), 1)
}

// databaseInfos is a SHOW DATABASES listing of the given names.
func databaseInfos(names ...string) []neo4jclient.DatabaseInfo {
	out := make([]neo4jclient.DatabaseInfo, 0, len(names))
	for _, n := range names {
		out = append(out, neo4jclient.DatabaseInfo{Name: n})
	}
	return out
}
