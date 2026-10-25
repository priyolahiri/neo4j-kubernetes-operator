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

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The v1.22.x re-walk logged "Failed to update cluster after removing
// finalizer … the object has been modified" and a "Reconciler error" on a
// cluster deletion; the retry succeeded the same second. The release patched
// with the resourceVersion of a copy older than the object — the pass had just
// written status, and the cache had not caught up — so it was a Conflict.
// Removing one finalizer does not need the lock: it is a JSON patch that tests
// the finalizer is where the copy has it. Every kind's release goes through it
// (knowledge rule 189).

// staleCacheOf serves every Get of key from stale, as an informer cache that
// has not seen the last write would; everything else goes to the store.
type staleCacheOf struct {
	client.Client
	key   client.ObjectKey
	stale client.Object
}

func (l *staleCacheOf) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if key == l.key && reflect.TypeOf(obj) == reflect.TypeOf(l.stale) {
		reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(l.stale.DeepCopyObject()).Elem())
		return nil
	}
	return l.Client.Get(ctx, key, obj, opts...)
}

// bumped writes an annotation to the stored object, so its resourceVersion is
// newer than any copy read before — what a status write does.
func bumped(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	live, ok := obj.DeepCopyObject().(client.Object)
	require.True(t, ok)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), live))
	live.SetAnnotations(map[string]string{"test/bumped": "true"})
	require.NoError(t, c.Update(context.Background(), live))
	require.NotEqual(t, obj.GetResourceVersion(), live.GetResourceVersion())
}

type releaseCase struct {
	name      string
	obj       client.Object
	finalizer string
	others    []client.Object
	reconcile func(c client.Client) reconcile.Reconciler
}

// releaseCases is every kind whose deletion ends in a finalizer release
// through Reconcile: the eight of id 178 with a Ready target that drops at
// once, the plugin, the backup and the restore with no target, and the Aura
// kinds under deletionPolicy Orphan. (The cluster and the standalone are
// driven through handleDeletion below.)
func releaseCases(t *testing.T) []releaseCase {
	var cases []releaseCase
	for _, k := range deletionKinds() {
		cases = append(cases, releaseCase{name: k.name, obj: k.obj(time.Now()), finalizer: k.finalizer,
			others: []client.Object{unreachableTarget()},
			reconcile: func(c client.Client) reconcile.Reconciler {
				var connErr error
				r, _ := k.setup(c, record.NewFakeRecorder(50), &connErr)
				return r
			}})
	}
	at := metav1.Now()
	plugin := &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: deletionMeta("apoc", PluginFinalizer, at),
		Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "absent", Name: "apoc"},
	}
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: deletionMeta("nightly", BackupFinalizer, at),
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "absent", Database: "neo4j"},
	}
	restore := &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: deletionMeta("back", RestoreFinalizer, at),
		Spec:       neo4jv1beta1.Neo4jRestoreSpec{InstanceRef: "absent", Database: "neo4j"},
	}
	cases = append(cases,
		releaseCase{name: "Neo4jPlugin", obj: plugin, finalizer: PluginFinalizer,
			reconcile: func(c client.Client) reconcile.Reconciler {
				return &Neo4jPluginReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(50)}
			}},
		releaseCase{name: "Neo4jBackup", obj: backup, finalizer: BackupFinalizer,
			reconcile: func(c client.Client) reconcile.Reconciler {
				return &Neo4jBackupReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(50)}
			}},
		releaseCase{name: "Neo4jRestore", obj: restore, finalizer: RestoreFinalizer,
			reconcile: func(c client.Client) reconcile.Reconciler {
				return &Neo4jRestoreReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(50)}
			}},
	)
	for _, tc := range auraDeletionCases() {
		cases = append(cases, releaseCase{name: tc.name, obj: tc.obj("Orphan", time.Now()), finalizer: tc.finalizer,
			reconcile: func(c client.Client) reconcile.Reconciler {
				r, _ := tc.newReconciler(c, record.NewFakeRecorder(50))
				return r
			}})
	}
	return cases
}

func releaseClient(t *testing.T, tc releaseCase) *fake.ClientBuilder {
	t.Helper()
	// The Aura builder adds the Aura kinds' credentials Secret and status
	// subresources; the others' are added here.
	objs := append([]client.Object{tc.obj}, tc.others...)
	b := auraFakeClientBuilder(t, auraTestScheme(t), objs...)
	if !strings.HasPrefix(tc.name, "Aura") {
		b = b.WithStatusSubresource(tc.obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{})
	}
	return b
}

func TestFinalizerRelease_FromACopyOlderThanTheObject(t *testing.T) {
	for _, tc := range releaseCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			store := releaseClient(t, tc).Build()
			key := client.ObjectKeyFromObject(tc.obj)
			stale, ok := tc.obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, store.Get(context.Background(), key, stale))
			bumped(t, store, stale)
			c := &staleCacheOf{Client: store, key: key, stale: stale}

			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			_, err := tc.reconcile(c).Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err, "a release from a copy older than the object is not a Conflict")
			assert.Empty(t, sink.errors())
			assert.True(t, finalizerReleased(t, store, tc.obj, tc.finalizer))
		})
	}
}

// Item 2 of the re-walk: two of six plugin deletions logged "Failed to remove
// finalizer … not found". The plugin, the backup and the restore release
// through removeFinalizer, so an object already gone is released.
func TestFinalizerRelease_OfAPluginBackupOrRestoreAlreadyGone(t *testing.T) {
	for _, tc := range releaseCases(t) {
		switch tc.name {
		case "Neo4jPlugin", "Neo4jBackup", "Neo4jRestore":
		default:
			continue // TestDeletion_ and TestAuraDeletion_ReleasingAnObjectAlreadyGoneIsNotAnError
		}
		t.Run(tc.name, func(t *testing.T) {
			key := client.ObjectKeyFromObject(tc.obj)
			c := releaseClient(t, tc).WithInterceptorFuncs(goneOnRelease(key)).Build()
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			_, err := tc.reconcile(c).Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			assert.Empty(t, sink.errors(), "no \"Failed to remove finalizer\"")
		})
	}
}

// The cluster's and the standalone's deletion write status before they
// release; their own copy is older than the object.
func TestHandleDeletion_ReleasesFromACopyOlderThanTheObject(t *testing.T) {
	now := metav1.Now()
	sink := newLevelCaptureSink()
	ctx := log.IntoContext(context.Background(), logr.New(sink))

	cluster := minimalCluster("old-copy", "default")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.DeletionTimestamp = &now
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(cluster).
		WithStatusSubresource(cluster).Build()
	stale := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, fc.Get(ctx, client.ObjectKeyFromObject(cluster), stale))
	bumped(t, fc, stale)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: newTestScheme()}
	_, err := r.handleDeletion(ctx, stale)
	require.NoError(t, err)
	assert.True(t, finalizerReleased(t, fc, cluster, ClusterFinalizer))

	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{
		Name: "old-copy", Namespace: "default", Finalizers: []string{StandaloneFinalizer}, DeletionTimestamp: &now}}
	sc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(sa).WithStatusSubresource(sa).Build()
	staleSA := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, sc.Get(ctx, client.ObjectKeyFromObject(sa), staleSA))
	bumped(t, sc, staleSA)
	rs := &Neo4jEnterpriseStandaloneReconciler{Client: sc, Scheme: newTestScheme()}
	_, err = rs.handleDeletion(ctx, staleSA)
	require.NoError(t, err)
	assert.True(t, finalizerReleased(t, sc, sa, StandaloneFinalizer))
	assert.Empty(t, sink.errors())
}

// removeFinalizer never drops another writer's finalizer: one added after the
// copy was read stays, and one inserted before this one moves it, which the
// removal finds by reading the object again.
func TestRemoveFinalizer_KeepsAnotherWritersFinalizer(t *testing.T) {
	const other = "example.com/other"
	for _, tc := range []struct {
		name string
		live []string
	}{
		{"added after it", []string{BackupFinalizer, other}},
		{"inserted before it", []string{other, BackupFinalizer}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			obj := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "ns",
				Finalizers: []string{BackupFinalizer}}}
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj).Build()
			stale := &neo4jv1beta1.Neo4jBackup{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), stale))
			live := stale.DeepCopy()
			live.Finalizers = tc.live
			require.NoError(t, c.Update(ctx, live))

			require.NoError(t, removeFinalizer(ctx, c, stale, BackupFinalizer))
			got := &neo4jv1beta1.Neo4jBackup{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), got))
			assert.Equal(t, []string{other}, got.Finalizers)
			assert.False(t, controllerutil.ContainsFinalizer(got, BackupFinalizer))
		})
	}
}

// Against an API server: a copy older than the object (a status write since)
// releases with no Conflict, a finalizer another writer inserted before this
// one is kept (the server answers the moved index with 422, and the removal
// reads the object again), and an object already gone is released.
func TestRemoveFinalizer_APIServer(t *testing.T) {
	c, _ := startStoredSpecEnv(t)
	ctx := context.Background()
	const other = "example.com/other"
	newDB := func(name string, finalizers ...string) *neo4jv1beta1.Neo4jDatabase {
		t.Helper()
		db := &neo4jv1beta1.Neo4jDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Finalizers: finalizers},
			Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "absent", Name: "orders"},
		}
		require.NoError(t, c.Create(ctx, db))
		return db
	}
	finalizersOf := func(name string) []string {
		t.Helper()
		got := &neo4jv1beta1.Neo4jDatabase{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, got))
		return got.Finalizers
	}

	t.Run("a copy older than the object", func(t *testing.T) {
		stale := newDB("stale", DatabaseFinalizer)
		live := stale.DeepCopy()
		live.Status.Message = "written after the copy was read"
		require.NoError(t, c.Status().Update(ctx, live))
		require.NotEqual(t, stale.ResourceVersion, live.ResourceVersion)

		require.NoError(t, removeFinalizer(ctx, c, stale, DatabaseFinalizer))
		assert.Empty(t, finalizersOf("stale"))
	})

	t.Run("another writer's finalizer inserted before it", func(t *testing.T) {
		stale := newDB("moved", DatabaseFinalizer)
		live := stale.DeepCopy()
		live.Finalizers = []string{other, DatabaseFinalizer}
		require.NoError(t, c.Update(ctx, live))

		require.NoError(t, removeFinalizer(ctx, c, stale, DatabaseFinalizer))
		assert.Equal(t, []string{other}, finalizersOf("moved"))
	})

	t.Run("an object already gone", func(t *testing.T) {
		stale := newDB("gone")
		require.NoError(t, c.Delete(ctx, stale))
		stale.Finalizers = []string{DatabaseFinalizer}
		require.NoError(t, removeFinalizer(ctx, c, stale, DatabaseFinalizer))
	})
}
