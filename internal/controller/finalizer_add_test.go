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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The v1.22.x walk: a Conflict on a new Neo4jRoleBinding logged "Reconciler
// error". Its first pass added the finalizer with a merge patch locked to the
// resourceVersion of a copy older than the object. A finalizer's addition is
// now what its removal became in #579: a JSON patch that tests the list the
// copy has and appends to it, without the lock, keeping any finalizer another
// writer added (knowledge rule 199).

// addCases is every kind whose first pass adds a finalizer: the kinds of
// releaseCases as new objects (the Aura ones with credentials they can read,
// which they resolve before the finalizer), and the cluster and standalone.
func addCases(t *testing.T) []releaseCase {
	var cases []releaseCase
	for _, tc := range releaseCases(t) {
		obj, ok := tc.obj.DeepCopyObject().(client.Object)
		require.True(t, ok)
		obj.SetDeletionTimestamp(nil)
		obj.SetFinalizers(nil)
		switch v := obj.(type) {
		case *neo4jv1beta1.AuraInstance:
			v.Spec.CredentialsSecretRef = credRef()
		case *neo4jv1beta1.AuraCustomerManagedKey:
			v.Spec.CredentialsSecretRef = credRef()
		case *neo4jv1beta1.AuraIPFilter:
			v.Spec.CredentialsSecretRef = credRef()
		case *neo4jv1beta1.AuraInvite:
			v.Spec.CredentialsSecretRef = credRef()
		case *neo4jv1beta1.AuraOrganizationMember:
			v.Spec.CredentialsSecretRef = credRef()
		case *neo4jv1beta1.AuraProjectMember:
			v.Spec.CredentialsSecretRef = credRef()
		}
		tc.obj = obj
		cases = append(cases, tc)
	}
	cluster := minimalCluster("new", repeatedWarningsNS)
	standalone := authTargetStandalone()
	standalone.Name = "new"
	standalone.Status = neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{}
	return append(cases,
		releaseCase{name: "Neo4jEnterpriseCluster", obj: cluster, finalizer: ClusterFinalizer,
			others: []client.Object{&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "standard"},
				Provisioner: "kubernetes.io/no-provisioner"}},
			reconcile: func(c client.Client) reconcile.Reconciler {
				return &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(50)}
			}},
		releaseCase{name: "Neo4jEnterpriseStandalone", obj: standalone, finalizer: StandaloneFinalizer,
			reconcile: func(c client.Client) reconcile.Reconciler {
				return &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(50)}
			}},
	)
}

func TestFinalizerAdd_FromACopyOlderThanTheObject(t *testing.T) {
	for _, tc := range addCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			store := releaseClient(t, tc).WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
			key := client.ObjectKeyFromObject(tc.obj)
			stale, ok := tc.obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, store.Get(context.Background(), key, stale))
			bumped(t, store, stale)
			c := &staleCacheOf{Client: store, key: key, stale: stale}

			_, err := tc.reconcile(c).Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			if err != nil {
				// What a pass does after the addition is not this test's: an
				// Aura kind goes on to its API. The addition is not a Conflict.
				require.False(t, apierrors.IsConflict(err), "%v", err)
				require.NotContains(t, err.Error(), "has been modified")
			}
			live, ok := tc.obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, store.Get(context.Background(), key, live))
			assert.True(t, controllerutil.ContainsFinalizer(live, tc.finalizer), "the finalizer is added")
			assert.Equal(t, "true", live.GetAnnotations()["test/bumped"], "the other write is kept")
		})
	}
}

// addFinalizer keeps every finalizer another writer added since the copy was
// read, whether the copy had none (metadata.finalizers absent) or some, and
// adds nothing twice.
func TestAddFinalizer_KeepsAnotherWritersFinalizer(t *testing.T) {
	const other = "example.com/other"
	for _, tc := range []struct {
		name        string
		copyHas     []string
		liveHas     []string
		want        []string
		wantPatches int
	}{
		{"the copy has none, the object none", nil, nil, []string{BackupFinalizer}, 1},
		{"the copy has none, another writer added one", nil, []string{other}, []string{other, BackupFinalizer}, 2},
		{"the copy has one, another writer added one", []string{"a.example.com/x"}, []string{"a.example.com/x", other},
			[]string{"a.example.com/x", other, BackupFinalizer}, 2},
		{"another writer added this one", nil, []string{BackupFinalizer}, []string{BackupFinalizer}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			obj := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "ns",
				Finalizers: tc.copyHas}}
			store := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj).Build()
			stale := &neo4jv1beta1.Neo4jBackup{}
			require.NoError(t, store.Get(ctx, client.ObjectKeyFromObject(obj), stale))
			live := stale.DeepCopy()
			live.Finalizers = tc.liveHas
			require.NoError(t, store.Update(ctx, live))

			require.NoError(t, addFinalizer(ctx, store, stale, BackupFinalizer))
			got := &neo4jv1beta1.Neo4jBackup{}
			require.NoError(t, store.Get(ctx, client.ObjectKeyFromObject(obj), got))
			assert.Equal(t, tc.want, got.Finalizers)
		})
	}
}

// Against an API server: a copy older than the object, with no finalizers
// (metadata.finalizers absent) or some, gets the finalizer with no Conflict;
// one another writer added since is kept.
func TestAddFinalizer_APIServer(t *testing.T) {
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

	t.Run("no finalizers, a status write since", func(t *testing.T) {
		stale := newDB("none")
		live := stale.DeepCopy()
		live.Status.Message = "written after the copy was read"
		require.NoError(t, c.Status().Update(ctx, live))

		require.NoError(t, addFinalizer(ctx, c, stale, DatabaseFinalizer))
		assert.Equal(t, []string{DatabaseFinalizer}, finalizersOf("none"))
	})

	t.Run("no finalizers, another writer's added since", func(t *testing.T) {
		stale := newDB("raced")
		live := stale.DeepCopy()
		live.Finalizers = []string{other}
		require.NoError(t, c.Update(ctx, live))

		require.NoError(t, addFinalizer(ctx, c, stale, DatabaseFinalizer))
		assert.Equal(t, []string{other, DatabaseFinalizer}, finalizersOf("raced"))
	})

	t.Run("some finalizers, a status write since", func(t *testing.T) {
		stale := newDB("some", other)
		live := stale.DeepCopy()
		live.Status.Message = "written after the copy was read"
		require.NoError(t, c.Status().Update(ctx, live))

		require.NoError(t, addFinalizer(ctx, c, stale, DatabaseFinalizer))
		assert.Equal(t, []string{other, DatabaseFinalizer}, finalizersOf("some"))
	})
}
