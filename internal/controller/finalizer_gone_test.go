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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A pass that read an object before an earlier pass released its finalizer —
// the cache lags the deletion that release allowed — releases it again and
// finds nothing to patch. That NotFound was returned: logged at ERROR by the
// controller, and again by controller-runtime as "Reconciler error", on routine
// deletions of a Neo4jDatabase and of every other kind (#572 fixed the
// cluster's alone). An object that is gone is released (knowledge rule 189).

// goneOnRelease makes the API server answer the finalizer release of key with
// NotFound, as it does once the object is gone.
func goneOnRelease(key client.ObjectKey) interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if client.ObjectKeyFromObject(obj) == key {
				return apierrors.NewNotFound(schema.GroupResource{Group: neo4jv1beta1.GroupVersion.Group}, key.Name)
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}
}

func TestRemoveFinalizer_AnObjectAlreadyGoneIsReleased(t *testing.T) {
	stale := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "ns",
		Finalizers: []string{BackupFinalizer}}}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).Build()

	assert.NoError(t, removeFinalizer(context.Background(), c, stale, BackupFinalizer),
		"the backup, restore, plugin, cluster and standalone deletions release through this too")

	// Any other failure is still the caller's.
	refused := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(stale.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				return apierrors.NewConflict(schema.GroupResource{}, "nightly", nil)
			},
		}).Build()
	assert.True(t, apierrors.IsConflict(removeFinalizer(context.Background(), refused, stale.DeepCopy(), BackupFinalizer)))
}

// Each kind whose deletion runs Cypher, driven through Reconcile with a Ready
// target: the drop succeeds and the release finds the object gone.
func TestDeletion_ReleasingAnObjectAlreadyGoneIsNotAnError(t *testing.T) {
	for _, k := range deletionKinds() {
		t.Run(k.name, func(t *testing.T) {
			obj := k.obj(time.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).
				WithInterceptorFuncs(goneOnRelease(client.ObjectKeyFromObject(obj))).Build()
			var connErr error
			r, sent := k.setup(c, record.NewFakeRecorder(50), &connErr)

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
			require.NoError(t, err, "an object already gone is released, not a reconcile error")
			assert.Equal(t, 1, sent(), "the cleanup ran")
		})
	}
}

// The Aura kinds release through their own patch helpers, which read the
// object again first: gone, that read is the NotFound.
func TestAuraDeletion_ReleasingAnObjectAlreadyGoneIsNotAnError(t *testing.T) {
	scheme := auraTestScheme(t)
	type auraCase struct {
		name          string
		obj           client.Object
		newReconciler func(c client.Client) reconcile.Reconciler
	}
	var cases []auraCase
	for _, tc := range auraDeletionCases() {
		cases = append(cases, auraCase{name: tc.name, obj: tc.obj("Orphan", time.Now()),
			newReconciler: func(c client.Client) reconcile.Reconciler {
				r, _ := tc.newReconciler(c, record.NewFakeRecorder(20))
				return r
			}})
	}
	db := &neo4jv1beta1.AuraDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "db-gone", Namespace: testNS,
			Annotations: map[string]string{AuraExternalDatabaseAnnotation: "db-1"}},
		Spec: neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "no-such-instance", Name: "analytics", OrganizationID: "org-1"},
	}
	terminatingSince(db, AuraDatabaseFinalizer, time.Now())
	cases = append(cases, auraCase{name: "AuraDatabase", obj: db,
		newReconciler: func(c client.Client) reconcile.Reconciler {
			return &AuraDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(20),
				ClientFactory: dbFactory(&fakeDatabaseAPI{})}
		}})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := client.ObjectKeyFromObject(tc.obj)
			c := auraFakeClientBuilder(t, scheme, tc.obj).WithInterceptorFuncs(goneOnRelease(key)).Build()

			_, err := tc.newReconciler(c).Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err, "an object already gone is released, not a reconcile error")
		})
	}
}
