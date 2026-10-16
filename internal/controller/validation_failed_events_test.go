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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// A CR whose spec is invalid is reconciled again every RequeueAfter (and on
// every resync) until someone fixes it. Its ValidationFailed Warning used to
// be emitted on each of those passes, spending the object's event budget —
// client-go's spam filter allows a burst of 25 per object, then one every five
// minutes — on a failure already in status. It is announced when the failure
// starts or its message changes, read from the persisted status, so an
// operator restart announces nothing either (knowledge rule 136).

type validationFailedCase struct {
	name string
	// obj is invalid; edit makes it invalid differently (a new message).
	obj           client.Object
	edit          func(client.Object)
	newReconciler func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
}

// editAs adapts an edit of one concrete kind to validationFailedCase.edit.
func editAs[T client.Object](edit func(T)) func(client.Object) {
	return func(o client.Object) {
		if typed, ok := o.(T); ok {
			edit(typed)
		}
	}
}

func validationFailedCases() []validationFailedCase {
	meta := func(name, finalizer string) metav1.ObjectMeta {
		m := metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1}
		if finalizer != "" {
			m.Finalizers = []string{finalizer}
		}
		return m
	}
	return []validationFailedCase{
		{
			name: "Neo4jDatabase",
			obj: &neo4jv1beta1.Neo4jDatabase{
				ObjectMeta: meta("db", DatabaseFinalizer),
				Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "ec", Name: "system"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jDatabase) { o.Spec.Name = "bad_name" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					DatabaseValidator: validation.NewDatabaseValidator(c)}
			},
		},
		{
			name: "Neo4jShardedDatabase",
			obj: func() client.Object {
				sd := productsShardedDB()
				sd.Spec.DefaultCypherLanguage = "5"
				return sd
			}(),
			edit: editAs(func(o *neo4jv1beta1.Neo4jShardedDatabase) {
				o.Spec.PropertySharding.PropertyShards = 0
			}),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jShardedDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			},
		},
		{
			name: "Neo4jCompositeDatabase",
			obj: &neo4jv1beta1.Neo4jCompositeDatabase{
				ObjectMeta: meta("cd", Neo4jCompositeDatabaseFinalizer),
				Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "ec", Name: "bad_name"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jCompositeDatabase) { o.Spec.Name = "system" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewCompositeDatabaseValidator(c)}
			},
		},
		{
			name: "Neo4jRole",
			obj: &neo4jv1beta1.Neo4jRole{
				ObjectMeta: meta("role", Neo4jRoleFinalizer),
				Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "ec", Name: "admin"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jRole) { o.Spec.Name = "reader" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewRoleValidator(c)}
			},
		},
		{
			name: "Neo4jRoleBinding",
			obj: &neo4jv1beta1.Neo4jRoleBinding{
				ObjectMeta: meta("rb", Neo4jRoleBindingFinalizer),
				Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "ec", Username: "alice"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jRoleBinding) { o.Spec.Roles = []string{" "} }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewRoleBindingValidator(c)}
			},
		},
		{
			name: "Neo4jDatabaseAlias",
			obj: &neo4jv1beta1.Neo4jDatabaseAlias{
				ObjectMeta: meta("alias", Neo4jDatabaseAliasFinalizer),
				Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "ec", Name: "system", TargetDatabase: "orders"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jDatabaseAlias) { o.Spec.Name = "orders" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewAliasValidator(c)}
			},
		},
		{
			name: "Neo4jUser",
			obj: &neo4jv1beta1.Neo4jUser{
				ObjectMeta: meta("user", Neo4jUserFinalizer),
				Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "ec", Username: "system"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jUser) { o.Spec.Username = "bad name!" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewUserValidator(c)}
			},
		},
		{
			name: "Neo4jAuthRule",
			obj: &neo4jv1beta1.Neo4jAuthRule{
				ObjectMeta: meta("rule", Neo4jAuthRuleFinalizer),
				Spec:       neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "ec", Name: "1bad"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jAuthRule) { o.Spec.Name = "2bad" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewAuthRuleValidator(c)}
			},
		},
		{
			name: "Neo4jReplicaDatabase",
			obj: &neo4jv1beta1.Neo4jReplicaDatabase{
				ObjectMeta: meta("replica", Neo4jReplicaDatabaseFinalizer),
				Spec:       neo4jv1beta1.Neo4jReplicaDatabaseSpec{ClusterRef: "ec", Name: "orders"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jReplicaDatabase) { o.Spec.Name = "ord`ers" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jReplicaDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewReplicaValidator(c)}
			},
		},
		{
			name: "Neo4jPlugin",
			obj: &neo4jv1beta1.Neo4jPlugin{
				ObjectMeta: meta("apoc", PluginFinalizer),
				Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "ec", Name: "apoc"},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jPlugin) { o.Spec.Name = "" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jPluginReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewPluginValidator()}
			},
		},
		{
			name: "Neo4jEnterpriseCluster",
			obj: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: meta("ec", ClusterFinalizer),
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
					Topology: neo4jv1beta1.TopologyConfiguration{Servers: 1},
					Storage:  neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jEnterpriseCluster) { o.Spec.Storage.Size = "" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewClusterValidator(c)}
			},
		},
		{
			name: "Neo4jEnterpriseStandalone",
			obj: &neo4jv1beta1.Neo4jEnterpriseStandalone{
				ObjectMeta: meta("sa", StandaloneFinalizer),
				Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
					AcceptLicenseAgreement: "eval",
					Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
					Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard"},
				},
			},
			edit: editAs(func(o *neo4jv1beta1.Neo4jEnterpriseStandalone) {
				o.Spec.AcceptLicenseAgreement = ""
			}),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewStandaloneValidator()}
			},
		},
	}
}

func TestReconcile_ValidationFailedIsAnnouncedOnce(t *testing.T) {
	for _, tc := range validationFailedCases() {
		t.Run(tc.name, func(t *testing.T) {
			const reason = EventReasonValidationFailed
			ctx := context.Background()
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(tc.obj).
				WithStatusSubresource(tc.obj).Build()
			rec := record.NewFakeRecorder(100)
			key := client.ObjectKeyFromObject(tc.obj)
			reconcileWith := func(r reconcile.Reconciler) {
				t.Helper()
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
			}
			r := tc.newReconciler(c, rec)

			for range 3 {
				reconcileWith(r)
			}
			failed := eventsWithReason(drainEvents(rec), reason)
			require.Len(t, failed, 1, "announced when the failure starts, not on every pass: %q", failed)

			// The spec is still invalid, differently: the new message is news.
			live, ok := tc.obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(ctx, key, live))
			tc.edit(live)
			require.NoError(t, c.Update(ctx, live))
			reconcileWith(r)
			reconcileWith(r)
			again := eventsWithReason(drainEvents(rec), reason)
			require.Len(t, again, 1, "a changed failure is announced once: %q", again)
			assert.NotEqual(t, failed[0], again[0])

			// An operator restart reads the failure from status and stays quiet.
			reconcileWith(tc.newReconciler(c, rec))
			assert.Empty(t, eventsWithReason(drainEvents(rec), reason))
		})
	}
}
