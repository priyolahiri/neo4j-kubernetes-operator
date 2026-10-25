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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Deleting a user, role, binding, auth rule, composite, alias, replica or
// database while its cluster or standalone restarts — or reads Ready before its
// Bolt port accepts connections, as it does after every restart and during a
// stopCluster restore — released the finalizer at once (or, for some kinds
// and causes, waited for ever). The CR went and the user, the grant, the rule
// or the data stayed in Neo4j: access the user had asked to revoke. A deletion
// that cannot run its cleanup now waits, says so once, and after
// finalizerDeletionGracePeriod releases the finalizer with one Warning naming
// what is left and how to remove it. A target CR that is gone releases at
// once (knowledge rule 178).

// errCredentialsUnreadable is a connection that fails before reaching the
// target: its admin Secret cannot be read. Its text says "not found", which
// the drop classifier reads as "already gone" — the case a deletion must not
// take for one.
var errCredentialsUnreadable = errors.New(`failed to get credentials: secrets "sa-admin-secret" not found`)

// deletionKind is one kind whose deletion removes something in Neo4j.
type deletionKind struct {
	name      string
	finalizer string
	// reason is the Warning of a deletion that cannot finish; dropped the
	// Normal event of one that did.
	reason, dropped string
	// leftBehind and byHand are what the release Warning names.
	leftBehind, byHand string
	// checksReady: the deletion waits on a target that is not Ready before it
	// connects (the database's deletion connects and lets Neo4j answer).
	checksReady bool
	obj         func(deleted time.Time) client.Object
	// setup returns the reconciler, every connection of which returns
	// *connErr when that is set, and a count of the cleanup statements sent.
	setup func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int)
}

func deletionKinds() []deletionKind {
	authServer := func(connErr *error) (*fakeAuthServer, func() error) {
		s := newFakeAuthServer()
		s.users["alice"] = nil
		return s, func() error { return *connErr }
	}
	return []deletionKind{
		{
			name: "Neo4jUser", finalizer: Neo4jUserFinalizer, checksReady: true,
			reason: EventReasonUserDeletionFailed, dropped: EventReasonUserDeleted,
			leftBehind: `user "alice" on clusterRef "sa"`, byHand: "DROP USER `alice`",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jUser{ObjectMeta: deletionMeta("alice", Neo4jUserFinalizer, metav1.NewTime(at)),
					Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice"}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s, conn := authServer(connErr)
				return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) {
						if err := conn(); err != nil {
							return nil, err
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jRole", finalizer: Neo4jRoleFinalizer, checksReady: true,
			reason: EventReasonRoleDeletionFailed, dropped: EventReasonRoleDeleted,
			leftBehind: `role "analyst" on clusterRef "sa"`, byHand: "DROP ROLE `analyst`",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jRole{ObjectMeta: deletionMeta("analyst", Neo4jRoleFinalizer, metav1.NewTime(at)),
					Spec: neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s, conn := authServer(connErr)
				return &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (roleDeletionClient, error) {
						if err := conn(); err != nil {
							return nil, err
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jRoleBinding", finalizer: Neo4jRoleBindingFinalizer, checksReady: true,
			reason: EventReasonBindingFailed, dropped: EventReasonBindingDeleted,
			leftBehind: `the role(s) [reader] granted to user "alice" on clusterRef "sa"`,
			byHand:     "REVOKE ROLE `reader` FROM `alice`",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jRoleBinding{
					ObjectMeta: deletionMeta("alice-binding", Neo4jRoleBindingFinalizer, metav1.NewTime(at)),
					Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "alice", Roles: []string{"reader"}},
					Status:     neo4jv1beta1.Neo4jRoleBindingStatus{GrantedRoles: []string{"reader"}},
				}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s, conn := authServer(connErr)
				return &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) {
						if err := conn(); err != nil {
							return nil, err
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jAuthRule", finalizer: Neo4jAuthRuleFinalizer, checksReady: true,
			reason: EventReasonAuthRuleFailed, dropped: EventReasonAuthRuleDeleted,
			leftBehind: `auth rule "rule" on clusterRef "sa"`, byHand: "DROP AUTH RULE `rule`",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: deletionMeta("rule", Neo4jAuthRuleFinalizer, metav1.NewTime(at)),
					Spec: neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "rule", Condition: "true"}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s, conn := authServer(connErr)
				return &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (authRuleDeletionClient, error) {
						if err := conn(); err != nil {
							return nil, err
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jCompositeDatabase", finalizer: Neo4jCompositeDatabaseFinalizer, checksReady: true,
			reason: EventReasonCompositeFailed, dropped: EventReasonCompositeDropped,
			leftBehind: `composite database "sales" and its constituent aliases on clusterRef "sa"`,
			byHand:     "DROP COMPOSITE DATABASE `sales` CASCADE ALIASES",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jCompositeDatabase{
					ObjectMeta: deletionMeta("sales", Neo4jCompositeDatabaseFinalizer, metav1.NewTime(at)),
					Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "sales"}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s, conn := authServer(connErr)
				return &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (compositeDeletionClient, error) {
						if err := conn(); err != nil {
							return nil, err
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jDatabaseAlias", finalizer: Neo4jDatabaseAliasFinalizer, checksReady: true,
			reason: EventReasonAliasFailed, dropped: EventReasonAliasDropped,
			leftBehind: `alias "latest" on clusterRef "sa"`, byHand: "DROP ALIAS `latest` FOR DATABASE",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jDatabaseAlias{
					ObjectMeta: deletionMeta("latest", Neo4jDatabaseAliasFinalizer, metav1.NewTime(at)),
					Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "latest", TargetDatabase: "orders"}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s, conn := authServer(connErr)
				return &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (aliasDeletionClient, error) {
						if err := conn(); err != nil {
							return nil, err
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jReplicaDatabase", finalizer: Neo4jReplicaDatabaseFinalizer, checksReady: true,
			reason: EventReasonReplicaFailed, dropped: EventReasonReplicaDropped,
			leftBehind: `replica database "orders-replica" on clusterRef "sa"`, byHand: "DROP DATABASE `orders-replica`",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jReplicaDatabase{
					ObjectMeta: deletionMeta("orders-replica", Neo4jReplicaDatabaseFinalizer, metav1.NewTime(at)),
					Spec:       neo4jv1beta1.Neo4jReplicaDatabaseSpec{ClusterRef: "sa", Name: "orders-replica"}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s := &scriptedReplicaServer{info: &neo4jclient.DatabaseInfo{Name: "orders-replica",
					Type: neo4jclient.DatabaseTypeReplica}}
				return &Neo4jReplicaDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget, client.Client) (replicaNeo4jClient, error) {
						if *connErr != nil {
							return nil, *connErr
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
		{
			name: "Neo4jDatabase", finalizer: DatabaseFinalizer,
			reason: EventReasonDeletionFailed, dropped: EventReasonDatabaseDeleted,
			leftBehind: `database "orders" on clusterRef "sa"`, byHand: "DROP DATABASE `orders`",
			obj: func(at time.Time) client.Object {
				return &neo4jv1beta1.Neo4jDatabase{
					ObjectMeta: deletionMeta("orders", DatabaseFinalizer, metav1.NewTime(at)),
					Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "sa", Name: "orders", IfNotExists: true}}
			},
			setup: func(c client.Client, rec record.EventRecorder, connErr *error) (reconcile.Reconciler, func() int) {
				s := &failingDropServer{}
				return &Neo4jDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, RequeueAfter: 30 * time.Second,
					newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone) (databaseNeo4jClient, error) {
						if *connErr != nil {
							return nil, *connErr
						}
						return s, nil
					}}, func() int { return s.drops }
			},
		},
	}
}

// statusMessage reads status.message of any of the kinds above.
func statusMessage(t *testing.T, c client.Client, obj client.Object) string {
	t.Helper()
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	require.NoError(t, err)
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), u))
	msg, _, _ := unstructured.NestedString(u.Object, "status", "message")
	return msg
}

// TestDeletion_DuringARestartWaitsThenNamesWhatIsLeft drives each kind's
// deletion through Reconcile.
func TestDeletion_DuringARestartWaitsThenNamesWhatIsLeft(t *testing.T) {
	type scenario struct {
		name      string
		ready     bool  // the standalone reads Ready
		connErr   error // what connecting returns
		cause     string
		readyOnly bool // only for kinds that check readiness
	}
	waits := []scenario{
		{name: "a target that is not Ready", ready: false, cause: "is not Ready or does not accept connections yet", readyOnly: true},
		{name: "a target that refuses Bolt", ready: true, connErr: errBoltRefused,
			cause: "is not Ready or does not accept connections yet"},
		{name: "credentials that cannot be read", ready: true, connErr: errCredentialsUnreadable,
			cause: `cannot connect to clusterRef "sa": failed to get credentials`},
	}
	for _, k := range deletionKinds() {
		for _, sc := range waits {
			if sc.readyOnly && !k.checksReady {
				continue
			}
			t.Run(k.name+"/waits for "+sc.name, func(t *testing.T) {
				obj := k.obj(time.Now())
				target := unreachableTarget()
				target.Status.Ready = sc.ready
				c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, target).
					WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
				rec := record.NewFakeRecorder(50)
				connErr := sc.connErr
				r, sent := k.setup(c, rec, &connErr)

				for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), repeatPasses) {
					require.NoError(t, err, "a deletion that waits is not an error (it would be logged at ERROR)")
				}
				assert.False(t, finalizerReleased(t, c, obj, k.finalizer), "the finalizer is kept while the cleanup cannot run")
				assert.Zero(t, sent())
				events := drainEvents(rec)
				warned := eventsWithReason(events, k.reason)
				require.Len(t, warned, 1, "the wait is announced once: %q", events)
				assert.Contains(t, warned[0], sc.cause)
				assert.Contains(t, warned[0], k.leftBehind)
				msg := statusMessage(t, c, obj)
				assert.Contains(t, msg, "deletion waits to remove "+k.leftBehind, "status says why")
			})
		}

		t.Run(k.name+"/after the grace period it releases and names what is left", func(t *testing.T) {
			obj := k.obj(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			connErr := errBoltRefused
			r, sent := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
				require.NoError(t, err)
			}
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer), "a deletion does not wedge")
			assert.Zero(t, sent())
			released := eventsWithReason(drainEvents(rec), k.reason)
			require.Len(t, released, 1)
			assert.True(t, strings.HasPrefix(released[0], "Warning "))
			assert.Contains(t, released[0], "Released the finalizer without removing "+k.leftBehind)
			assert.Contains(t, released[0], k.byHand)
		})

		t.Run(k.name+"/a Ready target drops at once", func(t *testing.T) {
			obj := k.obj(time.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, sent := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
				require.NoError(t, err)
			}
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer))
			assert.Equal(t, 1, sent())
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, k.dropped), 1, "%q", events)
			assert.Empty(t, eventsWithReason(events, k.reason))
		})

		t.Run(k.name+"/a target that is gone releases at once", func(t *testing.T) {
			obj := k.obj(time.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj).
				WithStatusSubresource(obj).Build()
			rec := record.NewFakeRecorder(50)
			connErr := errBoltRefused
			r, sent := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
				require.NoError(t, err)
			}
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer), "nothing can be left in a deployment that is gone")
			assert.Zero(t, sent())
			assert.Empty(t, eventsWithReason(drainEvents(rec), k.reason))
		})
	}
}

// The alias and the replica released their finalizer at once when Neo4j
// refused the drop, and the replica when it could not read the database's
// type: the alias kept routing, the replica kept its data. They wait as the
// other kinds do (knowledge rule 178).
func TestDeletion_AliasAndReplicaWaitForAStatementNeo4jRefuses(t *testing.T) {
	refused := errors.New("Neo.ClientError.Security.Forbidden: permission denied")
	cases := []struct {
		name   string
		script func(alias *fakeAuthServer, replica *scriptedReplicaServer)
		kind   string
		cause  string
	}{
		{name: "alias drop", kind: "Neo4jDatabaseAlias", cause: "DROP ALIAS failed: " + refused.Error(),
			script: func(a *fakeAuthServer, _ *scriptedReplicaServer) { a.dropErr = refused }},
		{name: "replica drop", kind: "Neo4jReplicaDatabase", cause: "DROP DATABASE failed: " + refused.Error(),
			script: func(_ *fakeAuthServer, s *scriptedReplicaServer) { s.dropErr = refused }},
		{name: "replica type read", kind: "Neo4jReplicaDatabase",
			cause:  "reading the database's type failed: " + refused.Error(),
			script: func(_ *fakeAuthServer, s *scriptedReplicaServer) { s.infoErr = refused }},
	}
	kinds := map[string]deletionKind{}
	for _, k := range deletionKinds() {
		kinds[k.name] = k
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := kinds[tc.kind]
			obj := k.obj(time.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			aliasServer := newFakeAuthServer()
			replicaServer := &scriptedReplicaServer{info: &neo4jclient.DatabaseInfo{Name: "orders-replica",
				Type: neo4jclient.DatabaseTypeReplica}}
			tc.script(aliasServer, replicaServer)
			var r reconcile.Reconciler
			switch tc.kind {
			case "Neo4jDatabaseAlias":
				r = &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (aliasDeletionClient, error) { return aliasServer, nil }}
			default:
				r = &Neo4jReplicaDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget, client.Client) (replicaNeo4jClient, error) { return replicaServer, nil }}
			}

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), repeatPasses) {
				require.NoError(t, err)
			}
			assert.False(t, finalizerReleased(t, c, obj, k.finalizer), "the finalizer is kept while Neo4j refuses")
			events := drainEvents(rec)
			warned := eventsWithReason(events, k.reason)
			require.Len(t, warned, 1, "the wait is announced once: %q", events)
			assert.Contains(t, warned[0], tc.cause)
			assert.Contains(t, warned[0], k.leftBehind)
		})
	}
}
