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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A deletion whose drop (or revoke) Neo4j refuses is retried every
// RequeueAfter and on every status change of the target until it succeeds or
// finalizerDeletionGracePeriod ends. Each retry used to emit the same "will
// retry" Warning; the Neo4jDatabase equivalent came about ten times a minute
// during a teardown (v1.22.0 journey). The failure is written to status and
// announced when it starts or its message changes (knowledge rule 145, #549).

type deletionRetryCase struct {
	name      string
	reason    string
	finalizer string
	obj       func(deleted metav1.Time) client.Object
	// reconciler builds the controller with its deletion talking to server.
	reconciler func(c client.Client, rec record.EventRecorder, server *fakeAuthServer) reconcile.Reconciler
	// status reads the persisted phase and message.
	status func(t *testing.T, c client.Client, key client.ObjectKey) (phase, message string)
}

func deletionMeta(name, finalizer string, deleted metav1.Time) metav1.ObjectMeta {
	m := repeatedWarningsMeta(name, finalizer)
	m.DeletionTimestamp = &deleted
	return m
}

func deletionRetryCases() []deletionRetryCase {
	ctx := context.Background()
	return []deletionRetryCase{
		{
			name: "Neo4jUser", reason: EventReasonUserDeletionFailed, finalizer: Neo4jUserFinalizer,
			obj: func(deleted metav1.Time) client.Object {
				return &neo4jv1beta1.Neo4jUser{
					ObjectMeta: deletionMeta("alice", Neo4jUserFinalizer, deleted),
					Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice"},
				}
			},
			reconciler: func(c client.Client, rec record.EventRecorder, server *fakeAuthServer) reconcile.Reconciler {
				return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, string) {
				o := &neo4jv1beta1.Neo4jUser{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Message
			},
		},
		{
			name: "Neo4jRole", reason: EventReasonRoleDeletionFailed, finalizer: Neo4jRoleFinalizer,
			obj: func(deleted metav1.Time) client.Object {
				return &neo4jv1beta1.Neo4jRole{
					ObjectMeta: deletionMeta("analyst", Neo4jRoleFinalizer, deleted),
					Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"},
				}
			},
			reconciler: func(c client.Client, rec record.EventRecorder, server *fakeAuthServer) reconcile.Reconciler {
				return &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (roleDeletionClient, error) { return server, nil }}
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, string) {
				o := &neo4jv1beta1.Neo4jRole{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Message
			},
		},
		{
			name: "Neo4jRoleBinding", reason: EventReasonBindingFailed, finalizer: Neo4jRoleBindingFinalizer,
			obj: func(deleted metav1.Time) client.Object {
				return &neo4jv1beta1.Neo4jRoleBinding{
					ObjectMeta: deletionMeta("bob", Neo4jRoleBindingFinalizer, deleted),
					Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "bob", Roles: []string{"reader"}},
					Status:     neo4jv1beta1.Neo4jRoleBindingStatus{GrantedRoles: []string{"reader"}},
				}
			},
			reconciler: func(c client.Client, rec record.EventRecorder, server *fakeAuthServer) reconcile.Reconciler {
				return &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) { return server, nil }}
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, string) {
				o := &neo4jv1beta1.Neo4jRoleBinding{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Message
			},
		},
		{
			name: "Neo4jAuthRule", reason: EventReasonAuthRuleFailed, finalizer: Neo4jAuthRuleFinalizer,
			obj: func(deleted metav1.Time) client.Object {
				return &neo4jv1beta1.Neo4jAuthRule{
					ObjectMeta: deletionMeta("rule", Neo4jAuthRuleFinalizer, deleted),
					Spec:       neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "rule", Condition: "true"},
				}
			},
			reconciler: func(c client.Client, rec record.EventRecorder, server *fakeAuthServer) reconcile.Reconciler {
				return &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					newDeletionClient: func(ResolvedTarget) (authRuleDeletionClient, error) { return server, nil }}
			},
			status: func(t *testing.T, c client.Client, key client.ObjectKey) (string, string) {
				o := &neo4jv1beta1.Neo4jAuthRule{}
				require.NoError(t, c.Get(ctx, key, o))
				return o.Status.Phase, o.Status.Message
			},
		},
	}
}

// TestAuthDeletion_ARetriedDropIsAnnouncedOnce: three retries, one Warning,
// the drop retried each time and the finalizer kept; a new failure, one more;
// a restarted reconciler, none.
func TestAuthDeletion_ARetriedDropIsAnnouncedOnce(t *testing.T) {
	for _, tc := range deletionRetryCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			obj := tc.obj(metav1.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, authTargetStandalone()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			server := newFakeAuthServer()
			server.dropErr = errors.New("Neo.ClientError.Security.Forbidden: permission denied")
			key := client.ObjectKeyFromObject(obj)

			for _, err := range reconcileTimes(t, tc.reconciler(c, rec, server), key, repeatPasses) {
				require.NoError(t, err)
			}
			failed := eventsWithReason(drainEvents(rec), tc.reason)
			require.Len(t, failed, 1, "announced when the failure starts, not on every retry: %q", failed)
			assert.Contains(t, failed[0], "failed: Neo.ClientError.Security.Forbidden: permission denied")
			assert.Contains(t, failed[0], "deletion waits to remove")
			assert.Equal(t, repeatPasses, server.drops, "the cleanup is still retried on every pass")
			phase, message := tc.status(t, c, key)
			assert.Equal(t, "Failed", phase)
			assert.Contains(t, message, "deletion waits to remove")
			live, ok := obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(ctx, key, live))
			assert.Contains(t, live.GetFinalizers(), tc.finalizer, "the finalizer stays while the cleanup is retried")

			// A different failure is news.
			server.dropErr = errors.New("Neo.ClientError.Statement.ArgumentError: refused")
			reconcileTimes(t, tc.reconciler(c, rec, server), key, repeatPasses)
			require.Len(t, eventsWithReason(drainEvents(rec), tc.reason), 1)

			// An operator restart reads the failure from status and stays quiet.
			reconcileTimes(t, tc.reconciler(c, rec, server), key, 1)
			assert.Empty(t, eventsWithReason(drainEvents(rec), tc.reason))
		})
	}
}

// A composite whose DROP Neo4j keeps refusing was retried for ever, so the CR
// never left Terminating (#526). It now takes the bounded retry every other
// kind whose deletion runs Cypher takes: retried, and announced once, within
// finalizerDeletionGracePeriod; after it, the finalizer is released.
func TestCompositeDeletion_ARefusedDropReleasesTheFinalizerAfterTheGracePeriod(t *testing.T) {
	refused := errors.New("Neo.ClientError.Statement.ArgumentError: the composite database cannot be dropped")
	composite := func(deleted metav1.Time) *neo4jv1beta1.Neo4jCompositeDatabase {
		return &neo4jv1beta1.Neo4jCompositeDatabase{
			ObjectMeta: deletionMeta("cd", Neo4jCompositeDatabaseFinalizer, deleted),
			Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "sales"},
		}
	}
	run := func(t *testing.T, cd *neo4jv1beta1.Neo4jCompositeDatabase) (client.Client, *record.FakeRecorder, *fakeAuthServer) {
		t.Helper()
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(cd, authTargetStandalone()).
			WithStatusSubresource(cd, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
		rec := record.NewFakeRecorder(50)
		server := newFakeAuthServer()
		server.dropErr = refused
		r := &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
			newDeletionClient: func(ResolvedTarget) (compositeDeletionClient, error) { return server, nil }}
		for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(cd), repeatPasses) {
			require.NoError(t, err)
		}
		return c, rec, server
	}

	t.Run("within the grace period: retried, announced once, kept", func(t *testing.T) {
		cd := composite(metav1.Now())
		c, rec, server := run(t, cd)
		assert.Equal(t, repeatPasses, server.drops)
		require.Len(t, eventsWithReason(drainEvents(rec), EventReasonCompositeFailed), 1)
		live := &neo4jv1beta1.Neo4jCompositeDatabase{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cd), live))
		assert.Contains(t, live.Finalizers, Neo4jCompositeDatabaseFinalizer)
		assert.Contains(t, live.Status.Message, "deletion waits to remove composite database")
	})

	t.Run("after the grace period: released", func(t *testing.T) {
		cd := composite(metav1.NewTime(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute)))
		c, rec, server := run(t, cd)
		assert.Equal(t, 1, server.drops, "one more attempt, then the finalizer goes")
		released := eventsWithReason(drainEvents(rec), EventReasonCompositeFailed)
		require.Len(t, released, 1)
		assert.Contains(t, released[0], "Released the finalizer without removing composite database")
		assert.Contains(t, released[0], "DROP COMPOSITE DATABASE `sales` CASCADE ALIASES")
		err := c.Get(context.Background(), client.ObjectKeyFromObject(cd), &neo4jv1beta1.Neo4jCompositeDatabase{})
		assert.True(t, apierrors.IsNotFound(err), "the CR is gone once its finalizer is released: %v", err)
	})
}
