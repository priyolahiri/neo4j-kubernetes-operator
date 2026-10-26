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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The round-8 walk: in a bulk delete, DROP ROLE hit
// Neo.TransientError.Transaction.DeadlockDetected and raised a
// RoleDeletionFailed Warning before the retry succeeded. A transient Neo4j
// asks to be retried is retried quietly — logged at info, nothing on status,
// no event — and becomes the deletion's Warning only if it lasts past the
// grace period (knowledge rules 164, 178).

// errDeadlock is Neo4j's deadlock error, wrapped as the client wraps it.
func errDeadlock(statement string) error {
	return fmt.Errorf("failed to run %s: %w", statement, &driver.Neo4jError{
		Code: "Neo.TransientError.Transaction.DeadlockDetected",
		Msg:  "ForsetiClient[transactionId=12] can't acquire ExclusiveLock{owner=ForsetiClient[transactionId=11]}"})
}

func TestDeletion_ATransientNeo4jErrorIsRetriedQuietly(t *testing.T) {
	t.Cleanup(func() { errScriptedDrop = nil })
	for _, k := range deletionKinds() {
		t.Run(k.name, func(t *testing.T) {
			obj := k.obj(time.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, sent := k.setup(c, rec, &connErr)
			key := client.ObjectKeyFromObject(obj)
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))

			errScriptedDrop = errDeadlock("the cleanup")
			for range repeatPasses {
				res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err, "a transient is not a reconcile error")
				assert.Positive(t, res.RequeueAfter, "it is retried")
			}
			assert.False(t, finalizerReleased(t, c, obj, k.finalizer))
			assert.Equal(t, repeatPasses, sent(), "the statement is retried on every pass")
			assert.Empty(t, eventsWithReason(drainEvents(rec), k.reason), "no Warning for a transient")
			assert.NotContains(t, statusMessage(t, c, obj), "deletion waits", "nothing on status")
			assert.Empty(t, sink.errors())

			errScriptedDrop = nil
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer), "the retry succeeds")
			assert.Empty(t, eventsWithReason(drainEvents(rec), k.reason))
		})
	}
}

// A transient that lasts past the grace period is the wait's Warning, as any
// cause the deletion could not get past.
func TestDeletion_ATransientThatOutlastsTheGraceIsNamed(t *testing.T) {
	t.Cleanup(func() { errScriptedDrop = nil })
	for _, k := range deletionKinds() {
		t.Run(k.name, func(t *testing.T) {
			obj := k.obj(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, _ := k.setup(c, rec, &connErr)

			errScriptedDrop = errDeadlock("the cleanup")
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
			require.NoError(t, err)
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer))
			warned := eventsWithReason(drainEvents(rec), k.reason)
			require.Len(t, warned, 1)
			assert.True(t, strings.HasPrefix(warned[0], "Warning "))
			assert.Contains(t, warned[0], "DeadlockDetected")
			assert.Contains(t, warned[0], k.byHand)
		})
	}
}

// The same holds outside deletion: a user, role, binding or auth rule whose
// GRANT, CREATE or ALTER hit a transient went Failed with a Warning, and the
// error went to controller-runtime (an ERROR log, error backoff), before the
// retry succeeded. It is retried quietly while it lasts no longer than such
// errors do, and is the object's failure past that (knowledge rule 164).
func TestUserReconcile_ATransientOnAlterIsRetriedQuietly(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	server.users["alice"] = nil
	user := &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", HomeDatabase: "orders"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(user, authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(50)
	r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
	key := client.ObjectKeyFromObject(user)

	server.alterErr = errDeadlock("ALTER USER")
	for range repeatPasses {
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err, "a transient is not a reconcile error")
		assert.Equal(t, transientRetry, res.RequeueAfter)
	}
	events := drainEvents(rec)
	assert.Empty(t, eventsWithReason(events, EventReasonUserSyncFailed), "%q", events)
	live := &neo4jv1beta1.Neo4jUser{}
	require.NoError(t, c.Get(ctx, key, live))
	assert.NotEqual(t, "Failed", live.Status.Phase)

	server.alterErr = nil
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, live))
	assert.Equal(t, "Ready", live.Status.Phase)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserSyncFailed))
}

func TestAuthFail_ATransientIsQuietUntilItLasts(t *testing.T) {
	ctx := context.Background()
	type failer struct {
		name   string
		obj    client.Object
		reason string
		fail   func(c client.Client, rec record.EventRecorder, s *transientStreaks) (ctrl.Result, error)
	}
	user := &neo4jv1beta1.Neo4jUser{ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice"}}
	role := &neo4jv1beta1.Neo4jRole{ObjectMeta: repeatedWarningsMeta("analyst", Neo4jRoleFinalizer),
		Spec: neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"}}
	rb := &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: repeatedWarningsMeta("bob", Neo4jRoleBindingFinalizer),
		Spec: neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "bob", Roles: []string{"reader"}}}
	rule := &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: repeatedWarningsMeta("rule", Neo4jAuthRuleFinalizer),
		Spec: neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "rule", Condition: "true"}}
	deadlock := errDeadlock("GRANT ROLE")
	for _, f := range []failer{
		{"Neo4jUser", user, EventReasonUserSyncFailed, func(c client.Client, rec record.EventRecorder, s *transientStreaks) (ctrl.Result, error) {
			r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			copyStreaks(&r.transients, s)
			return r.fail(ctx, user, "grant failed", deadlock, time.Minute)
		}},
		{"Neo4jRole", role, EventReasonRoleSyncFailed, func(c client.Client, rec record.EventRecorder, s *transientStreaks) (ctrl.Result, error) {
			r := &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			copyStreaks(&r.transients, s)
			return r.fail(ctx, role, "create role failed", deadlock, time.Minute)
		}},
		{"Neo4jRoleBinding", rb, EventReasonBindingFailed, func(c client.Client, rec record.EventRecorder, s *transientStreaks) (ctrl.Result, error) {
			r := &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			copyStreaks(&r.transients, s)
			return r.fail(ctx, rb, "grant failed", deadlock, time.Minute)
		}},
		{"Neo4jAuthRule", rule, EventReasonAuthRuleFailed, func(c client.Client, rec record.EventRecorder, s *transientStreaks) (ctrl.Result, error) {
			r := &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
			copyStreaks(&r.transients, s)
			return r.fail(ctx, rule, "create rule failed", deadlock, time.Minute)
		}},
	} {
		t.Run(f.name+"/quiet", func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(f.obj).
				WithStatusSubresource(f.obj).Build()
			rec := record.NewFakeRecorder(10)
			res, err := f.fail(c, rec, &transientStreaks{})
			require.NoError(t, err)
			assert.Equal(t, transientRetry, res.RequeueAfter)
			assert.Empty(t, drainEvents(rec))
			assert.NotContains(t, statusMessage(t, c, f.obj), "DeadlockDetected", "nothing on status")
		})
		t.Run(f.name+"/lasting", func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(f.obj).
				WithStatusSubresource(f.obj).Build()
			rec := record.NewFakeRecorder(10)
			lasting := &transientStreaks{}
			lasting.m.Store(client.ObjectKeyFromObject(f.obj).String(), transientStreak{
				first: time.Now().Add(-targetUnreachableGrace - time.Minute), last: time.Now().Add(-10 * time.Second)})
			_, _ = f.fail(c, rec, lasting)
			assert.Len(t, eventsWithReason(drainEvents(rec), f.reason), 1, "a transient that lasts is the object's failure")
			assert.Contains(t, statusMessage(t, c, f.obj), "DeadlockDetected")
		})
	}
}

// copyStreaks seeds a reconciler's streaks from s.
func copyStreaks(dst, src *transientStreaks) {
	src.m.Range(func(k, v any) bool {
		dst.m.Store(k, v)
		return true
	})
}
