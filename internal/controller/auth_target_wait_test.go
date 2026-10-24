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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The v1.22.0 re-walk saw UserReady 28 times in 40 minutes of standalone
// restarts, and none in the quiet windows between them. Each restart made the
// user Pending (ClusterNotReady) and the pass that found the standalone back
// moved it into Ready again, which UserReady announced. Nothing about the user
// had changed — it lives in the standalone's system database — and the
// standalone announces its own state. A Ready user, role, binding or auth rule
// now stays Ready while its target is not, with ClusterNotReady=True, and its
// Ready event is announced from the write that moves it into Ready, never from
// the pass's copy (knowledge rule 158).

const targetRestarts = 3

// staleFirstRead serves stale for the next Get of its key, once, as an
// informer cache that has not caught up with the last write would; every
// other read, the status writers' re-reads included, goes to the store.
type staleFirstRead struct {
	client.Client
	stale client.Object
}

func (s *staleFirstRead) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if s.stale != nil && key == client.ObjectKeyFromObject(s.stale) {
		switch stale := s.stale.(type) {
		case *neo4jv1beta1.Neo4jUser:
			if o, ok := obj.(*neo4jv1beta1.Neo4jUser); ok {
				stale.DeepCopyInto(o)
			}
		case *neo4jv1beta1.Neo4jRoleBinding:
			if o, ok := obj.(*neo4jv1beta1.Neo4jRoleBinding); ok {
				stale.DeepCopyInto(o)
			}
		}
		s.stale = nil
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

func TestUserReconcile_ATargetRestartIsNotATransition(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	user := &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", Roles: []string{"reader"}},
	}
	base := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(user.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	c := &staleFirstRead{Client: base}
	rec := record.NewFakeRecorder(100)
	newReconciler := func() *Neo4jUserReconciler {
		return &Neo4jUserReconciler{Client: c, Scheme: base.Scheme(), Recorder: rec,
			newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
	}
	r := newReconciler()
	key := client.ObjectKeyFromObject(user)
	live := func() *neo4jv1beta1.Neo4jUser {
		t.Helper()
		u := &neo4jv1beta1.Neo4jUser{}
		require.NoError(t, base.Get(ctx, key, u))
		return u
	}

	reconcileTimes(t, r, key, 1)
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonUserReady), 1, "the first Ready is announced")
	require.Equal(t, "Ready", live().Status.Phase)

	for range targetRestarts {
		setStandaloneReady(t, base, false)
		reconcileTimes(t, r, key, 1)
		u := live()
		assert.Equal(t, "Ready", u.Status.Phase, "a Ready user stays Ready while its target restarts")
		cond := meta.FindStatusCondition(u.Status.Conditions, ConditionTypeClusterNotReady)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionTrue, cond.Status, "ClusterNotReady says it waits")

		setStandaloneReady(t, base, true)
		reconcileTimes(t, r, key, 1)
		assert.Equal(t, metav1.ConditionFalse,
			meta.FindStatusCondition(live().Status.Conditions, ConditionTypeClusterNotReady).Status)
	}
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserReady),
		"a target restart is not a transition of the user")

	// A pass whose cache still holds the copy from before the first Ready
	// write announces nothing: the event follows the write.
	c.stale = user.DeepCopy()
	reconcileTimes(t, r, key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserReady), "announced from the pass's copy")

	reconcileTimes(t, newReconciler(), key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserReady), "a restarted operator stays quiet")
}

func TestRoleBindingReconcile_ATargetRestartIsNotATransition(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	server.users["bob"] = nil
	rb := &neo4jv1beta1.Neo4jRoleBinding{
		ObjectMeta: repeatedWarningsMeta("bob", Neo4jRoleBindingFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "bob", Roles: []string{"reader"}},
	}
	base := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(rb.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(rb, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	c := &staleFirstRead{Client: base}
	rec := record.NewFakeRecorder(100)
	newReconciler := func() *Neo4jRoleBindingReconciler {
		return &Neo4jRoleBindingReconciler{Client: c, Scheme: base.Scheme(), Recorder: rec,
			newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) { return server, nil }}
	}
	r := newReconciler()
	key := client.ObjectKeyFromObject(rb)
	live := func() *neo4jv1beta1.Neo4jRoleBinding {
		t.Helper()
		b := &neo4jv1beta1.Neo4jRoleBinding{}
		require.NoError(t, base.Get(ctx, key, b))
		return b
	}

	reconcileTimes(t, r, key, 1)
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonBindingCreated), 1)

	for range targetRestarts {
		setStandaloneReady(t, base, false)
		reconcileTimes(t, r, key, 1)
		assert.Equal(t, "Ready", live().Status.Phase, "a Ready binding stays Ready while its target restarts")
		setStandaloneReady(t, base, true)
		reconcileTimes(t, r, key, 1)
	}
	c.stale = rb.DeepCopy()
	reconcileTimes(t, r, key, 1)
	reconcileTimes(t, newReconciler(), key, 1)
	events := drainEvents(rec)
	assert.Empty(t, eventsWithReason(events, EventReasonBindingCreated),
		"a target restart, a stale copy and an operator restart are none of them a new binding")
	assert.Empty(t, eventsWithReason(events, EventReasonBindingUpdated))

	// A change to the granted set is still announced, once.
	b := live()
	b.Spec.Roles = []string{"reader", "editor"}
	require.NoError(t, base.Update(ctx, b))
	reconcileTimes(t, r, key, 2)
	events = drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonBindingUpdated), 1)
	assert.Empty(t, eventsWithReason(events, EventReasonBindingCreated))
}

// The role's Ready path runs against Neo4j end to end; its two halves are
// driven here: the target wait through Reconcile, and the Ready write through
// reportReady.
func TestRoleReconcile_ATargetRestartIsNotATransition(t *testing.T) {
	ctx := context.Background()
	role := &neo4jv1beta1.Neo4jRole{
		ObjectMeta: repeatedWarningsMeta("analyst", Neo4jRoleFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(role.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(role, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
	key := client.ObjectKeyFromObject(role)
	live := func() *neo4jv1beta1.Neo4jRole {
		t.Helper()
		got := &neo4jv1beta1.Neo4jRole{}
		require.NoError(t, c.Get(ctx, key, got))
		return got
	}

	r.reportReady(ctx, live(), "analyst", `role "analyst" is in sync (2 privileges)`, []string{"a", "b"}, false)
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonRoleReady), 1)

	for range targetRestarts {
		setStandaloneReady(t, c, false)
		reconcileTimes(t, r, key, 1)
		assert.Equal(t, "Ready", live().Status.Phase, "a Ready role stays Ready while its target restarts")
		setStandaloneReady(t, c, true)
		// The pass that finds the target back, from a copy as old as the
		// role itself: the write, not the copy, decides.
		r.reportReady(ctx, role.DeepCopy(), "analyst", `role "analyst" is in sync (2 privileges)`, []string{"a", "b"}, false)
	}
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRoleReady))

	// Back from a failure of its own: announced.
	r.setStatus(ctx, role, "Failed", metav1.ConditionFalse, EventReasonRoleSyncFailed, "boom", nil, false)
	r.reportReady(ctx, live(), "analyst", `role "analyst" is in sync (2 privileges)`, []string{"a", "b"}, false)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonRoleReady), 1)
}

func TestAuthRuleReconcile_ATargetRestartKeepsItReady(t *testing.T) {
	ctx := context.Background()
	rule := &neo4jv1beta1.Neo4jAuthRule{
		ObjectMeta: repeatedWarningsMeta("rule", Neo4jAuthRuleFinalizer),
		Spec:       neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "rule", Condition: "true"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(rule.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(rule, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	r := &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(10)}
	key := client.ObjectKeyFromObject(rule)
	require.True(t, r.setStatus(ctx, rule, "Ready", metav1.ConditionTrue, ConditionReasonAuthRuleReady, "in sync", nil, nil))

	setStandaloneReady(t, c, false)
	reconcileTimes(t, r, key, 1)
	got := &neo4jv1beta1.Neo4jAuthRule{}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, "Ready", got.Status.Phase)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, ConditionTypeClusterNotReady))
}

func TestRecordTargetWait(t *testing.T) {
	const gen = int64(2)
	ready := func(observed int64) []metav1.Condition {
		return []metav1.Condition{{Type: ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "UserReady",
			ObservedGeneration: observed, Message: "in sync"}}
	}
	for _, tc := range []struct {
		name      string
		phase     string
		conds     []metav1.Condition
		wantPhase string
	}{
		{"a new object waits", "", nil, "Pending"},
		{"a Ready object stays Ready", "Ready", ready(gen), "Ready"},
		{"a spec change not yet applied waits", "Ready", ready(gen - 1), "Pending"},
		{"a failed object waits", "Failed", nil, "Pending"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			phase, message, conds := tc.phase, "in sync", tc.conds
			recordTargetWait(&conds, &phase, &message, gen, `clusterRef "sa" is not Ready`)
			assert.Equal(t, tc.wantPhase, phase)
			assert.True(t, meta.IsStatusConditionTrue(conds, ConditionTypeClusterNotReady))
			readyCond := meta.FindStatusCondition(conds, ConditionTypeReady)
			require.NotNil(t, readyCond)
			if tc.wantPhase == "Ready" {
				assert.Equal(t, metav1.ConditionTrue, readyCond.Status)
				assert.Equal(t, "in sync", message)
			} else {
				assert.Equal(t, ConditionReasonClusterNotReady, readyCond.Reason)
				assert.Equal(t, `clusterRef "sa" is not Ready`, message)
			}
		})
	}
}
