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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// #573: a Failed Neo4jUser announced UserSyncFailed again after every restart
// of its standalone. The restart made it Pending (ClusterNotReady), and the
// pass that found the standalone back met the same failure, a transition back
// into Failed, which the Warning announced. A binding waiting for its user did
// the same with UserNotFound. An object whose status records an outcome of its
// own for its generation keeps it while the target restarts (knowledge rule
// 189), so the same cause is announced once.

func TestUserReconcile_AFailureIsAnnouncedOnceThroughTargetRestarts(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	server.users["alice"] = nil
	server.alterErr = errors.New("Neo.ClientError.Database.DatabaseNotFound: Database `nodb` does not exist")
	user := &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", HomeDatabase: "nodb"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(user.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	newReconciler := func() *Neo4jUserReconciler {
		return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
			newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
	}
	r := newReconciler()
	key := client.ObjectKeyFromObject(user)
	live := func() *neo4jv1beta1.Neo4jUser {
		t.Helper()
		u := &neo4jv1beta1.Neo4jUser{}
		require.NoError(t, c.Get(ctx, key, u))
		return u
	}

	reconcileTimes(t, r, key, 1)
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonUserSyncFailed), 1, "the failure is announced")
	require.Equal(t, "Failed", live().Status.Phase)
	failedWith := live().Status.Message

	for range targetRestarts {
		setStandaloneReady(t, c, false)
		reconcileTimes(t, r, key, 1)
		u := live()
		assert.Equal(t, "Failed", u.Status.Phase, "the failure is the user's own; the restart does not change it")
		assert.Equal(t, failedWith, u.Status.Message)
		assert.True(t, meta.IsStatusConditionTrue(u.Status.Conditions, ConditionTypeClusterNotReady), "the wait is said")

		setStandaloneReady(t, c, true)
		reconcileTimes(t, r, key, 1)
		assert.Equal(t, "Failed", live().Status.Phase)
	}
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserSyncFailed),
		"the same failure after a target restart is not announced again")
	reconcileTimes(t, newReconciler(), key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserSyncFailed), "nor after an operator restart")

	// A different cause is announced.
	server.alterErr = errors.New("Neo.ClientError.Security.Forbidden: permission denied")
	reconcileTimes(t, r, key, 1)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonUserSyncFailed), 1)

	// A spec change is new work: it waits on a target that is not Ready.
	u := live()
	require.NoError(t, patchObject(ctx, c, u, func() {
		u.Spec.HomeDatabase = "other"
		u.Generation++ // the fake API server does not bump it
	}))
	require.Equal(t, u.Generation, live().Generation)
	setStandaloneReady(t, c, false)
	reconcileTimes(t, r, key, 1)
	assert.Equal(t, "Pending", live().Status.Phase)
}

func TestRoleBindingReconcile_AMissingUserIsAnnouncedOnceThroughTargetRestarts(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer() // no user "ghost"
	rb := &neo4jv1beta1.Neo4jRoleBinding{
		ObjectMeta: repeatedWarningsMeta("ghost", Neo4jRoleBindingFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "ghost", Roles: []string{"reader"}},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(rb.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(rb, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) { return server, nil }}
	key := client.ObjectKeyFromObject(rb)

	reconcileTimes(t, r, key, 1)
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonUserNotFound), 1)

	for range targetRestarts {
		setStandaloneReady(t, c, false)
		reconcileTimes(t, r, key, 1)
		got := &neo4jv1beta1.Neo4jRoleBinding{}
		require.NoError(t, c.Get(ctx, key, got))
		ready := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeReady)
		require.NotNil(t, ready)
		assert.Equal(t, EventReasonUserNotFound, ready.Reason, "the binding still waits for its user")
		setStandaloneReady(t, c, true)
		reconcileTimes(t, r, key, 1)
	}
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonUserNotFound),
		"the same missing user after a target restart is not announced again")
}

func TestSettledAtGeneration(t *testing.T) {
	const gen = int64(3)
	cond := func(status metav1.ConditionStatus, reason string, observed int64) []metav1.Condition {
		return []metav1.Condition{{Type: ConditionTypeReady, Status: status, Reason: reason, ObservedGeneration: observed}}
	}
	for _, tc := range []struct {
		name  string
		phase string
		conds []metav1.Condition
		want  bool
	}{
		{"new", "", nil, false},
		{"Ready at its generation", "Ready", cond(metav1.ConditionTrue, ConditionReasonUserReady, gen), true},
		{"Ready at an earlier generation", "Ready", cond(metav1.ConditionTrue, ConditionReasonUserReady, gen-1), false},
		{"Failed at its generation", "Failed", cond(metav1.ConditionFalse, EventReasonUserSyncFailed, gen), true},
		{"Failed after a lasting outage", "Failed", cond(metav1.ConditionFalse, EventReasonConnectionFailed, gen), true},
		{"Failed at an earlier generation", "Failed", cond(metav1.ConditionFalse, EventReasonUserSyncFailed, gen-1), false},
		{"Failed with no condition", "Failed", nil, false},
		{"waiting for its user", "Pending", cond(metav1.ConditionFalse, EventReasonUserNotFound, gen), true},
		{"waiting for roles", "Pending", cond(metav1.ConditionFalse, ConditionReasonRolesPending, gen), true},
		{"waiting for the target", "Pending", cond(metav1.ConditionFalse, ConditionReasonClusterNotReady, gen), false},
		{"waiting for an unreachable target", "Pending", cond(metav1.ConditionFalse, ConditionReasonClusterUnreachable, gen), false},
		{"its target was gone", "Pending", cond(metav1.ConditionFalse, EventReasonClusterNotFound, gen), false},
		{"its password Secret was missing", "Pending", cond(metav1.ConditionFalse, ConditionReasonSecretPending, gen), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, settledAtGeneration(tc.phase, tc.conds, gen))
		})
	}
}
