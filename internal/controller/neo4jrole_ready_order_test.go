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
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// #594: a role applied with the database it grants on announced "Role is
// ready (0 privileges in sync)" on its first pass, its statements skipped
// until the database existed, then PrivilegesApplied on the pass after. RoleReady
// now comes from the pass that applies them, with their count; until then the
// role is Ready with reason PrivilegesWaitForDatabase and says nothing.
func TestRoleReady_IsAnnouncedOnceItsPrivilegesAreApplied(t *testing.T) {
	ctx := context.Background()
	role := &neo4jv1beta1.Neo4jRole{
		ObjectMeta: repeatedWarningsMeta("reader", Neo4jRoleFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "c3_reader"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(role.DeepCopy()).
		WithStatusSubresource(role).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
	key := client.ObjectKeyFromObject(role)
	live := func() *neo4jv1beta1.Neo4jRole {
		t.Helper()
		got := &neo4jv1beta1.Neo4jRole{}
		require.NoError(t, c.Get(ctx, key, got))
		return got
	}
	readyReason := func() string {
		t.Helper()
		cond := meta.FindStatusCondition(live().Status.Conditions, ConditionTypeReady)
		require.NotNil(t, cond)
		return cond.Reason
	}

	// First pass: both statements wait for their database.
	r.reportReady(ctx, live(), "c3_reader", "in sync (0 privileges; 2 skipped)", []string{}, false, 2)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRoleReady), "not before its privileges are applied")
	assert.Equal(t, "Ready", live().Status.Phase)
	assert.Equal(t, ConditionReasonPrivilegesWaitForDatabase, readyReason())

	// The database exists; the pass applies them.
	r.reportReady(ctx, live(), "c3_reader", "in sync (2 privileges)", []string{"a", "b"}, false, 0)
	ready := eventsWithReason(drainEvents(rec), EventReasonRoleReady)
	require.Len(t, ready, 1)
	assert.Contains(t, ready[0], "(2 privileges in sync)")
	assert.Equal(t, ConditionReasonRoleReady, readyReason())

	// A database it grants on goes away and comes back: the role was
	// announced, keeps its reason, and says nothing more here.
	r.reportReady(ctx, live(), "c3_reader", "in sync (1 privileges; 1 skipped)", []string{"a"}, false, 1)
	assert.Equal(t, ConditionReasonRoleReady, readyReason())
	r.reportReady(ctx, live(), "c3_reader", "in sync (2 privileges)", []string{"a", "b"}, false, 0)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRoleReady))
}
