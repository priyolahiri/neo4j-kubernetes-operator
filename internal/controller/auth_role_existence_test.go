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
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// spec.roles of a Neo4jUser or Neo4jRoleBinding takes a role created in Neo4j
// directly — by an admin, or by a tool other than the operator — as well as a
// built-in or a Neo4jRole (the API reference says so). The existence check
// knew only the first two and the CRs, so such a role was reported missing on
// every pass and never granted (#526).

func TestMissingRoles(t *testing.T) {
	ctx := context.Background()
	managed := &neo4jv1beta1.Neo4jRole{
		ObjectMeta: repeatedWarningsMeta("analyst-role", Neo4jRoleFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(managed).Build()
	server := newFakeAuthServer()
	server.roles["auditor"] = true // created in Neo4j, no Neo4jRole
	server.roles["Auditor2"] = true

	missing, err := missingRoles(ctx, c, server, repeatedWarningsNS, targetRefKey("sa"),
		[]string{"reader", "granted", "analyst", "auditor", "ghost", "auditor2"},
		map[string]struct{}{"granted": {}})
	require.NoError(t, err)
	assert.Equal(t, []string{"auditor2", "ghost"}, missing,
		"a role in SHOW ROLES exists; role names are case-sensitive, so auditor2 is not Auditor2")
	assert.Equal(t, 1, server.listRoles)

	// A pass whose roles all exist elsewhere does not read SHOW ROLES.
	missing, err = missingRoles(ctx, c, server, repeatedWarningsNS, targetRefKey("sa"),
		[]string{"reader", "granted", "analyst"}, map[string]struct{}{"granted": {}})
	require.NoError(t, err)
	assert.Empty(t, missing)
	assert.Equal(t, 1, server.listRoles, "a steady pass sends no SHOW ROLES")
}

func TestUserReconcile_GrantsARoleCreatedInNeo4j(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	server.roles["auditor"] = true
	user := &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", Roles: []string{"auditor", "ghost"}},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(user, authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(50)
	r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}

	reconcileTimes(t, r, client.ObjectKeyFromObject(user), 1)

	assert.Equal(t, []string{"auditor->alice"}, server.granted, "the role created in Neo4j is granted")
	pending := eventsWithReason(drainEvents(rec), EventReasonRolePending)
	require.Len(t, pending, 1)
	assert.Contains(t, pending[0], "waiting for roles to exist: [ghost]", "only the role that exists nowhere waits")

	// Once it exists in Neo4j, the next pass grants it and the user is Ready.
	server.roles["ghost"] = true
	reconcileTimes(t, r, client.ObjectKeyFromObject(user), 1)
	live := &neo4jv1beta1.Neo4jUser{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(user), live))
	assert.Equal(t, "Ready", live.Status.Phase)
	assert.ElementsMatch(t, []string{"auditor", "ghost"}, live.Status.CurrentRoles)
}

func TestRoleBindingReconcile_GrantsARoleCreatedInNeo4j(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	server.users["bob"] = nil // provisioned by SSO
	server.roles["auditor"] = true
	rb := &neo4jv1beta1.Neo4jRoleBinding{
		ObjectMeta: repeatedWarningsMeta("bob", Neo4jRoleBindingFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "bob", Roles: []string{"auditor"}},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(rb, authTargetStandalone()).
		WithStatusSubresource(rb, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(50)
	r := &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) { return server, nil }}

	reconcileTimes(t, r, client.ObjectKeyFromObject(rb), 1)

	assert.Equal(t, []string{"auditor->bob"}, server.granted, "the role created in Neo4j is granted")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRolePending))
	live := &neo4jv1beta1.Neo4jRoleBinding{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(rb), live))
	assert.Equal(t, "Ready", live.Status.Phase)
	assert.Equal(t, []string{"auditor"}, live.Status.GrantedRoles)
}
