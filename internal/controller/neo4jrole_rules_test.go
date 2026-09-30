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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Each rule renders to the same statement, modulo quoting, as one Neo4j was
// RECORDED accepting and storing (testdata/privilege_rendering.json, 5.26.31
// and 2026.08.1) — and, through the normaliser, a rule's desired rows are
// exactly what the server stores for that statement. The renderer backticks
// every name; that the server accepts the backticked form is checked live.
func TestPrivilegeRulesRenderToRecordedStatements(t *testing.T) {
	f := loadRenderingFixture(t)
	unquoted := func(stmt string) string {
		return neo4jclient.CanonicalisePrivilegeStatement(strings.ReplaceAll(stmt, "`", ""))
	}
	recorded := map[string][]string{}
	for _, c := range f.Cases {
		recorded[unquoted(c.Statement)] = c.Stored
	}
	// A prober that answers by the recorded statement, matched modulo quoting.
	prober := proberFunc(func(role, stmt string) ([]string, error) {
		probe, err := neo4jclient.ReplacePrivilegeGrantee(stmt, "probe")
		require.NoError(t, err)
		stored, ok := recorded[unquoted(probe)]
		require.True(t, ok, "no recorded statement matches %q", stmt)
		out := make([]string, 0, len(stored))
		for _, row := range stored {
			r, err := neo4jclient.ReplacePrivilegeGrantee(row, role)
			require.NoError(t, err)
			out = append(out, r)
		}
		return out, nil
	})

	for _, r := range []neo4jv1beta1.PrivilegeRule{
		{Grant: "MATCH", Properties: []string{"*"}, OnGraph: "sales", Nodes: []string{"*"}},
		{Grant: "READ", Properties: []string{"name", "age"}, OnGraph: "sales", Nodes: []string{"Person"}},
		{Grant: "MATCH", Properties: []string{"*"}, OnGraph: "sales", Elements: []string{"*"}},
		{Grant: "MATCH", Properties: []string{"*"}, OnGraph: "sales"},
		{Grant: "READ", Properties: []string{"name"}, OnGraph: "sales", Relationships: []string{"*"}},
	} {
		stmt, err := neo4jclient.RenderPrivilegeRule(r, "probe")
		require.NoError(t, err)
		stored, ok := recorded[unquoted(stmt)]
		require.True(t, ok, "rendered %q matches no recorded server-accepted statement", stmt)

		desired, _ := (&Neo4jRoleReconciler{}).normaliseDesired(context.Background(),
			prober, "scope", "probe", []string{stmt}, nil)
		assert.ElementsMatch(t, storedFor(t, stored, "probe"), desired, stmt)
	}
}

func ruleRole(rules ...neo4jv1beta1.PrivilegeRule) *neo4jv1beta1.Neo4jRole {
	return &neo4jv1beta1.Neo4jRole{
		ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "ns"},
		Spec: neo4jv1beta1.Neo4jRoleSpec{
			ClusterRef:     "c",
			Name:           "salesReader",
			Privileges:     []string{"GRANT ACCESS ON DATABASE legacy TO salesReader"},
			PrivilegeRules: rules,
		},
	}
}

func TestRolePrivilegeStatementsCombinesBothForms(t *testing.T) {
	role := ruleRole(
		neo4jv1beta1.PrivilegeRule{Grant: "ACCESS", OnDatabase: "sales"},
		neo4jv1beta1.PrivilegeRule{Grant: "MATCH" /* invalid: no graph, no properties */},
	)
	assert.Equal(t, []string{
		"GRANT ACCESS ON DATABASE legacy TO salesReader",
		"GRANT ACCESS ON DATABASE `sales` TO `salesReader`",
	}, rolePrivilegeStatements(role), "spec.privileges first, then each valid rule; an invalid rule is skipped")
	assert.Equal(t, []string{"legacy", "sales"}, roleDatabaseNames(role))
}

func TestRuleFieldsNaming(t *testing.T) {
	role := ruleRole(
		neo4jv1beta1.PrivilegeRule{Grant: "ACCESS", OnDatabase: "sales"},
		neo4jv1beta1.PrivilegeRule{Grant: "WRITE", OnGraph: "Sales"},
	)
	assert.Equal(t, []string{"spec.privilegeRules[0].onDatabase", "spec.privilegeRules[1].onGraph"},
		ruleFieldsNaming(role, "sales"), "case-insensitive, as Neo4j database names are")
	assert.Empty(t, ruleFieldsNaming(role, "legacy"), "a spec.privileges name has no field to point at")
}

func TestRolesNamingDatabase(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	byRule := ruleRole(neo4jv1beta1.PrivilegeRule{Grant: "ACCESS", OnDatabase: "sales"})
	byString := ruleRole()
	byString.Name = "legacy-reader"
	byString.Spec.Privileges = []string{"GRANT ACCESS ON DATABASE SALES TO salesReader"}
	otherCluster := ruleRole(neo4jv1beta1.PrivilegeRule{Grant: "ACCESS", OnDatabase: "sales"})
	otherCluster.Name = "elsewhere"
	otherCluster.Spec.ClusterRef = "other"
	unrelated := ruleRole(neo4jv1beta1.PrivilegeRule{Grant: "ACCESS", OnDatabase: "orders"})
	unrelated.Name = "orders-reader"

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(byRule, byString, otherCluster, unrelated).Build()
	r := &Neo4jRoleReconciler{Client: c}
	db := &neo4jv1beta1.Neo4jDatabase{ObjectMeta: metav1.ObjectMeta{Name: "sales-db", Namespace: "ns"},
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "c", Name: "sales"}}

	var names []string
	for _, req := range r.rolesNamingDatabase(context.Background(), db) {
		names = append(names, req.Name)
	}
	assert.ElementsMatch(t, []string{"reader", "legacy-reader"}, names)
}
