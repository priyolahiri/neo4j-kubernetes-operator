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

package neo4j

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

type rule = neo4jv1beta1.PrivilegeRule

func TestRenderPrivilegeRule(t *testing.T) {
	tests := []struct {
		name string
		r    rule
		want string
	}{
		{"access", rule{Grant: "ACCESS", OnDatabase: "analytics"},
			"GRANT ACCESS ON DATABASE `analytics` TO `r`"},
		{"access all", rule{Grant: "ACCESS", OnDatabase: "*"},
			"GRANT ACCESS ON DATABASE * TO `r`"},
		{"match all nodes", rule{Grant: "MATCH", Properties: []string{"*"}, OnGraph: "analytics", Nodes: []string{"*"}},
			"GRANT MATCH {*} ON GRAPH `analytics` NODES * TO `r`"},
		{"read two properties of a label", rule{Grant: "READ", Properties: []string{"name", "age"}, OnGraph: "g", Nodes: []string{"Person"}},
			"GRANT READ {`name`, `age`} ON GRAPH `g` NODES `Person` TO `r`"},
		{"traverse relationships", rule{Grant: "TRAVERSE", OnGraph: "g", Relationships: []string{"KNOWS"}},
			"GRANT TRAVERSE ON GRAPH `g` RELATIONSHIPS `KNOWS` TO `r`"},
		{"no segment", rule{Grant: "TRAVERSE", OnGraph: "g"},
			"GRANT TRAVERSE ON GRAPH `g` TO `r`"},
		{"elements", rule{Deny: "READ", Properties: []string{"ssn"}, OnGraph: "g", Elements: []string{"*"}},
			"DENY READ {`ssn`} ON GRAPH `g` ELEMENTS * TO `r`"},
		{"write", rule{Grant: "WRITE", OnGraph: "*"},
			"GRANT WRITE ON GRAPH * TO `r`"},
		{"names needing quotes", rule{Grant: "ACCESS", OnDatabase: "movies-latest"},
			"GRANT ACCESS ON DATABASE `movies-latest` TO `r`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RenderPrivilegeRule(tt.r, "r")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	got, err := RenderPrivilegeRule(rule{Grant: "ACCESS", OnDatabase: "a`b"}, "role`x")
	require.NoError(t, err)
	assert.Equal(t, "GRANT ACCESS ON DATABASE `a``b` TO `role``x`", got, "backticks are escaped, never an injection")
}

func TestPrivilegeRuleProblems(t *testing.T) {
	tests := []struct {
		name      string
		r         rule
		wantField string
	}{
		{"neither grant nor deny", rule{OnDatabase: "a"}, "grant"},
		{"both grant and deny", rule{Grant: "ACCESS", Deny: "ACCESS", OnDatabase: "a"}, "deny"},
		{"ACCESS without a database", rule{Grant: "ACCESS"}, "onDatabase"},
		{"ACCESS on a graph", rule{Grant: "ACCESS", OnGraph: "g"}, "onGraph"},
		{"MATCH on a database", rule{Grant: "MATCH", OnDatabase: "a", Properties: []string{"*"}}, "onDatabase"},
		{"both targets", rule{Grant: "WRITE", OnDatabase: "a", OnGraph: "g"}, "onGraph"},
		{"READ without properties", rule{Grant: "READ", OnGraph: "g"}, "properties"},
		{"TRAVERSE with properties", rule{Grant: "TRAVERSE", OnGraph: "g", Properties: []string{"x"}}, "properties"},
		{"WRITE with a segment", rule{Grant: "WRITE", OnGraph: "g", Nodes: []string{"*"}}, "nodes"},
		{"two segments", rule{Grant: "TRAVERSE", OnGraph: "g", Nodes: []string{"A"}, Relationships: []string{"R"}}, "relationships"},
		{"* with others", rule{Grant: "MATCH", OnGraph: "g", Properties: []string{"*", "x"}}, "properties"},
		{"empty label", rule{Grant: "TRAVERSE", OnGraph: "g", Nodes: []string{""}}, "nodes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := PrivilegeRuleProblems(tt.r)
			require.NotEmpty(t, problems)
			var fields []string
			for _, p := range problems {
				fields = append(fields, p.Field)
			}
			assert.Contains(t, fields, tt.wantField)
			_, err := RenderPrivilegeRule(tt.r, "r")
			assert.Error(t, err, "a rule with problems never renders")
		})
	}
	assert.Empty(t, PrivilegeRuleProblems(rule{Grant: "MATCH", OnGraph: "g", Properties: []string{"*"}, Nodes: []string{"*"}}))
}

func TestPrivilegeRuleTarget(t *testing.T) {
	assert.Equal(t, "a", PrivilegeRuleTarget(rule{Grant: "ACCESS", OnDatabase: "a"}))
	assert.Equal(t, "g", PrivilegeRuleTarget(rule{Grant: "WRITE", OnGraph: "g"}))
	assert.Equal(t, "", PrivilegeRuleTarget(rule{Grant: "WRITE", OnGraph: "*"}), "* names nothing that could be missing")
	assert.Equal(t, "onGraph", PrivilegeRuleTargetField(rule{Grant: "WRITE", OnGraph: "g"}))
	assert.True(t, strings.HasPrefix(PrivilegeRuleTargetField(rule{Grant: "ACCESS", OnDatabase: "a"}), "onDatabase"))
}
