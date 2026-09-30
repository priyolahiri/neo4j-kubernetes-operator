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
	"fmt"
	"strings"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// rolePrivilegeStatements is every statement a role asks for: spec.privileges
// as written, then each spec.privilegeRules entry rendered to Cypher. From here
// on a rendered rule is an ordinary statement — normalised, diffed, applied,
// and learned exactly like a hand-written one — so every path that used to read
// spec.privileges reads this instead. A rule that does not render is skipped:
// the validator refuses the CR before any of this runs.
func rolePrivilegeStatements(role *neo4jv1beta1.Neo4jRole) []string {
	out := append([]string(nil), role.Spec.Privileges...)
	roleName := effectiveRoleName(role)
	for _, rule := range role.Spec.PrivilegeRules {
		if stmt, err := neo4jclient.RenderPrivilegeRule(rule, roleName); err == nil {
			out = append(out, stmt)
		}
	}
	return out
}

// ruleFieldsNaming returns the field paths of the privilege rules whose
// database or graph is name, so a report can point at the exact field rather
// than at a substring of Cypher — which is what the structured form is for.
func ruleFieldsNaming(role *neo4jv1beta1.Neo4jRole, name string) []string {
	var out []string
	for i, rule := range role.Spec.PrivilegeRules {
		if t := neo4jclient.PrivilegeRuleTarget(rule); t != "" && strings.EqualFold(t, name) {
			out = append(out, fmt.Sprintf("spec.privilegeRules[%d].%s", i, neo4jclient.PrivilegeRuleTargetField(rule)))
		}
	}
	return out
}

// roleDatabaseNames is every database or graph a role names, from both forms.
func roleDatabaseNames(role *neo4jv1beta1.Neo4jRole) []string {
	return privilegeDatabaseNames(rolePrivilegeStatements(role))
}
