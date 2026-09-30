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
	"fmt"
	"strings"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Structured privileges (Neo4jRole.spec.privilegeRules) — design:
// docs/design/privilege-database-references.md §4.
//
// Render, never parse: a rule becomes one Cypher statement, and from there it
// is an ordinary spec statement — normalised to Neo4j's stored form and diffed
// like a hand-written one (neo4jrole_normalise.go / neo4jrole_learn.go). So the
// renderer's formatting does not matter, only that Neo4j accepts it, and the
// structured form never has to be complete: spec.privileges is the escape
// hatch for everything it does not model.

// RuleProblem is one thing wrong with a rule: the offending field (JSON name,
// relative to the rule) and why.
type RuleProblem struct {
	Field  string
	Detail string
}

// PrivilegeRuleProblems returns what is wrong with a rule; none means it
// renders. The validator reports these by field path; the renderer refuses
// on any.
func PrivilegeRuleProblems(r neo4jv1beta1.PrivilegeRule) []RuleProblem {
	var out []RuleProblem
	add := func(field, format string, args ...any) {
		out = append(out, RuleProblem{Field: field, Detail: fmt.Sprintf(format, args...)})
	}

	action := r.Grant
	switch {
	case r.Grant != "" && r.Deny != "":
		add("deny", "set exactly one of grant or deny")
	case r.Grant == "" && r.Deny == "":
		add("grant", "set exactly one of grant or deny")
	case r.Deny != "":
		action = r.Deny
	}

	// The wrong field is a more useful message than the missing one.
	switch {
	case r.OnDatabase != "" && r.OnGraph != "":
		add("onGraph", "set exactly one of onDatabase or onGraph")
	case action == "ACCESS" && r.OnGraph != "":
		add("onGraph", "ACCESS applies to a database: use onDatabase, not onGraph")
	case action != "ACCESS" && action != "" && r.OnDatabase != "":
		add("onDatabase", "%s applies to a graph: use onGraph, not onDatabase", action)
	case action == "ACCESS" && r.OnDatabase == "":
		add("onDatabase", "ACCESS is a database privilege: set onDatabase (a database name, or \"*\")")
	case action != "ACCESS" && action != "" && r.OnGraph == "":
		add("onGraph", "%s is a graph privilege: set onGraph (a database name, or \"*\")", action)
	}

	segments := 0
	for _, seg := range []struct {
		name   string
		values []string
	}{{"nodes", r.Nodes}, {"relationships", r.Relationships}, {"elements", r.Elements}} {
		if len(seg.values) == 0 {
			continue
		}
		segments++
		if p := listProblem(seg.values); p != "" {
			add(seg.name, "%s", p)
		}
	}
	switch action {
	case "ACCESS", "WRITE":
		if len(r.Properties) > 0 {
			add("properties", "%s takes no properties", action)
		}
		if segments > 0 {
			add("nodes", "%s takes no nodes, relationships or elements", action)
		}
	case "TRAVERSE":
		if len(r.Properties) > 0 {
			add("properties", "TRAVERSE takes no properties; use READ or MATCH")
		}
	case "READ", "MATCH":
		if len(r.Properties) == 0 {
			add("properties", "%s needs properties: the property names, or [\"*\"] for all", action)
		} else if p := listProblem(r.Properties); p != "" {
			add("properties", "%s", p)
		}
	}
	if segments > 1 {
		add("relationships", "set at most one of nodes, relationships and elements — one rule per segment")
	}
	return out
}

// listProblem checks a name list: no empty names, and "*" only on its own.
func listProblem(values []string) string {
	for _, v := range values {
		if strings.TrimSpace(v) == "" {
			return "names must not be empty"
		}
		if v == "*" && len(values) > 1 {
			return "\"*\" means all, so it must be the only entry"
		}
	}
	return ""
}

// RenderPrivilegeRule renders a rule as a GRANT or DENY statement granted TO
// role. It refuses a rule PrivilegeRuleProblems rejects.
func RenderPrivilegeRule(r neo4jv1beta1.PrivilegeRule, role string) (string, error) {
	if problems := PrivilegeRuleProblems(r); len(problems) > 0 {
		return "", fmt.Errorf("invalid privilege rule: %s: %s", problems[0].Field, problems[0].Detail)
	}
	verb, action := "GRANT", r.Grant
	if r.Deny != "" {
		verb, action = "DENY", r.Deny
	}

	var b strings.Builder
	b.WriteString(verb + " " + action)
	if action == "READ" || action == "MATCH" {
		b.WriteString(" {" + nameList(r.Properties) + "}")
	}
	if action == "ACCESS" {
		b.WriteString(" ON DATABASE " + nameOrAll(r.OnDatabase))
	} else {
		b.WriteString(" ON GRAPH " + nameOrAll(r.OnGraph))
	}
	switch {
	case len(r.Nodes) > 0:
		b.WriteString(" NODES " + nameList(r.Nodes))
	case len(r.Relationships) > 0:
		b.WriteString(" RELATIONSHIPS " + nameList(r.Relationships))
	case len(r.Elements) > 0:
		b.WriteString(" ELEMENTS " + nameList(r.Elements))
	}
	b.WriteString(" TO `" + escapeBackticks(role) + "`")
	return b.String(), nil
}

// PrivilegeRuleTarget is the database or graph a rule names, "" for "*".
func PrivilegeRuleTarget(r neo4jv1beta1.PrivilegeRule) string {
	t := r.OnDatabase
	if t == "" {
		t = r.OnGraph
	}
	if t == "*" {
		return ""
	}
	return t
}

// PrivilegeRuleTargetField is the JSON field a rule's target lives in.
func PrivilegeRuleTargetField(r neo4jv1beta1.PrivilegeRule) string {
	if r.OnDatabase != "" {
		return "onDatabase"
	}
	return "onGraph"
}

func nameOrAll(name string) string {
	if name == "*" {
		return "*"
	}
	return "`" + escapeBackticks(name) + "`"
}

func nameList(names []string) string {
	if len(names) == 1 && names[0] == "*" {
		return "*"
	}
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, "`"+escapeBackticks(n)+"`")
	}
	return strings.Join(quoted, ", ")
}
