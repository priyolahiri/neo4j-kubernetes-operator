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
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Role-name resolution shared by the Neo4jUser and Neo4jRoleBinding
// controllers (#260). A Neo4jRole CR's metadata.name is Kubernetes-style
// (hyphens allowed) while its effective Neo4j role name (spec.name, or
// metadata.name when spec.name is empty) cannot contain hyphens — so for
// virtually every real custom role the two differ. Users naturally reference
// the CR (the GitOps object) in spec.roles, which previously left them stuck
// in RolesPending forever. These helpers let the CR name resolve to the Neo4j
// role name while keeping plain Neo4j names (built-ins, externally-created
// roles) working unchanged.

// roleCRIndex lists same-namespace Neo4jRole CRs whose spec.clusterRef matches
// clusterRef and returns two views used by role-name resolution:
//
//	effective:       the set of effective Neo4j role names — spec.name, or
//	                 metadata.name when spec.name is empty.
//	metaToEffective: metadata.name -> effective name, only for CRs where the
//	                 two differ (the GitOps CR-name -> Neo4j-name map).
//
// On a List error it returns empty maps, so callers degrade to literal
// (no-resolution) behaviour rather than failing.
func roleCRIndex(ctx context.Context, c client.Client, namespace, targetKey string) (effective map[string]struct{}, metaToEffective map[string]string) {
	effective = map[string]struct{}{}
	metaToEffective = map[string]string{}
	list := &neo4jv1beta1.Neo4jRoleList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return effective, metaToEffective
	}
	for i := range list.Items {
		role := &list.Items[i]
		if targetRefKey(role.Spec.ClusterRef) != targetKey {
			continue
		}
		name := role.Spec.Name
		if name == "" {
			name = role.Name
		}
		effective[name] = struct{}{}
		if name != role.Name {
			metaToEffective[role.Name] = name
		}
	}
	return effective, metaToEffective
}

// roleNameExists reports whether roleName matches a same-namespace Neo4jRole
// CR's effective Neo4j role name (spec.name, or metadata.name when spec.name
// is empty) pointing at the same clusterRef.
func roleNameExists(ctx context.Context, c client.Client, namespace, targetKey, roleName string) bool {
	effective, _ := roleCRIndex(ctx, c, namespace, targetKey)
	_, ok := effective[roleName]
	return ok
}

// roleLister is the one call the role-existence pre-flight needs from Neo4j.
type roleLister interface {
	ListRoles(ctx context.Context) ([]neo4jclient.RoleInfo, error)
}

// missingRoles is the existence pre-flight shared by the Neo4jUser and
// Neo4jRoleBinding diffRoles: the desired roles, sorted, that exist nowhere. A
// role exists when it is built in, already granted to the user, managed by a
// same-namespace Neo4jRole on the same target, or listed by SHOW ROLES — a role
// created in Neo4j directly, by an admin or another tool. Without that last
// check such a role was reported missing on every pass and never granted,
// although spec.roles is documented to take one (#526). SHOW ROLES is read
// only when some role is none of the others, so a steady pass does not run it.
// Role names are case-sensitive in Neo4j, so they are compared exactly.
func missingRoles(
	ctx context.Context, c client.Client, roles roleLister,
	namespace, targetKey string, desired []string, current map[string]struct{},
) ([]string, error) {
	var unknown []string
	for _, role := range desired {
		if validation.IsBuiltInRole(role) {
			continue
		}
		if _, ok := current[role]; ok {
			continue
		}
		if roleNameExists(ctx, c, namespace, targetKey, role) {
			continue
		}
		unknown = append(unknown, role)
	}
	if len(unknown) == 0 {
		return nil, nil
	}
	live, err := roles.ListRoles(ctx)
	if err != nil {
		return nil, fmt.Errorf("list roles: %w", err)
	}
	inNeo4j := make(map[string]struct{}, len(live))
	for _, r := range live {
		inNeo4j[r.Role] = struct{}{}
	}
	var missing []string
	for _, role := range unknown {
		if _, ok := inNeo4j[role]; !ok {
			missing = append(missing, role)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

// resolveRoleNames maps each desired spec.roles entry to the Neo4j role name to
// grant. Literal-first: an entry that is already a built-in or an effective
// Neo4jRole name is kept as-is; only an entry that matches NO real role name but
// DOES match a same-namespace Neo4jRole CR's metadata.name is resolved to that
// CR's effective spec.name. Unknown entries pass through unchanged so diffRoles
// can report them as missing. resolved maps any rewritten input -> output for
// caller diagnostics (empty when nothing was rewritten).
func resolveRoleNames(ctx context.Context, c client.Client, namespace, targetKey string, desired []string) (out []string, resolved map[string]string) {
	resolved = map[string]string{}
	if len(desired) == 0 {
		return desired, resolved
	}
	effective, metaToEffective := roleCRIndex(ctx, c, namespace, targetKey)
	out = make([]string, 0, len(desired))
	for _, d := range desired {
		switch {
		case validation.IsBuiltInRole(d):
			out = append(out, d)
		default:
			if _, isEffective := effective[d]; isEffective {
				// d is already a real Neo4j role name — keep literal.
				out = append(out, d)
			} else if eff, ok := metaToEffective[d]; ok {
				// d is a CR metadata.name; resolve to its Neo4j role name.
				out = append(out, eff)
				resolved[d] = eff
			} else {
				out = append(out, d)
			}
		}
	}
	return out, resolved
}

// resolvedRolesMessage is the RoleNamesResolved condition message, and the
// RolesResolved event text, for a CR-name -> Neo4j-name mapping. fmt prints a
// map sorted by key, so the same mapping always reads the same.
func resolvedRolesMessage(resolved map[string]string) string {
	return fmt.Sprintf("resolved Neo4jRole CR names to Neo4j role names: %v", resolved)
}

// setResolvedRolesCondition records which Neo4jRole CR names in spec.roles were
// resolved to which Neo4j role names: the RoleNamesResolved condition, present
// only while there is a mapping. write says the conditions changed and need
// writing; announce says the mapping is new or different — the change
// RolesResolved reports. RolesResolved was emitted on every pass that resolved
// a name (40 in five minutes on one Neo4jUser, v1.22.0 journey); with the
// mapping in status the event follows its change, and an operator restart
// announces nothing (knowledge rule 136).
func setResolvedRolesCondition(conditions *[]metav1.Condition, generation int64, resolved map[string]string) (write, announce bool) {
	if len(resolved) == 0 {
		return meta.RemoveStatusCondition(conditions, ConditionTypeRoleNamesResolved), false
	}
	msg := resolvedRolesMessage(resolved)
	existing := findCondition(*conditions, ConditionTypeRoleNamesResolved)
	announce = existing == nil || existing.Message != msg
	if !announce && existing.Status == metav1.ConditionTrue &&
		existing.Reason == ConditionReasonRoleCRNamesResolved && existing.ObservedGeneration == generation {
		return false, false
	}
	SetNamedCondition(conditions, ConditionTypeRoleNamesResolved, generation, metav1.ConditionTrue,
		ConditionReasonRoleCRNamesResolved, msg)
	return true, announce
}
