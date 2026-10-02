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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func role(name, cluster string, privileges ...string) neo4jv1beta1.Neo4jRole {
	return neo4jv1beta1.Neo4jRole{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod"},
		Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: cluster, Privileges: privileges},
	}
}

func names(roles []*neo4jv1beta1.Neo4jRole) []string {
	out := []string{}
	for _, r := range roles {
		out = append(out, r.Name)
	}
	return out
}

// When a database is dropped, Neo4j removes the privileges that named it, and
// a role whose spec still grants on it asks for something that cannot exist.
// These are the roles the operator must warn about at the moment of the drop.
func TestRolesGrantingOn(t *testing.T) {
	tests := []struct {
		name  string
		roles []neo4jv1beta1.Neo4jRole
		want  []string
	}{
		{
			name:  "a role granting on the dropped database is reported",
			roles: []neo4jv1beta1.Neo4jRole{role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE sales TO reader")},
			want:  []string{"reader"},
		},
		{
			name:  "a GRAPH-scoped privilege counts too",
			roles: []neo4jv1beta1.Neo4jRole{role("reader", "prod-cluster", "GRANT MATCH {*} ON GRAPH sales NODES * TO reader")},
			want:  []string{"reader"},
		},
		{
			name: "a role naming some other database is not",
			roles: []neo4jv1beta1.Neo4jRole{
				role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE orders TO reader"),
			},
			want: []string{},
		},
		{
			// A role on another cluster naming a database of the same name is
			// entirely unrelated: that cluster's database still exists.
			name: "the same database name on a different cluster is not reported",
			roles: []neo4jv1beta1.Neo4jRole{
				role("reader", "dr-cluster", "GRANT ACCESS ON DATABASE sales TO reader"),
			},
			want: []string{},
		},
		{
			// Neo4j lowercases database names, so `ON DATABASE Sales` targets
			// `sales`. A case-sensitive match would stay silent — the one
			// outcome a warning cannot afford.
			name:  "matching is case-insensitive",
			roles: []neo4jv1beta1.Neo4jRole{role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE Sales TO reader")},
			want:  []string{"reader"},
		},
		{
			name:  "a backtick-quoted name matches its unquoted form",
			roles: []neo4jv1beta1.Neo4jRole{role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE `sales` TO reader")},
			want:  []string{"reader"},
		},
		{
			// One role with several privileges on the dropped database is one
			// affected role, not several warnings.
			name: "a role is reported once however many privileges name it",
			roles: []neo4jv1beta1.Neo4jRole{role("reader", "prod-cluster",
				"GRANT ACCESS ON DATABASE sales TO reader",
				"GRANT MATCH {*} ON GRAPH sales NODES * TO reader",
				"DENY WRITE ON GRAPH sales TO reader")},
			want: []string{"reader"},
		},
		{
			name: "several affected roles are all reported, unaffected ones are not",
			roles: []neo4jv1beta1.Neo4jRole{
				role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE sales TO reader"),
				role("writer", "prod-cluster", "GRANT WRITE ON GRAPH sales TO writer"),
				role("other", "prod-cluster", "GRANT ACCESS ON DATABASE orders TO other"),
			},
			want: []string{"reader", "writer"},
		},
		{
			// A wildcard names no specific database, so nothing on it became
			// dangling: the privilege still applies to every database left.
			name:  "a wildcard grant is not reported",
			roles: []neo4jv1beta1.Neo4jRole{role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE * TO reader")},
			want:  []string{},
		},
		{
			name:  "DBMS-scoped privileges name no database",
			roles: []neo4jv1beta1.Neo4jRole{role("admin2", "prod-cluster", "GRANT ROLE MANAGEMENT ON DBMS TO admin2")},
			want:  []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, names(rolesGrantingOn(tc.roles, "prod-cluster", "sales")))
		})
	}
}

// Dashes are the reason database names get backticked at all — `movies-latest`
// is not a valid bare identifier — so that is the case worth testing, rather
// than a quoted name that never needed quoting.
func TestRolesGrantingOn_DashedName(t *testing.T) {
	roles := []neo4jv1beta1.Neo4jRole{
		role("reader", "prod-cluster", "GRANT MATCH {*} ON GRAPH `movies-latest` NODES * TO reader"),
		role("other", "prod-cluster", "GRANT MATCH {*} ON GRAPH `movies-upcoming` NODES * TO other"),
	}
	assert.Equal(t, []string{"reader"}, names(rolesGrantingOn(roles, "prod-cluster", "movies-latest")))
}

// The wiring: after a drop, the warning lands on BOTH the role (durable, and
// where the fix is made) and the database (visible to whoever is watching the
// delete). Exercised with a fake client and recorder, because the real drop
// needs a Neo4j server and envtest has none — the drop's own path is covered
// by the live check instead.
func TestWarnRolesStillGranting_EmitsOnRoleAndDatabase(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, neo4jv1beta1.AddToScheme(scheme))

	affected := role("reader", "prod-cluster", "GRANT ACCESS ON DATABASE sales TO reader")
	unaffected := role("other", "prod-cluster", "GRANT ACCESS ON DATABASE orders TO other")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&affected, &unaffected).Build()
	rec := record.NewFakeRecorder(10)
	r := &Neo4jDatabaseReconciler{Client: c, Recorder: rec}

	db := &neo4jv1beta1.Neo4jDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "sales-db", Namespace: "prod"},
		Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "prod-cluster", Name: "sales"},
	}
	r.warnRolesStillGranting(context.Background(), db)
	close(rec.Events)

	var got []string
	for e := range rec.Events {
		got = append(got, e)
	}
	assert.Len(t, got, 2, "one event on the affected role, one on the database: %v", got)
	joined := strings.Join(got, "\n")
	assert.Contains(t, joined, EventReasonPrivilegeTargetDropped)
	assert.Contains(t, joined, `Database "sales" was dropped, and this role's spec still grants on it`)
	assert.Contains(t, joined, "1 role(s) still grant on it in spec: reader")
	// The message must not repeat the claim that turned out to be false on
	// 2026.08.1: Neo4j does NOT keep a privilege on a dropped database.
	assert.NotContains(t, joined, "Neo4j keeps those privileges")
	assert.NotContains(t, joined, "other", "a role naming a different database must not be warned")
}

// Nothing affected means nothing said — a warning on every drop would be noise.
func TestWarnRolesStillGranting_SilentWhenNothingAffected(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	unaffected := role("other", "prod-cluster", "GRANT ACCESS ON DATABASE orders TO other")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&unaffected).Build()
	rec := record.NewFakeRecorder(10)
	r := &Neo4jDatabaseReconciler{Client: c, Recorder: rec}

	r.warnRolesStillGranting(context.Background(), &neo4jv1beta1.Neo4jDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "sales-db", Namespace: "prod"},
		Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "prod-cluster", Name: "sales"},
	})
	close(rec.Events)
	assert.Empty(t, rec.Events, "no affected roles, so no events")
}

// The resolve lookup is now load-bearing — an unresolved privilege is SKIPPED,
// not just warned about — so it must be case-insensitive. Neo4j database names
// are: GRANT ACCESS ON DATABASE `ORDERS` lands on `orders` (verified on
// 2026.08.1). An exact-case lookup would silently withhold a valid grant.
func TestUnresolvedDatabaseNames_CaseInsensitive(t *testing.T) {
	// resolvableDatabaseNames lower-cases its keys; mirror that here.
	known := map[string]bool{"orders": true, "movies-latest": true}

	assert.Empty(t, unresolvedDatabaseNames([]string{"ORDERS"}, known),
		"a differently-cased name for an existing database is resolved")
	assert.Empty(t, unresolvedDatabaseNames([]string{"Movies-Latest"}, known))
	assert.Equal(t, []string{"sales"}, unresolvedDatabaseNames([]string{"sales", "Orders"}, known),
		"a genuinely missing database is still reported")
}

// The same drop was announced twice on the database, once as "sf-probe,
// sf-role" and once as "sf-role, sf-probe", so the event recorder could not
// fold them (v1.18.0 journey). The names are sorted whatever order the roles
// were listed in (a fake client always lists in name order, so the input is
// built out of order here directly).
func TestSortedRoleNames_IsStableWhateverTheListOrder(t *testing.T) {
	zeta := role("zeta", "prod-cluster", "")
	alpha := role("alpha", "prod-cluster", "")
	mid := role("mid", "prod-cluster", "")
	assert.Equal(t, []string{"alpha", "mid", "zeta"}, sortedRoleNames([]*neo4jv1beta1.Neo4jRole{&zeta, &alpha, &mid}))
	assert.Equal(t, []string{"alpha", "mid", "zeta"}, sortedRoleNames([]*neo4jv1beta1.Neo4jRole{&mid, &zeta, &alpha}))
}
