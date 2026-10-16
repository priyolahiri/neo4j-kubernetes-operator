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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

const learnRole = "r"

// fakeRoleServer is one role on a Neo4j that stores grants the way the
// recorded fixture says the real server does.
type fakeRoleServer struct {
	t        *testing.T
	render   map[string][]string // statement TO `probe` -> stored rows TO `probe`
	rows     map[string]bool     // the role's stored rows, as SHOW returns them
	grants   int
	failOn   string
	rendered func(stmt string) []string // override for statements outside the fixture
}

func newFakeRoleServer(t *testing.T) *fakeRoleServer {
	t.Helper()
	f := loadRenderingFixture(t)
	s := &fakeRoleServer{t: t, render: map[string][]string{}, rows: map[string]bool{}}
	for _, c := range f.Cases {
		key, err := neo4jclient.ReplacePrivilegeGrantee(c.Statement, "probe")
		require.NoError(t, err)
		s.render[key] = c.Stored
	}
	return s
}

func (s *fakeRoleServer) storedRows(stmt string) []string {
	s.t.Helper()
	key, err := neo4jclient.ReplacePrivilegeGrantee(stmt, "probe")
	require.NoError(s.t, err)
	stored, ok := s.render[key]
	if !ok && s.rendered != nil {
		return s.rendered(stmt)
	}
	require.True(s.t, ok, "statement not in fixture: %s", stmt)
	out := make([]string, 0, len(stored))
	for _, row := range stored {
		r, err := neo4jclient.ReplacePrivilegeGrantee(row, learnRole)
		require.NoError(s.t, err)
		out = append(out, r)
	}
	return out
}

// grant applies a statement directly, as an out-of-band admin would.
func (s *fakeRoleServer) grant(stmt string) {
	for _, row := range s.storedRows(spec(s.t, stmt)) {
		s.rows[row] = true
	}
}

func (s *fakeRoleServer) revokeCanonical(canon string) {
	for row := range s.rows {
		if neo4jclient.CanonicalisePrivilegeStatement(row) == canon {
			delete(s.rows, row)
		}
	}
}

func (s *fakeRoleServer) list() []string { return sortedKeys(s.rows) }

func (s *fakeRoleServer) current() map[string]bool { return newLearnState("").rememberAll(s.list()) }

func (s *fakeRoleServer) GrantAndReadPrivileges(_ context.Context, role, stmt string) ([]string, []string, error) {
	require.Equal(s.t, learnRole, role)
	if stmt == s.failOn {
		return nil, nil, errors.New("Invalid input")
	}
	before := s.list()
	s.grants++
	for _, row := range s.storedRows(stmt) {
		s.rows[row] = true
	}
	return before, s.list(), nil
}

// spec writes a fixture statement as a spec entry for learnRole.
func spec(t *testing.T, stmt string) string {
	t.Helper()
	out, err := neo4jclient.ReplacePrivilegeGrantee(stmt, learnRole)
	require.NoError(t, err)
	return out
}

// revocable mirrors reconcileLearned: rows neither wanted nor unattributed.
func revocable(lo learnOutcome, now map[string]bool) []string {
	var out []string
	for _, row := range sortedKeys(now) {
		if _, wanted := lo.desired[row]; wanted || lo.state.baseline[row] {
			continue
		}
		out = append(out, row)
	}
	return out
}

func learn(t *testing.T, s *fakeRoleServer, stmts []string, prior learnState, scope string) learnOutcome {
	t.Helper()
	lo, err := learnDesired(context.Background(), s, learnRole, stmts, nil, prior, scope, s.list(), false)
	require.NoError(t, err)
	return lo
}

// A role the operator creates: every statement learns exactly what the
// server stores, so nothing is ever revoked that the spec asks for — the
// learn-mode twin of TestNormaliseDesired_MatchesServerStorage.
func TestLearnDesired_FreshRoleLearnsServerStorage(t *testing.T) {
	for _, c := range loadRenderingFixture(t).Cases {
		t.Run(c.Statement, func(t *testing.T) {
			s := newFakeRoleServer(t)
			stmts := []string{spec(t, c.Statement)}

			lo := learn(t, s, stmts, learnState{}, "scope")

			assert.Equal(t, 1, s.grants)
			assert.Empty(t, lo.ambiguous)
			assert.Empty(t, lo.state.baseline)
			assert.ElementsMatch(t, sortedKeys(s.current()), sortedKeys(keysOf(lo.desired)))
			assert.Empty(t, revocable(lo, s.current()), "would revoke a privilege the spec asks for")
		})
	}
}

func TestLearnDesired_SteadyStateGrantsNothing(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{
		spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe"),
		spec(t, "GRANT READ {name, age} ON GRAPH sales NODES Person TO probe"),
		spec(t, "GRANT MATCH {*} ON GRAPH sales TO probe"),
	}
	first := learn(t, s, stmts, learnState{}, "scope")
	require.Equal(t, 3, s.grants)

	for i := 0; i < 3; i++ {
		lo := learn(t, s, stmts, first.state, "scope")
		assert.Equal(t, 3, s.grants, "a learned, present statement is not re-granted")
		assert.Empty(t, revocable(lo, s.current()))
	}
}

// Drift inside a list: one of the rows a statement stands for is removed.
// The statement is re-granted and its rendering stays complete.
func TestLearnDesired_RestoresOneRowOfAList(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{spec(t, "GRANT READ {name, age} ON GRAPH sales NODES Person TO probe")}
	first := learn(t, s, stmts, learnState{}, "scope")
	require.Len(t, first.state.renderings[stmts[0]].rows, 2)

	s.revokeCanonical("GRANT READ {age} ON GRAPH sales NODE Person TO r")
	lo := learn(t, s, stmts, first.state, "scope")

	assert.Equal(t, 2, s.grants)
	assert.Len(t, s.rows, 2, "the missing row is back")
	assert.Len(t, lo.state.renderings[stmts[0]].rows, 2, "the rendering is still both rows")
}

// Out-of-band rows added after learning are foreign and revocable.
func TestLearnDesired_ForeignRowIsRevocable(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{spec(t, "GRANT MATCH {*} ON GRAPH sales NODES * TO probe")}
	first := learn(t, s, stmts, learnState{}, "scope")

	s.grant("GRANT WRITE ON GRAPHS sales, probedb TO probe")
	lo := learn(t, s, stmts, first.state, "scope")

	assert.ElementsMatch(t, []string{
		"GRANT WRITE ON GRAPH probedb TO r",
		"GRANT WRITE ON GRAPH sales TO r",
	}, revocable(lo, s.current()))
}

// A role that already had privileges when learn mode first met it: nothing
// it had is revoked, because learn mode cannot tell whose rows they are.
func TestLearnDesired_PreexistingRowsAreNeverRevoked(t *testing.T) {
	s := newFakeRoleServer(t)
	s.grant("GRANT ACCESS ON DATABASES sales, probedb TO probe")
	s.grant("GRANT WRITE ON GRAPHS * TO probe") // not in spec at all
	stmts := []string{spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe")}

	lo := learn(t, s, stmts, learnState{}, "scope")

	assert.Empty(t, revocable(lo, s.current()))
	assert.Contains(t, lo.ambiguous, stmts[0], "its rows all pre-existed, so they are unknown")
	assert.Len(t, lo.state.baseline, 3)
}

// Partly pre-existing: the statement's grant creates SOME of its rows, so it
// is not ambiguous — and the row that was already there is invisible to it.
// Only the first-meeting baseline stops that row being revoked.
func TestLearnDesired_PartlyPreexistingStatementKeepsItsHiddenRow(t *testing.T) {
	s := newFakeRoleServer(t)
	s.rows["GRANT WRITE ON GRAPH `sales` TO `r`"] = true // granted by hand before the CR
	stmts := []string{spec(t, "GRANT WRITE ON GRAPHS sales, probedb TO probe")}

	lo := learn(t, s, stmts, learnState{}, "scope")

	require.Empty(t, lo.ambiguous, "the probedb row was new, so the statement looks learned")
	assert.Equal(t, []string{"GRANT WRITE ON GRAPH probedb TO r"}, lo.state.renderings[stmts[0]].rows)
	assert.Empty(t, revocable(lo, s.current()), "the hidden sales row is the spec's own and must stay")
}

// A statement already written in stored form is attributed by its own text
// even when its row pre-existed.
func TestLearnDesired_StoredFormTextAttributesPreexistingRow(t *testing.T) {
	s := newFakeRoleServer(t)
	s.rows["GRANT ACCESS ON DATABASE `sales` TO `r`"] = true
	s.rendered = func(string) []string { return []string{"GRANT ACCESS ON DATABASE `sales` TO `r`"} }
	stmts := []string{"GRANT ACCESS ON DATABASE sales TO r"}

	lo := learn(t, s, stmts, learnState{}, "scope")

	assert.Empty(t, lo.ambiguous)
	assert.Equal(t, []string{"GRANT ACCESS ON DATABASE sales TO r"}, lo.state.renderings[stmts[0]].rows)
}

// The crash window: the operator granted a statement, then died before the
// rendering reached status. On restart the grant is a no-op, so its rows
// cannot be attributed — they must be held, not revoked.
func TestLearnDesired_LostRenderingDoesNotCostThePrivilege(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{spec(t, "GRANT MATCH {*} ON GRAPH sales NODES * TO probe")}
	learn(t, s, stmts, learnState{}, "scope") // granted...
	lost := newLearnState(scopeDigest("scope"))

	lo := learn(t, s, stmts, lost, "scope") // ...but never recorded

	assert.Contains(t, lo.ambiguous, stmts[0])
	assert.Empty(t, revocable(lo, s.current()))
}

// Two statements grant the same row. When the one that learned it leaves the
// spec, its rows are revoked — then everything is re-learned, and the grant
// of the remaining statement recreates the row it needs.
func TestLearnDesired_OverlapSurvivesRemovalOfTheOtherStatement(t *testing.T) {
	s := newFakeRoleServer(t)
	s.rendered = func(stmt string) []string {
		if stmt == "GRANT ACCESS ON DATABASES sales TO r" {
			return []string{"GRANT ACCESS ON DATABASE `sales` TO `r`"}
		}
		return nil
	}
	both := []string{
		spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe"),
		"GRANT ACCESS ON DATABASES sales TO r",
	}
	first := learn(t, s, both, learnState{}, "scope")
	require.Contains(t, first.ambiguous, both[1], "its only row was already granted by the first")

	remaining := both[1:]
	lo := learn(t, s, remaining, first.state, "scope")
	toRevoke := revocable(lo, s.current())
	orphans := orphanedRows(first.state, remaining)
	require.ElementsMatch(t, []string{
		"GRANT ACCESS ON DATABASE probedb TO r",
		"GRANT ACCESS ON DATABASE sales TO r",
	}, toRevoke, "rows of a removed statement are attributable, so revocable")
	relearn := false
	for _, row := range toRevoke {
		s.revokeCanonical(row)
		relearn = relearn || orphans[row]
	}
	require.True(t, relearn)

	after, err := learnDesired(context.Background(), s, learnRole, remaining, nil, lo.state, "scope", s.list(), true)
	require.NoError(t, err)
	assert.Equal(t, []string{"GRANT ACCESS ON DATABASE `sales` TO `r`"}, s.list(), "the remaining statement's row is back")
	assert.Empty(t, after.ambiguous, "and now it is attributed")
}

// A pre-existing row going away may unhide a statement's row: re-learn.
func TestLearnDesired_VanishedPreexistingRowTriggersRelearn(t *testing.T) {
	s := newFakeRoleServer(t)
	s.grant("GRANT MATCH {*} ON GRAPH sales NODES * TO probe")
	stmts := []string{spec(t, "GRANT MATCH {*} ON GRAPH sales NODES * TO probe")}
	first := learn(t, s, stmts, learnState{}, "scope")
	require.Contains(t, first.ambiguous, stmts[0])
	grants := s.grants

	s.revokeCanonical("GRANT MATCH {*} ON GRAPH sales NODE * TO r") // an admin cleans up by hand
	lo := learn(t, s, stmts, first.state, "scope")

	assert.Equal(t, grants+1, s.grants)
	assert.Empty(t, lo.ambiguous)
	assert.Empty(t, lo.state.baseline)
	assert.True(t, s.rows["GRANT MATCH {*} ON GRAPH `sales` NODE * TO `r`"], "and the spec's privilege is in place")
}

func TestLearnDesired_ScopeChangeRelearnsAndKeepsRows(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe")}
	first := learn(t, s, stmts, learnState{}, "old-image")

	lo := learn(t, s, stmts, first.state, "new-image")

	assert.Equal(t, 2, s.grants, "re-learned once on the new server")
	assert.Equal(t, first.state.renderings[stmts[0]].rows, lo.state.renderings[stmts[0]].rows)
	assert.Empty(t, revocable(lo, s.current()))
}

// The v1.20.0 and v1.21.0 journeys saw PrivilegesApplied "N added" on roles
// where nothing had changed, once per alias change on the cluster ("7 added"
// six times on one role). A scope change re-learns every statement: the
// GRANTs really run again, once, and are no-ops unless the statement's
// rendering moved. Only a grant that created a row is counted as added. (An
// alias the role does not name no longer changes its scope at all —
// TestLearnDesired_AnAliasTheRoleDoesNotNameSendsNoGrants.)
func TestLearnDesired_RelearnCountsOnlyGrantsThatAddedARow(t *testing.T) {
	s := newFakeRoleServer(t)
	target := "orders-v1"
	const viaAlias = "GRANT ACCESS ON DATABASE orders TO r" // orders is an alias
	s.rendered = func(stmt string) []string {
		if stmt == viaAlias {
			return []string{"GRANT ACCESS ON DATABASE `" + target + "` TO `r`"}
		}
		return nil
	}
	stmts := []string{
		spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe"),
		spec(t, "GRANT READ {name, age} ON GRAPH sales NODES Person TO probe"),
		spec(t, "GRANT MATCH {*} ON GRAPH sales TO probe"),
		viaAlias,
	}

	first := learn(t, s, stmts, learnState{}, "orders=orders-v1")
	require.Equal(t, 4, first.added, "a new role: every grant created its rows")

	steady := learn(t, s, stmts, first.state, "orders=orders-v1")
	require.Equal(t, 4, s.grants)
	assert.Zero(t, steady.added)

	// The scope changes without moving any rendering: a new image, or an
	// alias created on a cluster where the role has a statement on `*`.
	elsewhere := learn(t, s, stmts, steady.state, "orders=orders-v1,reports=reports-v1")
	assert.Equal(t, 8, s.grants, "the scope changed, so every statement is granted again once...")
	assert.Zero(t, elsewhere.added, "...and none of those grants added anything, so nothing is reported")
	assert.Empty(t, revocable(elsewhere, s.current()))

	// The alias a statement names is re-pointed: that grant adds a row.
	target = "orders-v2"
	moved := learn(t, s, stmts, elsewhere.state, "orders=orders-v2,reports=reports-v1")
	assert.Equal(t, 1, moved.added, "the one statement whose rendering moved")
	assert.True(t, s.rows["GRANT ACCESS ON DATABASE `orders-v2` TO `r`"])
	assert.Empty(t, revocable(moved, s.current()), "the old target's row is kept, as Neo4j keeps it")

	after := learn(t, s, stmts, moved.state, "orders=orders-v2,reports=reports-v1")
	assert.Zero(t, after.added)
	assert.Equal(t, 12, s.grants, "the pass after a re-learn grants nothing")
}

// learnTarget is the cluster the alias-layout tests' roles live on.
var learnTarget = ResolvedTarget{Found: true, Cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
	ObjectMeta: metav1.ObjectMeta{UID: "cluster-uid"},
	Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
		Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
	},
}}

// scopeFor is the scope a reconcile computes for these statements when SHOW
// ALIASES lists these aliases.
func scopeFor(stmts []string, aliases ...neo4jclient.AliasInfo) string {
	return roleRenderingScope(learnTarget, aliases, true, stmts)
}

func alias(name, target string) neo4jclient.AliasInfo {
	return neo4jclient.AliasInfo{Name: name, Database: target, Location: "local"}
}

func constituent(composite, name, target string) neo4jclient.AliasInfo {
	return neo4jclient.AliasInfo{Name: composite + "." + name, Composite: composite, Database: target, Location: "local"}
}

// aliasedRole is a role with one statement through each alias in targets,
// whose stored form follows that alias's current target, as Neo4j's does.
func aliasedRole(t *testing.T, targets map[string]*string) (*fakeRoleServer, []string) {
	t.Helper()
	s := newFakeRoleServer(t)
	stmts := []string{
		spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe"),
		spec(t, "GRANT MATCH {*} ON GRAPH sales TO probe"),
	}
	via := map[string]string{}
	for _, name := range sortedKeys(keysOfPtr(targets)) {
		stmt := "GRANT ACCESS ON DATABASE " + name + " TO r"
		via[stmt] = name
		stmts = append(stmts, stmt)
	}
	s.rendered = func(stmt string) []string {
		name, ok := via[stmt]
		require.True(t, ok, "statement not in fixture: %s", stmt)
		return []string{"GRANT ACCESS ON DATABASE `" + *targets[name] + "` TO `r`"}
	}
	return s, stmts
}

func keysOfPtr(m map[string]*string) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

// The scope used to fingerprint every alias on the cluster, so an alias
// created, re-pointed or dropped anywhere, or a constituent added to any
// composite, re-sent every grant of every role. A role re-learns only on a
// change it can be stored through.
func TestLearnDesired_AnAliasTheRoleDoesNotNameSendsNoGrants(t *testing.T) {
	ordersTarget := "orders-v1"
	s, stmts := aliasedRole(t, map[string]*string{"orders": &ordersTarget})
	orders := alias("orders", "orders-v1")

	first := learn(t, s, stmts, learnState{}, scopeFor(stmts, orders))
	require.Equal(t, 3, s.grants)

	prior := first.state
	for _, elsewhere := range []struct {
		change string
		layout []neo4jclient.AliasInfo
	}{
		{"an alias created", []neo4jclient.AliasInfo{orders, alias("reports", "reports-v1")}},
		{"that alias re-pointed", []neo4jclient.AliasInfo{orders, alias("reports", "reports-v2")}},
		{"a constituent added", []neo4jclient.AliasInfo{orders, alias("reports", "reports-v2"),
			constituent("cineasts", "latest", "movies")}},
		{"the constituent re-pointed", []neo4jclient.AliasInfo{orders, alias("reports", "reports-v2"),
			constituent("cineasts", "latest", "movies-v2")}},
		{"both dropped", []neo4jclient.AliasInfo{orders}},
	} {
		lo := learn(t, s, stmts, prior, scopeFor(stmts, elsewhere.layout...))
		assert.Equal(t, 3, s.grants, "%s elsewhere on the cluster re-sent the role's grants", elsewhere.change)
		assert.Zero(t, lo.added)
		assert.Empty(t, revocable(lo, s.current()))
		prior = lo.state
	}
}

// Nothing that needs a re-learn is missed: a change to an alias the role
// names — re-pointed, dropped, created, or a constituent it names re-pointed —
// still re-grants once, and the grant through the moved alias adds its new
// target.
func TestLearnDesired_AChangeToAnAliasTheRoleNamesRelearns(t *testing.T) {
	ordersTarget, latestTarget := "orders-v1", "movies-v1"
	s, stmts := aliasedRole(t, map[string]*string{"orders": &ordersTarget, "cineasts.latest": &latestTarget})
	layout := func() []neo4jclient.AliasInfo {
		return []neo4jclient.AliasInfo{alias("orders", ordersTarget), constituent("cineasts", "latest", latestTarget),
			alias("reports", "reports-v1")}
	}
	prior := learn(t, s, stmts, learnState{}, scopeFor(stmts, layout()...)).state
	require.Equal(t, 4, s.grants)

	ordersTarget = "orders-v2"
	lo := learn(t, s, stmts, prior, scopeFor(stmts, layout()...))
	assert.Equal(t, 8, s.grants, "the alias the role names was re-pointed: every statement is granted once")
	assert.Equal(t, 1, lo.added)
	assert.True(t, s.rows["GRANT ACCESS ON DATABASE `orders-v2` TO `r`"], "the new target's row is in place")

	latestTarget = "movies-v2"
	lo = learn(t, s, stmts, lo.state, scopeFor(stmts, layout()...))
	assert.Equal(t, 12, s.grants, "so was the constituent it names")
	assert.Equal(t, 1, lo.added)
	assert.True(t, s.rows["GRANT ACCESS ON DATABASE `movies-v2` TO `r`"])

	// Dropped, then created again: each is a change to a name the role uses.
	withoutOrders := []neo4jclient.AliasInfo{constituent("cineasts", "latest", latestTarget), alias("reports", "reports-v1")}
	lo = learn(t, s, stmts, lo.state, scopeFor(stmts, withoutOrders...))
	assert.Equal(t, 16, s.grants, "the alias the role names was dropped")
	lo = learn(t, s, stmts, lo.state, scopeFor(stmts, layout()...))
	assert.Equal(t, 20, s.grants, "and created again")

	learn(t, s, stmts, lo.state, scopeFor(stmts, layout()...))
	assert.Equal(t, 20, s.grants, "the pass after a re-learn grants nothing")
}

// A statement on `*` ranges over every alias, and one whose targets cannot be
// read might name any: both keep the role on the whole layout.
func TestLearnDesired_AWildcardRoleRelearnsOnAnyAliasChange(t *testing.T) {
	for _, tc := range []struct {
		name string
		stmt string
	}{
		{"a statement on *", spec(t, "GRANT ACCESS ON DATABASES * TO probe")},
		{"a statement whose targets cannot be read", "GRANT LOAD ON ALL DATA TO r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newFakeRoleServer(t)
			s.rendered = func(stmt string) []string {
				require.Equal(t, "GRANT LOAD ON ALL DATA TO r", stmt)
				return []string{"GRANT LOAD ON ALL DATA TO `r`"}
			}
			stmts := []string{spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe"), tc.stmt}

			first := learn(t, s, stmts, learnState{}, scopeFor(stmts, alias("orders", "orders-v1")))
			require.Equal(t, 2, s.grants)

			lo := learn(t, s, stmts, first.state, scopeFor(stmts, alias("orders", "orders-v1"), alias("reports", "reports-v1")))
			assert.Equal(t, 4, s.grants, "any alias change re-learns the role")
			assert.Zero(t, lo.added)

			learn(t, s, stmts, lo.state, scopeFor(stmts, alias("orders", "orders-v1"), alias("reports", "reports-v1")))
			assert.Equal(t, 4, s.grants, "once")
		})
	}
}

// A failed SHOW ALIASES (or SHOW DATABASES) is an unknown layout, not an empty
// one. Read as empty, it changed the scope on the failure and again on the
// recovery: two rounds of every grant of every role on the cluster.
func TestLearnDesired_AnUnreadableAliasLayoutIsNotAScopeChange(t *testing.T) {
	ordersTarget := "orders-v1"
	s, stmts := aliasedRole(t, map[string]*string{"orders": &ordersTarget})
	layout := []neo4jclient.AliasInfo{alias("orders", "orders-v1"), alias("reports", "reports-v1")}
	first := learn(t, s, stmts, learnState{}, scopeFor(stmts, layout...))
	require.Equal(t, 3, s.grants)

	unknown := roleRenderingScope(learnTarget, nil, false, stmts)
	failed := learn(t, s, stmts, first.state, unknown)
	assert.Equal(t, 3, s.grants, "the failed listing re-sent the role's grants")
	assert.Equal(t, first.state.scope, failed.state.scope, "the scope the renderings were learned in is kept")

	recovered := learn(t, s, stmts, failed.state, scopeFor(stmts, layout...))
	assert.Equal(t, 3, s.grants, "the recovery re-sent them")

	// A change made while the layout could not be read is still seen, on the
	// first pass that can read it.
	ordersTarget = "orders-v2"
	during := learn(t, s, stmts, recovered.state, unknown)
	assert.Equal(t, 3, s.grants)
	moved := learn(t, s, stmts, during.state, scopeFor(stmts, alias("orders", "orders-v2"), alias("reports", "reports-v1")))
	assert.Equal(t, 6, s.grants, "the re-point the failed pass could not see re-learns the role")
	assert.Equal(t, 1, moved.added)
	assert.True(t, s.rows["GRANT ACCESS ON DATABASE `orders-v2` TO `r`"])
}

// The scope's key changed with the narrowing, so a role learned by a release
// that fingerprinted every alias re-learns once on upgrade — one round of
// no-op grants, then nothing. A role on `*` keeps the cluster-wide key it
// already had, and re-learns nothing.
func TestLearnDesired_UpgradeRelearnsOnceThenSettles(t *testing.T) {
	layout := []neo4jclient.AliasInfo{alias("orders", "orders-v1"), alias("reports", "reports-v1")}
	// What the previous release stored: every alias in the scope.
	previous := renderingScope(learnTarget, aliasFingerprint(layout))

	t.Run("a role naming databases", func(t *testing.T) {
		ordersTarget := "orders-v1"
		s, stmts := aliasedRole(t, map[string]*string{"orders": &ordersTarget})
		before := learn(t, s, stmts, learnState{}, previous)
		require.Equal(t, 3, s.grants)

		upgraded := learn(t, s, stmts, before.state, scopeFor(stmts, layout...))
		assert.Equal(t, 6, s.grants, "one round on the new key")
		assert.Zero(t, upgraded.added, "of grants that change nothing")
		assert.Empty(t, revocable(upgraded, s.current()))

		next := upgraded.state
		for i := 0; i < 3; i++ {
			next = learn(t, s, stmts, next, scopeFor(stmts, layout...)).state
		}
		assert.Equal(t, 6, s.grants, "and never again")
	})

	t.Run("a role on *", func(t *testing.T) {
		s := newFakeRoleServer(t)
		stmts := []string{spec(t, "GRANT ACCESS ON DATABASES * TO probe")}
		before := learn(t, s, stmts, learnState{}, previous)
		require.Equal(t, previous, scopeFor(stmts, layout...), "its key is the one the previous release stored")

		learn(t, s, stmts, before.state, scopeFor(stmts, layout...))
		assert.Equal(t, 1, s.grants, "nothing to re-learn")
	})
}

// A grant failing mid-pass must not lose what was known about the
// statements the pass had not reached yet.
func TestLearnDesired_ErrorKeepsUnreachedRenderings(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{
		spec(t, "GRANT WRITE ON GRAPHS sales, probedb TO probe"),
		spec(t, "GRANT MATCH {*} ON GRAPH sales NODES * TO probe"),
	}
	first := learn(t, s, stmts, learnState{}, "scope")

	s.revokeCanonical("GRANT WRITE ON GRAPH sales TO r")
	s.failOn = stmts[0]
	lo, err := learnDesired(context.Background(), s, learnRole, stmts, nil, first.state, "scope", s.list(), false)

	require.Error(t, err)
	assert.Equal(t, stmts[0], lo.grantErrStm)
	assert.Equal(t, first.state.renderings[stmts[1]], lo.state.renderings[stmts[1]])
}

func TestLearnDesired_UnresolvedIsNeitherGrantedNorForgotten(t *testing.T) {
	s := newFakeRoleServer(t)
	stmts := []string{spec(t, "GRANT ACCESS ON DATABASES sales, probedb TO probe")}
	first := learn(t, s, stmts, learnState{}, "scope")

	lo, err := learnDesired(context.Background(), s, learnRole, stmts,
		func(string) bool { return false }, first.state, "scope", s.list(), false)

	require.NoError(t, err)
	assert.Equal(t, 1, s.grants)
	assert.Equal(t, stmts, lo.unresolved)
	assert.Equal(t, first.state.renderings[stmts[0]], lo.state.renderings[stmts[0]])
}

func TestLearnStateRoundTripsThroughStatus(t *testing.T) {
	s := newFakeRoleServer(t)
	s.grant("GRANT WRITE ON GRAPHS * TO probe")
	stmts := []string{spec(t, "GRANT READ {name, age} ON GRAPH sales NODES Person TO probe")}
	lo := learn(t, s, stmts, learnState{}, "scope")

	renderings, baseline := lo.state.toStatus(stmts)
	back := learnStateFromStatus(neo4jv1beta1.Neo4jRoleStatus{
		PrivilegeRenderingScope: lo.state.scope,
		PrivilegeRenderings:     renderings,
		UnattributedPrivileges:  baseline,
	})
	assert.Equal(t, lo.state.scope, back.scope)
	assert.Equal(t, lo.state.renderings, back.renderings)
	assert.Equal(t, lo.state.baseline, back.baseline)
}

// Status holds rows as Neo4j showed them, not in canonical form, so a change
// to CanonicalisePrivilegeStatement cannot strand what was learned.
func TestLearnStatePersistsNeo4jsOwnWording(t *testing.T) {
	s := newFakeRoleServer(t)
	s.grant("GRANT WRITE ON GRAPHS * TO probe")
	stmts := []string{spec(t, "GRANT READ {name, age} ON GRAPH sales NODES Person TO probe")}
	lo := learn(t, s, stmts, learnState{}, "scope")

	renderings, baseline := lo.state.toStatus(stmts)
	require.Len(t, renderings, 1)
	assert.ElementsMatch(t, []string{
		"GRANT READ {age} ON GRAPH `sales` NODE Person TO `r`",
		"GRANT READ {name} ON GRAPH `sales` NODE Person TO `r`",
	}, renderings[0].Rows)
	assert.Equal(t, []string{"GRANT WRITE ON GRAPH * TO `r`"}, baseline)
}

func TestSetPrivilegeNormalisation(t *testing.T) {
	defer func() { privilegeNormalisationMode = PrivilegeNormalisationLearn }()
	assert.True(t, (&Neo4jRoleReconciler{}).learnMode(), "learn is the default")
	require.NoError(t, SetPrivilegeNormalisation("learn"))
	assert.True(t, (&Neo4jRoleReconciler{}).learnMode())
	assert.False(t, (&Neo4jRoleReconciler{PrivilegeNormalisation: "probe"}).learnMode())
	require.NoError(t, SetPrivilegeNormalisation("probe"))
	assert.False(t, (&Neo4jRoleReconciler{}).learnMode())
	assert.Error(t, SetPrivilegeNormalisation("off"))
}

func keysOf(m map[string]string) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}
