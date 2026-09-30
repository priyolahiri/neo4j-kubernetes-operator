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
