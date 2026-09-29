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
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// renderingFixture is Neo4j's recorded storage of each statement — see
// testdata/privilege_rendering.json. It is what the server does, not what we
// think it does; re-record it rather than edit it.
type renderingFixture struct {
	Cases []struct {
		Statement string   `json:"statement"`
		Stored    []string `json:"stored"`
	} `json:"cases"`
}

func loadRenderingFixture(t *testing.T) renderingFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/privilege_rendering.json")
	require.NoError(t, err)
	var f renderingFixture
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	return f
}

// fixtureProber answers probes from the recording, as the server would: it
// matches the probe statement (recorded TO `probe`) and returns the stored
// rows granted to whatever throwaway role was asked for.
type fixtureProber struct {
	byStatement map[string][]string
	calls       int
	err         error
}

func newFixtureProber(t *testing.T, f renderingFixture) *fixtureProber {
	t.Helper()
	p := &fixtureProber{byStatement: map[string][]string{}}
	for _, c := range f.Cases {
		key, err := neo4jclient.ReplacePrivilegeGrantee(c.Statement, "probe")
		require.NoError(t, err)
		p.byStatement[key] = c.Stored
	}
	return p
}

func (p *fixtureProber) ProbePrivilegeRendering(_ context.Context, probeRole, stmt string) ([]string, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	key, err := neo4jclient.ReplacePrivilegeGrantee(stmt, "probe")
	if err != nil {
		return nil, err
	}
	stored, ok := p.byStatement[key]
	if !ok {
		return nil, errors.New("statement not in fixture: " + stmt)
	}
	out := make([]string, 0, len(stored))
	for _, row := range stored {
		r, err := neo4jclient.ReplacePrivilegeGrantee(row, probeRole)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// storedFor is what SHOW ROLE <role> PRIVILEGES returns after the statement
// is granted to the real role, canonicalised the way fetchCurrentPrivileges
// does it.
func storedFor(t *testing.T, stored []string, role string) []string {
	t.Helper()
	set := map[string]bool{}
	for _, row := range stored {
		r, err := neo4jclient.ReplacePrivilegeGrantee(row, role)
		require.NoError(t, err)
		set[neo4jclient.CanonicalisePrivilegeStatement(r)] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

// The regression: for every recorded statement, the desired set is exactly
// what the server stores, so the diff is empty in both directions — nothing
// re-granted, and above all nothing revoked. Before normalisation, every
// statement with a plural, a list, a default segment, a verb synonym or an
// upper-case database name failed this and, under enforcePrivileges, was
// revoked on alternate reconciles.
func TestNormaliseDesired_MatchesServerStorage(t *testing.T) {
	f := loadRenderingFixture(t)
	const role = "salesReader"

	for _, c := range f.Cases {
		t.Run(c.Statement, func(t *testing.T) {
			r := &Neo4jRoleReconciler{}
			spec, err := neo4jclient.ReplacePrivilegeGrantee(c.Statement, role)
			require.NoError(t, err)

			desired, byCanonical := r.normaliseDesired(context.Background(),
				newFixtureProber(t, f), "scope", role, []string{spec}, nil)
			current := storedFor(t, c.Stored, role)

			assert.ElementsMatch(t, current, desired)
			assert.Empty(t, setDifference(sortedCopy(desired), sortedCopy(current)), "would re-grant")
			assert.Empty(t, setDifference(sortedCopy(current), sortedCopy(desired)), "would REVOKE a privilege the spec asks for")
			for _, row := range desired {
				assert.Equal(t, spec, byCanonical[row], "a missing row must map back to the spec statement that restores it")
			}
		})
	}
}

// Pins why normalisation exists, so no one "simplifies" it back to textual
// comparison: the shipped sample's own statement does not match its storage.
func TestTextualComparisonCannotMatchStorage(t *testing.T) {
	r := &Neo4jRoleReconciler{}
	spec := "GRANT MATCH {*} ON GRAPH sales NODES * TO salesReader"
	textual, _, err := r.canonicaliseDesired([]string{spec})
	require.NoError(t, err)
	current := storedFor(t, []string{"GRANT MATCH {*} ON GRAPH `sales` NODE * TO `probe`"}, "salesReader")
	assert.NotEqual(t, current, textual)
}

func TestNormaliseDesired_CachesPerScope(t *testing.T) {
	f := loadRenderingFixture(t)
	p := newFixtureProber(t, f)
	r := &Neo4jRoleReconciler{}
	stmts := []string{
		"GRANT ACCESS ON DATABASES sales, probedb TO a",
		"GRANT MATCH {*} ON GRAPH sales NODES * TO a",
	}

	r.normaliseDesired(context.Background(), p, "scope-1", "a", stmts, nil)
	require.Equal(t, 2, p.calls)

	// Same statements, another role on the same server: served from cache.
	other := []string{
		"GRANT ACCESS ON DATABASES sales, probedb TO b",
		"GRANT MATCH {*} ON GRAPH sales NODES * TO b",
	}
	desired, _ := r.normaliseDesired(context.Background(), p, "scope-1", "b", other, nil)
	assert.Equal(t, 2, p.calls, "rendering is per statement, not per role")
	assert.Contains(t, desired, "GRANT ACCESS ON DATABASE probedb TO b")

	// Another server, version or alias map: probed afresh.
	r.normaliseDesired(context.Background(), p, "scope-2", "a", stmts, nil)
	assert.Equal(t, 4, p.calls)
}

func TestNormaliseDesired_CacheExpires(t *testing.T) {
	f := loadRenderingFixture(t)
	p := newFixtureProber(t, f)
	now := time.Now()
	r := &Neo4jRoleReconciler{rendering: newPrivilegeRenderingCache()}
	r.rendering.now = func() time.Time { return now }
	stmts := []string{"GRANT MATCH {*} ON GRAPH sales NODES * TO a"}

	r.normaliseDesired(context.Background(), p, "s", "a", stmts, nil)
	r.normaliseDesired(context.Background(), p, "s", "a", stmts, nil)
	require.Equal(t, 1, p.calls)

	now = now.Add(renderingCacheTTL + time.Second)
	r.normaliseDesired(context.Background(), p, "s", "a", stmts, nil)
	assert.Equal(t, 2, p.calls)
}

// A statement naming a missing database is not probed — the server would
// refuse the probe grant as it refuses the real one — and falls back to its
// spec text so the skip-and-report path still sees it.
func TestNormaliseDesired_UnresolvableIsNotProbed(t *testing.T) {
	p := newFixtureProber(t, loadRenderingFixture(t))
	r := &Neo4jRoleReconciler{}
	spec := "GRANT ACCESS ON DATABASE gone TO a"

	desired, byCanonical := r.normaliseDesired(context.Background(), p, "s", "a",
		[]string{spec}, func(string) bool { return false })

	assert.Zero(t, p.calls)
	require.Len(t, desired, 1)
	assert.Equal(t, spec, byCanonical[desired[0]])
}

// A probe that fails (a syntax error, a privilege the operator may not grant)
// falls back to the spec text and is not cached; the real grant then fails
// with the error the user needs to see.
func TestNormaliseDesired_ProbeErrorFallsBackUncached(t *testing.T) {
	p := newFixtureProber(t, loadRenderingFixture(t))
	p.err = errors.New("Invalid input")
	r := &Neo4jRoleReconciler{}
	spec := "GRANT MATCH {*} ON GRAPH sales NODES * TO a"

	desired, _ := r.normaliseDesired(context.Background(), p, "s", "a", []string{spec}, nil)
	assert.Equal(t, []string{neo4jclient.CanonicalisePrivilegeStatement(spec)}, desired)

	r.normaliseDesired(context.Background(), p, "s", "a", []string{spec}, nil)
	assert.Equal(t, 2, p.calls, "a failed probe must not be cached")
}

// The probe role is throwaway and named so a leak is recognisable.
func TestNormaliseDesired_ProbesUnderThrowawayRole(t *testing.T) {
	var seen []string
	r := &Neo4jRoleReconciler{}
	r.normaliseDesired(context.Background(), proberFunc(func(role, stmt string) ([]string, error) {
		seen = append(seen, role)
		assert.True(t, strings.HasSuffix(stmt, "TO `"+role+"`"), stmt)
		return []string{"GRANT ACCESS ON DATABASE `sales` TO `" + role + "`"}, nil
	}), "s", "salesReader", []string{
		"GRANT ACCESS ON DATABASE sales TO salesReader",
		"GRANT ACCESS ON DATABASE SALES TO salesReader",
	}, nil)

	require.Len(t, seen, 2)
	for _, role := range seen {
		assert.True(t, strings.HasPrefix(role, probeRolePrefix), role)
		assert.NotEqual(t, "salesReader", role)
	}
	assert.NotEqual(t, seen[0], seen[1], "each probe gets its own role")
}

func TestAliasFingerprintTracksTargets(t *testing.T) {
	a := aliasFingerprint([]neo4jclient.AliasInfo{{Name: "x", Database: "sales"}, {Name: "y", Database: "orders"}})
	b := aliasFingerprint([]neo4jclient.AliasInfo{{Name: "y", Database: "orders"}, {Name: "x", Database: "sales"}})
	c := aliasFingerprint([]neo4jclient.AliasInfo{{Name: "x", Database: "orders"}, {Name: "y", Database: "orders"}})
	assert.Equal(t, a, b, "order-independent")
	assert.NotEqual(t, a, c, "retargeting an alias changes what a grant on it means")
}

type proberFunc func(role, stmt string) ([]string, error)

func (f proberFunc) ProbePrivilegeRendering(_ context.Context, role, stmt string) ([]string, error) {
	return f(role, stmt)
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
