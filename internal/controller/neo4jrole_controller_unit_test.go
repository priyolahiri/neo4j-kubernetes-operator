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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCanonicaliseDesired_MapsCanonicalToOriginal pins audit finding #3: the
// add loop must execute the user's ORIGINAL statement text, not the canonical
// form. The canonical form upper-cases bare tokens — including unquoted
// identifiers — so a role named `users` written unquoted canonicalises to
// "... TO USERS" and would target the wrong (case-sensitive) role if executed.
// canonicaliseDesired must therefore return the original text keyed by canon.
func TestCanonicaliseDesired_MapsCanonicalToOriginal(t *testing.T) {
	r := &Neo4jRoleReconciler{}

	t.Run("keyword-like identifier preserves original case", func(t *testing.T) {
		orig := "GRANT ACCESS ON DATABASE neo4j TO users"
		canon, byCanonical, err := r.canonicaliseDesired([]string{orig})
		assert.NoError(t, err)
		assert.Len(t, canon, 1)
		// Canonical upper-cases the bare role name (it is a reserved keyword).
		assert.Equal(t, "GRANT ACCESS ON DATABASE neo4j TO USERS", canon[0])
		// But the map gives back the original text we must actually execute.
		assert.Equal(t, orig, byCanonical[canon[0]],
			"add loop must run the original statement, not the upper-cased canonical")
	})

	t.Run("dedup keeps first original and sorts canon", func(t *testing.T) {
		canon, byCanonical, err := r.canonicaliseDesired([]string{
			"GRANT ACCESS ON DATABASE neo4j TO r",
			"grant   access  on database neo4j to r", // same canonical, different text
			"GRANT MATCH {*} ON GRAPH neo4j NODES * TO r",
		})
		assert.NoError(t, err)
		assert.Len(t, canon, 2, "two distinct canonical statements")
		// First original wins for the duplicated canonical form.
		assert.Equal(t, "GRANT ACCESS ON DATABASE neo4j TO r",
			byCanonical["GRANT ACCESS ON DATABASE neo4j TO r"])
	})

	t.Run("empty and whitespace-only statements are skipped", func(t *testing.T) {
		canon, byCanonical, err := r.canonicaliseDesired([]string{"", "   ", " ; "})
		assert.NoError(t, err)
		assert.Empty(t, canon)
		assert.Empty(t, byCanonical)
	})
}

// The silent DR failure, in the two pieces that can be tested without a live
// server: which databases a role's privileges name, and which of those the
// cluster does not have.
//
// Why it matters: Neo4j accepts `GRANT ACCESS ON DATABASE does_not_exist TO r`
// without an error. A Neo4jRole copied verbatim from an upstream cluster onto
// a DR cluster therefore reconciles to Ready, holds there under
// enforcePrivileges, and grants access to nothing — because the replica of
// `foo` is called `foo-replica` and privileges attach to the database, not to
// an alias. The discovery happens at failover otherwise.
func TestUnresolvedPrivilegeDatabases(t *testing.T) {
	t.Run("the DR case: upstream names on a downstream cluster", func(t *testing.T) {
		privileges := []string{
			"GRANT ACCESS ON DATABASE foo TO analytics_reader",
			"GRANT MATCH {*} ON GRAPH foo NODES * TO analytics_reader",
		}
		// The downstream has the replica, under its own name.
		known := map[string]bool{"system": true, "neo4j": true, "foo-replica": true}

		named := privilegeDatabaseNames(privileges)
		assert.Equal(t, []string{"foo"}, named, "both statements name the same database, listed once")
		assert.Equal(t, []string{"foo"}, unresolvedDatabaseNames(named, known))
	})

	t.Run("the rewritten role is clean", func(t *testing.T) {
		privileges := []string{
			"GRANT ACCESS ON DATABASE `foo-replica` TO analytics_reader",
			"GRANT MATCH {*} ON GRAPH `foo-replica` NODES * TO analytics_reader",
		}
		known := map[string]bool{"foo-replica": true}
		assert.Empty(t, unresolvedDatabaseNames(privilegeDatabaseNames(privileges), known))
	})

	// An alias is a legitimate target — the privilege resolves through it — so
	// naming one must not be reported.
	t.Run("an alias counts as resolvable", func(t *testing.T) {
		privileges := []string{"GRANT ACCESS ON DATABASE foo TO r"}
		known := map[string]bool{"foo-replica": true, "foo": true} // `foo` here is the alias
		assert.Empty(t, unresolvedDatabaseNames(privilegeDatabaseNames(privileges), known))
	})

	// Nothing here can be missing, so nothing may be reported — a false
	// warning on every DBMS-scoped role would make the real one worthless.
	t.Run("scopes that name no database are never reported", func(t *testing.T) {
		privileges := []string{
			"GRANT ROLE MANAGEMENT ON DBMS TO r",
			"GRANT ACCESS ON HOME DATABASE TO r",
			"GRANT ACCESS ON DATABASE * TO r",
		}
		assert.Empty(t, privilegeDatabaseNames(privileges))
	})

	t.Run("several missing databases are all reported, in order", func(t *testing.T) {
		privileges := []string{
			"GRANT ACCESS ON DATABASES foo, bar TO r",
			"GRANT ACCESS ON DATABASE baz TO r",
		}
		known := map[string]bool{"bar": true}
		named := privilegeDatabaseNames(privileges)
		assert.Equal(t, []string{"foo", "bar", "baz"}, named)
		assert.Equal(t, []string{"foo", "baz"}, unresolvedDatabaseNames(named, known))
	})
}
