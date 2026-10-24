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

	// A constituent whose parts need their quotes was read as two names,
	// `my-comp` and .`latest x`. The second never resolved, so the grant was
	// SKIPPED as naming a missing database, and the warning named a database
	// that nobody wrote. SHOW ALIASES lists the constituent as
	// my-comp.latest x, and resolvableDatabaseNames lower-cases it.
	t.Run("a constituent whose parts are quoted resolves", func(t *testing.T) {
		privileges := []string{
			"GRANT ACCESS ON DATABASE `my-comp`.`latest x` TO r",
			"GRANT MATCH {*} ON GRAPH `My-Comp`.latest NODES * TO r",
		}
		known := map[string]bool{"my-comp": true, "my-comp.latest x": true, "my-comp.latest": true}
		named := privilegeDatabaseNames(privileges)
		assert.Equal(t, []string{"my-comp.latest x", "My-Comp.latest"}, named)
		assert.Empty(t, unresolvedDatabaseNames(named, known))
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
