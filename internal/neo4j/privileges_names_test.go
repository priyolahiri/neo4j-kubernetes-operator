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
	"testing"

	"github.com/stretchr/testify/require"
)

// Names keep their case even when they spell a keyword; keywords are still
// upper-cased. Names are case-sensitive in Neo4j, so upper-casing them made
// different privileges canonicalise to the same string.
func TestCanonicalisePrivilegeStatement_NamesKeepTheirCase(t *testing.T) {
	tests := []struct{ in, want string }{
		{"grant read {name} on graph sales nodes Person to r",
			"GRANT READ {name} ON GRAPH sales NODES Person TO r"},
		{"GRANT READ {name, user} ON GRAPH sales NODE Person TO r",
			"GRANT READ {name, user} ON GRAPH sales NODE Person TO r"},
		{"GRANT TRAVERSE ON GRAPH sales NODES Role, Label TO r",
			"GRANT TRAVERSE ON GRAPH sales NODES Role, Label TO r"},
		{"GRANT ACCESS ON DATABASE users TO r", "GRANT ACCESS ON DATABASE users TO r"},
		{"GRANT ACCESS ON DATABASES sales, users TO r", "GRANT ACCESS ON DATABASES sales, users TO r"},
		{"GRANT ACCESS ON DATABASE neo4j TO users", "GRANT ACCESS ON DATABASE neo4j TO users"},
		{"GRANT SET LABEL Role, Name ON GRAPH g TO r", "GRANT SET LABEL Role, Name ON GRAPH g TO r"},
		{"GRANT IMPERSONATE (user, role) ON DBMS TO r", "GRANT IMPERSONATE (user, role) ON DBMS TO r"},
		{"GRANT MATCH {*} ON GRAPH g FOR (n:Role) WHERE n.x is null TO r",
			"GRANT MATCH {*} ON GRAPH g FOR (n:Role) WHERE n.x IS NULL TO r"},
		{"GRANT MATCH {*} ON GRAPH g FOR (n:Label where n.x = 1) TO r",
			"GRANT MATCH {*} ON GRAPH g FOR (n:Label WHERE n.x = 1) TO r"},
		{"GRANT MATCH {*} ON HOME GRAPH NODE Role TO r", "GRANT MATCH {*} ON HOME GRAPH NODE Role TO r"},
		{"GRANT MATCH {*} ON GRAPH * NODE Role TO r", "GRANT MATCH {*} ON GRAPH * NODE Role TO r"},
		// Keywords in keyword positions are still upper-cased.
		{"grant create new node label on database sales to r", "GRANT CREATE NEW NODE LABEL ON DATABASE sales TO r"},
		{"grant all graph privileges on graph sales to r", "GRANT ALL GRAPH PRIVILEGES ON GRAPH sales TO r"},
		{"grant show user on dbms to r", "GRANT SHOW USER ON DBMS TO r"},
		{"grant access on home database to r", "GRANT ACCESS ON HOME DATABASE TO r"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, CanonicalisePrivilegeStatement(tt.in))
		})
	}

	// The collision this fixes: two different properties, one canonical form.
	require.NotEqual(t,
		CanonicalisePrivilegeStatement("GRANT READ {name} ON GRAPH g NODE * TO r"),
		CanonicalisePrivilegeStatement("GRANT READ {NAME} ON GRAPH g NODE * TO r"))
}

// DerivePrivilegeRevoke builds its REVOKE from the canonical form, so an
// upper-cased grantee revoked from a DIFFERENT role.
func TestDerivePrivilegeRevoke_KeepsTheGranteesCase(t *testing.T) {
	got, err := DerivePrivilegeRevoke("GRANT ACCESS ON DATABASE neo4j TO users")
	require.NoError(t, err)
	require.Equal(t, "REVOKE GRANT ACCESS ON DATABASE neo4j FROM users", got)
}

// DerivePrivilegeRevoke canonicalises text that is often already canonical,
// so canonicalising twice must change nothing.
func TestCanonicalisePrivilegeStatement_Idempotent(t *testing.T) {
	for _, in := range []string{
		"grant read {name, user} on graph sales nodes Role to users",
		"GRANT MATCH {*} ON GRAPH `sales` NODE * TO `probe`",
		"grant traverse on graphs sales, users nodes Person, Role to r",
		"GRANT MATCH {*} ON GRAPH g FOR (n:Role where n.x is not null) TO r",
		"grant create new relationship types on databases a, b to r",
		"GRANT IMPERSONATE (`user`, role) ON DBMS TO `my role`",
	} {
		once := CanonicalisePrivilegeStatement(in)
		require.Equal(t, once, CanonicalisePrivilegeStatement(once), in)
	}
}
