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
	"strings"
	"testing"
)

// The Cypher 25 directive belongs on the OIDC form only. The 5.26 LTS rejects
// the directive outright ("25 is not a valid option for cypher version"), and
// the stored-credential form is Cypher 5 syntax both lines parse; CalVer
// rejects OIDC CREDENTIAL FORWARDING without it (42I67). Verified on 5.26.31
// and 2026.08.1.
func TestRemoteAliasStatementPinsCypher25OnlyForOIDC(t *testing.T) {
	head := "CREATE ALIAS %s IF NOT EXISTS FOR DATABASE `%s`"
	url := "neo4j+s://upstream.example:7687"

	stored, params := buildRemoteAliasStatement(head, "cmp", "rem", "neo4j", url, RemoteConstituentAuth{
		Username: "u", Password: "p", DriverSettings: map[string]string{"connection_timeout": "duration({seconds: 5})"},
	})
	if HasCypher25Prefix(stored) {
		t.Errorf("stored-credential remote alias must run on 5.26, which rejects the Cypher 25 directive: %s", stored)
	}
	if !strings.HasPrefix(stored, "CYPHER 5 CREATE ALIAS `cmp`.`rem`") || !strings.Contains(stored, "USER $remoteUser PASSWORD $remotePassword") {
		t.Errorf("unexpected stored-credential statement: %s", stored)
	}
	if params["remotePassword"] != "p" {
		t.Errorf("password must travel as a parameter, got params %v", params)
	}

	oidc, _ := buildRemoteAliasStatement(head, "cmp", "rem", "neo4j", url, RemoteConstituentAuth{OIDCForwarding: true})
	// Cypher 25 refuses separately quoted name parts (42NAA) and reads the
	// whole-quoted qualified name as the constituent. Verified on 2026.08.1.
	if !strings.HasPrefix(oidc, "CYPHER 25 CREATE ALIAS `cmp.rem`") || !strings.HasSuffix(oidc, "OIDC CREDENTIAL FORWARDING") {
		t.Errorf("OIDC remote alias must be pinned to Cypher 25 with the whole-quoted name: %s", oidc)
	}
}

// Graph references mean different things in the two languages, so every
// constituent and alias statement is pinned rather than left to the server's
// default — which since v1.17.0 is Cypher 25 on a new CalVer deployment, where
// the unpinned `comp`.`name` form is refused outright (42NAA). Verified on
// 5.26.0, 5.26.28 and 2026.08.1: `CYPHER 5` is accepted by all three.
func TestCompositeAndAliasDDLIsPinnedToCypher5(t *testing.T) {
	for _, stmt := range []string{
		Cypher5("CREATE ALIAS `a`.`b` IF NOT EXISTS FOR DATABASE `c`"),
		Cypher5(Cypher5("DROP ALIAS `a`.`b` IF EXISTS FOR DATABASE")),
		Cypher5("  cypher 5 ALTER ALIAS `a` SET DATABASE TARGET `c`"),
	} {
		if !strings.HasPrefix(strings.ToUpper(stmt), "CYPHER 5 ") || strings.Count(strings.ToUpper(stmt), "CYPHER 5") != 1 {
			t.Errorf("want exactly one CYPHER 5 directive: %q", stmt)
		}
	}
}
