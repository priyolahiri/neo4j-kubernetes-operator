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
	head := "CREATE ALIAS `%s`.`%s` IF NOT EXISTS FOR DATABASE `%s`"
	url := "neo4j+s://upstream.example:7687"

	stored, params := buildRemoteAliasStatement(head, "cmp", "rem", "neo4j", url, RemoteConstituentAuth{
		Username: "u", Password: "p", DriverSettings: map[string]string{"connection_timeout": "duration({seconds: 5})"},
	})
	if HasCypher25Prefix(stored) {
		t.Errorf("stored-credential remote alias must run on 5.26, which rejects the Cypher 25 directive: %s", stored)
	}
	if !strings.HasPrefix(stored, "CREATE ALIAS `cmp`.`rem`") || !strings.Contains(stored, "USER $remoteUser PASSWORD $remotePassword") {
		t.Errorf("unexpected stored-credential statement: %s", stored)
	}
	if params["remotePassword"] != "p" {
		t.Errorf("password must travel as a parameter, got params %v", params)
	}

	oidc, _ := buildRemoteAliasStatement(head, "cmp", "rem", "neo4j", url, RemoteConstituentAuth{OIDCForwarding: true})
	if !strings.HasPrefix(oidc, "CYPHER 25 CREATE ALIAS `cmp`.`rem`") || !strings.HasSuffix(oidc, "OIDC CREDENTIAL FORWARDING") {
		t.Errorf("OIDC remote alias must be pinned to Cypher 25: %s", oidc)
	}
}
