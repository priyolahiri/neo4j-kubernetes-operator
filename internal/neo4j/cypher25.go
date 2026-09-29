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

import "strings"

// cypher25Prefix is the directive that makes Neo4j parse one statement as
// Cypher 25. Use Cypher25; nothing else should spell it (TestCypher25Guard).
const cypher25Prefix = "CYPHER 25 "

// Cypher25 pins one statement to Cypher 25.
//
// Every statement whose syntax is a Cypher 25 language feature must go
// through here. Neo4j CalVer defaults the system database to Cypher 5, and the
// failure is not a warning: the server cannot parse the statement and reports
// it as invalid input, which reads as though the feature did not exist. The
// convention of prepending a constant at each call site was missed twice —
// CREATE REPLICA DATABASE until the v1.15.0 journey ran it against a real
// 2026.08 server — so TestCypher25Guard now fails the build when a function
// builds Cypher-25-only syntax without calling this.
//
// It is per-statement on purpose: it changes nothing outside the statement it
// is attached to, unlike the server-wide db.query.default_language. It is safe
// when the database default is already 25, and idempotent.
//
// Only for syntax that exists ONLY in Cypher 25, which only CalVer servers
// have: the 5.26 LTS does not know the directive. Never apply it to a
// statement that must also run on 5.26.
func Cypher25(stmt string) string {
	trimmed := strings.TrimLeft(stmt, " \t\r\n")
	if HasCypher25Prefix(trimmed) {
		return trimmed
	}
	return cypher25Prefix + trimmed
}

// HasCypher25Prefix reports whether stmt already starts with the directive,
// in any case and with any spacing.
func HasCypher25Prefix(stmt string) bool {
	f := strings.Fields(strings.TrimSpace(stmt))
	return len(f) >= 2 && strings.EqualFold(f[0], "CYPHER") && f[1] == "25"
}
