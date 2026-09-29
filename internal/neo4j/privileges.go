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
	"fmt"
	"regexp"
	"strings"
)

// CanonicalisePrivilegeStatement normalises a Cypher GRANT/DENY/REVOKE
// statement so that two semantically-equivalent statements compare equal.
//
// The canonicalisation is intentionally conservative — it does not parse
// Cypher. It performs textual normalisation that is safe for the dialect
// of statements emitted by `SHOW ... PRIVILEGES AS COMMANDS` and the
// statements users typically write in Neo4jRole.spec.privileges:
//
//   - collapses runs of ASCII whitespace to a single space
//   - trims leading and trailing whitespace and any single trailing semicolon
//   - upper-cases reserved keywords (GRANT/DENY/REVOKE/ON/TO/FROM/...)
//     while preserving identifiers, quoted strings and braces
//   - strips redundant backticks around simple, non-reserved identifiers so
//     that `neo4j` (the form `SHOW ... PRIVILEGES AS COMMANDS` emits) and
//     neo4j (the form users typically write in spec) compare equal
//
// The result is suitable for use as a map key when diffing desired vs.
// actual privileges. It is NOT safe to feed the canonical form back to
// Neo4j — always execute the original statement text.
func CanonicalisePrivilegeStatement(stmt string) string {
	s := strings.TrimSpace(stmt)
	s = strings.TrimSuffix(s, ";")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Collapse whitespace runs (but only outside of single/double-quoted strings)
	s = collapseWhitespacePreservingQuotes(s)

	// Upper-case reserved keywords while preserving everything else.
	s = upperCaseReservedKeywords(s)

	return s
}

// privilegeKeywords is the set of tokens that should be upper-cased when
// found as standalone words in a privilege statement. The list is closed
// rather than open so we never accidentally mutate identifiers that happen
// to spell a keyword (e.g. a role literally named "graph").
var privilegeKeywords = map[string]struct{}{
	"GRANT":         {},
	"DENY":          {},
	"REVOKE":        {},
	"IMMUTABLE":     {},
	"ON":            {},
	"TO":            {},
	"FROM":          {},
	"DATABASE":      {},
	"DATABASES":     {},
	"GRAPH":         {},
	"GRAPHS":        {},
	"DBMS":          {},
	"HOME":          {},
	"DEFAULT":       {},
	"NODES":         {},
	"NODE":          {},
	"RELATIONSHIPS": {},
	"RELATIONSHIP":  {},
	"ELEMENTS":      {},
	"ELEMENT":       {},
	"FOR":           {},
	"ALL":           {},
	"ACCESS":        {},
	"READ":          {},
	"MATCH":         {},
	"TRAVERSE":      {},
	"WRITE":         {},
	"CREATE":        {},
	"DELETE":        {},
	"DROP":          {},
	"ALTER":         {},
	"SET":           {},
	"REMOVE":        {},
	"LOAD":          {},
	"INDEX":         {},
	"CONSTRAINT":    {},
	"PRIVILEGE":     {},
	"PRIVILEGES":    {},
	"ROLE":          {},
	"ROLES":         {},
	"USER":          {},
	"USERS":         {},
	"NAME":          {},
	"NAMES":         {},
	"LABEL":         {},
	"LABELS":        {},
	"NEW":           {},
	"TYPE":          {},
	"TYPES":         {},
	"PROPERTY":      {},
	"EXECUTE":       {},
	"PROCEDURE":     {},
	"PROCEDURES":    {},
	"FUNCTION":      {},
	"FUNCTIONS":     {},
	"BOOSTED":       {},
	"MANAGEMENT":    {},
	"TRANSACTION":   {},
	"SHOW":          {},
	"START":         {},
	"STOP":          {},
	"TERMINATE":     {},
	"ASSIGN":        {},
	"IMPERSONATE":   {},
	"AUTH":          {},
	"SERVER":        {},
	"COMPOSITE":     {},
	"ALIAS":         {},
	"ALIASES":       {},
	"OF":            {},
	"AS":            {},
	"ANY":           {},
	"AWAIT":         {},
	"WAIT":          {},
	// Property-based access control (PBAC) WHERE-clause keywords. These appear
	// in `GRANT/DENY MATCH/READ/TRAVERSE … FOR pattern WHERE … TO role` and
	// must be upper-cased so spec strings round-trip equal against the output
	// of `SHOW ROLE PRIVILEGES AS COMMANDS`.
	"WHERE": {},
	"IS":    {},
	"NULL":  {},
	"NOT":   {},
	"IN":    {},
	"AND":   {},
	"OR":    {},
}

var (
	tokenSplit = regexp.MustCompile(`\s+`)
	// simpleIdentifierRe matches an identifier that is valid unquoted in
	// Cypher: a letter followed by letters, digits or underscores. Names
	// requiring quotes (spaces, dots, hyphens, leading digits) do not match,
	// so their backticks are preserved.
	simpleIdentifierRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
)

// redundantlyBacktickedIdentifier reports whether t is a backtick-quoted token
// whose backticks are redundant — the inner name is a simple identifier that
// is valid unquoted and is not a reserved privilege keyword — and if so returns
// the unquoted inner name. Names that genuinely require quoting (special
// characters, embedded/doubled backticks, or a reserved word that would change
// meaning or become invalid Cypher when unquoted) keep their backticks so the
// canonical form remains executable.
func redundantlyBacktickedIdentifier(t string) (string, bool) {
	if len(t) < 2 || t[0] != '`' || t[len(t)-1] != '`' {
		return "", false
	}
	inner := t[1 : len(t)-1]
	if !simpleIdentifierRe.MatchString(inner) {
		return "", false
	}
	if _, reserved := privilegeKeywords[strings.ToUpper(inner)]; reserved {
		return "", false
	}
	return inner, true
}

// collapseWhitespacePreservingQuotes collapses whitespace runs to a single
// space, but does not modify whitespace inside single- or double-quoted
// substrings (which would otherwise be folded incorrectly for things like
// `'multi  word' literals`). Backslash escapes inside quotes are honoured.
func collapseWhitespacePreservingQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSingle, inDouble, inBacktick, escape := false, false, false, false
	prevWasSpace := false
	for _, r := range s {
		if escape {
			b.WriteRune(r)
			escape = false
			prevWasSpace = false
			continue
		}
		switch r {
		case '\\':
			if inSingle || inDouble {
				escape = true
			}
			b.WriteRune(r)
			prevWasSpace = false
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
			b.WriteRune(r)
			prevWasSpace = false
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
			b.WriteRune(r)
			prevWasSpace = false
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
			b.WriteRune(r)
			prevWasSpace = false
		default:
			if !inSingle && !inDouble && !inBacktick && (r == ' ' || r == '\t' || r == '\n' || r == '\r') {
				if !prevWasSpace {
					b.WriteRune(' ')
					prevWasSpace = true
				}
				continue
			}
			b.WriteRune(r)
			prevWasSpace = false
		}
	}
	return b.String()
}

// upperCaseReservedKeywords walks tokens (delimited by ASCII whitespace and
// punctuation) and upper-cases any that match privilegeKeywords — unless the
// token sits where Cypher expects a NAME (see identifierPositions). Quoted
// strings and backtick-delimited identifiers are passed through unchanged.
//
// Names are case-sensitive in Neo4j: property `name` and property `NAME` are
// different properties, label `Role` is not label `ROLE`, a role called `user`
// is not one called `USER`. Upper-casing a name that happens to spell a
// keyword made two different privileges canonicalise to the same string, so
// the diff could take a foreign `READ {NAME}` as satisfying the spec's
// `READ {name}` and never grant it — or never revoke the foreign one.
func upperCaseReservedKeywords(s string) string {
	items := privilegeItems(s)
	ident := identifierPositions(items)

	var out strings.Builder
	out.Grow(len(s))
	for i, it := range items {
		if !it.token {
			out.WriteString(it.text)
			continue
		}
		t := it.text
		// Drop redundant backticks around simple identifiers (e.g. `neo4j` →
		// neo4j) so spec and `AS COMMANDS` forms canonicalise identically.
		// This is what stops the privilege diff from seeing perpetual drift
		// (and GRANT/REVOKE-churning every reconcile) under enforcePrivileges.
		if inner, ok := redundantlyBacktickedIdentifier(t); ok {
			out.WriteString(inner)
			continue
		}
		if _, ok := privilegeKeywords[strings.ToUpper(t)]; ok && !ident[i] {
			out.WriteString(strings.ToUpper(t))
			continue
		}
		out.WriteString(t)
	}
	return out.String()
}

// privilegeItem is a token (a word, a quoted string or a backtick-quoted
// identifier) or the literal separator text between tokens.
type privilegeItem struct {
	text  string
	token bool
}

// privilegeItems splits s the way upperCaseReservedKeywords always has:
// whitespace and the punctuation below end a token and are kept verbatim;
// quoted strings and backtick identifiers are single tokens.
func privilegeItems(s string) []privilegeItem {
	var items []privilegeItem
	inSingle, inDouble, inBacktick, escape := false, false, false, false
	var token strings.Builder
	flush := func() {
		if token.Len() > 0 {
			items = append(items, privilegeItem{text: token.String(), token: true})
			token.Reset()
		}
	}
	sep := func(r rune) { items = append(items, privilegeItem{text: string(r)}) }

	for _, r := range s {
		if escape {
			token.WriteRune(r)
			escape = false
			continue
		}
		if inSingle || inDouble {
			token.WriteRune(r)
			if r == '\\' {
				escape = true
				continue
			}
			if (inSingle && r == '\'') || (inDouble && r == '"') {
				flush()
				inSingle, inDouble = false, false
			}
			continue
		}
		if inBacktick {
			token.WriteRune(r)
			if r == '`' {
				flush()
				inBacktick = false
			}
			continue
		}
		switch r {
		case '\'':
			flush()
			inSingle = true
			token.WriteRune(r)
		case '"':
			flush()
			inDouble = true
			token.WriteRune(r)
		case '`':
			flush()
			inBacktick = true
			token.WriteRune(r)
		case ' ', '\t', '\n', '\r':
			flush()
			items = append(items, privilegeItem{text: " "})
		case '{', '}', '(', ')', ',', '*', '/', ':', ';':
			flush()
			sep(r)
		default:
			token.WriteRune(r)
		}
	}
	flush()
	return items
}

var (
	scopeKeywords   = map[string]bool{"GRAPH": true, "GRAPHS": true, "DATABASE": true, "DATABASES": true}
	segmentKeywords = map[string]bool{
		"NODE": true, "NODES": true, "RELATIONSHIP": true, "RELATIONSHIPS": true, "ELEMENT": true, "ELEMENTS": true,
	}
)

// identifierPositions marks the tokens Cypher reads as names, not keywords:
//
//	READ {name, age}                  property names
//	IMPERSONATE (alice, bob)          user names (and PBAC FOR (n:Label …))
//	ON GRAPH sales / ON DATABASES a, b  database and graph names
//	… ON GRAPH g NODE Person, Movie   labels and relationship types
//	SET LABEL Foo, Bar                labels
//	TO reader / FROM reader           the role
//
// and the comma-separated continuation of each of those lists.
func identifierPositions(items []privilegeItem) map[int]bool {
	ident := map[int]bool{}
	braces, parens := 0, 0
	// sig holds the indices of the significant (non-space) items so far.
	var sig []int
	at := func(back int) (privilegeItem, int, bool) {
		if len(sig) < back {
			return privilegeItem{}, -1, false
		}
		j := sig[len(sig)-back]
		return items[j], j, true
	}
	upper := func(it privilegeItem) string {
		if !it.token {
			return it.text
		}
		return strings.ToUpper(it.text)
	}

	for i, it := range items {
		if !it.token {
			switch it.text {
			case "{":
				braces++
			case "}":
				braces--
			case "(":
				parens++
			case ")":
				parens--
			}
			if it.text != " " {
				sig = append(sig, i)
			}
			continue
		}
		if strings.HasPrefix(it.text, "`") {
			ident[i] = true
			sig = append(sig, i)
			continue
		}

		prev, _, hasPrev := at(1)
		prev2, prev2Idx, hasPrev2 := at(2)
		switch {
		case braces > 0:
			ident[i] = true
		case parens > 0:
			ident[i] = hasPrev && !prev.token && (prev.text == "(" || prev.text == "," || prev.text == ":")
		case hasPrev && !prev.token && prev.text == ",":
			// A list continues only if what came before the comma was a name.
			ident[i] = hasPrev2 && ident[prev2Idx]
		case hasPrev && prev.token && hasPrev2 && scopeKeywords[upper(prev)] && upper(prev2) == "ON":
			ident[i] = true
		case hasPrev && prev.token && segmentKeywords[upper(prev)] && hasPrev2 &&
			(ident[prev2Idx] || prev2.text == "*" || upper(prev2) == "GRAPH"):
			// A segment after a graph name, `*`, or HOME/DEFAULT GRAPH. Not
			// `CREATE NEW NODE LABEL`, where NODE follows NEW.
			ident[i] = true
		case hasPrev && prev.token && (upper(prev) == "LABEL" || upper(prev) == "LABELS") && hasPrev2 &&
			(upper(prev2) == "SET" || upper(prev2) == "REMOVE"):
			ident[i] = true
		case hasPrev && prev.token && (upper(prev) == "TO" || upper(prev) == "FROM"):
			ident[i] = true
		}
		sig = append(sig, i)
	}
	return ident
}

// DerivePrivilegeRevoke turns a GRANT or DENY statement into the matching
// REVOKE statement that, when executed, removes that exact assignment. It
// is a textual transform: the input must already be canonical (run
// CanonicalisePrivilegeStatement first if unsure).
//
// The transform is:
//
//	GRANT  [IMMUTABLE] <body> TO <role>   →   REVOKE GRANT [IMMUTABLE] <body> FROM <role>
//	DENY   [IMMUTABLE] <body> TO <role>   →   REVOKE DENY  [IMMUTABLE] <body> FROM <role>
//
// Returns an error if the statement does not start with GRANT/DENY or
// does not contain a `TO <role>` clause.
func DerivePrivilegeRevoke(stmt string) (string, error) {
	canon := CanonicalisePrivilegeStatement(stmt)
	if canon == "" {
		return "", fmt.Errorf("empty privilege statement")
	}

	tokens := tokenSplit.Split(canon, -1)
	if len(tokens) < 4 {
		return "", fmt.Errorf("privilege statement too short: %q", stmt)
	}

	var verb string
	switch strings.ToUpper(tokens[0]) {
	case "GRANT", "DENY":
		verb = strings.ToUpper(tokens[0])
	default:
		return "", fmt.Errorf("statement must start with GRANT or DENY, got %q", tokens[0])
	}

	// Find the last `TO <role>` boundary. Privileges may contain a TO inside
	// quoted bodies, but our canonical form upper-cases bare TO only — quoted
	// 'TO' will not be re-cased. Walk tokens right-to-left for the first
	// bare TO.
	toIdx := -1
	for i := len(tokens) - 2; i > 0; i-- {
		if tokens[i] == "TO" {
			toIdx = i
			break
		}
	}
	if toIdx < 0 {
		return "", fmt.Errorf("statement missing TO <role>: %q", stmt)
	}

	body := strings.Join(tokens[1:toIdx], " ")
	roles := strings.Join(tokens[toIdx+1:], " ")
	return fmt.Sprintf("REVOKE %s %s FROM %s", verb, body, roles), nil
}

// PrivilegeStatementMatchesRole returns true when the (canonicalised)
// privilege statement ends with `TO <role>`. Used by validators to ensure
// each entry in Neo4jRole.spec.privileges names the role being defined.
func PrivilegeStatementMatchesRole(stmt, role string) bool {
	canon := CanonicalisePrivilegeStatement(stmt)
	if canon == "" {
		return false
	}
	tokens := tokenSplit.Split(canon, -1)
	if len(tokens) < 3 {
		return false
	}
	last := tokens[len(tokens)-1]
	prev := tokens[len(tokens)-2]
	if prev != "TO" {
		return false
	}
	// Strip backticks from `roleName` form
	last = strings.Trim(last, "`")
	return strings.EqualFold(last, role)
}

// PrivilegeStatementVerb returns "GRANT", "DENY" or "" for the first token
// of the (canonicalised) statement.
func PrivilegeStatementVerb(stmt string) string {
	canon := CanonicalisePrivilegeStatement(stmt)
	if canon == "" {
		return ""
	}
	first := strings.SplitN(canon, " ", 2)[0]
	switch strings.ToUpper(first) {
	case "GRANT":
		return "GRANT"
	case "DENY":
		return "DENY"
	default:
		return ""
	}
}

// PrivilegeDatabaseTargets returns the database or graph names a privilege
// statement is scoped to.
//
// It exists because a privilege that names a database which does not exist is
// accepted by Neo4j without complaint. On a DR cluster that is the difference
// between working authorization and none: the replica of `foo` is called
// `foo-replica` (Cypher has no RENAME DATABASE), privileges attach to the
// database rather than to an alias, and a role copied verbatim from the
// upstream therefore grants access to nothing. The Neo4jRole reconciles to
// Ready, `enforcePrivileges: true` holds it there, and the failure is visible
// only at failover — which is the one moment it must not be.
//
// Returns nil for statements with no database scope: ON DBMS, ON HOME/DEFAULT
// DATABASE, and the ON DATABASE * / ON GRAPH * wildcards, none of which names
// anything that could be missing.
//
// This is deliberately the same kind of conservative textual pass as
// CanonicalisePrivilegeStatement — it does not parse Cypher. Its output feeds
// a WARNING, never a rejection, so the cost of a name it fails to recognise is
// a warning not raised, not a valid configuration refused.
func PrivilegeDatabaseTargets(stmt string) []string {
	return privilegeTargets(stmt, map[string]bool{
		"DATABASE": true, "DATABASES": true, "GRAPH": true, "GRAPHS": true,
	})
}

// PrivilegeGraphTargets returns only the names under a GRAPH scope.
//
// The distinction matters for composite databases, and only for them. On a
// composite, `GRANT ACCESS ON DATABASE <composite>` is correct and required —
// it is how a user is allowed to query through the composite at all. But
// `GRANT MATCH {*} ON GRAPH <composite>` is inert: graph privileges attach to
// the constituents' target databases, never to the composite. Neo4j accepts
// the statement, persists it, and shows it back in SHOW ROLE PRIVILEGES, so
// nothing about the system says it does nothing.
func PrivilegeGraphTargets(stmt string) []string {
	return privilegeTargets(stmt, map[string]bool{"GRAPH": true, "GRAPHS": true})
}

// privilegeTargets extracts the names under the given ON-scopes.
func privilegeTargets(stmt string, scopes map[string]bool) []string {
	tokens := privilegeTokens(CanonicalisePrivilegeStatement(stmt))

	var out []string
	for i := 0; i < len(tokens); i++ {
		if tokens[i] != "ON" {
			continue
		}
		j := i + 1
		// ON DEFAULT DATABASE / ON HOME GRAPH name nothing.
		if j < len(tokens) && (tokens[j] == "DEFAULT" || tokens[j] == "HOME") {
			continue
		}
		if j >= len(tokens) {
			continue
		}
		if !scopes[tokens[j]] {
			// A scope we were not asked about, ON DBMS, or something we do
			// not recognise.
			continue
		}
		// Everything up to the next keyword is the comma-separated name list.
		for k := j + 1; k < len(tokens); k++ {
			t := tokens[k]
			if t == "," {
				continue
			}
			if _, reserved := privilegeKeywords[t]; reserved {
				break
			}
			if t == "*" {
				continue
			}
			if name := strings.Trim(t, "`"); name != "" {
				out = append(out, name)
			}
		}
	}
	return out
}

// privilegeTokens splits a canonicalised privilege statement into words,
// commas, and backtick-quoted identifiers (which are kept whole, backticks
// included, so a database named `TO` is not mistaken for the keyword).
func privilegeTokens(s string) []string {
	var tokens []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}
	inBacktick := false
	for _, r := range s {
		switch {
		case r == '`':
			inBacktick = !inBacktick
			cur.WriteRune(r)
			if !inBacktick {
				flush()
			}
		case inBacktick:
			cur.WriteRune(r)
		case r == ',':
			flush()
			tokens = append(tokens, ",")
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return tokens
}
