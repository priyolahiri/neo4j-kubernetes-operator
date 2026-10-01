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
	"strings"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// DefaultLanguageForImage returns the language to put in a CREATE DATABASE
// statement's DEFAULT LANGUAGE CYPHER clause for a deployment running imageTag,
// or "" when the clause must be left out.
//
// The clause exists only on CalVer. The 5.26 LTS does not parse it (verified
// on 5.26.0 and 5.26.31), and every database there runs Cypher 5 anyway, so a
// requested "5" is dropped rather than refused — the outcome is what the user
// asked for — while a requested "25" is refused earlier, by the
// DatabaseValidator, and never reaches here.
//
// An image tag that cannot be parsed (a digest, "latest") cannot be gated, so
// the request passes through unchanged: the same answer the composite
// validator gives for a tag it cannot read.
func DefaultLanguageForImage(imageTag, requested string) string {
	if requested == "" {
		return ""
	}
	v, err := ParseVersion(imageTag)
	if err != nil {
		return requested
	}
	if !v.SupportsCypherLanguageVersion() {
		return ""
	}
	return requested
}

// createDatabaseStatement is everything that shapes a CREATE DATABASE
// statement for a Neo4jDatabase. The four public create methods (plain, with
// topology, from a seed, from a seed with topology) used to render their own
// copies of this text, and the plain one silently lacked the language: one
// builder means a clause cannot be missing from one path only.
type createDatabaseStatement struct {
	name        string
	ifNotExists bool
	// cypherVersion is the DEFAULT LANGUAGE CYPHER value, already gated for
	// the target version (see DefaultLanguageForImage). Only "5" and "25" are
	// ever rendered.
	cypherVersion string
	// primaries and secondaries render a TOPOLOGY clause when either is > 0.
	primaries   int32
	secondaries int32
	options     map[string]string
	seedURI     string
	seedConfig  *neo4jv1beta1.SeedConfiguration
	wait        bool
}

// buildCreateDatabaseQuery renders
//
//	CREATE DATABASE `name` [IF NOT EXISTS] [DEFAULT LANGUAGE CYPHER n]
//	  [TOPOLOGY …] [OPTIONS {…}] WAIT|NOWAIT
//
// and the driver parameters its OPTIONS clause references.
func (c *Client) buildCreateDatabaseQuery(s createDatabaseStatement) (string, map[string]any) {
	query := fmt.Sprintf("CREATE DATABASE `%s`", s.name)
	if s.ifNotExists {
		query += " IF NOT EXISTS"
	}

	// Only "5"/"25" are ever emitted.
	query += cypherLanguageClause(s.cypherVersion)
	query += createTopologyClause(s.primaries, s.secondaries)

	// seedURI, seedConfig (provider-config string), seedRestoreUntil
	// (point-in-time) and any general options are emitted as one documented
	// OPTIONS map; every value is a driver parameter. This replaces the
	// non-grammar `FROM '<uri>'` and `SEED CONFIG {…}` clauses (issue #169).
	optClause, params := c.buildOptionsClause(s.options, s.seedURI, s.seedConfig)
	query += optClause

	if s.wait {
		query += " WAIT"
	} else {
		query += " NOWAIT"
	}
	return query, params
}

// createTopologyClause renders ` TOPOLOGY n PRIMAR{Y|IES} [m SECONDAR{Y|IES}]`
// (with its leading space), or "" when neither count is set. Unlike
// topologyClause, which serves CREATE REPLICA DATABASE and needs a primary, a
// secondaries-only request is still rendered: the validator, not this builder,
// decides whether that is legal.
func createTopologyClause(primaries, secondaries int32) string {
	var parts []string
	if primaries > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", primaries, pluralise(primaries, "PRIMARY", "PRIMARIES")))
	}
	if secondaries > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", secondaries, pluralise(secondaries, "SECONDARY", "SECONDARIES")))
	}
	if len(parts) == 0 {
		return ""
	}
	return " TOPOLOGY " + strings.Join(parts, " ")
}
