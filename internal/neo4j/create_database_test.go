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

	"github.com/stretchr/testify/assert"
)

// TestDefaultLanguageForImage pins the one place that decides whether a
// Neo4jDatabase's spec.defaultCypherLanguage reaches the server. The 5.26 LTS
// has no DEFAULT LANGUAGE clause, so the value must be dropped there; CalVer
// keeps it. An image tag we cannot parse (a digest, "latest") cannot be
// gated, so the request passes through exactly as it did before the gate
// existed.
func TestDefaultLanguageForImage(t *testing.T) {
	cases := []struct {
		name      string
		tag       string
		requested string
		want      string
	}{
		{"LTS drops 5", "5.26-enterprise", "5", ""},
		{"LTS drops 25", "5.26.0-enterprise", "25", ""},
		{"CalVer keeps 5", "2026.08.1-enterprise", "5", "5"},
		{"CalVer keeps 25", "2026.08.1-enterprise", "25", "25"},
		{"first CalVer keeps 25", "2025.01.0-enterprise", "25", "25"},
		{"nothing requested stays empty on CalVer", "2026.08.1-enterprise", "", ""},
		{"nothing requested stays empty on LTS", "5.26-enterprise", "", ""},
		{"unparsable tag passes the request through", "latest", "25", "25"},
		{"empty tag passes the request through", "", "5", "5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DefaultLanguageForImage(tc.tag, tc.requested))
		})
	}
}

// TestBuildCreateDatabaseQuery_DefaultLanguage pins the CREATE DATABASE text
// for every create path the Neo4jDatabase controller takes — plain, with
// topology, from a seed, and from a seed with topology — on both supported
// lines. The plain path used to drop the language entirely (CreateDatabase took
// no Cypher version), and on 5.26 the other three emitted a clause the server
// cannot parse, so each cell of this matrix is a distinct past or possible
// regression. The image tag goes through DefaultLanguageForImage, the same
// call the controller makes, so the test covers the decision and the text
// together.
func TestBuildCreateDatabaseQuery_DefaultLanguage(t *testing.T) {
	c := &Client{}

	const (
		lts    = "5.26-enterprise"
		calver = "2026.08.1-enterprise"
	)

	type path struct {
		name        string
		primaries   int32
		secondaries int32
		seedURI     string
	}
	plain := path{name: "plain"}
	topology := path{name: "topology", primaries: 3, secondaries: 1}
	seed := path{name: "seed", seedURI: "s3://bucket/backups/"}
	seedTopology := path{name: "seed+topology", primaries: 1, secondaries: 2, seedURI: "s3://bucket/backups/"}

	build := func(p path, tag, requested string) string {
		q, _ := c.buildCreateDatabaseQuery(createDatabaseStatement{
			name:          "movies",
			ifNotExists:   true,
			cypherVersion: DefaultLanguageForImage(tag, requested),
			primaries:     p.primaries,
			secondaries:   p.secondaries,
			seedURI:       p.seedURI,
			wait:          true,
		})
		return q
	}

	t.Run("5.26 LTS never emits the clause", func(t *testing.T) {
		for _, lang := range []string{"", "5", "25"} {
			for _, p := range []path{plain, topology, seed, seedTopology} {
				assert.NotContains(t, build(p, lts, lang), "LANGUAGE",
					"path %s with defaultCypherLanguage %q on %s", p.name, lang, lts)
			}
		}
	})

	t.Run("CalVer emits the requested language on every path", func(t *testing.T) {
		for _, lang := range []string{"5", "25"} {
			for _, p := range []path{plain, topology, seed, seedTopology} {
				assert.Contains(t, build(p, calver, lang), " DEFAULT LANGUAGE CYPHER "+lang+" ",
					"path %s with defaultCypherLanguage %q on %s", p.name, lang, calver)
			}
		}
	})

	t.Run("CalVer with nothing requested emits no clause", func(t *testing.T) {
		for _, p := range []path{plain, topology, seed, seedTopology} {
			assert.NotContains(t, build(p, calver, ""), "LANGUAGE", "path %s", p.name)
		}
	})

	t.Run("exact statements", func(t *testing.T) {
		assert.Equal(t,
			"CREATE DATABASE `movies` IF NOT EXISTS DEFAULT LANGUAGE CYPHER 25 WAIT",
			build(plain, calver, "25"))
		assert.Equal(t,
			"CREATE DATABASE `movies` IF NOT EXISTS WAIT",
			build(plain, lts, "25"))
		assert.Equal(t,
			"CREATE DATABASE `movies` IF NOT EXISTS DEFAULT LANGUAGE CYPHER 5 TOPOLOGY 3 PRIMARIES 1 SECONDARY WAIT",
			build(topology, calver, "5"))
		assert.Equal(t,
			"CREATE DATABASE `movies` IF NOT EXISTS TOPOLOGY 3 PRIMARIES 1 SECONDARY WAIT",
			build(topology, lts, "5"))
	})

	t.Run("seed statements carry the seed as a parameter after the language", func(t *testing.T) {
		q, params := c.buildCreateDatabaseQuery(createDatabaseStatement{
			name:          "movies",
			cypherVersion: DefaultLanguageForImage(calver, "25"),
			primaries:     1,
			secondaries:   0,
			seedURI:       "s3://bucket/backups/",
			wait:          false,
		})
		assert.Equal(t,
			"CREATE DATABASE `movies` DEFAULT LANGUAGE CYPHER 25 TOPOLOGY 1 PRIMARY OPTIONS {seedURI: $opt_seedURI} NOWAIT", q)
		assert.Equal(t, "s3://bucket/backups/", params["opt_seedURI"])
	})
}

// TestBuildCreateDatabaseQuery_UnexpectedLanguageIsNotInterpolated keeps the
// long-standing guarantee that only "5" and "25" can ever reach the statement
// text, now that all four create paths share one builder.
func TestBuildCreateDatabaseQuery_UnexpectedLanguageIsNotInterpolated(t *testing.T) {
	c := &Client{}
	q, _ := c.buildCreateDatabaseQuery(createDatabaseStatement{
		name:          "movies",
		cypherVersion: "25; DROP DATABASE neo4j",
		wait:          true,
	})
	assert.Equal(t, "CREATE DATABASE `movies` WAIT", q)
}
