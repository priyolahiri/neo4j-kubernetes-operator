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

package resources

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The design matrix (docs/design/cypher-language-defaulting.md §5.4), plus the
// rows it depends on: existing deployments record what they already run.
func TestStampServerCypherLanguage(t *testing.T) {
	tests := []struct {
		name string
		in   ServerCypherLanguageInputs
		want string
	}{
		{"new CalVer deployment, unset", ServerCypherLanguageInputs{CalVer: true}, CypherLanguage25},
		{"new LTS deployment, unset", ServerCypherLanguageInputs{}, CypherLanguage5},
		{"existing CalVer deployment, never stamped, nothing set",
			ServerCypherLanguageInputs{CalVer: true, Exists: true}, CypherLanguage5},
		{"existing sharding cluster: its ConfigMap carries CYPHER_25 from when sharding forced it",
			ServerCypherLanguageInputs{CalVer: true, Exists: true, Running: CypherLanguage25}, CypherLanguage25},
		{"stamp lost to an etcd restore: re-derived from the running ConfigMap",
			ServerCypherLanguageInputs{CalVer: true, Exists: true, Running: CypherLanguage25}, CypherLanguage25},
		{"stamped: never re-derived, even for a CalVer deployment with nothing running",
			ServerCypherLanguageInputs{CalVer: true, Exists: true, Stamped: CypherLanguage5}, CypherLanguage5},
		{"5.26-born deployment upgraded to CalVer keeps CYPHER_5 (§5.7)",
			ServerCypherLanguageInputs{CalVer: true, Exists: true, Stamped: CypherLanguage5}, CypherLanguage5},
		{"explicit spec wins over the stamp",
			ServerCypherLanguageInputs{Spec: CypherLanguage25, Stamped: CypherLanguage5, CalVer: true, Exists: true}, CypherLanguage25},
		{"legacy spec.config key is the user's explicit choice",
			ServerCypherLanguageInputs{Legacy: CypherLanguage5, CalVer: true}, CypherLanguage5},
		{"garbage in the running ConfigMap is not trusted",
			ServerCypherLanguageInputs{CalVer: true, Exists: true, Running: "CYPHER_4"}, CypherLanguage5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, StampServerCypherLanguage(tt.in))
		})
	}
}

func TestEmitServerCypherLanguage(t *testing.T) {
	tests := []struct {
		name                  string
		spec, legacy, stamped string
		calver                bool
		want                  string
	}{
		{"LTS: never, even when asked", CypherLanguage5, "", CypherLanguage5, false, ""},
		{"LTS stamped", "", "", CypherLanguage5, false, ""},
		{"CalVer, stamped 25", "", "", CypherLanguage25, true, CypherLanguage25},
		{"CalVer, stamped 5: the server default, so nothing — keeps neo4j.conf unchanged", "", "", CypherLanguage5, true, ""},
		{"CalVer, explicit CYPHER_5 is written", CypherLanguage5, "", CypherLanguage5, true, CypherLanguage5},
		{"CalVer, explicit CYPHER_25", CypherLanguage25, "", CypherLanguage25, true, CypherLanguage25},
		{"legacy key: its own config path writes it", "", CypherLanguage25, CypherLanguage25, true, ""},
		{"unstamped (never happens after a stamp) writes nothing", "", "", "", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EmitServerCypherLanguage(tt.spec, tt.legacy, tt.stamped, tt.calver))
		})
	}
}

func languageLines(conf string) []string {
	var out []string
	for _, l := range strings.Split(conf, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), ServerCypherLanguageKey+"=") {
			out = append(out, strings.TrimSpace(l))
		}
	}
	return out
}

func languageCluster(tag string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	c.Name = "c"
	c.Namespace = "ns"
	c.Spec.Image = neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag}
	c.Spec.Topology.Servers = 3
	return c
}

func TestClusterConfWritesTheStampedLanguage(t *testing.T) {
	c := languageCluster("2026.08.1-enterprise")
	c.Status.EffectiveCypherLanguage = CypherLanguage25
	assert.Equal(t, []string{"db.query.default_language=CYPHER_25"}, languageLines(buildNeo4jConfigForEnterprise(c)))

	c.Status.EffectiveCypherLanguage = CypherLanguage5
	assert.Empty(t, languageLines(buildNeo4jConfigForEnterprise(c)), "CYPHER_5 is the CalVer default")

	lts := languageCluster("5.26-enterprise")
	lts.Status.EffectiveCypherLanguage = CypherLanguage5
	assert.Empty(t, languageLines(buildNeo4jConfigForEnterprise(lts)), "the setting does not exist on the LTS")
}

// Sharding no longer forces the server language: shard sub-databases inherit
// the parent's (measured on 2026.06.0), and the sharded CREATE sets it.
func TestPropertyShardingNoLongerForcesTheServerLanguage(t *testing.T) {
	c := languageCluster("2026.06-enterprise")
	c.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{Enabled: true}
	c.Status.EffectiveCypherLanguage = CypherLanguage5
	assert.NotContains(t, buildPropertyShardingConfig(c), ServerCypherLanguageKey)
	assert.Empty(t, languageLines(buildNeo4jConfigForEnterprise(c)))

	// A pre-existing sharding cluster is stamped CYPHER_25 from its ConfigMap,
	// so it keeps writing exactly what it wrote before.
	c.Status.EffectiveCypherLanguage = CypherLanguage25
	assert.Equal(t, []string{"db.query.default_language=CYPHER_25"}, languageLines(buildNeo4jConfigForEnterprise(c)))
}

// A user who set the key directly keeps exactly one line: theirs.
func TestLegacyLanguageKeyIsWrittenOnce(t *testing.T) {
	c := languageCluster("2026.08.1-enterprise")
	c.Spec.Config = map[string]string{ServerCypherLanguageKey: CypherLanguage5}
	c.Status.EffectiveCypherLanguage = CypherLanguage5
	assert.Equal(t, []string{"db.query.default_language=CYPHER_5"}, languageLines(buildNeo4jConfigForEnterprise(c)))

	s := languageCluster("2026.08.1-enterprise")
	s.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{Enabled: true,
		Config: map[string]string{ServerCypherLanguageKey: CypherLanguage25}}
	s.Status.EffectiveCypherLanguage = CypherLanguage25
	assert.Equal(t, []string{"db.query.default_language=CYPHER_25"}, languageLines(buildNeo4jConfigForEnterprise(s)))
}
