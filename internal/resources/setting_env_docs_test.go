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
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publishedDocRoots are the docs and examples users copy from.
var publishedDocRoots = []string{"../../docs", "../../examples", "../../charts", "../../config/samples", "../../README.md"}

// eachPublishedDocLine calls check with every line of the published docs and
// examples (Markdown, YAML and text), and its place as path:line.
func eachPublishedDocLine(t *testing.T, check func(place, line string)) {
	t.Helper()
	for _, root := range publishedDocRoots {
		require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			switch filepath.Ext(path) {
			case ".md", ".yaml", ".yml", ".txt":
			default:
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for i, line := range strings.Split(string(data), "\n") {
				check(strings.TrimPrefix(path, "../../")+":"+strconv.Itoa(i+1), line)
			}
			return nil
		}))
	}
}

// upperCasedSettingVar matches a NEO4J_ variable named after a setting of one
// of Neo4j's namespaces in upper case. The image keeps the case of the name
// (NEO4J_apoc_export_file_enabled sets apoc.export.file.enabled), so an
// upper-cased one sets APOC.EXPORT.FILE.ENABLED, which nothing reads. A
// wildcard (NEO4J_APOC_*) counts.
var upperCasedSettingVar = regexp.MustCompile(`NEO4J_(APOC|DBMS|SERVER|DB|GDS|GENAI|BLOOM|N10S|INITIAL|BROWSER|INTERNAL)_([A-Z0-9_]+|\*)`)

// The plugin examples told users to look for NEO4J_APOC_EXPORT_FILE_ENABLED
// and NEO4J_DBMS_SECURITY_PROCEDURES_UNRESTRICTED on the StatefulSet, where
// the operator writes NEO4J_apoc_export_file_enabled and
// NEO4J_dbms_security_procedures_unrestricted (Neo4jSettingEnvVarName): the
// jsonpath checks they gave printed nothing. The published docs and examples
// name these variables as the operator writes them.
func TestPublishedDocsNameSettingVariablesAsTheImageReadsThem(t *testing.T) {
	assert.Equal(t, "NEO4J_apoc_export_file_enabled", Neo4jSettingEnvVarName("apoc.export.file.enabled"))
	assert.Empty(t, upperCasedSettingVar.FindString(Neo4jSettingEnvVarName("dbms.security.procedures.unrestricted")))

	eachPublishedDocLine(t, func(place, line string) {
		if name := upperCasedSettingVar.FindString(line); name != "" {
			t.Errorf("%s names %s; the Neo4j image keeps the case of a setting's variable, so write it as the operator does (Neo4jSettingEnvVarName)",
				place, name)
		}
	})
}

// deprecated4xSettings are the 4.x settings the project lists under "Never
// use": a setting, or a family by its prefix (ending in '.').
var deprecated4xSettings = []string{"dbms.mode", "causal_clustering.", "dbms.connector.", "dbms.jvm.additional", "metrics.bolt."}

// deprecated4xUse matches a line that sets one of deprecated4xSettings: a
// YAML key or a neo4j.conf line that opens with the setting (quoted or not,
// after an optional list dash), or its NEO4J_ variable anywhere. A line that
// only names one — a migration table, "not dbms.connector.*" — is not a use.
func deprecated4xUse() *regexp.Regexp {
	var keys, vars []string
	for _, s := range deprecated4xSettings {
		key, env := regexp.QuoteMeta(s), regexp.QuoteMeta(Neo4jSettingEnvVarName(s))
		if strings.HasSuffix(s, ".") {
			key += `[A-Za-z0-9_.]+`
			env += `[A-Za-z0-9_]+`
		}
		keys = append(keys, key)
		vars = append(vars, env)
	}
	return regexp.MustCompile(`^\s*(?:-\s+)?["']?(` + strings.Join(keys, "|") + `)["']?\s*[:=]` +
		`|\b(` + strings.Join(vars, "|") + `)\b`)
}

// The standalone API reference's spec.env example set NEO4J_dbms_jvm_additional,
// the 4.x name of server.jvm.additional. The published docs and examples set
// none of the 4.x settings CLAUDE.md lists under "Never use".
func TestPublishedDocsSetNoDeprecated4xSettings(t *testing.T) {
	use := deprecated4xUse()
	for _, line := range []string{
		"  - name: NEO4J_dbms_jvm_additional",
		"    dbms.mode: SINGLE",
		`    "causal_clustering.minimum_core_cluster_size_at_formation": "3"`,
		"dbms.connector.bolt.enabled=true",
		"  NEO4J_metrics_bolt_messages_enabled: \"true\"",
	} {
		assert.NotEmpty(t, use.FindString(line), "a use: %s", line)
	}
	for _, line := range []string{
		"  - name: NEO4J_server_jvm_additional",
		"| `dbms.mode=SINGLE` | Removed in 5.x | (no replacement) |",
		"- **TLS/SSL**: `server.https.*` and `server.bolt.*` (not `dbms.connector.*`)",
		"    server.jvm.additional: -XX:+ExitOnOutOfMemoryError",
	} {
		assert.Empty(t, use.FindString(line), "not a use: %s", line)
	}

	eachPublishedDocLine(t, func(place, line string) {
		if m := use.FindString(line); m != "" {
			t.Errorf("%s sets %s, a 4.x setting Neo4j 5.26+ replaced; use its 5.x name (CLAUDE.md, \"Never use\")",
				place, strings.TrimSpace(m))
		}
	})
}
