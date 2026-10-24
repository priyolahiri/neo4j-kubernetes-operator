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

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// pluginManifest is a Managed Neo4jPlugin named name for target prod.
func pluginManifest(name string) string {
	return `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata: {name: ` + name + `, namespace: neo4j}
spec:
  clusterRef: prod
  name: ` + name + `
  version: "5.26.0"
`
}

func targetCluster(tag string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}}
	c.Spec.Image.Tag = tag
	return c
}

// #563: with --connect, validate holds a Managed plugin's NEO4J_PLUGINS name to
// the image its target runs, as the operator does (operations.md id 170) —
// n10s passes against 5.26 and is refused against CalVer; graphql is refused on
// both; a standalone target is read too.
func TestValidate_Connected_ChecksAPluginAgainstItsTargetsImage(t *testing.T) {
	for _, tc := range []struct {
		name, plugin, tag string
		refused           bool
	}{
		{"n10s on 5.26", "n10s", "5.26.32-enterprise", false},
		{"n10s on a floating 5.26 tag", "n10s", "5.26-enterprise", false},
		{"n10s on CalVer", "n10s", "2026.08.1-enterprise", true},
		{"n10s on a floating CalVer tag", "n10s", "2026.08-enterprise", true},
		{"graphql on 5.26", "graphql", "5.26.32-enterprise", true},
		{"graphql on CalVer", "graphql", "2026.08.1-enterprise", true},
		{"apoc on CalVer", "apoc", "2026.08.1-enterprise", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results, err := validateSource(writeManifest(t, pluginManifest(tc.plugin)), testClient(t, targetCluster(tc.tag)), "")
			require.NoError(t, err)
			require.Len(t, results, 1)
			if !tc.refused {
				assert.Empty(t, results[0].findings, "%s", findingsText(results[0]))
				return
			}
			require.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
			assert.Equal(t, "spec.name", results[0].findings[0].path)
			assert.Contains(t, results[0].findings[0].detail, "not a plugin the neo4j:"+tc.tag+" image can install")
			assert.Contains(t, results[0].findings[0].detail, "installMode: VerifiedDownload")
		})
	}

	t.Run("a standalone target", func(t *testing.T) {
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"}}
		sa.Spec.Image.Tag = "2026.08.1-enterprise"
		results, err := validateSource(writeManifest(t, pluginManifest("n10s")), testClient(t, sa), "")
		require.NoError(t, err)
		assert.Equal(t, 1, results[0].errorCount(), "%s", findingsText(results[0]))
	})

	t.Run("a target that does not exist yet: every supported image, and said", func(t *testing.T) {
		results, err := validateSource(writeManifest(t, pluginManifest("n10s")), testClient(t), "")
		require.NoError(t, err)
		assert.Equal(t, 0, results[0].errorCount())
		require.Equal(t, 1, results[0].warningCount(), "%s", findingsText(results[0]))
		assert.Contains(t, findingsText(results[0]), "checked against every supported Neo4j image")
	})
}

// Offline the image is unknown: a name no supported image installs is a
// warning, a name one of them does (n10s, on 5.26) passes, and the summary says
// the rest needs --connect.
func TestValidate_Offline_WarnsAboutAPluginNoImageInstalls(t *testing.T) {
	results, err := validateSource(writeManifest(t, pluginManifest("graphql")), nil, "")
	require.NoError(t, err)
	assert.Equal(t, 0, results[0].errorCount())
	// Beside the compatibility matrix's advisory note, which knows no graphql.
	require.Equal(t, 2, results[0].warningCount(), "%s", findingsText(results[0]))
	assert.Contains(t, findingsText(results[0]), `spec.name "graphql" is not a plugin any supported Neo4j image can install`)

	results, err = validateSource(writeManifest(t, pluginManifest("n10s")), nil, "")
	require.NoError(t, err)
	assert.Empty(t, results[0].findings, "%s", findingsText(results[0]))

	out, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer out.Close()
	assert.Equal(t, exitOK, report(results, out, false, false, false))
	printed, err := os.ReadFile(out.Name())
	require.NoError(t, err)
	assert.Contains(t, string(printed),
		"run with --connect to also check a Managed Neo4jPlugin's NEO4J_PLUGINS names against the image its target runs")
}
