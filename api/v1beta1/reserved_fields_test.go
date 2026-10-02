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

package v1beta1

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// reservedMarker is the sentence every reserved schema field carries in its Go
// doc comment, and therefore in the CRD description a user sees under
// `kubectl explain`.
const reservedMarker = "Reserved: accepted by the schema but never populated today"

// crdProperty walks a generated CRD's schema by property names (descending
// through array items) and returns the schema at the end of the path.
func crdProperty(t *testing.T, crdFile string, path ...string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", crdFile))
	require.NoError(t, err)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.NotEmpty(t, crd.Spec.Versions)

	cur := *crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	for _, name := range path {
		if cur.Items != nil && cur.Items.Schema != nil {
			cur = *cur.Items.Schema
		}
		next, ok := cur.Properties[name]
		require.True(t, ok, "%s has no property %q on the way to %v", crdFile, name, path)
		cur = next
	}
	return cur
}

// BackupStats.size, .throughput and .fileCount are accepted by the schema but no
// controller ever writes them (only duration is populated). Each says so in its
// description, so `kubectl explain` does not promise a number that never comes.
// The three are listed together on purpose: when one starts being populated,
// this test is where its "Reserved" note is deliberately removed.
func TestBackupStatsReservedFieldsAreMarkedInTheCRD(t *testing.T) {
	stats := crdProperty(t, "neo4j.neo4j.com_neo4jbackups.yaml", "status", "history", "stats")

	for _, name := range []string{"size", "throughput", "fileCount"} {
		prop, ok := stats.Properties[name]
		require.True(t, ok, "BackupStats has no %q property", name)
		assert.Contains(t, prop.Description, reservedMarker,
			"stats.%s is never populated and must say so", name)
	}

	duration, ok := stats.Properties["duration"]
	require.True(t, ok)
	assert.NotContains(t, duration.Description, "Reserved",
		"duration IS populated (jobToBackupRun) and must not be marked reserved")
}
