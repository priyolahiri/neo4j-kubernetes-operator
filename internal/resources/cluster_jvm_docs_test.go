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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A cluster's servers run the image's own JVM arguments first — the startup
// script copies them from the image's neo4j.conf, which the entrypoint
// replaces (buildImageJVMArgumentsBlock, operations.md id 212) — then the
// user's spec.config, then the operator's list (buildJVMSettings), as
// NEO4J_server_jvm_additional, which server.jvm.additional set in spec.env or
// spec.config replaces (id 208). The cluster API reference says so and lists
// every default flag, so a flag added or dropped here has to be added or
// dropped there.
func TestClusterJVMDefaultsAreDocumented(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	flags := strings.Fields(buildJVMSettings(cluster))
	require.NotEmpty(t, flags)

	data, err := os.ReadFile("../../docs/api_reference/neo4jenterprisecluster.md")
	require.NoError(t, err)
	doc := string(data)
	start := strings.Index(doc, "#### JVM arguments")
	require.GreaterOrEqual(t, start, 0, "the cluster API reference has a JVM arguments section")
	section := doc[start:]
	if end := strings.Index(section, "\n### "); end > 0 {
		section = section[:end]
	}
	for _, flag := range flags {
		assert.Contains(t, section, "`"+flag+"`", "the documented default list names %s", flag)
	}
	flat := strings.Join(strings.Fields(section), " ")
	for _, says := range []string{
		"**The Neo4j image's own JVM arguments**",
		"replaces that list, not the image's lines",
	} {
		assert.True(t, strings.Contains(flat, says), "the cluster API reference says: %s", says)
	}
	assert.False(t, strings.Contains(flat, "do not apply"), "the image's lines apply")

	// Set in spec.env, the variable is the user's alone.
	cluster.Spec.Env = []corev1.EnvVar{{Name: "NEO4J_server_jvm_additional", Value: "-XX:+UseZGC"}}
	assert.Empty(t, buildJVMSettings(cluster))
	// Set in spec.config, it takes the defaults' place.
	cluster.Spec.Env = nil
	cluster.Spec.Config = map[string]string{"server.jvm.additional": "-XX:+UseZGC"}
	assert.Equal(t, "-XX:+UseZGC", buildJVMSettings(cluster))
}
