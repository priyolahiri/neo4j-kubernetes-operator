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
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func envConfCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "envconf", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 2},
		},
	}
}

// The Neo4j entrypoint converts every NEO4J_<name> variable that is not on its
// own control-variable allowlist into a neo4j.conf setting, and writes the
// result into ${NEO4J_HOME}/conf — the file the server reads.
//
// So a NEO4J_-prefixed variable that is NOT a Neo4j setting becomes an
// undeclared setting, and the operator's strict config validation refuses to
// start the server. Both of these did exactly that:
//
//	Unrecognized setting. No declared setting with name: UDC.PACKAGING
//	Unrecognized setting. No declared setting with name: SERVER.NAME
//
// Anything the operator wants to pass to its own startup script must therefore
// avoid the NEO4J_ prefix. ACCEPT_LICENSE_AGREEMENT and EDITION are exempt
// because the entrypoint lists them explicitly.
func TestClusterEnvVars_NoNonSettingNeo4jPrefixedVars(t *testing.T) {
	// The entrypoint's own not_configs allowlist.
	exempt := map[string]bool{
		"NEO4J_ACCEPT_LICENSE_AGREEMENT": true, "NEO4J_AUTH": true,
		"NEO4J_AUTH_PATH": true, "NEO4J_DEBUG": true, "NEO4J_EDITION": true,
		"NEO4J_HOME": true, "NEO4J_PLUGINS": true, "NEO4J_SHA256": true,
		"NEO4J_TARBALL": true, "NEO4J_DEPRECATION_WARNING": true,
	}

	spec := BuildPodSpecForEnterprise(envConfCluster(), "server", "neo4j-admin-secret")
	require.NotEmpty(t, spec.Containers)

	for _, e := range spec.Containers[0].Env {
		if !strings.HasPrefix(e.Name, "NEO4J_") || exempt[e.Name] {
			continue
		}
		suffix := strings.TrimPrefix(e.Name, "NEO4J_")
		// A real Neo4j setting is lower-case; an upper-case suffix means the
		// variable is a control value that would translate to garbage.
		assert.NotEqual(t, strings.ToUpper(suffix), suffix,
			"%s is not a Neo4j setting, so the entrypoint would turn it into the "+
				"undeclared setting %q and strict config validation would refuse to "+
				"start the server. Use an OPERATOR_ prefix instead",
			e.Name, strings.ReplaceAll(suffix, "_", "."))
	}
}

// NEO4J_CONF must stay unset. Setting it points the server away from the
// configuration the entrypoint assembles in ${NEO4J_HOME}/conf — which is
// where it writes every env-var-derived setting — and those are then silently
// discarded, LDAP system-account credentials included.
func TestClusterEnvVars_NoConfOverride(t *testing.T) {
	spec := BuildPodSpecForEnterprise(envConfCluster(), "server", "neo4j-admin-secret")
	require.NotEmpty(t, spec.Containers)
	for _, e := range spec.Containers[0].Env {
		assert.NotEqual(t, "NEO4J_CONF", e.Name,
			"NEO4J_CONF must not be set: the entrypoint writes env-derived settings to "+
				"${NEO4J_HOME}/conf, so overriding it discards them all")
	}
}

// /conf has to be WRITABLE, because the startup script assembles the effective
// configuration there — the ConfigMap plus the per-pod values that can only be
// computed at pod start — and the entrypoint reads /conf as its input.
func TestClusterVolumes_ConfIsWritableAndConfigMapIsStaged(t *testing.T) {
	spec := BuildPodSpecForEnterprise(envConfCluster(), "server", "neo4j-admin-secret")
	require.NotEmpty(t, spec.Containers)

	var confMount, stagingMount string
	for _, m := range spec.Containers[0].VolumeMounts {
		switch m.MountPath {
		case "/conf":
			confMount = m.Name
		case OperatorConfStagingPath:
			stagingMount = m.Name
			assert.True(t, m.ReadOnly, "the ConfigMap staging mount should be read-only")
		}
	}
	require.Equal(t, EntrypointConfVolume, confMount, "/conf must be the writable emptyDir")
	require.Equal(t, ConfigVolume, stagingMount, "the ConfigMap belongs at the staging path")

	for _, v := range spec.Volumes {
		if v.Name == EntrypointConfVolume {
			assert.NotNil(t, v.EmptyDir,
				"/conf must be an emptyDir — the startup script writes the advertised "+
					"addresses into it, which a ConfigMap mount cannot accept")
		}
		if v.Name == ConfigVolume {
			assert.NotNil(t, v.ConfigMap, "the staged config still comes from the ConfigMap")
		}
	}
}

// The startup script is itself a ConfigMap file, so it lives at the staging
// path — /conf is empty until the script populates it. Getting this wrong
// crash-loops every pod with "No such file or directory".
func TestClusterCommand_StartupScriptComesFromTheStagingPath(t *testing.T) {
	spec := BuildPodSpecForEnterprise(envConfCluster(), "server", "neo4j-admin-secret")
	require.NotEmpty(t, spec.Containers)
	cmd := strings.Join(spec.Containers[0].Command, " ")
	assert.Contains(t, cmd, OperatorConfStagingPath+"/startup.sh")
	assert.NotContains(t, cmd, "/conf/startup.sh",
		"/conf is an empty emptyDir at container start; the script cannot live there")
}
