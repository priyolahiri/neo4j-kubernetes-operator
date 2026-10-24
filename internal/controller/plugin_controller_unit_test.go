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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Unit tests using standard Go testing for unexported methods

func TestMapPluginName(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "apoc plugin",
			input:    "apoc",
			expected: "apoc",
		},
		{
			name:     "graph data science plugin",
			input:    "graph-data-science",
			expected: "graph-data-science",
		},
		{
			name:     "bloom plugin",
			input:    "bloom",
			expected: "bloom",
		},
		{
			name:     "custom plugin",
			input:    "my-custom-plugin",
			expected: "my-custom-plugin",
		},
		{name: "gds alias", input: "gds", expected: "graph-data-science"},
		{name: "neosemantics alias", input: "neosemantics", expected: "n10s"},
		// No longer mapped by a case of its own (operations.md id 170): no
		// supported image installs it, so a Managed graphql plugin is refused.
		{name: "graphql as it is", input: "graphql", expected: "graphql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.mapPluginName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddPluginToList(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	tests := []struct {
		name      string
		existing  string
		newPlugin string
		expected  string
		wantErr   bool
	}{
		{
			name:      "add to empty list",
			existing:  "[]",
			newPlugin: "apoc",
			expected:  "[\"apoc\"]",
			wantErr:   false,
		},
		{
			name:      "add to existing list",
			existing:  "[\"apoc\"]",
			newPlugin: "graph-data-science",
			expected:  "[\"apoc\",\"graph-data-science\"]",
			wantErr:   false,
		},
		{
			name:      "plugin already exists",
			existing:  "[\"apoc\",\"bloom\"]",
			newPlugin: "apoc",
			expected:  "[\"apoc\",\"bloom\"]", // Should not duplicate
			wantErr:   false,
		},
		{
			name:      "add to list with spaces",
			existing:  "[\"apoc\", \"bloom\"]",
			newPlugin: "graph-data-science",
			expected:  "[\"apoc\",\"bloom\",\"graph-data-science\"]",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := r.addPluginToList(tt.existing, tt.newPlugin)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetStatefulSetName(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	tests := []struct {
		name       string
		deployment *DeploymentInfo
		expected   string
	}{
		{
			name: "cluster deployment",
			deployment: &DeploymentInfo{
				Type: "cluster",
				Name: "my-cluster",
			},
			expected: "my-cluster-server",
		},
		{
			name: "standalone deployment",
			deployment: &DeploymentInfo{
				Type: "standalone",
				Name: "my-standalone",
			},
			expected: "my-standalone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.getStatefulSetName(tt.deployment)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetPodLabels(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	tests := []struct {
		name       string
		deployment *DeploymentInfo
		expected   map[string]string
	}{
		{
			name: "cluster deployment",
			deployment: &DeploymentInfo{
				Type: "cluster",
				Name: "my-cluster",
			},
			expected: map[string]string{
				"app.kubernetes.io/name":     "neo4j",
				"app.kubernetes.io/instance": "my-cluster",
			},
		},
		{
			name: "standalone deployment",
			deployment: &DeploymentInfo{
				Type: "standalone",
				Name: "my-standalone",
			},
			expected: map[string]string{
				"app": "my-standalone",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.getPodLabels(tt.deployment)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetExpectedReplicas(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	tests := []struct {
		name       string
		deployment *DeploymentInfo
		expected   int
	}{
		{
			name: "cluster with 3 servers",
			deployment: &DeploymentInfo{
				Type: "cluster",
				Object: &neo4jv1beta1.Neo4jEnterpriseCluster{
					Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
						AcceptLicenseAgreement: "eval",
						Topology: neo4jv1beta1.TopologyConfiguration{
							Servers: 3,
						},
					},
				},
			},
			expected: 3,
		},
		{
			name: "cluster with 5 servers",
			deployment: &DeploymentInfo{
				Type: "cluster",
				Object: &neo4jv1beta1.Neo4jEnterpriseCluster{
					Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
						AcceptLicenseAgreement: "eval",
						Topology: neo4jv1beta1.TopologyConfiguration{
							Servers: 5,
						},
					},
				},
			},
			expected: 5,
		},
		{
			name: "standalone always 1",
			deployment: &DeploymentInfo{
				Type:   "standalone",
				Object: &neo4jv1beta1.Neo4jEnterpriseStandalone{},
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.getExpectedReplicas(tt.deployment)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMergeNeo4jPluginList tests the exported package-level function used by both the
// plugin controller and the fleet management reconciler to safely merge plugin names into
// the NEO4J_PLUGINS JSON array without overwriting each other's entries.
func TestMergeNeo4jPluginList(t *testing.T) {
	tests := []struct {
		name      string
		existing  string
		newPlugin string
		expected  string
		wantErr   bool
	}{
		{
			name:      "add fleet-management to empty list",
			existing:  "[]",
			newPlugin: "fleet-management",
			expected:  `["fleet-management"]`,
		},
		{
			name:      "add fleet-management when apoc already present",
			existing:  `["apoc"]`,
			newPlugin: "fleet-management",
			expected:  `["apoc","fleet-management"]`,
		},
		{
			name:      "add apoc when fleet-management already present",
			existing:  `["fleet-management"]`,
			newPlugin: "apoc",
			expected:  `["fleet-management","apoc"]`,
		},
		{
			name:      "idempotent — fleet-management already in list",
			existing:  `["apoc","fleet-management"]`,
			newPlugin: "fleet-management",
			expected:  `["apoc","fleet-management"]`,
		},
		{
			name:      "add to multi-plugin list",
			existing:  `["apoc","graph-data-science"]`,
			newPlugin: "fleet-management",
			expected:  `["apoc","graph-data-science","fleet-management"]`,
		},
		{
			name:      "handles spaces in existing list",
			existing:  `["apoc", "bloom"]`,
			newPlugin: "fleet-management",
			expected:  `["apoc","bloom","fleet-management"]`,
		},
		{
			name:      "empty string is treated as empty list",
			existing:  "",
			newPlugin: "apoc",
			expected:  `["apoc"]`,
		},
		{
			name:      "whitespace-only is treated as empty list",
			existing:  "   ",
			newPlugin: "apoc",
			expected:  `["apoc"]`,
		},
		{
			name:      "rejects non-JSON garbage (was silently parsed as a fake plugin previously)",
			existing:  "not a json array",
			newPlugin: "apoc",
			wantErr:   true,
		},
		{
			name:      "rejects malformed JSON",
			existing:  `["apoc",`,
			newPlugin: "fleet-management",
			wantErr:   true,
		},
		{
			name:      "rejects non-string element in JSON array",
			existing:  `["apoc", 123]`,
			newPlugin: "fleet-management",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeNeo4jPluginList(tt.existing, tt.newPlugin)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestFleetManagementPluginType verifies that "fleet-management" gets the
// correct security settings and plugin name.
func TestFleetManagementPluginType(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	t.Run("fleet-management security settings are correct", func(t *testing.T) {
		settings := r.getAutomaticSecuritySettings("fleet-management")
		assert.Equal(t, "fleetManagement.*", settings["dbms.security.procedures.unrestricted"])
		assert.Equal(t, "fleetManagement.*", settings["dbms.security.procedures.allowlist"])
	})

	t.Run("fleet-management maps to 'fleet-management' plugin name", func(t *testing.T) {
		assert.Equal(t, "fleet-management", r.mapPluginName("fleet-management"))
	})
}

func TestGetAutomaticSecuritySettings(t *testing.T) {
	r := &Neo4jPluginReconciler{}

	t.Run("bloom settings", func(t *testing.T) {
		settings := r.getAutomaticSecuritySettings("bloom")
		assert.Contains(t, settings, "dbms.security.procedures.unrestricted")
		assert.Contains(t, settings["dbms.security.procedures.unrestricted"], "bloom.*")
		assert.Contains(t, settings, "server.unmanaged_extension_classes")
	})

	t.Run("gds settings", func(t *testing.T) {
		settings := r.getAutomaticSecuritySettings("graph-data-science")
		assert.Contains(t, settings, "dbms.security.procedures.unrestricted")
		assert.Contains(t, settings["dbms.security.procedures.unrestricted"], "gds.*")
	})

	t.Run("fleet-management settings", func(t *testing.T) {
		settings := r.getAutomaticSecuritySettings("fleet-management")
		assert.Contains(t, settings, "dbms.security.procedures.unrestricted")
		assert.Contains(t, settings, "dbms.security.procedures.allowlist")
	})

	t.Run("unknown returns empty map", func(t *testing.T) {
		settings := r.getAutomaticSecuritySettings("apoc")
		assert.Empty(t, settings)
	})
}

func TestIsSecuritySetting(t *testing.T) {
	assert.True(t, pluginConfKeyIsSecurity("dbms.security.procedures.unrestricted"))
	assert.True(t, pluginConfKeyIsSecurity("dbms.security.procedures.allowlist"))
	assert.True(t, pluginConfKeyIsSecurity("server.unmanaged_extension_classes"))
	assert.True(t, pluginConfKeyIsSecurity("dbms.bloom.license_file"))
	assert.False(t, pluginConfKeyIsSecurity("server.memory.heap.max_size"))
	assert.False(t, pluginConfKeyIsSecurity("gds.maxConcurrentRequests"))
}

// TestRemoveFromNeo4jPluginList covers the inverse of MergeNeo4jPluginList,
// which is what actually uninstalls a Managed plugin.
//
// /plugins is a per-pod EmptyDir the Neo4j entrypoint repopulates from
// NEO4J_PLUGINS on every container start, so the env var is the only durable
// lever. The uninstall path used to run a `rm -f /plugins/*.jar` Job against a
// fresh EmptyDir of its own — deleting nothing — while leaving the plugin name in
// NEO4J_PLUGINS, so the next restart reinstalled it. That same Job also wedged
// deletion: it outlived its reconcile, and every later pass re-issued the Create
// and failed "already exists", so the finalizer never released.
func TestRemoveFromNeo4jPluginList(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		remove   string
		expected string
		wantErr  bool
	}{
		{
			name:     "remove the only plugin leaves an explicit empty array",
			existing: `["apoc"]`,
			remove:   "apoc",
			expected: `[]`,
		},
		{
			name:     "remove one, keep the others in order",
			existing: `["apoc","graph-data-science","bloom"]`,
			remove:   "graph-data-science",
			expected: `["apoc","bloom"]`,
		},
		{
			name:     "does not disturb a plugin another controller owns",
			existing: `["apoc","fleet-management"]`,
			remove:   "apoc",
			expected: `["fleet-management"]`,
		},
		{
			name:     "absent plugin returns the input unchanged (caller skips the update)",
			existing: `["apoc","bloom"]`,
			remove:   "graph-data-science",
			expected: `["apoc","bloom"]`,
		},
		{
			name:     "empty string stays empty rather than becoming []",
			existing: "",
			remove:   "apoc",
			expected: "",
		},
		{
			name:     "handles spaces in the existing list",
			existing: `["apoc", "bloom"]`,
			remove:   "bloom",
			expected: `["apoc"]`,
		},
		{
			name:     "malformed JSON is an error, not a silent wipe",
			existing: `apoc,bloom`,
			remove:   "apoc",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RemoveFromNeo4jPluginList(tt.existing, tt.remove)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestRemoveFromNeo4jPluginListRoundTripsWithMerge pins the two helpers together:
// installing then uninstalling a plugin must leave the list exactly as it was, or
// repeated install/uninstall cycles would drift and restart the pods each time.
func TestRemoveFromNeo4jPluginListRoundTripsWithMerge(t *testing.T) {
	const start = `["apoc","fleet-management"]`

	merged, err := MergeNeo4jPluginList(start, "graph-data-science")
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	back, err := RemoveFromNeo4jPluginList(merged, "graph-data-science")
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if back != start {
		t.Errorf("install/uninstall did not round-trip: got %q, want %q", back, start)
	}
}
