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

package validation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

// The table is the images' (operations.md id 193): for every vendored
// release, each plugin the image's /startup/neo4j-plugins.json gives an
// unrestricted entry has exactly that entry here — except fleet-management
// where Fleet Manager is built in, which the entrypoint cannot install — and a
// name the image does not list has none.
func TestImageUnrestrictedDefaults_MatchTheImages(t *testing.T) {
	everyName := map[string]bool{}
	for _, list := range imageUnrestrictedDefaultLists {
		for name := range list.entries {
			everyName[name] = true
		}
	}
	for _, release := range entrypointsim.Releases {
		var table map[string]struct {
			Properties map[string]string `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(entrypointsim.PluginTable(t, release), &table), release)
		tag := release + "-enterprise"
		for name := range everyName {
			entry, listed := table[name]
			got := ImageUnrestrictedDefaults(tag, []string{name})
			switch {
			case !listed:
				assert.Empty(t, got, "%s: %s is not in the image's table", release, name)
			case name == fleetManagementName && FleetManagementBuiltIn(tag):
				assert.Empty(t, got, "%s: the entrypoint cannot install fleet-management", release)
			default:
				assert.Equal(t, []string{entry.Properties["dbms.security.procedures.unrestricted"]}, got, "%s: %s", release, name)
			}
		}
		for name, entry := range table {
			if _, has := entry.Properties["dbms.security.procedures.unrestricted"]; has && name != "_testing" {
				assert.True(t, everyName[name], "%s: %s has an entry the table does not know", release, name)
			}
		}
	}
}

// In the order of NEO4J_PLUGINS, one entry per plugin it installs; a floating
// tag is its line's newest patch, and a release newer than the table its
// line's newest entry.
func TestImageUnrestrictedDefaults(t *testing.T) {
	for _, tc := range []struct {
		tag     string
		plugins []string
		want    []string
	}{
		{"2026.10.0-enterprise", []string{"genai", "apoc", "custom"}, []string{"genai.*,ai.*", "apoc.*"}},
		{"2026.09.0-enterprise", []string{"genai", "apoc"}, []string{"genai.*", "apoc.*"}},
		{"2026.11.0-enterprise", []string{"genai"}, []string{"genai.*,ai.*"}},
		{"2026.09-enterprise", []string{"genai"}, []string{"genai.*"}},
		{"2026.09.0-enterprise", []string{"fleet-management", "graph-data-science"}, []string{"gds.*"}},
		{"2026.02.2-enterprise", []string{"fleet-management"}, []string{"fleetManagement.*"}},
		{"2025.03.0-enterprise", []string{"n10s", "bloom"}, []string{"bloom.*"}},
		{"5.26.0-enterprise", []string{"fleet-management", "n10s"}, []string{"semantics.*"}},
		{"5.26-enterprise", []string{"fleet-management", "apoc", "apoc-extended"}, []string{"fleetManagement.*", "apoc.*", "apoc.*"}},
		{"latest", []string{"apoc"}, nil},
		{"4.4.30-enterprise", []string{"apoc"}, nil},
	} {
		assert.Equal(t, tc.want, ImageUnrestrictedDefaults(tc.tag, tc.plugins), "%s %v", tc.tag, tc.plugins)
	}
}
