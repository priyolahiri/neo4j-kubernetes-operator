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
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

// The image gives each plugin it installs an unrestricted entry (apoc.*,
// gds.*, genai.*, …), but only to a neo4j.conf without the list, and a
// NEO4J_dbms_security_procedures_unrestricted variable replaces it: a cluster
// with Aura Fleet Management's line, or a plugin that sets the list, lost the
// entries of every other plugin (operations.md id 193). The block adds them
// back, from the image's own table, unless the cluster sets the list itself —
// its spec.config, or its spec.env's variable, which the entrypoint writes in
// place of the whole line; an explicit setting wins. The allowlist gets none
// (id 192).
func TestServerStart_KeepsThePluginsUnrestrictedDefaults(t *testing.T) {
	const fleet = "dbms.security.procedures.unrestricted=fleetManagement.*\n"
	r5 := entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}
	r9 := entrypointsim.Image{Release: entrypointsim.Release2026090}
	r10 := entrypointsim.Image{Release: entrypointsim.Release2026100}
	for _, tc := range []struct {
		name      string
		image     entrypointsim.Image
		config    map[string]string // the cluster's spec.config
		specEnv   []corev1.EnvVar   // the cluster's spec.env
		conf      string
		env       map[string]string
		want      string // the unrestricted list the server starts with
		wantImage string // what the image alone (no block) starts it with
	}{
		{"Aura Fleet Management and a Managed APOC", r5, nil, nil, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`},
			"fleetManagement.*,apoc.*", "fleetManagement.*"},
		{"GDS and APOC: GDS's list is a variable", r5, nil, nil, "",
			map[string]string{"NEO4J_PLUGINS": `["apoc","graph-data-science"]`, unrestrictedEnv: "gds.*,apoc.load.*"},
			"gds.*,apoc.load.*,apoc.*", "gds.*,apoc.load.*"},
		{"GenAI with spec.security, 2026.10: genai.* and ai.*", r10, nil, nil, "",
			map[string]string{"NEO4J_PLUGINS": `["genai"]`, allowlistEnv: "genai.vector.*", unrestrictedEnv: "genai.vector.*"},
			"genai.vector.*,genai.*,ai.*", "genai.vector.*"},
		{"GenAI with spec.security, 2026.09: genai.*", r9, nil, nil, "",
			map[string]string{"NEO4J_PLUGINS": `["genai"]`, allowlistEnv: "genai.vector.*", unrestrictedEnv: "genai.vector.*"},
			"genai.vector.*,genai.*", "genai.vector.*"},
		{"Bloom and APOC: Bloom's list is a variable", r5, nil, nil, "",
			map[string]string{"NEO4J_PLUGINS": `["bloom","apoc"]`, unrestrictedEnv: "bloom.*"},
			"bloom.*,apoc.*", "bloom.*"},
		{"the cluster's spec.config sets the list: it is left as it is", r5,
			map[string]string{UnrestrictedSetting: "custom.*"}, nil, "dbms.security.procedures.unrestricted=custom.*,fleetManagement.*\n",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`},
			"custom.*,fleetManagement.*", "custom.*,fleetManagement.*"},
		{"and with a plugin's variable, the two together and no more", r5,
			map[string]string{UnrestrictedSetting: "custom.*"}, nil, "dbms.security.procedures.unrestricted=custom.*\n",
			map[string]string{"NEO4J_PLUGINS": `["apoc","graph-data-science"]`, unrestrictedEnv: "gds.*"},
			"custom.*,gds.*", "gds.*"},
		{"the cluster's spec.env sets the list: Aura Fleet Management's line joins it, no image entries", r5,
			nil, []corev1.EnvVar{{Name: unrestrictedEnv, Value: "custom.*"}}, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`, unrestrictedEnv: "custom.*"},
			"fleetManagement.*,custom.*", "custom.*"},
		{"and a plugin's tokens in the variable beside it", r5,
			nil, []corev1.EnvVar{{Name: unrestrictedEnv, Value: "custom.*"}}, "",
			map[string]string{"NEO4J_PLUGINS": `["apoc","graph-data-science"]`, unrestrictedEnv: "custom.*,gds.*,apoc.load.*"},
			"custom.*,gds.*,apoc.load.*", "custom.*,gds.*,apoc.load.*"},
		{"set nowhere: the image's own entries, untouched", r5, nil, nil, "",
			map[string]string{"NEO4J_PLUGINS": `["apoc","graph-data-science"]`},
			"gds.*,apoc.*", "gds.*,apoc.*"},
		{"2026.09: fleet-management left in NEO4J_PLUGINS installs nothing, and adds nothing", r9, nil, nil, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`},
			"fleetManagement.*,apoc.*", "fleetManagement.*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := entrypointsim.Start(t, tc.image, tc.conf, buildPluginListMergeBlock(tc.config, tc.specEnv), tc.env)
			if got, lines := entrypointsim.Setting(conf, UnrestrictedSetting); lines != 1 || got != tc.want {
				t.Errorf("unrestricted %q (%d lines), want %q\n%s", got, lines, tc.want, conf)
			}
			if allow, ok := tc.env[allowlistEnv]; ok {
				if got, _ := entrypointsim.Setting(conf, AllowlistSetting); got != allow {
					t.Errorf("allowlist %q, want the plugin's %q: no plugin defaults there", got, allow)
				}
			}
			// The premise: the image alone loses them.
			image := entrypointsim.Start(t, tc.image, tc.conf, "", tc.env)
			if got, _ := entrypointsim.Setting(image, UnrestrictedSetting); got != tc.wantImage {
				t.Errorf("the image alone: unrestricted %q, want %q", got, tc.wantImage)
			}
		})
	}
}
