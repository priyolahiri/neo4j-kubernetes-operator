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
	"sort"
	"strings"
	"testing"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

const (
	allowlistEnv    = "NEO4J_dbms_security_procedures_allowlist"
	unrestrictedEnv = "NEO4J_dbms_security_procedures_unrestricted"
)

var (
	image526  = entrypointsim.Image{FleetJAR: true}                                         // 5.26.32: products/ has the fleet JAR
	image2026 = entrypointsim.Image{Release: entrypointsim.Release2026090, FleetJAR: false} // 2026.09.0: built in, no JAR
)

func sortedTokens(list string) string {
	var out []string
	for _, tok := range strings.Split(list, ",") {
		if tok = strings.TrimSpace(tok); tok != "" {
			out = append(out, tok)
		}
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// The image's own narrowing (operations.md id 192): with fleet-management in
// NEO4J_PLUGINS, the entrypoint's apply_plugin_default_configuration adds
// dbms.security.procedures.allowlist=fleetManagement.* to a neo4j.conf
// without the setting — on every 5.26.12+ and 2025.08–2026.02 server — and
// nothing else loads. The startup block exports Neo4j's default, *, which the
// environment pass, after it, puts in its place. Where something sets the
// allowlist the block adds fleet's entry; where the image has no fleet JAR
// (2026.03+) the entrypoint applies no default, and * is the default anyway.
func TestServerStart_TheEntrypointsFleetDefault(t *testing.T) {
	block := buildPluginListMergeBlock(nil, nil)
	fleet := "dbms.security.procedures.unrestricted=fleetManagement.*\n"
	for _, tc := range []struct {
		name          string
		image         entrypointsim.Image
		conf          string
		env           map[string]string
		wantAllowlist string // "" for no allowlist line
		wantUnres     string
	}{
		{"5.26: Aura Fleet Management alone", image526, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`}, "*", "fleetManagement.*"},
		{"5.26: Aura Fleet Management and a Managed APOC", image526, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`}, "*", "fleetManagement.*,apoc.*"},
		{"5.26: and a plugin's allowlist", image526, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`, allowlistEnv: "apoc.*", unrestrictedEnv: "apoc.*"},
			"apoc.*,fleetManagement.*", "apoc.*,fleetManagement.*"},
		{"5.26: and the cluster's own allowlist", image526, fleet + "dbms.security.procedures.allowlist=custom.*,fleetManagement.*\n",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`}, "custom.*,fleetManagement.*", "fleetManagement.*"},
		{"5.26: a fleet-management Neo4jPlugin's own variable", image526, "",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`, unrestrictedEnv: "fleetManagement.*"}, "*", "fleetManagement.*"},
		{"5.26: an empty allowlist of the user's stays", image526, fleet + "dbms.security.procedures.allowlist=\n",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`}, "-", "fleetManagement.*"},
		{"2026.09: fleet-management left in NEO4J_PLUGINS by an earlier operator", image2026, fleet,
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`}, "*", "fleetManagement.*"},
		{"no fleet: APOC alone keeps Neo4j's default", image526, "",
			map[string]string{"NEO4J_PLUGINS": `["apoc"]`}, "", "apoc.*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := entrypointsim.Start(t, tc.image, tc.conf, block, tc.env)
			allow, lines := entrypointsim.Setting(conf, "dbms.security.procedures.allowlist")
			switch tc.wantAllowlist {
			case "":
				if lines != 0 {
					t.Errorf("allowlist %q, want none\n%s", allow, conf)
				}
			case "-":
				if lines != 1 || allow != "" {
					t.Errorf("allowlist %q (%d lines), want the user's empty one\n%s", allow, lines, conf)
				}
			default:
				if lines != 1 || sortedTokens(allow) != sortedTokens(tc.wantAllowlist) {
					t.Errorf("allowlist %q (%d lines), want %q\n%s", allow, lines, tc.wantAllowlist, conf)
				}
			}
			if unres, _ := entrypointsim.Setting(conf, "dbms.security.procedures.unrestricted"); sortedTokens(unres) != sortedTokens(tc.wantUnres) {
				t.Errorf("unrestricted %q, want %q", unres, tc.wantUnres)
			}
		})
	}

	// The premise: without the block, the image narrows the list itself.
	conf := entrypointsim.Start(t, image526, fleet, "", map[string]string{"NEO4J_PLUGINS": `["fleet-management","apoc"]`})
	if allow, _ := entrypointsim.Setting(conf, "dbms.security.procedures.allowlist"); allow != "fleetManagement.*" {
		t.Errorf("the entrypoint's own default: allowlist %q, want fleetManagement.*", allow)
	}
}
