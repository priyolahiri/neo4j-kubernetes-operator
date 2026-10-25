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
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
)

// runPluginListMerge runs the block on conf with env set, from a directory
// holding files whose names a glob of the tokens would match, and returns the
// NEO4J_ variables afterwards.
func runPluginListMerge(t *testing.T, conf string, env map[string]string) map[string]string {
	t.Helper()
	root := t.TempDir()
	confPath := filepath.Join(root, "neo4j.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"apoc.coll", "apoc.load", "gds.x"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script := "set -e\n" + strings.ReplaceAll(buildPluginListMergeBlock(), "/conf/neo4j.conf", confPath) + "\nenv\n"
	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Dir = root
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("block failed: %v\n%s", err, out)
	}
	got := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok && strings.HasPrefix(k, "NEO4J_") {
			got[k] = v
		}
	}
	return got
}

// TestPluginListMergeBlock pins operations.md id 179's server side: a list a
// Neo4jPlugin sets as a NEO4J_<setting> variable is given the cluster's own
// entries from neo4j.conf, each token once, the cluster's first; a variable
// alone, or a conf line alone, is left as it is. The entrypoint then writes the
// variable in place of the conf line.
func TestPluginListMergeBlock(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	const allow, unres = "NEO4J_dbms_security_procedures_allowlist", "NEO4J_dbms_security_procedures_unrestricted"
	for _, tc := range []struct {
		name string
		conf string
		env  map[string]string
		want map[string]string
	}{
		{"the cluster's spec.config list and a plugin's",
			"server.memory.heap.max_size=1G\ndbms.security.procedures.allowlist=apoc.coll.*\n",
			map[string]string{allow: "genai.*,ai.*"},
			map[string]string{allow: "apoc.coll.*,genai.*,ai.*"}},
		{"Aura Fleet Management's entries and a plugin's, on both lists",
			"dbms.security.procedures.unrestricted=fleetManagement.*\ndbms.security.procedures.allowlist=fleetManagement.*\n",
			map[string]string{allow: "apoc.*", unres: "apoc.*"},
			map[string]string{allow: "fleetManagement.*,apoc.*", unres: "fleetManagement.*,apoc.*"}},
		{"a token in both is kept once, and spaces go",
			"dbms.security.procedures.unrestricted=gds.*, apoc.*\n",
			map[string]string{unres: " gds.*,apoc.load.*, "},
			map[string]string{unres: "gds.*,apoc.*,apoc.load.*"}},
		{"a wildcard token is neither expanded nor matched as a pattern",
			"dbms.security.procedures.allowlist=apoc.coll\n",
			map[string]string{allow: "apoc.*"},
			map[string]string{allow: "apoc.coll,apoc.*"}},
		{"no conf line: the variable is the plugins' alone",
			"server.memory.heap.max_size=1G\n",
			map[string]string{allow: "genai.*"},
			map[string]string{allow: "genai.*"}},
		{"no variable: nothing is exported, the conf line stands",
			"dbms.security.procedures.allowlist=apoc.coll.*\n",
			nil,
			map[string]string{}},
		{"a key that only starts with the setting's name is another setting",
			"dbms.security.procedures.allowlist_other=x.*\n",
			map[string]string{allow: "genai.*"},
			map[string]string{allow: "genai.*"}},
		{"Aura Fleet Management's entry joins an allowlist only a plugin sets, while NEO4J_PLUGINS installs it",
			"dbms.security.procedures.unrestricted=fleetManagement.*\n",
			map[string]string{allow: "apoc.*", unres: "apoc.*", "NEO4J_PLUGINS": `["fleet-management","apoc"]`},
			map[string]string{allow: "fleetManagement.*,apoc.*", unres: "fleetManagement.*,apoc.*", "NEO4J_PLUGINS": `["fleet-management","apoc"]`}},
		{"and an allowlist only the cluster's spec.config sets (fleet installed by a Neo4jPlugin)",
			"dbms.security.procedures.allowlist=custom.*\n",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`},
			map[string]string{allow: "custom.*,fleetManagement.*", "NEO4J_PLUGINS": `["fleet-management"]`}},
		{"an allowlist set nowhere, with fleet installed, is Neo4j's default: the entrypoint's fleet default would narrow it",
			"dbms.security.procedures.unrestricted=fleetManagement.*\n",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`},
			map[string]string{allow: "*", "NEO4J_PLUGINS": `["fleet-management"]`}},
		{"a conf that has the entry already is left as it is",
			"dbms.security.procedures.allowlist=custom.*,fleetManagement.*\n",
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`},
			map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`}},
		{"no fleet-management in NEO4J_PLUGINS: no entry",
			"server.memory.heap.max_size=1G\n",
			map[string]string{allow: "apoc.*", "NEO4J_PLUGINS": `["apoc"]`},
			map[string]string{allow: "apoc.*", "NEO4J_PLUGINS": `["apoc"]`}},
		{"an underscore setting, and a conf without a final newline",
			"dbms.security.http_auth_allowlist=/,/browser.*",
			map[string]string{"NEO4J_dbms_security_http__auth__allowlist": "/bloom.*"},
			map[string]string{"NEO4J_dbms_security_http__auth__allowlist": "/,/browser.*,/bloom.*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runPluginListMerge(t, tc.conf, tc.env)
			if len(got) != len(tc.want) {
				t.Errorf("variables %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s=%q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// The block covers every additive list (IsAdditiveConfKey), sits between the
// restart-neutral markers, and the startup script still parses.
func TestPluginListMergeBlock_InTheStartupScript(t *testing.T) {
	block := buildPluginListMergeBlock()
	keys := make([]string, 0, len(additiveConfKeys))
	for key := range additiveConfKeys {
		keys = append(keys, key)
		call := "operator_merge_list " + key + " " + Neo4jSettingEnvVarName(key)
		if key == AllowlistSetting {
			call += " fleet-management 'fleetManagement.*'"
		}
		if !strings.Contains(block, call+"\n") {
			t.Errorf("%s is not merged", key)
		}
	}
	sort.Strings(keys)
	if !strings.HasPrefix(block, RestartNeutralBegin+"\n") || !strings.HasSuffix(strings.TrimSpace(block), RestartNeutralEnd) {
		t.Error("the block must sit between the restart-neutral markers")
	}
	script := BuildConfigMapForEnterprise(systemModeCluster("2026.08.1-enterprise", 3)).Data["startup.sh"]
	merge := strings.Index(script, PluginListMergeMarker)
	if merge < 0 || merge > strings.Index(script, "exec /startup/docker-entrypoint.sh") {
		t.Fatal("the block must run before the entrypoint")
	}
	if _, err := exec.LookPath("bash"); err == nil {
		cmd := exec.CommandContext(t.Context(), "bash", "-n")
		cmd.Stdin = strings.NewReader(script)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("startup.sh does not parse: %v\n%s", err, out)
		}
	}
}

// ServerProcedureList, what the operator reads back (the ProceduresHidden
// check), says what the block and the entrypoint make of the same inputs.
func TestServerProcedureList_AgreesWithTheBlock(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	const allow = "NEO4J_dbms_security_procedures_allowlist"
	for _, tc := range []struct {
		conf    string // the allowlist line's value; "-" for none
		env     string // the variable; "-" for none
		plugins string
	}{
		{"-", "-", `["apoc"]`},
		{"-", "-", `["fleet-management"]`},
		{"apoc.*", "-", `["apoc"]`},
		{"apoc.*", "-", `["fleet-management"]`},
		{"apoc.*,fleetManagement.*", "-", `["fleet-management"]`},
		{"-", "genai.*", `["genai"]`},
		{"-", "genai.*", `["fleet-management","genai"]`},
		{"apoc.coll.*", "genai.*,ai.*", `["apoc","genai"]`},
		{"apoc.coll.*", "genai.*", `["fleet-management","genai"]`},
		{"", "-", `["fleet-management"]`},
	} {
		conf := "server.memory.heap.max_size=1G\n"
		if tc.conf != "-" {
			conf += AllowlistSetting + "=" + tc.conf + "\n"
		}
		env := map[string]string{"NEO4J_PLUGINS": tc.plugins}
		if tc.env != "-" {
			env[allow] = tc.env
		}
		// The server: the block, then the entrypoint's plugin defaults and its
		// environment pass, on an image with the fleet JAR (5.26.32).
		started := entrypointsim.Start(t, image526, conf, buildPluginListMergeBlock(), env)
		wantValue, lines := entrypointsim.Setting(started, AllowlistSetting)
		wantSet := lines > 0
		var plugins []string
		for _, name := range []string{"apoc", "genai", FleetManagementPlugin} {
			if strings.Contains(tc.plugins, `"`+name+`"`) {
				plugins = append(plugins, name)
			}
		}
		confValue := tc.conf
		if tc.conf == "-" {
			confValue = ""
		}
		envValue := tc.env
		if tc.env == "-" {
			envValue = ""
		}
		got, set := ServerProcedureList(AllowlistSetting, confValue, tc.conf != "-", envValue, tc.env != "-", plugins)
		if set != wantSet || (set && got != wantValue) {
			t.Errorf("conf %q, env %q, %s: ServerProcedureList = %q (set %v), the server starts with %q (set %v)",
				tc.conf, tc.env, tc.plugins, got, set, wantValue, wantSet)
		}
	}
}
