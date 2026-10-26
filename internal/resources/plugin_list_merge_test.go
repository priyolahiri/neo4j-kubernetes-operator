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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

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
	script := "set -e\n" + strings.ReplaceAll(buildPluginListMergeBlock(nil, nil), "/conf/neo4j.conf", confPath) + "\nenv\n"
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
// restart-neutral markers, and the startup script still parses. The
// unrestricted list gets the image's plugin entries unless the cluster sets
// it itself, in its spec.config or its spec.env (operations.md id 193).
func TestPluginListMergeBlock_InTheStartupScript(t *testing.T) {
	fromSecret := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}
	for _, tc := range []struct {
		config   map[string]string
		env      []corev1.EnvVar
		defaults bool
	}{
		{nil, nil, true},
		{map[string]string{AllowlistSetting: "apoc.*"}, nil, true},
		{map[string]string{UnrestrictedSetting: "custom.*"}, nil, false},
		{map[string]string{UnrestrictedSetting: ""}, nil, false},
		{nil, []corev1.EnvVar{{Name: unrestrictedEnv, Value: "custom.*"}}, false},
		{nil, []corev1.EnvVar{{Name: unrestrictedEnv, ValueFrom: fromSecret}}, false},
		{nil, []corev1.EnvVar{{Name: unrestrictedEnv, Value: " "}}, true},
		{nil, []corev1.EnvVar{{Name: allowlistEnv, Value: "apoc.*"}}, true},
	} {
		block := buildPluginListMergeBlock(tc.config, tc.env)
		for key := range additiveConfKeys {
			call := "operator_merge_list " + key + " " + Neo4jSettingEnvVarName(key)
			switch {
			case key == AllowlistSetting:
				call += " fleet-management 'fleetManagement.*'"
			case key == UnrestrictedSetting && tc.defaults:
				call += " '' '' plugin-defaults"
			}
			if !strings.Contains(block, call+"\n") {
				t.Errorf("config %v, env %v: %s is not merged as %q", tc.config, tc.env, key, call)
			}
		}
		if !strings.HasPrefix(block, RestartNeutralBegin+"\n") || !strings.HasSuffix(strings.TrimSpace(block), RestartNeutralEnd) {
			t.Error("the block must sit between the restart-neutral markers")
		}
	}
	cluster := systemModeCluster("2026.08.1-enterprise", 3)
	cluster.Spec.Env = []corev1.EnvVar{{Name: unrestrictedEnv, Value: "custom.*"}}
	if own := BuildConfigMapForEnterprise(cluster).Data["startup.sh"]; strings.Contains(own, " '' '' "+pluginDefaultsArg+"\n") {
		t.Error("a cluster whose spec.env sets the unrestricted list gets no image entries")
	}
	script := BuildConfigMapForEnterprise(systemModeCluster("2026.08.1-enterprise", 3)).Data["startup.sh"]
	if !strings.Contains(script, " '' '' "+pluginDefaultsArg+"\n") {
		t.Error("the rendered script gives the unrestricted list the image's entries")
	}
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
// check), says what the block and the entrypoint make of the same inputs: the
// allowlist with Aura Fleet Management's entry (id 192), the unrestricted list
// with the image's entries for the plugins installed (id 193), on each
// vendored release.
func TestServerProcedureList_AgreesWithTheBlock(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	r0 := entrypointsim.Image{Release: entrypointsim.Release5260}
	r5 := entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}
	r9 := entrypointsim.Image{Release: entrypointsim.Release2026090}
	r10 := entrypointsim.Image{Release: entrypointsim.Release2026100}
	for _, tc := range []struct {
		setting  string
		image    entrypointsim.Image
		own      bool   // the line is the cluster's spec.config, or (ownEnv) the variable its spec.env
		conf     string // the line's value; "-" for none
		env      string // the variable; "-" for none
		plugins  string
		defaults []string // the image's entries for the plugins, in order
	}{
		{AllowlistSetting, r5, false, "-", "-", `["apoc"]`, nil},
		{AllowlistSetting, r5, false, "-", "-", `["fleet-management"]`, nil},
		{AllowlistSetting, r5, false, "apoc.*", "-", `["apoc"]`, nil},
		{AllowlistSetting, r5, false, "apoc.*", "-", `["fleet-management"]`, nil},
		{AllowlistSetting, r5, false, "apoc.*,fleetManagement.*", "-", `["fleet-management"]`, nil},
		{AllowlistSetting, r5, false, "-", "genai.*", `["genai"]`, nil},
		{AllowlistSetting, r5, false, "-", "genai.*", `["fleet-management","genai"]`, nil},
		{AllowlistSetting, r5, false, "apoc.coll.*", "genai.*,ai.*", `["apoc","genai"]`, nil},
		{AllowlistSetting, r5, false, "apoc.coll.*", "genai.*", `["fleet-management","genai"]`, nil},
		{AllowlistSetting, r5, false, "", "-", `["fleet-management"]`, nil},
		{UnrestrictedSetting, r5, false, "-", "-", `["apoc","graph-data-science"]`, []string{"apoc.*", "gds.*"}},
		// apoc-extended is downloaded (a versions URL), which the harness never
		// does: the block counts it all the same, as a server that downloads it.
		{UnrestrictedSetting, r5, false, "-", "gds.*", `["apoc-extended","graph-data-science"]`, []string{"apoc.*", "gds.*"}},
		{UnrestrictedSetting, r5, false, "fleetManagement.*", "-", `["fleet-management","apoc"]`, []string{"fleetManagement.*", "apoc.*"}},
		{UnrestrictedSetting, r5, false, "fleetManagement.*", "-", `["fleet-management"]`, []string{"fleetManagement.*"}},
		{UnrestrictedSetting, r5, false, "-", "gds.*,apoc.load.*", `["apoc","graph-data-science"]`, []string{"apoc.*", "gds.*"}},
		{UnrestrictedSetting, r5, false, "fleetManagement.*", "genai.vector.*", `["fleet-management","genai"]`, []string{"fleetManagement.*", "genai.*"}},
		{UnrestrictedSetting, r5, false, "-", "bloom.*", `["bloom","apoc"]`, []string{"bloom.*", "apoc.*"}},
		{UnrestrictedSetting, r5, true, "custom.*,fleetManagement.*", "-", `["fleet-management","apoc"]`, nil},
		{UnrestrictedSetting, r5, true, "custom.*", "gds.*", `["graph-data-science"]`, nil},
		// ownEnv: the cluster's spec.env sets the list (custom.*), which its
		// conf holds (id 209); the variable is the plugins'.
		{UnrestrictedSetting, r5, true, "custom.*,fleetManagement.*", "-", `["fleet-management","apoc"]`, []string{"ownEnv"}},
		{UnrestrictedSetting, r10, true, "custom.*", "gds.*", `["apoc","graph-data-science","genai"]`, []string{"ownEnv"}},
		{UnrestrictedSetting, r0, false, "-", "gds.*", `["apoc","graph-data-science"]`, []string{"apoc.*", "gds.*"}},
		{UnrestrictedSetting, r9, false, "fleetManagement.*", "-", `["fleet-management","apoc"]`, []string{"apoc.*"}},
		{UnrestrictedSetting, r9, false, "-", "genai.vector.*", `["genai"]`, []string{"genai.*"}},
		{UnrestrictedSetting, r10, false, "-", "genai.vector.*", `["genai"]`, []string{"genai.*,ai.*"}},
		{UnrestrictedSetting, r10, false, "-", "-", `["apoc","genai"]`, []string{"apoc.*", "genai.*,ai.*"}},
	} {
		conf := "server.memory.heap.max_size=1G\n"
		if tc.conf != "-" {
			conf += tc.setting + "=" + tc.conf + "\n"
		}
		env := map[string]string{"NEO4J_PLUGINS": tc.plugins}
		if tc.env != "-" {
			env[Neo4jSettingEnvVarName(tc.setting)] = tc.env
		}
		var config map[string]string
		var specEnv []corev1.EnvVar
		ownEnv := len(tc.defaults) == 1 && tc.defaults[0] == "ownEnv"
		switch {
		case ownEnv:
			specEnv = []corev1.EnvVar{{Name: Neo4jSettingEnvVarName(tc.setting), Value: "custom.*"}}
		case tc.own:
			config = map[string]string{tc.setting: tc.conf}
		}
		// The server: the block, then the entrypoint's plugin defaults and its
		// environment pass.
		started := entrypointsim.Start(t, tc.image, conf, buildPluginListMergeBlock(config, specEnv), env)
		wantValue, lines := entrypointsim.Setting(started, tc.setting)
		wantSet := lines > 0
		var plugins []string
		for _, name := range []string{"apoc", "apoc-extended", "bloom", "genai", "graph-data-science", FleetManagementPlugin} {
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
		defaults := tc.defaults
		if tc.own {
			defaults = nil // the caller passes none for a list the cluster sets itself (OwnsUnrestrictedList)
		}
		got, set := ServerProcedureList(tc.setting, confValue, tc.conf != "-", envValue, tc.env != "-", plugins, defaults)
		if set != wantSet || (set && got != wantValue) {
			t.Errorf("%s %s, conf %q (own %v), env %q, %s: ServerProcedureList = %q (set %v), the server starts with %q (set %v)",
				tc.image.Release, tc.setting, tc.conf, tc.own, tc.env, tc.plugins, got, set, wantValue, wantSet)
		}
	}
}
