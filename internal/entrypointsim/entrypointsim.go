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

// Package entrypointsim runs, for tests, the part of the Neo4j Docker image's
// entrypoint that turns the neo4j.conf a pod is given and its NEO4J_*
// variables into the neo4j.conf the server reads: the plugin defaults of
// install_neo4j_plugins (apply_plugin_default_configuration), then the
// environment pass (add_env_setting_to_conf). testdata/entrypoint-excerpt.sh
// holds those functions, copied verbatim from github.com/neo4j/docker-neo4j,
// and testdata/neo4j-plugins-{5,calver}.json the images' plugin tables
// (docs/knowledge/operations.md ids 179, 192).
//
// It is test support: nothing outside tests imports it.
package entrypointsim

import (
	"embed"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed testdata/entrypoint-excerpt.sh testdata/neo4j-plugins-5.json testdata/neo4j-plugins-calver.json
var files embed.FS

// ConfPath is the path the operator's startup script reads the neo4j.conf it
// assembled from; Start points a prelude's references to it at the conf it
// runs on.
const ConfPath = "/conf/neo4j.conf"

// Image is what the entrypoint of one Neo4j image finds.
type Image struct {
	// CalVer picks the CalVer plugin table, else the 5.x one.
	CalVer bool
	// FleetJAR is a fleet-management JAR in products/: on 5.26.12 and later
	// and 2025.08.0 to 2026.02.x. From 2026.03 Fleet Manager is built in and
	// products/ has none; the table still names the plugin, without a
	// versions URL, so the entrypoint cannot install it.
	FleetJAR bool
}

// Start runs prelude — the operator's startup-script lines that run before
// the entrypoint; "" for none — then the entrypoint's plugin install (when
// NEO4J_PLUGINS is set) and its environment pass, over conf with env set, and
// returns the neo4j.conf the server reads. It skips the test without bash or
// jq.
func Start(t testing.TB, image Image, conf, prelude string, env map[string]string) string {
	t.Helper()
	for _, tool := range []string{"bash", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	root := t.TempDir()
	home := filepath.Join(root, "neo4j")
	for _, dir := range []string{"conf", "plugins", "labs", "products"} {
		mkdir(t, filepath.Join(home, dir))
	}
	jars := []string{"labs/apoc-5.26.32-core.jar", "products/neo4j-genai-plugin-1.0.0.jar",
		"products/neo4j-graph-data-science-2.13.0.jar"}
	if image.FleetJAR {
		jars = append(jars, "products/neo4j-fleet-management-plugin-1.2.0-v5.jar")
	}
	for _, jar := range jars {
		write(t, filepath.Join(home, jar), "")
	}
	table := "testdata/neo4j-plugins-5.json"
	if image.CalVer {
		table = "testdata/neo4j-plugins-calver.json"
	}
	tablePath := filepath.Join(root, "neo4j-plugins.json")
	write(t, tablePath, strings.ReplaceAll(read(t, table), "/var/lib/neo4j", home))
	confPath := filepath.Join(home, "conf", "neo4j.conf")
	write(t, confPath, conf)

	excerpt := strings.ReplaceAll(read(t, "testdata/entrypoint-excerpt.sh"), "/startup/neo4j-plugins.json", tablePath)
	script := "set -eu\n" + excerpt + "\nNEO4J_HOME=" + home + "\n" +
		strings.ReplaceAll(prelude, ConfPath, confPath) + "\n" +
		"if [[ -n \"${NEO4J_PLUGINS:-}\" ]]; then install_neo4j_plugins; fi\n" +
		"apply_environment_settings\n"
	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Dir = root
	cmd.Env = []string{"PATH=" + gnuPath(t, root) + string(os.PathListSeparator) + os.Getenv("PATH"), "HOME=" + root}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the entrypoint failed: %v\n%s", err, out)
	}
	return read(t, confPath)
}

// Setting returns the value of key in conf — its last line, as Neo4j 5.26
// reads it — and how many lines set it.
func Setting(conf, key string) (value string, lines int) {
	for _, line := range strings.Split(conf, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+"="); ok {
			value, lines = v, lines+1
		}
	}
	return value, lines
}

// gnuPath returns a directory of stand-ins for the GNU options the excerpt
// uses (sed --in-place, cp --preserve) where sed is not GNU sed (macOS), and
// an empty directory where it is.
func gnuPath(t testing.TB, root string) string {
	t.Helper()
	bin := filepath.Join(root, "bin")
	mkdir(t, bin)
	if exec.CommandContext(t.Context(), "sed", "--version").Run() == nil {
		return bin
	}
	sed, err := exec.LookPath("sed")
	if err != nil {
		t.Skip("sed not available")
	}
	cp, err := exec.LookPath("cp")
	if err != nil {
		t.Skip("cp not available")
	}
	write(t, filepath.Join(bin, "sed"), "#!/bin/bash\nargs=()\nfor a in \"$@\"; do\n  if [ \"$a\" = \"--in-place\" ]; then args+=(-i \"\"); else args+=(\"$a\"); fi\ndone\nexec "+sed+" \"${args[@]}\"\n")
	write(t, filepath.Join(bin, "cp"), "#!/bin/bash\nargs=()\nfor a in \"$@\"; do\n  case \"$a\" in --preserve|--preserve=*) ;; *) args+=(\"$a\") ;; esac\ndone\nexec "+cp+" \"${args[@]}\"\n")
	for _, name := range []string{"sed", "cp"} {
		if err := os.Chmod(filepath.Join(bin, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return bin
}

func mkdir(t testing.TB, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func write(t testing.TB, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t testing.TB, path string) string {
	t.Helper()
	var (
		data []byte
		err  error
	)
	if strings.HasPrefix(path, "testdata/") {
		data, err = files.ReadFile(path)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
