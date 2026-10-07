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
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func systemModeCluster(tag string, servers int32) *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: servers},
		},
	}
}

// TestStartupScript_ParsesAsBash: the rendered startup script, with the system
// database role block, is valid bash on both lines.
func TestStartupScript_ParsesAsBash(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	for _, tag := range []string{"5.26-enterprise", "2026.08.1-enterprise"} {
		script := BuildConfigMapForEnterprise(systemModeCluster(tag, 5)).Data["startup.sh"]
		if !strings.Contains(script, RestartNeutralBegin) || !strings.Contains(script, "OPERATOR_SYSTEM_PRIMARIES=3") {
			t.Fatalf("%s: the system database role block is missing", tag)
		}
		cmd := exec.CommandContext(t.Context(), "bash", "-n")
		cmd.Stdin = strings.NewReader(script)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("%s: startup.sh does not parse: %v\n%s", tag, err, out)
		}
	}
}

// TestSystemDatabaseModeBlock pins #467: a server's role for the system
// database is decided at its first start from its ordinal, recorded on the
// data volume, and kept on every later start; an existing server never
// changes role.
func TestSystemDatabaseModeBlock(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	block := buildSystemDatabaseModeBlock(systemModeCluster("2026.08.1-enterprise", 5))

	type state struct {
		hasSystemStore bool
		record         string // "" = no record
	}
	cases := []struct {
		name          string
		index         int
		before        state
		wantSecondary bool   // conf gets system_database_mode=SECONDARY
		wantRecord    string // record afterwards
	}{
		{"new server below the primaries count", 0, state{}, false, "PRIMARY"},
		{"new server at the primaries count", 3, state{}, true, "SECONDARY"},
		{"new server past it", 4, state{}, true, "SECONDARY"},
		{"secondary restarting keeps its role", 4, state{hasSystemStore: true, record: "SECONDARY"}, true, "SECONDARY"},
		{"recorded primary past the count stays primary", 5, state{hasSystemStore: true, record: "PRIMARY"}, false, "PRIMARY"},
		{"existing server without a record keeps the default", 4, state{hasSystemStore: true}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			data, conf := filepath.Join(root, "data"), filepath.Join(root, "neo4j.conf")
			if err := os.MkdirAll(data, 0o755); err != nil {
				t.Fatal(err)
			}
			if tc.before.hasSystemStore {
				if err := os.MkdirAll(filepath.Join(data, "databases", "system"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			record := strings.Replace(SystemDatabaseModeFile, "/data", data, 1)
			if tc.before.record != "" {
				if err := os.MkdirAll(filepath.Dir(record), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(record, []byte(tc.before.record+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			// Point the block's quoted /data/ paths at the temp volume. Matching
			// the opening quote too keeps "/databases" in /data/databases/system
			// from being rewritten as well.
			script := "set -e\nSERVER_INDEX=" + strconv.Itoa(tc.index) + "\n" +
				strings.ReplaceAll(strings.ReplaceAll(block, "/conf/neo4j.conf", conf), `"/data/`, `"`+data+`/`)
			out, err := exec.CommandContext(t.Context(), "bash", "-c", script).CombinedOutput()
			if err != nil {
				t.Fatalf("block failed: %v\n%s", err, out)
			}

			confText, _ := os.ReadFile(conf)
			if got := strings.Contains(string(confText), "server.cluster.system_database_mode=SECONDARY"); got != tc.wantSecondary {
				t.Errorf("secondary=%v, want %v (conf %q)", got, tc.wantSecondary, confText)
			}
			recorded, _ := os.ReadFile(record)
			if got := strings.TrimSpace(string(recorded)); got != tc.wantRecord {
				t.Errorf("record %q, want %q", got, tc.wantRecord)
			}
		})
	}
}

// TestAsyncRaftChannelsBlock pins #468: on 5.26 the setting is enabled only by a
// container whose own Neo4j is 5.26.29 or later (an older patch would refuse an
// unknown setting and not start), never twice, and never on CalVer.
func TestAsyncRaftChannelsBlock(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	if b := buildAsyncRaftChannelsBlock(systemModeCluster("2026.08.1-enterprise", 3)); b != "" {
		t.Fatalf("CalVer has the setting on by default; nothing may be rendered, got %q", b)
	}
	block := buildAsyncRaftChannelsBlock(systemModeCluster("5.26-enterprise", 3))
	line := AsyncRaftChannelsSetting + "=true"
	for _, tc := range []struct {
		name     string
		jar      string // kernel jar in $NEO4J_HOME/lib; "" for none
		conf     string // neo4j.conf before the block
		wantLine int    // occurrences of the setting afterwards
	}{
		{"5.26.28 lacks the setting", "neo4j-kernel-5.26.28.jar", "", 0},
		{"5.26.29 has it", "neo4j-kernel-5.26.29.jar", "", 1},
		{"5.26.31 has it", "neo4j-kernel-5.26.31.jar", "", 1},
		{"a user value is left alone", "neo4j-kernel-5.26.31.jar", AsyncRaftChannelsSetting + "=false\n", 1},
		{"no kernel jar found", "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			lib, conf := filepath.Join(root, "lib"), filepath.Join(root, "neo4j.conf")
			if err := os.MkdirAll(lib, 0o755); err != nil {
				t.Fatal(err)
			}
			for _, j := range []string{tc.jar, "neo4j-kernel-api-5.26.0.jar"} {
				if j != "" {
					if err := os.WriteFile(filepath.Join(lib, j), nil, 0o644); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := os.WriteFile(conf, []byte(tc.conf), 0o644); err != nil {
				t.Fatal(err)
			}
			script := "set -e\nNEO4J_HOME=" + root + "\n" + strings.ReplaceAll(block, "/conf/neo4j.conf", conf)
			if out, err := exec.CommandContext(t.Context(), "bash", "-c", script).CombinedOutput(); err != nil {
				t.Fatalf("block failed: %v\n%s", err, out)
			}
			got, _ := os.ReadFile(conf)
			if n := strings.Count(string(got), AsyncRaftChannelsSetting+"="); n != tc.wantLine {
				t.Errorf("setting appears %d times, want %d (conf %q)", n, tc.wantLine, got)
			}
			if tc.conf == "" && tc.wantLine == 1 && !strings.Contains(string(got), line) {
				t.Errorf("want %q in conf, got %q", line, got)
			}
		})
	}
}

// TestTLSReloadBlock pins #469's cluster side: the restart-neutral block is
// rendered for CalVer 2025.03+ with cert-manager TLS only, and the whole script
// still parses.
func TestTLSReloadBlock(t *testing.T) {
	withTLS := func(tag string) *neo4jv1beta1.Neo4jEnterpriseCluster {
		c := systemModeCluster(tag, 3)
		c.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: CertManagerMode}
		return c
	}
	for tag, want := range map[string]bool{"2026.08.1-enterprise": true, "2025.03.0-enterprise": true, "2025.01.0-enterprise": false, "5.26-enterprise": false} {
		if got := strings.Contains(buildTLSReloadBlock(withTLS(tag)), TLSReloadSetting+"=true"); got != want {
			t.Errorf("%s: rendered=%v, want %v", tag, got, want)
		}
	}
	if b := buildTLSReloadBlock(systemModeCluster("2026.08.1-enterprise", 3)); b != "" {
		t.Errorf("no TLS: nothing to reload, got %q", b)
	}
	if _, err := exec.LookPath("bash"); err == nil {
		script := BuildConfigMapForEnterprise(withTLS("2026.08.1-enterprise")).Data["startup.sh"]
		cmd := exec.CommandContext(t.Context(), "bash", "-n")
		cmd.Stdin = strings.NewReader(script)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("startup.sh with the TLS reload block does not parse: %v\n%s", err, out)
		}
	}
}
