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
