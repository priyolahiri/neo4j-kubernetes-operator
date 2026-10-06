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
	"strings"
	"testing"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A Neo4jRestore seeds a PVC artifact over HTTP, one file, and Neo4j refuses a
// differential that way. The warning fires exactly for the PVC backups whose
// runs will be differentials.
func TestBackupWarnings_PVCDifferentials(t *testing.T) {
	pvc := neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}}
	s3 := neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b"}
	opts := func(backupType string) *neo4jv1beta1.BackupOptions {
		return &neo4jv1beta1.BackupOptions{BackupType: backupType}
	}
	cases := []struct {
		name string
		spec neo4jv1beta1.Neo4jBackupSpec
		want string // substring of the warning; "" = no warning
	}{
		{"scheduled, no options (AUTO by default)", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: pvc, Schedule: "0 2 * * *"}, "every run after the first is a differential"},
		{"scheduled AUTO", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: pvc, Schedule: "0 2 * * *", Options: opts("AUTO")}, "every run after the first"},
		{"scheduled FULL", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: pvc, Schedule: "0 2 * * *", Options: opts("FULL")}, ""},
		{"one-shot AUTO: a full into its own directory", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: pvc}, ""},
		{"one-shot DIFF", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: pvc, Options: opts("DIFF")}, "backupType is DIFF"},
		{"chained child", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: pvc, ChainFromBackup: "nightly-full"}, "spec.chainFromBackup"},
		{"all databases, scheduled", neo4jv1beta1.Neo4jBackupSpec{AllDatabases: true, Storage: pvc, Schedule: "0 2 * * *"}, "every run after the first"},
		{"cloud storage reads the whole chain", neo4jv1beta1.Neo4jBackupSpec{Database: "neo4j", Storage: s3, Schedule: "0 2 * * *"}, ""},
		{"sharded: restored through Neo4jShardedDatabase", neo4jv1beta1.Neo4jBackupSpec{ShardedDatabase: "products", Storage: pvc, Schedule: "0 2 * * *"}, ""},
	}
	v := NewBackupValidator()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := v.Warnings(&neo4jv1beta1.Neo4jBackup{Spec: tc.spec})
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("expected no warning, got %q", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("expected one warning containing %q, got %q", tc.want, got)
			}
			if !strings.Contains(got[0], "backupType: FULL") || !strings.Contains(got[0], "a cluster refuses it") {
				t.Errorf("the warning must say what happens and what to do: %q", got[0])
			}
		})
	}
}
