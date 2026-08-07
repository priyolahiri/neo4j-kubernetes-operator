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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func replicationSourceCR(mutate func(*neo4jv1beta1.Neo4jBackup)) *neo4jv1beta1.Neo4jBackup {
	b := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-chain", Namespace: "prod"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			Mode:        BackupModeReplicationSource,
			InstanceRef: "prod-cluster",
			Database:    "foo",
			Schedule:    "0 * * * *",
			Storage: neo4jv1beta1.StorageLocation{
				Type: "s3", Bucket: "backups", Path: "prod-foo",
			},
		},
	}
	if mutate != nil {
		mutate(b)
	}
	return b
}

func TestReplicationSource_Valid(t *testing.T) {
	if errs := ValidateReplicationSourceMode(replicationSourceCR(nil), nil); len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

// A standard-mode backup must be entirely unaffected by these rules.
func TestReplicationSource_StandardModeUnaffected(t *testing.T) {
	b := replicationSourceCR(func(b *neo4jv1beta1.Neo4jBackup) {
		b.Spec.Mode = "standard"
		b.Spec.AllDatabases = true
		b.Spec.Database = ""
		b.Spec.Schedule = ""
		b.Spec.Retention = &neo4jv1beta1.RetentionPolicy{MaxCount: 5}
	})
	if errs := ValidateReplicationSourceMode(b, nil); len(errs) != 0 {
		t.Fatalf("standard mode must not be constrained, got %v", errs)
	}
}

// R1 — an instance-wide backup produces an aggregate layout a per-database
// pullURI cannot consume.
func TestReplicationSource_R1_AllDatabasesRejected(t *testing.T) {
	b := replicationSourceCR(func(b *neo4jv1beta1.Neo4jBackup) {
		b.Spec.AllDatabases = true
		b.Spec.Database = ""
	})
	errs := ValidateReplicationSourceMode(b, nil)
	if len(errs) == 0 {
		t.Fatal("expected allDatabases to be rejected in replication-source mode")
	}
}

func TestReplicationSource_R1_ShardedRejected(t *testing.T) {
	b := replicationSourceCR(func(b *neo4jv1beta1.Neo4jBackup) {
		b.Spec.ShardedDatabase = "sharded-foo"
		b.Spec.Database = ""
	})
	if errs := ValidateReplicationSourceMode(b, nil); len(errs) == 0 {
		t.Fatal("expected sharded scope to be rejected in replication-source mode")
	}
}

// R2 — pruning an artifact a later differential chains from breaks the replica.
func TestReplicationSource_R2_RetentionRejected(t *testing.T) {
	b := replicationSourceCR(func(b *neo4jv1beta1.Neo4jBackup) {
		b.Spec.Retention = &neo4jv1beta1.RetentionPolicy{MaxCount: 5}
	})
	errs := ValidateReplicationSourceMode(b, nil)
	if len(errs) == 0 {
		t.Fatal("expected retention to be rejected in replication-source mode")
	}
	// The message must also disclose the limit the operator cannot enforce,
	// or a user will assume cloud retention is equally protected.
	if !strings.Contains(errs.ToAggregate().Error(), "lifecycle") {
		t.Error("retention rejection should warn that bucket lifecycle rules break the chain too")
	}
}

// R4 — a source with no cadence is a replica that falls arbitrarily behind.
func TestReplicationSource_R4_ScheduleRequired(t *testing.T) {
	b := replicationSourceCR(func(b *neo4jv1beta1.Neo4jBackup) {
		b.Spec.Schedule = "   "
	})
	if errs := ValidateReplicationSourceMode(b, nil); len(errs) == 0 {
		t.Fatal("expected a missing schedule to be rejected")
	}
}

// R3 — the existing part-of interlock serialises Jobs within one chain; it
// does not stop an unrelated CR sharing the directory.
func TestReplicationSource_R3_CompetingWriterRejected(t *testing.T) {
	b := replicationSourceCR(nil)
	siblings := []neo4jv1beta1.Neo4jBackup{{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated-backup", Namespace: "prod"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef: "prod-cluster",
			Database:    "foo",
			Storage:     neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "backups", Path: "prod-foo"},
		},
	}}
	errs := ValidateReplicationSourceMode(b, siblings)
	if len(errs) == 0 {
		t.Fatal("expected a competing writer to the same storage location to be rejected")
	}
	if !strings.Contains(errs.ToAggregate().Error(), "unrelated-backup") {
		t.Error("error should name the conflicting CR")
	}
}

// The daily-FULL + hourly-DIFF composition is the RECOMMENDED shape and must
// not be flagged as a competing writer.
func TestReplicationSource_R3_SameChainAllowed(t *testing.T) {
	b := replicationSourceCR(nil)
	siblings := []neo4jv1beta1.Neo4jBackup{{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-chain-hourly", Namespace: "prod"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:     "prod-cluster",
			Database:        "foo",
			ChainFromBackup: "foo-chain",
			Storage:         neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "backups", Path: "prod-foo"},
		},
	}}
	if errs := ValidateReplicationSourceMode(b, siblings); len(errs) != 0 {
		t.Fatalf("a CR chained from this one is not a competing writer, got %v", errs)
	}
}

func TestReplicationSource_R3_DifferentPathAllowed(t *testing.T) {
	b := replicationSourceCR(nil)
	siblings := []neo4jv1beta1.Neo4jBackup{{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "prod"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			Storage: neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "backups", Path: "somewhere-else"},
		},
	}}
	if errs := ValidateReplicationSourceMode(b, siblings); len(errs) != 0 {
		t.Fatalf("a CR writing elsewhere is not a conflict, got %v", errs)
	}
}

func TestReplicationPullURI(t *testing.T) {
	cases := []struct {
		name        string
		storage     neo4jv1beta1.StorageLocation
		backupsPath string
		want        string
	}{
		{"s3 with path", neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b", Path: "p"}, "chain", "s3://b/p/chain/"},
		{"s3 no path", neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b"}, "chain", "s3://b/chain/"},
		{"gcs", neo4jv1beta1.StorageLocation{Type: "gcs", Bucket: "b", Path: "p"}, "chain", "gs://b/p/chain/"},
		{"azure", neo4jv1beta1.StorageLocation{Type: "azure", Bucket: "c", Path: "p"}, "chain", "azb://c/p/chain/"},
		{"trims slashes", neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "/b/", Path: "/p/"}, "/chain/", "s3://b/p/chain/"},
		// A PVC is not reachable from another Kubernetes cluster, so there is
		// no URI form a downstream replica could use.
		{"pvc has no cross-cluster form", neo4jv1beta1.StorageLocation{Type: "pvc"}, "chain", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReplicationPullURI(tc.storage, tc.backupsPath); got != tc.want {
				t.Errorf("ReplicationPullURI() = %q, want %q", got, tc.want)
			}
		})
	}
}
