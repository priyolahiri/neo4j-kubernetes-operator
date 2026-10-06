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

// TestBackupValidator_ScopeAPI covers the v1.13 scope-based selector
// (spec.instanceRef + spec.database/allDatabases) and its mutual exclusion with
// the deprecated spec.target block.
func TestBackupValidator_ScopeAPI(t *testing.T) {
	validator := NewBackupValidator()

	s3 := neo4jv1beta1.StorageLocation{
		Type:   "s3",
		Bucket: "backup-bucket",
		Cloud:  &neo4jv1beta1.CloudBlock{Provider: "aws"},
	}

	tests := []struct {
		name         string
		spec         neo4jv1beta1.Neo4jBackupSpec
		expectError  bool
		errSubstring string
	}{
		{
			name: "instanceRef + database (single) is valid",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef: "my-neo4j",
				Database:    "customers",
				Storage:     s3,
			},
		},
		{
			name: "instanceRef + allDatabases is valid",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef:  "my-neo4j",
				AllDatabases: true,
				Storage:      s3,
			},
		},
		{
			name: "instanceRef + shardedDatabase is valid",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef:     "my-neo4j",
				ShardedDatabase: "products-sharded",
				Storage:         s3,
			},
		},
		{
			name: "database and allDatabases are mutually exclusive",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef:  "my-neo4j",
				Database:     "customers",
				AllDatabases: true,
				Storage:      s3,
			},
			expectError:  true,
			errSubstring: "mutually exclusive",
		},
		{
			name: "shardedDatabase and allDatabases are mutually exclusive",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef:     "my-neo4j",
				ShardedDatabase: "products-sharded",
				AllDatabases:    true,
				Storage:         s3,
			},
			expectError:  true,
			errSubstring: "mutually exclusive",
		},
		{
			name: "instanceRef requires a scope",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef: "my-neo4j",
				Storage:     s3,
			},
			expectError:  true,
			errSubstring: "spec.database",
		},
		{
			name: "neither instanceRef nor target set",
			spec: neo4jv1beta1.Neo4jBackupSpec{
				Storage: s3,
			},
			expectError:  true,
			errSubstring: "instanceRef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := &neo4jv1beta1.Neo4jBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "test-backup"},
				Spec:       tt.spec,
			}
			errs := validator.Validate(backup)
			if tt.expectError {
				if len(errs) == 0 {
					t.Fatalf("expected a validation error, got none")
				}
				if tt.errSubstring != "" && !strings.Contains(errs.ToAggregate().Error(), tt.errSubstring) {
					t.Fatalf("expected error containing %q, got: %s", tt.errSubstring, errs.ToAggregate().Error())
				}
			} else if len(errs) != 0 {
				t.Fatalf("expected no validation error, got: %s", errs.ToAggregate().Error())
			}
		})
	}
}

// TestNeo4jBackupSpec_Scope verifies the scope accessors the controller relies
// on: Scope() classifies the backup and ScopedName() names the targeted
// resource, both derived from the scope fields.
func TestNeo4jBackupSpec_Scope(t *testing.T) {
	tests := []struct {
		name          string
		spec          neo4jv1beta1.Neo4jBackupSpec
		wantScope     string
		wantScopeName string
	}{
		{
			name:          "single database -> Database scope",
			spec:          neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "my-neo4j", Database: "customers"},
			wantScope:     neo4jv1beta1.BackupTargetKindDatabase,
			wantScopeName: "customers",
		},
		{
			name:          "all databases -> Cluster scope",
			spec:          neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "my-neo4j", AllDatabases: true},
			wantScope:     neo4jv1beta1.BackupTargetKindCluster,
			wantScopeName: "my-neo4j",
		},
		{
			name:          "sharded database -> ShardedDatabase scope",
			spec:          neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "my-cluster", ShardedDatabase: "products-sharded"},
			wantScope:     neo4jv1beta1.BackupTargetKindShardedDatabase,
			wantScopeName: "products-sharded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.Scope(); got != tt.wantScope {
				t.Fatalf("Scope() = %q, want %q", got, tt.wantScope)
			}
			if got := tt.spec.ScopedName(); got != tt.wantScopeName {
				t.Fatalf("ScopedName() = %q, want %q", got, tt.wantScopeName)
			}
		})
	}
}

// Neo4j refuses differential backups of property shards (observed on the
// 2026.08.1 CI anchor: "Differential backups of property shards are not
// allowed"), so a DIFF backup of a sharded database fails on every run. Only
// the sharded scope is refused; AUTO and FULL are fine, and DIFF stays valid
// for a single database.
func TestBackupValidator_ShardedDatabaseRefusesDIFF(t *testing.T) {
	validator := NewBackupValidator()
	s3 := neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b", Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws"}}
	backup := func(scope func(*neo4jv1beta1.Neo4jBackupSpec), backupType string) *neo4jv1beta1.Neo4jBackup {
		spec := neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", Storage: s3, Options: &neo4jv1beta1.BackupOptions{BackupType: backupType}}
		scope(&spec)
		return &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}, Spec: spec}
	}
	sharded := func(s *neo4jv1beta1.Neo4jBackupSpec) { s.ShardedDatabase = "products" }
	single := func(s *neo4jv1beta1.Neo4jBackupSpec) { s.Database = "neo4j" }

	errs := validator.Validate(backup(sharded, "DIFF"))
	if len(errs) != 1 || errs[0].Field != "spec.options.backupType" || !strings.Contains(errs[0].Detail, "property shards") {
		t.Fatalf("a DIFF backup of a sharded database must be refused on spec.options.backupType, got %v", errs)
	}
	for _, typ := range []string{"AUTO", "FULL", ""} {
		if errs := validator.Validate(backup(sharded, typ)); len(errs) != 0 {
			t.Errorf("backupType %q of a sharded database must be valid, got %v", typ, errs)
		}
	}
	if errs := validator.Validate(backup(single, "DIFF")); len(errs) != 0 {
		t.Errorf("DIFF of a single database must stay valid, got %v", errs)
	}
}
