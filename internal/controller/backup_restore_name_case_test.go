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

package controller

// The backup and restore names #521 left exact (#529). What Neo4j does with a
// database name it was given in capitals, read from neo4j-admin and the server
// rather than assumed (knowledge operations.md id 146):
//   - neo4j-admin backs up the databases the server lists that match its
//     --database pattern (compiled CASE_INSENSITIVE), and names each artifact
//     `<name>-<yyyy-MM-dd'T'HH-mm-ss>.backup` from the database's layout,
//     whose name is normalized to lower case (OnlineBackupExecutor,
//     BackupWorkspace.nextArtifact, FileManager in neo4j-backup 5.23 and
//     2025.09; PlainDatabaseLayout and NormalizedDatabaseName in
//     neo4j/neo4j release/5.26.0).
//   - A sharded CREATE DATABASE derives the shard names from the normalized
//     database name and removes each from the seedURI map's key set with an
//     exact HashSet.remove; a key left over or missing is refused
//     (InputValidators.validateSharedSeedURI in 2025.12).

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The PVC restore that knows no artifact globbed `<spec name>-*.backup`. For
// `Products` that matched nothing, since neo4j-admin wrote `products-…`; and
// `products-*.backup` also matched `products-eu-…`, the newest name in the
// listing, so `tail -1` restored another database. The generated command runs
// here against files named as neo4j-admin names them.
func TestLatestArtifactSubstitution_FindsTheArtifactNeo4jAdminWrote(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"products-2026-10-20T10-00-00.backup",
		"products-2026-10-21T10-00-00.backup",
		"products-2026-10-21T10-00-00.backup.1", // a split archive's part (2026.09)
		"products-eu-2026-10-22T10-00-00.backup",
		"products-2025-2026-10-23T10-00-00.backup", // database "products-2025"
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}

	for _, tc := range []struct{ database, want string }{
		{"Products", "products-2026-10-21T10-00-00.backup"},
		{"products", "products-2026-10-21T10-00-00.backup"},
		{"Products-EU", "products-eu-2026-10-22T10-00-00.backup"},
		{"products-2025", "products-2025-2026-10-23T10-00-00.backup"},
	} {
		t.Run(tc.database, func(t *testing.T) {
			out, err := exec.CommandContext(context.Background(), "sh", "-c", "printf '%s' "+latestArtifactSubstitution(dir, tc.database)).Output()
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(dir, tc.want), string(out))
		})
	}
}

// resolveLocalPVCFromPath is the entry the main and point-in-time paths use.
func TestResolveLocalPVCFromPath_LowerCasesTheDatabase(t *testing.T) {
	assert.Equal(t,
		"$(ls '/backup/nightly'/'products'"+latestArtifactGlob+" | tail -1)",
		resolveLocalPVCFromPath("/backup/nightly", "Products"))
}

// A sharded restore under a capitalised spec.name built its per-shard seed keys
// as `Products-g000`; Neo4j derives `products-g000` and refuses the map
// ("entry is missing for 'products-g000'"). Both builders, cloud and PVC.
func TestPerShardSeedKeys_AreTheLowerCaseShardNames(t *testing.T) {
	artifacts := []neo4jv1beta1.ShardArtifact{
		{ShardName: "products-g000", Filename: "products-g000-2026-10-06T13-46-10.backup", Type: "FULL"},
		{ShardName: "products-p000", Filename: "products-p000-2026-10-06T13-46-09.backup", Type: "FULL"},
	}
	want := []string{"products-g000", "products-p000"}

	t.Run("cloud", func(t *testing.T) {
		perShard, err := buildPerShardCloudURIs(neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b"}, "all", "Products", artifacts)
		require.NoError(t, err)
		assert.ElementsMatch(t, want, mapKeys(perShard))
	})

	t.Run("pvc", func(t *testing.T) {
		r := shardedSeedReconciler(t, seedTestCluster(), seedTestPVCBackup(artifacts...))
		sd := seedTestShardedDB()
		sd.Spec.Name = "Products"
		sd.Spec.SeedSourceDatabase = ""
		resolved, err := r.resolveShardedSeed(context.Background(), sd)
		require.NoError(t, err)
		assert.ElementsMatch(t, want, mapKeys(resolved.PerShardURIs))
	})

	t.Run("spec.seedURIs", func(t *testing.T) {
		sd := &neo4jv1beta1.Neo4jShardedDatabase{Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
			SeedURIs: map[string]string{"Products-g000": "s3://b/g.backup", "Products-p000": "s3://b/p.backup"},
		}}
		clause, _, err := buildShardedDatabaseOptions(sd)
		require.NoError(t, err)
		assert.Equal(t, "seedURI: { `products-g000`: $seed_uri_0, `products-p000`: $seed_uri_1 }", clause)
	})
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Two Neo4jBackups that spell one database differently write one chain: the
// artifacts of both are `orders-<timestamp>.backup`. The chain check refused
// them as different targets.
func TestValidateChainParent_OneDatabaseInAnotherCase(t *testing.T) {
	backupOf := func(name, database, chainFrom string) *neo4jv1beta1.Neo4jBackup {
		return &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef: "c", Database: database, ChainFromBackup: chainFrom,
				Storage: neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b", Path: "p"},
			},
		}
	}
	parent := backupOf("daily", "Orders", "")
	sameDB := backupOf("hourly", "orders", "daily")
	otherDB := backupOf("hourly-other", "customers", "daily")
	r := &Neo4jBackupReconciler{Client: fake.NewClientBuilder().WithScheme(backupLifecycleScheme(t)).
		WithObjects(parent, sameDB, otherDB).Build()}

	assert.NoError(t, r.validateChainParent(context.Background(), sameDB))
	err := r.validateChainParent(context.Background(), otherDB)
	require.Error(t, err, "another database is still another target")
	assert.Contains(t, err.Error(), "chained backups must share the same target")
}
