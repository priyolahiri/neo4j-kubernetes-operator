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

// Restoring under a different name on a standalone. neo4j-admin renames only
// when --from-path names a single artifact; the operator globbed
// `<target>-*.backup`, which matches nothing when the target is new — so the
// tutorial's "restore into a new database" step could not work on a standalone.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const renameArtifact = "neo4j-2026-10-05T00-04-58.backup"

func renameStandalone() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
		},
	}
}

// resolvedRename is a type=backup restore after resolveRestoreSource: the spec
// source is the concrete storage, the pinned snapshot carries the artifact.
func resolvedRename(target string, storage neo4jv1beta1.StorageLocation, snap neo4jv1beta1.ResolvedRestoreSource) *neo4jv1beta1.Neo4jRestore {
	snap.Storage = &storage
	if snap.BackupPath == "" {
		snap.BackupPath = "nightly"
	}
	return &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef: "sa",
			Database:    target,
			StopCluster: true,
			Source: neo4jv1beta1.RestoreSource{
				Type:       "storage",
				Storage:    &storage,
				BackupPath: snap.BackupPath,
			},
		},
		Status: neo4jv1beta1.Neo4jRestoreStatus{ResolvedSource: &snap},
	}
}

var renamePVC = neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}}

func restoreCmd(t *testing.T, restore *neo4jv1beta1.Neo4jRestore) string {
	t.Helper()
	cmd, err := (&Neo4jRestoreReconciler{}).buildRestoreCommand(context.Background(), restore, renameStandalone())
	require.NoError(t, err)
	return cmd
}

func TestStandaloneRestore_UnderANewNameReadsTheRecordedArtifact(t *testing.T) {
	cmd := restoreCmd(t, resolvedRename("restored", renamePVC, neo4jv1beta1.ResolvedRestoreSource{
		BackupRef: "nightly", ArtifactFilename: renameArtifact,
	}))
	assert.Contains(t, cmd, "--from-path='/backup/nightly/"+renameArtifact+"' 'restored'",
		"one exact artifact, restored under the target name")
	assert.NotContains(t, cmd, "$(ls", "no glob: it searched for the TARGET name's files")

	// The same artifact serves a same-name restore.
	cmd = restoreCmd(t, resolvedRename("neo4j", renamePVC, neo4jv1beta1.ResolvedRestoreSource{
		BackupRef: "nightly", ArtifactFilename: renameArtifact,
	}))
	assert.Contains(t, cmd, "--from-path='/backup/nightly/"+renameArtifact+"' 'neo4j'")
}

func TestStandaloneRestore_OneDatabaseOutOfAnAllDatabasesBackup(t *testing.T) {
	restore := resolvedRename("customers-copy", renamePVC, neo4jv1beta1.ResolvedRestoreSource{
		BackupRef: "everything",
		DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{
			{Database: "neo4j", Filename: renameArtifact},
			{Database: "customers", Filename: "customers-2026-10-05T00-04-58.backup"},
		},
	})
	restore.Spec.Source.SourceDatabase = "customers"
	cmd := restoreCmd(t, restore)
	assert.Contains(t, cmd, "--from-path='/backup/nightly/customers-2026-10-05T00-04-58.backup' 'customers-copy'")
}

func TestStandaloneRestore_ExactStorageFileIsNotGlobbed(t *testing.T) {
	restore := &neo4jv1beta1.Neo4jRestore{
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			Database: "restored",
			Source: neo4jv1beta1.RestoreSource{
				Type: "storage", Storage: &renamePVC, BackupPath: "nightly/" + renameArtifact,
			},
		},
	}
	cmd := restoreCmd(t, restore)
	assert.Contains(t, cmd, "--from-path='/backup/nightly/"+renameArtifact+"' 'restored'",
		"a backupPath that names the file was globbed as if it were a directory")
}

// No recorded artifact (an older backup, or a storage directory): glob the
// SOURCE database's files.
func TestStandaloneRestore_DirectoryGlobsTheSourceDatabase(t *testing.T) {
	restore := resolvedRename("restored", renamePVC, neo4jv1beta1.ResolvedRestoreSource{BackupRef: "old"})
	restore.Spec.Source.SourceDatabase = "neo4j"
	assert.Contains(t, restoreCmd(t, restore), "$(ls '/backup/nightly'/'neo4j'-*.backup | tail -1) 'restored'")
}

func TestStandaloneRestore_Cloud(t *testing.T) {
	s3 := neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "neo4j-backups", Path: "prod"}

	cmd := restoreCmd(t, resolvedRename("restored", s3, neo4jv1beta1.ResolvedRestoreSource{
		BackupRef: "nightly", ArtifactFilename: renameArtifact,
	}))
	assert.Contains(t, cmd, "--from-path='s3://neo4j-backups/prod/nightly/"+renameArtifact+"' 'restored'")

	// A cloud directory can only be restored under its own name.
	restore := resolvedRename("restored", s3, neo4jv1beta1.ResolvedRestoreSource{BackupRef: "old"})
	restore.Spec.Source.SourceDatabase = "neo4j"
	_, err := (&Neo4jRestoreReconciler{}).buildRestoreCommand(context.Background(), restore, renameStandalone())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "needs the exact .backup artifact")

	// Same name from a directory: unchanged.
	cmd = restoreCmd(t, resolvedRename("neo4j", s3, neo4jv1beta1.ResolvedRestoreSource{BackupRef: "old"}))
	assert.Contains(t, cmd, "--from-path='s3://neo4j-backups/prod/nightly' 'neo4j'")
}

func TestStandaloneRestore_SourceDatabaseMustMatchAOneDatabaseBackup(t *testing.T) {
	restore := resolvedRename("restored", renamePVC, neo4jv1beta1.ResolvedRestoreSource{
		BackupRef: "nightly", ArtifactFilename: renameArtifact,
	})
	restore.Spec.Source.SourceDatabase = "customers"
	_, err := (&Neo4jRestoreReconciler{}).buildRestoreCommand(context.Background(), restore, renameStandalone())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `holds database "neo4j"`)
}

func TestArtifactDatabase(t *testing.T) {
	assert.Equal(t, "neo4j", artifactDatabase("neo4j-2026-10-05T00-04-58.backup"))
	assert.Equal(t, "my-db", artifactDatabase("my-db-2026-10-05T00-04-58.842.backup"))
	assert.Equal(t, "", artifactDatabase("something.else"))
}

func renameScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	return scheme
}

func TestValidateRestore_SourceDatabase(t *testing.T) {
	r := &Neo4jRestoreReconciler{Client: fake.NewClientBuilder().WithScheme(renameScheme(t)).Build()}
	base := func() *neo4jv1beta1.Neo4jRestore {
		return &neo4jv1beta1.Neo4jRestore{
			Spec: neo4jv1beta1.Neo4jRestoreSpec{
				InstanceRef: "sa",
				Database:    "restored",
				Source: neo4jv1beta1.RestoreSource{
					Type: "storage", Storage: &renamePVC, BackupPath: "nightly", SourceDatabase: "neo4j",
				},
			},
		}
	}
	require.NoError(t, r.validateRestore(context.Background(), base()))

	for _, tc := range []struct {
		mutate func(*neo4jv1beta1.Neo4jRestore)
		want   string
	}{
		{func(r *neo4jv1beta1.Neo4jRestore) { r.Spec.Source.SourceDatabase = "system" }, "system database"},
		{func(r *neo4jv1beta1.Neo4jRestore) { r.Spec.Source.SourceDatabase = "bad;name" }, "is invalid"},
	} {
		restore := base()
		tc.mutate(restore)
		err := r.validateRestore(context.Background(), restore)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), tc.want)
		}
	}
}

// The cluster path had the same gap for all-databases backups: it looked the
// TARGET name up in the per-database map.
func TestClusterRestore_AllDatabasesBackupLooksUpTheSourceDatabase(t *testing.T) {
	scheme := renameScheme(t)
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "everything", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "c", AllDatabases: true},
		Status: neo4jv1beta1.Neo4jBackupStatus{History: []neo4jv1beta1.BackupRun{{
			Status: "Succeeded",
			DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{
				{Database: "customers", Filename: "customers-2026-10-05T00-04-58.backup"},
			},
		}}},
	}
	r := &Neo4jRestoreReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(backup).Build()}
	restore := &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			Database: "customers-copy",
			Source:   neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "everything", SourceDatabase: "customers"},
		},
	}
	got, err := r.resolvedOrLiveArtifactFilename(context.Background(), restore)
	require.NoError(t, err)
	assert.Equal(t, "customers-2026-10-05T00-04-58.backup", got)
	assert.True(t, strings.HasPrefix(got, restoreSourceDatabase(restore)+"-"))
}
