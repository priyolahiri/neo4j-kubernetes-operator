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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The 2026-10-25 walk restored a PVC source with backupPath: /backup/bk1. A
// PVC backupPath is relative to the PVC's root and the operator adds /backup/
// itself, so the Job read /backup//backup/bk1 and failed. The restore says so,
// as an advisory: the PVC may have a backup directory at its root.
func TestRestorePVCPathWarnings(t *testing.T) {
	pvc := func() *neo4jv1beta1.StorageLocation {
		return &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}}
	}
	storage := func(storage *neo4jv1beta1.StorageLocation, backupPath string) *neo4jv1beta1.Neo4jRestore {
		return &neo4jv1beta1.Neo4jRestore{Spec: neo4jv1beta1.Neo4jRestoreSpec{Source: neo4jv1beta1.RestoreSource{
			Type: "storage", Storage: storage, BackupPath: backupPath,
		}}}
	}

	warnings := RestorePVCPathWarnings(storage(pvc(), "/backup/bk1"))
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], `spec.source.backupPath "/backup/bk1"`)
	assert.Contains(t, warnings[0], "names backup/bk1 on the PVC")
	assert.Contains(t, warnings[0], `write "bk1"`)

	warnings = RestorePVCPathWarnings(storage(pvc(), "/backup"))
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "leave out the /backup prefix")

	for _, path := range []string{"bk1", "bk1/neo4j-2026-10-25T01-00-00.backup", "backup/bk1", "/bk1", "/backups/bk1"} {
		assert.Empty(t, RestorePVCPathWarnings(storage(pvc(), path)), path)
	}
	s3 := &neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b"}
	assert.Empty(t, RestorePVCPathWarnings(storage(s3, "/backup/bk1")), "only a PVC is mounted at /backup")

	pitr := &neo4jv1beta1.Neo4jRestore{Spec: neo4jv1beta1.Neo4jRestoreSpec{Source: neo4jv1beta1.RestoreSource{
		Type: "pitr",
		PITR: &neo4jv1beta1.PITRConfig{BaseBackup: &neo4jv1beta1.BaseBackupSource{
			Type: "storage", Storage: pvc(), BackupPath: "/backup/base",
		}},
	}}}
	warnings = RestorePVCPathWarnings(pitr)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], `spec.source.pitr.baseBackup.backupPath "/backup/base"`)
}
