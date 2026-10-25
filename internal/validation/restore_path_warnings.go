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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// restorePVCMountPath is where a restore mounts a PVC source: the restore Job
// at /backup, and the seed proxy serves the same root. A PVC backupPath is
// relative to the PVC's root, and the operator adds this prefix itself.
const restorePVCMountPath = "/backup"

// RestorePVCPathWarnings reports a PVC backupPath that starts with the mount
// path /backup. Such a path is relative to the PVC's root like any other, so
// the operator's own prefix makes `/backup/bk1` read `backup/bk1` on the PVC
// (the Job's `/backup//backup/bk1`), which fails unless the PVC has a `backup`
// directory at its root (2026-10-25 walk). Advisory: that directory may exist.
// Restore has no validator type, so this is a package function the reconciler
// records with its other advisories (knowledge operations.md id 102).
func RestorePVCPathWarnings(restore *neo4jv1beta1.Neo4jRestore) []string {
	src := restore.Spec.Source
	sourcePath := field.NewPath("spec", "source")
	var warnings []string
	if src.Type == "storage" && isPVCStorage(src.Storage) {
		warnings = append(warnings, pvcBackupPathWarnings(sourcePath.Child("backupPath"), src.BackupPath)...)
	}
	if src.PITR != nil && src.PITR.BaseBackup != nil {
		base := src.PITR.BaseBackup
		if base.Type == "storage" && isPVCStorage(base.Storage) {
			warnings = append(warnings, pvcBackupPathWarnings(
				sourcePath.Child("pitr", "baseBackup", "backupPath"), base.BackupPath)...)
		}
	}
	return warnings
}

func isPVCStorage(storage *neo4jv1beta1.StorageLocation) bool {
	return storage != nil && storage.Type == "pvc"
}

func pvcBackupPathWarnings(path *field.Path, backupPath string) []string {
	if backupPath != restorePVCMountPath && !strings.HasPrefix(backupPath, restorePVCMountPath+"/") {
		return nil
	}
	onPVC := strings.TrimPrefix(backupPath, "/")
	fix := fmt.Sprintf("write %q", strings.TrimPrefix(strings.TrimPrefix(backupPath, restorePVCMountPath), "/"))
	if strings.Trim(strings.TrimPrefix(backupPath, restorePVCMountPath), "/") == "" {
		fix = "leave out the /backup prefix"
	}
	return []string{fmt.Sprintf(
		"%s %q starts with %s, where the restore mounts the PVC: the path is relative to the PVC's root and the "+
			"operator adds %s/ itself, so this names %s on the PVC. Unless the PVC has a backup directory at its "+
			"root, %s",
		path.String(), backupPath, restorePVCMountPath, restorePVCMountPath, onPVC, fix)}
}
