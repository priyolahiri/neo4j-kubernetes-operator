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

// BackupModeReplicationSource is the spec.mode value marking a Neo4jBackup as
// the differential chain a cross-cluster replica pulls from.
const BackupModeReplicationSource = "replication-source"

// ValidateReplicationSourceMode enforces the extra constraints a backup must
// satisfy when it feeds a cross-cluster replica (design §4.4, rules R1–R5).
//
// `siblings` is every other Neo4jBackup in the same namespace, used for the
// competing-writer check (R3). Pass nil to skip that rule when the caller has
// no list available.
//
// What this CANNOT check, and why the caller must warn regardless: for cloud
// storage the operator never prunes — RetentionPolicy delegates to bucket
// lifecycle rules it can neither read nor validate. A lifecycle rule expiring
// old objects breaks the chain silently. These rules narrow the footgun
// surface; they do not close it.
func ValidateReplicationSourceMode(
	backup *neo4jv1beta1.Neo4jBackup,
	siblings []neo4jv1beta1.Neo4jBackup,
) field.ErrorList {
	var errs field.ErrorList

	if backup.Spec.Mode != BackupModeReplicationSource {
		return errs
	}

	specPath := field.NewPath("spec")

	// R1 — single-database scope. An instance-wide backup produces an
	// aggregate artifact layout that a per-database pullURI cannot consume.
	if backup.Spec.AllDatabases {
		errs = append(errs, field.Invalid(specPath.Child("allDatabases"), true,
			"mode=replication-source requires single-database scope; a cross-cluster replica pulls one "+
				"database's chain and cannot consume the instance-wide artifact layout. Set spec.database instead"))
	}
	if backup.Spec.ShardedDatabase != "" {
		errs = append(errs, field.Invalid(specPath.Child("shardedDatabase"), backup.Spec.ShardedDatabase,
			"mode=replication-source does not support property-sharded databases in this release"))
	}
	if backup.Spec.Database == "" && !backup.Spec.AllDatabases && backup.Spec.ShardedDatabase == "" {
		errs = append(errs, field.Required(specPath.Child("database"),
			"mode=replication-source requires spec.database"))
	}

	// R2 — no operator-side retention. For PVC storage the delete-time
	// cleanup Job would prune a differential's parent and break the chain.
	if backup.Spec.Retention != nil {
		errs = append(errs, field.Invalid(specPath.Child("retention"), backup.Spec.Retention,
			"mode=replication-source forbids an operator-side retention policy: pruning an artifact that a "+
				"later differential chains from breaks the replica and forces a rebuild. Note that for cloud "+
				"storage the operator does not prune at all — retention there is delegated to bucket lifecycle "+
				"rules, which the operator cannot see and which will break the chain just as effectively"))
	}

	// R4 — a schedule is required. A replication source with no cadence is a
	// replica that falls arbitrarily far behind.
	if strings.TrimSpace(backup.Spec.Schedule) == "" {
		errs = append(errs, field.Required(specPath.Child("schedule"),
			"mode=replication-source requires a schedule; without one the chain never advances and the "+
				"replica's lag grows without bound"))
	}

	// R3 — no competing writer to the same storage location. The existing
	// part-of label interlock serialises Jobs *within* one chain; it does not
	// stop an unrelated CR from writing into the same directory.
	for i := range siblings {
		other := &siblings[i]
		if other.Name == backup.Name && other.Namespace == backup.Namespace {
			continue
		}
		if !sameStorageLocation(backup.Spec.Storage, other.Spec.Storage) {
			continue
		}
		// Same chain is fine — that is the daily-FULL + hourly-DIFF pattern.
		if other.Spec.ChainFromBackup == backup.Name || backup.Spec.ChainFromBackup == other.Name {
			continue
		}
		if other.Spec.ChainFromBackup != "" && other.Spec.ChainFromBackup == backup.Spec.ChainFromBackup {
			continue
		}
		errs = append(errs, field.Invalid(specPath.Child("storage"), backup.Spec.Storage,
			fmt.Sprintf("Neo4jBackup %q writes to the same storage location and is not part of this chain; "+
				"a second writer can interleave artifacts and break the differential chain a replica depends "+
				"on. Give this replication source its own storage.path, or chain the other CR from it via "+
				"spec.chainFromBackup", other.Name)))
	}

	return errs
}

// sameStorageLocation reports whether two backups write to the same place.
func sameStorageLocation(a, b neo4jv1beta1.StorageLocation) bool {
	if a.Type != b.Type {
		return false
	}
	if a.Bucket != b.Bucket {
		return false
	}
	return strings.TrimSuffix(a.Path, "/") == strings.TrimSuffix(b.Path, "/")
}

// ReplicationPullURI builds the object-storage directory a downstream replica
// should pull from, for publishing to status.replicationPullURI.
//
// Returns "" when the storage type has no URI form a replica can consume —
// notably PVC-backed storage, which is not reachable from another Kubernetes
// cluster.
func ReplicationPullURI(storage neo4jv1beta1.StorageLocation, backupsPath string) string {
	var scheme string
	switch storage.Type {
	case "s3":
		scheme = "s3://"
	case "gcs":
		scheme = "gs://"
	case "azure":
		scheme = "azb://"
	default:
		// pvc (or anything unknown): not addressable cross-cluster.
		return ""
	}

	parts := []string{strings.Trim(storage.Bucket, "/")}
	if p := strings.Trim(storage.Path, "/"); p != "" {
		parts = append(parts, p)
	}
	if p := strings.Trim(backupsPath, "/"); p != "" {
		parts = append(parts, p)
	}
	return scheme + strings.Join(parts, "/") + "/"
}
