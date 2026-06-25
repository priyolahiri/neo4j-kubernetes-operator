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

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ResolvedShardedSeed captures the outcome of resolving
// `spec.seedBackupRef` on a Neo4jShardedDatabase. Either URI (cloud) or
// PerShardURIs (PVC) is populated, never both.
type ResolvedShardedSeed struct {
	// URI is a directory seedURI (cloud-backed backups) consumed by
	// `OPTIONS { seedURI: '<URI>' }` in the CREATE DATABASE Cypher.
	URI string
	// PerShardURIs maps shard name (e.g. "products-g000") to an HTTP URL
	// served by the operator-managed PVC seed proxy. Populated when the
	// backup is PVC-storage; consumed under the SINGULAR Cypher key with a
	// map value: OPTIONS { seedURI: { <shard>: '<URL>', ... } } — the same
	// seedURI key as the cloud single-URI case (only the CR spec field for
	// the map is the plural seedURIs).
	PerShardURIs map[string]string
	// CredsSecretName is the cloud creds Secret to project onto cluster
	// pods for cloud seedURIs. Empty for PVC (the proxy is in-cluster
	// HTTP, no creds needed) and for backups that use workload identity.
	CredsSecretName string
	// ProxyAvailable is meaningful only for PVC-backed restores: true
	// when the proxy Deployment reports ≥1 Ready replica. False means
	// the caller should route to Pending and requeue.
	ProxyAvailable bool
}

// resolveShardedSeed resolves spec.seedBackupRef on a Neo4jShardedDatabase
// into either a cloud directory URI or a per-shard HTTP-proxy URL map,
// depending on the backup's storage type.
//
// Returns:
//   - (nil, nil)  when SeedBackupRef is empty — the caller falls through
//     to the existing manual spec.SeedURI / spec.SeedURIs paths.
//   - (nil, wrapped ErrBackupNotReady)  when the backup CR exists but
//     has no Succeeded run yet. Caller should route to Pending + requeue.
//   - (nil, err)  for permanent failures (missing backup CR, missing
//     shard artifact metadata for PVC, unsupported storage type).
//   - (resolved, nil)  on success. Inspect resolved.URI vs
//     resolved.PerShardURIs to pick the right OPTIONS clause.
//
// For PVC-backed backups: the operator creates a per-shardedDB
// HTTP-proxy Deployment + Service mounting the backup PVC RO and serving
// shard `.backup` files over HTTP. Cluster pods use Neo4j's
// URLConnectionSeedProvider via per-shard URLs.
func (r *Neo4jShardedDatabaseReconciler) resolveShardedSeed(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) (*ResolvedShardedSeed, error) {
	if shardedDB.Spec.SeedBackupRef == "" {
		return nil, nil
	}

	storage, backupPath, err := ResolveBackupRef(ctx, r.Client, shardedDB.Spec.SeedBackupRef, shardedDB.Namespace)
	if err != nil {
		return nil, err
	}

	// Classify the referenced backup and locate THIS family's shard artifacts.
	// A single-family ShardedDatabase-scoped backup records run.ShardArtifacts
	// (the shard names are always present, from the CR spec); an all-databases
	// backup records each captured family under run.ShardedFamilies. The two
	// seed differently — see below.
	run, err := r.findSeedRun(ctx, shardedDB, backupPath)
	if err != nil {
		return nil, err
	}
	isAllDB := len(run.ShardArtifacts) == 0 && len(run.ShardedFamilies) > 0
	famArtifacts := run.ShardArtifacts
	if isAllDB {
		// Which source family to pull from the all-databases backup: the source
		// name (spec.seedSourceDatabase) when restoring under a different target
		// name, else the target's own name (restore-in-place).
		sourceFamily := shardedDB.Spec.Name
		if shardedDB.Spec.SeedSourceDatabase != "" {
			sourceFamily = shardedDB.Spec.SeedSourceDatabase
		}
		if famArtifacts = findFamilyArtifacts(run, sourceFamily); famArtifacts == nil {
			return nil, fmt.Errorf("seed: Neo4jBackup %q is an all-databases backup that did not capture sharded family %q (status.shardedFamilies has: %s) — set spec.seedSourceDatabase to one of those, or back up the family with a shardedDatabase-scoped Neo4jBackup", shardedDB.Spec.SeedBackupRef, sourceFamily, familyNames(run.ShardedFamilies))
		}
	}

	credsSecretName := ""
	if storage.Cloud != nil {
		credsSecretName = storage.Cloud.CredentialsSecretRef
	}

	switch storage.Type {
	case "s3", "gcs", "azure":
		if isAllDB {
			// The all-databases backup directory holds EVERY database's files,
			// so a directory-scan seedURI can't isolate this family (and can't
			// remap a renamed target). Build explicit per-shard cloud URIs.
			perShard, err := buildPerShardCloudURIs(storage, backupPath, shardedDB.Spec.Name, famArtifacts)
			if err != nil {
				return nil, err
			}
			return &ResolvedShardedSeed{PerShardURIs: perShard, CredsSecretName: credsSecretName, ProxyAvailable: true}, nil
		}
		// Single-family: the directory holds only this family's shards →
		// CloudSeedProvider directory URI.
		uri, err := buildSeedURIFromBackupStorage(storage, backupPath)
		if err != nil {
			return nil, err
		}
		return &ResolvedShardedSeed{URI: uri, CredsSecretName: credsSecretName}, nil

	case "pvc":
		// PVC always uses per-shard proxy URLs (single-family or all-DB family).
		return r.resolvePVCShardedSeed(ctx, shardedDB, storage, backupPath, famArtifacts)

	default:
		return nil, fmt.Errorf("seedBackupRef does not support storage type %q", storage.Type)
	}
}

// findSeedRun fetches the referenced Neo4jBackup and returns the Succeeded
// BackupRun matching the resolved BackupsPath (the run whose artifacts we seed).
func (r *Neo4jShardedDatabaseReconciler) findSeedRun(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, backupPath string) (*neo4jv1beta1.BackupRun, error) {
	backup := &neo4jv1beta1.Neo4jBackup{}
	if err := r.Get(ctx, types.NamespacedName{Name: shardedDB.Spec.SeedBackupRef, Namespace: shardedDB.Namespace}, backup); err != nil {
		return nil, fmt.Errorf("seed: re-fetch backup %q: %w", shardedDB.Spec.SeedBackupRef, err)
	}
	for i := range backup.Status.History {
		if backup.Status.History[i].BackupsPath == backupPath && backup.Status.History[i].Status == "Succeeded" {
			return &backup.Status.History[i], nil
		}
	}
	return nil, fmt.Errorf("seed: no Succeeded run with BackupsPath %q on Neo4jBackup %q", backupPath, shardedDB.Spec.SeedBackupRef)
}

// findFamilyArtifacts returns the per-shard artifacts for the named logical
// family from an all-databases run's ShardedFamilies, or nil if absent.
func findFamilyArtifacts(run *neo4jv1beta1.BackupRun, family string) []neo4jv1beta1.ShardArtifact {
	for i := range run.ShardedFamilies {
		if run.ShardedFamilies[i].Family == family {
			return run.ShardedFamilies[i].ShardArtifacts
		}
	}
	return nil
}

// familyNames lists the family names catalogued in an all-databases run, for
// error messages.
func familyNames(fams []neo4jv1beta1.ShardedFamilyArtifacts) string {
	names := make([]string, 0, len(fams))
	for i := range fams {
		names = append(names, fams[i].Family)
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// buildPerShardCloudURIs builds an explicit per-shard cloud seedURI map for an
// all-databases backup: each TARGET shard (the restoring family's shard name)
// maps to the exact source `.backup` file's cloud URI. This isolates one family
// from the shared all-databases directory and remaps a renamed target (e.g.
// restoring source "products" into target "products-restored"). Mirrors the PVC
// proxy path's key transformation, with cloud URIs instead of HTTP URLs.
func buildPerShardCloudURIs(storage neo4jv1beta1.StorageLocation, backupPath, targetFamily string, artifacts []neo4jv1beta1.ShardArtifact) (map[string]string, error) {
	missing := []string{}
	for _, a := range artifacts {
		if a.Filename == "" {
			missing = append(missing, a.ShardName)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("cloud per-shard seed: shards %v have empty Filename in backup status.history — re-run the backup so per-shard filenames are captured", missing)
	}

	var scheme string
	switch storage.Type {
	case "s3":
		scheme = "s3"
	case "gcs":
		scheme = "gs"
	case "azure":
		scheme = "azb"
	default:
		return nil, fmt.Errorf("buildPerShardCloudURIs: unsupported storage type %q", storage.Type)
	}

	dir := strings.TrimRight(storage.Path, "/")
	if backupPath != "" {
		if dir != "" {
			dir += "/"
		}
		dir += strings.Trim(backupPath, "/")
	}

	perShard := make(map[string]string, len(artifacts))
	for _, a := range artifacts {
		suffix := shardSuffix(a.ShardName)
		if suffix == "" {
			return nil, fmt.Errorf("cloud per-shard seed: cannot parse shard suffix from %q — expected `<name>-(g|p)NNN`", a.ShardName)
		}
		targetShard := targetFamily + suffix
		filePath := strings.TrimLeft(strings.TrimRight(dir, "/")+"/"+a.Filename, "/")
		perShard[targetShard] = fmt.Sprintf("%s://%s/%s", scheme, storage.Bucket, filePath)
	}
	return perShard, nil
}

// resolvePVCShardedSeed handles the PVC-backed branch of resolveShardedSeed.
// Builds per-shard URLs against the operator-managed seed proxy from the
// already-resolved per-shard artifacts — a single-family backup's
// run.ShardArtifacts, or one family's artifacts pulled from an all-databases
// backup's run.ShardedFamilies (resolveShardedSeed picks which and passes them
// in). Requires per-shard filenames; if the backup didn't capture them, returns
// an error directing the user to re-run the backup.
func (r *Neo4jShardedDatabaseReconciler) resolvePVCShardedSeed(
	ctx context.Context,
	shardedDB *neo4jv1beta1.Neo4jShardedDatabase,
	storage neo4jv1beta1.StorageLocation,
	backupPath string,
	artifacts []neo4jv1beta1.ShardArtifact,
) (*ResolvedShardedSeed, error) {
	if storage.PVC == nil || storage.PVC.Name == "" {
		return nil, fmt.Errorf("PVC-backed seedBackupRef requires the backup's storage.pvc.name to be set")
	}
	if len(artifacts) == 0 {
		return nil, fmt.Errorf("PVC seed: no shard artifacts resolved for %q — re-run the backup with a recent operator version (per-shard filename capture required)", shardedDB.Spec.SeedBackupRef)
	}

	// Validate that filenames are captured (ShardName-only entries won't have
	// the per-shard filenames the proxy serves).
	missingFilenames := []string{}
	for _, a := range artifacts {
		if a.Filename == "" {
			missingFilenames = append(missingFilenames, a.ShardName)
		}
	}
	if len(missingFilenames) > 0 {
		return nil, fmt.Errorf("PVC seed: shards %v have empty Filename in backup status.history — Pod-log parsing didn't capture them. Re-run the backup, or use a cloud-backed seedBackupRef instead", missingFilenames)
	}

	// Ensure the proxy exists + is Ready. If not Ready yet, route to Pending so
	// the next reconcile picks it up.
	available, err := r.ensurePVCSeedProxy(ctx, shardedDB, storage.PVC.Name)
	if err != nil {
		return nil, err
	}

	// Key transformation: Neo4j matches `seedURIs` entries against the TARGET
	// database's shard names, not the SOURCE backup's. Strip the source DB name
	// prefix from each shard (e.g. "products-g000") and rebuild against the
	// target (e.g. "products-restored-g000"). Shard suffix (`-g000` / `-pNNN`)
	// carries the per-shard role; everything before is just the DB name.
	//
	// Assumes source + target sharded DBs share the same shard count and
	// graph/property layout; resharding-on-restore would need a different code
	// path.
	perShard := make(map[string]string, len(artifacts))
	for _, a := range artifacts {
		suffix := shardSuffix(a.ShardName)
		if suffix == "" {
			return nil, fmt.Errorf("PVC seed: cannot parse shard suffix from %q — expected `<name>-(g|p)NNN`", a.ShardName)
		}
		targetShardName := shardedDB.Spec.Name + suffix
		perShard[targetShardName] = pvcSeedProxyURLForShard(shardedDB.Name, shardedDB.Namespace, backupPath, a.Filename)
	}

	return &ResolvedShardedSeed{
		PerShardURIs:   perShard,
		ProxyAvailable: available,
	}, nil
}

// shardSuffixRegex extracts the trailing `-g000` / `-pNNN` portion of a
// shard name. Used to map source shard names (e.g. "products-g000") to
// target shard names (e.g. "products-restored-g000") for PVC-seeded
// sharded restores.
var shardSuffixRegex = regexp.MustCompile(`-(?:g|p)\d{3}$`)

func shardSuffix(shardName string) string {
	return shardSuffixRegex.FindString(shardName)
}

// buildSeedURIFromBackupStorage converts a backup's resolved StorageLocation
// + per-run backupPath into the directory URI that the Neo4j sharded
// CloudSeedProvider expects. The trailing slash is critical: without it,
// Neo4j's seed code treats the value as a single artifact path rather than
// a directory to scan for per-shard files.
//
// Currently supports s3:// (S3 / MinIO / R2 / etc.), gs:// (GCS), and azb://
// (Azure Blob). PVC and other storage types return an explanatory error.
func buildSeedURIFromBackupStorage(storage neo4jv1beta1.StorageLocation, backupPath string) (string, error) {
	basePath := storage.Path
	var fullPath string
	switch {
	case basePath != "" && backupPath != "":
		fullPath = fmt.Sprintf("%s/%s", strings.TrimRight(basePath, "/"), backupPath)
	case basePath != "":
		fullPath = basePath
	case backupPath != "":
		fullPath = backupPath
	}
	// Trailing slash → directory semantics for the CloudSeedProvider.
	if !strings.HasSuffix(fullPath, "/") {
		fullPath += "/"
	}

	switch storage.Type {
	case "s3":
		return fmt.Sprintf("s3://%s/%s", storage.Bucket, strings.TrimLeft(fullPath, "/")), nil
	case "gcs":
		return fmt.Sprintf("gs://%s/%s", storage.Bucket, strings.TrimLeft(fullPath, "/")), nil
	case "azure":
		return fmt.Sprintf("azb://%s/%s", storage.Bucket, strings.TrimLeft(fullPath, "/")), nil
	case "pvc", "":
		return "", fmt.Errorf("seedBackupRef requires cloud-backed backup storage (s3, gcs, azure); got %q — copy backup artifacts to a cloud bucket or set spec.seedURI manually", storage.Type)
	default:
		return "", fmt.Errorf("seedBackupRef does not support storage type %q", storage.Type)
	}
}
