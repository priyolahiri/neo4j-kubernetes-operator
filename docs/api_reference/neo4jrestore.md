# Neo4jRestore API Reference

This document provides a comprehensive reference for the `Neo4jRestore` Custom Resource Definition (CRD). This resource is used to restore Neo4j Enterprise databases from backups, including support for point-in-time recovery (PITR) and both cluster and standalone deployments.

For practical examples and usage guidance, see the [Backup and Restore Guide](../user_guide/guides/backup_restore.md).

## API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1beta1`
- **Kind**: `Neo4jRestore`

## How it works

The operator restores **online**, against the running DBMS, so only the database being restored is unavailable. The Neo4j docs flag `neo4j-admin database restore` as **unsafe on clusters**, so a cluster always takes the online path; a standalone takes it too, except in the [offline cases](#stopcluster-and-offline-restore-standalone-targets) where the server cannot fetch the seed itself.

**Online path — `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`** — Cypher over Bolt, no Job:

1. Resolves `source.backupRef` (or `source.storage`) into the exact `.backup` **file** URI of the latest successful run, e.g. `s3://bucket/path/<cr-name>/<dbname>-<timestamp>.backup`. Neo4j's `CloudSeedProvider` seeds a single database from one file (a directory URI fails with `Can't open seed file`); when that file is a differential, Neo4j resolves and applies the full + differential chain from the same directory automatically.
   - **Mixed-cadence chains (`spec.chainFromBackup`)**: "latest successful run" is scoped to the **referenced CR**, not the shared chain directory. Reference the **differential** CR to restore the latest state; referencing the parent **full** CR seeds from its latest full snapshot — the newer diffs are *not* applied. Restoring via a chain-parent CR emits a `RestoreFromChainParent` Warning event naming the differential children, so a restore intending "latest" that references the full CR isn't a silent surprise. To pin an arbitrary run, use `source.type: storage` with `backupPath` set to the exact `.backup` file.
   - For **PVC-backed** backups, the operator spawns a `backup-seed-proxy-<restore-name>` Deployment + Service serving the backup PVC read-only over HTTP, plus a **NetworkPolicy restricting proxy ingress to the target's server pods** (effective on enforcing CNIs). The seedURI becomes an `http://` URL at the exact `.backup` filename (`URLConnectionSeedProvider`). That provider fetches one file, so the artifact must be a **full** backup — Neo4j refuses a differential without its chain. The operator reads each run's `FULL`/`DIFF` from the backup's `status.history[].artifactType`: a cluster refuses a PVC differential before creating anything, and a standalone restores it offline. The whole proxy stack is **torn down automatically** as soon as the restore reaches `Completed` or `Failed` (and on CR deletion) — it does not linger for the lifetime of the CR.
2. Projects cloud credentials onto the server pods via `spec.extraEnvFrom` (on the cluster or standalone CR) — required so the JVM's AWS/GCP/Azure SDK can authenticate. The operator emits an actionable error if the Secret isn't projected; set the annotation `neo4j.com/auto-inherit-seed-creds=true` on the cluster or standalone to auto-patch (triggers a rolling restart, which the restore waits out in `Pending`). With **workload identity** (no `credentialsSecretRef`), the SDK default chain is used — the IAM binding must then be on the **server pods'** ServiceAccount, since the seed fetch runs inside the Neo4j JVM, not in a Job. (Only a cluster can annotate it, through `spec.podServiceAccountAnnotations`; a standalone restoring by pod identity runs offline.)
3. Opens a Bolt session and runs `SHOW DATABASES` to detect whether the target database already exists.
4. Existing database → `CALL dbms.[cluster.]recreateDatabase($db, {seedURI: $uri})`. **Requires `spec.options.replaceExisting: true`** — recreating wipes and replaces the live contents, so the operator refuses without the explicit opt-in (clear `Failed` message instead of a silent overwrite). Preserves user/role privileges, atomically swaps the database on every server, no `DROP` needed.
5. New database → `CREATE DATABASE $db OPTIONS { seedURI: '<file>' } WAIT` (no opt-in needed; nothing is overwritten).
6. `CREATE … WAIT` blocks until online, but `dbms.recreateDatabase` is **asynchronous** — it returns once the recreate is scheduled. The operator therefore polls `SHOW DATABASE` until every allocation reports `online` before marking the restore `Completed`. The poll deadline is **`spec.timeout`** (default **5m** when unset) — raise it for multi-GB stores seeded from object storage; on expiry the restore goes `Failed` with the database's last `statusMessage`.

7. `spec.options.preRestore` hooks run before the restore is issued, `postRestore` hooks once the database is online; a failing post-restore hook fails the restore.

**Offline path — `Neo4jEnterpriseStandalone` only, in the [offline cases](#stopcluster-and-offline-restore-standalone-targets)** — Kubernetes Job:

1. Spawns a restore Job that runs `neo4j-admin database restore --from-path=$(ls <dir>/<dbname>-*.backup | tail -1) <dbname>`. The shell substitution picks the latest run in the chain by default.
2. If `stopCluster: true`, the operator scales down the StatefulSet first and mounts the standalone's data PVC (`neo4j-data-{name}-0`) directly into the Job container for offline access. With `stopCluster: false` the operator **refuses to start the Job while any server pod is running** (it never writes into a live data volume) — so in practice standalone restores need `stopCluster: true` unless you have already scaled the instance down yourself.
3. After the Job succeeds, automatically runs `CREATE DATABASE <dbname>` (new) or `START DATABASE <dbname>` (existed but stopped) via Bolt.
4. `spec.options.preRestore` hooks run **before** the instance is stopped (so Cypher like `CALL db.checkpoint()` hits a live Bolt endpoint); `postRestore` hooks run **after** the restored database is registered and started.

**`Neo4jShardedDatabase` target** — rejected. Sharded restore is owned by the `Neo4jShardedDatabase` CRD; set `spec.replaceExisting: true` + `spec.force: true` on the target sharded DB and reference the backup via `spec.seedBackupRef`. The Neo4jRestore validator emits an actionable error pointing at this flow.

**No manual post-restore Cypher is required** for either path — with one exception: if the backup was taken with `includeMetadata` (users/roles), `neo4j-admin database restore` writes a `restore_metadata.cypher` script next to the restored store, and the operator does **not** run it. Re-creating the database's users/roles is a manual step — run it against the `system` database, e.g.:

```bash
# Standalone pod; the script exists only after a neo4j-admin (Job-path) restore
kubectl exec <instance>-0 -c neo4j -- bash -c \
  'cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -d system \
     -f /data/scripts/<dbname>/restore_metadata.cypher'
```

### Retrying a finished restore

`Completed` and `Failed` are terminal **for the current spec generation**, not forever. To re-run a restore, **edit the spec** (any change that bumps `metadata.generation` — typically `source.backupRef` or `database`): the operator clears the previous attempt's one-shot state and issues a fresh restore. Changing `source.backupRef` also invalidates the pinned `status.resolvedSource`, so the new reference is re-resolved rather than silently reusing the old snapshot. Alternatively, delete and recreate the CR.

## Neo4jRestore Spec

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `instanceRef` | `string` | ✅ | The Neo4j deployment to restore into — a `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone` (topology-agnostic; the operator picks the restore engine). |
| `source` | [`RestoreSource`](#restoresource) | ✅ | Source of the backup data to restore |
| `database` | `string` | ✅ (or `allDatabases`) | Name of the database to restore. |
| `allDatabases` | `bool` | ❌ | Restore **every** user database recorded in the source backup (the `system` database is excluded) — the restore counterpart of an all-databases backup ([#222](https://github.com/priyolahiri/neo4j-kubernetes-operator/issues/222)). Requires `source.type=backup`; mutually exclusive with `database`; per-database progress in `status.databaseResults`. Restores one database per reconcile pass via the online Cypher path (cloud and PVC-backed backups), on clusters and standalones alike (`options.replaceExisting: true` to overwrite existing databases). In the [offline cases](#stopcluster-and-offline-restore-standalone-targets) a standalone ([#288](https://github.com/priyolahiri/neo4j-kubernetes-operator/issues/288)) instead runs a single multi-database `neo4j-admin database restore` Job (needs `stopCluster: true`), after which each database is brought online. |
| `options` | [`RestoreOptionsSpec`](#restoreoptionsspec) | ❌ | Additional restore configuration options — including `replaceExisting` (the confirmation required to overwrite an existing database). |
| `stopCluster` | `bool` | ❌ | **Offline standalone restores only** — online restores (every cluster restore, and most standalone ones) ignore it, with an event on a standalone. `true` scales the instance down before the restore Job (mounting the data PVC `neo4j-data-{name}-0` directly) and scales it back up after. With `false`, the operator **refuses** to run the Job while any server pod is running — it never writes into a live data volume — and says why the restore is offline. See [the offline cases](#stopcluster-and-offline-restore-standalone-targets). |
| `timeout` | `string` | ❌ | Go duration (e.g. `"30m"`, `"2h"`). For **online** restores (clusters, and most standalone ones) this bounds the online-convergence wait after `dbms.recreateDatabase` is issued (default **5m** when unset) — raise it for multi-GB stores seeded from object storage. For **PVC-backed online restores** it also bounds the wait for the backup-seed-proxy Deployment to become Ready (default **3m** when unset); on expiry the restore fails with the proxy pod's condition (e.g. an RWO backup PVC still attached elsewhere). |

> The pre-v1.13 top-level `clusterRef`, `databaseName`, and `force` fields were deprecated in v1.13 and **removed in v1.14**. Use `instanceRef`, `database`, and `options.replaceExisting` respectively.

**Target compatibility**: `instanceRef` can reference either:

- `Neo4jEnterpriseCluster` — for HA cluster restore operations
- `Neo4jEnterpriseStandalone` — for single-node restore operations

### RestoreSource

Defines the source of the backup to restore from.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | ✅ | Type of restore source. Valid values: `"backup"`, `"storage"`, `"pitr"`. |
| `backupRef` | `string` | ❌ | Name of a `Neo4jBackup` resource to restore from (used when `type="backup"`) |
| `storage` | [`StorageLocation`](#storagelocation) | ❌ | Direct storage location (used when `type="storage"`). The cloud backend — `s3`, `gcs`, `azure`, or `pvc` — is selected on `storage.type` inside this struct, *not* on the outer `source.type`. |
| `backupPath` | `string` | conditional | Specific backup path within the storage location. **Required (non-empty) when `type="storage"`** — the controller fails the restore otherwise; for cluster targets it must be the exact `.backup` file. On a standalone it may be the directory or the exact `.backup` file; restoring under a different name from a cloud directory needs the exact file. Not used for `type="backup"` (resolved from the `Neo4jBackup`). |
| `sourceDatabase` | `string` | ❌ | The database to take from the backup when it is not `spec.database` — restoring under a different name. Needed only when the backup holds more than one database (an `allDatabases` backup, or a storage directory); a backup of one database already says which it is, and naming a different one is refused. Defaults to `spec.database`. Not allowed with `spec.allDatabases`, or as `system`. |
| `pointInTime` | `*metav1.Time` | ❌ | Recovery point in RFC3339 format; maps to `--restore-until`. **Standalone targets only** — a cluster target rejects it (use a `Neo4jDatabase` with `spec.seedConfig.restoreUntil`). |
| `pitr` | [`PITRConfig`](#pitrconfig) | conditional | Full PITR configuration. **Required when `type="pitr"`**, together with `pitr.baseBackup` and/or `pointInTime`. |

**Valid `type` values:**

| Value | Description |
|-------|-------------|
| `"backup"` | Restore from a `Neo4jBackup` CR referenced by `backupRef` |
| `"storage"` | Restore from an explicit storage location in `storage`. The cloud backend is set via `storage.type` (`s3` / `gcs` / `azure` / `pvc`). |
| `"pitr"` | Point-in-time recovery using transaction log replay |

**Examples:**

```yaml
# Restore from a Neo4jBackup resource
source:
  type: backup
  backupRef: daily-production-backup

# Restore from an explicit S3 path. backupPath is relative to storage.path and
# must point at the exact .backup artifact (layout:
# <chain-root>/<dbname>-YYYY-MM-DDThh-mm-ss.backup — the chain root is the
# Neo4jBackup CR name; the filename is recorded on
# Neo4jBackup.status.history[*].artifactFilename).
source:
  type: storage
  storage:
    type: s3
    bucket: neo4j-backups
    path: production
    cloud:
      provider: aws
      credentialsSecretRef: aws-restore-credentials
  backupPath: daily-backup/mydb-2026-06-01T02-00-00.backup

# Point-in-time recovery
source:
  type: pitr
  pointInTime: "2025-01-04T12:30:00Z"
  pitr:
    baseBackup:
      type: backup
      backupRef: daily-backup
    logStorage:
      type: s3
      bucket: neo4j-transaction-logs
      path: production/logs/
      cloud:
        provider: aws
```

> **Note:** `source.type: pitr` (the `--restore-until` path) applies only to a `Neo4jEnterpriseStandalone` target. For cluster point-in-time recovery, create a `Neo4jDatabase` with `spec.seedConfig.restoreUntil`. The operator rejects `source.type: pitr` against a cluster target with an actionable error.

### PITRConfig

Point-in-time recovery configuration for advanced restore scenarios.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `baseBackup` | [`BaseBackupSource`](#basebackupsource) | ❌ | Base backup to restore before applying transaction logs |
| `logStorage` | [`StorageLocation`](#storagelocation) | ❌ | Storage location for transaction logs |

### BaseBackupSource

Base backup configuration for PITR (avoids circular references).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | ✅ | Source type: `"backup"` or `"storage"` |
| `backupRef` | `string` | ❌ | Name of the `Neo4jBackup` resource (when `type="backup"`) |
| `storage` | [`StorageLocation`](#storagelocation) | ❌ | Direct storage location (when `type="storage"`) |
| `backupPath` | `string` | ❌ | Specific backup path within the storage location |

### RestoreOptionsSpec

Additional restore execution options.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `replaceExisting` | `bool` | ❌ | Confirm restoring **over an existing database** (default: `false`). Required before the operator overwrites existing contents on **either** target: cluster restores issue the destructive `dbms.recreateDatabase`; standalone restores pass `--overwrite-destination` to `neo4j-admin`. |
| `additionalArgs` | `[]string` | ❌ | Additional arguments passed verbatim to `neo4j-admin database restore` |
| `tempPath` | `string` | ❌ | Local directory for temporary files during restore. When `tempStorage` is configured this is set automatically to the mount path; only set manually if you mount your own volume by other means. |
| `tempStorage` | [`TempStorageSpec`](#tempstoragespec) | ❌ | Provisions a PVC for temporary staging files during cloud restores. Without it, cloud restores use the container's ephemeral disk (may be too small for large databases). The operator mounts the PVC and passes `--temp-path` automatically. |
| `resources` | `corev1.ResourceRequirements` | ❌ | CPU/memory requests + limits on the restore Job's container. When unset, the operator applies a Burstable default (request 100m CPU / 512Mi memory, limit 1 CPU / 2Gi memory). **Standalone restores only** — cluster targets use the Cypher path (no Job) and ignore this field. |
| `preRestore` | [`RestoreHooks`](#restorehooks) | ❌ | Executed **before anything is restored** (offline, before the instance is stopped), so Cypher hooks (e.g. `CALL db.checkpoint()`) hit a live Bolt endpoint. Once per attempt, including for an all-databases restore. |
| `postRestore` | [`RestoreHooks`](#restorehooks) | ❌ | Executed once the restored database is online (offline: after the restore Job succeeds **and** `CREATE`/`START DATABASE` ran), so Cypher hooks target a live database. A failing hook fails the restore. |

### TempStorageSpec

Provisions a temporary PVC for staging files during cloud restores.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `size` | `string` | ✅ | Size of the temporary PVC (e.g., `"50Gi"`). Should be at least as large as the expected backup/restore artifact. |
| `storageClassName` | `string` | ❌ | StorageClassName for the temporary PVC. If empty, uses the cluster default. |

### RestoreHooks

Hooks to run before or after the restore, on every target — online and offline. (Cluster restores used to skip them.)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `job` | [`RestoreHookJob`](#restorehookjob) | ❌ | Kubernetes Job to run as a hook. The hook Job runs in its own pod — commands like `cypher-shell` must be pointed at the instance's service explicitly (e.g. `cypher-shell -a neo4j://<standalone>-client:7687 …`), otherwise they dial localhost inside the hook pod. |
| `cypherStatements` | `[]string` | ❌ | Cypher statements the operator executes over Bolt against the standalone instance (pre-restore: before the instance is stopped; post-restore: after the database is registered/started) |

### RestoreHookJob

Kubernetes Job configuration for restore hooks.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `template` | [`JobTemplateSpec`](#jobtemplatespec) | ✅ | Job template specification |
| `timeout` | `string` | ❌ | Timeout for the hook Job (e.g., `"30m"`) |

### JobTemplateSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `container` | [`ContainerSpec`](#containerspec) | ✅ | Container specification |
| `backoffLimit` | `*int32` | ❌ | Job backoff limit |

### ContainerSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `image` | `string` | ✅ | Container image |
| `command` | `[]string` | ❌ | Entrypoint command |
| `args` | `[]string` | ❌ | Arguments to pass to the command |
| `env` | `[]EnvVar` | ❌ | Environment variables |

### StorageLocation

Storage backend configuration (shared with `Neo4jBackup`).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | ✅ | Storage backend: `"s3"`, `"gcs"`, `"azure"`, `"pvc"` |
| `bucket` | `string` | ❌ | Bucket or container name (required for cloud types) |
| `path` | `string` | ❌ | Path within the storage location |
| `cloud` | [`CloudBlock`](#cloudblock) | ❌ | Cloud provider configuration including optional credentials secret |
| `pvc` | [`PVCSpec`](#pvcspec) | ❌ | PVC configuration (when `type="pvc"`) |

### PVCSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | `string` | ❌ | Name of an existing PVC to use |
| `storageClassName` | `string` | ❌ | Storage class name |
| `size` | `string` | ❌ | Size for a new PVC (e.g., `"100Gi"`) |

### CloudBlock

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | `string` | ❌ | Cloud provider: `"aws"`, `"gcp"`, `"azure"` |
| `credentialsSecretRef` | `string` | ❌ | Name of a Kubernetes Secret containing cloud credentials as environment variables. When absent, ambient workload identity is used. |
| `endpointURL` | `string` | ❌ | Overrides the S3 API endpoint URL. Use to target S3-compatible stores such as MinIO, Ceph RGW, or Cloudflare R2. |
| `forcePathStyle` | `bool` | ❌ | Forces S3 path-style addressing (bucket name in the URL path rather than as a subdomain). Typically required for S3-compatible stores. |
| `identity` | [`*CloudIdentity`](#cloudidentity) | ❌ | Cloud identity configuration |

### CloudIdentity

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | `string` | ✅ | Identity provider: `"aws"`, `"gcp"`, `"azure"` |
| `serviceAccount` | `string` | ❌ | **RESERVED — currently a no-op.** Restore Jobs always run as the operator-managed `neo4j-restore-sa`. Bind your cloud IAM role via `autoCreate.annotations`. A `ValidationWarning` event is raised when it is set. |
| `autoCreate` | [`*AutoCreateSpec`](#autocreatespec) | ❌ | Workload-identity annotations for the operator-managed ServiceAccount |

### AutoCreateSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | `bool` | ❌ | **RESERVED — currently a no-op.** The operator always ensures the ServiceAccount exists; only `annotations` is honored. A `ValidationWarning` event is raised when it is set to `false` (the schema default is `true`, so only `false` is detectable). |
| `annotations` | `map[string]string` | ❌ | Annotations applied to the operator-managed ServiceAccount on every reconcile |

## Neo4jRestore Status

### Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `conditions` | `[]metav1.Condition` | Current conditions of the restore resource — see [Condition Types](#condition-types) |
| `phase` | `string` | Current phase of the restore operation |
| `message` | `string` | Human-readable message about the current state |
| `startTime` | `*metav1.Time` | When the restore operation started |
| `completionTime` | `*metav1.Time` | When the restore operation completed |
| `stats` | [`RestoreStats`](#restorestats) | Restore operation statistics — populated on success with the restore `duration`. |
| `backupInfo` | [`RestoreBackupInfo`](#restorebackupinfo) | Provenance of the backup this restore was seeded from — populated on success (see [RestoreBackupInfo](#restorebackupinfo)). |
| `resolvedSource` | [`ResolvedRestoreSource`](#resolvedrestoresource) | The concrete backup location this restore pinned the first time it resolved `source.backupRef`. From then on the restore reads this snapshot, so deleting the `Neo4jBackup` CR mid-restore doesn't break it. Cleared automatically if `spec.source.backupRef` changes (the new reference is re-resolved). |
| `databaseResults` | `[]DatabaseRestoreResult` | **(v1.13)** Per-database progress for an all-databases restore (`spec.allDatabases`): one entry per user database with `database`, `phase` (`Pending`/`Running`/`Completed`/`Failed`), `message`, and `completionTime`. Empty for single-database restores. |
| `observedGeneration` | `int64` | Generation of the most recently observed `Neo4jRestore` spec |

### ResolvedRestoreSource

| Field | Type | Description |
|-------|------|-------------|
| `backupRef` | `string` | The `Neo4jBackup` name this source was resolved from (provenance) |
| `storage` | [`StorageLocation`](#storagelocation) | Concrete storage location (PVC or cloud, credentials reference folded in) of the resolved backup |
| `backupPath` | `string` | The per-CR shared chain directory of the resolved most-recent Succeeded run |
| `artifactFilename` | `string` | Exact `.backup` filename of the resolved run — required by the online Cypher paths (cloud seedURI + PVC proxy), which seed from a single file |
| `artifactType` | `string` | `FULL` or `DIFF`, from the resolved run's `artifactType` (or `FULL` for an unrecorded run of a backup whose `backupType` is `FULL`); empty when unknown. A PVC seed must be `FULL` |
| `databaseArtifacts` | `[]DatabaseArtifact` | **(v1.13)** Per-database `.backup` map pinned from the resolved backup's latest Succeeded run, driving an all-databases restore (`spec.allDatabases`). Empty for single-database restores. |
| `shardedDatabasesExcluded` | `[]string` | Logical property-sharded databases the all-databases **restore loop** does not recreate (carried forward from `Neo4jBackup` `status.history[].shardedDatabasesExcluded`). The restore surfaces these (`RestoreShardedDatabasesNotCovered` warning) — but they **are** restorable from the same backup: re-apply each one's `Neo4jShardedDatabase` CR with `spec.seedBackupRef` pointing at this backup (its per-shard files are in the backup's `shardedFamilies`). Empty otherwise. |
| `resolvedAt` | `*metav1.Time` | When the `backupRef` was first dereferenced |
| `backupCreatedAt` | `*metav1.Time` | Completion time of the resolved most-recent Succeeded run, captured at resolution so restore provenance survives deletion of the source `Neo4jBackup` CR |

### RestoreStats

| Field | Type | Description |
|-------|------|-------------|
| `duration` | `string` | Wall-clock time the restore took, derived from `completionTime − startTime` (e.g. `"2m3s"`). Populated on success. |

> **(v1.14)** `RestoreStats` was trimmed to the one field the operator can derive without the backup Pod's log. The former `dataSize`, `throughput`, `fileCount`, and `errorCount` fields were never populated and were removed.

### RestoreBackupInfo

Provenance about the backup this restore was seeded from, populated on success. Fields the operator cannot derive without the (possibly-deleted) source backup are omitted rather than guessed.

| Field | Type | Description |
|-------|------|-------------|
| `backupPath` | `string` | Source backup location: the per-CR chain directory (with the resolved `.backup` filename when known) for a `source.type: backup` restore, or the explicit storage path for a `source.type: storage` restore |
| `backupCreatedAt` | `*metav1.Time` | Completion time of the backup run this restore was seeded from. Populated only for `source.type: backup` restores; empty for `source.type: storage` (no backup CR) |
| `originalDatabase` | `string` | The logical database that was restored. Empty for an all-databases restore (see [`databaseResults`](#status-fields) for per-database detail) |

> **(v1.14)** `RestoreBackupInfo` was trimmed to the fields derivable from the resolved source. The former `neo4jVersion` (the backup run records no version) and `backupSize` (Pod-log only) fields were never populated and were removed.

### Restore Phases

| Phase | Description |
|-------|-------------|
| `Pending` | A transient precondition isn't met yet — the target cluster/standalone CR doesn't exist yet, the referenced backup has no Succeeded run, or (cluster restores) the seed-credentials rollout / seed proxy is still in progress. The controller requeues and retries automatically. |
| `Running` | The restore Job (standalone) is executing, or the cluster Cypher recreate has been issued and the operator is polling for online convergence. |
| `Completed` | Restore completed successfully; the database is online. Terminal for the current spec generation — see [Retrying a finished restore](#retrying-a-finished-restore). |
| `Failed` | The restore failed (validation, Job failure, seed failure, or convergence timeout). Terminal for the current spec generation — bump the spec or recreate the CR to retry. |

### Condition Types

The operator maintains a single **`Ready`** condition, derived from `phase`: `Completed` → `True`; `Failed` → `False` (reason `Failed`); `Pending`/`Running` → `Unknown`. No other condition types are set.

## Post-Restore Database Bring-Up (standalone Job path)

After the restore Job completes successfully on a **standalone** target, the operator automatically issues a Cypher command to make the database available:

- **New database** (did not exist before): `CREATE DATABASE <dbname>`
- **Existing database** (was stopped for restore): `START DATABASE <dbname>`

This means the restore workflow is fully automated — you do not need to manually start the database after restore completes. The `status.phase` transitions to `Completed` only after the database bring-up (and any post-restore hooks) succeed.

Cluster targets don't have a separate bring-up step: the Cypher restore (`CREATE DATABASE … OPTIONS { seedURI } WAIT` / `dbms.recreateDatabase`) brings the database online as part of the restore itself, and the operator marks `Completed` only after every allocation reports `online`.

> **Note on the legacy multi-server re-seed**: older operator releases ran the restore Job against `data-{cluster}-server-0` on multi-server clusters and then called `dbms.[cluster.]recreateDatabase($db, {seedingServers: [server0]})` to force every server to re-seed from server-0. On current releases, **cluster targets never take the Job path** — they restore via the seedURI Cypher path, where every server seeds from the backup artifact in parallel and no re-seed step is needed. The re-seed code remains only as a non-fatal safety net on the Job path (which is now reachable only for standalone/single-server targets, where it is skipped).

## `stopCluster` and Offline Restore (standalone targets)

`spec.stopCluster` applies only to a standalone restore that runs **offline**. Every cluster restore, and every other standalone restore, runs online and ignores it. A standalone restores offline when the server cannot fetch the seed itself:

| Case | Why |
|---|---|
| Point-in-time (`source.type: pitr`, or `source.pointInTime`) | `dbms.recreateDatabase` has no restore-until option |
| `source.type: storage` | the path may be a directory or part of a chain; only `neo4j-admin` resolves those |
| A PVC artifact that is a differential (`status.history[].artifactType: DIFF` — `backupType: AUTO` writes one on every run after the first) | the seed proxy serves one file, and Neo4j refuses a differential without its chain |
| A PVC artifact whose type was not recorded (unless the backup's `backupType` is `FULL`) | it might be a differential |
| Cloud storage with no `credentialsSecretRef` | a standalone's pods cannot carry a workload identity; the Job's ServiceAccount can |
| A Job restore already holding the instance (`neo4j.com/restore-in-progress`), e.g. across an operator upgrade | it finishes where it started |

The operator emits a `RestoreStarted` event naming the case.

When `spec.stopCluster: true`:

1. The operator scales the target StatefulSet down to 0 replicas (recording the original replica count so re-entries don't lose it).
2. The restore Job is created with the standalone's actual data PVC (`neo4j-data-{name}-0`) mounted into the container, enabling direct offline file-level restore.
3. After the restore Job succeeds, the StatefulSet is scaled back up. If the restore fails after the scale-down (hook failure, Job-create failure), the operator scales the instance back up rather than leaving it stranded at 0 replicas.
4. The operator then issues `CREATE DATABASE` or `START DATABASE` as described above.

When `spec.stopCluster: false`, the operator treats the instance as **already quiesced** and **refuses to start the Job while any server pod is running** — restoring into a live data volume is invisible data loss. The refusal names the case that made the restore offline. Use `stopCluster: true` unless you have scaled the instance down yourself.

## Examples

### Simple Restore from a Neo4jBackup Reference

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: simple-backup-restore
  namespace: neo4j
spec:
  instanceRef: test-cluster   # Neo4jEnterpriseCluster (Cypher path — no Job)
  database: testdb
  source:
    type: backup
    backupRef: daily-test-backup   # References a Neo4jBackup resource
  options:
    replaceExisting: true   # required if "testdb" already exists on the cluster
  timeout: "30m"   # online-convergence budget for the cluster restore (default 5m)
```

### Restore from S3 (Static Credentials, Standalone)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: dev-s3-restore
  namespace: development
spec:
  instanceRef: dev-standalone   # Neo4jEnterpriseStandalone (source.type storage: offline Job path)
  database: dev-app
  source:
    type: storage
    storage:
      type: s3
      bucket: dev-neo4j-backups
      path: snapshots
      cloud:
        provider: aws
        credentialsSecretRef: aws-restore-credentials
    # Exact artifact: <chain-root>/<dbname>-<timestamp>.backup
    backupPath: dev-backup/dev-app-2026-06-01T10-30-00.backup
  options:
    replaceExisting: true   # confirms overwriting the existing database
    postRestore:   # once the restored database is online
      cypherStatements:
        - "CALL db.awaitIndexes(60)"
        - "CREATE (:TestNode {restored: datetime()})"
  stopCluster: true   # an offline standalone restore needs the instance stopped
  timeout: "30m"
```

### Restore from GCS (Static Credentials, Cluster)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: gcs-restore
  namespace: neo4j
spec:
  instanceRef: analytics-cluster   # Neo4jEnterpriseCluster (Cypher path)
  database: analytics-db
  source:
    type: storage
    storage:
      type: gcs
      bucket: neo4j-analytics-backups
      path: weekly
      cloud:
        provider: gcp
        credentialsSecretRef: gcs-restore-credentials
    # Cluster targets REQUIRE the exact .backup file (CloudSeedProvider seeds
    # a single database from one file — a directory path fails):
    backupPath: weekly-backup/analytics-db-2026-06-01T03-00-00.backup
  options:
    replaceExisting: true   # required: analytics-db already exists
  timeout: "2h"   # online-convergence budget — raise for large stores
```

> For the cluster Cypher path the credentials Secret must also be projected onto the **server pods** via the cluster's `spec.extraEnvFrom` (or annotate the cluster with `neo4j.com/auto-inherit-seed-creds: "true"` and let the operator patch it).

### Restore from Azure Blob Storage (Cluster)

Cluster targets restore via Cypher — `additionalArgs` and `stopCluster` belong to the offline standalone Job path and are intentionally absent here.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: enterprise-azure-restore
  namespace: neo4j
  labels:
    compliance: required
    environment: production
spec:
  instanceRef: enterprise-cluster   # Neo4jEnterpriseCluster (Cypher path)
  database: customer-data
  source:
    type: storage
    storage:
      type: azure
      bucket: enterprise-backups   # Azure container name
      path: production
      cloud:
        provider: azure
        credentialsSecretRef: azure-restore-credentials
    # Exact .backup file, required for cluster targets:
    backupPath: nightly-backup/customer-data-2026-06-01T02-00-00.backup
  options:
    replaceExisting: true   # required: customer-data already exists
  timeout: "6h"   # large store — generous online-convergence budget
```

### Point-in-Time Recovery (PITR)

> **Note:** `source.type: pitr` applies only to a `Neo4jEnterpriseStandalone` target. For cluster point-in-time recovery, create a `Neo4jDatabase` with `spec.seedConfig.restoreUntil`. The operator rejects `source.type: pitr` against a cluster target with an actionable error.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: production-pitr-restore
  namespace: neo4j
spec:
  instanceRef: recovery-standalone   # Neo4jEnterpriseStandalone (PITR is standalone-only)
  database: production-db
  source:
    type: pitr
    pointInTime: "2025-01-04T12:30:00Z"
    pitr:
      baseBackup:
        type: backup
        backupRef: daily-production-backup
      logStorage:
        type: s3
        bucket: neo4j-transaction-logs
        path: production/logs
        cloud:
          provider: aws
          credentialsSecretRef: aws-restore-credentials
  options:
    replaceExisting: true   # confirms overwriting the existing database
    preRestore:     # BEFORE the restore (and, offline, before the stop)
      cypherStatements:
        - "CALL db.checkpoint()"
    postRestore:    # AFTER the restored database is registered/started
      cypherStatements:
        - "CALL db.awaitIndexes(600)"
        - "CALL dbms.security.clearAuthCache()"
  stopCluster: true
  timeout: "4h"
```

### Offline Restore via stopCluster (Standalone)

`stopCluster` applies only to a standalone restore that runs [offline](#stopcluster-and-offline-restore-standalone-targets) — here, because the source is a `source.type: storage` path. When `stopCluster: true`, the operator scales the standalone to 0 and mounts its data PVC (`neo4j-data-{name}-0`) directly into the restore Job for a cold/offline restore.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: offline-restore
  namespace: neo4j
spec:
  instanceRef: reporting-standalone   # Neo4jEnterpriseStandalone
  database: large-graph
  source:
    type: storage
    storage:
      type: s3
      bucket: neo4j-backups
      path: large-graph
      cloud:
        provider: aws
        credentialsSecretRef: aws-restore-credentials
    backupPath: nightly-backup/large-graph-2026-06-01T02-00-00.backup
  options:
    replaceExisting: true   # confirms overwriting the existing database
  stopCluster: true   # Scales the standalone to 0; mounts neo4j-data-reporting-standalone-0
  timeout: "8h"
```

### Standalone Restore

`instanceRef` can reference a `Neo4jEnterpriseStandalone` resource. The controller detects the type automatically, and restores online like a cluster: the instance keeps running and its other databases stay online.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: standalone-restore
  namespace: development
spec:
  instanceRef: dev-standalone   # Neo4jEnterpriseStandalone
  database: app-db
  source:
    type: backup
    backupRef: dev-daily-backup
  options:
    replaceExisting: true   # confirms overwriting the existing database
  timeout: "30m"
```

### Cross-Cloud Disaster Recovery (Cluster)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata:
  name: cross-cloud-dr-restore
  namespace: disaster-recovery
spec:
  instanceRef: dr-cluster   # Neo4jEnterpriseCluster (Cypher path — no Job)
  database: critical-app
  source:
    type: storage
    storage:
      type: gcs
      bucket: primary-site-backups
      path: disaster-recovery
      cloud:
        provider: gcp
        credentialsSecretRef: gcs-dr-credentials
    # Exact .backup file, required for cluster targets:
    backupPath: dr-backup/critical-app-2026-06-01T02-00-00.backup
  options:
    replaceExisting: true   # confirms overwriting the existing database
  timeout: "3h"
```

## Monitoring

```bash
# List all restore resources
kubectl get neo4jrestore -n neo4j

# Watch restore status
kubectl get neo4jrestore production-pitr-restore -w

# View detailed status and events
kubectl describe neo4jrestore production-pitr-restore

# Check restore phase
kubectl get neo4jrestore production-pitr-restore -o jsonpath='{.status.phase}'

# Check restore statistics
kubectl get neo4jrestore production-pitr-restore -o jsonpath='{.status.stats}'

# Monitor restore Job logs (standalone targets only — cluster targets restore
# via Cypher and create no Job; follow the operator log instead)
kubectl logs -n neo4j job/production-pitr-restore-restore --follow

# Check completion time
kubectl get neo4jrestore production-pitr-restore -o jsonpath='{.status.completionTime}'
```

## Version-Specific Notes

### Neo4j 5.26.x

- Restore command: `neo4j-admin database restore`
- Key flags: `--from-path` (source), `--overwrite-destination` (not `--force`)
- PITR flag: `--restore-until` in RFC3339 format
- Automatic database state management via `STOP DATABASE` / `START DATABASE`

### Neo4j 2025.x (CalVer)

- Same restore command structure as 5.26.x
- Enhanced metadata restoration
- Additional `--restore-until` precision for PITR scenarios

## Version Requirements

- **Neo4j Version**: 5.26.0+ (semver) or 2025.01.0+ (CalVer)
- **Kubernetes**: 1.19+
- **Operator**: Latest version with restore support

## Related Resources

- [`Neo4jBackup`](neo4jbackup.md) — Backup operations
- [`Neo4jEnterpriseCluster`](neo4jenterprisecluster.md) — Target cluster resource
- [Backup and Restore Guide](../user_guide/guides/backup_restore.md) — Usage examples and best practices
