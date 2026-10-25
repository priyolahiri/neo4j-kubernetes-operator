# Knowledge Base: Backup & Restore

> **Status: EXEMPLAR.** This is the gold-standard knowledge-base entry — the
> template every other subsystem should follow. It re-homes the backup/restore
> regression rules (the old CLAUDE.md "rules 40–79") into structured,
> enforcement-tagged entries. CLAUDE.md no longer carries the checklist; this
> file is the single home.
>
> Every symbol (file path, function, test) below was verified against the tree
> at the time of writing. Stale references found in the old checklist are called
> out inline with a **STALE REF** badge and the corrected current name. When you
> change backup/restore code, update the matching entry here in the same PR —
> the `Pinned-by` test is the machine-checked half of the contract; this entry
> is the human-readable half.

## How to read this file

Each rule is a numbered entry. **Rule IDs are preserved from the original
CLAUDE.md checklist for traceability** — they are intentionally non-contiguous
(some original numbers were merged or retired upstream; 54, 62, 68, 69, 73 do
not appear). Numbers are **per file**: `operations.md` also has an `id 80` and an
`id 81` (unrelated to Rules 80/81 here), so always cite the file with the number
(`backup-restore.md Rule 80`). Each entry carries:

- **Scope** — the file(s) the rule governs.
- **Rule** — the invariant, stated imperatively.
- **Why** — the failure mode that justifies it (delete the rule and *this*
  breaks).
- **Pinned-by** — the test that mechanically enforces it, with file:identifier.
  `(none)` means no dedicated test guards this rule — treat it as documentation
  only and add a test if you touch the code.
- **Status** — `Enforced` (a verified test pins it), `Documented` (correct but
  no dedicated test), or `Stale-ref-corrected` (the original checklist named a
  symbol that no longer exists; the corrected name is in the entry).

## Architecture invariant (read first)

Backups are **Job-per-`Neo4jBackup`-CR exclusively**. There is no centralized
`{cluster}-backup` StatefulSet, no `spec.backups` field, no `BuildBackupStatefulSet`
builder, and no standalone backup sidecar — all of that was removed (see rule 79)
and must never be reintroduced. The `Neo4jBackup` CRD spawns a one-shot Job (or a
CronJob for scheduled backups); the `Neo4jRestore` CRD restores via Cypher for
cluster targets (rule 75) and via a Job for standalone targets.

`StorageLocation`, `CloudBlock`, `CloudIdentity`, and `AutoCreateSpec` types
survive (verified in `api/v1beta1/neo4jenterprisecluster_types.go`) because
`Neo4jBackup` / `Neo4jRestore` consume them.

---

## Backup: run identity, history & chaining

### Rule 40 — Shared `--to-path` per CR; per-run identity via filename
- **Scope:** `internal/controller/neo4jbackup_controller.go` (`buildToPath`, `chainRoot`, `backupRunIDEnvVar`, `jobToBackupRun`)
- **Rule:** All runs of one `Neo4jBackup` CR share a single `--to-path = <base>/<chain-root>/` (NOT per-run subfolders). Per-run identity is the ISO-8601 timestamp neo4j-admin embeds in each `.backup` filename, captured to `BackupRun.ArtifactFilename` (standard) / `ShardArtifacts.Filename` (sharded). The `BACKUP_RUN_ID` env var stays on the Pod (downward API → Job name) for log correlation only. One-shot Job name = `<backup>-backup`; CronJob child = `<cronjob>-<unix-seconds>`. Never re-introduce a `${BACKUP_RUN_ID}` subfolder under `--to-path`.
- **Why:** `neo4j-admin database backup --type=DIFF` reads the prior FULL artifact from the *same* directory to compute the delta. Per-run subfolders break chaining — every DIFF would fail to find its parent.
- **Pinned-by:** `internal/controller/neo4jbackup_cloud_test.go:TestBuildToPath_SharedDirectoryPerCR` (the shared, no-run-subfolder `--to-path`); the per-run-identity half by `:TestBackupRunIDEnvVar` + `internal/controller/neo4jbackup_history_test.go:TestJobToBackupRun`
- **Status:** Enforced

### Rule 41 — CronJob backup defaults are load-bearing
- **Scope:** `internal/controller/neo4jbackup_controller.go` (`createBackupCronJob`, `reconcileScheduledHistory`)
- **Rule:** Keep `ConcurrencyPolicy=Forbid`, `StartingDeadlineSeconds=60`, `TTLSecondsAfterFinished=1800`, `SuccessfulJobsHistoryLimit=10`, `FailedJobsHistoryLimit=3`.
- **Why:** These give `reconcileScheduledHistory` a 30-minute window to record a child Job in `status.history` before Kubernetes GCs it. Relaxing them risks losing run history.
- **Pinned-by:** (none — values asserted indirectly via scheduled-history tests in `internal/controller/neo4jbackup_history_test.go`)
- **Status:** Documented

### Rule 78 — `spec.chainFromBackup` composes mixed-cadence FULL+DIFF
- **Scope:** `internal/controller/neo4jbackup_controller.go` (`chainRoot`, `buildToPath`, `validateChainParent`, `waitForChainConcurrencyClear`, `errChainBusy`)
- **Rule:** A daily FULL and an hourly DIFF CR with `chainFromBackup: daily` share `<base>/<chain-root>/` so `--type=DIFF` finds the prior FULL. `chainRoot(backup)` returns `spec.chainFromBackup` if set, else `backup.Name` — used by `buildToPath`, `BackupRun.BackupsPath`, and the `app.kubernetes.io/part-of` Job label. The validator rejects self-reference; `validateChainParent` enforces parent existence + matching target + matching storage. The target's scoped name matches ignoring case: `Orders` and `orders` are one database, and neo4j-admin writes one chain of `orders-<timestamp>.backup` for both (operations.md id 146). `waitForChainConcurrencyClear` refuses to start while any Job with the same `part-of` label has `status.active>0`, routing `errChainBusy` to Pending+requeue.
- **Why:** Two backups writing the same directory concurrently corrupt neo4j-admin's chain detection. The PVC path additionally takes a `flock` on `<to-path>/.chain.lock` (`chainLockWaitSeconds`) because two CronJob children can still fire within the reconcile gap that the creation-time gate misses.
- **Pinned-by:** end-to-end by `test/integration/backup_chain_test.go` (the `It("composes a daily FULL + hourly DIFF chain via spec.chainFromBackup", …)` spec — verifies both Jobs share `part-of: inventory-daily` and the DIFF restore applies the full chain). Unit-level: `internal/controller/neo4jbackup_lifecycle_test.go:TestChainConcurrency_SeesCronJobChildren` + `:TestValidateChainParent_NotFoundIsTransient` + `:TestBuildRetentionScript_FileLevelChainScoped`; the name's case by `internal/controller/backup_restore_name_case_test.go:TestValidateChainParent_OneDatabaseInAnotherCase`; PVC chain-dir flock by `internal/controller/backup_restore_field_findings_test.go:TestBuildBackupCommand_PVCChainDirFlock`.
- **Status:** Enforced — the original checklist's `backup_chain_test.go` reference is **accurate**: it lives at `test/integration/backup_chain_test.go` (not under `internal/controller/`).

---

## Backup: resolver & transient-vs-terminal routing

### Rule 55 — `ResolveBackupRef` is the canonical name→StorageLocation resolver
- **Scope:** `internal/controller/backup_resolver.go` (`ResolveBackupRef`, `ErrBackupNotReady`)
- **Rule:** Its callers — `Neo4jRestore` (`neo4jrestore_controller.go`, via the `resolveBackupRef` wrapper and a direct call) and `Neo4jShardedDatabase` (`neo4jshardeddatabase_seed.go`) — delegate to `ResolveBackupRef(ctx, client.Reader, backupRef, namespace)`. (`Neo4jDatabase` does NOT use it: it has no `backupRef` field, only a user-supplied `seedURI` string.) It returns a wrapped `ErrBackupNotReady` when the backup exists but has no Succeeded run — callers `errors.Is` to route Pending+requeue. Never duplicate the lookup; never compare error strings.
- **Why:** A single resolver keeps the "backup not ready yet" → transient routing consistent. String-matching error messages (the pre-fix approach) misclassified RBAC denials and other API failures as "not found".
- **Pinned-by:** `internal/controller/neo4jrestore_cloud_test.go:TestResolveRestoreSource_BackupRefNoSucceededRun_IsTransient` + `:TestResolveRestoreSource_BackupRefMissingCR_IsPermanent`
- **Status:** Enforced — note the canonical sentinel is the exported `ErrBackupNotReady`; the package-internal alias `errBackupNotReady` in `neo4jrestore_controller.go` is kept for backward compat (new code uses `ErrBackupNotReady`).

### Rule 43 — `errBackupNotReady` → Pending, not Failed
- **Scope:** `internal/controller/backup_resolver.go`, `internal/controller/neo4jrestore_controller.go` (`startRestore`)
- **Rule:** `ResolveBackupRef` wraps `ErrBackupNotReady` via `fmt.Errorf("…%w")` when history has no Succeeded run; `startRestore` uses `errors.Is` and routes Pending+requeue. Pending is NOT in the "previously failed" guard set, so it re-promotes. Missing-CR errors stay terminal Failed.
- **Why:** `kubectl apply -f dir/` commonly creates the restore before its backup completes. A terminal Failed here is pinned forever (the previously-failed guard never re-enters) even after the backup succeeds.
- **Pinned-by:** `internal/controller/neo4jrestore_cloud_test.go:TestResolveRestoreSource_BackupRefNoSucceededRun_IsTransient` + `:TestResolveRestoreSource_BackupRefMissingCR_IsPermanent`
- **Status:** Enforced

### Rule 42 — `source.type: backup` resolved upstream; the `case "backup"` is dead-code
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`resolveRestoreSource`, `buildRestoreCommand`)
- **Rule:** `resolveRestoreSource` swaps `Spec.Source` away from `"backup"` on a shallow restore copy and threads the concrete `StorageLocation` through every builder. `buildRestoreCommand`'s `case SourceTypeBackup:` is therefore unreachable and carries a defensive `internal:` error (`"source.type=backup reached buildRestoreCommand without being resolved"`).
- **Why:** Without upstream dereference, `type=backup` restores silently pointed at an empty volume (the legacy `case "backup"` branch hardcoded `/backup/<backup-name>` over an EmptyDir).
- **Pinned-by:** (none dedicated — exercised indirectly via the resolver tests in `internal/controller/neo4jrestore_cloud_test.go`)
- **Status:** Documented

---

## Standalone restore (Job + `neo4j-admin restore`)

### Rule 44 — Standalone `--from-path` is a FILE via shell substitution; `tail -1` picks the latest
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`resolveLocalPVCFromPath`, `latestArtifactSubstitution`, `latestArtifactGlob`, `standaloneRestoreFromPath`, `buildRestoreFromPath`); `internal/validation/restore_path_warnings.go` (`RestorePVCPathWarnings`)
- **Rule:** `resolveLocalPVCFromPath(backupPath, databaseName)` emits `$(ls '<backupPath>'/'<dbname lower-cased>'-[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*.backup | tail -1)` (`latestArtifactSubstitution`). **BOTH the path AND the database name MUST go through `shellQuote()`.** `tail -1` (not `head -1`) selects the latest run in the shared chain dir (rule 40), because neo4j-admin's ISO-8601 timestamp sorts lexicographically into chronological order. The name is lower-cased and the glob carries the timestamp's shape because that is how neo4j-admin names an artifact: `<database>-<yyyy-MM-dd'T'HH-mm-ss>.backup`, with the database name normalized to lower case (operations.md id 146). Cloud URIs skip this (neo4j-admin's native readers handle file selection). Never pass the directory; never substitute the timestamp in Go; never drop quoting; never revert to `head -1`, to the spec's spelling of the name, or to a bare `-*.backup`.
  - **A PVC `backupPath` is relative to the PVC's root.** The restore Job mounts the PVC at `/backup` and `buildRestoreFromPath` prefixes `/backup/` (the seed proxy serves the same root; `storage.path` is not added on either path), so `backupPath: /backup/bk1` read `/backup//backup/bk1` — `backup/bk1` on the PVC — and failed (2026-10-25 walk). The docs say so on the field, and a PVC `backupPath` (or `pitr.baseBackup.backupPath`) that is `/backup` or starts with `/backup/` draws an advisory `ValidationWarning` naming the path to write (`RestorePVCPathWarnings`, recorded with the restore's other advisories, operations.md ids 102 and 145). Advisory, not an error: the PVC may have a `backup` directory at its root.
- **Why:** `spec.source.backupPath` and `spec.databaseName` are user-controlled and land in a `/bin/sh -c` command in a Pod that mounts `/data` RW and carries `NEO4J_ADMIN_PASSWORD`. An unquoted value like `foo; rm -rf /data #` would escape the `ls` and run arbitrary commands. The glob found nothing for `spec.database: Products` (the artifact is `products-…`), and `orders-*.backup` also matched another database's `orders-eu-….backup`, which sorts after `orders-2…`, so `tail -1` restored the wrong database (#529).
- **Pinned-by:** `internal/controller/neo4jrestore_cloud_test.go:TestResolveLocalPVCFromPath_BackupPathShellInjectionGuard` + `:TestResolveLocalPVCFromPath_NestedCommandSubstitutionGuard` + `:TestResolveLocalPVCFromPath_EmbeddedSingleQuoteGuard`; `internal/controller/backup_restore_name_case_test.go:TestLatestArtifactSubstitution_FindsTheArtifactNeo4jAdminWrote` (the generated command run by `sh` over real files) + `:TestResolveLocalPVCFromPath_LowerCasesTheDatabase`; `internal/validation/restore_path_warnings_test.go:TestRestorePVCPathWarnings` and `internal/controller/restore_pvc_path_warning_test.go:TestRestoreReconcile_APVCBackupPathUnderTheMountPathIsAnnounced` (one warning over three passes; fails with the warning left out of the advisories)
- **Status:** Enforced — **NOTE:** the checklist named the helper `buildLocalRestoreFilePath`, a thin wrapper nothing called, removed as dead code (#533); the function that emits the `$(ls … | tail -1)` form (and that the three guard tests target) is `resolveLocalPVCFromPath` (through `latestArtifactSubstitution`), whose comment says why the substitution exists. No comment in `neo4jrestore_controller.go` says `head -1` any more.

### Rule 45 — Restore `--temp-path=/tmp/restore-tmp` default for PVC sources
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`buildRestoreCommand` PVC branch)
- **Rule:** PVC sources emit `--temp-path=/tmp/restore-tmp` plus a `rm -rf /tmp/restore-tmp && mkdir -p /tmp/restore-tmp &&` prelude (neo4j-admin needs an empty staging dir). Explicit `Options.TempStorage` / `Options.TempPath` win. Cloud URIs skip.
- **Why:** The backup PVC is mounted ReadOnly, so neo4j-admin can't extract in place — without `--temp-path` it fails with `FileSystemException: Read-only file system`.
- **Pinned-by:** (none dedicated — covered by `internal/controller/neo4jrestore_cloud_test.go` restore-command assertions)
- **Status:** Documented

### Rule 46 — Restore reconcile race tolerance
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`createRestoreJob`, `clearEarlierRestoreJob`, `restoreJobOfThisAttempt`, `AnnotationRestoreAttempt`, `startCluster`)
- **Rule:** Job creation treats `AlreadyExists` as "another reconcile got there first" and re-fetches; `startCluster` treats a missing `neo4j.neo4j.com/original-replicas` annotation as "first reconcile already deleted it" and returns nil. Don't revert either. **Only this attempt's Job is adopted (#565):** the Job carries the attempt — the restore's generation — in `neo4j.com/restore-attempt`; a Job under the restore's Job name that is terminating, controlled by another Neo4jRestore (one deleted and created again under the same name), or annotated with another attempt is never adopted — before `Create` it is deleted (whatever its state) and its name waited for up to 10s, and while it is still there the attempt goes `Pending` (`errStaleRestoreJob`) and retries. A Job from an operator that recorded no attempt counts as this attempt's unless the restore reads `Failed` or `Completed` (startRestore meets those only at a new generation).
- **Why:** Two reconciles race during the ~10s `stopCluster` scale-down window. Reverting either flips successful restores to terminal Failed. The stale-Job deletion of #254 ran only while the restore read `Failed`: a retry whose old Job was still terminating went `Pending`, and the next pass — `Pending`, not `Failed` — skipped it, hit `AlreadyExists` and adopted the failed Job, which failed the new attempt at once. A `Completed` restore run again within the Job's five-minute TTL adopted the completed Job and was `Completed` without restoring anything.
- **Pinned-by:** `TestStartRestore_ARetryThroughPendingNeverAdoptsTheEarlierJob` (waits out the 10s poll), `TestStartRestore_AFreshAttemptRunsOnAJobOfItsOwn`, `TestStartRestore_AdoptsThisAttemptsJob` (the race this rule keeps) and `TestRestoreJobOfThisAttempt_NotAnotherRestoresJob` (`internal/controller/restore_job_attempt_test.go`); the first two fail with the `Failed`-only deletion and unconditional adoption restored. The race itself is exercised by the coordination specs in `internal/controller/neo4jrestore_coordination_test.go`.
- **Status:** Documented

### Rule 47 — Legacy post-restore re-seed via `dbms.[cluster.]recreateDatabase` (Job/standalone path only)
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`recreateRestoredDatabaseOnCluster`), `internal/neo4j/version.go` (`RecreateDatabaseProcedure`)
- **Rule:** After the Job-based restore writes server-0's PVC, `recreateRestoredDatabaseOnCluster` forces all servers to re-seed from server-0. server-0 is matched by `cluster.Name + "-server-0"` against `SHOW SERVERS YIELD address` (the `name` column is unreliable). The procedure comes from `version.RecreateDatabaseProcedure()`: `dbms.cluster.recreateDatabase` (5.24–2025.03) → `dbms.recreateDatabase` (2025.04+). Skipped for standalone, `Topology.Servers < 2`, and pre-5.24 SemVer / pre-2025.02 CalVer. Non-fatal — failure emits a Warning `DatabaseCreateFailed`.
- **Why:** Post-restart bootstrap picks the database's primary non-deterministically; if a stale-data server wins consensus, the restored data is overwritten when others re-sync. The cluster Cypher path (rule 75) doesn't need this — it seeds every server from the URI in parallel.
- **Pinned-by:** procedure-name selection by `internal/neo4j/version_test.go` (`RecreateDatabaseProcedure` cases); controller wiring (none dedicated)
- **Status:** Documented

---

## Cluster restore (Cypher, never a Job)

### Rule 75 — Cluster `Neo4jRestore` uses Cypher, NOT a Job
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`startRestore`, `isRestoreTargetTrueCluster`, `startClusterCypherRestore`, `validateRestore`)
- **Rule:** `startRestore` branches via `isRestoreTargetTrueCluster`: cluster + standard DB → `startClusterCypherRestore` (`DatabaseExists` → `RecreateDatabaseWithSeedURI` if it exists, `CreateDatabaseWithSeedURIOptions` if not); standalone → the same online path, unless rule 108 sends it to the Job + `neo4j-admin restore`; sharded → rejected by `validateRestore` pointing at rule 63. Never re-introduce a cluster-target Job path.
- **Why:** `neo4j-admin restore --overwrite-destination` is unsafe on a cluster — clusters carry additional state that becomes inconsistent with a file-level restore.
- **Pinned-by:** (none dedicated unit test for the branch; integration coverage in `test/integration/standard_database_minio_restore_test.go`)
- **Status:** Documented

### Rule 76 — seedURI = exact `.backup` FILE; wait for online before Completed
- **Scope:** `internal/controller/neo4jrestore_controller.go` (`startClusterCypherRestore`, `pollClusterRestoreOnline`, `markCypherRestoreIssued`), `internal/neo4j/client.go` (`DatabaseOnlineState`, `DatabaseSeedFailureMessage`)
- **Rule:** The cluster seedURI is the exact `.backup` file, never a directory — Neo4j's seed providers seed a single DB from one file. Cloud → `buildSeedURIFromBackupStorage` builds the dir then the filename is appended via `resolvedOrLiveArtifactFilename` / `latestSucceededArtifactFilename` (latest Succeeded run); PVC → `resolveClusterPVCRestoreURI` spawns the seed proxy (rule 71) at the filename. A single DIFF-file URI suffices for cloud (`CloudSeedProvider`) and `file://` seeds — Neo4j resolves the full parent chain from the same dir — but **not** for a PVC seed: the proxy serves it over HTTP (`URLConnectionSeedProvider`), which fetches the one file, and Neo4j refuses a DIFF without its chain — so the proxy merges the chain into one full artifact first (rule 109). **After the asynchronous `dbms.recreateDatabase`, the operator MUST confirm the database converged online before marking Completed** — the procedure returns before the seed finishes, so a bare return is false-success. `CREATE DATABASE … OPTIONS{seedURI} WAIT` blocks; recreate does not.
- **Why:** A directory URI fails (`Can't open seed file: …/<chain-root>`). Declaring Completed on the recreate's early return reports success while the seed is still downloading (or has silently failed — `CREATE … WAIT` returns without error on a failed download and leaves the allocation offline with the reason in `SHOW DATABASE`'s statusMessage).
- **Pinned-by:** (none dedicated unit test; the online-wait/false-success guards are integration-verified in `test/integration/standard_database_minio_restore_test.go`)
- **Status:** Stale-ref-corrected — **STALE REF:** the checklist said the operator "MUST `WaitForDatabaseOnline` before marking Completed". **No function named `WaitForDatabaseOnline` exists.** The wait is implemented as: issue the recreate exactly once (guarded by the `neo4j.com/cypher-restore-issued` annotation), then hand off to the requeue-driven `pollClusterRestoreOnline`, which polls `DatabaseOnlineState` / `DatabaseSeedFailureMessage` per reconcile (bounded by `cypherRestoreOnlineTimeout` = 5m). The intent of the rule is correct; only the symbol name was wrong.

### Rule 77 — `RecreateDatabaseWithSeedURI` vs `RecreateDatabase` (seedingServers)
- **Scope:** `internal/neo4j/client.go` (`RecreateDatabaseWithSeedURI`, `RecreateDatabase`)
- **Rule:** `RecreateDatabaseWithSeedURI` is the cluster-native restore primitive — every server pulls from the URI in parallel, no Job. `RecreateDatabase` (seedingServerIDs) is the post-`neo4j-admin restore` consistency fix that picks server-0 as seed — legacy/standalone only (rule 47). Never use seedingServers where seedURI works.
- **Why:** seedingServers re-seeds from one server's local store, which only makes sense after a Job wrote that store; the seedURI variant is the correct cluster-wide path.
- **Pinned-by:** (none dedicated)
- **Status:** Documented

---

## Sharded backup & restore

### Rule 48 — Sharded backup names its shards with `{name}-g*` and `{name}-p*` (lower-cased) + always-quoted db args
- **Scope:** `internal/controller/neo4jbackup_controller.go` (`buildBackupCommand`), `internal/neo4j/version.go` (`GetBackupCommand`, `GetBackupCommandForDatabases`), `internal/controller/neo4jbackup_sharded.go` (`shardedBackupPatterns`, `matchesShardedBackupPattern`, `shardedGlobPoisoning`, `shardedPreflightGlobSafety`, `shardedPreflightStatic`, `shardedLogicalNameForBackup`)
- **Rule:** One `neo4j-admin database backup "{name}-g*" "{name}-p*"` captures every shard consistently in one run (`[<database>...]` takes several patterns; `*` is one or more characters, `ConfigPatternBuilder`). `{name}` is the logical name from the `Neo4jShardedDatabase`, lower-cased as Neo4j names the shards (`shardedLogicalNameForBackup`). `GetBackupCommandForDatabases` ALWAYS double-quotes each database arg so the shell can't pre-expand a pattern (`--to-path` is single-quoted via `shellQuoteArg`). `shardedPreflightGlobSafety` rejects any DB the patterns match outside `^{name}-(g|p)\d{3}$` (terminal Failed). `shardedPreflightStatic` routes a not-yet-Ready sharded DB to Pending. The optional validate step takes the parent name, `--database="{name}"`.
- **Why:** Without quoting, the shell expands `*` before neo4j-admin sees it. Without the glob-safety check, a backup for `products` could silently sweep in an unrelated `products-gold` database. The single pattern `{name}*` (the Operations Manual's sharded example, `"foo*"`) also took every database whose name merely begins with the family's: the v1.22.x walk restored `Products` beside itself as `ProductsCopy`, and `Products*` matched `productscopy-g000`, so the glob-safety check refused every later backup of `Products` (and, from a stale pass, wrote `Failed` over a `Completed` backup, Rule 113).
- **Pinned-by:** `internal/neo4j/version_test.go:TestGetBackupCommandQuotesShardedGlob` + `:TestGetBackupCommandQuotesPlainName`; `internal/controller/backup_walk_test.go:TestShardedBackup_NamesOnlyItsOwnShards` (the command for `spec.name: Products`, and a copy beside it not refused) + `internal/controller/database_name_case_test.go:TestShardedGlobPoisoning_IgnoresCase`; `internal/controller/neo4jbackup_sharded_test.go:TestBuildBackupCommand_ShardedDatabase_EmitsFlagsWithNilOptions`
- **Status:** Enforced

### Rule 49 — `--remote-address-resolution` is `*bool` with sharded-aware defaulting
- **Scope:** `internal/controller/neo4jbackup_sharded.go` / `internal/controller/neo4jbackup_controller.go` (`effectiveRemoteAddressResolution`, emitted in `buildBackupCommand`)
- **Rule:** `effectiveRemoteAddressResolution` defaults `true` ONLY when `kind=ShardedDatabase` AND Neo4j ≥ 2025.09 AND the user didn't set it. Explicit values win. The flag is emitted OUTSIDE the `Options != nil` guard (the sharded default fires even for a spec that sets only target + storage). Never re-introduce a `bool` zero-value default.
- **Why:** A `bool` field can't distinguish "user set false" from "unset", so the sharded default would clobber an explicit false; and gating emission on `Options != nil` would swallow the default for minimal specs.
- **Pinned-by:** `internal/controller/neo4jbackup_sharded_test.go:TestEffectiveRemoteAddressResolution`
- **Status:** Enforced

### Rule 50 — `IsClusterShardingReady` is the canonical sharding precondition
- **Scope:** `internal/validation/sharding.go` (`IsClusterShardingReady`), callers in `internal/controller/neo4jbackup_sharded.go` + `internal/validation/cluster_validator.go`
- **Rule:** `IsClusterShardingReady(cluster)` returns nil only when `cluster.spec.propertySharding.enabled=true` AND `IsNeo4jVersion202512OrHigher(image.tag)`. Used by the cluster validator + backup reconciler preflight; never inline the check at new call sites.
- **Why:** Centralizing the precondition keeps backup preflight and cluster validation in lockstep — a property-sharded backup must never run against a cluster that isn't configured for sharding on a sufficiently new Neo4j.
- **Pinned-by:** (none dedicated for the helper; `IsNeo4jVersion202512OrHigher` is exercised in `internal/resources` tests)
- **Status:** Documented

### Rule 51 — Sharded DB Ready signal is `Status.ShardingReady` (bool pointer)
- **Scope:** `api/v1beta1/neo4jshardeddatabase_types.go` (`Status.ShardingReady`), read in `internal/controller/neo4jbackup_sharded.go`
- **Rule:** Sharded-DB readiness for backups is `Neo4jShardedDatabase.Status.ShardingReady` (a `*bool`), NOT the generic `Ready` condition.
- **Why:** The generic Ready condition is coarser and would let backups run before all shards exist. (The *cluster* CR has a separate `Status.PropertyShardingReady` field — don't confuse the two.)
- **Pinned-by:** (none dedicated)
- **Status:** Documented

### Rule 52 — `Neo4jShardedDatabase.status.lastBackup` reverse-lookup is non-fatal observability
- **Scope:** `internal/controller/neo4jbackup_sharded.go` (`updateShardedDBLastBackup`), `internal/controller/neo4jbackup_controller.go` (`recordOneShotBackupRun`, `reconcileScheduledHistory`)
- **Rule:** `updateShardedDBLastBackup` populates the sharded DB CR's `status.lastBackup` from both the one-shot (`recordOneShotBackupRun`) and CronJob (`reconcileScheduledHistory`) paths. Only Succeeded runs update it; Failed runs don't overwrite. CR-not-found is logged and swallowed. The source of truth stays `Neo4jBackup.status.history`.
- **Why:** It's a UX hint so operators can audit backup health from the sharded DB CR without grepping `Neo4jBackup` CRs — never gate reconcile state on it.
- **Pinned-by:** (none dedicated)
- **Status:** Documented

### Rule 53 — `BackupRun.ShardArtifacts.ShardName` derived from spec, lower-cased
- **Scope:** `internal/controller/neo4jbackup_sharded.go` (`expectedShardArtifactsForBackup`)
- **Rule:** `expectedShardArtifactsForBackup` reads `propertySharding.propertyShards` from the `Neo4jShardedDatabase` spec and emits `{name}-g000` + `{name}-p000…p{N-1}`, `{name}` being `spec.name` lower-cased: the shard as Neo4j names it and as neo4j-admin writes it into its artifact and its log (operations.md id 146). `ShardName` is the audit-load-bearing field; `Filename`/`Size` are populated opportunistically by Pod-log parsing (rule 67) and stay informational.
- **Why:** The audit question "did all shards back up?" is answered by `ShardName` alone, which is derivable from the spec without log parsing. With the spec's capitals (`Products-g000`) no shard matched its log line: the run never had its file names, waited for them until the Job's TTL (Rule 174), and a seed from it failed with "shards … have empty Filename" (v1.22.x walk).
- **Pinned-by:** `internal/controller/backup_walk_test.go:TestShardedBackup_ACapitalisedNameRecordsItsShards`
- **Status:** Enforced by unit test

### Rule 63 — `replaceExisting` + `force` = destructive sharded restore
- **Scope:** `api/v1beta1/neo4jshardeddatabase_types.go`, `internal/controller/neo4jshardeddatabase_controller.go`
- **Rule:** Both `replaceExisting` and `force` true → `CYPHER 25 DROP DATABASE {name} IF EXISTS DESTROY DATA WAIT` before the standard CREATE. Validator: `replaceExisting=true` requires `force=true`; mutex with `ifNotExists=true`; requires a seed source. The DROP is idempotent across requeues.
- **Why:** Destructive data loss must require an explicit second confirmation field (`force`) so an accidental `replaceExisting` flip can't wipe a database.
- **Pinned-by:** `test/integration/property_sharding_minio_restore_test.go` (the `It("destructively replaces an existing sharded DB via replaceExisting+force", …)` spec, ~line 323)
- **Status:** Enforced (integration)

### Rule 64 — `Status.LastDestructiveRestoreGeneration` gates `replaceExisting`
- **Scope:** `api/v1beta1/neo4jshardeddatabase_types.go` (`Status.LastDestructiveRestoreGeneration`), `internal/controller/neo4jshardeddatabase_controller.go`
- **Rule:** The destructive branch fires only when `LastDestructiveRestoreGeneration < Generation`; it stamps `= Generation` on success. Re-trigger by mutating the spec (which bumps generation) — typically editing `seedBackupRef`.
- **Why:** Without the generation gate, every reconcile of a `replaceExisting=true` CR would re-drop and re-seed the database in a loop.
- **Pinned-by:** `TestDestructiveRestorePending_GatesOnGeneration` (`internal/controller/neo4jshardeddatabase_gate_test.go`) pins the controller's own predicate `destructiveRestorePending` (replaceExisting && force && `LastDestructiveRestoreGeneration < Generation`); the end-to-end stamp after a real destructive restore is also covered by `test/integration/property_sharding_minio_restore_test.go` (local-only; it skips in CI). The related *seed-consumable* gate (skip seed resolution once the database is Ready, #224) is pinned by `TestShardedSeedConsumableGate`, which now calls the controller's `shardedSeedConsumable` rather than a copy of it.
- **Status:** Enforced (unit test on the real predicate; integration stamp local-only)

### Rule 65 — Sharded DDL requires `CYPHER 25` prefix
- **Scope:** `internal/controller/neo4jshardeddatabase_controller.go` (CREATE / DROP statements)
- **Rule:** Every sharded CREATE/DROP statement must prepend `CYPHER 25`.
- **Why:** The 2026.x system DB defaults to Cypher 5; without the prefix the sharded syntax fails to parse. Same invariant as AUTH RULE (the user/auth subsystem's rule 30).
- **Pinned-by:** (none dedicated; integration-exercised in `property_sharding_minio_restore_test.go`)
- **Status:** Documented

### Rule 66 — `Spec.IfNotExists` is `*bool`; callers use `IfNotExistsEffective()`
- **Scope:** `api/v1beta1/neo4jshardeddatabase_types.go` (`IfNotExistsEffective`)
- **Rule:** `Neo4jShardedDatabase.spec.IfNotExists` is a `*bool`. Callers MUST use `Spec.IfNotExistsEffective()`, never dereference directly.
- **Why:** kubebuilder `+default=true` on a `bool omitempty` field silently re-applies the default when the user sets `false`. A pointer preserves explicit-false.
- **Pinned-by:** `test/integration/property_sharding_minio_restore_test.go` (Phase 2c replaceExisting spec — explicit-false `ifNotExists` is required for the destructive path)
- **Status:** Enforced (integration)

### Rule 72 — `ResolvedShardedSeed.URI` vs `PerShardURIs` are mutually exclusive
- **Scope:** `internal/controller/neo4jshardeddatabase_seed.go` (`ResolvedShardedSeed`, `resolveShardedSeed`, `resolvePVCShardedSeed`)
- **Rule:** Cloud → `URI` (`OPTIONS { seedURI: <uri> }`); PVC → `PerShardURIs`, emitted under the **same singular** Cypher key with a map value (`OPTIONS { seedURI: { `shard`: <uri>, … } }`). The emitted Cypher OPTIONS key is `seedURI` (singular) in BOTH cases — even though the CR spec field for the per-shard map is `spec.seedURIs` (plural); see `buildShardedDatabaseOptions` (`neo4jshardeddatabase_controller.go`, ~L639). Wire ONE and clear the OTHER — the validator rejects both populated. `ProxyAvailable=false` → requeue while the proxy rolls out. A map key is a shard name as Neo4j names it — `shardSeedKey`: the target `spec.name` lower-cased plus `-g000` / `-pNNN` — and `buildShardedDatabaseOptions` lower-cases every key it emits, the user's `spec.seedURIs` included: Neo4j matches the keys exactly against the shard names it derives from the lower-cased database name (operations.md id 146).
- **Why:** The two seed shapes are different Cypher OPTIONS clauses; populating both is ambiguous.
- **Pinned-by:** integration in `property_sharding_minio_restore_test.go`; the keys' case by `internal/controller/backup_restore_name_case_test.go:TestPerShardSeedKeys_AreTheLowerCaseShardNames` (cloud, PVC, `spec.seedURIs`)
- **Status:** Documented (the key case is enforced by unit test) — **NOTE:** the checklist referred to `resolveShardedSeed`/`ResolveShardedSeed` with the signature `(uri, credsSecretName, err)`. The actual function is **lowercase** `resolveShardedSeed` and returns `(*ResolvedShardedSeed, error)` — the `URI`, `PerShardURIs`, `CredsSecretName`, and `ProxyAvailable` are fields on the returned struct, not a multi-value return.

---

## PVC seed proxy

### Rule 71 — PVC seed proxy + `URLConnectionSeedProvider`
- **Scope:** `internal/controller/pvc_seed_proxy.go` (`ensurePVCSeedProxyResources`, `pvcSeedProxyURL`, `teardownPVCSeedProxyResources`, `ensurePVCSeedProxyNetworkPolicy`), gate in `internal/controller/neo4jshardeddatabase_seed.go` (`resolvePVCShardedSeed`)
- **Rule:** PVC-backed `seedBackupRef` (sharded) and PVC-backed cluster `Neo4jRestore` spawn a `backup-seed-proxy-<owner>` Deployment + Service via `ensurePVCSeedProxyResources` — the backup PVC is mounted RO, busybox httpd serves it on `:8080`, owner-ref'd to the consuming CR and torn down explicitly on finalize. URLs target the exact `.backup` filename (`URLConnectionSeedProvider` only supports single-file URIs). **Hard-gated on F3 filename capture: an empty `ShardArtifacts.Filename` is rejected** before any proxy URL is built.
- **Why:** `URLConnectionSeedProvider` fetches one file per URL — it can't enumerate a directory. If F3 Pod-log parsing didn't capture the per-shard filenames, there's no file to point the proxy at, so the seed must fail fast with an actionable error rather than build a broken URL.
- **Pinned-by:** (none dedicated unit test; integration in `test/integration/property_sharding_minio_restore_test.go` + `standard_database_minio_restore_test.go`)
- **Status:** Documented — **NOTE:** the checklist said "empty filename → validator rejects." There is no separate admission validator (the project forbids webhooks); the gate is enforced inline in the reconciler at `neo4jshardeddatabase_seed.go:140-148` (`resolvePVCShardedSeed`), which returns an actionable error for empty `Filename`. Substance correct; "validator" is imprecise — it's controller-side validation.

---

## Seed credentials projection

### Rule 56 — `spec.seedBackupRef` supports cloud + PVC only
- **Scope:** `internal/controller/neo4jshardeddatabase_seed.go` (`resolveShardedSeed`)
- **Rule:** `spec.seedBackupRef` supports cloud (CloudSeedProvider) and PVC (HTTP proxy + URLConnectionSeedProvider — rule 71). Other storage types are rejected (`"seedBackupRef does not support storage type %q"`).
- **Why:** Only these two have a defined seed path; anything else would silently produce a non-functional seedURI.
- **Pinned-by:** (none dedicated)
- **Status:** Documented

### Rule 57 — `seedBackupRef` mutex with `seedURI` / `seedURIs`
- **Scope:** sharded DB validator + `internal/controller/neo4jshardeddatabase_seed.go`
- **Rule:** The validator rejects combining `seedBackupRef` with `seedURI` / `seedURIs`. `seedBackupRef` materializes into `seedURI` at reconcile time on a shallow in-memory copy — the original spec is not persisted with the resolved URI.
- **Why:** Two competing seed sources are ambiguous; resolving on a copy keeps the user's spec clean.
- **Pinned-by:** (none dedicated)
- **Status:** Documented

### Rule 58 — Sharded phase "Pending" is for waits on something that does not exist yet
- **Scope:** `internal/controller/neo4jshardeddatabase_controller.go` (`resolveShardedSeed` → phase routing, the cluster lookup), `internal/controller/neo4jshardeddatabase_pvc_proxy.go` (`ensureClusterSeedConfig`)
- **Rule:** `status.phase=Pending` and a requeue when: `resolveShardedSeed` returns `errors.Is(err, ErrBackupNotReady)`; the PVC seed proxy is not Ready; the cluster's server pods lack the seed credentials or endpoint, they are being projected, or the pods have not rolled out with them; the `clusterRef` names no cluster (operations.md id 145). A cluster that exists but is not Ready yet is `Waiting`. Don't route other conditions through Pending without explicit design: a refused spec, a cluster without property sharding, a missing or unusable `seedBackupRef` backup (as for a restore, Rule 43) and a failed reconcile stay `Failed`.
- **Why:** Pending means "not yet, not wrong" (`kubectl neo4j explain Pending`): the database is created by itself once the thing it waits for is there. The seed-configuration and missing-cluster waits used to be `Failed` while the controller retried them on its own.
- **Pinned-by:** `internal/controller/repeated_warnings_test.go:TestShardedReconcile_MissingSeedCredentialsAreAWait` + `:TestShardedReconcile_AMissingClusterIsAWaitThatEnds` + `:TestShardedReconcile_LastingFailuresAreAnnouncedOnce` (the `Failed` cases); `internal/controller/neo4jshardeddatabase_create_once_test.go:TestShardedReconcile_AnnouncesTheSeedWaitOnce`
- **Status:** Enforced by unit test

### Rule 59 — Seed-creds projection via `spec.extraEnvFrom` (cluster + standalone)
- **Scope:** `api/v1beta1/seed_creds_target.go`, `internal/controller/cluster_seed_creds.go`
- **Rule:** `CREATE DATABASE … OPTIONS { seedURI }` runs on the Neo4j server pods, so cloud creds must be in their env (or the JVM's SDK default chain can't authenticate). Both CRs' `spec.extraEnvFrom` wire onto the neo4j container's `envFrom`. Generic (cloud creds, plugin tokens, any Secret-projected env). An empty `credsSecretName` is a no-op (user on IRSA / Workload Identity).
- **Why:** The server JVM — not the operator — fetches the seed, so the creds must live on the server pods.
- **Pinned-by:** (none dedicated for the env wiring)
- **Status:** Documented

### Rule 60 — `SeedCredsTarget` interface + `EnsureSeedCredsProjected`
- **Scope:** `internal/controller/cluster_seed_creds.go` (`SeedCredsTarget`, `EnsureSeedCredsProjected`), `api/v1beta1/seed_creds_target.go` (interface impls)
- **Rule:** `SeedCredsTarget` + `EnsureSeedCredsProjected` are the canonical projection check. Both `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone` implement the interface via `api/v1beta1/seed_creds_target.go`. New callers go through the interface. The sharded `seedBackupRef` path projects credentials AND a custom endpoint through the restore path's `projectSeedConfig` (rule 110).
- **Why:** The interface lets cluster + standalone restore paths share the projection plumbing without templated duplication.
- **Pinned-by:** (none dedicated)
- **Status:** Documented — **NOTE:** the checklist also described `ResolveShardedSeed` returning `(uri, credsSecretName, err)`. As corrected under rule 72, the function is `resolveShardedSeed` and `CredsSecretName` is a field on `ResolvedShardedSeed`. The wiring into `EnsureSeedCredsProjected` reads that field.

### Rule 61 — Auto-inherit seed creds is annotation-gated, triggers a rolling restart
- **Scope:** `internal/controller/cluster_seed_creds.go` (`AutoInheritSeedCredsAnnotation`, `AutoInheritedFromAnnotation`, `EnsureSeedCredsProjected`)
- **Rule:** Without `neo4j.com/auto-inherit-seed-creds=true` on the *hosting* CR, missing projection emits an actionable copy-pasteable error (a `Neo4jRestore` waits on it, Rule 138). With it, the operator patches `spec.extraEnvFrom`, records the source in `neo4j.com/seed-creds-auto-inherited-from`, rolls out the STS, and the DB controller routes to Pending+requeue. Never auto-inherit without the annotation. The `RetryOnConflict` block re-fetches inside the loop: the patch is locked to the resourceVersion it read, and the owning controller's status writes move it on many passes (it never rewrites the spec, operations.md id 150). The entry is the operator's, so a user who applies with `kubectl apply --server-side` and whose manifest sets `spec.extraEnvFrom` (or `spec.env`, which a restore's endpoint projection writes) owns the whole list and meets a conflict on the next apply: the documented answer is to add the entry to the manifest, or leave the field out of it (docs/gitops/README.md); `--force-conflicts` takes the entry away and restarts the servers.
- **Why:** Patching `extraEnvFrom` triggers a rolling restart of server pods — a sharded-DB controller must not do that unsolicited. The annotation lives on the cluster/standalone CR because infrastructure operators own that authorization.
- **Pinned-by:** (none dedicated)
- **Status:** Documented

### Rule 138 — A restore whose target lacks the seed credentials waits for them; it is not Failed
- **Scope:** `awaitSeedConfig`, `seedConfigMissingMessage`, `projectSeedConfig` and the `errSeedConfigNotAutoInherited` branch of `startClusterCypherRestore` (`internal/controller/neo4jrestore_controller.go`); the same branch of `ensureClusterSeedConfigReady` (`internal/controller/neo4jrestore_alldatabases.go`); the previously-failed guard in `Neo4jRestoreReconciler.Reconcile`.
- **Rule:**
  - **Waiting:** when an online restore from cloud storage finds that its target's server pods lack the credentials Secret or the custom S3 endpoint, and the target is not opted in to Rule 61, the restore is `Pending`, never `Failed`. It requeues every `RequeueAfter` (30s) and carries on once the target has them, whether the user projects them or sets the annotation. The message names what is missing and says the restore carries on by itself. `SeedEndpointNotProjected` (Warning) and the status write happen only when the restore starts waiting (its phase or message changes), never on each check.
  - **Why waiting is safe:** the check runs before anything touches a database. On the single-database path, only the source pin (`status.resolvedSource`), the point-in-time record (Rule 112) and the chain-parent advisory come before it. It precedes the pre-restore hooks, `DatabaseExists`, and the CREATE or recreate. On the all-databases path it runs before the loop seeds the first database. The pre-restore hooks have run by then, once, and `neo4j.com/restore-pre-hooks-ran` keeps them from running again, as for the other waits on that path.
  - **What stays terminal:** every other `Failed`. The previously-failed guard (`Failed` with `observedGeneration == generation`) is unchanged. Never route a failure through this wait once a database has been touched: a seed that failed inside Neo4j, a refused recreate, or an unconfirmed overwrite. A restore that an earlier operator failed this way stays `Failed`: its message is not parsed to revive it.
  - **No watch on the target:** the requeue re-reads only cached objects, so it costs nothing while the user acts. A watch would enqueue restores on every status write of their target. That makes a pass more likely to read a stale restore from the cache just after a CREATE or recreate, the race the all-databases loop already needs an uncached guard for (`issueGuardAllowsRecreate`). At most 30s of latency is not worth that risk.
- **Why:** the v1.21.0 journey: after the user set `neo4j.com/auto-inherit-seed-creds` as the message said, the restore stayed `Failed`, because the previously-failed guard returns before anything is checked. Only deleting and recreating it, or editing its spec, helped. The sharded-database and replica controllers already retry the same precondition on their own (Rule 110; `missingObjectStoreEnv`, operations.md id 133); they reported it as phase `Failed` while they did, and now wait in `Pending` too (operations.md id 145).
- **Pinned-by:** `internal/controller/neo4jrestore_seed_config_wait_test.go:TestRestore_WaitsForSeedConfigAndCarriesOn` (single-database and all-databases restores, through `Reconcile`: `Pending` with a requeue, one Warning over two checks, and after the opt-in the cluster gets the Secret) + `:TestRestore_GenuineFailureStaysTerminal`; `internal/controller/neo4jrestore_online_standalone_test.go:TestStartRestore_StandaloneTakesTheOnlinePath` (a standalone target). Against the previous code the first and the last fail (`Failed`, no requeue on the all-databases path, cluster never patched); the second guards the terminal side and passes on both.
- **Status:** Enforced by unit test.

---

## Backup options: validate & log parsing

### Rule 70 — `BackupOptions.Validate` is `*bool` opt-in
- **Scope:** `internal/controller/neo4jbackup_controller.go` (`buildBackupCommand` validate clause, `warnValidateUnsupported`), `internal/controller/neo4jbackup_log_parser.go` (`parseValidationFromLog`)
- **Rule:** When `*Validate == true`, append `&& (neo4j-admin backup validate --from-path=… --database="…" || true)` (validate failures don't fail the Job). The operator parses the Pod log into `BackupRun.Validation`: all rows OK → OK; any Ahead/Behind → Degraded; no parseable rows → Unknown + truncated `RawOutput` (capped 2 KiB). For sharded, the validate dbArg is the literal name with the trailing `*` stripped (validate auto-discovers shards). `neo4j-admin backup validate` exists only on CalVer — on 5.26 the clause is skipped and `warnValidateUnsupported` emits a one-time Warning at Job/CronJob creation.
- **Why:** Validate is informational; failing the Job on it would discard a successful backup. The `*` glob is stripped because `validate` evaluates `foo*-g000` literally and errors, whereas `backup` needs the glob to capture all shards in one invocation.
- **Pinned-by:** (none dedicated unit test for the clause; log-parser behavior in `internal/controller/neo4jbackup_log_parser.go` paths)
- **Status:** Documented

### Rule 67 — Backup-Pod log parsing is opportunistic, not load-bearing
- **Scope:** `internal/controller/neo4jbackup_controller.go` (`Neo4jBackupReconciler.Clientset`), `internal/controller/neo4jbackup_log_parser.go` (`fetchBackupPodLog`, `parseShardArtifactsFromLog`, `parseStandardArtifactFromLog`, `parseValidationFromLog`, `mergeShardArtifactsFromLog`)
- **Rule:** `Reconciler.Clientset` (typed Kubernetes client) enables Pod-log fetches that populate `BackupRun.ShardArtifacts.Filename/Size`, `BackupRun.ArtifactFilename`, and `BackupRun.Validation`. All best-effort: log-fetch failures, format drift, or `Clientset == nil` (unit tests with a fake client) leave the fields empty without failing the reconcile. `ShardName` / `RunID` / `Status` are load-bearing — never gate reconcile state on parsed filename/size. Names read from the log are matched ignoring case (`mergeShardArtifactsFromLog`, `parseStandardArtifactFromLog`): neo4j-admin writes them lower-cased. How long a run waits for them: Rule 174.
- **Why:** Pod logs are a soft signal (TTL'd, format may drift). Gating reconcile correctness on them would make backups fail for cosmetic reasons.
- **Pinned-by:** (none dedicated; covered by parser unit paths in `internal/controller/neo4jbackup_log_parser.go`)
- **Status:** Documented

---

## Seed-from-URI providers config

### Rule 74 — `dbms.databases.seed_from_uri_providers` is version-gated
- **Scope:** `internal/resources/cluster.go` (`SeedFromURIProvidersConfigValue`, `IsNeo4jVersion202604OrHigher`)
- **Rule:** `SeedFromURIProvidersConfigValue(imageTag)` returns the base `CloudSeedProvider,FileSeedProvider,URLConnectionSeedProvider`; `ServerSeedProvider` is appended only on `IsNeo4jVersion202604OrHigher`. Both the cluster and standalone config builders call the helper — never inline the value. The deprecated `S3SeedProvider` is excluded across all versions (`CloudSeedProvider` handles `s3://` via the SDK default credential chain — rule 59).
- **Why:** `ServerSeedProvider` doesn't exist before 2026.04; listing it on an older image makes Neo4j refuse to bootstrap. `S3SeedProvider` is deprecated and redundant with `CloudSeedProvider`.
- **Pinned-by:** `internal/resources/seed_providers_test.go:TestSeedFromURIProvidersConfigValue` + `:TestIsNeo4jVersion202604OrHigher`
- **Status:** Enforced

---

## Removal invariant

### Rule 79 — `spec.backups` and all centralized-backup plumbing are REMOVED
- **Scope:** repo-wide (`api/v1beta1/`, `internal/resources/`, `internal/controller/`, `internal/validation/`)
- **Rule:** There is no `spec.backups` / `spec.storage.backupStorage` field, no `BackupsSpec` / `BackupStorageSpec` type, no `BuildBackupStatefulSet` / `buildCentralizedBackup*` builder, no standalone `buildBackupSidecarContainer`, and no `cloud_validator.go`. The `Neo4jBackup` CRD (Job-per-CR) is the only backup path. `StorageLocation` / `CloudBlock` / `CloudIdentity` / `AutoCreateSpec` types survive because `Neo4jBackup` / `Neo4jRestore` use them. **Never reintroduce a `spec.backups` field or a long-running backup pod/sidecar.**
- **Why:** The centralized backup StatefulSet / sidecar architecture was deliberately removed in the backup-restore overhaul; reintroducing it resurrects a banned architecture and a long-running pod that holds cloud credentials indefinitely.
- **Verified (this audit):** `grep` confirmed `BuildBackupStatefulSet`, `BackupsSpec`, `BackupStorageSpec`, `buildBackupSidecarContainer`, and `cloud_validator.go` are ALL absent from the tree (worktrees excluded); the four surviving types are present in `api/v1beta1/neo4jenterprisecluster_types.go`.
- **Pinned-by:** guard-checked, advisory — `scripts/check-invariants.sh` (`make check-invariants`) fails on a reintroduced `Build*BackupStatefulSet` builder or a `backups:` / `json:"backups"` field in `config/` + `api/`. The other removed symbols (`BackupsSpec`, `BackupStorageSpec`, `buildBackupSidecarContainer`, `cloud_validator.go`) are kept gone by absence only. No unit test.
- **Status:** Enforced (advisory guard + absence) — the guard runs in the non-blocking `Invariant Guards (advisory)` CI job, not as a merge gate

---

## Backup scope (v1.13): instanceRef, shardedDatabase, same-namespace

### Rule 80 — Backup scope is same-namespace (enforced by API shape, NOT by a validator)
- **Scope:** `api/v1beta1/neo4jbackup_types.go` (`InstanceRef`); `internal/validation/backup_validator.go` (`validateScopeSelection`)
- **Rule:** a `Neo4jBackup` always backs up a deployment in its **own** namespace. `InstanceRef` names "a Neo4jEnterpriseCluster OR a Neo4jEnterpriseStandalone **in this namespace**" and carries no namespace field. To back up a cluster in another namespace, create the `Neo4jBackup` in that namespace.
- **Why:** same blast-radius boundary as rule 18 in `docs/knowledge/operations.md` — a backup reads the target's data using the target's admin credentials, so a cross-namespace scope would be a confused-deputy escalation.
- **How it is actually enforced:** **structurally.** There is no namespace field on the scope API, so there is nothing for a validator to reject. Adding one would silently remove the only enforcement — such a change MUST ship a consent mechanism in the same PR (see rule 18).
- **Status:** Enforced by API shape. Deliberately not a validator.

> **Correction (2026-08-28).** This rule previously described a function named **validateBackupTarget** (deliberately not backticked — backticks assert a real symbol) rejecting a cross-namespace `spec.target.namespace` "for every kind", pinned to two named tests, with Status "Enforced". **None of it exists.** That function is not defined anywhere; `spec.target` was removed when `instanceRef` replaced it in v1.13 (confirmed absent from the generated CRD schema); and neither pinned test exists in `backup_validator_test.go`. The rule was describing the pre-`instanceRef` API — and even contradicted itself mid-sentence, noting "The `instanceRef` scope API has no namespace field" while claiming a check against `target.namespace`. `scripts/check-knowledge-drift.sh` passed it green because its pin was a real file path plus quoted test-case *names*, neither of which the guard validated (now fixed — pass 4 of that script now checks quoted test-case names on `Pinned-by` lines against the test files).

### Rule 81 — `shardedDatabase` scope; all-databases CATALOGUES sharded families (restorable, not just surfaced)
- **Scope:** `api/v1beta1/neo4jbackup_types.go` (`ShardedDatabase`, `ResolvedTarget`, `ShardedFamilyArtifacts`, `BackupRun.ShardedFamilies`), `internal/validation/backup_validator.go` (`validateScopeSelection`), `internal/controller/neo4jbackup_log_parser.go` (`parseShardedFamiliesExcludedFromLog`, `groupShardedFamiliesFromLog`), `internal/controller/neo4jbackup_controller.go` (`recordShardedExclusion`), `internal/controller/neo4jshardeddatabase_seed.go` (`resolveShardedSeed`, `findFamilyArtifacts`, `buildPerShardCloudURIs`), `internal/controller/neo4jrestore_alldatabases.go`
- **Rule:** `spec.instanceRef + spec.shardedDatabase` is the scope for a property-sharded backup (synthesizes `target.kind=ShardedDatabase` via `ResolvedTarget`); mutually exclusive with `database`/`allDatabases`. An **all-databases** backup (`allDatabases` / legacy `kind: Cluster`) writes shard physical DBs (`-g000`/`-pNNN`) to disk and EXCLUDES them from `databaseArtifacts` (`shardSuffixRegex`) — so the all-databases **restore loop** (`neo4jrestore_alldatabases.go`) recreates standard DBs only — BUT it now **catalogues** each family's per-shard artifacts in `BackupRun.ShardedFamilies` (`groupShardedFamiliesFromLog`), so one all-databases backup IS a complete DR source. Family names are still recorded in `ShardedDatabasesExcluded` (the restore-loop skip-set) with a `BackupShardedDatabasesExcluded` event, carried to `ResolvedRestoreSource`, re-warned by the all-databases restore (`RestoreShardedDatabasesNotCovered`, now naming the `seedBackupRef` to use). Restore a sharded family via `Neo4jShardedDatabase.spec.seedBackupRef` pointing at **either** a `shardedDatabase`-scoped backup **or** an all-databases backup — `resolveShardedSeed` classifies by `run.ShardArtifacts` (single-family) vs `run.ShardedFamilies` (all-DB), selects the family by `spec.seedSourceDatabase`/`spec.name`, and for all-DB builds explicit per-shard URIs (cloud `buildPerShardCloudURIs`; PVC proxy) because a cloud directory-scan can't isolate one family. Never `Neo4jRestore` (which rejects sharded — rule 63/64 path).
- **Why:** Sharded restore needs the shard topology (only on the `Neo4jShardedDatabase` CR) + a per-shard seed map the generic single-seedURI path can't express, so it stays out of the all-databases restore *loop*. But excluding it from the *backup catalogue* too forced a separate per-family backup for DR completeness; cataloguing the families' artifacts (without re-backing-up — the `"*"` glob already wrote them) makes one all-databases backup recover the whole cluster, declaratively (re-apply each `Neo4jShardedDatabase` with `seedBackupRef` → that backup), with the operator never mutating user CRs.
- **Pinned-by:** `internal/validation/backup_validator_scope_test.go` (`shardedDatabase` scope + `ResolvedTarget` → ShardedDatabase kind), `internal/controller/backup_alldb_parser_test.go` (`TestParseShardedFamiliesExcludedFromLog`, `TestGroupShardedFamiliesFromLog`), `internal/controller/neo4jshardeddatabase_seed_test.go` (`TestResolveShardedSeed_AllDatabasesFamily`)
- **Status:** Enforced

---

## Restore under a different name

### Rule 107 — A single-database restore names the exact artifact whenever the operator knows it
- **Scope:** `standaloneRestoreArtifact`, `standaloneRestoreFromPath`, `restoreSourceDatabase`, `artifactDatabase`, `resolveRestoreSource` and `resolveLocalPVCFromPath` (`internal/controller/neo4jrestore_controller.go`); the cluster artifact lookups (`resolvedOrLiveArtifactFilename`, the PVC seed-proxy filename); `Neo4jRestore.spec.source.sourceDatabase` (`api/v1beta1/neo4jrestore_types.go`) and its checks in `validateRestore`.
- **Rule:** `neo4j-admin database restore` renames (`[<database>]`) only when `--from-path` names a single artifact — on 5.x and CalVer alike. So a standalone restore that runs offline (rule 108) passes the exact file whenever it is known: `source.backupPath` ending in `.backup`, or the pinned `backupRef` snapshot's `ArtifactFilename` (one-database backup) or `DatabaseArtifacts` entry (all-databases backup, looked up by the SOURCE database). Only when neither exists does it fall back to the glob — on the SOURCE database's files (PVC) — or the cloud directory, which can only be restored under its own name (a rename from one is refused with a message). The source database is `spec.source.sourceDatabase`, defaulting to `spec.database`; it must survive `resolveRestoreSource`, which rebuilds the source struct. Cluster paths look up all-databases maps by the same source database. `sourceDatabase` is refused with `allDatabases`, as `system`, and when it contradicts a one-database backup.
- **Why:** the standalone path globbed `<target>-*.backup`, so restoring into a new database matched nothing and the tutorial's "restore into a new database" could not work on a standalone; the cluster path already seeded from the recorded artifact. Restoring one database of an all-databases backup under a new name failed on both paths, which looked the TARGET up in the per-database map. Walked live on Kind 2026-10-05 (5.26 standalone, then a 5.26 cluster): a one-database PVC backup into `neo4jrestored`, `second` out of an all-databases backup into `secondcopy`, a MinIO backup into `fromcloud`, a same-name restore with `replaceExisting`, and on the cluster `second` into `secondc` via the seed proxy — every database created with its source's data. The walk caught `resolveRestoreSource` dropping `sourceDatabase`, which tests that started from an already-resolved source could not.
- **Pinned-by:** `TestStandaloneRestore_UnderANewNameReadsTheRecordedArtifact`, `TestStandaloneRestore_OneDatabaseOutOfAnAllDatabasesBackup`, `TestStandaloneRestore_ExactStorageFileIsNotGlobbed`, `TestStandaloneRestore_DirectoryGlobsTheSourceDatabase`, `TestStandaloneRestore_Cloud`, `TestStandaloneRestore_SourceDatabaseMustMatchAOneDatabaseBackup`, `TestStandaloneRestore_SourceDatabaseSurvivesResolution`, `TestValidateRestore_SourceDatabase`, `TestClusterRestore_AllDatabasesBackupLooksUpTheSourceDatabase`, `TestArtifactDatabase` (`internal/controller/neo4jrestore_rename_test.go`).
- **Status:** Enforced by unit test.

### Rule 108 — A standalone restores online unless its server cannot fetch the seed
- **Scope:** `standaloneOfflineReason`, `restoreRunsOffline`, `restoreOnJobPath`, `restoreSeedArtifacts`, `pvcDiffSeedRefusal` and `restoreTarget` (`internal/controller/restore_target.go`); the routing in `Reconcile`, `startRestore` and `checkRestoreProgress`, plus `startClusterCypherRestore`, `completeOnlineRestore`, `refuseRestoreIfPodsRunning` (`neo4jrestore_controller.go`) and `startAllDatabasesRestore` / `finishAllDatabasesRestore` (`neo4jrestore_alldatabases.go`); `parseArtifactTypesFromLog` / `recordArtifactTypes` (`neo4jbackup_log_parser.go`); `BackupRun.ArtifactType`, `DatabaseArtifact.Type`, `ResolvedRestoreSource.ArtifactType`.
- **Rule:** A restore into a standalone takes rule 75's online path — `dbms.recreateDatabase` / `CREATE DATABASE … OPTIONS {seedURI}` against the running instance — and the `neo4j-admin` Job only when `standaloneOfflineReason` names a reason: a point in time that cannot run online (rule 112); `source.type: storage`; no resolved snapshot, or no recorded artifact for a database; cloud storage with no `credentialsSecretRef` on a standalone without `spec.podServiceAccountAnnotations` (rule 111; the restore Job's ServiceAccount carries the identity). `restoreOnJobPath` pins a restore to the path it started on: one that holds the standalone's `neo4j.com/restore-in-progress` marker stays on the Job (so one started before an upgrade is not re-driven online against a stopped instance), and one already issued online (`neo4j.com/cypher-restore-issued`, or an all-databases loop with `status.databaseResults`) stays online, since the pod identity is live spec. All three routing points call `restoreOnJobPath`, and otherwise decide from the pinned `status.resolvedSource`, so they agree on every pass. Each backup run records `FULL`/`DIFF` from its log: any `full backup of database '<db>'` line makes it `FULL` (an AUTO run that fell back), else a `differential` line makes it `DIFF`; an unrecorded run counts as `FULL` only when the backup's `backupType` is `FULL`. A PVC artifact not recorded as `FULL` is merged by the seed proxy (rule 109), on clusters and standalones alike. Target-specific pieces — the StatefulSet name (`<name>` vs `<name>-server`), the pod labels the seed proxy's NetworkPolicy admits (`app: <name>` vs `neo4j.com/cluster`), and the CR that carries the seed `spec.env` / `spec.extraEnvFrom` — come from `restoreTarget`. Pre/post-restore hooks run on both paths; the online all-databases loop runs pre hooks once per attempt (`neo4j.com/restore-pre-hooks-ran`).
- **Why:** the Job stopped the whole instance — every database offline — to restore one. Walked live on Kind 2026-10-06 (5.26, then 2026.09): recreate over `neo4j` and create under a new name through the PVC proxy, a MinIO seed (one restart to project the credentials, refused with a message before the opt-in), and an all-databases restore — no Job, no restart, the other database online in every sample; a post hook ran. The walk found that a PVC DIFF seeded over HTTP fails inside Neo4j ("not part of a valid backup chain") and leaves the database offline — `backupType: AUTO`, the default, makes every scheduled run after the first a DIFF — while a MinIO DIFF seeds its whole chain. With types recorded, a PVC DIFF went to the Job (refused without `stopCluster`, naming the reason; restored with it, the full chain applied). The proxy's NetworkPolicy admitted only `neo4j.com/cluster` pods, which Kind does not enforce, so no walk would have shown it.
- **Pinned-by:** `TestRestoreRunsOffline`, `TestRestoreOnJobPath_FinishesAJobRestoreItAlreadyHolds`, `TestStartRestore_StandaloneTakesTheOnlinePath`, `TestStartRestore_StandalonePointInTimeStaysOnTheJob`, `TestStartRestore_StandaloneOfflineRefusalSaysWhy`, `TestRestoreTarget_StandaloneDiffersFromCluster`, `TestSeedProxyNetworkPolicy_AdmitsTheStandalonesPods`, `TestSeedCredsRolledOut_ReadsTheStandalonesStatefulSet`, `TestProjectSeedConfig_OntoAStandalone`, `TestCompleteOnlineRestore_RunsPostRestoreHooks`, `TestFinishAllDatabasesRestore_StampsCompletionTime`, `TestRestorePreHooksRanMarker` (`internal/controller/neo4jrestore_online_standalone_test.go`); `TestParseArtifactTypesFromLog`, `TestRecordArtifactTypes` (`backup_alldb_parser_test.go`); `TestEnsureResolvedBackupSource_SuccessPinsSnapshot`, `TestEnsureResolvedBackupSource_UntypedRunOfAFullOnlyBackup` (`neo4jrestore_resolvedsource_test.go`).
- **Status:** Enforced by unit test.

### Rule 109 — The seed proxy merges a PVC backup chain before serving it
- **Scope:** `seedMergePlan`, `newSeedMergePlan`, `newSeedArtifact`, `seedMergeCommand`, `seedMergeScript`, `applySeedMerge`, `pvcSeedProxyMergeFailure` (`internal/controller/pvc_seed_merge.go`); `buildPVCSeedProxyDeployment` / `ensurePVCSeedProxyResources` (`pvc_seed_proxy.go`); `resolveClusterPVCRestoreURI`, `seedProxyBudget` (`neo4jrestore_controller.go`); `ensurePVCSeedProxyReady` and the all-databases PVC case (`neo4jrestore_alldatabases.go`).
- **Rule:** A PVC seed reaches Neo4j over HTTP as one file, and Neo4j refuses a differential without its chain. (Not for sharded databases: merging drops their sharding header — rule 110.) Unless the backup recorded the artifact as `FULL`, the proxy pod gets an init container (`merge-chain`) running the TARGET's Neo4j image that copies the chain to scratch space, merges it with `neo4j-admin database aggregate-backup` (5.x) or `neo4j-admin backup aggregate` (CalVer; the old name is deprecated there), and publishes the merged artifact at the artifact's own relative path under `/scratch/serve`, which httpd then serves — the seed URL is unchanged. Known `FULL` artifacts are symlinked (busybox httpd follows links). Five properties of neo4j-admin, established live on 5.26 and 2026.09, are load-bearing: (1) aggregate writes the merged artifact BESIDE its input, resolving symlinks first, so the chain is copied — the backup PVC stays read-only; (2) it exits 0 on failure, so the script reads its result line (`new artifact: '<path>'`, or `Found existing full backup: '<path>'` — a no-op for a full) and fails otherwise, writing the reason to the termination message; (3) pointed at a differential it merges the chain ENDING there; (4) `backup inspect --latest-chain` names exactly the chain, but only for the newest run in the directory, and (5) it omits EMPTY backups (no new transactions — routine for a quiet database in an all-databases backup) unless given `--empty`; and (6) on 5.26, aggregate pointed at an EMPTY differential whose chain has lost its full reports it as an *"existing full backup"* — so the script checks the result with `backup inspect --show-metadata --format=JSON` (which says `"full": true|false` on 5.26 and CalVer; plain `--format=JSON` on 5.26 prints only the URIs) and refuses a differential with the reason. The script uses `--latest-chain --empty` when the target is the newest run of its database, and otherwise — or if the listing omits the target — copies every artifact of that database up to the target (the database is parsed from the filename, with a literal dot match). A failed merge fails the restore at once with the reason (`pvcSeedProxyMergeFailure`), instead of waiting out the proxy budget, which defaults to 30m while merging (`spec.timeout` overrides). Scratch is an `emptyDir`, or a PVC from `spec.options.tempStorage`; the init container takes `spec.options.resources`. The pod runs with the Neo4j pod security context (fsGroup 7474) so either is writable.
- **Why:** with `backupType: AUTO`, the default, every scheduled PVC run after the first is a differential: a cluster restore of one created the database and left it offline, and a standalone had to restore it offline. Walked live on Kind 2026-10-06: a split-archive FULL+DIFF+DIFF chain into a new database and over `neo4j` on a 2026.09 standalone, an all-databases restore whose artifacts were all differentials (one empty — the walk found the `--empty` gap), a deleted full failing in 35s with neo4j-admin's reason, and on a 5.26 cluster a PVC differential into a new database and over `neo4j` — every database online with every run's data, no Job, no restart.
- **Pinned-by:** `TestSeedMergeScript_MergesTheLatestChain`, `TestSeedMergeScript_AnEmptyNewestRunIsPartOfTheChain`, `TestSeedMergeScript_AnEarlierRunCopiesUpToIt`, `TestSeedMergeScript_AFullIsServedAsItIs`, `TestSeedMergeScript_FailsWithNeo4jAdminsReason`, `TestSeedMergeScript_ADifferentialReportedAsFullIsRefused` (the real script against a fake neo4j-admin), `TestNewSeedMergePlan`, `TestBuildPVCSeedProxyDeployment_Merge`, `TestPVCSeedProxyMergeFailure`, `TestSeedProxyBudget`, `TestAllDatabasesRestore_MergesPVCDiffsPerDatabase` (`internal/controller/pvc_seed_merge_test.go`); `TestStartRestore_StandalonePVCDiffMergesOnline`, `TestClusterRestore_MergesAPVCDiffBeforeSeeding`, `TestClusterRestore_FailsAtOnceWhenTheMergeFails` (`neo4jrestore_online_standalone_test.go`).
- **Status:** Enforced by unit test.

### Rule 110 — A sharded seed from a PVC is a full backup of every shard; never merge one
- **Scope:** `resolvePVCShardedSeed` (`internal/controller/neo4jshardeddatabase_seed.go`); `differentialShards`, `runDifferentialShards`, `recordArtifactTypes` (`neo4jbackup_log_parser.go`); `warnShardedPVCDifferential`, `effectiveBackupType` (`neo4jbackup_controller.go`); `ensureClusterSeedConfig` (`neo4jshardeddatabase_pvc_proxy.go`); `ShardArtifact.Type` (`api/v1beta1/neo4jbackup_types.go`).
- **Rule:** Rule 109's merge must NOT be applied to shards. Over HTTP (the PVC seed proxy), Neo4j seeds a sharded database only from a full backup of each shard ("does not point to a full backup"), and `neo4j-admin backup aggregate` writes the merged artifact WITHOUT the sharding header the seed requires — the original artifacts' header carries `topology_version`, `shard_count`, `virtual_name`, `virtual_id`; the merged one only `compressedMetadataScript` — so Neo4j refuses it ("points to a backup without sharding information"). So each run records every shard's `FULL`/`DIFF` (`ShardArtifact.Type`, in `shardArtifacts` and in an all-databases run's `shardedFamilies`); `resolvePVCShardedSeed` refuses a run with a `DIFF` shard before exposing the PVC, naming the shards, and the reconciler's requeue seeds once a full run lands; an untyped shard (a run from before types were recorded) is served and Neo4j checks it. So `effectiveBackupType` turns `AUTO` (the default, also when unset) into `--type=FULL` for a `shardedDatabase`-scoped backup on a PVC — the user chose this over a warning, since `AUTO` otherwise made every run after the first unrestorable; cloud storage keeps `AUTO`, and an all-databases backup keeps `AUTO` (its one `neo4j-admin` run covers every database). `backupType: DIFF` with `spec.shardedDatabase` is refused by the validator: Neo4j refuses differential backups of property shards (*"Differential backups of property shards are not allowed"*, seen on 2026.08.1 in the v1.19.0 walk — every run failed, with no warning to show for it); `AUTO` writes a differential of the graph shard only. A sharded PVC run that still writes a `DIFF` shard raises `BackupShardedDifferential` at backup time, naming the fix for its scope, while a full backup can still be taken. The PVC retention caveat keys off the effective type. Cloud storage seeds a DIFF chain directly (CloudSeedProvider reads it, sharding header included); the server pods need the credentials AND, for S3-compatible stores, the endpoint — `ensureClusterSeedConfig` projects both through the restore path's `projectSeedConfig` under `neo4j.com/auto-inherit-seed-creds` (one rolling restart), refuses without the opt-in naming what is missing, and seeds only once the StatefulSet carries them.
- **Why:** walked live on Kind 2026-10-06 (2026.08.1 sharding cluster, 3 servers): the merged graph shard was refused by `CREATE DATABASE`, and aggregate dropped the header the same way on 2026.08.1 and 2026.09, in file, database and glob modes — no flag keeps it. Patching Neo4j's binary header was rejected. Then a DIFF run refused in seconds with no proxy, `BackupShardedDifferential` fired on an AUTO run, switching the backup to `FULL` seeded the failed database on its next requeue with every marker, and a MinIO seed onto a cluster stripped of its hand-set endpoint projected credentials + endpoint + path-style in one roll and seeded. Before the fix the projection carried only the credentials, so Neo4j's S3 client went to AWS.
- **Pinned-by:** `TestRecordArtifactTypes_Shards`, `TestResolvePVCShardedSeed_RefusesDifferentialShards`, `TestResolvePVCShardedSeed_ServesFullAndUntypedShards`, `TestWarnShardedPVCDifferential`, `TestEnsureClusterSeedConfig`, `TestEffectiveBackupType` (`internal/controller/neo4jshardeddatabase_seed_pvc_diff_test.go`); `TestBackupValidator_ShardedDatabaseRefusesDIFF` (`internal/validation/backup_validator_scope_test.go`); `TestBuildBackupCommand_ShardedDatabase_EmitsFlagsWithNilOptions` (`neo4jbackup_sharded_test.go`).
- **Status:** Enforced by unit test.

### Rule 111 — A standalone's pod identity is opt-in, and never restarts a standalone that does not use it
- **Scope:** `Neo4jEnterpriseStandaloneSpec.PodServiceAccountAnnotations` (`api/v1beta1/neo4jenterprisestandalone_types.go`); `standaloneHasPodIdentity`, `standaloneServiceAccountName`, `podIdentityHash`, `applyStandalonePodIdentity`, `ensureStandaloneServiceAccount`, `cleanupStandaloneServiceAccount` (`internal/controller/standalone_identity.go`); `operatorManagedPodAnnotations` (`owned_keys.go`); `standaloneOfflineReason`, `restoreOnJobPath`, `restoreStandalone`, `standaloneIdentityRolledOut`, `awaitSeedIdentity` (`restore_target.go`) and its calls in `startClusterCypherRestore` / `ensureClusterSeedConfigReady`; `usesWorkloadIdentity` (`neo4jreplicadatabase_controller.go`); the CLI's `resolveReplicaTargetEnv` (`cmd/kubectl-neo4j/preflight.go`).
- **Rule:** With the field set, the standalone controller creates `<name>-neo4j` (controller-owned; annotations merged with owned-key tracking, foreign ones kept) BEFORE the StatefulSet, and the pod template gets `serviceAccountName` plus `neo4j.com/pod-identity-hash`, so any change to the annotations recreates the pod — identity webhooks inject only at admission. With the field unset the template is exactly what it was before the field existed: no `serviceAccountName`, no hash (an operator-managed pod annotation, so it is dropped on removal) — upgrading the operator must not restart a standalone that does not use it. Removing the field deletes the ServiceAccount once the StatefulSet no longer names it, and only if this standalone controls it. A restore from cloud storage with no `credentialsSecretRef` into a standalone with the field runs online (rule 108), and `awaitSeedIdentity` holds it `Pending` until the StatefulSet's template names the ServiceAccount with the current identity hash and the pod has rolled out — seeding from a pod admitted earlier fails inside Neo4j with no retry. Clusters need no wait: their pods always run under `<name>-discovery`.
- **Why:** a standalone could not carry a workload identity, so a pod-identity cloud restore had to stop the whole instance through the Job. Walked live on Kind 2026-10-06 (2026.08.1): a standalone created on the previous operator kept its pod UID across the upgrade (no ServiceAccount, StatefulSet generation unchanged); setting the field created `ws-neo4j` with the role annotation and recreated the pod under it; a MinIO restore resolved without `credentialsSecretRef` (credentials in the pod env standing in for a webhook) ran online into a new database — no Job, no restart, every node; removing the field put the pod back on `default` and deleted the ServiceAccount; the same restore was then refused naming the field.
- **Pinned-by:** `TestCreateStatefulSet_PodIdentityOnlyWhenSet`, `TestStandaloneServiceAccount_Lifecycle`, `TestCleanupStandaloneServiceAccount_LeavesAForeignOne`, `TestStandaloneOfflineReason_PodIdentity`, `TestRestoreOnJobPath_PinnedOnceStarted`, `TestAwaitSeedIdentity` (`internal/controller/standalone_identity_test.go`); `TestMissingObjectStoreEnv_WorkloadIdentityDefersToTheServer` (`neo4jreplicadatabase_controller_test.go`); `TestPreflightReplica` (`cmd/kubectl-neo4j/preflight_test.go`).
- **Status:** Enforced by unit test.

### Rule 112 — A point in time runs online only by creating a database from the run that holds it; decide once
- **Scope:** `restoreRunIndex`, `runStartedAfter`, `pointInTimeCovered`, `ensureResolvedBackupSource` (`internal/controller/neo4jrestore_controller.go`); `AnnotationPointInTimePath`, `pointInTimeOnlineBlocker`, `pointInTimeOfflineReason`, `decidePointInTimePath(With)` (`restore_point_in_time.go`); the point-in-time guards in `startClusterCypherRestore` and `validateRestore`; `CreateDatabaseWithSeedURIUntil` (`internal/neo4j/client.go`); `ResolvedRestoreSource.BackupStartedAt`.
- **Rule:** With `source.pointInTime`, resolution pins the earliest Succeeded run that STARTED at or after it (it holds every transaction before it; the docs say to seed "the differential backup containing the data up to that date"), else the most recent, and records its start (`backupStartedAt`). Online means `CREATE DATABASE … OPTIONS {existingData:'use', seedURI: <that run>, seedRestoreUntil: datetime(...)}` — never `dbms.recreateDatabase`, which would silently restore the whole run. `pointInTimeOnlineBlocker` allows it only for `source.type: backup`, one database, CalVer, cloud storage (the PVC proxy is HTTP, which has no restore-until), a covering run with a recorded artifact, and a database that does not exist. `decidePointInTimePath` evaluates that ONCE per attempt, before routing, and records `neo4j.com/point-in-time-path` (`online` | `offline: <reason>`), because the online restore itself makes the database exist; it asks the target over Bolt only when nothing else blocks, never for a restore already holding the standalone stopped, and stays Pending (recording nothing) if the target cannot be asked. `standaloneOfflineReason` reads the record (no record = offline). A cluster that cannot run it online fails with the reason. The record is cleared with the other per-attempt annotations when the spec changes. If the database appears between decision and CREATE, the restore fails rather than recreate it.
- **Why:** a point-in-time restore stopped a standalone for the Job and was refused on clusters, though CalVer seeds to a point in time online. Walked live on Kind 2026-10-06 (2026.08.1, MinIO, `backupType: AUTO`): a `Phase 1` node, the point in time, a `Phase 2` node, then a run holding both. On a standalone, a restore to the point in time pinned that run, ran online with no Job and no restart, and the new database held `Phase 1` and not `Phase 2` — Neo4j truncated the run, so the operator did not merely pick an older one. Over the existing `neo4j` and past the newest run it was refused naming the reason; with `stopCluster` the Job restored the pinned run's 3-artifact chain until the point in time. On a 2-server cluster the same restore ran online with the same result, and over `neo4j` it failed naming the reason, leaving `neo4j` untouched.
- **Pinned-by:** `TestRestoreRunIndex`, `TestPointInTimeOnlineBlocker`, `TestDecidePointInTimePath`, `TestStandaloneOfflineReason_PointInTimeUndecided`, `TestEnsureResolvedBackupSource_PointInTimePinsTheCoveringRun` (`internal/controller/restore_point_in_time_test.go`); `TestValidateRestore_PointInTimeOnClusterNamesInstanceRef` (`neo4jrestore_resolvedsource_test.go`).
- **Status:** Enforced by unit test.

### Rule 113 — A one-time backup's terminal phase is decided before its target is looked at, from the API server when the cached copy is not terminal
- **Scope:** the one-time terminal guard near the top of `Neo4jBackupReconciler.Reconcile`, before `getTargetCluster` / `isClusterReady`, and `oneTimeBackupDone` (`internal/controller/neo4jbackup_controller.go`); `Neo4jBackupReconciler.APIReader`, wired in both controller registries of `cmd/main.go`; the original guard in the one-time path stays; the `BackupCompleted` / `BackupFailed` of `handleExistingBackupJob`.
- **Rule:** A `Neo4jBackup` without `spec.schedule` whose phase is `Completed` or `Failed` returns before the target lookup and readiness check (after removing a leftover CronJob). Nothing on the target's side may overwrite that phase: the lookup's `Waiting` was the only thing between a terminal one-time backup and the "no Job yet, create one" branch once the finished Job's TTL had expired (#116's original failure, reached by a different door). The pass's copy comes from the informer cache: when it is not terminal, `oneTimeBackupDone` reads the backup through the uncached `APIReader` (operations.md id 94), one read per pass of an unfinished one-time backup. `BackupCompleted` and the Job-failed `BackupFailed` are announced from the status write that enters the phase (operations.md id 136).
- **Why:** the v1.19.0 journey (2026-10-06) saw a `Completed` one-time backup read `Waiting` whenever its standalone restarted (`BackupCompleted ×7`), and after the Job was garbage-collected, recreating the standalone started a new backup Job that wrote a new artifact while `status.history` kept the old one. The v1.22.x walk saw the cache door: a sharded backup announced `BackupCompleted` as its Job went (TTL), and the next pass, on a cached copy from before that write, found the Job gone, started a new one — refused by the glob-safety check (Rule 48) — and wrote `Failed` over `Completed`.
- **Pinned-by:** `TestReconcile_TerminalOneTimeBackupIgnoresItsTarget` (`internal/controller/neo4jbackup_terminal_test.go`); `TestReconcile_AStalePassDoesNotReopenACompletedBackup` (a pass whose cache serves the copy from before the Completed write: no Job, still Completed) and `TestHandleExistingBackupJob_AnnouncesCompletedOnce` (`internal/controller/backup_walk_test.go`).
- **Status:** Enforced by unit test.

### Rule 174 — A succeeded one-time run waits for its artifact names a bounded time after its Job ends, never until the Job's TTL
- **Scope:** the deferral in `recordOneShotBackupRun`, `artifactCaptureGrace`, `jobFinishedAt`, `backupPodExists`, and the "capturing artifact metadata" pass of `handleExistingBackupJob` (`internal/controller/neo4jbackup_controller.go`).
- **Rule:** A Succeeded one-time run whose restore-critical artifact names (a database's `artifactFilename`, an all-databases `databaseArtifacts`, every shard's `filename`) were not read from its Pod's log stays `Running` ("Backup completed; capturing artifact metadata from the backup Pod") and is read again on the next pass — while its Pod exists AND for at most `artifactCaptureGrace` (5 minutes) after the Job finished (`completionTime`). After that the run is recorded with what was read, and the backup is `Completed`. A log that did not name the artifacts when the Pod finished never will; the wait is for a log read that failed on the way.
- **Why:** the wait used to last as long as the Pod, which the Job's 30-minute TTL removes with the Job. The v1.22.x walk's sharded backup of `spec.name: Products` (Rule 53) waited the whole 30 minutes, writing `status.lastRunTime` every pass (operations.md id 122), and its Job went from under it: the next pass found no Job, and only the terminal-phase read (Rule 113) keeps such a pass from starting the backup again.
- **Pinned-by:** `TestRecordOneShotBackupRun_StopsWaitingAfterTheGrace` (`internal/controller/neo4jbackup_history_test.go`: the Pod still there, the Job finished six minutes ago, the run recorded); `TestRecordOneShotBackupRun_DefersWhenMetadataMissingAndPodAlive` (same file) keeps the wait within the grace.
- **Status:** Enforced by unit test.

## Cross-references

- **AUTH RULE / sharded DDL `CYPHER 25` prefix** (rules 30, 65) — same root cause: 2026.x system DB defaults to Cypher 5.
- **`RetryOnConflict` on every status/spec write** (Key Implementation Patterns) — required across all backup/restore status updates for Neo4j 2025.01.0 cluster formation; `updateShardedDBLastBackup` and `markCypherRestoreIssued` both follow it.
- **Shell-quoting discipline** — `shellQuote` (`neo4jbackup_controller.go`) for any user-controlled value entering `/bin/sh -c`; `GetBackupCommand` / `GetRestoreCommand` (`internal/neo4j/version.go`) own quoting for the admin command itself.
