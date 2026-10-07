# Upgrade Guide

**v1.13.0 is the first public release of this independent project.** There is no
upgrade path from earlier version numbers — install v1.13.0 fresh (see the
[Installation guide](installation.md)).

> This is a personally maintained, community project with no affiliation to
> Neo4j, Inc. APIs and behaviour **may change between releases** — review the
> release notes before upgrading, and validate independently before relying on
> a new version.

## v1.14 — backup & restore API cleanup (breaking)

v1.14 removes the deprecated `Neo4jBackup` / `Neo4jRestore` fields that v1.13
kept working behind deprecation warnings. `v1beta1` is still a beta API, so these
are removed in place rather than versioned. **Update your `Neo4jBackup` and
`Neo4jRestore` manifests before upgrading** — the removed fields are silently
dropped by the API server (they're no longer in the CRD schema), which would
otherwise leave a backup/restore mis-scoped or unconfirmed.

Nothing else changes: cluster, standalone, database, user/role, and plugin CRs
are untouched.

### `Neo4jBackup`

| Removed | Replacement |
|---|---|
| `spec.target.{kind,name,clusterRef}` | `spec.instanceRef` + exactly one of `spec.database` / `spec.allDatabases` / `spec.shardedDatabase` |
| `spec.cloud` (top-level) | `spec.storage.cloud` |
| `spec.options.verify` | *(removed — it was never wired to `neo4j-admin backup validate`; use `spec.options.validate`)* |
| `spec.retention.deletePolicy: Archive` | `Delete` (the only supported value; `Archive` never had archival logic) |

```yaml
# BEFORE (v1.13)                          # AFTER (v1.14)
spec:                                     spec:
  target:                                   instanceRef: my-cluster
    kind: Cluster                           allDatabases: true
    name: my-cluster                        storage:
  cloud:                                       type: s3
    provider: aws                              bucket: backups
  storage:                                     cloud:
    type: s3                                     provider: aws
    bucket: backups
```

Scope mapping: `kind: Cluster` → `allDatabases: true`; `kind: Database` (with
`name: db`, `clusterRef: c`) → `instanceRef: c` + `database: db`;
`kind: ShardedDatabase` (with `name: sd`, `clusterRef: c`) → `instanceRef: c` +
`shardedDatabase: sd`.

### `Neo4jRestore`

| Removed | Replacement |
|---|---|
| `spec.clusterRef` | `spec.instanceRef` |
| `spec.databaseName` | `spec.database` |
| `spec.force` | `spec.options.replaceExisting: true` |
| `spec.verifyBackup` | *(removed — it was never implemented)* |

```yaml
# BEFORE (v1.13)                          # AFTER (v1.14)
spec:                                     spec:
  clusterRef: my-cluster                    instanceRef: my-cluster
  databaseName: orders                      database: orders
  force: true                               options:
  source:                                     replaceExisting: true
    type: backup                            source:
    backupRef: nightly                        type: backup
                                              backupRef: nightly
```

Also new in v1.14 (additive, no action needed): `Neo4jRestore.status.stats.duration`
and `status.backupInfo` are now populated on a successful restore.

## Upgrading from v1.16.x

Apply the new release's CRDs **before** upgrading the operator (step 1
[below](#upgrading-between-future-releases)). This release needs them for more
than new fields: learn mode keeps its state in new `Neo4jRole` status fields,
and without the CRDs every `Neo4jRole` goes `Failed` saying so. No privilege is
removed in that state.

### `Neo4jRole`: privileges are matched by learn mode (behaviour change)

Neo4j stores a privilege differently from how it is written (`NODES` becomes
`NODE`, lists become one row per item, aliases resolve to their target). The
operator used to compare the text, which toggled some privileges off on every
other reconcile. It now compares against what Neo4j stores, learned from its
own grants — **learn mode**, the new default; see
[Privilege drift reconciliation](user_role_management.md#privilege-drift-reconciliation).

What you will see on upgrade: **every existing role reports
`PrivilegesSynced=Unknown` (reason `UnattributedPrivileges`)**, because learn
mode cannot attribute rows that were already there. Nothing is revoked and
access is unchanged; the difference is that out-of-band additions to those rows
are not removed until you clean them up. To restore full enforcement on a
role, remove unwanted rows by hand or delete and recreate the `Neo4jRole`. If
you would rather have exact attribution immediately and can accept short-lived
`operator_privilege_probe_*` roles in Neo4j's security log, set the Helm value
`privilegeNormalisation: probe`.

### Sharding no longer sets the server's Cypher language

Enabling property sharding used to set `db.query.default_language=CYPHER_25`
for the whole server. It no longer does: sharded databases get Cypher 25 on
their own, and the server default is the new
`spec.serverDefaultCypherLanguage`. **Existing clusters are unaffected** — the
operator records what each one already runs and keeps writing it, so the
upgrade restarts nothing. See the
[property sharding guide](property_sharding.md).

### New, no action needed

- `spec.serverDefaultCypherLanguage` on clusters and standalones — see
  [Configuration](configuration.md). Existing deployments keep their current
  language.
- `spec.privilegeRules` on `Neo4jRole`: privileges as fields instead of Cypher.
- Remote composite constituents with stored credentials now work on the 5.26
  LTS; they failed there in v1.16.0. Their URL must be `neo4j+s://` or
  `neo4j+ssc://`, which is now checked at apply time.
- `spec.graphAnalytics` on `AuraInstance`: `unavailable`, `plugin` or
  `serverless`. Serverless graph analytics is created through Aura's v2beta1
  API. See the [AuraInstance API reference](../api_reference/aurainstance.md).

### Deprecated

- `AuraInstance.spec.graphAnalyticsPlugin` — use `spec.graphAnalytics`. It keeps
  working and maps to `plugin` (true) or `unavailable` (false). Setting both is
  refused. An existing instance may switch to the equivalent `graphAnalytics`
  value; setting any other `graphAnalytics` value is refused, because Aura cannot
  change it after creation. Editing or removing the boolean itself is accepted
  but has no effect on an existing instance, which only reads it at creation.

## Upgrading from v1.17.x

Apply the new release's CRDs before upgrading the operator, as always (step 1
[below](#upgrading-between-future-releases)). Nothing fails without them, but
`status.upgradeStatus.phaseStartTime` is new: on the old CRDs the API server
drops it, and `neo4j_operator_upgrade_duration_seconds` records nothing until
the CRDs are applied.

### Behaviour changes

- **Scheduled backups report `Ready=True`** (reason `BackupScheduled`). A
  `Neo4jBackup` with a `schedule` used to stay `Ready=Unknown` for its whole
  life, so `kubectl wait --for=condition=Ready` never returned. A suspended one
  now reports `Ready=False` with reason `Suspended` instead of
  `ReconciliationFailed`.
- **If you use the [ArgoCD health checks](../gitops/README.md), re-apply
  `docs/gitops/argocd-health-checks.yaml`.** In the copy published with v1.17.0,
  a scheduled or suspended `Neo4jBackup` stayed `Progressing` forever, which
  holds up any sync wave behind it. Both now report `Healthy`.
- **`Neo4jDatabase.spec.defaultCypherLanguage` is honoured on every create
  path, and gated on the server version.** On CalVer, a database created with no
  `topology` and no `seedURI` used to ignore the field and get the server
  default; it now gets the language you set. Existing databases keep theirs,
  because a database's language is fixed when it is created. On the 5.26 LTS,
  `"25"` now fails validation with a message naming the field (it used to fail
  at `CREATE DATABASE` with a syntax error), and `"5"` is accepted and the clause
  left out, since every database there runs Cypher 5. `Neo4jCompositeDatabase` follows the same rule.
- **A standalone ignores `Neo4jDatabase.spec.topology`**, as the docs always
  said: the operator no longer sends a `TOPOLOGY` clause to a standalone, and
  `primaries: 0` there is a warning instead of an error.
- **Sizes are checked before they reach Kubernetes.** A cluster and a standalone
  now accept the same `spec.storage.size`: any Kubernetes quantity greater than
  zero, so `1.5Gi` works on a cluster and `0` is refused on both. A malformed
  size (`fifty`, `10 Gi`, or `5K`, since a capital K is not a Kubernetes suffix)
  used to pass validation and crash the operator; the CR now goes `Failed` with
  a message naming the field. `Neo4jBackup.spec.storage.pvc.size` is checked
  the same way. Every value that is newly refused either crashed the operator
  or could hold no data.
- **A dotted `Neo4jCompositeDatabase.spec.name` is refused when you apply.** It
  always failed at reconcile; the schema now says so up front.
- **Auth provider names follow the Neo4j manual.** `plugin-<name>`, for an auth
  plugin or an add-on such as Kerberos, is now accepted in
  `spec.auth.authenticationProviders` and `authorizationProviders`; it used to
  be refused. `oidc` without a provider name, `kerberos`, `jwt`, `saml` and
  `custom` are not values Neo4j documents: they still validate, and now raise a
  `ValidationWarning` event that names what to use instead. See
  [Multi-provider support](security.md#multi-provider-support).

### New warnings

A field the schema accepts but nothing in the operator acts on now raises a
`ValidationWarning` event when you set it: `… is accepted but has no effect
today: <what to do instead>`. The warnings never block a reconcile. They cover:

- `Neo4jPlugin`: `spec.resources`, `spec.source.registry` (and its `tls`),
  `spec.security.securityPolicy`;
- clusters and standalones: `spec.tls.certificateSecret`; standalones only:
  `spec.tls.strictPeerValidation: false`;
- clusters: `spec.topology.placement.nodeSelector` and `.requiredDuringScheduling`;
- `Neo4jDatabase`: `spec.initialData.source` (other than `cypher`),
  `.configMapRef`, `.secretRef` and `.storage`;
- `Neo4jBackup` and `Neo4jRestore` cloud storage: `identity.serviceAccount`, and
  `identity.autoCreate.enabled: false`;
- `AuraInstance`: `spec.connectionSecretFormat: custom`.

### Metrics

**Removed:** twelve families that were registered but that no code ever
recorded. Each was a vector with no observations, so none ever exported a
series and no dashboard or alert loses data; you can delete panels built on
them. `neo4j_operator_` followed by: `backup_size_bytes`,
`cypher_execution_duration_seconds`, `cypher_executions_total`,
`disaster_recovery_status`, `failover_total`, `replication_lag_seconds`,
`manual_scaler_enabled`, `primary_count`, `secondary_count`,
`scale_events_total`, `scaling_validation_total`, `security_operations_total`.

**New or now populated:**

- `neo4j_operator_replica_lag_transactions` and
  `neo4j_operator_replica_promotions_total` for cross-cluster replication (see
  [Cross-cluster replication metrics](guides/monitoring.md#cross-cluster-replication-metrics)).
  The lag series is removed when it can no longer be trusted (the replica is
  gone or promoted, or its lag cannot be read) instead of holding its last value.
- `neo4j_operator_upgrade_duration_seconds` is now recorded, once per ended
  rolling-upgrade phase.

### New, no action needed

- `status.upgradeStatus.phaseStartTime` on clusters.
- `Neo4jShardedDatabase` status now fills `creationTime`, `graphShard`,
  `propertyShards` and `virtualDatabase`; `Neo4jPlugin` status fills
  `installedVersion` and `installationTime`. They were in the schema but empty.
- `ConnectivityDegraded` (a Warning on the cluster) now needs ten consecutive
  connectivity failures **spanning at least five minutes**, not ten alone. A
  new cluster no longer raises it while it forms; if you alert on this event,
  expect it no earlier than five minutes into a real outage.
- A failed Aura Fleet Management token registration raises an
  `AuraFleetManagementFailed` Warning event, once per distinct failure message.
  It almost never fired before.

## Upgrading from v1.18.x

Apply the new CRDs with the operator. They add three spec fields —
`Neo4jRestore.spec.source.sourceDatabase`,
`Neo4jBackup.spec.options.splitArchivePartSize` and
`Neo4jEnterpriseStandalone.spec.podServiceAccountAnnotations` — and status
fields the operator now records: `Neo4jBackup` `status.history[].artifactType`
(with `databaseArtifacts[].type` and `shardArtifacts[].type`), and `Neo4jRestore`
`status.resolvedSource.artifactType` and `backupStartedAt`. Without the new
CRDs those status fields are dropped on write; restores still work, merging
the chain of every PVC artifact they cannot tell is a full backup. The
behaviour changes below take effect on the first reconcile.

### Behaviour changes

- **A cluster that loses a server stays `Ready`**. It used to drop to
  `Forming` on any one pod restart, which paused every `Neo4jUser`,
  `Neo4jRole`, `Neo4jDatabase`, `Neo4jBackup` and the rest until the server
  came back, and left a cluster whose server never came back `Forming` for
  good. A cluster that has formed now stays `Ready` while a majority of its
  servers is serving, with a new `Degraded` condition naming the missing
  servers. If a server is still missing after the grace period (5 minutes;
  `--server-unavailable-grace`, Helm `serverUnavailableGrace`), the phase
  turns **`Degraded`**: `status.ready` is `false`, the `Ready` condition is
  `False` with reason `ClusterDegraded`, and a `ClusterDegraded` Warning event
  is raised. Losing a majority still reports `Forming`, now with a
  `ClusterQuorumLost` Warning. See
  [Server availability](../api_reference/neo4jenterprisecluster.md#server-availability).
- **Dependents keep working against a `Degraded` cluster.** Users, roles,
  bindings, auth rules, databases, sharded databases, aliases, composites,
  replicas, promotions and backups reconcile as they do against a `Ready` one.
  A rolling image upgrade or a `Neo4jPlugin` install waits until every server
  is back, including during the grace period.
- **Alerts and pipelines:** `kubectl wait --for=condition=Ready`, Flux health
  checks and the [ArgoCD health checks](../gitops/README.md) report a
  `Degraded` cluster as not ready — that is new, since it used to be
  `Forming` (`Ready=Unknown`, ArgoCD `Progressing`). If you alert on
  `neo4j_operator_cluster_phase{phase="Forming"}` to catch a lost server, alert
  on `phase="Degraded"` too, or on the `Degraded` condition directly.
- **Diagnostics keep running while a server is down.** `ServersHealthy`,
  `DatabasesHealthy`, `status.diagnostics` and `neo4j_operator_server_health`
  used to freeze at their last healthy values whenever the cluster was not
  `Ready`; they are now collected whenever the cluster has formed.
- **`ClusterFormationStarted` is raised on first formation only**, no longer
  on every pod restart of a formed cluster.
- **`neo4j_operator_server_health` no longer leaves series behind.** A server
  that was down was reported with `server_address="<nil>"` (also in
  `status.diagnostics.servers[].address`), which started a new series and left
  the old one exported at its last value — a `0` that kept a
  `server_health == 0` alert firing after the server came back. A down server
  now keeps the last address it reported, and series a cluster no longer
  reports are withdrawn.
- **A standalone restores online.** A `Neo4jRestore` into a
  `Neo4jEnterpriseStandalone` no longer stops the instance: it runs
  `dbms.recreateDatabase` / `CREATE DATABASE … OPTIONS { seedURI }` against
  it, so only the restored database is unavailable, and `stopCluster: true` is
  ignored. These still restore offline and need `stopCluster: true`:
  `source.type: storage`; a point-in-time restore that cannot run online
  (below); cloud storage by pod identity, unless the standalone sets the new
  `spec.podServiceAccountAnnotations` (below); and a backup run that recorded
  no artifact. A restore already running offline when you upgrade finishes
  offline. See
  [Restore to a Standalone Instance](guides/backup_restore.md#restore-to-a-standalone-instance).
  - From **cloud storage with static credentials**, the standalone's own pods
    now fetch the seed. They need the credentials Secret in `spec.extraEnvFrom`
    (and, for MinIO, the endpoint in `spec.env`), or the
    `neo4j.com/auto-inherit-seed-creds: "true"` annotation, which adds them at
    the cost of one restart. Without either, the restore fails and says so.
- **Point-in-time restores run online.** A `Neo4jRestore` with
  `source.pointInTime` from a `Neo4jBackup` in cloud storage, into a database
  that does not exist yet, on CalVer, now creates the database with
  `seedRestoreUntil`, on a cluster or a standalone. It used to stop a
  standalone for the Job, and a cluster refused it. The restore now uses the
  backup run that holds the point in time (the earliest that started at or
  after it), not the most recent run. Other point-in-time restores still take
  the Job on a standalone; on a cluster they fail naming the reason. See
  [Point-in-Time Recovery](guides/backup_restore.md#point-in-time-recovery-pitr).
- **Cluster restores run their hooks.** `spec.options.preRestore` and
  `postRestore` were ignored on clusters; they now run before the restore is
  issued and once the database is online, as on a standalone. A failing
  post-restore hook fails the restore. Check any cluster `Neo4jRestore` that
  carries hooks before re-running it.
- **PVC differentials restore online.** Seeding a cluster from a PVC
  differential — every scheduled `backupType: AUTO` run after the first —
  created the database and left it offline (*"not part of a valid backup
  chain"*). The seed proxy now merges the chain into one full backup first,
  with the target's own Neo4j image, unless the backup recorded the artifact
  as a full backup. The merge needs scratch space about the size of the chain
  (an `emptyDir` unless you set `spec.options.tempStorage`) and time: the
  restore waits up to `spec.timeout`, 30 minutes by default while merging.
  Sharded databases are the exception (next two items).
- **A sharded seed from a PVC differential fails at once.** A sharded
  database seeds from a PVC only when every shard of the run is a full
  backup, and a differential cannot be merged into one. A `seedBackupRef`
  whose latest run holds a differential shard used to fail inside Neo4j after
  the seed proxy started; it now fails at once, naming the shards, and seeds
  once a full run lands. An all-databases PVC run that writes a differential
  shard raises a `BackupShardedDifferential` warning.
- **`backupType: AUTO` takes full backups of a sharded database on a PVC.**
  A `Neo4jBackup` with `spec.shardedDatabase` and PVC storage used to write a
  differential on every run after the first, and none of those can seed a
  sharded database. `AUTO` (the default) now takes a full backup on every run
  there. Expect each run to be as large as the first; size the PVC and
  `spec.retention` for it. Cloud storage keeps `AUTO` as it was, since it
  seeds differentials. See
  [Restoring a sharded database](property_sharding.md#restoring-a-sharded-database).
- **A finished one-time backup stays finished.** A `Neo4jBackup` without a
  schedule that was `Completed` or `Failed` turned `Waiting` whenever its
  target restarted or was briefly missing, and once its Job had been cleaned
  up it could run again, writing a new backup. It now keeps its phase.
- **`backupType: DIFF` is refused for a sharded database.** Neo4j does not
  back up property shards differentially, so every run of such a backup
  failed. A `Neo4jBackup` with `spec.shardedDatabase` and `backupType: DIFF`
  is now refused by validation and names the field; use `AUTO` or `FULL`.
- **A sharded seed from MinIO gets its endpoint.** Seeding a sharded database
  from S3-compatible storage under `neo4j.com/auto-inherit-seed-creds` added
  only the credentials to the cluster, so Neo4j went to AWS for the bucket.
  The operator now adds the endpoint too, in the same rolling restart, and
  waits for it before seeding.

### New, no action needed

- `Neo4jEnterpriseStandalone.spec.podServiceAccountAnnotations` binds a
  standalone's pod to a cloud role (Workload Identity), as the cluster field
  does: the operator creates a `<name>-neo4j` ServiceAccount carrying them and
  runs the pod under it, so a restore from cloud storage without a credentials
  Secret runs online. Standalones that do not set it are not restarted by the
  upgrade. Apply the new CRDs to use it.
- `Neo4jBackup` `status.history[].artifactType` (and `databaseArtifacts[].type`):
  `FULL` or `DIFF`, read from each run's log. `Neo4jRestore`
  `status.resolvedSource.artifactType` carries it into the restore.
- An all-databases restore records `status.completionTime`.
- `Neo4jRestore` `status.resolvedSource.backupStartedAt`: when the resolved
  backup run started.
- **A standalone restores into a new database name.** It used to look for the
  *target* name's files and fail. Standalone restores from a
  `backupRef` now read the run's recorded artifact, as cluster restores
  always have. New `Neo4jRestore.spec.source.sourceDatabase` picks one
  database out of an all-databases backup to restore under `spec.database`,
  on clusters and standalones. See
  [Restoring under a different name](guides/backup_restore.md#restoring-under-a-different-name).
- `Neo4jBackup.spec.options.splitArchivePartSize` (Neo4j 2026.09+) writes each artifact as several files — see [Split backup archives](guides/backup_restore.md#split-backup-archives). Apply the new CRDs to use it. PVC retention now deletes a split artifact's data parts with it; before, passing `--split-archive-part-size` through `additionalArgs` left the parts on the volume.
- Conditions `Degraded` and `ClusterFormed` on `Neo4jEnterpriseCluster`.
- The `Ready` condition of a `Degraded` cluster has reason `ClusterDegraded`
  instead of `ReconciliationFailed`.
- Operator flag `--server-unavailable-grace` (default `5m`) and Helm value
  `serverUnavailableGrace`.

## Upgrading between future releases

When a newer version ships:

1. **Refresh the CRDs first.** Helm does not upgrade CRDs automatically, so a new
   field or validation won't take effect until the CRDs are applied. Apply the
   bundle for the version you are upgrading to (replace the tag):

   ```bash
   kubectl apply --server-side -f \
     https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v1.19.0/neo4j-kubernetes-operator.yaml
   ```

2. **Upgrade the operator** via Helm:

   ```bash
   helm repo update
   helm upgrade neo4j-operator neo4j-operator/neo4j-operator \
     --namespace neo4j-operator-system
   ```

   (or re-apply the complete bundle if you installed with plain `kubectl` —
   see the [Installation guide](installation.md)).

3. **Read the release notes** for that version for any breaking changes or
   manual steps, and roll forward one minor version at a time if you are
   skipping several.

The operator reconciles declaratively, so once the new version is running it
converges existing `Neo4j*` resources to the new behaviour automatically.
