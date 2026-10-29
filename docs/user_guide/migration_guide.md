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
- **A plugin no longer overrides a standalone's own procedure settings.**
  On a `Neo4jEnterpriseStandalone`, a `Neo4jPlugin` also wrote its security
  settings (`dbms.security.procedures.*` and the like) as environment
  variables, which Neo4j applies over `neo4j.conf` — so a standalone's own
  `spec.config` values for those keys were silently replaced by the plugin's
  (for example `gds.*,apoc.*` cut to `apoc.*`). They now live only in
  `neo4j.conf`, merged with yours, and the plugin's `spec.security` is
  written there too. **Each standalone with a `Neo4jPlugin` restarts once on
  upgrade** as the environment copies are removed. Clusters are unchanged.
- **A deleted cluster stops exporting metrics.** Its series —
  `cluster_healthy`, `cluster_phase`, `server_health` and the rest — stayed
  exported at their last values until the operator restarted; they are now
  removed when the cluster is deleted.
- **`kubectl neo4j validate --connect` checks namespace-less manifests in the
  right namespace.** Without `-n` it looked cross-references up in an empty
  namespace and reported raw client errors (and could pass a remote composite
  whose target has no keystore); it now uses the kubeconfig context's
  namespace, else `default`.
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

## Upgrading from v1.19.x

Apply the new CRDs with the operator. They add `spec.monitoring.logs` to
`Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`, and change the
description of `spec.topology.minSystemPrimaries`. **Upgrading the operator
restarts no server.** The new startup-script blocks it adds take effect at each server's
next restart, and the operator does not restart servers for them. The
behaviour changes below take effect on the first reconcile.

### Behaviour changes

- **A new cluster of more than three servers has three `system` primaries.**
  Each server's role for the `system` database is now fixed at its first
  start: servers below `spec.topology.minSystemPrimaries` (default
  `min(3, servers)`) are primaries, the rest secondaries. Before, every server
  was a primary. `system` (databases, users, roles) stays writable while a
  majority of its primaries is up — with the default, two of servers 0–2,
  however large the cluster. Set `minSystemPrimaries` to an odd number above
  3 if you need more. Servers that already exist keep their role. See
  [System primaries and secondaries](clustering.md#system-primaries-and-secondaries).
- **Scaling up restarts nothing**, as long as every added server is a system
  secondary (with the default, any scale-up from three servers or more); a
  `ScaledWithoutRestart` event says so. A scale-up that adds a system
  primary, such as 2 → 3, still restarts the servers. Scaling down restarts
  nothing.
- **Changing a setting Neo4j can change at runtime restarts nothing.** When a
  `spec.config` change touches only dynamic settings, the operator applies it
  to every running server (`dbms.setConfigValue`) and then writes it to
  `neo4j.conf`, raising `ConfigAppliedLive`. Anything else restarts the
  servers as before, and `ConfigNeedsRestart` names the setting and the
  reason. A change to comments or blank lines in the rendered configuration
  restarts nothing either. See
  [Which changes restart the servers](configuration.md#which-changes-restart-the-servers).
- **5.26 clusters stop stalling writes while a server is down.** On
  5.26.29 and later the operator enables
  `dbms.cluster.raft.async_channel_acquisition_enabled`, which stops the
  leader waiting up to 30 seconds for a stopped member. Existing servers get
  it at their next restart; until then the new `RestartPending` condition
  names them. See
  [5.26: writes while a server is stopped](clustering.md#526-writes-while-a-server-is-stopped).
- **Renewed TLS certificates are reloaded without a restart** with
  `spec.tls.mode: cert-manager` on CalVer 2025.03 and later. The operator
  enables `dbms.security.tls_reload_enabled` and reloads each renewal,
  raising `TLSCertificateReloaded`. Existing
  servers get the setting at their next restart (`RestartPending` names them
  on a cluster); until then a renewal reaches them only when they restart,
  and a `TLSCertificateNeedsRestart` Warning names them. On 5.26 a renewal
  still takes effect at the next restart. See
  [TLS certificate rotation](security.md#tls-certificate-rotation).
- **Deleting a TLS deployment deletes its certificate Secret.** cert-manager
  leaves the Secrets it issues behind, so `{name}-tls-secret` (a certificate
  and private key) outlived every deleted cluster and standalone. The
  operator now deletes it when cert-manager issued it for that deployment's
  Certificate; a Secret of that name you created yourself is kept.
- **A Ready cluster reconciles twice a minute, not continuously.** Each pass
  wrote a fresh `status.diagnostics.lastCollected`, which re-queued the
  cluster at once. Diagnostics are now written when they change, or at least
  every 5 minutes, so `lastCollected` can be up to 5 minutes old on a healthy
  cluster; alert on the conditions (`ServersHealthy`, `DatabasesHealthy`),
  not on its age. The standalone is the same.
- **Split-brain detection is paced on a quiet cluster.** The full check,
  which connects to every server, runs on a `Ready` cluster at most every
  5 minutes while no server pod changes, and at once when one is deleted,
  recreated, restarts or changes readiness, or when a server is missing from
  `SHOW SERVERS`. `--split-brain-check-interval=0s` checks on every pass, as
  before. See [Many large clusters](guides/performance.md#many-large-clusters).
- **A crashed server container no longer turns the cluster `Forming`.** When
  Neo4j cannot be queried at all — the check landed on a container that had
  just died — a formed cluster with a majority of its server pods Ready now
  keeps its phase. If it stays unreachable for the grace period (a wrong
  admin password, say) it turns `Forming` as before.
- **`Neo4jDatabase` names must be 3 to 63 characters**, as Neo4j requires.
  A name outside that range used to pass validation and fail at
  `CREATE DATABASE`; it is now refused by validation, naming the rule.
- **A scale-down no longer raises a false `ScaleDownBlocked`.** A drain step
  Neo4j had already taken ("already deallocating", "already dropped") was
  reported as a blocked scale-down while it went on to complete.

### New, no action needed

- `spec.monitoring.logs.stdout` (clusters and standalones) writes Neo4j's
  `query`, `security` and `debug` logs to standard output as JSON, besides
  their files, for an OpenTelemetry Collector or another node-level log
  collector. Turning it on restarts each server once; deployments that do not
  set it are unchanged. See [OpenTelemetry](guides/opentelemetry.md), which
  includes a tested Collector configuration for these logs and Neo4j's
  metrics. Apply the new CRDs to use it.
- Operator flags `--ready-poll-interval` (default `30s`) and
  `--split-brain-check-interval` (default `5m`); Helm values
  `readyPollInterval` and `splitBrainCheckInterval`.
- Condition `RestartPending` on `Neo4jEnterpriseCluster`.
- Events `ConfigAppliedLive`, `ConfigNeedsRestart`, `ScaledWithoutRestart`,
  `TLSCertificateReloaded` and `TLSCertificateNeedsRestart`.
- Annotations the operator records: `neo4j.com/deferred-settings` and
  `neo4j.com/deferred-settings-since` on the cluster ConfigMap,
  `neo4j.com/tls-certificate` on the StatefulSet of a TLS deployment, and
  `neo4j.com/config-semantic-hash` on the standalone's StatefulSet. Each
  cluster server records its `system` role in
  `/data/.neo4j-operator/system-database-mode`.

## Upgrading from v1.20.x

### Behaviour changes

- **A rolling upgrade waits for each restarted server's databases.** Before
  the next server restarts, every database copy on the one that just did must
  have caught up (`dbms.cluster.statusCheck`, and `SHOW DATABASES` for
  single-primary databases and secondaries); `status.upgradeStatus.message`
  names the databases it is waiting for. A server used to count as done once
  it rejoined, while its databases could still be catching up, and restarting
  the next one then left a database with three primaries a single working
  copy. Upgrades of clusters with many databases take somewhat longer — about
  15 seconds per server with 30 databases on a laptop — and stay within
  `upgradeTimeout`.
- **`spec.upgradeStrategy.maxUnavailableDuringUpgrade` set to anything other
  than `1` raises a `ValidationWarning`.** It never had an effect: servers
  restart one at a time, as Neo4j's rolling upgrade requires. Remove it, or
  leave it at `1`.
- **Diagnostics keep updating during a rolling upgrade.** `status.diagnostics`,
  the `ServersHealthy` and `DatabasesHealthy` conditions and the
  `neo4j_operator_server_health` metric used to keep their values from before
  the upgrade until it finished; they now follow the servers as they restart,
  at most once per `--ready-poll-interval`. Alerts on them should allow for a
  server being down for a minute or so, as the documented examples (5 minutes)
  do.
- **The operator reuses its Neo4j connections.** It used to open new ones, and
  log in, on every pass: about 17 `logged in` lines a minute in `security.log`
  for one quiet 3-server cluster, now under one. A changed admin password or
  TLS certificate still takes effect at the next pass.
- **The `<cluster>-discovery` Role, RoleBinding and Service are deleted.** The
  Role and RoleBinding let the server pods list Services and Endpoints; the
  Service exposed the V1 discovery port 5000. Nothing used them: discovery reads
  pod addresses from DNS. Upgrading the operator deletes the ones it created and
  restarts nothing; the `<cluster>-discovery` ServiceAccount stays, with any
  `spec.podServiceAccountAnnotations`. An object of that name that the operator
  did not create is left alone. The operator's own ClusterRole now only reads
  and deletes Roles and RoleBindings.
- **A failed cluster reconcile is retried on its own backoff** (5–30 seconds).
  Retries used to share one slot every 6 seconds across all clusters, so after
  a failure that hit many clusters at once some waited minutes.
- **Clusters get `dbms.cluster.network.connect_timeout=5s`** (Neo4j's default
  is 30s). Upgrading the operator applies it to the running servers without a
  restart (a `ConfigAppliedLive` event); Neo4j's Raft connections take it up at
  each server's next restart. It removes pauses of about 10 seconds on a
  database whose leadership moved to a server that had just restarted. To keep
  30s, set it in `spec.config`. See
  [Writes while a restarted server rejoins](clustering.md#writes-while-a-restarted-server-rejoins).
- **Clusters are reconciled four at a time** instead of one, and each
  cluster's reconcile is cut off and retried after 10 minutes. One slow or
  unreachable cluster no longer delays the others. The new flags
  `--cluster-max-concurrent-reconciles` and `--cluster-reconcile-timeout`
  (Helm `clusterMaxConcurrentReconciles`, `clusterReconcileTimeout`) change
  them; `--cluster-max-concurrent-reconciles=1` restores the old behaviour.
  See [Many large clusters](guides/performance.md#many-large-clusters).

## Upgrading from v1.21.0

No CRD changes; upgrading the operator restarts no server.

### Fixes

- **A backup-mode `Neo4jReplicaDatabase` finds `AWS_REGION` in
  `spec.extraEnvFrom`.** The region check before seeding from `s3://` read only
  `spec.env` and the server container's `env`, so a downstream cluster whose
  region came from a Secret or ConfigMap in `spec.extraEnvFrom` — the field is
  documented for exactly that — had its replica `Failed` with "AWS_REGION is not
  set". A replica in that state is retried once the upgraded operator runs.
  `kubectl neo4j preflight` reads the same sources.
- **A plugin's `NEO4J_PLUGINS` could vanish from a cluster's StatefulSet.**
  When the cluster controller's StatefulSet update conflicted with the plugin
  controller's patch, the retry applied the template it had just read and
  recorded the plugin's variables as its own; the next pass removed them. The
  `Neo4jPlugin` stayed `Ready` with the plugin missing from the servers. The
  upgraded operator reconciles every `Neo4jPlugin` when it starts, which puts a
  missing `NEO4J_PLUGINS` back — rolling that cluster's servers once, as the
  original install did.
- **Advisory warnings are announced once, not on every reconcile.**
  `ValidationWarning` events — and a cluster's `TopologyWarning` — were
  emitted again on every pass, dozens a minute for one object, which also used
  up that object's event budget so other events could be dropped. Each warning
  is now announced when it starts to apply and again every 30 minutes while it
  still does, so it stays visible in `kubectl describe` without the flood.

## Upgrading from v1.21.x

Apply the new CRDs before upgrading the operator. They add three status fields — `status.runningVersion` on `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`, and `status.upgradeStatus.resolvedTargetVersion` on the cluster — where the operator records the version the servers report for an image tag without a patch (see [Tags without a patch](guides/upgrades.md#tags-without-a-patch)); the other changes are field descriptions (the `spec.upgradeStrategy` timeouts and strategy, `Neo4jDatabase` status fields, `Neo4jShardedDatabase`'s `ifNotExists`, `status.version`, `status.observedGeneration`, the conditions of `Neo4jUser` and `Neo4jRoleBinding`). The operator needs no new permissions. If the CRDs are not applied, the API server drops the new fields: the operator works as before, but a tag without a patch is not checked against what the servers run (the downgrade below is accepted, as in v1.21.x), and on every `Ready` pass the cluster controller logs `Recording the version the servers report` and sends the field again, which the API server drops without changing the object (no event, no extra reconcile). Apply the CRDs and the next pass records it. Upgrading the operator restarts no server by itself, except as listed under [Restarts after the upgrade](#restarts-after-the-upgrade).

### Act now: re-apply manifests that set `false`

Earlier releases wrote most custom resources back whole when they added a finalizer or an annotation, and so stored values you never wrote (see "The operator no longer rewrites the spec you applied" below). The upgrade does not undo what they stored. If you set any of these, re-apply the manifest after upgrading — with server-side apply, add `--force-conflicts`:

- `enforcePrivileges: false` on a `Neo4jRole`, or `enforceRoles: false` on a `Neo4jAuthRule`. They were stored as `true`, so the operator has been revoking privileges and roles you chose to keep.
- `spec.mcp.readOnly: false` on a cluster or standalone. It was stored as `true`, so the MCP server has run read-only.
- Any other `false` the CRD defaults to `true`: `Neo4jDatabase.ifNotExists` and `wait`, and on a cluster or standalone `spec.monitoring.enabled` and the `spec.upgradeStrategy` flags (`preUpgradeHealthCheck`, `postUpgradeHealthCheck`, `autoPauseOnFailure`).
- `spec.auth.authorizationProviders` on a cluster that set no `authenticationProviders`. Both lists were stored as `[native]`.
- Provider settings in `spec.config` (`dbms.security.authentication_providers`, `dbms.security.authorization_providers`) on a cluster that set no `spec.auth` lists. Both lists were stored as `[native]`, which take precedence over `spec.config`, and re-applying does not remove them. Remove them by hand:

  ```bash
  kubectl patch neo4jenterprisecluster <name> --type=json -p \
    '[{"op":"remove","path":"/spec/auth/authenticationProviders"},{"op":"remove","path":"/spec/auth/authorizationProviders"}]'
  ```

Re-applying `monitoring.enabled: false` or a provider list changes what the servers run, so they restart (rolling).

Three more were stored differently but behave the same, so they show only as drift in GitOps tools: `Neo4jPlugin.enabled: false` was stored as `true` (the plugin controller does not read the field, so the plugin is installed either way; [#540](https://github.com/priyolahiri/neo4j-kubernetes-operator/issues/540)), `identity.autoCreate.enabled: false` on a backup or restore was stored as `true` (the field has no effect yet), and a `false` with no CRD default, such as an `AuraInstance`'s `deletionProtection: false`, was dropped.

### Newly refused — check your manifests first

Run `kubectl neo4j validate -f <manifests>` (add `--connect` to also check image tags against what the deployment runs) before upgrading. A cluster or standalone that carries one of these turns `Failed`, and the operator makes no further changes to it until it is fixed. Its running servers are left alone; the databases, users and backups that target it wait. A composite database or alias refused for a name turns `Failed` itself.

- **`spec.config` values Neo4j cannot parse.** 46 commonly set settings are type-checked the way Neo4j parses them, in `spec.config` and in a cluster's `spec.propertySharding.config`: `db.tx_log.buffer.size: "1MiB"` (write `1048576`), `server.memory.heap.max_size: "2Gi"` (write `2G` or `2GiB`), `db.transaction.timeout: "30 sec"`, `db.logs.query.enabled: "true"` (use `INFO`), or an empty value for one of them. Each of these would have stopped the server at its next start. Memory sizes in Neo4j's own units such as `1GiB` or `512MB`, previously refused as "invalid memory format" when a memory limit was set, are now accepted.
- **Downgrades.** Once a cluster has been `Ready`, `spec.image.tag` is checked against `status.version` (what the servers run) on every pass, mid-upgrade included, and a lower tag is refused before anything changes. Earlier releases let it through to the upgrade, which then failed. Once a rolling upgrade has restarted a server on its target, a tag below that target is refused too, `status.version` included: those servers would be rolled back. A standalone's tag is checked against its `status.version` (it had no check before). For a tag without a patch, such as `5.26-enterprise`, the patch is checked against the version the servers report, once the operator has recorded it: `5.26.31-enterprise` is refused for a cluster whose servers run 5.26.32 under `5.26-enterprise`, during an upgrade to it and after (in v1.21.x it was accepted, and the servers were rolled down). Upgrade paths are otherwise unchanged: CalVer only from 5.26.
  - **When the versions are recorded after upgrading the operator** (from v1.21.x, or from a v1.22.0 pre-release build). The new status fields start empty, and until they are filled a patch is not ordered against a tag without one, as in v1.21.x. A cluster fills them on its first `Ready` pass under the new operator, from the `SHOW SERVERS` its formation check already runs on every pass (no extra query): `status.runningVersion` when `status.version` has no patch, and, for a `Paused` or `Failed` upgrade to a tag without a patch, `status.upgradeStatus.resolvedTargetVersion` from the servers it restarted. An upgrade in flight fills `resolvedTargetVersion` at its next availability check of a restarted server (every 15 seconds while it waits). A standalone asks its server once, at its first `Ready` pass, and again only when its container runs another image. Each is written when it changes, not on every pass. A cluster or standalone whose `status.version` names a patch needs none of this: the tag says the version.
- **Upgrade timeouts of zero or less** on a cluster (`spec.upgradeStrategy.upgradeTimeout`, `healthCheckTimeout`, `stabilizationTimeout`).
- **Names that differ only in case** (`Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`): two constituents of one composite, a constituent that targets its own composite, or an alias that targets a database of its own name, in another case.
- **An enabled `spec.auth.externalSecrets` that `spec.tls.externalSecrets` would refuse**: no `secretStoreRef` or store name, an unknown store `kind`, no `data` entries, or a bad `refreshInterval`. Before, a block without a `secretStoreRef` panicked on every pass of its cluster before any server was built; the rest were accepted.

### Behaviour changes

- **The operator no longer rewrites the spec you applied.** Every write of a custom resource (finalizers, annotations) is a patch of what changed, and defaults fill only what you left unset, in memory. Earlier releases sent the whole object back, so a `false` the CRD defaults to `true` came back `true`, a `false` with no default was dropped, quantities were re-spelled (`cpu: "0.5"` as `500m`), and a cluster's first accepted pass stored the operator's defaults (`tls`, `service`, both auth provider lists, the image pull policy). Objects keep what earlier releases stored; see [Act now](#act-now-re-apply-manifests-that-set-false). Rendering is unchanged for every spec an earlier release stored, so this restarts nothing (one exception is under [Restarts after the upgrade](#restarts-after-the-upgrade)).
- **A Paused or Failed rolling upgrade holds where it stopped.** The servers it has not reached stay on the old version until it goes on: a `Failed` upgrade is retried once every server is available; a `Paused` one resumes on a new `spec.image.tag`, or when `spec.upgradeStrategy.autoPauseOnFailure` is turned off, and an `UpgradeStarted` event says it resumed. Earlier releases restarted the remaining servers onto the target at the next pass, without the database catch-up checks, and a Paused upgrade could not resume. While it holds, the cluster is `Ready` once every server is available, `status.message` names the held upgrade, and the rest of reconciliation runs. See [When an upgrade stops](guides/upgrades.md#when-an-upgrade-stops-paused-and-failed).
- **Upgrades to a tag without a patch complete.** An upgrade to a tag such as `2026.08-enterprise` is verified against any patch of its line; a tag with a patch must still match exactly. Earlier releases failed it after every server had restarted.
- **Events are announced once.** `ValidationFailed` (all 12 kinds that emit it), `DatabaseReady`, `BackupScheduled`, the sharded database's state events and about 40 other Warnings are emitted when the condition starts (or its message changes), not on every reconcile, and not again after an operator restart. Ten more Warnings that stay true while the resource works, such as `RouteAPINotFound` and `MCPApocMissing`, now follow `ValidationWarning`: once, then every 30 minutes while they still apply. A retried storage expansion or replica promotion keeps showing its last failure until it succeeds, instead of changing phase on every attempt. Alerts that count repeats will see fewer events.
- **Waits are `Pending`, not `Failed`.** A precondition you can fix and the operator retries now waits in `Pending`, naming what is missing, and carries on by itself once fixed: a `Neo4jRestore` missing seed credentials; a `Neo4jShardedDatabase` missing seed credentials or whose `clusterRef` names no cluster; a `Neo4jDatabase` waiting for seed credentials (was `Unknown`); a `Neo4jReplicaDatabase` whose servers lack `AWS_REGION` (Warning reason `ReplicaObjectStoreEnvMissing`, was `ReplicaFailed`). Argo CD shows the restore, sharded-database and replica waits as Progressing instead of Degraded. An `AuraInstance` deletion that `deletionProtection` blocks sets `Synced=False` with reason `DeletionProtected`.
- **Plugins wait for a fully available target.** A plugin install, repair or trusted-CA update waits in `Waiting` while its target is not `Ready` with every server available, or has not finished rolling out its own latest change, with one `PluginInstallDeferred` event (`PluginMissing` for a repair). Plugins applied together with a new cluster install once it is `Ready`. A plugin is `Ready` only once the restart its own write caused has reached every server; while its init container keeps a server from starting, its message names the pod. The writes that do not wait are a fix of a `VerifiedDownload` plugin's own failing download, when every server that is down is failing in that download (one `PluginInitContainerFailing` Warning; see the [plugins guide](guides/plugins.md#installmode-verifieddownload)), and a change of a plugin's settings when every server that is down keeps failing while running them (one `PluginSettingsFailing` Warning; see [Plugin configuration](guides/plugins.md#plugin-configuration)).
- **A failed plugin install is retried** every 30 seconds, with one `PluginInstallFailed` Warning per failure. It used to stay `Failed` until the CR was edited or the operator restarted. A server that cannot be reached mid-install keeps the plugin `Installing`, with no Warning.
- **A `Ready` plugin repairs its target.** If `NEO4J_PLUGINS`, a variable the plugin sets, or a `VerifiedDownload` init container or volume is removed from the target's StatefulSet, the plugin turns `Installing` with one `PluginMissing` Warning and puts it back (one rolling restart). A plugin that would change a variable the cluster renders itself (typically `NEO4J_PLUGINS` in `spec.env`) goes `Failed` with one `PluginInstallFailed` Warning and retries every 30 seconds, instead of the two controllers restarting the servers in turn.
- **A `VerifiedDownload` plugin follows `trustedCASecrets`.** Changing the target's `spec.trustedCASecrets` restarts its servers twice: for the target's truststore, then for the plugin's `plugin-ca` volume. The plugin waits for the first restart to finish before it starts the second. The download trusts those CAs in addition to the init image's default CAs, so a plugin downloaded from a public URL keeps working. The first time after upgrading, delete a removed CA's Secret only once the plugin is `Ready` again: a volume an earlier version wrote still requires it. A `VerifiedDownload` install is now one StatefulSet write, so one restart.
- **`RestartPending` tracks connect-timeout changes.** A change of `dbms.cluster.network.connect_timeout` (applied live; Neo4j's Raft connections take it up at each server's next restart) names the servers that have not restarted since. Upgrading from v1.21.x names nobody, including servers that took the 5s value live under v1.21.x and have not restarted since.
- **`upgradeStrategy.strategy: Recreate` now applies changes.** A cluster upgrades through the rolling-upgrade state machine one server at a time, with a `ValidationWarning` saying so; a cluster whose new `spec.image.tag` was held back starts that upgrade. A cluster's or standalone's StatefulSet switches from `OnDelete` to `RollingUpdate`, so template changes that were stuck roll out.
- **Role privileges re-learn only for aliases a role names.** Statements on `*`, `HOME`, `DEFAULT` and `DBMS` no longer re-learn on alias changes. After upgrading, a role on a cluster with an alias it doesn't name re-learns once (no-op grants, not reported), then settles. `PrivilegesApplied` counts only the grants that created a row.
- **Database names are compared ignoring case**, as Neo4j does. Resources with capitals stop re-sending `CREATE`/`ALTER` on every pass, and a `Neo4jDatabase` or `Neo4jReplicaDatabase` that went `Failed` with "already exists" recovers. A `Neo4jRestore` naming a sharded database in another case is refused and pointed at the `Neo4jShardedDatabase` restore path, as the same spelling was.
- **`kubectl neo4j validate`** runs the rules the operator runs on every pass — for a cluster after the operator's defaults, `spec.upgradeStrategy` included; `--connect` also checks a cluster's or standalone's tag against the version it runs, and a `Neo4jBackup`'s `spec.shardedDatabase` against the `Neo4jShardedDatabase` it names (by CR name, as the operator does). `preflight` marks a pass with ✓. `status --problems` lists a `Neo4jDatabase` in `ValidationFailed`, and `diagnose` sends a refused cluster or standalone without a StatefulSet to its status message rather than to the operator's logs.
- **More events once:** `RolesResolved` when a `Neo4jUser`'s or `Neo4jRoleBinding`'s CR-name mapping appears or changes (it is kept in a new `RoleNamesResolved` condition), and `DeletionFailed` once per failure while a `Neo4jDatabase` deletion retries its `DROP DATABASE` (the database shows phase `Error` meanwhile). `ShardedDatabaseReady` and `SeedEndpointNotProjected` no longer come twice in the same second, and an offline standalone restore announces `RestoreStarted` only once its checks have passed.
- **Messages that alerts may match.** A cluster refused before it was ever accepted reports "Cluster validation failed", not "Update validation failed". `BackupScheduled` names the schedule, so a `spec.schedule` edit is announced. A refused cluster or `Neo4jShardedDatabase` says "Validation failed" once, not "Validation failed: validation failed: …", and the sharded database's `ValidationFailed` event carries its status message. The offline restore's `RestoreStarted` with `stopCluster: false` reads `Restoring offline with neo4j-admin into standalone "<name>", which is stopped: …`.

### Fixes

- **A deployment with an OpenShift Route reconciles.** Since v1.13.0, every pass of a cluster or standalone with `spec.service.route.enabled` or an MCP Route (`spec.mcp.http.service.route.enabled`) panicked before the server StatefulSet was built, on OpenShift and elsewhere. Such a deployment never got any servers, and one that enabled a Route later stopped being reconciled; it gets its servers once the upgraded operator runs.
- **The per-server checks find the admin Secret on a cluster that names none.** Live config, TLS reload, `RestartPending`, split-brain detection and the upgrade's database gate refused a cluster without `spec.auth.adminSecret`; they now use `neo4j-admin-secret`, as the operator's other connections do. On such a cluster a rolling upgrade failed after its first server once `upgradeTimeout` passed, a dynamic `spec.config` change restarted the servers instead of being applied live, and split-brain detection could not connect.
- **The cluster controller no longer removes a `VerifiedDownload` plugin's init container** and its `plugin-auth-<name>` / `plugin-ca` volumes when it applies its StatefulSet template. Before, the servers restarted without the plugin's JAR while the plugin stayed `Ready`.
- `Neo4jDatabase` `status.state` and `status.servers` follow `SHOW DATABASES`.
- A `Neo4jShardedDatabase` stays `Ready` after a `replaceExisting` restore; mixed-case names get their shard status.
- A `VerifiedDownload` plugin's trusted CAs follow the cluster's or standalone's `spec.trustedCASecrets`.
- **A plugin takes away the settings it no longer sets.** Removing a key from a `Neo4jPlugin`'s `spec.config` now removes its `NEO4J_<setting>` variable from the servers (one rolling restart), and deleting the plugin removes everything it wrote in one restart: its variables, its procedure-allowlist tokens, its `NEO4J_PLUGINS` entry and its dependencies' (an alias such as `gds` included), and a `VerifiedDownload` init container and volumes. Before, both left the variables on the StatefulSet, so a setting Neo4j refuses kept crash-looping a server until the StatefulSet was edited by hand. Variables the cluster or standalone sets itself, and those another plugin sets, are left alone. If the key being removed keeps servers from starting, the removal does not wait for a `Ready` cluster (one `PluginSettingsFailing` Warning); see [Plugin configuration](guides/plugins.md#plugin-configuration). The GenAI example no longer sets `genai.vector.enabled` / `genai.embeddings.enabled`, which no Neo4j version accepts.
- `status.version` records the image tag the servers have rolled out, not `spec.image.tag`.
- **A cluster no longer reports `Ready` from its own template write.** Right after the operator wrote a new server template, the StatefulSet's status still described the previous one, so the cluster could read `Ready` for a moment before its servers restarted, and users, roles, databases and an upgrade acted on that. A template write the StatefulSet controller has not seen yet now counts as a rollout in flight (`Forming`, as any rollout), and `status.observedGeneration` is the generation the operator acted on, not one it read back after the fact. A server held below a `Paused` or `Failed` upgrade's partition is still not a rollout in flight.
- `kubectl neo4j diagnose` no longer says Enterprise images need a pull secret; an image-pull failure names the registry's likely answer.

### Restarts after the upgrade

- A plugin whose target lost its `NEO4J_PLUGINS` entry, a variable it sets, or a `VerifiedDownload` init container or volume under an earlier version restores it: one `PluginMissing` Warning and one rolling restart, once the target is `Ready` with every server available. Expect this on most clusters with a `Ready` `VerifiedDownload` plugin.
- A `VerifiedDownload` plugin whose target's `spec.trustedCASecrets` changed after the plugin was installed updates its `plugin-ca` volume: one rolling restart, with the plugin `Installing` until it has reached every server, then `Ready`. The same write updates the download script to keep the default CAs, so a plugin from a public URL keeps starting.
- A cluster or standalone with `strategy: Recreate` applies what it held back: template changes made since roll out, and a cluster whose `spec.image.tag` was changed starts a rolling upgrade to it, one server at a time.
- A cluster whose stored spec names authorization providers but no authentication providers, or names its providers only in `spec.config`, now gets those providers instead of `native`, and its servers restart. Such a spec exists only if the stored lists were removed after the first pass (for example by `kubectl replace`).
- New StatefulSet annotations (`neo4j.com/cluster-controller-init-containers`, `-volumes`) are written at the next template apply; writing them restarts nothing.
- Each `Neo4jPlugin` records on its target's StatefulSet the variables it sets (`neo4j.com/plugin-env-vars.<plugin name>`) at its first reconcile after the upgrade; writing it restarts nothing. It removes nothing then: a variable left behind by a `spec.config` key removed before the upgrade is not attributed to the plugin and stays — remove it from the StatefulSet by hand (`kubectl edit statefulset <cluster>-server`, one rolling restart). Keys you remove after the upgrade take their variables with them.

## Upgrading from v1.22.x

Apply the new CRDs before upgrading the operator. They add the `Disabled` phase for `Neo4jPlugin`, the `TopologyChangeDeferred` condition and the status field `status.upgradeStatus.startedTargetVersion` for clusters, and change field descriptions. Upgrading the operator restarts no server by itself, except as listed under [Restarts after the upgrade](#restarts-after-the-upgrade_1).

### Act now

- **Argo CD: re-apply `docs/gitops/argocd-health-checks.yaml`.** A stopped upgrade no longer changes the cluster's phase (see below); the new check reads `status.upgradeStatus.phase` and reports a `Failed` upgrade as Degraded and a `Paused` one as Suspended (which, unlike Degraded, does not hold a sync wave). With the old check both show Healthy. The new checks also report a refused `Neo4jDatabase` (`ValidationFailed`, or `Error` while a deletion retries) as Degraded, a `Disabled` plugin as Healthy, and a resource waiting for its cluster as Progressing.
- **Custom RBAC: add `get` on `apps/controllerrevisions`.** Helm, kustomize and the bundle carry it. Without it, a StatefulSet a storage expansion recreates after an operator restart is built from the spec, as before.
- **Plugins stored with `enabled: false` come off their target** on the first pass after the upgrade (one rolling restart), and stay as CRs in the new phase `Disabled`. A `false` you have not re-applied since v1.22.0 is still stored as `true`, so nothing changes until you re-apply it. Set `enabled: true` (or drop the field) to keep a plugin installed.
- **A role listed in a `Neo4jUser`'s `spec.roles` that exists in Neo4j but has no `Neo4jRole` is now granted.** It used to wait in `PendingDependencies` forever. Remove any such role you did not mean to grant before upgrading.
- **Scripts and alerts that read a cluster's phase for upgrades**: a cluster whose upgrade is `Paused` or `Failed` now stays `Ready`; read `status.upgradeStatus.phase` or the new `Stalled` condition. The `ServersPendingDrain` reason `ScaleDownDeferredByUpgrade` is gone. A formed cluster short of a server now reads "Neo4j cluster is serving. 2 of 3 servers available; unavailable: …" or "Neo4j still lists 3 of 3 servers available; server pods not ready: …", not "Cluster formation in progress" or "Waiting for every server pod to be updated".

### Newly refused — check your manifests first

- **A Managed plugin the target's image cannot install.** The Neo4j entrypoint exits on a `NEO4J_PLUGINS` name its image does not list: `graphql` on every supported release, `n10s` on CalVer from 2025.05, `fleet-management` before 5.26.11 and 2025.08, and any custom name. `n10s` is also refused from 5.26.22 and on 2025.01–2025.04, where the entrypoint lists it but its JAR stops the server (`NoSuchMethodError` from Jackson 2.21); it still installs on 5.26.0–5.26.21. Such a `Neo4jPlugin` is `Failed` with one `PluginUnknownToImage` Warning naming `installMode: VerifiedDownload`; servers crash-looping on a name an earlier operator wrote get it taken off (at once, with one `PluginSettingsFailing` Warning, or once the target is `Ready`). Move such plugins to `VerifiedDownload` (`source.url` + `source.checksum`); see the plugins guide for n10s on CalVer. In `Managed` mode `source.url` was never used.
- **An image change a Managed plugin cannot follow** — for example a 5.26 cluster with Managed n10s going to CalVer or to 5.26.22+ — is refused (`Failed`, one `ValidationFailed` naming the plugin; nothing rolls). Change the plugin first (`VerifiedDownload`, disabled or deleted) and leave the new tag: the first restarted server may fail once and is then repaired automatically. Do not set the tag back to put a new-release JAR on the old image.
- **`spec.auraFleetManagement.enabled` on 5.26.0–5.26.10 or 2025.01–2025.07**, whose entrypoint cannot install `fleet-management`, is `Failed` with `ValidationFailed`. Such a deployment's restarted servers were already failing on it; it is left as it is until you raise `spec.image.tag`, or remove `spec.auraFleetManagement` and take `fleet-management` out of the StatefulSet's `NEO4J_PLUGINS` by hand.

### Behaviour changes

**Upgrades**

- **A stopped upgrade leaves the cluster `Ready`.** A refused start, or an upgrade that turns `Paused` or `Failed` part-way, keeps the cluster `Ready` — it serves — with `status.message` naming the stopped upgrade, `UpgradeFailed` or `UpgradePaused` once per target and cause, and the new condition `Stalled=True` (reason `UpgradeFailed` or `UpgradePaused`; `False`, `UpgradeNotStopped`, once it goes on). Dependents keep working while an upgrade is held. Flux needs nothing: kstatus reads `Stalled=True` as failed, as it read v1.22.0's `Failed` phase. `kubectl neo4j status --problems` lists such a cluster as `Ready (upgrade Failed)`; `diagnose` reports it and exits 1. A `Failed` upgrade is retried, without a reconcile error, once every server pod is `Ready` and not within a minute of a refused retry; a server down meanwhile shows as `Degraded=True`, then phase `Degraded` after the grace period.
- **A held upgrade can go on to a new tag, or back.** A `Paused` upgrade whose restarted server cannot start (image pull failure, crash loop) continues when you set a new `spec.image.tag`, replacing that server first. The operator records the version a server's container actually started on (`status.upgradeStatus.startedTargetVersion`) and holds the tag to that rather than to the target, so a typo or an unpublished tag no longer forces you forward; a server that crash-looped on the target counts as started. An upgrade an earlier operator paused keeps the earlier rule.
- **Redirecting an upgrade never brings a server back on the version before it.** A new `spec.image.tag` set mid-walk, or on a held upgrade, after servers moved onto the earlier target, is staged at the lowest of those servers: Kubernetes moves them on to the new target at once, highest first, each once the one before is `Ready`, and the upgrade's own checks walk the rest. It waits until every other server is `Ready`. One `UpgradeRetargeted` event per new target.
- **Topology waits for an upgrade.** A change of `spec.topology.servers` made while an upgrade is in flight, or `Paused`/`Failed` part-way, waits for it to complete (condition `TopologyChangeDeferred`). A scale-down in the same apply as an image change no longer removes servers without a drain.
- **A storage expansion restarts nothing.** The StatefulSet is recreated as it was — its template with everything plugins and other controllers wrote, its replicas, strategy, partition and template hash — with the new claim templates, in the same pass that deleted it. Before, it was rebuilt from the cluster's spec alone, and every server restarted without its plugins. During a `Paused` or `Failed` upgrade it is created in two steps, so the servers below the partition keep their version if they restart; the StatefulSet carries `neo4j.com/recreate-restore-partition` and `neo4j.com/recreate-restore-revision` between them.
- **Image tags pinned to a digest** (`2026.08.1-enterprise@sha256:…`) work: labels carry the tag without the digest, and an image change that is held back splits the reference correctly.

- **On Kubernetes 1.35.0–1.35.3** (`MaxUnavailableStatefulSet` on by default) the StatefulSet controller replaces no pod while one is unavailable, so a repair of a cluster with a server that cannot start used to wait for ever. The operator now replaces such a server pod itself — at or above the partition, on the revision being left, failing to start — with one Normal `ServerPodReplaced`. The server StatefulSet carries `updateStrategy.rollingUpdate.maxUnavailable: 1`, which restarts nothing. Upgrade steps read the stored upgrade record and StatefulSet uncached where they move the partition.

**Clusters**

- **A cluster server pod is `Ready` only once its server is available in the cluster** — `system` available on it, no database failing, a primary back in its Raft groups — for at most 5 minutes after its container starts. Rolls the StatefulSet paces (plugin installs, configuration and TLS changes) now restart the next server only after the previous one rejoined. Upgrading the operator restarts nothing for this; each server gets the new check at its next restart. A pod may take longer to turn `Ready` after a restart, and a wrong admin password in the cluster's Secret holds every restart for the full 5 minutes.
- **A standalone is `Pending` while it restarts onto a new pod template**, so databases, users, roles and plugins wait for that roll.
- **The operator no longer rewrites Routes on every pass.** It keeps what it does not render — `wildcardPolicy`, a generated host, a certificate added by hand. Removing `host` from the spec keeps the Route's current host; delete the Route to have OpenShift generate a new one.
- **Events:** `ClusterFormationFailed` is gone (it was never emitted; use `ConnectivityDegraded` and `status.message`). `TopologyPlacementCalculated` names the zones and is raised when they change; `UpgradeDeferred` once per held change; `ClusterFormationStarted` once per new cluster; `ScaleDownBlocked` once per cause. A diagnostics query refused with a Neo4j transient error (`Neo.TransientError.*`, `NotALeader`) during a restart is logged at info, and a refused spec once at info, not as an ERROR on every pass.

- **The servers run the Neo4j image's own JVM arguments.** The image's conf directory is replaced by the operator's, so until now no server ran the image's `server.jvm.additional` list — its `--add-opens` and `--enable-native-access` flags and, from 2026.08, `--add-modules=jdk.incubator.vector`. They now come first in every server's `neo4j.conf`: a cluster's servers read them from the image at each start, a standalone's conf carries them from a list the operator keeps per release. Your `server.jvm.additional` (`spec.config` or `spec.env`) comes after them and wins a conflicting flag — turn one off with its `-XX:-` form; on a cluster the operator's own list comes next, and yours follows it in the same variable (see below). Existing servers pick the image's arguments up at their next restart (see below). One image line is left out: `-XX:+AlwaysPreTouch`. Pre-touch commits the whole heap at start, and at the documented 1.5Gi minimum a cluster server under write load was OOMKilled with it; add it in `spec.config` if you size memory for it.

- **A cluster's own `server.jvm.additional` adds to the operator's JVM flags instead of replacing them.** `spec.config["server.jvm.additional"]` and a plain `spec.env` `NEO4J_server_jvm_additional` now follow the operator's list (G1 with a 200 ms pause target, string deduplication, `ExitOnOutOfMemoryError`, the truststore flags), which stays; each flag is given once, and the `spec.config` line is no longer in `neo4j.conf`. To drop one of the operator's flags, negate it (`-XX:-UseStringDeduplication`). A variable taken from a Secret or ConfigMap is used as it is.
- **A scale-down issued while a server is down drains that server too.** If the operator cannot tell a removed server from a kept one (both down and never seen), it keeps the replicas held, with `ServersPendingDrain` saying why, instead of releasing them and leaving a server `Enabled/Unavailable` in `SHOW SERVERS`.
- **The `Degraded` condition follows the servers during an upgrade too:** it turns `True` while a server is unavailable — including the one the walk is restarting — and `False` afterwards; the phase is unchanged. Alerts on the `Degraded` *condition* (not the phase) now fire during upgrades. When an upgrade stops with a server down, `status.message` gives the shortfall ("serving. 2 of 3 servers available…"); the upgrade record and `Stalled` still carry the failure.

**Plugins**

- **A plugin's procedure lists join the cluster's own.** On a cluster, a plugin's `dbms.security.procedures.*` list used to replace the cluster's own `spec.config` list and Aura Fleet Management's `fleetManagement.*` entry; the servers now load the union. Security-relevant: procedures the cluster's own list allows load again. Affected clusters get it at the servers' next restart (`kubectl rollout restart statefulset/<cluster>-server` applies it now); a plain-valued procedure or extension list variable in the cluster's `spec.env` (`NEO4J_dbms_security_procedures_allowlist` / `_unrestricted` / `_denylist`, `NEO4J_dbms_security_http__auth__allowlist`, `NEO4J_server_unmanaged__extension__classes`) now moves into its `neo4j.conf` and is joined too — before, it replaced the plugins' list on the StatefulSet and procedures such as `gds.util.*` ran sandboxed. A list read from a Secret or ConfigMap cannot be joined and raises a `ValidationWarning`; give it as a value or in `spec.config`.
- **Plugins write only what they ask for.** Each plugin's write adds only its own tokens to a cluster's procedure lists, which hold exactly what the enabled plugins request: tokens left by an older `spec.security` go, a list key in a plugin's `spec.config` adds to the list instead of replacing it, and installing a plugin beside others costs one roll and raises no `PluginMissing` on the others. `NEO4J_PLUGINS` names are recorded per plugin (`neo4j.com/plugin-entries.<plugin>`, a metadata patch, no restart), so a dropped dependency or a switch away from Managed mode is cleaned up; names dropped before the upgrade stay until removed by hand. Nothing calls `dbms.setConfigValue` any more, so a plugin stuck `Failed` on a static `spec.config` key goes `Ready`.
- **A plugin hidden by another plugin's allowlist says so**: condition `ProceduresHidden` and one `PluginProceduresHidden` Warning (once more at an operator restart). The allowlist is not widened.
- **A plugin waits for its target.** One applied before its cluster or standalone is `Pending` with one `ClusterNotFound`, then installs (it used to be `Failed` until edited). One whose target's StatefulSet does not exist yet, or while a storage expansion creates it again, waits with no event and no ERROR, and a `Ready` plugin stays `Ready`.
- **Deleting a plugin whose standalone pod keeps failing on it replaces that pod**: the finalizer waits up to five minutes for it.

- **Plugins keep their image's unrestricted defaults.** Neo4j's image gives each installed plugin an unrestricted entry (`apoc.*`, `gds.*`, `genai.*` — `genai.*,ai.*` from 2026.10 — `bloom.*`) only when nothing else sets `dbms.security.procedures.unrestricted`; Aura Fleet Management, another plugin (GDS, Bloom) or a plugin's `spec.security` used to wipe them, leaving APOC procedures that need unrestricted access restricted. The servers now keep them, unless the deployment sets the list itself — in `spec.config`, or as `NEO4J_dbms_security_procedures_unrestricted` in `spec.env` — where, as in Neo4j, the explicit setting wins. Clusters take it at each server's next start; see the restarts below for standalones.
- **A standalone's `spec.env` list variable joins its `neo4j.conf`** (a plain-valued `NEO4J_dbms_security_procedures_*` and the other shared lists) instead of replacing its `spec.config` and plugin entries; one restart. A variable from a Secret or ConfigMap is unchanged.
- **Duplicate plugins:** an enabled `Neo4jPlugin` wins over a disabled one with the same `clusterRef` and `spec.name` (the disabled one is `Disabled` and names it); handing a plugin over between duplicates restarts nothing, and a held-back duplicate takes over as soon as the other is disabled or deleted. A `Disabled` plugin clears `status.installedVersion` and `status.installationTime`.
- `PluginProceduresHidden` is judged against what the servers will start with: no transient Warning while the operator upgrade rewrites ConfigMaps, and a newly applied hiding list is reported before the roll.

**Aura Fleet Management**

- **It no longer narrows the allowlist.** Fleet Management (and a fleet-management `Neo4jPlugin`) used to set the servers' allowlist to `fleetManagement.*` alone — through the operator's configuration and through the Neo4j image's own plugin defaults — which hid APOC and every plugin without `allowedProcedures`. It now joins the allowlist only beside one something else sets; otherwise the servers keep Neo4j's default `*`.
- **It is built into Neo4j from 2026.03**, so the operator no longer adds `fleet-management` to `NEO4J_PLUGINS` there. Deployments that already list it keep it and restart nothing; their servers log a harmless `ERROR: could not query null` at start (remove the entry by hand at a restart of your choosing). A `Neo4jPlugin` named fleet-management on 2026.03+ is `Ready` and installs nothing. Before 2026.03, a new deployment with `spec.auraFleetManagement.enabled` starts with `fleet-management` and does not restart for it.
- `AuraFleetManagementPluginPatchFailed` is announced and logged once per cause.

- `AuraFleetManagementPluginPatchFailed` says "Failed to add fleet-management to NEO4J_PLUGINS" only when the StatefulSet write fails; a failed read or check says "Could not check the server StatefulSet's NEO4J_PLUGINS for fleet-management". Alerts matching the old text need updating.

**Users, roles, bindings and auth rules**

- **A target restart is not a transition.** A `Ready` user, role, binding or auth rule stays `Ready` while its cluster or standalone is down, or reads `Ready` but refuses Bolt, with `ClusterNotReady=True` (reason `ClusterNotReady` or `ClusterUnreachable`), logged at info; `UserReady`, `RoleReady` and `BindingCreated` are not re-announced. One that failed keeps its failure, and the same failure is not announced again. `ConnectionFailed` is raised only after 5 minutes of a target that refuses the connection. An auth rule its `Ready` target refuses (version, OIDC) reads `ClusterNotReady=False`.
- **Learn mode resolves a statement that names a database in capitals** on the first reconcile after the upgrade, without a grant. Out-of-band rows it absorbed into `status.unattributedPrivileges` stay there until removed by hand.
- **Password events:** `PasswordRotated` once per password Secret version; a user whose password Neo4j already holds (the admin `neo4j` user) emits only "password already matches spec".

- A `Neo4jUser` applied before both its cluster and its password Secret reports `ClusterNotFound` first and `SecretPending` after.

- **A Neo4j error that asks to be retried** (a deadlock in a bulk change, a leader switch: `Neo.TransientError.*`, `NotALeader`) on a user's, role's, binding's or auth rule's statements, or in any deletion's cleanup, is retried every few seconds at info — no `Failed` phase, no `…SyncFailed` / `…DeletionFailed` Warning, no ERROR — and reported only if it lasts more than 5 minutes.

- A `Neo4jUser` for the operator's own login (its admin Secret's user, or `neo4j`) whose `spec.roles` leave out `admin` raises a `ValidationWarning`: the operator would revoke `admin` from itself. Behaviour is unchanged — add `admin`.

- A new `Neo4jRole` whose privileges name a database that does not exist yet reads `Ready` with reason `PrivilegesWaitForDatabase`, and announces `RoleReady` once they are applied, with their count — no longer "(0 privileges in sync)" first. `PrivilegesSynced=Unknown` now names an ambiguous statement, the alias it grants through, that alias's target and what clears it, instead of "cannot attribute 0 privilege row(s)".
- A user, role, binding, auth rule, database, composite, alias or replica whose cluster or standalone is deleted reads `Pending` / `ClusterNotFound` at once, but its `ClusterNotFound` Warning comes only if it is still there a minute later, so tearing a deployment and its dependents down together raises none. One applied before its deployment still warns at once.

**Deletion**

- **Deleting during a restart no longer leaves access behind.** Deleting a `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jDatabase`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias` or `Neo4jReplicaDatabase` while its target is down, refusing connections or being replaced keeps the CR `Terminating` (status `deletion waits to remove …`, one Warning) until the target answers, for at most 5 minutes; it is then released with a Warning naming what is left in Neo4j and the statement to run. A failed `DROP` or `REVOKE` sets the object to `Failed`, announced once, and is retried for the same 5 minutes. A target CR that is gone still releases at once; `deletionPolicy: Retain` is unchanged.
- **Aura deletion needs credentials only to call Aura.** A terminating `AuraInstance`, `AuraCustomerManagedKey`, `AuraIPFilter`, `AuraInvite`, `AuraOrganizationMember` or `AuraProjectMember` with `deletionPolicy: Orphan` is removed even after its Secret or provider config is gone. With `Delete` and no credentials it waits 5 minutes, then is removed with a Warning (`…Orphaned`, or the new `AuraMemberOrphaned`); delete what was left in the Aura console. A `Delete` CR already stuck `Terminating` for longer is released on the upgraded operator's first pass, and the cloud resource stays.
- **A finalizer release never logs an ERROR**: on an object already gone it is success, and it no longer fails with a Conflict on a stale copy; another controller's finalizer is kept.

- **A deleted target's retention policy decides its dependents.** When the cluster or standalone itself is being deleted with `spec.storage.retentionPolicy: Delete` (the default), its users, roles, bindings, auth rules, databases, composites, aliases and replicas are released at once, without a Warning — a namespace teardown no longer waits on them. With `Retain`, a dependent whose target CR is gone while its volumes remain waits up to 5 minutes for a deployment re-created from them, then is released with a Warning naming what the volumes still hold; before, it was released silently and a re-created deployment brought those users and grants back.
- **Aura deletions Aura refuses are bounded.** A `deletionPolicy: Delete` that Aura refuses (not a 409, 429, 5xx or network error) waits with phase `Error`, reason `DeletionRefused`, then after 5 minutes is removed with the kind's `…Orphaned` Warning; a customer-managed key still in use is bounded the same way (`AuraCustomerManagedKeyOrphaned`). An `AuraDatabase` whose `AuraInstance` CR is already gone is removed at once with a Warning `AuraDatabaseOrphaned` (delete databases before their instance); one whose credentials are missing waits up to 5 minutes.
- Adding a finalizer no longer logs a Conflict or `Reconciler error` when the operator's copy is out of date.

**Databases**

- **Database kinds stay `Ready` through a target restart.** A `Neo4jDatabase`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`, `Neo4jShardedDatabase` (`Ready`) or `Neo4jReplicaDatabase` (`Replicating`) keeps its phase while its target restarts, rolls out or refuses Bolt, with `ClusterNotReady=True`; its Ready event is not repeated. A target that does not answer for 5 minutes makes it `Failed` with `ConnectionFailed` (`ClientCreationFailed` for a sharded database). Each `Neo4jDatabase` gets `ClusterNotReady=False` on its first pass after the upgrade: one status write, no event. `Neo4jDatabase` never reports phase `Unknown`.
- **A missing cluster is a wait on every kind.** A `Neo4jDatabase`, `Neo4jShardedDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding` or `Neo4jAuthRule` applied before its cluster or standalone waits in `Pending` with one `ClusterNotFound` Warning and goes on once it exists, instead of going `ValidationFailed`/`Failed` with a `ValidationFailed` Warning; every spec error is still reported at once. Composites, aliases and replicas raise that one Warning too.

- "Database validation failed" and the database kinds' other refusals and waits are logged once at info per cause, not as an ERROR on every requeue.

- **Losing a cluster's majority is a wait for its databases.** While a cluster that lost its majority still reads `Ready` and answers with a transient error (`Neo.TransientError.*`), the `Neo4jDatabase`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`, `Neo4jReplicaDatabase` and `Neo4jShardedDatabase` keep their phase with `ClusterNotReady=True` (reason `ClusterUnreachable`), log at info and raise no `CreationFailed` / `AliasFailed` / `CompositeDatabaseFailed` / `ReplicaFailed` Warning; only a wait over 5 minutes makes them `Failed` with one `ConnectionFailed` (`ClientCreationFailed` for a sharded database). Their Ready event is not repeated on recovery, and `ClusterNotReady` turns `False` once their statements answer.

- **Two `Neo4jDatabase`, `Neo4jShardedDatabase`, `Neo4jCompositeDatabase` or `Neo4jDatabaseAlias` resources for one name on one target, in any case, no longer share one database.** The older owns it; the newer is `Failed` with one `DuplicateName` Warning naming it, and takes over by itself once the older goes. After the upgrade the newer of an existing pair goes `Failed` — find them with `kubectl get neo4jdatabase,neo4jshardeddatabase,neo4jcompositedatabase,neo4jdatabasealias -A` and delete the newer one: deleting a resource whose name another still gives drops nothing (`DuplicateNameKept`).
- A replica's `LAG` column reads `0` once it has caught up (it was blank), and a promotion that took no lag records `observedLagTxIds: 0`; blank now means not read yet.

**Backups and restores**

- **Sharded backups name only their shards**: the Job runs `neo4j-admin database backup "<name>-g*" "<name>-p*"` (name lower-cased) instead of `"<name>*"`, so a database that only begins with the name (a restored copy) is no longer taken along or blocks the backup, and `shardArtifacts[].shardName` is lower-cased. A backup of a capitalised `spec.name` now records each shard's file; runs taken earlier recorded none and cannot seed — take a new backup. A one-time backup that is `Completed` stays so; `status.lastRunTime` is set when a run starts and no longer rewritten on every pass.
- **Restore attempts:** restore Jobs carry `neo4j.com/restore-attempt`. Re-running a restore by editing its spec deletes the previous attempt's Job first — a completed one still within its TTL too — and waits in `Pending` until it is gone; a retry never adopts a terminating Job.
- **Restore events:** an offline restore emits one `RestoreStarted` ("Restore job … started: restoring offline with neo4j-admin, …"). An all-databases restore emits one `RestoreStarted` naming the databases, from `neo4j-restore-controller`, one `RestoreDatabaseFailed` Warning per database it cannot restore and one `RestoreSeedProgress` summary ("N of M database(s) restored, F failed; seeding "db": …, unchanged for …") at the first seed poll and every 10 minutes after, from `neo4j-restore-progress` — no longer one event per database change, so alerts matching `database "x": …` need updating, and ends with one `RestoreCompleted` or `RestoreFailed`.
- **A PVC `backupPath` is relative to the PVC's root**; one starting with `/backup/` raises a `ValidationWarning`.

- **A backup or restore whose target does not exist yet** raises one `ClusterNotFound` Warning; the backup stays in phase `Waiting`, the restore in `Pending`. A `Neo4jRestore` that is `Completed` or `Failed` at its generation stays so when its target is deleted — before, it went `Pending` and could run again when a target of the same name was created.

- **An all-databases restore waits through a target that is briefly unavailable** (for example while `neo4j` is recreated with `replaceExisting`): it keeps its phase and results with `ClusterNotReady=True` and resumes, instead of logging an ERROR, dropping back to `Pending` and announcing `RestoreStarted` again; a wait over 5 minutes raises one `ConnectionFailed` and it keeps waiting.

- **A restore's `Ready` condition uses restore reasons:** `RestoreCompleted` (was `BackupSucceeded`), `RestoreFailed` (was `ReconciliationFailed`), `RestoreRunning` (was `Pending`). Alerts or scripts matching the old reasons on `Neo4jRestore` need updating.
- **A refused restore spec is no longer an operator ERROR** (validation, an unsupported version, a database that exists without `replaceExisting`, an offline restore into a running standalone without `stopCluster`): `Failed` with its reason, logged once at info.
- **A failed backup or restore Job says why:** `status.message` and the `BackupFailed` / `RestoreFailed` event give the Job's reason, the container's exit code and its last log lines, instead of "Backup job failed" / "Restore job failed"; every failed backup run in `status.history` carries the same text in `error`. Backup and restore Jobs set `terminationMessagePolicy: FallbackToLogsOnError` (a scheduled backup's CronJob template takes it on the first pass; nothing runs because of it).
- An online standalone restore with `stopCluster: true` emits one `RestoreStarted`, with the "spec.stopCluster is ignored" notice inside it.

**Aura**

- **Status and events.** `AuraInstance`, `AuraCustomerManagedKey` and `AuraSnapshot` show phase `Error` while they fail (`AuraSnapshotFailed` once; a finished snapshot is not polled again). Aura status messages no longer contain `request-id=…` (the Warning event still does), so a repeated refusal is one write and one Warning. `status.lastSyncedTime` is refreshed at least every 15 minutes, not on every poll. `…Ready` and `AuraCredentialsValidated` fire on transitions only. A 429, 5xx or network failure on an Aura create or backup no longer sets `Error` or raises a Warning. Alerts that relied on these recurring, or on `lastSyncedTime` being younger than 15 minutes, need adjusting.
- **An `AuraProviderConfig` revalidates every 10 minutes** instead of every 30 seconds.

**`kubectl neo4j`**

- `validate` applies each CRD's defaults before judging a manifest, for every kind (a manifest it passed may now report what the operator would refuse after apply, such as a standalone `tls: {}` without `issuerRef`). With `--connect` it reports a missing deployment as pending, not as an error, may warn that `spec.storage.className` has no effect on existing volumes, and compares versions only with the operator that is running (or prints `version skew not checked: …`).
- `preflight` prints node capacity in readable units; `diagnose` marks only Warnings that describe the current state and counts the earlier ones in one line; `status` and `explain` know `PendingDependencies`, `Disabled` and a stopped upgrade.

**New advisory `ValidationWarning`s** (no restart): a standalone's `spec.upgradeStrategy` timeouts set to anything but their default; `spec.storage.className` naming a class the existing data volumes do not use; `spec.service.route.tls.secretName` (and the MCP Route's), which never had an effect; a PVC restore `backupPath` under `/backup/`; `storage.path` on a PVC backup or restore storage, which nothing reads.

### Fixes

- **Restore from a PVC directory** finds the source database's artifacts whatever case `spec.database` or `sourceDatabase` uses (neo4j-admin writes them lower-cased), and never takes another database whose name begins with it plus a dash (`orders-eu` for `orders`).
- **`Neo4jShardedDatabase` with capitals in `spec.name`** sends seed shard keys lower-cased, as Neo4j requires; one that failed with "entry is missing for '<name>-g000'" seeds on its next retry.
- **`chainFromBackup`** accepts a chain whose two backups spell the database differently.
- **A `VerifiedDownload` plugin on a target using the legacy `spec.auth.trustStore`** trusts that CA.
- **A `Neo4jCompositeDatabase` whose DROP Neo4j refuses** releases its finalizer after 5 minutes instead of staying `Terminating`.

### Restarts after the upgrade

- A plugin stored with `enabled: false` is removed: one rolling restart.
- A Managed plugin whose name the target's image does not list is taken off its servers: one rolling restart (servers that were crash-looping on it recover).
- A `VerifiedDownload` plugin on a target that uses the legacy `spec.auth.trustStore` updates its `plugin-ca` volume: one rolling restart, no `PluginMissing`.
- A cluster or standalone with Aura Fleet Management whose allowlist was fleet's entry alone (no allowlist of its own): one rolling restart, after which APOC and other plugins load again. A cluster with Fleet Management and a plugin's allowlist: one rolling restart, after which the servers list both.
- `neo4j.com/plugin-entries.*` and the other new records are metadata patches; writing them restarts nothing.
- The new readiness check and the plugin-list union reach each server at its next restart; upgrading the operator restarts nothing for them.
- The Neo4j image's own JVM arguments reach each cluster server and each standalone at its next restart; upgrading the operator restarts nothing for them.
- A standalone whose `neo4j.conf` sets the unrestricted list and runs Managed plugins (Aura Fleet Management beside a plugin, GDS or Bloom beside APOC, a plugin with `spec.security`): one restart, after which it keeps the plugins' image entries. Clusters take the same change at each server's next start and restart nothing for it.
- A cluster whose `spec.config` sets `server.jvm.additional`, or whose `spec.env` sets a plain `NEO4J_server_jvm_additional`: one rolling restart, after which the line follows the operator's list in one variable.
- A cluster or standalone whose `spec.env` sets a shared-list variable with a plain value: one rolling restart, moving it into `neo4j.conf` (a cluster hands the StatefulSet variable to its plugins in the same write).

## Upgrading from v1.23.x

This release changes no CRD and needs no new permissions. Upgrading the operator restarts no server, except for the deployments listed under "Restarts after the upgrade" below.

### Behaviour changes

**Clusters**

- **`ClusterReady` is announced only when a cluster enters `Ready` from another phase.** That means its first formation, a scale-up, or a return from `Degraded`, `Forming` or `Failed`. A cluster that stays `Ready` no longer raises it:
  - not when an upgrade completes, retargeted or not: read `UpgradeCompleted`;
  - not when a server comes back within the grace period: read the `Degraded` condition;
  - not after a spec edit.

  Automation that counted `ClusterReady` for any of those should switch.
- **A completed scale-down drain says so.** `ServersPendingDrain` ends `False` with reason `NoServersPendingDrain` and message "Scale-down drain complete", and keeps that message until the next scale-down. "Scale-down not in progress" now means only a scale-down set back before its drain ended. A cluster keeps its current message until its next scale-down.
- **A scale-down you set back gives back the servers its drain cordoned.** If you raise `spec.topology.servers` again before the drain begins deallocating (in practice within about a second of lowering it), the operator runs `ENABLE SERVER` for each server the drain cordoned, and a `ScaleDownSetBack` event names them. Up to v1.23.x those servers stayed `Cordoned`, and the cluster stayed `Forming` until a later scale-down dropped them. The drain now records the servers it cordons in the cluster annotation `neo4j.com/scale-down-cordoned-servers`, so a server you cordoned yourself stays cordoned. A set-back only part of the way (5 → 3, then 4) gives back the server the cluster keeps again, where it used to drain it as well.
- **A set-back that comes after the drain began deallocating is refused.** Neo4j cannot enable a deallocating server again. The replicas stay held, and `ServersPendingDrain` reads `True` with reason `ScaleDownBlocked` and one `ScaleDownBlocked` event, naming the servers and the value to set `spec.topology.servers` to so the drain can finish. Lower it, wait for "Scale-down drain complete" and until the removed servers' pods and volumes are gone, then raise it again to add new servers. Raised within seconds of the drain completing, a removed server can come back on its old volume as `Dropped`, and the cluster stays `Forming` until `servers` is lowered again (#619, also in v1.23.x). Up to v1.23.x the operator released the hold and left those servers deallocating in pods it kept.
- **A cordoned server counts as a cluster member.** A cluster with a server you cordoned (`dbms.cluster.cordonServer`) is `Ready`. Up to v1.23.x it read `Forming` with "Cluster formation verification incomplete", and every resource that depends on it waited. `ServersHealthy` still names the server (`state=Cordoned`). A cordoned server that is down now shows in the `Degraded` condition like any other server that is down. A rolling upgrade goes on once a restarted cordoned server is back, where it waited for the server to be `Enabled`.
- **`Degraded` is `True` only while a server is short.** For about 10 seconds after a completed scale-down drain it read `True` (`ServersUnavailable`) with "3 of 3 servers available". When every server the spec asks for is available and each of its pods is ready, the operator now leaves the phase and the conditions as they are. Alerts on `Degraded=True` no longer fire after each drain.

**Aura Fleet Management**

- **Turning it off takes `fleet-management` out of `NEO4J_PLUGINS`**, in the same rolling restart as the `neo4j.conf` change of the disable. Before, the plugin stayed on the servers. The operator removes only an entry it added. It leaves an entry that a `Neo4jPlugin` installs, and a `NEO4J_PLUGINS` your own `spec.env` sets. See [Turning it off](aura_fleet_management.md#turning-it-off).
- **The operator records the entry it adds** in the server StatefulSet's annotation `neo4j.com/aura-fleet-management-entries`. On a deployment with Fleet Management on, the first pass after the upgrade records the existing entry. That is a metadata patch and restarts nothing. On 2026.03+, the entry that v1.23.0 kept is recorded too, and goes when Fleet Management is turned off. A `NEO4J_PLUGINS` that your own `spec.env` sets carries no record: when `spec.env` sets the variable again while Fleet Management is on, the annotation goes in the restart that writes your list.
- **A deployment where Fleet Management was turned off under an earlier operator** still lists `fleet-management` in `NEO4J_PLUGINS`. The operator cannot tell that entry from one you added, so it leaves it. Remove it by hand with `kubectl edit statefulset <cluster>-server` (or `<standalone>`), which is one restart. On a standalone on 5.26.12+ or 2025.08–2026.02, the leftover name also lets the image narrow the allowlist to `fleetManagement.*`, so removing it brings back the procedures that the allowlist hid.
- **A `NEO4J_PLUGINS` that your own `spec.env` sets now carries `fleet-management` itself.** Up to v1.23.x, a cluster with a JSON list there (`'["apoc"]'`) and Fleet Management on restarted its servers without end. The Fleet Management merge added the name, and the cluster put your value back. The cluster now appends `fleet-management` to your list, keeping your names and their order, and the merge never writes that variable. A list that already names the plugin is left as it is. On such a deployment, turning Fleet Management on or off is one rolling restart, and turning it off gives back the list you wrote. On 2026.03 and later nothing is appended. A `Neo4jPlugin` on such a deployment is still refused. See [Your own `NEO4J_PLUGINS` in `spec.env`](aura_fleet_management.md#your-own-neo4j_plugins-in-specenv).
- **A `NEO4J_PLUGINS` from a Secret or ConfigMap, or one that is not a JSON list,** is left as it is, with one `ValidationWarning` saying that Aura Fleet Management needs `fleet-management` in that list and the operator cannot add it. Before, the write of the StatefulSet failed: a new cluster or standalone was never created, and an existing standalone applied no template change. List the plugin there yourself, or set the list as a value.
- **Turning it on for a running cluster is one new StatefulSet revision.** `fleet-management` now joins `NEO4J_PLUGINS` in the write that rolls the servers for the `neo4j.conf` change of the enable (the ConfigMap's restart stamp). Up to v1.23.x the cluster wrote two revisions in the same second, the stamp and then the name, and a slow StatefulSet controller could restart a pod twice. A standalone's enable was already one write. It now stays one when the operator reads the old `neo4j.conf` from its cache.
- **Plugin-only mode is logged once.** "Aura Fleet Management enabled but neither tokenSecretRef nor provision is configured; plugin installed, registration deferred" is logged at info when the mode starts, and at debug level on later reconciles. It is logged at info again if the mode comes back after you add a token source, turn Fleet Management off, or recreate the deployment. Up to v1.23.x it was logged at info on every reconcile.

**Plugins**

- **A refused `Neo4jPlugin` is logged once at info, never as an ERROR.** A Managed plugin whose cluster's own `spec.env` `NEO4J_PLUGINS` lacks its name used to log `ERROR "Failed to install plugin"`, with a stack trace, on every retry (about every 30 seconds). It is still `Failed`, with one `PluginInstallFailed` event. Its message now reads "Plugin installation refused: NEO4J_PLUGINS on StatefulSet … comes from Neo4jEnterpriseCluster … itself …". It is logged at info once, and at debug level on each look after. The plugin is refused before it waits for the cluster or connects to it, so it is `Failed` even while the cluster is forming, not `Waiting`. A `NEO4J_PLUGINS` on the target that is not a JSON list is refused the same way. Before, it logged two ERRORs per look.
- **The other refusals now log one info line** from the status write that records them: an `Invalid` spec, a newer duplicate, and a name the target's image cannot install. A real failure, such as the API server refusing the StatefulSet write, is still logged as an ERROR. Log alerts on `Failed to install plugin` for a spec conflict should watch the `PluginInstallFailed` event or the plugin's `Failed` phase instead.

**Restores**

- **A finished `Neo4jRestore` stays quiet.** One that is `Completed` or `Failed` at its generation no longer logs "Restore already completed" or "Restore previously failed; not retrying…" at info whenever the operator's cache resyncs (every 30 seconds in development mode). Those lines are now at debug level. The outcome is in the restore's status, its `Ready` condition and its `RestoreCompleted` or `RestoreFailed` event, and a refusal is still logged once at info. Log-based alerts on those two lines should watch the event or the condition instead.
- **A `Neo4jRestore` without `spec.instanceRef` is refused once**, as `Failed` with "spec.instanceRef is required" and one info line "Restore refused: spec.instanceRef is required". It used to log "Invalid Neo4jRestore spec" and write its status on every pass. It now carries the restore finalizer; deleting it is unchanged.

### Restarts after the upgrade

- Recording an existing Fleet Management entry is a metadata patch and restarts nothing.
- A cluster caught in the `spec.env` `NEO4J_PLUGINS` restart loop (Fleet Management on, before 2026.03) rolls at most once more, to your list with `fleet-management`, and then stops. If its last write was the merge's, it does not roll at all.
- A standalone with Fleet Management on and a `NEO4J_PLUGINS` from a Secret, or one that is not a JSON list, rolls once if its template changed while its writes were failing. A cluster in that shape restarts nothing; one that was never created is created.

## Upgrading from v1.24.x

### Behaviour changes

**Deleting a namespace**

- **Deleting a namespace no longer logs ERRORs.** Kubernetes deletes what the namespace holds in no fixed order. When it deleted a deployment's Services or ConfigMap before the deployment itself, the operator created them again, the API server refused ("… is being terminated"), and each pass logged an ERROR and a `Reconciler error`. A standalone also went `Failed` with a `ReconcileFailed` event for a moment. The operator now treats that refusal as the namespace going away, for every kind: it logs one debug line, writes no status and returns no error. The deletion goes on through the finalizers as before. A cluster whose server pods are being deleted no longer logs ERROR "Failed to perform split-brain detection". Log alerts on those lines during a teardown stop firing. Any other refusal is still an ERROR.

**Plugins**

- **Removing your own `NEO4J_PLUGINS` from `spec.env` is one rolling restart.** If a Managed `Neo4jPlugin` was installed and its name was in that list, the write that takes the variable out of `spec.env` now keeps the plugin's name (and its dependencies'), and records it as the plugin's (`neo4j.com/plugin-entries.<plugin>`). Names that no installed plugin adds go, as you asked. With Aura Fleet Management on, `fleet-management` stays as well. Up to v1.24.x that write dropped the plugin's name, and the plugin raised `PluginMissing` and wrote it back: two rolls. This applies to clusters and standalones.

**`kubectl neo4j`**

- **`export` takes its flags before the target.** `kubectl neo4j export --context dr -n prod replica-database dr-copy …` now works, as `validate` and `preflight` always did. It used to exit 2 with `unknown export target "--context"`.
- **`diagnose` calls a crash-loop before Kubernetes does.** A container restarted at least twice whose latest run has just died with an error (kubectl shows `Error`, before `CrashLoopBackOff`) is reported as crash-looping. The command exits 1 and points at the `--previous` log. It used to give a ⚠ for the previous run and exit 0. A single restart, or a container running again since, is still a ⚠.

### Restarts after the upgrade

- None.

## Upgrading between future releases

When a newer version ships:

1. **Refresh the CRDs first.** Helm does not upgrade CRDs automatically, so a new
   field or validation won't take effect until the CRDs are applied. Apply the
   bundle for the version you are upgrading to (replace the tag):

   ```bash
   kubectl apply --server-side -f \
     https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v1.24.0/neo4j-kubernetes-operator.yaml
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
