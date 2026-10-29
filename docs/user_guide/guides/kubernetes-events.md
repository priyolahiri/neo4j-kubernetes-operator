# Kubernetes Events Reference

The Neo4j operator emits structured Kubernetes events for all material state
transitions. Unlike pod logs, events persist in the cluster (default 1 hour TTL),
are queryable by reason, and are consumed by monitoring pipelines, GitOps tools,
and alerting systems.

## Viewing Events

```bash
# All events for a specific cluster
kubectl get events --field-selector involvedObject.name=<cluster-name>

# Filter by event reason
kubectl get events --field-selector reason=ClusterFormationStarted

# Watch events in real time
kubectl get events -w

# All Neo4j operator events across namespaces
kubectl get events -A --field-selector involvedObject.apiVersion=neo4j.neo4j.com/v1beta1
```

## Event Reasons Reference

**How often a Warning repeats.** The operator retries a resource on a timer, on
error backoff and whenever something it watches changes, so a condition that lasts
is met on many passes. It is announced once:

- *Once per failure* (or *once per wait*): the Warning is emitted when the
  condition starts or its message changes — read from what the operator recorded
  in the resource's status — and not on the retries in between, nor after an
  operator restart. The current state is always in `status.message` and the
  `Ready` condition.
- *Advisory*: a Warning that stays true while the resource otherwise works is
  emitted when it starts to apply and again every 30 minutes while it still does
  (inside the default one-hour event TTL), and forgotten when it stops applying.

### Cluster Lifecycle

| Reason | Type | Description |
|---|---|---|
| `ClusterFormationStarted` | Normal | Cluster formation has begun (first formation only — a formed cluster that rolls or loses servers does not raise it). Once per cluster: it is announced by the status write that enters `Forming`, so a pass that reads the cluster from before that write does not raise it again. There is no `ClusterFormationFailed`: formation waits rather than fails, and a cluster that cannot be reached for long raises `ConnectivityDegraded` |
| `ClusterReady` | Normal | Cluster has reached the `Ready` phase from another phase: first formation, back from `Degraded`, `Forming` or `Failed`, or an upgrade's completion on a cluster it repaired. Announced by the status write that enters `Ready`, once. A cluster that stays `Ready` announces nothing: not over an upgrade (`UpgradeCompleted` says it finished), not when a server comes back within the grace period (the `Degraded` condition turns `False`), not after a spec edit |
| `ValidationFailed` | Warning | The spec failed validation; the message names what to fix and is also in `status.message`. Emitted when the failure starts or its message changes, not on every retry while the spec stays invalid (a cluster or replica also announces it again after an edit that leaves the spec invalid). Also emitted by `Neo4jEnterpriseStandalone`, `Neo4jDatabase`, `Neo4jShardedDatabase`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`, `Neo4jReplicaDatabase`, `Neo4jPlugin`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding` and `Neo4jAuthRule` |
| `TopologyWarning` | Warning | Topology validation produced warnings |
| `ValidationWarning` | Warning | A spec field the schema accepts but nothing reads is set (`spec.<path> is accepted but has no effect today: <what to do instead>`), or `spec.storage.className` names a StorageClass the existing data volumes do not use (a volume keeps the class it was created with; the message names each volume and its class), or Aura Fleet Management is on and `spec.env` sets `NEO4J_PLUGINS` from a Secret or ConfigMap, or to a value that is not a JSON list, which the operator cannot add `fleet-management` to (`spec.env[<i>] (NEO4J_PLUGINS) … the operator cannot add it`). Advisory: the resource is still reconciled. Also emitted by `Neo4jEnterpriseStandalone` (there also for `spec.upgradeStrategy` `upgradeTimeout`, `healthCheckTimeout` or `stabilizationTimeout` other than their defaults), `Neo4jBackup`, `Neo4jRestore` (there also for a PVC `backupPath` that starts with `/backup/`: the path is relative to the PVC's root and the operator adds that prefix itself), `Neo4jPlugin`, `Neo4jDatabase` and `AuraInstance` |
| `TopologyPlacementCalculated` | Normal | Topology placement of a zoned cluster calculated; names the zones. Announced when first calculated and when the zones change, not on every pass (an operator restart announces it once more) |
| `TopologyPlacementFailed` | Warning | Topology placement constraint calculation failed; the cluster is `Failed` and retried with backoff. Once per failure |
| `TopologyZoneDiscoveryDegraded` | Warning | Availability-zone auto-discovery is unavailable (the operator cannot list cluster-scoped nodes, as in namespace-scoped installs); best-effort zone spread is applied via the topology key — set `spec.topology.availabilityZones` to enumerate zones explicitly. Advisory |
| `PropertyShardingValidationFailed` | Warning | Property sharding configuration validation failed; the cluster is `Failed`. Once per failure |
| `ServerRoleValidationFailed` | Warning | Server role hint validation failed, one event per invalid hint; the cluster is `Failed`. Once per failure |
| `RouteAPINotFound` | Warning | OpenShift Route API not available in cluster, for the Route or the MCP Route. Advisory. Also emitted by `Neo4jEnterpriseStandalone` |
| `MCPApocMissing` | Warning | MCP server requires APOC plugin which is not installed. Advisory. Also emitted by `Neo4jEnterpriseStandalone` |
| `ClusterDegraded` | Warning | A formed cluster has been short of one or more servers for longer than the grace period (`--server-unavailable-grace`, default 5m) and its phase is now `Degraded`. It is still serving with a majority; dependents keep reconciling. Emitted once per transition |
| `ClusterQuorumLost` | Warning | A formed cluster has half or fewer of its servers available. Its phase is `Forming` and dependents pause until a majority is back. Emitted once per transition (an operator restart during the outage may repeat it once) |
| `ConnectivityDegraded` | Warning | The operator has failed to reach the cluster's Bolt endpoint for a sustained streak: at least 10 consecutive reconciles spanning at least 5 minutes, so a cluster that is still forming does not raise it. Emitted once per streak, with the last error and any pod issues |
| `ReconcileFailed` | Warning | A reconcile pass of a `Neo4jEnterpriseStandalone` or `Neo4jShardedDatabase` failed; the resource is `Failed` and retried with backoff. Once per failure. A statement Neo4j asks to be retried (a `Neo.TransientError` — a cluster that lost the majority of its servers still reads `Ready` and answers one) is not a failure: the sharded database waits quietly (condition `ClusterNotReady`, reason `ClusterUnreachable`; one that was `Ready` stays `Ready`, and its Ready event is not repeated when the cluster serves again) and only a wait that lasts more than 5 minutes is reported, as `ClientCreationFailed` |
| `ConfigAppliedLive` | Normal | A `spec.config` change touched only settings Neo4j can change at runtime, so the operator applied it to every running server (`dbms.setConfigValue`) and wrote it to `neo4j.conf`; nothing restarted. Names the settings. Also emitted by `Neo4jEnterpriseStandalone` |
| `TLSCertificateReloaded` | Normal | cert-manager renewed the TLS certificate and the operator reloaded it on the named servers (`dbms.security.reloadTLS()`, CalVer 2025.03+); no restart. Also emitted by `Neo4jEnterpriseStandalone` |
| `ServerPodReplaced` | Normal | The operator deleted a server pod that cannot start (an image it cannot pull, a container that keeps exiting, an init container that failed) and that the StatefulSet's update is to replace — after a plugin's repair, an upgrade's new tag or a corrected spec — so that the StatefulSet creates it again from the new template. On Kubernetes 1.35.0–1.35.3 (`MaxUnavailableStatefulSet` on) the StatefulSet does not replace a pod while it is unavailable; elsewhere it usually gets there first and the event does not appear. Once per pod deleted; the pod was down, so nothing it served is lost |
| `TLSCertificateNeedsRestart` | Warning | A renewed TLS certificate could not be reloaded on the named servers: TLS reload is not enabled on them yet (they get it at their next restart), or they still presented the old certificate ten minutes after the new one was issued. They serve the previous certificate until they restart. Also emitted by `Neo4jEnterpriseStandalone` |
| `ConfigNeedsRestart` | Normal | A `neo4j.conf` change could not be applied at runtime, so the servers restart as before. The message names the settings and the reason: a setting Neo4j reads only at startup, one also set through an environment variable, or the deployment not `Ready` with every server up. Also emitted by `Neo4jEnterpriseStandalone` |

### Scaling

| Reason | Type | Description |
|---|---|---|
| `ScaledWithoutRestart` | Normal | A scale-up added only servers that join as `system` secondaries, so the running servers were not restarted; they read the longer discovery list at their next restart. It names only the servers the StatefulSet does not run yet: a scale-down set back while the drain held the replicas adds none and raises none. A scale-up that adds a system primary (with the default, from 2 servers to 3+) restarts them instead and raises no such event |
| `ScaleUpDeferred` | Normal | A raise of `spec.topology.servers` waits until the pods and volumes of the servers a scale-down removed are gone, so no new server starts on a removed server's volume as the server Neo4j dropped (`TopologyChangeDeferred`, reason `WaitsForRemovedServers`); the servers are added once they are gone |
| `ScaleUpBlocked` | Warning | What is left of the removed servers has not gone in 2 minutes (a pod terminating on a node that is gone, a volume another finalizer keeps): the message names it and how to remove it. The raise stays held |
| `DroppedServersReturned` | Warning | Server pods the spec keeps run servers Neo4j has dropped: they started on the volumes of removed servers (a raise made too soon by an earlier release). The message says how to recover: lower `spec.topology.servers`, wait for the drain to complete, raise it again |
| `ScaleDownDraining` | Normal | A scale-down step ran: servers cordoned, their databases deallocated, or the drained servers dropped |
| `ScaleDownBlocked` | Warning | Scale-down refused or stuck — the target is below the `system` database's minimum voting members, the `DEALLOCATE` dry-run / `DROP SERVER` failed (the message quotes Neo4j and advises for the cause it names), or the scale-down was set back after its drain began deallocating a server the cluster would keep; replicas are held. Once per cause, announced from the `ServersPendingDrain` condition the operator writes, not on every retry |
| `ScaleDownSetBack` | Normal | A scale-down set back before its drain began deallocating — within 20 s of the cordon: the servers the drain had cordoned were enabled again (`ENABLE SERVER`). A server you cordoned yourself stays cordoned |

### Rolling Upgrades

| Reason | Type | Description |
|---|---|---|
| `UpgradeStarted` | Normal | Rolling upgrade initiated, or a `Paused` or `Failed` one resumed ("Rolling upgrade resumed after it was …"). A resumed one whose restarted servers cannot start on the earlier target names them: "… cannot start on … and is replaced first". On a `Neo4jEnterpriseStandalone`, once per new image: emitted by the pass that writes it, read from the API server, not the cache |
| `UpgradeCompleted` | Normal | Rolling upgrade finished successfully |
| `UpgradePaused` | Warning | Upgrade paused after a failed check or step (`autoPauseOnFailure`); the servers stay where it stopped until you change `spec.image.tag` or turn `autoPauseOnFailure` off. Announced once per target and cause; a check that refuses the upgrade before it starts is announced once, not on every attempt. The cluster's phase does not change (`Ready`, its message naming the paused upgrade) |
| `UpgradeFailed` | Warning | Upgrade failed. Announced once per target and cause: a check that refuses it before it starts, while the operator tries it every minute, and an upgrade the operator retries that stops again where it stopped, are announced again only with a new error or a new target; the cluster stays `Ready`, with no `ClusterReady` on a retry. On a `Neo4jEnterpriseStandalone` with `upgradeStrategy.preUpgradeHealthCheck` and `autoPauseOnFailure`, also: the pre-upgrade health check failed and holds the upgrade back — advisory while it stays blocked |
| `UpgradeRetargeted` | Normal | `spec.image.tag` changed while an upgrade was in flight (`Staging`, `Rolling`, `Stabilizing` or `Verifying`): the upgrade goes on towards the new tag, staged again ("Rolling upgrade retargeted from … to … during …"). Once per new target. A `Paused` or `Failed` upgrade taken up towards a new tag is announced by `UpgradeStarted` ("resumed") instead |
| `UpgradeDeferred` | Normal | An image change was held back — the cluster is not yet `Ready` (announced once the hold outlasts one reconcile), or a scale-down drain is in progress (once per target) — and will be performed by the rolling-upgrade state machine afterwards. Once per held target, not on every check (a drain's deferral names the target, and each drain announces it once) |

### Backups and Restores

| Reason | Type | Description |
|---|---|---|
| `BackupScheduled` | Normal | The backup's CronJob is armed: first created, back from another phase (`Suspended`, `Waiting`, …), or its schedule changed. The message names the CronJob and the schedule. Not repeated while the schedule stays as it is |
| `BackupStarted` | Normal | Backup job has started |
| `BackupCompleted` | Normal | Backup job completed successfully. Once, from the write that makes the backup `Completed`; a one-time backup that is `Completed` is never reopened |
| `BackupFailed` | Warning | Backup job failed (once, from the write that makes the backup `Failed`), saying why as `status.message` does: "Backup job <name> failed (<the Job's reason>: <its message>): backup exited with code <n>: <the last lines the container wrote>" (it was "Backup job failed" alone); a scheduled backup's failed run carries the same text in its `status.history[].error`. Or the spec is invalid (phase `Invalid`, once per refusal) |
| `RestoreStarted` | Normal | Restore operation has started, once per attempt. A restore that runs a `neo4j-admin` Job announces it when the restore turns `Running` (never for a restore a check then refuses): "Restore job … started", and for a standalone restore that runs offline (a point in time, a `source.type: storage` path) why — with `stopCluster: true` it says the restore stops the standalone, with `stopCluster: false` that the standalone is stopped already. One event: the offline reason used to be a second `RestoreStarted` of its own. An online restore announces "Online restore into …" once; into a standalone with `stopCluster: true` the same event says `spec.stopCluster is ignored` (the standalone is restored online), which was a second `RestoreStarted` of its own. An all-databases restore announces its start once, naming how many databases it restores — once per attempt, not again after a wait mid-way (a target briefly unavailable, the seed proxy restarting); each database's progress is in `status.databaseResults` |
| `RestoreCompleted` | Normal | Restore operation completed. Also once at the end of an all-databases restore ("All-databases restore completed: Restored N databases") |
| `RestoreFailed` | Warning | Restore operation failed. A failed restore Job says why, as `status.message` does: "Restore job <name> failed (<the Job's reason>: <its message>): neo4j-restore exited with code <n>: <the last lines the container wrote>" — the restore container's termination message, the tail of its log (it was "Restore job failed" alone). An online restore over a database that exists without `options.replaceExisting` raises it once. A spec the operator refuses before anything runs (`database: system`, an offline restore into a running standalone without `stopCluster`, a database that exists, a Neo4j version it does not support) is `Failed` with the reason in `status.message`, logged once at info, never as an ERROR. Also once at the end of an all-databases restore in which a database failed |
| `RestoreFromChainParent` | Warning | `source.backupRef` points at a FULL+DIFF chain parent; the restore seeds from its latest full snapshot, not the latest chain state. Advisory, so a restore that waits (for seed credentials, a rollout, the seed proxy) does not repeat it on every check |
| `DatabaseCreateFailed` | Warning | Database creation failed during a restore operation |
| `BackupRetentionCaveat` | Warning | Retention pruning is configured on a CR whose chain may contain differential artifacts; pruning can orphan DIFFs whose parent FULL ages out (prefer `backupType: FULL` with retention) |
| `BackupShardedDatabasesExcluded` | Warning | An all-databases backup captured property-sharded database(s) as per-shard artifacts; an all-databases restore does not recreate them — restore each from the same backup via its `Neo4jShardedDatabase` CR with `spec.seedBackupRef`. Once per run, when the run is recorded in `status.history` |
| `BackupShardedDifferential` | Warning | A run wrote differential backups of sharded-database shards to a PVC; no sharded database can be seeded from that run. Comes from an all-databases backup; a `spec.shardedDatabase` backup takes full backups on a PVC. Once per run, when the run is recorded in `status.history` |
| `BackupValidateUnsupported` | Warning | `options.validate` has no effect: `neo4j-admin backup validate` needs a CalVer (2025.x+) image. Advisory |
| `ServiceAccountAnnotationConflict` | Warning | A backup/restore CR overwrote different workload-identity annotations on the namespace's shared `neo4j-backup-sa` / `neo4j-restore-sa`; the last CR reconciled wins and the others' cloud access breaks. On a `Neo4jBackup` it is advisory: two backups that disagree overwrite each other on every pass, and each says so once |
| `SeedEndpointNotProjected` | Warning | An online restore's server pods (cluster or standalone) lack the cloud credentials or custom S3 endpoint (`AWS_ENDPOINT_URL_S3`) the seed fetch needs, and the target is not opted in to `neo4j.com/auto-inherit-seed-creds`. The restore waits in `Pending` with the missing items named, and carries on once the target has them or is opted in. Emitted when the restore starts waiting (or what it waits for changes), once |
| `RestoreDatabaseFailed` | Warning | One database of an all-databases restore could not be restored (it exists and `spec.options.replaceExisting` is not set, Neo4j refused the create or recreate, the backup recorded no artifact for it); the others carry on, and the restore ends `Failed` with `RestoreFailed`. Once per database, from the source `neo4j-restore-progress`; client-go folds the eleventh and later within ten minutes into one `(combined from similar events)` event, and `status.databaseResults` names each |
| `RestoreSeedProgress` | Normal | An all-databases restore's progress summary: how many databases are restored and failed, and the database being seeded with its progress (allocations online, or the last poll's error) and how long it has shown it — `3 of 30 database(s) restored, 0 failed; seeding "db03": 0/3 allocations online, unchanged for 40m` is a stall. One on the restore's first seed poll, then one every 10 minutes while it seeds: one per restore, not one per database change, so every one reaches the API server whatever the number of databases. From the source `neo4j-restore-progress`, which has an event budget of its own (client-go: a burst of 25 per source, object and type, then one every five minutes), so the progress cannot crowd out the restore's `RestoreCompleted` or `RestoreFailed`. The per-database record is `status.databaseResults` |
| `RestoreShardedDatabasesNotCovered` | Warning | An all-databases restore's source backup recorded property-sharded databases it does not recreate; restore each via its `Neo4jShardedDatabase` CR. Advisory |

### Databases

| Reason | Type | Description |
|---|---|---|
| `DatabaseReady` | Normal | Database became ready: created and online, or back from a failure of its own. Not repeated while it stays ready, and not on a restart of its host: a `Ready` database stays `Ready` (with the condition `ClusterNotReady=True`) while the host is not Ready or does not answer |
| `DatabaseDeleted` | Normal | Database was dropped |
| `DuplicateName` | Warning | An older `Neo4jDatabase` on the same cluster or standalone gives the same database name, ignoring case (Neo4j lower-cases names, so `Sales` and `sales` are one database): this one is `Failed`, sends Neo4j nothing, and carries on by itself once the older one is deleted or renamed. Once per conflict. Also emitted, with the same rule, by `Neo4jShardedDatabase`, `Neo4jCompositeDatabase` and `Neo4jDatabaseAlias` against an older resource of their own kind |
| `DuplicateNameKept` | Normal | A deleted `Neo4jDatabase` (or `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`) released its finalizer without dropping anything: another resource of its kind still gives the name, ignoring case, and keeps the database (composite, alias). The event names that resource |
| `DatabaseCreatedFromSeed` | Normal | Database created from a seed URI |
| `CreationFailed` | Warning | Database creation failed; the database is `Failed` and retried with backoff. Once per failure. A statement Neo4j asks to be retried (a `Neo.TransientError` — a cluster that lost the majority of its servers still reads `Ready` and answers one) is not a failure: the database waits quietly (condition `ClusterNotReady`, reason `ClusterUnreachable`; one that was `Ready` stays `Ready`, and its Ready event is not repeated when the cluster serves again) and only a wait that lasts more than 5 minutes is reported, as `ConnectionFailed` |
| `DeletionFailed` | Warning | `DROP DATABASE` failed, or the operator could not connect to the host, while deleting the `Neo4jDatabase`. The drop is retried (the database is phase `Error` with the failure in `status.message`) until it succeeds or five minutes have passed since the deletion was requested, then the finalizer is released. Once per failure, not on every retry; a second event says when the finalizer is released and names the database left in Neo4j with the `DROP DATABASE` that removes it |
| `DataImported` | Normal | Initial data imported successfully |
| `DataImportFailed` | Warning | Initial data import failed; retried with backoff. Once per failure |
| `DataSeeded` | Normal | Database seeded from URI |
| `SeedCredsMissing` | Warning | The seed credentials Secret (`spec.seedCredentials.secretRef`) is not projected onto the hosting cluster/standalone's `spec.extraEnvFrom` (for a `Neo4jShardedDatabase`'s cloud `seedBackupRef`: the backup's credentials Secret or custom S3 endpoint on the cluster's server pods); the message carries a copy-pasteable fix. The database waits in phase `Pending` and is created by itself once the host has them — nothing is asked of Neo4j before. Once per wait. Also emitted for `Neo4jShardedDatabase` |
| `SeedCredsAutoInherited` | Normal | The hosting cluster/standalone carries `neo4j.com/auto-inherit-seed-creds: "true"`, so the operator patched its `spec.extraEnvFrom` with the seed credentials Secret and is waiting (phase `Pending`) for the rolling restart (also emitted for `Neo4jShardedDatabase`) |
| `ValidationWarning` | Warning | Database spec produced validation warnings, including a set field that nothing reads (`spec.initialData.configMapRef`, `secretRef`, `storage`, or a `source` other than `cypher`) |
| `ClusterNotFound` | Warning | Referenced cluster or standalone not found; the database waits in phase `Pending` and carries on once it exists. Once per wait. For an object whose cluster or standalone went away (it had found it before), only if the object is still there a minute later: a teardown that deletes the deployment first raises none. A missing deployment is never a validation failure. Also emitted, with the same wait, by `Neo4jShardedDatabase` (for a cluster), `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`, `Neo4jReplicaDatabase` and `Neo4jPlugin` (phase `Pending`, then installed once its target exists), `Neo4jRestore` (phase `Pending`) and `Neo4jBackup` (phase `Waiting`, as a backup waits for a target that is not Ready) — the backup and the restore say "Waiting for target to appear: …". A restore that has already `Completed` or `Failed` at its generation is not reopened by its target going away |
| `ClusterNotReady` | Warning | On a `Neo4jShardedDatabase`: the referenced cluster cannot host it — property sharding needs Neo4j 2025.12+ and `spec.propertySharding.enabled` on the cluster; phase `Failed`. Once per failure. A cluster that is simply not Ready yet is a wait, with no event: phase `Waiting`, or, for a sharded database already `Ready`, `Ready` with the condition `ClusterNotReady=True` |
| `ConnectionFailed` | Warning | Could not connect to Neo4j via Bolt; retried with backoff. Once per failure. Emitted by `Neo4jReplicaDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jCompositeDatabase` and `Neo4jDatabaseAlias`, and by `Neo4jDatabase` for an outage that outlasted 5 minutes, and by an all-databases `Neo4jRestore` whose target has not answered for 5 minutes (it goes on waiting, and resumes where it was). An object whose target reads `Ready` but refuses the connection — Bolt not open yet after a restart, a `stopCluster` restore — waits quietly instead (condition `ClusterNotReady`, reason `ClusterUnreachable`; one that was `Ready` stays `Ready`) and raises this only if that lasts more than 5 minutes. So does a `Neo4jDatabase`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias` or `Neo4jReplicaDatabase` whose statements Neo4j asks to be retried (a `Neo.TransientError` — a cluster that lost the majority of its servers still reads `Ready` and answers one) |
| `ClientCreationFailed` | Warning | Failed to create Neo4j Bolt client for the cluster (`Neo4jShardedDatabase`); retried with backoff. Once per failure. A cluster that reads `Ready` but refuses the connection, or answers the statements with an error Neo4j asks to be retried (a cluster that lost the majority of its servers), is waited through quietly (condition `ClusterNotReady`, reason `ClusterUnreachable`) and reported only after 5 minutes |

### Plugins

| Reason | Type | Description |
|---|---|---|
| `PluginInstalled` | Normal | Plugin installed: every server runs the new pod template. Once per install, from the status write that makes the plugin `Ready` (a pass that read an older copy of the plugin says nothing) |
| `PluginInstallFailed` | Warning | Plugin installation failed (once per failure; retried every 30s without repeating it). A server that is restarting or briefly unreachable is not a failure: the plugin stays `Installing`. Also an install refused for something in the target's spec — a variable the cluster renders itself (`NEO4J_PLUGINS` in its `spec.env` without the plugin), or a `NEO4J_PLUGINS` that is not a JSON list: "Plugin <name> installation refused: …", once, from the status write that makes the plugin `Failed`, logged once at info and never as an ERROR |
| `PluginMissing` | Warning | A `Ready` plugin's target StatefulSet no longer carries what the plugin set (its `NEO4J_PLUGINS` entry, a variable it sets, a `VerifiedDownload` init container or its volumes); the plugin goes back to `Installing` and restores it, which restarts the servers — or, while the target is not `Ready` with every server available, says so and is `Waiting` until it is |
| `PluginInstallDeferred` | Normal | Installing the plugin — or a `VerifiedDownload` plugin's update of its trusted CAs after the target's `spec.trustedCASecrets` changed, or the removal of a plugin set to `spec.enabled: false` — would restart the servers of a target that is not `Ready` with every server available (a cluster `Degraded`, or forming, rolling or upgrading); the plugin is `Waiting` and writes once the target is. Once per wait |
| `PluginDuplicate` | Warning | Another enabled `Neo4jPlugin` already targets this name (an enabled CR wins over a disabled one, then the oldest; a newer enabled duplicate sits in `Failed` until the other is deleted or disabled, a disabled one is `Disabled`, never a duplicate). Once per conflict |
| `PluginInitContainerFailing` | Warning / Normal | A `VerifiedDownload` plugin writes a change of its own failing init container — a fixed source, a CA list, or its removal when it is disabled — to a target that is not `Ready` (Warning, once), or deletes a standalone's pod stuck in that container (Normal) |
| `PluginSettingsFailing` | Warning / Normal | The plugin writes a change of the settings every down server keeps failing with — a key removed, the plugin disabled, or the plugin refused (a `NEO4J_PLUGINS` name the image does not install) — to a target that is not `Ready` (Warning, once), or deletes a standalone's pod stuck on the old settings (Normal) |
| `PluginUnknownToImage` | Warning | A `Managed` plugin's `NEO4J_PLUGINS` name — its own or a dependency's — is not one the image of its target installs (`graphql` on every supported release, `n10s` on CalVer from 2025.05, a custom JAR's name): the entrypoint would exit on it. Also a name whose JAR the image does not run: `n10s` from 5.26.22 (the neosemantics 5.26.0 JAR stops the server on Jackson) and on CalVer 2025.01 – 2025.04 (no JAR to download). Phase `Failed`, nothing written, and what an earlier operator wrote of it taken off the target. Once, and again only when the refusal changes (the target's image did) |
| `PluginDisabled` | Normal | `spec.enabled: false`, and nothing of the plugin is on its target's servers any more (its removal written and rolled out, or nothing there): phase `Disabled`. Once |
| `PluginProceduresHidden` | Warning | The target's effective `dbms.security.procedures.allowlist` — shared by every plugin of the target, as its servers will start with it — loads none of this plugin's procedures; the plugin carries the condition `ProceduresHidden` (reason `NotInAllowlist`) and stays `Ready`. When the condition appears or its message changes |

### Split-Brain Detection

| Reason | Type | Description |
|---|---|---|
| `SplitBrainDetected` | Warning | Split-brain condition detected in the cluster |
| `SplitBrainRepaired` | Normal | Split-brain condition repaired automatically |
| `SplitBrainRepairFailed` | Warning | Automatic split-brain repair failed |

### Aura Fleet Management

| Reason | Type | Description |
|---|---|---|
| `AuraFleetManagementRegistered` | Normal | Successfully registered with Aura Fleet Management |
| `AuraFleetManagementFailed` | Warning | Aura Fleet Management token registration failed. Raised when a failure first appears or its message changes, not on every retry; the current message is in `status.auraFleetManagement.message` |
| `AuraFleetManagementPluginPatchFailed` | Warning | The fleet-management merge into the server StatefulSet's `NEO4J_PLUGINS` failed, and the message says which step: `Failed to add fleet-management to NEO4J_PLUGINS` for the write, `Could not check the server StatefulSet's NEO4J_PLUGINS for fleet-management` for the check before it (the StatefulSet could not be read, it has no `neo4j` container). On an image with Fleet Manager built in (2026.03+) only the check runs: nothing is added there. Raised when the failure starts or its message changes, not on every pass while it lasts; announced again if it comes back after a patch went through. Not raised while the server StatefulSet does not exist yet (a new deployment's first pass): the write that creates it already lists the plugin |
| `AuraFleetDeploymentCreated` | Normal | Operator-driven provisioning (`spec.auraFleetManagement.provision`) registered a Fleet Manager deployment in Aura |
| `AuraFleetDeploymentAdopted` | Normal | Provisioning adopted an existing Aura fleet deployment of the same name instead of creating one |
| `AuraFleetDeploymentDeleted` | Normal | The Aura fleet deployment was unregistered |
| `AuraFleetTokenProvisioned` | Normal | The fleet registration token was stored in a Secret |
| `AuraFleetTokenRotated` | Warning | The fleet token was rotated and the new one stored in its Secret; any previous registration is now invalid. Raised once the new token is stored: while the Secret cannot be written, each attempt rotates again and `status.auraFleetManagement.message` says why |

### Storage Expansion

| Reason | Type | Description |
|---|---|---|
| `StorageExpansionStarted` | Normal | PVC expansion has begun after detecting `spec.storage.size` increase |
| `StorageExpansionCompleted` | Normal | All PVCs expanded and StatefulSet recreated successfully |
| `StorageExpansionFailed` | Warning | Expansion failed (non-expandable StorageClass, shrink attempt, or patch error); the deployment is `Failed` and retried with backoff. Once per failure — a retry of the same failed expansion does not announce `StorageExpansionStarted` again, and a cluster's phase no longer flips `Expanding`/`Failed` on every retry |
| `StorageClassNotFound` | Warning | `spec.storage.className` references a StorageClass that does not exist in the cluster; the deployment is `Failed` until it exists. Once per failure |

### Sharded Databases

| Reason | Type | Description |
|---|---|---|
| `ShardedDatabaseReady` | Normal | Sharded database became ready (created and all shards online), or was recreated by `spec.replaceExisting`. Not repeated while it stays ready, and not after a rollout or restart of its cluster: a `Ready` sharded database stays `Ready` (with the condition `ClusterNotReady=True`) while the cluster is not Ready |
| `ShardedDatabaseDropped` | Normal | An existing sharded database was dropped before being recreated from a seed (`spec.replaceExisting` with `spec.force`) |
| `SeedBackupPending` | Normal | `spec.seedBackupRef` names a `Neo4jBackup` with no Succeeded run yet; the sharded database stays `Pending` and requeues. Emitted when the wait starts, not on every retry |
| `SeedBackupResolutionFailed` | Warning | `spec.seedBackupRef` could not be resolved into a concrete seed (the `Neo4jBackup` is missing, or its storage or run cannot seed a sharded database); phase `Failed`. Once per failure |
| `SeedProxyStarting` | Normal | Waiting for the PVC `backup-seed-proxy` Deployment to become Ready before seeding. Emitted when the wait starts, not on every retry |

A `Neo4jShardedDatabase` also emits `DuplicateName` (an older one gives the same name, ignoring case: phase `Failed`), `ClusterNotFound` (the cluster does not exist yet: phase `Pending`), `ClusterNotReady`, `ClientCreationFailed`, `ReconcileFailed`, `SeedCredsMissing` (phase `Pending`) and `SeedCredsAutoInherited`, described above.

### Users, Roles and Role Bindings

Emitted on `Neo4jUser`, `Neo4jRole` and `Neo4jRoleBinding` objects.

| Reason | Type | Description |
|---|---|---|
| `UserCreated` | Normal | User created in Neo4j |
| `UserUpdated` | Normal | User updated (or its password recorded as already applied — once, when status first records it, not on every retry of a pass that then fails; then without a `PasswordRotated`, since nothing was rotated, e.g. a `Neo4jUser` for `neo4j` whose Secret holds the admin password) |
| `UserReady` | Normal | User exists and its roles are in sync. When the user becomes `Ready` — first, or back from a failure of its own — not on a restart of its target: a `Ready` user stays `Ready` (with `ClusterNotReady=True`) while the target is down |
| `UserDeleted` | Normal | User dropped |
| `UserDeletionFailed` | Warning | `DROP USER` could not run; the user is `Failed` and the drop is retried. A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes). Once per failure, not on every retry |
| `UserSyncFailed` | Warning | Reconciling the user against Neo4j failed. Once per failure: the user keeps its failure while its target restarts, so the same failure afterwards is not announced again. A statement Neo4j asks to retry (a deadlock, a leader switch) is retried quietly, with nothing on status, unless the user keeps meeting such errors for 5 minutes |
| `PasswordRotated` | Normal | Password updated from the referenced Secret. Once per Secret version — at a user's creation, and at each change of its Secret — announced by the status write that records it. Not when Neo4j already held that password: that is the `UserUpdated` above |
| `RolesGranted` / `RolesRevoked` | Normal | Roles granted to / revoked from a user, role binding or auth rule |
| `RolesResolved` | Normal | `spec.roles` names `Neo4jRole` resources by their CR name, and those were granted as the Neo4j roles they manage; the message maps each, as the `RoleNamesResolved` condition does. Once when the mapping appears or changes, not on every reconcile. Emitted by `Neo4jUser` and `Neo4jRoleBinding` |
| `RolePending` | Warning | Waiting for referenced roles to exist — not built in, not a `Neo4jRole`, not in `SHOW ROLES`; names them (sorted), as the `PendingDependencies` condition does. Once per wait, and again when the set of missing roles changes. Emitted by `Neo4jUser`, `Neo4jRoleBinding` and `Neo4jAuthRule` |
| `RoleCreated` | Normal | Role created in Neo4j |
| `RoleReady` | Normal | Role exists and its privileges are in sync. When the role becomes `Ready`, not on a restart of its target; a new role whose statements wait for their database is announced once they are applied, after its `PrivilegesApplied`, with their count |
| `RoleDeleted` | Normal | Role dropped |
| `RoleDeletionFailed` | Warning | `DROP ROLE` could not run; the role is `Failed` and the drop is retried. A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes). Once per failure, not on every retry |
| `RoleSyncFailed` | Warning | Reconciling the role against Neo4j failed. Once per failure, a target restart included. A statement Neo4j asks to retry (a deadlock, a leader switch) is retried quietly, with nothing on status, unless the role keeps meeting such errors for 5 minutes |
| `PrivilegesApplied` | Normal | Privileges were added to and/or revoked from the role. Only grants that created a privilege row count: learn mode re-grants every statement after an alias change on the cluster or a new image, and grants that change nothing are not reported |
| `PrivilegesDriftKept` | Warning | A drifting privilege was not revoked (it is immutable, or no `REVOKE` could be derived). Advisory, one per kept privilege |
| `PrivilegeNamesUnknownDatabase` | Warning | A privilege names a database that does not exist on the cluster and is not an alias for one (it is skipped), or grants graph privileges on a composite database (accepted by Neo4j but inert); see `PrivilegesResolve` on the role |
| `PrivilegeTargetDropped` | Warning | A `Neo4jDatabase` was dropped while a `Neo4jRole` on the same cluster still grants on it; emitted on both objects |
| `BindingCreated` / `BindingUpdated` / `BindingDeleted` | Normal | `Neo4jRoleBinding` granted its roles, changed the granted set, or revoked its roles on deletion. `BindingCreated` is not repeated on a restart of the target |
| `BindingFailed` | Warning | A role binding grant or revoke failed. On deletion the revokes are retried. A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes); the roles still granted are listed. Once per failure |
| `UserNotFound` | Warning | The user a `Neo4jRoleBinding` names does not exist; the binding waits in `Pending`. Once per wait, a target restart included |

### Auth Rules

| Reason | Type | Description |
|---|---|---|
| `AuthRuleCreated` / `AuthRuleUpdated` / `AuthRuleDeleted` | Normal | `Neo4jAuthRule` created, its condition or enabled flag updated, or the rule dropped |
| `AuthRuleFailed` | Warning | Creating, updating or dropping the auth rule failed. On deletion the drop is retried. A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes). Once per failure |
| `AuthRuleVersionTooOld` | Warning | Auth rules need Neo4j 2026.03 or later on the referenced cluster. Once per failure |
| `OIDCProviderNotConfigured` | Warning | The referenced cluster's `spec.config` lists no OIDC provider for attribute-based access control; the rule stays `Pending`. Once per wait |

### Cross-Cluster Replication

| Reason | Type | Description |
|---|---|---|
| `ReplicationSourceCaveat` | Normal / Warning | On a `Neo4jBackup` with `mode: replication-source`: Normal when the pull URI is published (with the reminder that bucket lifecycle rules can break the chain); Warning when the storage type (e.g. `pvc`) has no cross-cluster URI form (advisory) |
| `ReplicaCreated` | Normal | Replica database created |
| `ReplicaReady` | Normal | Replica database is online: once when it becomes `Replicating`, not again after a restart of its downstream target (a `Replicating` replica stays `Replicating`, with the condition `ClusterNotReady=True`, while the target is away) |
| `ReplicaFailed` | Warning | Creating the replica, or dropping it on deletion, failed. A statement Neo4j asks to be retried (a `Neo.TransientError` — a cluster that lost the majority of its servers still reads `Ready` and answers one) is not a failure: the replica waits quietly (condition `ClusterNotReady`, reason `ClusterUnreachable`; one that was `Replicating` stays `Replicating`, and its Ready event is not repeated when the cluster serves again) and only a wait that lasts more than 5 minutes is reported, as `ConnectionFailed`. On deletion the drop is retried, and a database type that cannot be read is never taken for a promoted database; A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes). Once per failure |
| `ReplicaObjectStoreEnvMissing` | Warning | A backup-mode replica's downstream servers have no `AWS_REGION` (or `AWS_DEFAULT_REGION`) in any source of their environment, so Neo4j could not read the `s3://` seed or pulls. The replica waits in `Pending` — nothing is asked of Neo4j yet — and is created by itself, within two minutes, once the servers have it. Once per wait. (Before: `ReplicaFailed`, phase `Failed`) |
| `ReplicaDropped` | Normal | Replica database dropped on CR deletion |
| `ReplicaVersionTooOld` | Warning | The downstream cluster predates Neo4j 2026.08; phase `Failed`. Once per failure |
| `ReplicaPromotionDetected` | Warning | The database is no longer a replica (promoted, possibly out of band); the CR goes inert |
| `ReplicaRetainedAfterPromotion` | Warning | The CR was deleted after promotion; the promoted database is retained, not dropped |
| `ReplicaPromotionStarted` | Warning | A `Neo4jReplicaPromotion` started; irreversible, and any outstanding lag becomes permanent data loss. Once per promotion, not on each retry of the `PROMOTE` call |
| `ReplicaPromotionCompleted` | Normal | Promotion completed |
| `ReplicaPromotionFailed` | Warning | Promotion failed, or a `PROMOTE` call failed and is retried (the promotion stays `Promoting`, with the failure in `status.message`). Once per failure |
| `CrossClusterProxyUnauthenticated` | Warning | The CCDR proxy was published while `spec.tls.strictPeerValidation` is disabled, so the exposed tx-shipping port is encrypted but not authenticated |

The replica's `UpstreamClusterNotFound`, `UpstreamClusterNotReady`, `UpstreamBackupNotFound` and `UpstreamBackupNotReady` are not events: they are the reason on the replica's `Ready` condition (phase `Pending`) while `source.upstreamClusterRef` / `source.upstreamBackupRef` cannot be resolved yet — read them with `kubectl describe neo4jreplicadatabase`.

### Composite Databases and Aliases

| Reason | Type | Description |
|---|---|---|
| `CompositeDatabaseCreated` | Normal | Composite database created |
| `CompositeDatabaseReady` | Normal | Composite database is ready, with its observed constituent count. Once when it becomes `Ready`, not again after a restart of its target (a `Ready` composite stays `Ready`, with the condition `ClusterNotReady=True`, while the target is away) |
| `CompositeDatabaseFailed` | Warning | Reconciling the composite database failed. Once per failure. A statement Neo4j asks to be retried (a `Neo.TransientError` — a cluster that lost the majority of its servers still reads `Ready` and answers one) is not a failure: the composite waits quietly (condition `ClusterNotReady`, reason `ClusterUnreachable`; one that was `Ready` stays `Ready`, and its Ready event is not repeated when the cluster serves again) and only a wait that lasts more than 5 minutes is reported, as `ConnectionFailed`. On deletion the drop is retried. A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes) |
| `CompositeDatabaseDropped` | Normal | Composite dropped (constituent aliases removed; their target databases kept) |
| `CompositeConstituentAdded` | Normal | A constituent alias now resolves to its target |
| `CompositeConstituentRetargeted` | Normal | A constituent alias was re-pointed to a different target |
| `CompositeConstituentRemoved` | Normal | A constituent not in `spec.constituents` was removed |
| `CompositeDatabaseNameBlocked` | Warning | A dotted alias already occupies the composite's namespace, so the composite cannot be created until that alias is dropped (`kubectl neo4j explain CompositeDatabaseNameBlocked`). Once per blocking alias |
| `AliasCreated` | Normal | Database alias created |
| `AliasRetargeted` | Normal | Alias re-pointed to a different target database |
| `AliasReady` | Normal | Alias resolves to its target. Once when it becomes `Ready`, not again after a restart of its target (a `Ready` alias stays `Ready`, with the condition `ClusterNotReady=True`, while the target is away) |
| `AliasDropped` | Normal | Alias dropped |
| `AliasFailed` | Warning | Reconciling or dropping the alias failed. Once per failure. A statement Neo4j asks to be retried (a `Neo.TransientError` — a cluster that lost the majority of its servers still reads `Ready` and answers one) is not a failure: the alias waits quietly (condition `ClusterNotReady`, reason `ClusterUnreachable`; one that was `Ready` stays `Ready`, and its Ready event is not repeated when the cluster serves again) and only a wait that lasts more than 5 minutes is reported, as `ConnectionFailed`. On deletion the drop is retried. A deletion that cannot run — the target not Ready, refusing the connection, its admin Secret unreadable, or Neo4j refusing the statement — waits with `deletion waits to remove …` in `status.message`; five minutes after the deletion was requested the finalizer is released with a second event, `Released the finalizer without removing …`, naming what is left in Neo4j and the statement that removes it (a target whose CR is gone, or is being deleted with its volumes (`storage.retentionPolicy: Delete`, the default), releases at once; one gone with its volumes kept waits, bounded, for a deployment created again from them; a statement Neo4j asks to retry — a deadlock in a bulk delete — is retried quietly and reported only if it lasts past the 5 minutes) |

A composite or an alias also emits `DuplicateName` when an older resource of its kind gives the same name on its target, ignoring case, and `DuplicateNameKept` when its deletion leaves the name to such a resource (see [Databases](#databases)).

### Aura Orchestration

Emitted by the Aura CRDs (`AuraProviderConfig`, `AuraInstance`, `AuraSnapshot`, `AuraRestore`, `AuraCustomerManagedKey`, `AuraIPFilter`, `AuraDatabase`, `AuraDatabaseBackup`, `AuraDatabaseRestore`, `AuraOrganizationMember`, `AuraProjectMember`, `AuraInvite`). "Adopted" means the CR took over an existing Aura resource; "Orphaned" means the CR was deleted but the Aura resource was left in place. The `…Ready` events and `AuraCredentialsValidated` are raised when the state is reached, not on each periodic re-check while it holds. A 429, a 5xx or a network failure of a create or a backup is retried without an event, as for every other Aura call. A failure that repeats is one Warning however often it is retried: Aura gives every response its own request ID, which the Warning carries (quote it to Aura support) and the status message leaves out, so the same refusal reads the same on every retry.

| Reason | Type | Description |
|---|---|---|
| `AuraCredentialsValidated` / `AuraCredentialsInvalid` | Normal / Warning | The provider config's Aura API credentials were accepted / rejected. Each once per change: valid again after a failure is announced again |
| `AuraInstanceCreated`, `AuraInstanceAdopted`, `AuraInstanceUpdated`, `AuraInstancePaused`, `AuraInstanceResumed`, `AuraInstanceUpgraded`, `AuraInstanceDeleted`, `AuraInstanceOrphaned` | Normal | Lifecycle of the Aura instance. `AuraInstanceOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials stayed unavailable for 5 minutes after deletion was requested, or Aura kept refusing the delete for 5 minutes (anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last): the cloud instance keeps running, so delete it in the Aura console |
| `AuraInstanceFailed` | Warning | Aura instance reconcile failed (also: deletion refused while `deletionProtection` is set, recorded on the `Synced` condition, reason `DeletionProtected`; a `Delete` waiting for credentials it cannot read, reason `CredentialsUnavailable`; a `Delete` Aura refused, reason `DeletionRefused`, asked again until 5 minutes after deletion was requested). Once per failure |
| `AuraSnapshotCreated` | Normal | Aura snapshot requested |
| `AuraSnapshotFailed` | Warning | Requesting or reading the snapshot failed with an error Aura will not clear on a retry (phase `Error`, reason `CreateFailed` or `ObserveFailed`). Once per failure; the operator keeps retrying. A busy instance (`snapshot-not-allowed`, HTTP 409) and a 429, 5xx or network failure are retried without it |
| `AuraRestoreStarted`, `AuraRestoreCompleted` | Normal | Instance restore from a snapshot started / completed |
| `AuraRestoreFailed` | Warning | The instance entered a failed status during restore |
| `AuraCustomerManagedKeyCreated`, `AuraCustomerManagedKeyAdopted`, `AuraCustomerManagedKeyReady`, `AuraCustomerManagedKeyDeleted`, `AuraCustomerManagedKeyOrphaned` | Normal | Lifecycle of the customer-managed key (`AuraCustomerManagedKeyReady` once when the key becomes ready). `AuraCustomerManagedKeyOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials stayed unavailable for 5 minutes, or Aura kept refusing the delete for 5 minutes (anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last), or the key was still in use 5 minutes after deletion was requested: the key stays registered in Aura |
| `AuraCustomerManagedKeyDeleteBlocked` | Warning | The key cannot be deleted: still in use by one or more instances. Once per refusal. The deletion asks again until 5 minutes after it was requested (condition `Synced`, reason `KeyInUse`, which names the time), then releases the finalizer with a Warning `AuraCustomerManagedKeyOrphaned` |
| `AuraCustomerManagedKeyFailed` | Warning | Customer-managed key reconcile failed (also: a `Delete` waiting for credentials, reason `CredentialsUnavailable`; a `Delete` Aura refused, reason `DeletionRefused`). Once per failure |
| `AuraIPFilterCreated`, `AuraIPFilterAdopted`, `AuraIPFilterUpdated`, `AuraIPFilterReady`, `AuraIPFilterDeleted`, `AuraIPFilterOrphaned` | Normal | Lifecycle of the IP filter (`AuraIPFilterReady` once when it becomes Ready). `AuraIPFilterOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials or organization stayed unavailable for 5 minutes, or Aura kept refusing the delete for 5 minutes (anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last): the filter stays in place in Aura |
| `AuraIPFilterFailed` | Warning | IP filter reconcile failed (also: a `Delete` waiting for credentials, reason `CredentialsUnavailable`; a `Delete` Aura refused, reason `DeletionRefused`). Once per failure |
| `AuraDatabaseCreated`, `AuraDatabaseAdopted`, `AuraDatabaseReady`, `AuraDatabaseDeleted`, `AuraDatabaseOrphaned` | Normal | Lifecycle of the Aura database (`AuraDatabaseReady` once when it becomes Ready; `AuraDatabaseCreated` is a Warning when the instance already had databases: a pre-existing one cannot be adopted by name, so verify no duplicate was created). `AuraDatabaseOrphaned` is a Warning when `deletionPolicy: Delete` could not run: at once when the database's `AuraInstance` CR no longer exists — nothing can reach Aura for it, since the credentials come through the instance; the database went with the instance if that was deleted in Aura, otherwise it is still there, so check the Aura console — and after 5 minutes when the credentials, the organization or project, or the instance's ID stayed unavailable, or Aura kept refusing the delete for 5 minutes (anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last): the database stays on the instance with its data |
| `AuraDatabaseFailed` | Warning | Aura database reconcile failed (also: a `Delete` waiting for what it needs to call Aura, reason `CredentialsUnavailable`; a `Delete` Aura refused, reason `DeletionRefused`). Once per failure |
| `AuraDatabaseBackupCreated` | Normal | Per-database backup created |
| `AuraDatabaseBackupFailed` | Warning | Per-database backup failed. Once per failure. Not raised while Aura is briefly unavailable or busy with another operation: the backup is retried |
| `AuraDatabaseRestoreStarted`, `AuraDatabaseRestoreCompleted` | Normal | Per-database restore started / submitted |
| `AuraDatabaseRestoreFailed` | Warning | Per-database restore failed. Once per failure |
| `AuraMemberRoleUpdated`, `AuraMemberReady`, `AuraMemberRemoved` | Normal | Organization/project member role set or member added/removed, reconciled (`AuraMemberReady` once when the membership becomes Ready), or removed on CR deletion |
| `AuraMemberNotFound` | Warning | The person is not an organization member; invite them with an `AuraInvite`. Once per wait |
| `AuraMemberFailed` | Warning | Member reconcile failed, or a `deletionPolicy: Delete` deletion waits for credentials it cannot read (`CredentialsUnavailable`) or on a delete Aura refused (`DeletionRefused`). Once per failure |
| `AuraMemberOrphaned` | Normal | A member's deletion left them in the organization or project (`deletionPolicy: Orphan`). A Warning when `deletionPolicy: Delete` could not run because the Aura credentials, organization or project stayed unavailable for 5 minutes after deletion was requested, or Aura kept refusing the delete for 5 minutes (anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last): the person keeps their role, so remove them in the Aura console |
| `AuraInviteCreated`, `AuraInviteAdopted`, `AuraInviteReady`, `AuraInviteDeleted`, `AuraInviteOrphaned` | Normal | Lifecycle of the invite (`AuraInviteReady` names the invite's state, once per state: sent, accepted, declined, expired, revoked). `AuraInviteOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials or organization stayed unavailable for 5 minutes after deletion was requested, or Aura kept refusing the delete for 5 minutes (anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last): the invite stays pending and can still be accepted, so revoke it in the Aura console |
| `AuraInviteFailed` | Warning | Invite reconcile failed, or a `deletionPolicy: Delete` deletion waits for credentials it cannot read (`CredentialsUnavailable`) or on a delete Aura refused (`DeletionRefused`). Once per failure |

## Using Events in Alerting

Events can drive Alertmanager rules via the `kube-state-metrics` `kube_event_*` metrics, or you can use the [Kubernetes Event Exporter](https://github.com/resmoio/kubernetes-event-exporter) to forward events to external systems.

Example: alert on any `BackupFailed` event:

```yaml
# Using kubernetes-event-exporter config
route:
  routes:
    - match:
        reason: "BackupFailed"
      receivers:
        - slack
```

Example Alertmanager rule via kube-state-metrics:

```yaml
- alert: Neo4jBackupFailed
  expr: |
    kube_event_unique_events_total{
      reason="BackupFailed",
      namespace=~"neo4j-.*"
    } > 0
  for: 0m
  labels:
    severity: critical
  annotations:
    summary: "Neo4j backup failed in {{ $labels.namespace }}"
```

## Event Retention

Kubernetes events are stored in etcd and deleted after a configurable TTL (default: 1 hour). For long-term event retention, deploy the [Kubernetes Event Exporter](https://github.com/resmoio/kubernetes-event-exporter) or use a log aggregation tool (Loki, Elasticsearch) that captures event logs from the API server.

To check the current event TTL on your cluster:

```bash
kubectl get pods -n kube-system -l component=kube-apiserver -o jsonpath='{.items[0].spec.containers[0].command}' | tr ',' '\n' | grep event-ttl
```
