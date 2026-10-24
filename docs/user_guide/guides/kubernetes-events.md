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
| `ClusterReady` | Normal | Cluster has reached Ready phase |
| `ValidationFailed` | Warning | The spec failed validation; the message names what to fix and is also in `status.message`. Emitted when the failure starts or its message changes, not on every retry while the spec stays invalid (a cluster or replica also announces it again after an edit that leaves the spec invalid). Also emitted by `Neo4jEnterpriseStandalone`, `Neo4jDatabase`, `Neo4jShardedDatabase`, `Neo4jCompositeDatabase`, `Neo4jDatabaseAlias`, `Neo4jReplicaDatabase`, `Neo4jPlugin`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding` and `Neo4jAuthRule` |
| `TopologyWarning` | Warning | Topology validation produced warnings |
| `ValidationWarning` | Warning | A spec field the schema accepts but nothing reads is set (`spec.<path> is accepted but has no effect today: <what to do instead>`), or `spec.storage.className` names a StorageClass the existing data volumes do not use (a volume keeps the class it was created with; the message names each volume and its class). Advisory: the resource is still reconciled. Also emitted by `Neo4jEnterpriseStandalone` (there also for `spec.upgradeStrategy` `upgradeTimeout`, `healthCheckTimeout` or `stabilizationTimeout` other than their defaults), `Neo4jBackup`, `Neo4jRestore`, `Neo4jPlugin`, `Neo4jDatabase` and `AuraInstance` |
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
| `ReconcileFailed` | Warning | A reconcile pass of a `Neo4jEnterpriseStandalone` or `Neo4jShardedDatabase` failed; the resource is `Failed` and retried with backoff. Once per failure |
| `ConfigAppliedLive` | Normal | A `spec.config` change touched only settings Neo4j can change at runtime, so the operator applied it to every running server (`dbms.setConfigValue`) and wrote it to `neo4j.conf`; nothing restarted. Names the settings. Also emitted by `Neo4jEnterpriseStandalone` |
| `TLSCertificateReloaded` | Normal | cert-manager renewed the TLS certificate and the operator reloaded it on the named servers (`dbms.security.reloadTLS()`, CalVer 2025.03+); no restart. Also emitted by `Neo4jEnterpriseStandalone` |
| `TLSCertificateNeedsRestart` | Warning | A renewed TLS certificate could not be reloaded on the named servers: TLS reload is not enabled on them yet (they get it at their next restart), or they still presented the old certificate ten minutes after the new one was issued. They serve the previous certificate until they restart. Also emitted by `Neo4jEnterpriseStandalone` |
| `ConfigNeedsRestart` | Normal | A `neo4j.conf` change could not be applied at runtime, so the servers restart as before. The message names the settings and the reason: a setting Neo4j reads only at startup, one also set through an environment variable, or the deployment not `Ready` with every server up. Also emitted by `Neo4jEnterpriseStandalone` |

### Scaling

| Reason | Type | Description |
|---|---|---|
| `ScaledWithoutRestart` | Normal | A scale-up added only servers that join as `system` secondaries, so the running servers were not restarted; they read the longer discovery list at their next restart. A scale-up that adds a system primary (with the default, from 2 servers to 3+) restarts them instead and raises no such event |
| `ScaleDownDraining` | Normal | A scale-down step ran: servers cordoned, their databases deallocated, or the drained servers dropped |
| `ScaleDownBlocked` | Warning | Scale-down refused or stuck — the target is below the `system` database's minimum voting members, or the `DEALLOCATE` dry-run / `DROP SERVER` failed; replicas are held |

### Rolling Upgrades

| Reason | Type | Description |
|---|---|---|
| `UpgradeStarted` | Normal | Rolling upgrade initiated, or a `Paused` or `Failed` one resumed ("Rolling upgrade resumed after it was …"). A resumed one whose restarted servers cannot start on the earlier target names them: "… cannot start on … and is replaced first" |
| `UpgradeCompleted` | Normal | Rolling upgrade finished successfully |
| `UpgradePaused` | Warning | Upgrade paused after a failed check or step (`autoPauseOnFailure`); the servers stay where it stopped until you change `spec.image.tag` or turn `autoPauseOnFailure` off. A check that refuses the upgrade before it starts is announced once, not on every attempt |
| `UpgradeFailed` | Warning | Upgrade failed. A check that refuses it before it starts is announced once, and again only with a new error, while the operator tries it every minute; the cluster stays `Ready`. On a `Neo4jEnterpriseStandalone` with `upgradeStrategy.preUpgradeHealthCheck` and `autoPauseOnFailure`, also: the pre-upgrade health check failed and holds the upgrade back — advisory while it stays blocked |
| `UpgradeDeferred` | Normal | An image change was held back — the cluster is not yet `Ready` (announced once the hold outlasts one reconcile), or a scale-down drain is in progress (once per target) — and will be performed by the rolling-upgrade state machine afterwards. Once per held target, not on every check (a drain's deferral names the target, and each drain announces it once) |

### Backups and Restores

| Reason | Type | Description |
|---|---|---|
| `BackupScheduled` | Normal | The backup's CronJob is armed: first created, back from another phase (`Suspended`, `Waiting`, …), or its schedule changed. The message names the CronJob and the schedule. Not repeated while the schedule stays as it is |
| `BackupStarted` | Normal | Backup job has started |
| `BackupCompleted` | Normal | Backup job completed successfully |
| `BackupFailed` | Warning | Backup job failed, or the spec is invalid (phase `Invalid`, once per refusal) |
| `RestoreStarted` | Normal | Restore operation has started. A standalone restore that runs offline with `neo4j-admin` (a point in time, a `source.type: storage` path) first says why, once its checks have passed — never for a restore that is then refused: with `stopCluster: true` when it stops the instance, with `stopCluster: false` (saying the standalone is already stopped) when the restore Job starts, so a restore that waits in `Pending` first does not say it twice. "Restore job … started" is emitted when the restore turns `Running`, once. Each database of an all-databases restore has its own, from the source `neo4j-restore-progress` |
| `RestoreCompleted` | Normal | Restore operation completed. Also once at the end of an all-databases restore ("All-databases restore completed: Restored N databases") |
| `RestoreFailed` | Warning | Restore operation failed. Also once at the end of an all-databases restore in which a database failed |
| `RestoreFromChainParent` | Warning | `source.backupRef` points at a FULL+DIFF chain parent; the restore seeds from its latest full snapshot, not the latest chain state. Advisory, so a restore that waits (for seed credentials, a rollout, the seed proxy) does not repeat it on every check |
| `DatabaseCreateFailed` | Warning | Database creation failed during a restore operation |
| `BackupRetentionCaveat` | Warning | Retention pruning is configured on a CR whose chain may contain differential artifacts; pruning can orphan DIFFs whose parent FULL ages out (prefer `backupType: FULL` with retention) |
| `BackupShardedDatabasesExcluded` | Warning | An all-databases backup captured property-sharded database(s) as per-shard artifacts; an all-databases restore does not recreate them — restore each from the same backup via its `Neo4jShardedDatabase` CR with `spec.seedBackupRef`. Once per run, when the run is recorded in `status.history` |
| `BackupShardedDifferential` | Warning | A run wrote differential backups of sharded-database shards to a PVC; no sharded database can be seeded from that run. Comes from an all-databases backup; a `spec.shardedDatabase` backup takes full backups on a PVC. Once per run, when the run is recorded in `status.history` |
| `BackupValidateUnsupported` | Warning | `options.validate` has no effect: `neo4j-admin backup validate` needs a CalVer (2025.x+) image. Advisory |
| `ServiceAccountAnnotationConflict` | Warning | A backup/restore CR overwrote different workload-identity annotations on the namespace's shared `neo4j-backup-sa` / `neo4j-restore-sa`; the last CR reconciled wins and the others' cloud access breaks. On a `Neo4jBackup` it is advisory: two backups that disagree overwrite each other on every pass, and each says so once |
| `SeedEndpointNotProjected` | Warning | An online restore's server pods (cluster or standalone) lack the cloud credentials or custom S3 endpoint (`AWS_ENDPOINT_URL_S3`) the seed fetch needs, and the target is not opted in to `neo4j.com/auto-inherit-seed-creds`. The restore waits in `Pending` with the missing items named, and carries on once the target has them or is opted in. Emitted when the restore starts waiting (or what it waits for changes), once |
| `RestoreSeedProgress` | Normal | The progress of one database's seed in an all-databases restore (allocations online, or the last poll's error). Emitted when it changes and every 10 minutes while it does not; the API server aggregates the repeats into a count with first/last timestamps, which shows whether a seed is progressing or stuck. From the source `neo4j-restore-progress`, which has an event budget of its own (client-go: a burst of 25 per source, object and type, then one every five minutes), so a long seed cannot crowd out the restore's `RestoreCompleted` or `RestoreFailed`. The per-database record is `status.databaseResults` |
| `RestoreShardedDatabasesNotCovered` | Warning | An all-databases restore's source backup recorded property-sharded databases it does not recreate; restore each via its `Neo4jShardedDatabase` CR. Advisory |

### Databases

| Reason | Type | Description |
|---|---|---|
| `DatabaseReady` | Normal | Database became ready: created and online, or back from a failure. Not repeated while it stays ready |
| `DatabaseDeleted` | Normal | Database was dropped |
| `DatabaseCreatedFromSeed` | Normal | Database created from a seed URI |
| `CreationFailed` | Warning | Database creation failed; the database is `Failed` and retried with backoff. Once per failure |
| `DeletionFailed` | Warning | `DROP DATABASE` failed while deleting the `Neo4jDatabase`. The drop is retried (the database is phase `Error` with the failure in `status.message`) until it succeeds or five minutes have passed since the deletion was requested, then the finalizer is released. Once per failure, not on every retry; a second event says when the finalizer is released |
| `DataImported` | Normal | Initial data imported successfully |
| `DataImportFailed` | Warning | Initial data import failed; retried with backoff. Once per failure |
| `DataSeeded` | Normal | Database seeded from URI |
| `SeedCredsMissing` | Warning | The seed credentials Secret (`spec.seedCredentials.secretRef`) is not projected onto the hosting cluster/standalone's `spec.extraEnvFrom` (for a `Neo4jShardedDatabase`'s cloud `seedBackupRef`: the backup's credentials Secret or custom S3 endpoint on the cluster's server pods); the message carries a copy-pasteable fix. The database waits in phase `Pending` and is created by itself once the host has them — nothing is asked of Neo4j before. Once per wait. Also emitted for `Neo4jShardedDatabase` |
| `SeedCredsAutoInherited` | Normal | The hosting cluster/standalone carries `neo4j.com/auto-inherit-seed-creds: "true"`, so the operator patched its `spec.extraEnvFrom` with the seed credentials Secret and is waiting (phase `Pending`) for the rolling restart (also emitted for `Neo4jShardedDatabase`) |
| `ValidationWarning` | Warning | Database spec produced validation warnings, including a set field that nothing reads (`spec.initialData.configMapRef`, `secretRef`, `storage`, or a `source` other than `cypher`) |
| `ClusterNotFound` | Warning | Referenced cluster or standalone not found; the database waits in phase `Pending` and carries on once it exists. Once per wait. Also emitted by `Neo4jShardedDatabase` (for a cluster) |
| `ClusterNotReady` | Warning | On a `Neo4jShardedDatabase`: the referenced cluster cannot host it — property sharding needs Neo4j 2025.12+ and `spec.propertySharding.enabled` on the cluster; phase `Failed`. Once per failure. A cluster that is simply not Ready yet is phase `Waiting`, with no event |
| `ConnectionFailed` | Warning | Could not connect to Neo4j via Bolt; retried with backoff. Once per failure. Emitted by `Neo4jReplicaDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jCompositeDatabase` and `Neo4jDatabaseAlias` |
| `ClientCreationFailed` | Warning | Failed to create Neo4j Bolt client for the cluster (`Neo4jShardedDatabase`); retried with backoff. Once per failure |

### Plugins

| Reason | Type | Description |
|---|---|---|
| `PluginInstalled` | Normal | Plugin successfully installed |
| `PluginInstallFailed` | Warning | Plugin installation failed (once per failure; retried every 30s without repeating it). A server that is restarting or briefly unreachable is not a failure: the plugin stays `Installing` |
| `PluginMissing` | Warning | A `Ready` plugin's target StatefulSet no longer carries what the plugin set (its `NEO4J_PLUGINS` entry, a variable it sets, a `VerifiedDownload` init container or its volumes); the plugin goes back to `Installing` and restores it, which restarts the servers — or, while the target is not `Ready` with every server available, says so and is `Waiting` until it is |
| `PluginInstallDeferred` | Normal | Installing the plugin — or a `VerifiedDownload` plugin's update of its trusted CAs after the target's `spec.trustedCASecrets` changed — would restart the servers of a target that is not `Ready` with every server available (a cluster `Degraded`, or forming, rolling or upgrading); the plugin is `Waiting` and writes once the target is. Once per wait |
| `PluginDuplicate` | Warning | Another `Neo4jPlugin` already targets this name (only the first owner reconciles; duplicates sit in `Failed` until the conflict is resolved). Once per conflict |

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
| `AuraFleetManagementPluginPatchFailed` | Warning | Failed to patch the fleet-management plugin onto the StatefulSet. Not raised while the server StatefulSet does not exist yet (a new deployment's first pass): the pass that finds it adds the plugin |
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
| `ShardedDatabaseReady` | Normal | Sharded database became ready (created and all shards online), or was recreated by `spec.replaceExisting`. Not repeated while it stays ready |
| `ShardedDatabaseDropped` | Normal | An existing sharded database was dropped before being recreated from a seed (`spec.replaceExisting` with `spec.force`) |
| `SeedBackupPending` | Normal | `spec.seedBackupRef` names a `Neo4jBackup` with no Succeeded run yet; the sharded database stays `Pending` and requeues. Emitted when the wait starts, not on every retry |
| `SeedBackupResolutionFailed` | Warning | `spec.seedBackupRef` could not be resolved into a concrete seed (the `Neo4jBackup` is missing, or its storage or run cannot seed a sharded database); phase `Failed`. Once per failure |
| `SeedProxyStarting` | Normal | Waiting for the PVC `backup-seed-proxy` Deployment to become Ready before seeding. Emitted when the wait starts, not on every retry |

A `Neo4jShardedDatabase` also emits `ClusterNotFound` (the cluster does not exist yet: phase `Pending`), `ClusterNotReady`, `ClientCreationFailed`, `ReconcileFailed`, `SeedCredsMissing` (phase `Pending`) and `SeedCredsAutoInherited`, described above.

### Users, Roles and Role Bindings

Emitted on `Neo4jUser`, `Neo4jRole` and `Neo4jRoleBinding` objects.

| Reason | Type | Description |
|---|---|---|
| `UserCreated` | Normal | User created in Neo4j |
| `UserUpdated` | Normal | User updated (or its password recorded as already applied) |
| `UserReady` | Normal | User exists and its roles are in sync. When the user becomes `Ready` — first, or back from a failure of its own — not on a restart of its target: a `Ready` user stays `Ready` (with `ClusterNotReady=True`) while the target is down |
| `UserDeleted` | Normal | User dropped |
| `UserDeletionFailed` | Warning | `DROP USER` failed; the user is `Failed` with the error in `status.message`, the drop is retried, and the finalizer is released if it keeps failing for 5 minutes so deletion is not wedged. Once per failure, not on every retry |
| `UserSyncFailed` | Warning | Reconciling the user against Neo4j failed. Once per failure |
| `PasswordRotated` | Normal | Password updated from the referenced Secret |
| `RolesGranted` / `RolesRevoked` | Normal | Roles granted to / revoked from a user, role binding or auth rule |
| `RolesResolved` | Normal | `spec.roles` names `Neo4jRole` resources by their CR name, and those were granted as the Neo4j roles they manage; the message maps each, as the `RoleNamesResolved` condition does. Once when the mapping appears or changes, not on every reconcile. Emitted by `Neo4jUser` and `Neo4jRoleBinding` |
| `RolePending` | Warning | Waiting for referenced roles to exist — not built in, not a `Neo4jRole`, not in `SHOW ROLES`; names them (sorted), as the `PendingDependencies` condition does. Once per wait, and again when the set of missing roles changes. Emitted by `Neo4jUser`, `Neo4jRoleBinding` and `Neo4jAuthRule` |
| `RoleCreated` | Normal | Role created in Neo4j |
| `RoleReady` | Normal | Role exists and its privileges are in sync. When the role becomes `Ready`, not on a restart of its target |
| `RoleDeleted` | Normal | Role dropped |
| `RoleDeletionFailed` | Warning | `DROP ROLE` failed; the role is `Failed` with the error in `status.message`, the drop is retried, and the finalizer is released if it keeps failing for 5 minutes. Once per failure, not on every retry |
| `RoleSyncFailed` | Warning | Reconciling the role against Neo4j failed. Once per failure |
| `PrivilegesApplied` | Normal | Privileges were added to and/or revoked from the role. Only grants that created a privilege row count: learn mode re-grants every statement after an alias change on the cluster or a new image, and grants that change nothing are not reported |
| `PrivilegesDriftKept` | Warning | A drifting privilege was not revoked (it is immutable, or no `REVOKE` could be derived). Advisory, one per kept privilege |
| `PrivilegeNamesUnknownDatabase` | Warning | A privilege names a database that does not exist on the cluster and is not an alias for one (it is skipped), or grants graph privileges on a composite database (accepted by Neo4j but inert); see `PrivilegesResolve` on the role |
| `PrivilegeTargetDropped` | Warning | A `Neo4jDatabase` was dropped while a `Neo4jRole` on the same cluster still grants on it; emitted on both objects |
| `BindingCreated` / `BindingUpdated` / `BindingDeleted` | Normal | `Neo4jRoleBinding` granted its roles, changed the granted set, or revoked its roles on deletion. `BindingCreated` is not repeated on a restart of the target |
| `BindingFailed` | Warning | A role binding grant or revoke failed (on deletion: retried, and the finalizer is released if it keeps failing for 5 minutes). Once per failure |
| `UserNotFound` | Warning | The user a `Neo4jRoleBinding` names does not exist; the binding waits in `Pending`. Once per wait |

### Auth Rules

| Reason | Type | Description |
|---|---|---|
| `AuthRuleCreated` / `AuthRuleUpdated` / `AuthRuleDeleted` | Normal | `Neo4jAuthRule` created, its condition or enabled flag updated, or the rule dropped |
| `AuthRuleFailed` | Warning | Creating, updating or dropping the auth rule failed (a failed drop on deletion is retried, and the finalizer released if it keeps failing for 5 minutes). Once per failure |
| `AuthRuleVersionTooOld` | Warning | Auth rules need Neo4j 2026.03 or later on the referenced cluster. Once per failure |
| `OIDCProviderNotConfigured` | Warning | The referenced cluster's `spec.config` lists no OIDC provider for attribute-based access control; the rule stays `Pending`. Once per wait |

### Cross-Cluster Replication

| Reason | Type | Description |
|---|---|---|
| `ReplicationSourceCaveat` | Normal / Warning | On a `Neo4jBackup` with `mode: replication-source`: Normal when the pull URI is published (with the reminder that bucket lifecycle rules can break the chain); Warning when the storage type (e.g. `pvc`) has no cross-cluster URI form (advisory) |
| `ReplicaCreated` | Normal | Replica database created |
| `ReplicaReady` | Normal | Replica database is online |
| `ReplicaFailed` | Warning | Creating the replica (or dropping it on deletion) failed. Once per failure |
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
| `CompositeDatabaseReady` | Normal | Composite database is ready, with its observed constituent count |
| `CompositeDatabaseFailed` | Warning | Reconciling the composite database failed. Once per failure. On deletion, a drop Neo4j refuses is retried for 5 minutes, then a second `CompositeDatabaseFailed` says the finalizer is released and the composite left in Neo4j |
| `CompositeDatabaseDropped` | Normal | Composite dropped (constituent aliases removed; their target databases kept) |
| `CompositeConstituentAdded` | Normal | A constituent alias now resolves to its target |
| `CompositeConstituentRetargeted` | Normal | A constituent alias was re-pointed to a different target |
| `CompositeConstituentRemoved` | Normal | A constituent not in `spec.constituents` was removed |
| `CompositeDatabaseNameBlocked` | Warning | A dotted alias already occupies the composite's namespace, so the composite cannot be created until that alias is dropped (`kubectl neo4j explain CompositeDatabaseNameBlocked`). Once per blocking alias |
| `AliasCreated` | Normal | Database alias created |
| `AliasRetargeted` | Normal | Alias re-pointed to a different target database |
| `AliasReady` | Normal | Alias resolves to its target |
| `AliasDropped` | Normal | Alias dropped |
| `AliasFailed` | Warning | Reconciling or dropping the alias failed. Once per failure |

### Aura Orchestration

Emitted by the Aura CRDs (`AuraProviderConfig`, `AuraInstance`, `AuraSnapshot`, `AuraRestore`, `AuraCustomerManagedKey`, `AuraIPFilter`, `AuraDatabase`, `AuraDatabaseBackup`, `AuraDatabaseRestore`, `AuraOrganizationMember`, `AuraProjectMember`, `AuraInvite`). "Adopted" means the CR took over an existing Aura resource; "Orphaned" means the CR was deleted but the Aura resource was left in place. The `…Ready` events and `AuraCredentialsValidated` are raised when the state is reached, not on each periodic re-check while it holds. A 429, a 5xx or a network failure of a create or a backup is retried without an event, as for every other Aura call.

| Reason | Type | Description |
|---|---|---|
| `AuraCredentialsValidated` / `AuraCredentialsInvalid` | Normal / Warning | The provider config's Aura API credentials were accepted / rejected. Each once per change: valid again after a failure is announced again |
| `AuraInstanceCreated`, `AuraInstanceAdopted`, `AuraInstanceUpdated`, `AuraInstancePaused`, `AuraInstanceResumed`, `AuraInstanceUpgraded`, `AuraInstanceDeleted`, `AuraInstanceOrphaned` | Normal | Lifecycle of the Aura instance. `AuraInstanceOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials stayed unavailable for 5 minutes after deletion was requested: the cloud instance keeps running, so delete it in the Aura console |
| `AuraInstanceFailed` | Warning | Aura instance reconcile failed (also: deletion refused while `deletionProtection` is set, recorded on the `Synced` condition, reason `DeletionProtected`; a `Delete` waiting for credentials it cannot read, reason `CredentialsUnavailable`). Once per failure |
| `AuraSnapshotCreated` | Normal | Aura snapshot requested |
| `AuraRestoreStarted`, `AuraRestoreCompleted` | Normal | Instance restore from a snapshot started / completed |
| `AuraRestoreFailed` | Warning | The instance entered a failed status during restore |
| `AuraCustomerManagedKeyCreated`, `AuraCustomerManagedKeyAdopted`, `AuraCustomerManagedKeyReady`, `AuraCustomerManagedKeyDeleted`, `AuraCustomerManagedKeyOrphaned` | Normal | Lifecycle of the customer-managed key (`AuraCustomerManagedKeyReady` once when the key becomes ready). `AuraCustomerManagedKeyOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials stayed unavailable for 5 minutes: the key stays registered in Aura |
| `AuraCustomerManagedKeyDeleteBlocked` | Warning | The key cannot be deleted: still in use by one or more instances. Once per refusal |
| `AuraCustomerManagedKeyFailed` | Warning | Customer-managed key reconcile failed (also: a `Delete` waiting for credentials, reason `CredentialsUnavailable`). Once per failure |
| `AuraIPFilterCreated`, `AuraIPFilterAdopted`, `AuraIPFilterUpdated`, `AuraIPFilterReady`, `AuraIPFilterDeleted`, `AuraIPFilterOrphaned` | Normal | Lifecycle of the IP filter (`AuraIPFilterReady` once when it becomes Ready). `AuraIPFilterOrphaned` is a Warning when `deletionPolicy: Delete` could not run because the Aura credentials or organization stayed unavailable for 5 minutes: the filter stays in place in Aura |
| `AuraIPFilterFailed` | Warning | IP filter reconcile failed (also: a `Delete` waiting for credentials, reason `CredentialsUnavailable`). Once per failure |
| `AuraDatabaseCreated`, `AuraDatabaseAdopted`, `AuraDatabaseReady`, `AuraDatabaseDeleted`, `AuraDatabaseOrphaned` | Normal | Lifecycle of the Aura database (`AuraDatabaseReady` once when it becomes Ready; `AuraDatabaseCreated` is a Warning when the instance already had databases: a pre-existing one cannot be adopted by name, so verify no duplicate was created) |
| `AuraDatabaseFailed` | Warning | Aura database reconcile failed. Once per failure |
| `AuraDatabaseBackupCreated` | Normal | Per-database backup created |
| `AuraDatabaseBackupFailed` | Warning | Per-database backup failed. Once per failure. Not raised while Aura is briefly unavailable or busy with another operation: the backup is retried |
| `AuraDatabaseRestoreStarted`, `AuraDatabaseRestoreCompleted` | Normal | Per-database restore started / submitted |
| `AuraDatabaseRestoreFailed` | Warning | Per-database restore failed. Once per failure |
| `AuraMemberRoleUpdated`, `AuraMemberReady`, `AuraMemberRemoved` | Normal | Organization/project member role set or member added/removed, reconciled (`AuraMemberReady` once when the membership becomes Ready), or removed on CR deletion |
| `AuraMemberNotFound` | Warning | The person is not an organization member; invite them with an `AuraInvite`. Once per wait |
| `AuraMemberFailed` | Warning | Member reconcile failed. Once per failure |
| `AuraInviteCreated`, `AuraInviteAdopted`, `AuraInviteReady`, `AuraInviteDeleted`, `AuraInviteOrphaned` | Normal | Lifecycle of the invite (`AuraInviteReady` names the invite's state, once per state: sent, accepted, declined, expired, revoked) |
| `AuraInviteFailed` | Warning | Invite reconcile failed. Once per failure |

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
