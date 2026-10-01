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

### Cluster Lifecycle

| Reason | Type | Description |
|---|---|---|
| `ClusterFormationStarted` | Normal | Cluster formation has begun (first time entering Forming phase) |
| `ClusterFormationFailed` | Warning | Cluster formation verification failed |
| `ClusterReady` | Normal | Cluster has reached Ready phase |
| `ValidationFailed` | Warning | Cluster spec validation failed |
| `TopologyWarning` | Warning | Topology validation produced warnings |
| `TopologyPlacementCalculated` | Normal | Topology placement constraints calculated successfully |
| `TopologyPlacementFailed` | Warning | Topology placement constraint calculation failed |
| `TopologyZoneDiscoveryDegraded` | Warning | Availability-zone auto-discovery is unavailable (the operator cannot list cluster-scoped nodes, as in namespace-scoped installs); best-effort zone spread is applied via the topology key — set `spec.topology.availabilityZones` to enumerate zones explicitly |
| `PropertyShardingValidationFailed` | Warning | Property sharding configuration validation failed |
| `ServerRoleValidationFailed` | Warning | Server role hint validation failed |
| `RouteAPINotFound` | Warning | OpenShift Route API not available in cluster |
| `MCPApocMissing` | Warning | MCP server requires APOC plugin which is not installed |
| `ConnectivityDegraded` | Warning | The operator has failed to reach the cluster's Bolt endpoint for a sustained streak of consecutive reconciles; emitted once when the streak threshold is reached, with the last error and any pod issues |
| `ReconcileFailed` | Warning | Reconciliation loop encountered an unrecoverable error |

### Scale-down

| Reason | Type | Description |
|---|---|---|
| `ScaleDownDraining` | Normal | A scale-down step ran: servers cordoned, their databases deallocated, or the drained servers dropped |
| `ScaleDownBlocked` | Warning | Scale-down refused or stuck — the target is below the `system` database's minimum voting members, or the `DEALLOCATE` dry-run / `DROP SERVER` failed; replicas are held |

### Rolling Upgrades

| Reason | Type | Description |
|---|---|---|
| `UpgradeStarted` | Normal | Rolling upgrade initiated |
| `UpgradeCompleted` | Normal | Rolling upgrade finished successfully |
| `UpgradePaused` | Normal | Upgrade paused (e.g., due to unhealthy pods) |
| `UpgradeFailed` | Warning | Upgrade failed |
| `UpgradeDeferred` | Normal | An image change was held back — the cluster is not yet `Ready`, or a scale-down drain is in progress — and will be performed by the rolling-upgrade state machine afterwards |

### Backups and Restores

| Reason | Type | Description |
|---|---|---|
| `BackupScheduled` | Normal | Backup CronJob created |
| `BackupStarted` | Normal | Backup job has started |
| `BackupCompleted` | Normal | Backup job completed successfully |
| `BackupFailed` | Warning | Backup job failed |
| `RestoreStarted` | Normal | Restore operation has started |
| `RestoreCompleted` | Normal | Restore operation completed |
| `RestoreFailed` | Warning | Restore operation failed |
| `RestoreFromChainParent` | Warning | `source.backupRef` points at a FULL+DIFF chain parent; the restore seeds from its latest full snapshot, not the latest chain state |
| `DatabaseCreateFailed` | Warning | Database creation failed during a restore operation |
| `BackupRetentionCaveat` | Warning | Retention pruning is configured on a CR whose chain may contain differential artifacts; pruning can orphan DIFFs whose parent FULL ages out (prefer `backupType: FULL` with retention) |
| `BackupShardedDatabasesExcluded` | Warning | An all-databases backup captured property-sharded database(s) as per-shard artifacts; an all-databases restore does not recreate them — restore each from the same backup via its `Neo4jShardedDatabase` CR with `spec.seedBackupRef` |
| `BackupValidateUnsupported` | Warning | `options.validate` has no effect: `neo4j-admin backup validate` needs a CalVer (2025.x+) image |
| `ServiceAccountAnnotationConflict` | Warning | A backup/restore CR overwrote different workload-identity annotations on the namespace's shared `neo4j-backup-sa` / `neo4j-restore-sa`; the last CR reconciled wins and the others' cloud access breaks |
| `SeedEndpointNotProjected` | Warning | A cluster restore's server pods lack the cloud credentials or custom S3 endpoint (`AWS_ENDPOINT_URL_S3`) the seed fetch needs, and the cluster is not opted in to `neo4j.com/auto-inherit-seed-creds`; the restore goes `Failed` with the missing items named |
| `RestoreSeedProgress` | Normal | One poll of an all-databases restore's per-database seed (emitted on every non-terminal poll; the apiserver aggregates repeats into a count with first/last timestamps, which shows whether a seed is progressing or stuck) |
| `RestoreShardedDatabasesNotCovered` | Warning | An all-databases restore's source backup recorded property-sharded databases it does not recreate; restore each via its `Neo4jShardedDatabase` CR |

### Databases

| Reason | Type | Description |
|---|---|---|
| `DatabaseReady` | Normal | Database is created and online |
| `DatabaseDeleted` | Normal | Database was dropped |
| `DatabaseCreatedFromSeed` | Normal | Database created from a seed URI |
| `CreationFailed` | Warning | Database creation failed |
| `DeletionFailed` | Warning | Database deletion failed |
| `DataImported` | Normal | Initial data imported successfully |
| `DataImportFailed` | Warning | Initial data import failed |
| `DataSeeded` | Normal | Database seeded from URI |
| `SeedCredsMissing` | Warning | The seed credentials Secret (`spec.seedCredentials.secretRef`) is not projected onto the hosting cluster/standalone's `spec.extraEnvFrom`; the message carries a copy-pasteable fix (also emitted for `Neo4jShardedDatabase`) |
| `SeedCredsAutoInherited` | Normal | The hosting cluster/standalone carries `neo4j.com/auto-inherit-seed-creds: "true"`, so the operator patched its `spec.extraEnvFrom` with the seed credentials Secret and is waiting for the rolling restart (also emitted for `Neo4jShardedDatabase`) |
| `ValidationWarning` | Warning | Database spec produced validation warnings |
| `ClusterNotFound` | Warning | Referenced cluster or standalone not found |
| `ClusterNotReady` | Warning | Referenced cluster is not yet Ready |
| `ConnectionFailed` | Warning | Could not connect to Neo4j via Bolt |
| `ClientCreationFailed` | Warning | Failed to create Neo4j Bolt client for the cluster |

### Plugins

| Reason | Type | Description |
|---|---|---|
| `PluginInstalled` | Normal | Plugin successfully installed |
| `PluginInstallFailed` | Warning | Plugin installation failed |
| `PluginDuplicate` | Warning | Another `Neo4jPlugin` already targets this name (only the first owner reconciles; duplicates sit in `Failed` until the conflict is resolved) |

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
| `AuraFleetManagementFailed` | Warning | Aura Fleet Management registration or operation failed |
| `AuraFleetManagementPluginPatchFailed` | Warning | Failed to patch the fleet-management plugin onto the StatefulSet |
| `AuraFleetDeploymentCreated` | Normal | Operator-driven provisioning (`spec.auraFleetManagement.provision`) registered a Fleet Manager deployment in Aura |
| `AuraFleetDeploymentAdopted` | Normal | Provisioning adopted an existing Aura fleet deployment of the same name instead of creating one |
| `AuraFleetDeploymentDeleted` | Normal | The Aura fleet deployment was unregistered |
| `AuraFleetTokenProvisioned` | Normal | The fleet registration token was stored in a Secret |
| `AuraFleetTokenRotated` | Warning | The fleet token was rotated; any previous registration is now invalid |

### Storage Expansion

| Reason | Type | Description |
|---|---|---|
| `StorageExpansionStarted` | Normal | PVC expansion has begun after detecting `spec.storage.size` increase |
| `StorageExpansionCompleted` | Normal | All PVCs expanded and StatefulSet recreated successfully |
| `StorageExpansionFailed` | Warning | Expansion failed (non-expandable StorageClass, shrink attempt, or patch error) |
| `StorageClassNotFound` | Warning | `spec.storage.className` references a StorageClass that does not exist in the cluster |

### Sharded Databases

| Reason | Type | Description |
|---|---|---|
| `ShardedDatabaseReady` | Normal | Sharded database is created and all shards are online |
| `ShardedDatabaseDropped` | Normal | An existing sharded database was dropped before being recreated from a seed (`spec.replaceExisting` with `spec.force`) |
| `SeedBackupPending` | Normal | `spec.seedBackupRef` names a `Neo4jBackup` with no Succeeded run yet; the sharded database stays `Pending` and requeues |
| `SeedBackupResolutionFailed` | Warning | `spec.seedBackupRef` could not be resolved into a concrete seed |
| `SeedProxyStarting` | Normal | Waiting for the PVC `backup-seed-proxy` Deployment to become Ready before seeding |

### Users, Roles and Role Bindings

Emitted on `Neo4jUser`, `Neo4jRole` and `Neo4jRoleBinding` objects.

| Reason | Type | Description |
|---|---|---|
| `UserCreated` | Normal | User created in Neo4j |
| `UserUpdated` | Normal | User updated (or its password recorded as already applied) |
| `UserReady` | Normal | User exists and its roles are in sync |
| `UserDeleted` | Normal | User dropped |
| `UserDeletionFailed` | Warning | `DROP USER` failed; retried, and the finalizer is released if it keeps failing so deletion is not wedged |
| `UserSyncFailed` | Warning | Reconciling the user against Neo4j failed |
| `PasswordRotated` | Normal | Password updated from the referenced Secret |
| `RolesGranted` / `RolesRevoked` | Normal | Roles granted to / revoked from a user, role binding or auth rule |
| `RolesResolved` | Normal | `Neo4jRole` CR names were resolved to Neo4j role names |
| `RolePending` | Warning | Waiting for referenced roles to exist |
| `RoleCreated` | Normal | Role created in Neo4j |
| `RoleReady` | Normal | Role exists and its privileges are in sync |
| `RoleDeleted` | Normal | Role dropped |
| `RoleDeletionFailed` | Warning | `DROP ROLE` failed; retried, finalizer released if it keeps failing |
| `RoleSyncFailed` | Warning | Reconciling the role against Neo4j failed |
| `PrivilegesApplied` | Normal | Privileges were added to and/or revoked from the role |
| `PrivilegesDriftKept` | Warning | A drifting privilege was not revoked (it is immutable, or no `REVOKE` could be derived) |
| `PrivilegeNamesUnknownDatabase` | Warning | A privilege names a database that does not exist on the cluster and is not an alias for one (it is skipped), or grants graph privileges on a composite database (accepted by Neo4j but inert); see `PrivilegesResolve` on the role |
| `PrivilegeTargetDropped` | Warning | A `Neo4jDatabase` was dropped while a `Neo4jRole` on the same cluster still grants on it; emitted on both objects |
| `BindingCreated` / `BindingUpdated` / `BindingDeleted` | Normal | `Neo4jRoleBinding` granted its roles, changed the granted set, or revoked its roles on deletion |
| `BindingFailed` | Warning | A role binding grant or revoke failed |
| `UserNotFound` | Warning | The user a `Neo4jRoleBinding` names does not exist |

### Auth Rules

| Reason | Type | Description |
|---|---|---|
| `AuthRuleCreated` / `AuthRuleUpdated` / `AuthRuleDeleted` | Normal | `Neo4jAuthRule` created, its condition or enabled flag updated, or the rule dropped |
| `AuthRuleFailed` | Warning | Creating, updating or dropping the auth rule failed |
| `AuthRuleVersionTooOld` | Warning | Auth rules need Neo4j 2026.03 or later on the referenced cluster |
| `OIDCProviderNotConfigured` | Warning | The referenced cluster's `spec.config` lists no OIDC provider for attribute-based access control; the rule stays `Pending` |

### Cross-Cluster Replication

| Reason | Type | Description |
|---|---|---|
| `ReplicationSourceCaveat` | Normal / Warning | On a `Neo4jBackup` with `mode: replication-source`: Normal when the pull URI is published (with the reminder that bucket lifecycle rules can break the chain); Warning when the storage type (e.g. `pvc`) has no cross-cluster URI form |
| `ReplicaCreated` | Normal | Replica database created |
| `ReplicaReady` | Normal | Replica database is online |
| `ReplicaFailed` | Warning | Creating the replica (or dropping it on deletion) failed |
| `ReplicaDropped` | Normal | Replica database dropped on CR deletion |
| `ReplicaVersionTooOld` | Warning | The downstream cluster predates Neo4j 2026.08 |
| `ReplicaPromotionDetected` | Warning | The database is no longer a replica (promoted, possibly out of band); the CR goes inert |
| `ReplicaRetainedAfterPromotion` | Warning | The CR was deleted after promotion; the promoted database is retained, not dropped |
| `ReplicaPromotionStarted` | Warning | A `Neo4jReplicaPromotion` started; irreversible, and any outstanding lag becomes permanent data loss |
| `ReplicaPromotionCompleted` | Normal | Promotion completed |
| `ReplicaPromotionFailed` | Warning | Promotion failed |
| `CrossClusterProxyUnauthenticated` | Warning | The CCDR proxy was published while `spec.tls.strictPeerValidation` is disabled, so the exposed tx-shipping port is encrypted but not authenticated |

The replica's `UpstreamClusterNotFound`, `UpstreamClusterNotReady`, `UpstreamBackupNotFound` and `UpstreamBackupNotReady` are not events: they are the reason on the replica's `Ready` condition (phase `Pending`) while `source.upstreamClusterRef` / `source.upstreamBackupRef` cannot be resolved yet — read them with `kubectl describe neo4jreplicadatabase`.

### Composite Databases and Aliases

| Reason | Type | Description |
|---|---|---|
| `CompositeDatabaseCreated` | Normal | Composite database created |
| `CompositeDatabaseReady` | Normal | Composite database is ready, with its observed constituent count |
| `CompositeDatabaseFailed` | Warning | Reconciling the composite database failed |
| `CompositeDatabaseDropped` | Normal | Composite dropped (constituent aliases removed; their target databases kept) |
| `CompositeConstituentAdded` | Normal | A constituent alias now resolves to its target |
| `CompositeConstituentRetargeted` | Normal | A constituent alias was re-pointed to a different target |
| `CompositeConstituentRemoved` | Normal | A constituent not in `spec.constituents` was removed |
| `CompositeDatabaseNameBlocked` | Warning | A dotted alias already occupies the composite's namespace, so the composite cannot be created until that alias is dropped (`kubectl neo4j explain CompositeDatabaseNameBlocked`) |
| `AliasCreated` | Normal | Database alias created |
| `AliasRetargeted` | Normal | Alias re-pointed to a different target database |
| `AliasReady` | Normal | Alias resolves to its target |
| `AliasDropped` | Normal | Alias dropped |
| `AliasFailed` | Warning | Reconciling or dropping the alias failed |

### Aura Orchestration

Emitted by the Aura CRDs (`AuraProviderConfig`, `AuraInstance`, `AuraSnapshot`, `AuraRestore`, `AuraCustomerManagedKey`, `AuraIPFilter`, `AuraDatabase`, `AuraDatabaseBackup`, `AuraDatabaseRestore`, `AuraOrganizationMember`, `AuraProjectMember`, `AuraInvite`). "Adopted" means the CR took over an existing Aura resource; "Orphaned" means the CR was deleted but the Aura resource was left in place.

| Reason | Type | Description |
|---|---|---|
| `AuraCredentialsValidated` / `AuraCredentialsInvalid` | Normal / Warning | The provider config's Aura API credentials were accepted / rejected |
| `AuraInstanceCreated`, `AuraInstanceAdopted`, `AuraInstanceUpdated`, `AuraInstancePaused`, `AuraInstanceResumed`, `AuraInstanceUpgraded`, `AuraInstanceDeleted`, `AuraInstanceOrphaned` | Normal | Lifecycle of the Aura instance |
| `AuraInstanceFailed` | Warning | Aura instance reconcile failed (also: deletion refused while `deletionProtection` is set) |
| `AuraSnapshotCreated` | Normal | Aura snapshot requested |
| `AuraRestoreStarted`, `AuraRestoreCompleted` | Normal | Instance restore from a snapshot started / completed |
| `AuraRestoreFailed` | Warning | The instance entered a failed status during restore |
| `AuraCustomerManagedKeyCreated`, `AuraCustomerManagedKeyAdopted`, `AuraCustomerManagedKeyReady`, `AuraCustomerManagedKeyDeleted`, `AuraCustomerManagedKeyOrphaned` | Normal | Lifecycle of the customer-managed key |
| `AuraCustomerManagedKeyDeleteBlocked` | Warning | The key cannot be deleted: still in use by one or more instances |
| `AuraCustomerManagedKeyFailed` | Warning | Customer-managed key reconcile failed |
| `AuraIPFilterCreated`, `AuraIPFilterAdopted`, `AuraIPFilterUpdated`, `AuraIPFilterReady`, `AuraIPFilterDeleted`, `AuraIPFilterOrphaned` | Normal | Lifecycle of the IP filter |
| `AuraIPFilterFailed` | Warning | IP filter reconcile failed |
| `AuraDatabaseCreated`, `AuraDatabaseAdopted`, `AuraDatabaseReady`, `AuraDatabaseDeleted`, `AuraDatabaseOrphaned` | Normal | Lifecycle of the Aura database (`AuraDatabaseCreated` is a Warning when the instance already had databases: a pre-existing one cannot be adopted by name, so verify no duplicate was created) |
| `AuraDatabaseFailed` | Warning | Aura database reconcile failed |
| `AuraDatabaseBackupCreated` | Normal | Per-database backup created |
| `AuraDatabaseBackupFailed` | Warning | Per-database backup failed |
| `AuraDatabaseRestoreStarted`, `AuraDatabaseRestoreCompleted` | Normal | Per-database restore started / submitted |
| `AuraDatabaseRestoreFailed` | Warning | Per-database restore failed |
| `AuraMemberRoleUpdated`, `AuraMemberReady`, `AuraMemberRemoved` | Normal | Organization/project member role set or member added/removed, reconciled, or removed on CR deletion |
| `AuraMemberNotFound` | Warning | The person is not an organization member; invite them with an `AuraInvite` |
| `AuraMemberFailed` | Warning | Member reconcile failed |
| `AuraInviteCreated`, `AuraInviteAdopted`, `AuraInviteReady`, `AuraInviteDeleted`, `AuraInviteOrphaned` | Normal | Lifecycle of the invite |
| `AuraInviteFailed` | Warning | Invite reconcile failed |

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
