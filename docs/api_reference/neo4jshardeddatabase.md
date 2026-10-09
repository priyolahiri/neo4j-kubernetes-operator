# Neo4jShardedDatabase API Reference

The `Neo4jShardedDatabase` Custom Resource Definition (CRD) manages sharded databases with property sharding for horizontal scaling of large datasets in Neo4j 2025.12+ clusters.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jShardedDatabase`
- **Supported Neo4j Versions**: 2025.12+ (requires property sharding support)
- **Prerequisites**: Neo4jEnterpriseCluster with `propertySharding.enabled: true`

This document provides detailed API specifications for both Neo4jShardedDatabase and the property sharding configuration in Neo4jEnterpriseCluster.

## Neo4jEnterpriseCluster.propertySharding

Property sharding configuration for Neo4j Enterprise clusters.

### PropertyShardingSpec

```yaml
propertySharding:
  enabled: boolean                    # Required: Enable property sharding support
  config: map[string]string          # Optional: Advanced configuration
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | `boolean` | Yes | - | Enables property sharding support on the cluster |
| `config` | `map[string]string` | No | See below | Advanced property sharding configuration |

#### Default Configuration

When `enabled: true`, these settings are automatically applied:

```yaml
config:
  internal.dbms.sharded_property_database.enabled: "true"
  internal.dbms.sharded_property_database.allow_external_shard_access: "false"
```

The server's `db.query.default_language` is not among them: the sharded
database and its shards get `CYPHER 25` from the `SET DEFAULT LANGUAGE` the
operator always emits on the sharded `CREATE`.

#### Configuration Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `internal.dbms.sharded_property_database.enabled` | string | "true" | Enable property sharding database feature |
| `internal.dbms.sharded_property_database.allow_external_shard_access` | string | "false" | Allow external access to individual shards |
| `db.tx_log.rotation.retention_policy` | string | _(not set by the operator)_ | Transaction log retention policy; Neo4j's own default applies unless you set it here |
| `internal.dbms.sharded_property_database.property_pull_interval` | string | _(not set by the operator)_ | Property synchronization interval; Neo4j's own default applies unless you set it here |

Only the first two keys are defaulted by the operator; any other key you list is passed through to `neo4j.conf`.

### Status Fields

```yaml
status:
  propertyShardingReady: boolean      # Indicates if property sharding is ready
```

| Field | Type | Description |
|-------|------|-------------|
| `propertyShardingReady` | `*bool` | Indicates whether property sharding is configured and operational |

#### Prerequisites for propertyShardingReady=true

1. Cluster phase is `Ready` or `Degraded` (formed and serving with a majority of its servers)
2. Neo4j version is 2025.12+
3. Minimum 2 servers configured (3+ recommended for HA graph shard primaries)
4. Minimum 4GB memory per server (8GB+ recommended for production)
5. Minimum 1 CPU core per server (2+ cores recommended for cross-shard queries)
6. All required configuration applied
7. Authentication configured (admin secret required)

#### Resource Planning Guidelines

**Development Environment:**
```yaml
topology:
  servers: 3
resources:
  requests:
    memory: 4Gi    # Absolute minimum for property sharding
    cpu: 1000m     # Basic operation
  limits:
    memory: 8Gi
    cpu: 2000m
```

**Production Environment:**
```yaml
topology:
  servers: 3      # or 5+ for larger datasets
resources:
  requests:
    memory: 8Gi    # Recommended for production
    cpu: 2000m     # Cross-shard query performance
  limits:
    memory: 8Gi    # Allow headroom for peak loads
    cpu: 4000m     # Handle concurrent operations
```

**High-Performance Production:**
```yaml
topology:
  servers: 7      # Better shard distribution
resources:
  requests:
    memory: 8Gi    # Production performance
    cpu: 4000m     # Maximum throughput
  limits:
    memory: 20Gi   # Peak load handling
    cpu: 6000m     # Burst capability
```

---

## Neo4jShardedDatabase

Manages property-sharded databases on Neo4j Enterprise clusters.

### Neo4jShardedDatabaseSpec

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jShardedDatabase
metadata:
  name: string
spec:
  clusterRef: string                             # Required
  name: string                                   # Required
  defaultCypherLanguage: string                  # Required: "25"
  propertySharding: PropertyShardingConfiguration  # Required
  wait: boolean                                  # Optional: true
  ifNotExists: boolean                          # Optional: *bool, default true
  replaceExisting: boolean                      # Optional: destructive recreate
  force: boolean                                # Optional: confirms replaceExisting
  seedURI: string                               # Optional
  seedURIs: map[string]string                   # Optional
  seedBackupRef: string                         # Optional
  seedSourceDatabase: string                    # Optional
  seedConfig: SeedConfiguration                 # Optional
  seedCredentials: SeedCredentials              # Optional
  txLogEnrichment: string                       # Optional
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clusterRef` | `string` | Yes | - | Reference to Neo4j cluster hosting this sharded database |
| `name` | `string` | Yes | - | Name of the sharded database to create |
| `defaultCypherLanguage` | `string` | Yes | - | Must be "25" for property sharding |
| `propertySharding` | `PropertyShardingConfiguration` | Yes | - | Property sharding configuration |
| `wait` | `boolean` | No | true | Wait for database creation to complete |
| `ifNotExists` | `*boolean` | No | true | Pointer type. When unset (nil) or `true`, creation is idempotent (`CREATE DATABASE ... IF NOT EXISTS`). Set explicitly to `false` to omit the `IF NOT EXISTS` clause — required when paired with `replaceExisting: true` |
| `replaceExisting` | `boolean` | No | false | **Destructive.** Drops and recreates the sharded database from the seed (typically `seedBackupRef`). Runs `DROP DATABASE {name} DESTROY DATA WAIT` before CREATE — all existing data is lost. Requires `force: true`; mutually exclusive with `ifNotExists: true`; requires a seed source |
| `force` | `boolean` | No | false | Confirms the destructive `replaceExisting` operation. The validator rejects `replaceExisting: true` without `force: true` so an accidental flip can't destroy data |
| `seedURI` | `string` | No | - | Seed URI for creating the sharded database |
| `seedURIs` | `map[string]string` | No | - | Seed URIs keyed by shard name |
| `seedBackupRef` | `string` | No | - | Names a `Neo4jBackup` CR (same namespace) whose most-recent Succeeded run seeds this database. Resolved to a concrete seed URI at reconcile time. Mutually exclusive with `seedURI` and `seedURIs`. If the referenced backup has no Succeeded run yet, the database stays in `Pending` and the reconciler requeues. From a PVC, every shard of that run must be a full backup: a run holding a differential shard fails the database until a full run lands ([why](../user_guide/property_sharding.md#restoring-a-sharded-database)). From cloud storage, differentials seed directly; the cluster needs the backup's credentials and endpoint, which the `neo4j.com/auto-inherit-seed-creds: "true"` annotation lets the operator add |
| `seedSourceDatabase` | `string` | No | - | The source sharded database's logical name in the backup (`seedSourceDatabase` OPTION). Needed when seeding from **cloud storage** under a different `spec.name` — Neo4j matches the seed's shards by name — and to pick one family out of an all-databases backup. A PVC seed maps the shards to the new name itself |
| `seedConfig` | `SeedConfiguration` | No | - | Seed configuration for initialization |
| `seedCredentials` | `SeedCredentials` | No | - | Credentials for seed URI access |
| `txLogEnrichment` | `string` | No | - | Transaction log enrichment option |

**Seed URI Notes**:

- `seedURI` is for a single backup location (expects shard-suffixed artifacts).
- `seedURIs` is for per-shard URIs (e.g., dump files or multi-location backups).
- `seedBackupRef` is mutually exclusive with both `seedURI` and `seedURIs`; the operator materialises it into a seed URI at reconcile time.
- `seedConfig.restoreUntil` maps to the `seedRestoreUntil` CREATE DATABASE option for sharded databases.

### PropertyShardingConfiguration

```yaml
propertySharding:
  propertyShards: int32                    # Required: 1-1000
  graphShard: DatabaseTopology             # Required
  propertyShardTopology: PropertyShardTopology  # Required
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `propertyShards` | `int32` | Yes | - | Number of property shards (1-1000) |
| `graphShard` | `DatabaseTopology` | Yes | - | Topology for graph shard database |
| `propertyShardTopology` | `PropertyShardTopology` | Yes | - | Replica topology for property shard databases |

### DatabaseTopology

```yaml
topology:
  primaries: int32     # Optional: Number of primary replicas
  secondaries: int32   # Optional: Number of secondary replicas
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `primaries` | `int32` | No | Number of primary replicas (at least 1 required to host the database) |
| `secondaries` | `int32` | No | Number of secondary replicas (read-only scaling) |

### PropertyShardTopology

```yaml
propertyShardTopology:
  replicas: int32   # Optional: Number of replicas per property shard (default 1)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `replicas` | `int32` | No | 1 | Number of replicas per property shard |

#### Topology Guidelines

**Graph Shard** (stores nodes/relationships):

- Recommended: 3+ primaries for high availability
- Uses Raft consensus for consistency

**Property Shards** (store properties):

- Recommended: 2+ replicas for fault tolerance
- Uses replica-based replication

### Backups and Restore

Sharded databases are a first-class backup/restore target. Set `Neo4jBackup.spec.shardedDatabase` to the `Neo4jShardedDatabase` CR name (with `spec.instanceRef` pointing at the owning cluster); a single backup run captures every shard consistently using a `{name}*` glob, with per-shard artifacts tracked in `status.history[].shardArtifacts`.

Restore is performed by re-creating the sharded database from a backup rather than via a restore Job:

- Seed a fresh sharded database from a backup with `spec.seedBackupRef` (resolves the named `Neo4jBackup`'s latest Succeeded run into a seed URI).
- Recover an existing sharded database destructively with `spec.replaceExisting: true` + `spec.force: true`, which drops and recreates the database from the seed (`Neo4jRestore` rejects sharded targets and points here).

### Deleting a Neo4jShardedDatabase

Deleting the resource does **not** drop the database: the sharded family (`<name>`, `<name>-g000`, `<name>-p000`…) stays in Neo4j with its data, unlike a `Neo4jDatabase`, whose deletion drops its database. To remove the data too, drop it yourself: `CYPHER 25 DROP DATABASE <name>` against `system`. A new `Neo4jShardedDatabase` with the same `spec.name` and the default `ifNotExists: true` finds the existing family instead of creating one (`CREATE DATABASE … IF NOT EXISTS` is then a no-op).

### Neo4jShardedDatabaseStatus

```yaml
status:
  conditions: []metav1.Condition         # Standard conditions
  phase: string                          # Current phase
  message: string                        # Status message
  observedGeneration: int64              # Observed generation
  shardingReady: boolean                 # All shards operational
  creationTime: metav1.Time              # When the operator first saw the graph shard after creating the database; set once
  graphShard: ShardStatus                # Graph shard state from SHOW DATABASES (absent until visible)
  propertyShards: []ShardStatus          # Property shard states from SHOW DATABASES, ordered by propertyShardIndex
  virtualDatabase: VirtualDatabaseStatus  # The logical database combining all shards (name, ready)
  totalSize: string                      # RESERVED: never populated today (SHOW DATABASES has no store size)
  lastBackup: object                     # Reverse-lookup of most recent Succeeded backup
  lastDestructiveRestoreGeneration: int64  # Generation at which the last replaceExisting recreate completed
```

`lastDestructiveRestoreGeneration` gates the destructive `replaceExisting` path: the controller only runs the `DROP ... DESTROY DATA` + recreate cycle when `lastDestructiveRestoreGeneration < metadata.generation`, stamping it on success so subsequent reconciles fall through to a no-op. Re-trigger by mutating the spec (which bumps the generation). `lastBackup` is a non-authoritative observability shortcut populated from the owning `Neo4jBackup` history; the source of truth remains `Neo4jBackup.status.history`.

#### Phase Values

| Phase | Description |
|-------|-------------|
| `Validating` | Initial phase while the spec is validated |
| `Creating` | Spec validated; the sharded database is being created |
| `Waiting` | The target cluster is not yet `Ready` (or `Degraded`) with property sharding operational (reconciler requeues) |
| `Pending` | Waiting on a `seedBackupRef` whose backup has no Succeeded run yet, on the PVC seed proxy, or on seed credentials being projected onto the cluster (reconciler requeues) |
| `Ready` | Sharded database created and operational |
| `Failed` | Validation, seed resolution, client creation or database creation failed |

The controller sets no other phases (for example there is no `Initializing` or `Mixed` phase).

#### Conditions

A single standard `Ready` condition, derived from `phase` (`Ready` → `True`; `Failed` → `False`; every other phase → `Unknown`). There are no per-shard or virtual-database condition types.

### ShardStatus

The controller fills `status.graphShard`, `status.propertyShards`, `status.virtualDatabase` and `status.creationTime` on every reconcile from the `SHOW DATABASES` rows it already reads (one row per hosting server, folded per database). A shard that is not visible yet is simply absent, and the status is only rewritten when something changed.

- **Populated:** `name`, `type`, `state`, `ready`, and `propertyShardIndex` (property shards). `state` is `online` when every copy is online, otherwise the first non-online state any copy reports (for example `offline`, `store copying`). `ready` means at least one copy is online and no copy that is meant to be online is not.
- **Not populated — Reserved:** `size`, `servers`, `lastError`, `propertyCount`, `virtualDatabase.endpoint`, `virtualDatabase.metrics` (and its metrics types below) and `status.totalSize`. `SHOW DATABASES` reports no store size, hosting server or property count, so there is nothing to fill them from. Use `kubectl exec ... SHOW DATABASES` or `status.diagnostics` for those.
- **`creationTime`** is when the operator first saw the graph shard after creating the database — set once and kept; restarted when a destructive restore (`replaceExisting` + `force`) recreates the database. It is not the CR's own `metadata.creationTimestamp`.

```yaml
shardStatus:
  name: string                  # Shard database name
  type: string                 # "graph" or "property"
  state: string                # "online", or the first non-online state a copy reports
  size: string                 # RESERVED: not populated
  servers: []string            # RESERVED: not populated
  ready: boolean              # Operational status
  lastError: string           # RESERVED: not populated
  propertyShardIndex: int32   # Property shard index (property shards only)
  propertyCount: int64        # RESERVED: not populated
```

#### Shard Types

| Type | Description | Naming Pattern |
|------|-------------|----------------|
| `graph` | Graph structure (nodes/relationships) | `{database}-g000` |
| `property` | Properties distributed by hash | `{database}-p{000-999}` |

#### Shard States

`state` carries the `currentStatus` value `SHOW DATABASES` reports, so it can be any Neo4j database state; the common ones:

| State | Description |
|-------|-------------|
| `online` | Shard is operational |
| `offline` | Shard is not available |
| `quarantined` | Shard temporarily excluded due to lag |

### VirtualDatabaseStatus

```yaml
virtualDatabase:
  name: string                    # Virtual database name
  ready: boolean                  # Ready for queries (at least one copy online, none meant to be online that is not)
  endpoint: string               # RESERVED: not populated
  metrics: VirtualDatabaseMetrics # RESERVED: not populated
```

`name` and `ready` are populated from `SHOW DATABASES` once the logical database is visible; until then `virtualDatabase` is absent. `endpoint`, `metrics` and the metrics types below are not populated.

#### VirtualDatabaseMetrics

```yaml
metrics:
  totalNodes: int64                          # Total nodes across shards
  totalRelationships: int64                  # Total relationships
  totalProperties: int64                     # Total properties
  queryMetrics: QueryPerformanceMetrics      # Query performance
```

#### QueryPerformanceMetrics

```yaml
queryMetrics:
  averageQueryTime: string                   # Average execution time
  crossShardQueriesPerSecond: string         # Cross-shard query rate
  propertyCacheHitRatio: string             # Property cache efficiency
```

## Validation Rules

### Neo4jEnterpriseCluster Validation

- Neo4j version must be 2025.12+ when property sharding enabled
- Minimum 2 servers required for property sharding (3+ recommended for HA graph shard primaries)
- Minimum 4GB memory per server (8GB+ recommended for production)
- Minimum 1 CPU core per server (2+ cores recommended for cross-shard queries)
- Authentication required (admin secret must be configured)
- Storage class must be specified
- Required configuration automatically applied

### Neo4jShardedDatabase Validation

- `clusterRef` must reference existing Neo4jEnterpriseCluster with property sharding enabled
- `name` is at most 63 characters, contains only letters, digits, `_` and `-`, and must not be `system` or `neo4j`
- `defaultCypherLanguage` must be "25"
- `propertyShards` must be 1-1000
- `graphShard.primaries` must be >= 1 (and `graphShard.primaries + graphShard.secondaries` must not exceed the cluster's `spec.topology.servers`)
- `propertyShardTopology.replicas` must be >= 1 and must not exceed the cluster's `spec.topology.servers`
- `seedURI` and `seedURIs` cannot both be set
- `seedBackupRef` is mutually exclusive with `seedURI` and `seedURIs`
- `seedSourceDatabase`, `seedConfig`, and `seedCredentials` require a seed source: `seedURI`, `seedURIs`, or `seedBackupRef`
- `seedConfig.restoreUntil`, when set, is an RFC3339 timestamp or `txId:<positive integer>`; `seedConfig.config` and `seedURIs` keys may contain only letters, digits, `.`, `_` and `-`, and `seedConfig.config` values may not contain `,`, `=`, quotes, backticks or newlines
- `replaceExisting: true` requires `force: true` (destructive `DROP ... DESTROY DATA`)
- `replaceExisting: true` is mutually exclusive with `ifNotExists: true` and requires a seed source (`seedURI`, `seedURIs`, or `seedBackupRef`)
- The target cluster must be `Ready` or `Degraded` with property sharding operational (`propertyShardingReady: true`). This is not a validation error: until then the sharded database waits in phase `Waiting` and retries
- `graphShard.primaries` should be >= 3 for high availability (advice only)

## Error Conditions

### Common Validation Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `property sharding requires Neo4j 2025.12+` | Old Neo4j version | Upgrade to 2025.12+ |
| `spec.topology.servers in body should be greater than or equal to 2` | Invalid server count | Increase server count to 2+ (3+ recommended for HA) |
| `property sharding requires minimum 4GB memory` | Insufficient memory | Increase memory to 8GB+ (recommended) |
| `defaultCypherLanguage must be '25'` | Wrong Cypher version | Set to "25" |
| `referenced cluster does not have property sharding enabled` | Cluster not configured | Enable property sharding on cluster |
| `propertyShards must be at least 1` | Invalid shard count | Set to valid range (1-1000) |

### Runtime Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `failed to create sharded database` | Cluster capacity or configuration issues | Check cluster resources and options |
| `failed to prepare cloud credentials` | Missing or invalid seed credentials | Verify seed credentials secret and URI scheme |

## Examples

See [examples directory](https://github.com/priyolahiri/neo4j-kubernetes-operator/tree/main/examples/property_sharding) for complete configuration examples.

## Related APIs

- [Neo4jEnterpriseCluster API](neo4jenterprisecluster.md)
- [Neo4jBackup API](neo4jbackup.md)
- [Neo4jRestore API](neo4jrestore.md)
