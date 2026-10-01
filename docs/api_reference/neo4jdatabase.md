# Neo4jDatabase API Reference

The `Neo4jDatabase` Custom Resource Definition (CRD) provides declarative database management for both Neo4j Enterprise clusters and standalone deployments.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jDatabase`
- **Supported Neo4j Versions**: 5.26.0+ (semver) and 2025.01.0+ (calver)
- **Target Deployments**: Both `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`
- **Database Creation**: Automated database provisioning with topology control
- **Schema Management**: Initial data import and schema creation
- **Seed URI Support**: Create databases from existing backups (Neo4j 5.26+)

## Key Features

**Universal Compatibility**: Works with both cluster and standalone deployments through automatic resource discovery:

- **Cluster Support**: Create databases across multiple servers with custom topology
- **Standalone Support**: Create databases in single-node deployments
- **Automatic Discovery**: Controller automatically detects target deployment type
- **Unified API**: Same resource definition works for both deployment types
- **Topology Control**: Specify primary/secondary distribution for cluster databases
- **Seed URI**: Create databases from S3, GCS, Azure, HTTP, or FTP backup sources

## Related Resources

- [`Neo4jEnterpriseCluster`](neo4jenterprisecluster.md) - Target cluster deployments
- [`Neo4jEnterpriseStandalone`](neo4jenterprisestandalone.md) - Target standalone deployments
- [`Neo4jBackup`](neo4jbackup.md) - Create backups of databases
- [`Neo4jRestore`](neo4jrestore.md) - Restore databases from backups
- [`Neo4jPlugin`](neo4jplugin.md) - Install plugins for database functionality

## Spec

| Field | Type | Description |
|---|---|---|
| `clusterRef` | `string` | **Required.** Name of the target `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone` in the same namespace. For databases on a Neo4j Aura instance, use the `AuraDatabase` CRD instead. |
| `name` | `string` | **Required**. Database name to create. Must start with a letter and contain only letters, digits, dots and dashes (max 65 characters). `system` is reserved; `neo4j` is allowed with a warning (it shadows the default database) |
| `wait` | `boolean` | Wait for database creation to complete (default: `true`) |
| `ifNotExists` | `boolean` | Create only if database doesn't exist - prevents reconciliation errors (default: `true`) |
| `topology` | [`DatabaseTopology`](#databasetopology) | Database distribution topology (cluster only; ignored, with a warning, for a standalone target). Applied at `CREATE DATABASE` only — an existing database is observed, not altered |
| `defaultCypherLanguage` | `string` (enum: `"5"`, `"25"`) | Default Cypher language for the database, set at creation with `DEFAULT LANGUAGE CYPHER <n>`. **CalVer only** — the clause does not exist on the 5.26 LTS, so the value is checked against the target's image: on the LTS `"25"` is **rejected** by validation (`spec.defaultCypherLanguage`), and `"5"` is accepted but has **no effect** (every LTS database already runs Cypher 5, and the clause is omitted from `CREATE DATABASE`). On CalVer it is applied on every create path — plain, with `topology`, from a `seedURI`, or both. Unset, the database takes the server default (`spec.serverDefaultCypherLanguage` on the deployment) |
| `options` | `map[string]string` | Additional `CREATE DATABASE` options. The validator accepts only these keys: `txLogEnrichment` (`OFF` or `DIFF`), `storeFormat` (`standard`, `high_limit` or `block`), `existingData` (`use` or `fail`), `existingDataSeedServer`, `existingDataSeedInstance`, `existingMetadata`, `seedCredentials`, and `seedURI` / `seedConfig` (the last two are deprecated as options — use the dedicated spec fields). Any other key is rejected, and values may not be empty |
| `initialData` | [`InitialDataSpec`](#initialdataspec) | Cypher statements run once after creation (**mutually exclusive with `seedURI`**). Only `cypherStatements` is executed |
| `seedURI` | `string` | Backup URI for database creation (**mutually exclusive with `initialData`**) |
| `seedConfig` | [`SeedConfiguration`](#seedconfiguration) | Advanced seed URI configuration |
| `seedCredentials` | [`SeedCredentials`](#seedcredentials) | Seed URI access credentials |

### DatabaseTopology

**Cluster-Only Feature**: Database topology is only applicable to `Neo4jEnterpriseCluster` deployments. Ignored for standalone deployments.

| Field | Type | Description |
|---|---|---|
| `primaries` | `int32` | Number of primary servers. Optional; when set, minimum is `1` |
| `secondaries` | `int32` | Number of secondary servers (minimum: `0`) |

**Validation**:

- `primaries + secondaries` must not exceed cluster's `spec.topology.servers`
- Servers are selected based on role constraints (if configured)
- For standalone deployments, topology is ignored: validation emits a warning, the controller sends no `TOPOLOGY` clause, and `primaries: 0` is **not** an error there (on a cluster it is, because a database needs at least one primary)
- Topology is applied when the database is created. Editing it on an existing database has no effect — the operator does not issue `ALTER DATABASE … SET TOPOLOGY`

### InitialDataSpec

| Field | Type | Description |
|---|---|---|
| `source` | `string` | Source type for initial data: `"cypher"`, `"dump"`, `"csv"`. **Accepted but not acted on** — the controller does not read it. A `ValidationWarning` event is raised when it is set to `"dump"` or `"csv"`; `"cypher"` (what `cypherStatements` does) is not warned about |
| `cypherStatements` | `[]string` | Cypher statements to execute, in order, against the new database. **The only field that is executed.** Runs once; `status.dataImported` records that it has run |
| `configMapRef` | `string` | **Accepted but not acted on** — the ConfigMap is never read, so put statements in `cypherStatements`. A `ValidationWarning` event is raised when it is set. |
| `secretRef` | `string` | **Accepted but not acted on.** A `ValidationWarning` event is raised when it is set. |
| `storage` | [`*StorageLocation`](#storagelocation) | **Accepted but not acted on** (the `StorageLocation` tables below describe the schema only). A `ValidationWarning` event is raised when it is set. |

### SeedConfiguration

Advanced configuration for creating databases from seed URIs using Neo4j's CloudSeedProvider.

| Field | Type | Description |
|---|---|---|
| `restoreUntil` | `string` | Point-in-time recovery timestamp (Neo4j 2025.x only; rejected on 5.26) |
| `config` | `map[string]string` | Seed-provider configuration, rendered into the `seedConfig` `OPTIONS` string as comma-separated `key=value` pairs (for example `region=eu-west-1`). Consumed by the S3SeedProvider; the CloudSeedProvider (the default for `s3://`, `gs://` and `azb://`) takes region and credentials from the environment, so most deployments leave this empty |

**Point-in-Time Recovery Formats** (Neo4j 2025.x only):

- **RFC3339 Timestamp**: `"2025-01-15T10:30:00Z"`
- **Transaction ID**: `"txId:12345"` (a positive integer that fits in int64)

**`config` rules**: keys may contain only letters, digits, `.`, `_` and `-`; values may not contain `,`, `=`, quotes, backticks or newlines. The operator forwards the pairs to Neo4j unchanged and does not define any keys of its own — see the Neo4j documentation for the keys your seed provider understands.

### SeedCredentials

| Field | Type | Description |
|---|---|---|
| `secretRef` | `string` | Name of Kubernetes secret containing credentials for seed URI access |

**The Secret must be projected onto the server pods.** The Neo4j JVM reads the credentials from its environment, so the Secret has to appear in the target `Neo4jEnterpriseCluster`/`Neo4jEnterpriseStandalone` `spec.extraEnvFrom`. If it does not, the database stays blocked with reason `SeedCredsMissing` and a message showing what to add. If the target carries the annotation `neo4j.com/auto-inherit-seed-creds: "true"`, the operator appends the entry to `spec.extraEnvFrom` itself (reason `SeedCredsAutoInherited`) — **this triggers a rolling restart** of the target. Omit `seedCredentials` entirely when you rely on IAM roles / workload identity.

### StorageLocation

> `StorageLocation` and the tables below are only reachable through `initialData.storage`, which is currently accepted but not acted on.

| Field | Type | Description |
|---|---|---|
| `type` | `string` (enum: `"s3"`, `"gcs"`, `"azure"`, `"pvc"`) | **Required**. Storage type |
| `bucket` | `string` | Bucket name (for cloud storage) |
| `path` | `string` | Path within bucket or PVC |
| `pvc` | [`*PVCSpec`](#pvcspec) | PVC configuration (for `pvc` type) |
| `cloud` | [`*CloudBlock`](#cloudblock) | Cloud provider configuration |

### PVCSpec

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Name of existing PVC to use |
| `storageClassName` | `string` | Storage class name |
| `size` | `string` | Size for new PVC (e.g., `"100Gi"`) |

### CloudBlock

| Field | Type | Description |
|---|---|---|
| `provider` | `string` (enum: `"aws"`, `"gcp"`, `"azure"`) | Cloud provider |
| `identity` | [`*CloudIdentity`](#cloudidentity) | Cloud identity configuration |
| `credentialsSecretRef` | `string` | Name of a Kubernetes Secret with cloud provider credentials as env vars. Optional when using workload identity / IAM instance profiles |
| `endpointURL` | `string` | Overrides the S3 API endpoint URL to target S3-compatible stores (MinIO, Ceph RGW, Cloudflare R2). Only applies to the `aws` provider |
| `forcePathStyle` | `boolean` | Forces S3 path-style addressing (bucket in URL path). Required for MinIO and most self-hosted S3-compatible stores. Only effective when `endpointURL` is set |

### CloudIdentity

| Field | Type | Description |
|---|---|---|
| `provider` | `string` (enum: `"aws"`, `"gcp"`, `"azure"`) | **Required**. Identity provider |
| `serviceAccount` | `string` | **Reserved — accepted but not acted on.** Backup/restore Jobs always run as the operator-managed ServiceAccounts |
| `autoCreate` | [`*AutoCreateSpec`](#autocreatespec) | Workload-identity annotations (see below) |

### AutoCreateSpec

| Field | Type | Description |
|---|---|---|
| `enabled` | `bool` | **Reserved — accepted but not acted on** (the operator always ensures its backup/restore ServiceAccount exists). Schema default: `true` |
| `annotations` | `map[string]string` | Annotations applied to the operator-managed backup/restore ServiceAccount (used by `Neo4jBackup`/`Neo4jRestore`; `Neo4jDatabase` itself does not read `initialData.storage`) |

#### Required Secret Keys by URI Scheme

**Amazon S3 (`s3://`)**:

- `AWS_ACCESS_KEY_ID` (required)
- `AWS_SECRET_ACCESS_KEY` (required)
- `AWS_SESSION_TOKEN` (optional, for temporary credentials)
- `AWS_REGION` (optional)

**Google Cloud Storage (`gs://`)**:

- `GOOGLE_APPLICATION_CREDENTIALS` (required, service account JSON key)
- `GOOGLE_CLOUD_PROJECT` (optional)

**Azure Blob Storage (`azb://`)**:

- `AZURE_STORAGE_ACCOUNT` (required)
- Either `AZURE_STORAGE_KEY` or `AZURE_STORAGE_SAS_TOKEN` (required)

**HTTP/HTTPS/FTP**:

- `USERNAME` (optional)
- `PASSWORD` (optional)
- `AUTH_HEADER` (optional, for custom authentication)

## Status

| Field | Type | Description |
|---|---|---|
| `conditions` | `[]metav1.Condition` | Current status conditions |
| `phase` | `string` | Current phase of the database: `Ready`, `Pending` (target not found / not ready), `Failed` (connection, creation or data-import failure), `ValidationFailed`, or `Unknown` (any other blocked state, e.g. `SeedCredsMissing`) |
| `message` | `string` | Human-readable status message |
| `observedGeneration` | `int64` | Generation observed by the controller |
| `dataImported` | `*bool` | Whether initial data has been imported |
| `creationTime` | `*metav1.Time` | When the database was created |
| `size` | `string` | Database size |
| `state` | `string` | Current database state: `"online"`, `"offline"`, `"started"`, `"stopped"` |
| `servers` | `[]string` | Servers hosting the database |

## Examples

### Basic Database in Cluster

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: cluster-database
spec:
  clusterRef: my-cluster  # References Neo4jEnterpriseCluster
  name: mydb
  wait: true
  ifNotExists: true
  topology:
    primaries: 2    # Distribute across 2 primary servers
    secondaries: 1  # 1 secondary for read scaling
```

### Basic Database in Standalone

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: standalone-database
spec:
  clusterRef: my-standalone  # References Neo4jEnterpriseStandalone
  name: mydb
  wait: true
  ifNotExists: true
  # Note: topology not needed for standalone (ignored if specified)
```

### Database with Advanced Topology (Cluster Only)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: distributed-database
spec:
  clusterRef: production-cluster  # Must be Neo4jEnterpriseCluster
  name: distributed
  wait: true
  ifNotExists: true
  topology:
    primaries: 3    # Uses 3 servers for primary role
    secondaries: 2  # Uses 2 servers for secondary role
  options:
    txLogEnrichment: "DIFF"  # Enhanced transaction logging
  defaultCypherLanguage: "25"  # CalVer only — rejected on the 5.26 LTS
```

### Multi-Database Setup

```yaml
# User-facing database with high availability
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: user-database
spec:
  clusterRef: production-cluster
  name: users
  topology:
    primaries: 3
    secondaries: 1
  initialData:
    source: cypher
    cypherStatements:
      - "CREATE CONSTRAINT user_email IF NOT EXISTS FOR (u:User) REQUIRE u.email IS UNIQUE"
      - "CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name)"

---
# Analytics database optimized for reads
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: analytics-database
spec:
  clusterRef: production-cluster
  name: analytics
  topology:
    primaries: 1     # Minimal write capacity
    secondaries: 4   # Optimized for read scaling
  options:
    txLogEnrichment: "OFF"  # Reduce overhead for analytics
```

### Database with Schema and Sample Data

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: app-database
spec:
  clusterRef: my-cluster  # Works with cluster or standalone
  name: appdb
  wait: true
  ifNotExists: true
  initialData:
    source: cypher
    cypherStatements:
      # Schema creation
      - "CREATE CONSTRAINT user_email IF NOT EXISTS FOR (u:User) REQUIRE u.email IS UNIQUE"
      - "CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name)"
      - "CREATE INDEX product_category IF NOT EXISTS FOR (p:Product) ON (p.category)"
      # Sample data
      - "CREATE (u:User {name: 'Alice', email: 'alice@example.com'})"
      - "CREATE (p:Product {name: 'Neo4j Enterprise', category: 'Database'})"
      - "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Neo4j Enterprise'}) CREATE (u)-[:PURCHASED]->(p)"
  topology:  # Only applied if clusterRef is a cluster
    primaries: 2
    secondaries: 1
```

### Neo4j 2025.x Database with Enhanced Features

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: modern-database
spec:
  clusterRef: neo4j-2025-cluster  # Neo4j 2025.x cluster
  name: moderndb
  wait: true
  ifNotExists: true
  defaultCypherLanguage: "25"  # Enable Cypher 25 features (CalVer only)
  topology:
    primaries: 2
    secondaries: 1
  options:
    txLogEnrichment: "DIFF"  # One of the few keys the validator accepts (see `options` above)
  initialData:
    source: cypher
    cypherStatements:
      # Use Cypher 25 syntax features
      - "CREATE VECTOR INDEX document_embedding IF NOT EXISTS FOR (d:Document) ON d.embedding OPTIONS {dimension: 1536, similarity: 'cosine'}"
      - "CREATE (d:Document {title: 'Neo4j 2025 Guide', embedding: [0.1, 0.2, 0.3]})"
```

### Database from Seed URI (Production Restore)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: prod-restore-database
spec:
  clusterRef: recovery-cluster  # Target cluster for restore
  name: restored-sales-db

  # Restore from S3 backup (system-wide IAM authentication)
  seedURI: "s3://prod-neo4j-backups/sales-database-2025-01-15.backup"

  # Production topology
  topology:
    primaries: 3    # High availability
    secondaries: 2  # Read scaling

  # Neo4j 2025.x point-in-time recovery
  seedConfig:
    restoreUntil: "2025-01-15T10:30:00Z"  # Specific point in time
    config:
      region: "us-east-1"       # S3SeedProvider key (rendered as seedConfig "region=us-east-1")

  wait: true
  ifNotExists: true
  defaultCypherLanguage: "25"  # CalVer only
  options:
    txLogEnrichment: "DIFF"    # Enhanced logging for production
```

### Database from Seed URI (Development Copy)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: dev-copy-database
spec:
  clusterRef: dev-standalone  # Can target standalone for development
  name: dev-copy

  # Copy from Google Cloud Storage backup
  seedURI: "gs://dev-backups/prod-snapshot-2025-01-15.backup"

  # Explicit credentials for dev environment (the Secret must be in the target's
  # spec.extraEnvFrom, or the target needs the neo4j.com/auto-inherit-seed-creds annotation)
  seedCredentials:
    secretRef: gcs-dev-credentials

  wait: true
  ifNotExists: true
  # No topology needed for standalone deployment
```

### Multi-Cloud Seed URI Examples

```yaml
# AWS S3 with explicit credentials
apiVersion: v1
kind: Secret
metadata:
  name: s3-credentials
type: Opaque
data:
  AWS_ACCESS_KEY_ID: <base64-encoded-access-key>
  AWS_SECRET_ACCESS_KEY: <base64-encoded-secret-key>
  AWS_REGION: dXMtZWFzdC0x  # us-east-1
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: s3-restore-db
spec:
  clusterRef: prod-cluster
  name: s3-restored
  seedURI: "s3://prod-backups/full-backup-2025-01-15.backup"
  seedCredentials:
    secretRef: s3-credentials
  topology:
    primaries: 2
    secondaries: 1

---
# Google Cloud Storage with service account
apiVersion: v1
kind: Secret
metadata:
  name: gcs-credentials
type: Opaque
data:
  GOOGLE_APPLICATION_CREDENTIALS: <base64-encoded-service-account-json>
  GOOGLE_CLOUD_PROJECT: <base64-encoded-project-id>
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: gcs-restore-db
spec:
  clusterRef: test-cluster
  name: gcs-restored
  seedURI: "gs://test-backups/snapshot-2025-01-15.backup"
  seedCredentials:
    secretRef: gcs-credentials

---
# Azure Blob Storage with SAS token
apiVersion: v1
kind: Secret
metadata:
  name: azure-credentials
type: Opaque
data:
  AZURE_STORAGE_ACCOUNT: <base64-encoded-account-name>
  AZURE_STORAGE_SAS_TOKEN: <base64-encoded-sas-token>
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: azure-restore-db
spec:
  clusterRef: azure-cluster
  name: azure-restored
  seedURI: "azb://backups/neo4j-backup-2025-01-15.backup"
  seedCredentials:
    secretRef: azure-credentials

---
# HTTP/HTTPS with basic authentication
apiVersion: v1
kind: Secret
metadata:
  name: http-credentials
type: Opaque
data:
  USERNAME: <base64-encoded-username>
  PASSWORD: <base64-encoded-password>
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: http-restore-db
spec:
  clusterRef: local-cluster
  name: http-restored
  seedURI: "https://backup-server.example.com/backups/neo4j-2025-01-15.backup"
  seedCredentials:
    secretRef: http-credentials
```

### Advanced Database Management

```yaml
# Asynchronous database creation
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: async-database
spec:
  clusterRef: my-cluster
  name: asyncdb
  wait: false  # Returns immediately (NOWAIT mode)
  ifNotExists: true
  topology:
    primaries: 1
    secondaries: 0

---
# Initial data: only initialData.cypherStatements is executed (configMapRef,
# secretRef, source and storage are accepted but not acted on), so put every
# statement inline, one per list item
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata:
  name: complex-database
spec:
  clusterRef: my-cluster
  name: complex-app
  initialData:
    cypherStatements:
      - "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE"
      - "CREATE CONSTRAINT product_sku IF NOT EXISTS FOR (p:Product) REQUIRE p.sku IS UNIQUE"
      - "CREATE (u:User {id: 1, name: 'Alice', email: 'alice@example.com'})"
      - "CREATE (p:Product {sku: 'NEO4J-ENT', name: 'Neo4j Enterprise'})"
  topology:
    primaries: 2
    secondaries: 1
```

## Behavior

### Target Discovery

**Automatic Resource Discovery**: The controller automatically determines the target deployment type:

1. **Cluster Lookup**: First attempts to find `Neo4jEnterpriseCluster` with matching name
2. **Standalone Fallback**: If cluster not found, looks for `Neo4jEnterpriseStandalone`
3. **Validation**: Applies appropriate validation rules based on target type
4. **Client Creation**: Uses correct Neo4j client (cluster vs standalone connection)

### Default Database (`neo4j`)

Neo4j automatically creates a default database called `neo4j` at cluster bootstrap with a topology of 1 primary and 0 secondaries. The operator does not create or manage this database.

If you create a `Neo4jDatabase` resource with `name: neo4j`, the operator will:

1. **Emit a validation warning**: `'neo4j' is the default database name; creating a database with this name will shadow the default database`
2. **Skip creation**: Since `ifNotExists: true` is the default and the database already exists, `CREATE DATABASE` is a no-op
3. **Not alter topology**: `topology` is applied at `CREATE DATABASE` only. Because `neo4j` already exists, a `topology` on the CR is **not** applied; change it yourself with `ALTER DATABASE neo4j SET TOPOLOGY …`
4. **Drop on deletion**: If you delete the `Neo4jDatabase` resource, the operator will drop the `neo4j` database — use caution

To control the default database topology at cluster creation time without using this CRD, set `initial.dbms.default_primaries_count` and `initial.dbms.default_secondaries_count` in the cluster's `spec.config` (bootstrap-only, see [Clustering Guide](../user_guide/clustering.md#default-database-topology)).

### Database Creation Process

**Standard Database Creation**:

1. Discover target deployment (cluster or standalone)
2. Check if database exists (if `ifNotExists: true`)
3. Validate topology constraints (cluster only)
4. Construct CREATE DATABASE command with Neo4j 5.26+ syntax
5. Execute command via appropriate client connection
6. Wait for completion (if `wait: true`)
7. Import initial data (if specified)
8. Update status with current state

**Seed URI Database Creation**:

1. Discover target deployment and validate seed URI format
2. Prepare cloud authentication (if `seedCredentials` specified)
3. Construct CREATE DATABASE FROM URI command with CloudSeedProvider
4. Execute command with seed configuration options
5. Wait for restoration completion (if `wait: true`)
6. Update status (**Note**: initial data import skipped - data comes from seed)

### Version-Specific Behavior

**Neo4j 5.26.x**:

- Standard `CREATE DATABASE` syntax with `TOPOLOGY` clause
- Seed URI support via CloudSeedProvider
- No `DEFAULT LANGUAGE CYPHER` clause: `defaultCypherLanguage: "25"` is rejected by validation, and `"5"` is accepted but omitted from the statement (every database runs Cypher 5)
- Compatible with both cluster and standalone deployments

**Neo4j 2025.x**:

- Enhanced `CREATE DATABASE` with `DEFAULT LANGUAGE CYPHER` support, applied on every create path
- Point-in-time recovery for seed URIs (`restoreUntil`)
- Advanced seed configuration options
- Same compatibility with cluster and standalone deployments

### Version-Specific Behavior

**Neo4j 5.26.x**:

- Standard CREATE DATABASE syntax
- Seed URI support with CloudSeedProvider
- No `DEFAULT LANGUAGE CYPHER` clause (`defaultCypherLanguage: "25"` is rejected; `"5"` has no effect)
- No point-in-time recovery for seed URIs
- Supports all topology and option features

**Neo4j 2025.x**:

- Supports `defaultCypherLanguage` (`"5"` and `"25"`)
- Enhanced seed URI support with point-in-time recovery (`restoreUntil`)
- Enhanced topology management
- Additional database options available

### Reconciliation

The operator creates the database if it is missing, then observes it:

- If the database doesn't exist, it is created (with `topology`, `defaultCypherLanguage`, seed and `options` applied at that moment)
- If the database already exists, nothing about it is altered: later edits to `topology`, `options`, `defaultCypherLanguage` or the seed fields are **not** applied, and the operator never starts or stops the database. Use `ALTER DATABASE …` for those changes
- `initialData.cypherStatements` run once after creation (recorded in `status.dataImported`)
- Status (`state`, `servers`, `phase`) is refreshed from the live database on every reconcile

## Best Practices

### General Best Practices
1. **Always use `ifNotExists: true`** in production to prevent reconciliation errors
2. **Set appropriate topology** based on your availability requirements
3. **Use `wait: true`** for critical databases to ensure they're ready
4. **Include IF NOT EXISTS** in schema creation statements
5. **Test database creation** in staging before production deployment

### Seed URI Best Practices
6. **Prefer system-wide authentication** (IAM roles, workload identity) over explicit credentials
7. **Use .backup format** for better performance with large datasets compared to .dump format
8. **Don't combine `seedURI` and `initialData`** - they conflict with each other
9. **Use point-in-time recovery** (`restoreUntil`) when available for precise restoration
10. **Test seed URI access** from Neo4j pods before creating databases
11. **Monitor restoration progress** - large backups may take significant time
12. **Use `seedConfig.config` only for seed-provider keys** the provider documents (for example `region` for the S3SeedProvider); the operator passes them through unchanged

## Troubleshooting

### Database Creation Fails

**Check operator logs**:
```bash
kubectl logs -n neo4j-operator deployment/neo4j-operator-controller-manager
```

**Common Issues**:

- **Target Not Found**: `clusterRef` doesn't match any cluster or standalone
- **Topology Validation**: Insufficient servers for requested topology (cluster only)
- **Name Conflicts**: Database name already exists
- **Authentication**: Connection issues to Neo4j instance
- **Seed URI Issues**: Invalid format, inaccessible backup, credential problems
- **Version Compatibility**: Using 2025.x features with 5.26.x Neo4j

**Cluster-Specific Issues**:

- Server capacity exceeded (primaries + secondaries > cluster servers)
- Role constraint conflicts (e.g., requesting primaries from SECONDARY-only servers)

**Standalone-Specific Issues**:

- Topology specified for standalone deployment (will be ignored)
- Authentication configuration missing (`adminSecret` not configured)

### Database Stuck in Pending

**Verify target deployment**:
```bash
# For cluster targets
kubectl get neo4jenterprisecluster <cluster-name>
kubectl describe neo4jenterprisecluster <cluster-name>

# For standalone targets
kubectl get neo4jenterprisestandalone <standalone-name>
kubectl describe neo4jenterprisestandalone <standalone-name>

# Check database status
kubectl describe neo4jdatabase <database-name>
kubectl get events --field-selector involvedObject.name=<database-name>
```

**Common Causes**:

- Target deployment not ready or in failed state
- Neo4j authentication issues
- Network connectivity problems
- Resource constraints (memory, CPU)
- Database name validation failures

### Seed URI Troubleshooting

**Authentication Issues:**
```bash
# Check secret exists and has correct keys
kubectl get secret backup-credentials -o yaml

# Test access from a pod
kubectl run test-pod --rm -it --image=amazon/aws-cli \
  -- aws s3 ls s3://my-bucket/backup.backup
```

**URI Access Issues:**

- Verify the backup file exists at the specified URI
- Check network connectivity from Neo4j pods to the URI
- Ensure firewall rules allow outbound access
- Test URI format: `scheme://host/path/file.backup`

**Performance Issues:**

- Use `.backup` format instead of `.dump` for large datasets
- Monitor pod resources during restoration

**Validation Errors:**
```bash
# Check for configuration conflicts
kubectl describe neo4jdatabase <database-name>

# Common validation errors:
# - seedURI and initialData cannot be used together
# - Database topology exceeds cluster capacity
# - Invalid URI scheme or format
# - Missing required credential keys in secret
```

### Initial Data Not Imported

**Troubleshooting Steps**:
```bash
# Check database is online
kubectl exec <target-pod> -c neo4j -- \
  cypher-shell -u neo4j -p <password> "SHOW DATABASES YIELD name, currentStatus WHERE name = '<db-name>'"

# Verify Cypher statements manually
kubectl exec <target-pod> -c neo4j -- \
  cypher-shell -u neo4j -p <password> -d <db-name> "<test-statement>"

# Check operator logs for import errors
kubectl logs -n neo4j-operator deployment/neo4j-operator-controller-manager | grep -i "initial.*data"
```

**Common Issues**:

- Invalid Cypher syntax in statements
- Constraint/index name conflicts
- Database not fully online before import
- Insufficient privileges for data import
- **Note**: Initial data automatically skipped when using `seedURI`
- **Standalone**: Ensure `adminSecret` properly configures authentication

### Seed URI Troubleshooting

**Monitor Progress**:
```bash
# Watch database creation events
kubectl get events --field-selector involvedObject.name=<database-name> --watch

# Check Neo4j logs for CloudSeedProvider activity
kubectl logs <target-pod> -c neo4j | grep -i "cloud.*seed\|restore"

# Verify seed URI accessibility
kubectl run test-uri --rm -it --image=curlimages/curl -- \
  curl -I "<seed-uri>"  # Test HTTP/HTTPS accessibility
```

**Key Events**:

- `DatabaseCreatedFromSeed`: Successful seed URI restoration
- `DataSeeded`: Database seeding completed
- `ValidationWarning`: Configuration or URI format warnings
- `CreationFailed`: Seed restoration failed
- `SeedCredsMissing`: `seedCredentials.secretRef` is not in the target's `spec.extraEnvFrom` (and the target lacks the `neo4j.com/auto-inherit-seed-creds` annotation)
- `SeedCredsAutoInherited`: the operator added the Secret to the target's `spec.extraEnvFrom`; waiting for the rolling restart

**Authentication Debugging**:
```bash
# Check secret exists and has correct format
kubectl get secret <seed-credentials-secret> -o yaml

# Test cloud credentials from pod
kubectl run aws-test --rm -it --image=amazon/aws-cli -- \
  aws s3 ls <s3-bucket>  # For S3 URIs
```

**Performance Issues**:

- Use `.backup` format instead of `.dump` for large datasets
- Monitor pod resource usage during restoration
