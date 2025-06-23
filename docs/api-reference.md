# API Reference

This document provides comprehensive API reference for the Neo4j Enterprise Operator Custom Resources.

## Table of Contents

- [Overview](#overview)
- [Common Types](#common-types)
- [Neo4jEnterpriseCluster](#neo4jenterprisecluster)
- [Neo4jDatabase](#neo4jdatabase)
- [Neo4jBackup](#neo4jbackup)
- [Neo4jRestore](#neo4jrestore)
- [Neo4jUser](#neo4juser)
- [Neo4jRole](#neo4jrole)
- [Neo4jGrant](#neo4jgrant)
- [Status Conditions](#status-conditions)
- [Examples](#examples)

## Overview

The Neo4j Enterprise Operator extends the Kubernetes API with custom resources for managing Neo4j database clusters and related resources. All resources follow Kubernetes conventions and implement the standard `spec-status` pattern.

### API Groups

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind Prefix**: `Neo4j`

### Resource Categories

1. **Cluster Management**: Neo4jEnterpriseCluster, Neo4jDatabase
2. **Data Protection**: Neo4jBackup, Neo4jRestore
3. **Access Control**: Neo4jUser, Neo4jRole, Neo4jGrant

## Common Types

### ObjectMeta

All resources inherit standard Kubernetes metadata:

```yaml
metadata:
  name: string          # Resource name
  namespace: string     # Kubernetes namespace
  labels: {}           # Key-value labels
  annotations: {}      # Key-value annotations
  finalizers: []       # Finalizer strings
```

### ResourceRequirements

Standard Kubernetes resource requirements:

```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "1"
  limits:
    memory: "4Gi"
    cpu: "2"
```

### CommonStatus

All resources implement common status fields:

```yaml
status:
  conditions: []        # Status conditions array
  phase: string        # Current phase
  observedGeneration: int64  # Last observed generation
```

## Neo4jEnterpriseCluster

Manages Neo4j Enterprise clusters with causal clustering support.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jEnterpriseCluster`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: cluster-name
  namespace: neo4j-system
spec:
  # Core cluster configuration
  coreServers: 3                    # Number of core servers (required)
  readReplicas: 2                   # Number of read replicas (optional)

  # Neo4j configuration
  image:
    repository: "neo4j"             # Container image repository
    tag: "5.26-enterprise"          # Image tag
    pullPolicy: "IfNotPresent"      # Image pull policy
    pullSecrets: []                 # Image pull secrets

  # Resource allocation
  resources:
    requests:
      memory: "2Gi"
      cpu: "1"
    limits:
      memory: "4Gi"
      cpu: "2"

  # Storage configuration
  storage:
    dataVolumeSize: "10Gi"          # Data volume size
    logsVolumeSize: "1Gi"           # Logs volume size
    storageClassName: "fast-ssd"     # Storage class
    accessModes: ["ReadWriteOnce"]   # Volume access modes

  # Network configuration
  networking:
    discovery:
      advertised_address: "CLUSTER_IP"  # Discovery address type
    ports:
      http: 7474                    # HTTP port
      https: 7473                   # HTTPS port
      bolt: 7687                    # Bolt port
      cluster: 5000                 # Cluster communication port
      raft: 7000                    # Raft port
      transaction: 6000             # Transaction port

  # Security configuration
  security:
    authEnabled: true               # Enable authentication
    tlsEnabled: true                # Enable TLS
    certificateSecretName: "neo4j-certs"  # TLS certificate secret
    adminPasswordSecretName: "neo4j-admin"  # Admin password secret

  # Backup configuration
  backup:
    enabled: true                   # Enable backups
    schedule: "0 2 * * *"          # Backup schedule (cron)
    retentionPolicy: "7d"          # Backup retention
    storageBackend:
      type: "s3"                   # Backend type (s3, gcs, azure)
      secretName: "backup-credentials"  # Backend credentials
      config:
        bucket: "neo4j-backups"
        region: "us-west-2"

  # Monitoring configuration
  monitoring:
    enabled: true                   # Enable monitoring
    metricsPort: 2004              # Metrics port
    prometheusEnabled: true        # Prometheus integration
```

### Status

```yaml
status:
  # Overall cluster status
  phase: "Ready|Pending|Failed"
  conditions:
  - type: "Ready"
    status: "True"
    reason: "ClusterReady"
    message: "All cluster nodes are ready"

  # Cluster topology
  coreMembers:
  - name: "cluster-core-0"
    role: "LEADER|FOLLOWER"
    address: "10.0.0.1:5000"
    status: "ONLINE|OFFLINE"
  - name: "cluster-core-1"
    role: "FOLLOWER"
    address: "10.0.0.2:5000"
    status: "ONLINE"

  readReplicas:
  - name: "cluster-replica-0"
    address: "10.0.0.3:5000"
    status: "ONLINE"

  # Service endpoints
  endpoints:
    bolt: "neo4j://cluster.neo4j-system.svc.cluster.local:7687"
    http: "http://cluster.neo4j-system.svc.cluster.local:7474"
    https: "https://cluster.neo4j-system.svc.cluster.local:7473"

  # Resource status
  observedGeneration: 1
  currentReplicas: 5
  readyReplicas: 5
```

## Neo4jDatabase

Manages individual databases within a Neo4j Enterprise cluster.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jDatabase`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDatabase
metadata:
  name: my-database
  namespace: neo4j-system
spec:
  # Target cluster
  clusterRef: my-neo4j-cluster

  # Database name
  databaseName: "production"

  # Initial data configuration
  initialData:
    cypherScript: |
      CREATE (n:User {name: 'admin'})
      RETURN n

  # Access control
  access:
    users: ["admin", "app-user"]
    roles: ["reader", "writer"]

  # Database-specific configuration
  config:
    "dbms.memory.heap.initial_size": "1G"
    "dbms.memory.heap.max_size": "2G"
```

### Status

```yaml
status:
  phase: "Ready|Creating|Failed"
  conditions:
  - type: "Ready"
    status: "True"
    reason: "DatabaseReady"
    message: "Database is ready for connections"

  databaseInfo:
    name: "production"
    status: "online"
    role: "primary"
    size: "1.2GB"
    lastBackup: "2024-01-15T10:30:00Z"
```

## Neo4jBackup

Manages Neo4j database backups.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jBackup`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jBackup
metadata:
  name: backup-name
  namespace: neo4j-system
spec:
  # Target database
  database:
    clusterRef:
      name: "neo4j-cluster"
      namespace: "neo4j-system"
    name: "analytics"               # Database name, optional for full backup

  # Backup configuration
  schedule: "0 2 * * *"            # Cron schedule
  type: "full"                     # full|incremental

  # Storage configuration
  storage:
    type: "s3"                     # s3|gcs|azure|pvc
    secretName: "backup-credentials"
    config:
      bucket: "neo4j-backups"
      path: "/cluster-backups"
      region: "us-west-2"

  # Retention policy
  retention:
    keepLast: 7                    # Keep last N backups
    keepDaily: 30                  # Keep daily backups for N days
    keepWeekly: 12                 # Keep weekly backups for N weeks
    keepMonthly: 6                 # Keep monthly backups for N months

  # Backup options
  options:
    compression: true              # Enable compression
    encryption: true               # Enable encryption
    consistencyCheck: true         # Run consistency check
```

### Status

```yaml
status:
  phase: "Ready|Running|Failed"
  conditions:
  - type: "Ready"
    status: "True"
    reason: "BackupReady"
    message: "Backup configuration is ready"

  # Last backup information
  lastBackup:
    startTime: "2024-01-15T02:00:00Z"
    completionTime: "2024-01-15T02:15:00Z"
    size: "1.2GB"
    location: "s3://neo4j-backups/cluster-backups/2024-01-15_02-00-00.backup"
    status: "Completed"

  # Next scheduled backup
  nextBackup: "2024-01-16T02:00:00Z"

  observedGeneration: 1
```

## Neo4jRestore

Manages Neo4j database restore operations.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jRestore`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: restore-name
  namespace: neo4j-system
spec:
  # Target database
  database:
    clusterRef:
      name: "neo4j-cluster"
      namespace: "neo4j-system"
    name: "analytics"               # Target database name

  # Source backup
  source:
    backupRef:
      name: "daily-backup"          # Reference to Neo4jBackup
    # OR direct backup location
    location: "s3://neo4j-backups/cluster-backups/2024-01-15_02-00-00.backup"

  # Restore options
  options:
    replaceExisting: true           # Replace existing database
    force: false                    # Force restore even if database exists
    consistencyCheck: true          # Run consistency check after restore
```

### Status

```yaml
status:
  phase: "Pending|Running|Completed|Failed"
  conditions:
  - type: "Completed"
    status: "True"
    reason: "RestoreCompleted"
    message: "Database restored successfully"

  # Restore progress
  startTime: "2024-01-15T10:00:00Z"
  completionTime: "2024-01-15T10:30:00Z"

  # Restore information
  sourceBackup: "s3://neo4j-backups/cluster-backups/2024-01-15_02-00-00.backup"
  targetDatabase: "analytics"

  observedGeneration: 1
```

## Neo4jPlugin

Manages Neo4j plugins including APOC, GDS, and custom plugins.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jPlugin`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jPlugin
metadata:
  name: apoc-plugin
  namespace: neo4j-system
spec:
  # Target cluster
  clusterRef: my-neo4j-cluster

  # Plugin details
  name: "apoc"
  version: "5.26.0"
  enabled: true

  # Plugin source
  source:
    type: "official"  # official, community, custom, url
    url: "https://custom-repo.com/plugin.jar"  # for custom type
    checksum: "sha256:abc123..."
    authSecret: "plugin-auth"

  # Plugin configuration
  config:
    "apoc.export.file.enabled": "true"
    "apoc.import.file.enabled": "true"

  # Dependencies
  dependencies:
  - name: "graph-data-science"
    versionConstraint: ">=2.6.0"
    optional: false

  # License configuration
  license:
    keySecret: "gds-license"
    serverURL: "https://license.neo4j.com"

  # Security settings
  security:
    allowedProcedures:
    - "apoc.create.*"
    - "apoc.load.*"
    deniedProcedures:
    - "apoc.load.jdbc"
    sandbox: true
    securityPolicy: "restricted"

  # Resource requirements
  resources:
    memoryLimit: "2Gi"
    cpuLimit: "1000m"
    threadPoolSize: 10
```

### Status

```yaml
status:
  phase: "Installed|Installing|Failed|Disabled"
  conditions:
  - type: "Installed"
    status: "True"
    reason: "PluginInstalled"
    message: "Plugin installed successfully"

  installedVersion: "5.26.0"
  installationTime: "2024-01-15T10:30:00Z"

  health:
    status: "healthy"
    lastHealthCheck: "2024-01-15T11:00:00Z"
    errors: []
    performance:
      memoryUsage: "512Mi"
      cpuUsage: "0.1"
      executionCount: 1250
      avgExecutionTime: "15ms"

  usage:
    proceduresCalled:
      "apoc.create.node": 500
      "apoc.load.json": 750
    lastUsed: "2024-01-15T10:55:00Z"
    usageFrequency: "high"
```

## Neo4jUser

Manages Neo4j database users and their authentication.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jUser`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jUser
metadata:
  name: app-user
  namespace: neo4j-system
spec:
  # Target cluster
  clusterRef: my-neo4j-cluster

  # User details
  username: "app-user"

  # Password configuration
  passwordSecret: "app-user-password"

  # User roles
  roles: ["reader", "writer"]

  # Home database
  homeDatabase: "production"

  # Force password change on first login
  requirePasswordChange: false

  # Account status
  suspended: false
```

### Status

```yaml
status:
  phase: "Ready|Creating|Failed"
  conditions:
  - type: "Ready"
    status: "True"
    reason: "UserReady"
    message: "User created and configured"

  userInfo:
    username: "app-user"
    roles: ["reader", "writer"]
    homeDatabase: "production"
    lastLogin: "2024-01-15T10:30:00Z"
    suspended: false
```

## Neo4jRole

Manages Neo4j database roles and permissions.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jRole`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRole
metadata:
  name: analytics-role
  namespace: neo4j-system
spec:
  # Target cluster
  clusterRef: my-neo4j-cluster

  # Role name
  roleName: "analytics-role"

  # Permissions
  permissions:
  - database: "production"
    privileges: ["READ", "MATCH"]
    resources: ["nodes:User", "relationships:FOLLOWS"]

  - database: "analytics"
    privileges: ["READ", "WRITE", "CREATE", "DELETE"]
    resources: ["*"]

  # Inherit from other roles
  inheritsFrom: ["reader"]
```

### Status

```yaml
status:
  phase: "Ready|Creating|Failed"
  conditions:
  - type: "Ready"
    status: "True"
    reason: "RoleReady"
    message: "Role created with permissions"

  roleInfo:
    roleName: "analytics-role"
    permissions:
    - database: "production"
      privileges: ["READ", "MATCH"]
    - database: "analytics"
      privileges: ["READ", "WRITE", "CREATE", "DELETE"]
    inheritsFrom: ["reader"]
```

## Neo4jGrant

Manages privilege grants between users, roles, and databases.

### API Version

- **Group**: `neo4j.neo4j.com`
- **Version**: `v1alpha1`
- **Kind**: `Neo4jGrant`

### Specification

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jGrant
metadata:
  name: user-analytics-grant
  namespace: neo4j-system
spec:
  # Target cluster
  clusterRef: my-neo4j-cluster

  # Grant type
  grantType: "role"  # role, privilege

  # Source (who gets the grant)
  source:
    type: "user"  # user, role
    name: "app-user"

  # Target (what is being granted)
  target:
    type: "role"  # role, privilege, database
    name: "analytics-role"
    database: "production"  # for database-specific grants

  # Grant details (for privilege grants)
  privileges: ["READ", "WRITE"]
  resources: ["nodes:*", "relationships:*"]

  # Conditional grants
  conditions:
    timeRestriction:
      startTime: "09:00"
      endTime: "17:00"
      timezone: "UTC"
    ipRestriction:
      allowedCIDRs: ["10.0.0.0/8", "192.168.1.0/24"]
```

### Status

```yaml
status:
  phase: "Active|Creating|Failed|Revoked"
  conditions:
  - type: "Active"
    status: "True"
    reason: "GrantActive"
    message: "Grant is active and enforced"

  grantInfo:
    grantType: "role"
    source: "user:app-user"
    target: "role:analytics-role"
    database: "production"
    createdAt: "2024-01-15T10:30:00Z"
    lastUsed: "2024-01-15T11:00:00Z"
```

## Status Conditions

All Neo4j operator resources implement standard Kubernetes status conditions:

### Common Condition Types

| Type | Description | Possible Status | Reason Examples |
|------|-------------|-----------------|-----------------|
| `Ready` | Resource is ready for use | `True`, `False`, `Unknown` | `ClusterReady`, `DatabaseReady`, `PluginInstalled` |
| `Progressing` | Resource is being processed | `True`, `False` | `Creating`, `Updating`, `Installing` |
| `Degraded` | Resource is partially functional | `True`, `False` | `PartialFailure`, `ReducedCapacity` |
| `Available` | Resource is available for connections | `True`, `False` | `ServiceAvailable`, `EndpointsReady` |

### Condition Structure

```yaml
conditions:
- type: "Ready"
  status: "True"              # True, False, Unknown
  lastTransitionTime: "2024-01-15T10:30:00Z"
  reason: "ClusterReady"      # CamelCase reason code
  message: "All cluster nodes are ready and accepting connections"
```

### Phase Values

Each resource type has specific phase values:

#### Neo4jEnterpriseCluster
- `Pending` - Cluster creation in progress
- `Ready` - Cluster is fully operational
- `Upgrading` - Rolling upgrade in progress
- `Scaling` - Auto-scaling operation in progress
- `Failed` - Cluster creation or operation failed
- `Terminating` - Cluster deletion in progress

#### Neo4jDatabase
- `Creating` - Database creation in progress
- `Ready` - Database is ready for use
- `Failed` - Database creation failed
- `Terminating` - Database deletion in progress

#### Neo4jPlugin
- `Installing` - Plugin installation in progress
- `Installed` - Plugin is installed and ready
- `Failed` - Plugin installation failed
- `Disabled` - Plugin is disabled
- `Updating` - Plugin update in progress

#### Neo4jBackup/Neo4jRestore
- `Pending` - Operation queued
- `Running` - Operation in progress
- `Completed` - Operation completed successfully
- `Failed` - Operation failed
- `Cancelled` - Operation was cancelled

## Examples

### Complete Production Cluster

```yaml
# Complete production setup with all resources
---
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: production-cluster
  namespace: neo4j
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: "5.26-enterprise"

  topology:
    primaries: 3
    secondaries: 2
    enforceDistribution: true
    availabilityZones: ["us-east-1a", "us-east-1b", "us-east-1c"]

  storage:
    className: "gp3"
    size: "500Gi"

  auth:
    provider: native
    secretRef: neo4j-auth

  tls:
    mode: cert-manager
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer

  autoScaling:
    enabled: true
    primaries:
      enabled: true
      minReplicas: 3
      maxReplicas: 7
      metrics:
      - type: cpu
        target: "70"
      - type: memory
        target: "80"
    secondaries:
      enabled: true
      minReplicas: 1
      maxReplicas: 10
      metrics:
      - type: connection_count
        target: "100"

---
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDatabase
metadata:
  name: production-db
  namespace: neo4j
spec:
  clusterRef: production-cluster
  databaseName: "production"
  initialData:
    cypherScript: |
      CREATE CONSTRAINT user_email IF NOT EXISTS FOR (u:User) REQUIRE u.email IS UNIQUE;
      CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name);

---
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jPlugin
metadata:
  name: apoc
  namespace: neo4j
spec:
  clusterRef: production-cluster
  name: apoc
  version: "5.26.0"
  source:
    type: official
  config:
    "apoc.export.file.enabled": "true"
    "apoc.import.file.enabled": "true"

---
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jUser
metadata:
  name: app-user
  namespace: neo4j
spec:
  clusterRef: production-cluster
  username: "app-user"
  passwordSecret: "app-user-password"
  roles: ["reader", "writer"]
  homeDatabase: "production"

---
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jBackup
metadata:
  name: daily-backup
  namespace: neo4j
spec:
  target:
    kind: Cluster
    name: production-cluster
  storage:
    type: s3
    bucket: neo4j-backups
    path: /daily
  schedule: "0 2 * * *"
  retention:
    maxAge: "30d"
    maxCount: 30
```

### Multi-Cluster Global Deployment

```yaml
# Primary cluster in US-East
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: global-primary
  namespace: neo4j
spec:
  multiCluster:
    enabled: true
    topology:
      clusters:
      - name: "us-east-primary"
        region: "us-east-1"
        endpoint: "global-primary.neo4j.svc.cluster.local:7687"
        nodeAllocation:
          primaries: 3
          secondaries: 2
      - name: "eu-west-secondary"
        region: "eu-west-1"
        endpoint: "global-secondary.neo4j.svc.cluster.local:7687"
        nodeAllocation:
          primaries: 1
          secondaries: 2
      strategy: "active-passive"
      primaryCluster: "us-east-primary"

    networking:
      type: "cilium"
      cilium:
        clusterMesh:
          enabled: true
          clusterId: 1
        encryption:
          enabled: true
          type: "wireguard"
```
