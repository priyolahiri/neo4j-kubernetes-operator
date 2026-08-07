# Neo4jReplicaDatabase

A read-only cross-cluster replica of a database hosted on **another** Neo4j cluster, fed by a differential backup chain in object storage.

## Overview

- **API version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jReplicaDatabase`
- **Scope**: Namespaced
- **Short names**: `n4jreplica`, `n4jreplicas`
- **Categories**: `neo4j`
- **Supported Neo4j Versions**: **2026.08 and later** on the downstream cluster (enforced). Upstream 2025.01+ (not enforced — the operator cannot inspect another cluster.)
- **Reconciliation**: replica existence and observed replication lag, via `SHOW DATABASES`

This CR is applied to the **downstream** cluster. The upstream lives in another Kubernetes cluster the operator cannot see; the only coupling between them is the object-storage location the upstream's `Neo4jBackup` chain writes to.

!!! warning "Backup mode only"

    `source.mode: network` is **rejected**. Network replication requires the upstream servers' `server.cluster.advertised_address` to be externally routable, and the operator pins those to in-cluster DNS — exposing a Service does not work around it, because the upstream hands the downstream the addresses to connect to. See `docs/design/cross-cluster-replication.md`.

See the [Cross-Cluster Replication guide](../user_guide/guides/cross_cluster_replication.md) for the end-to-end runbook.

## Spec

| Field | Type | Description |
|---|---|---|
| `clusterRef` | `string` | **Required.** Downstream `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone` in the same namespace that hosts the replica. |
| `name` | `string` | Replica database name. Defaults to `metadata.name`. **Permanent** — Cypher has no `RENAME DATABASE`, so this name survives promotion. Pair with a `Neo4jDatabaseAlias` if applications should address it under the upstream's name after failover. |
| `upstreamDatabase` | `string` | **Required.** Name of the database being replicated, as known on the upstream cluster. Becomes `replicaConfig.remote`. |
| `topology` | `object` | Distribution across downstream servers (`primaries`, `secondaries`). Both are read-only. |
| `source` | `object` | **Required.** Where the replica pulls from — see below. **Immutable**: Neo4j offers no way to re-point an existing replica, so a change would mean drop-and-reseed. Delete and recreate the CR instead. |
| `pullInterval` | `string` | How often the replica checks `pullURI` for new differentials (`db.cluster.backup.pull_interval`, default `1m`). Bounds the recovery point objective. Pattern `^[0-9]+(ms|s|m|h)$`. |
| `deletionPolicy` | `string` | `Delete` (default) drops the replica on CR deletion; `Retain` leaves it. **Ignored once promoted** — a promoted database is never dropped. |

### `source`

| Field | Type | Description |
|---|---|---|
| `mode` | `string` | `backup` (default) or `network`. Only `backup` is supported; `network` is rejected by the validator with the reason. |
| `pullURI` | `string` | Object-storage directory holding the upstream's differential chain. Read it from the upstream `Neo4jBackup` CR's `status.replicationPullURI`. |
| `seedURI` | `string` | Full backup artifact the replica is seeded from. Must belong to the same chain as `pullURI`. |
| `addresses` | `[]string` | Upstream cluster endpoints for network mode. Unused while network mode is rejected. |
| `credentialsSecretRef` | `string` | Secret holding object-storage credentials, when workload identity is unavailable. |

## Status

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | `Pending`, `Seeding`, `Replicating`, `Promoted`, or `Failed`. **`Promoted` is terminal.** |
| `message` | `string` | Short human-readable explanation of `phase`. |
| `observedGeneration` | `int64` | `metadata.generation` observed at the last successful reconcile. |
| `databaseType` | `string` | Live `type` column from `SHOW DATABASES` — `replica` while replicating, a standard type after promotion. This, not `phase`, is the operator's authoritative signal. |
| `lastCommittedTxn` | `int64` | Last transaction ID the replica has applied. |
| `replicationLag` | `int64` | Transactions behind — the data loss that promoting right now would make permanent. |
| `promotedAt` | `metav1.Time` | When the replica was promoted. |
| `promotedBy` | `string` | The `Neo4jReplicaPromotion` that promoted it, or `out-of-band`. |
| `conditions[]` | `metav1.Condition` | `Ready`, `ClusterNotReady`. |

## Promotion behaviour

Once the database is no longer a replica, this CR goes **inert**:

- The controller stops touching the database entirely — no create, no topology reconciliation, no drift correction.
- **Deleting the CR does not drop the database**, regardless of `deletionPolicy`.
- The guard is live state, not status: every mutating path re-reads `type` from `SHOW DATABASES` first. Promoting by hand at a `cypher-shell` is therefore safe — the operator notices and goes terminal rather than "correcting" the drift.

Promote via [`Neo4jReplicaPromotion`](neo4jreplicapromotion.md). Adopt the promoted database with a `Neo4jDatabase` CR using `ifNotExists: true`.

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jReplicaDatabase
metadata:
  name: foo-replica
  namespace: dr
spec:
  clusterRef: dr-cluster
  upstreamDatabase: foo
  topology:
    primaries: 3
    secondaries: 0
  pullInterval: 1m
  source:
    mode: backup
    pullURI: s3://prod-backups/foo/foo-chain/
    seedURI: s3://prod-backups/foo/foo-chain/foo-2026-08-01T02-00-00.backup
    credentialsSecretRef: s3-creds
```
