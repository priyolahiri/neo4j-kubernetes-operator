# Neo4jReplicaDatabase

A read-only cross-cluster replica of a database hosted on **another** Neo4j cluster, fed either by a differential backup chain in object storage (`mode: backup`) or by streaming directly from the upstream's cluster endpoints (`mode: network`).

## Overview

- **API version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jReplicaDatabase`
- **Scope**: Namespaced
- **Short names**: `n4jreplica`, `n4jreplicas`
- **Categories**: `neo4j`
- **Supported Neo4j Versions**: **2026.08 and later** on the downstream cluster (enforced). Upstream 2025.01+ (not enforced — the operator cannot inspect another cluster.)
- **Reconciliation**: replica existence and observed replication lag, via `SHOW DATABASES`

This CR is applied to the **downstream** cluster. The upstream lives in another Kubernetes cluster the operator cannot see; the only coupling between them is the object-storage location the upstream's `Neo4jBackup` chain writes to.

!!! note "Network mode requires the upstream to expose itself — unless it's the same Kubernetes cluster"

    `source.mode: network` streams directly from the upstream's cluster endpoints. Between **separate** Kubernetes clusters, this requires the upstream servers' `server.cluster.advertised_address` to be externally routable: set `spec.crossClusterReplication.enabled: true` on the upstream `Neo4jEnterpriseCluster` (see [`CrossClusterReplicationSpec`](neo4jenterprisecluster.md#crossclusterreplicationspec)) and list its `status.crossClusterReplication.addresses` in `source.addresses`. When both clusters are on the **same** Kubernetes cluster, skip all of that and set `source.upstreamClusterRef` instead — the controller resolves the address list itself. `mode: backup` needs no network setup of either kind — it has no network path between clusters at all. See `docs/design/cross-cluster-replication.md` §6.

See the [Cross-Cluster Replication guide](../user_guide/guides/cross_cluster_replication.md) for the end-to-end runbook (both modes).

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
| `mode` | `string` | `backup` (default) or `network`. |
| `pullURI` | `string` | **Backup mode.** Object-storage directory holding the upstream's differential chain. Read it from the upstream `Neo4jBackup` CR's `status.replicationPullURI`. Mutually exclusive with `upstreamBackupRef` — set exactly one. Ignored (warned) in network mode. |
| `upstreamBackupRef` | [`object`](#upstreambackupref) | **Backup mode.** Resolves `pullURI` automatically from an upstream `Neo4jBackup` CR's `status.replicationPullURI`, instead of typing it by hand. Only works when the upstream `Neo4jBackup` is on this **same Kubernetes cluster** — see [`UpstreamBackupRef`](#upstreambackupref). Mutually exclusive with `pullURI`. |
| `seedURI` | `string` | **Backup mode.** Full backup artifact the replica is seeded from. Must belong to the same chain as `pullURI`. Independent of `upstreamBackupRef` — set either alongside it. Ignored (warned) in network mode. |
| `addresses` | `[]string` | **Network mode.** Upstream cluster endpoints (`host:port`, the upstream's port 6000 — or, when the upstream has `spec.crossClusterReplication` enabled, its proxy's external port). One reachable address is sufficient: the upstream hands back the addresses the downstream actually uses. Mutually exclusive with `upstreamClusterRef` — set exactly one. Ignored in backup mode. |
| `upstreamClusterRef` | [`object`](#upstreamclusterref) | **Network mode.** Resolves `addresses` automatically from an upstream `Neo4jEnterpriseCluster`'s `status.internalAddresses`, instead of listing them by hand. Only works when the upstream is on this **same Kubernetes cluster** — see [`UpstreamClusterRef`](#upstreamclusterref). Mutually exclusive with `addresses`. |
| `credentialsSecretRef` | `string` | **Backup mode.** Secret holding object-storage credentials, when workload identity is unavailable. Independent of `upstreamBackupRef`. Ignored (warned) in network mode. |

### `upstreamClusterRef`

| Field | Type | Description |
|---|---|---|
| `name` | `string` | **Required.** Name of the upstream `Neo4jEnterpriseCluster`. |
| `namespace` | `string` | Namespace of the upstream `Neo4jEnterpriseCluster`. Defaults to this `Neo4jReplicaDatabase`'s own namespace when omitted. |

Resolution is a live `Get` against this operator's own Kubernetes API server — it can **only** reach an upstream on this same Kubernetes cluster, never a genuinely separate physical cluster. For an upstream on a different Kubernetes cluster, use `addresses` instead, populated from the upstream's `spec.crossClusterReplication` proxy (see the note above). If the referenced cluster doesn't exist yet, or exists but hasn't published `status.internalAddresses` yet, the replica sits in phase `Pending` and retries — this is treated as an ordinary transient state, not a spec error.

### `upstreamBackupRef`

| Field | Type | Description |
|---|---|---|
| `name` | `string` | **Required.** Name of the upstream `Neo4jBackup` CR. |
| `namespace` | `string` | Namespace of the upstream `Neo4jBackup`. Defaults to this `Neo4jReplicaDatabase`'s own namespace when omitted. |

Same same-Kubernetes-cluster-only constraint as `upstreamClusterRef`, for the same reason: resolution is a live `Get` against this operator's own API server. For a `Neo4jBackup` on a different Kubernetes cluster, use `pullURI` instead — paste it from that CR's own `status.replicationPullURI` by hand. If the referenced `Neo4jBackup` doesn't exist yet, or exists but hasn't run its first backup yet (empty `status.replicationPullURI`), the replica sits in phase `Pending` and retries.

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

Backup mode, same Kubernetes cluster — `upstreamBackupRef` resolves `pullURI` automatically from the upstream `Neo4jBackup`'s `status.replicationPullURI`; no `kubectl get ... -o jsonpath=...` copy-paste needed:

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
    upstreamBackupRef:
      name: foo-chain
      namespace: prod
    credentialsSecretRef: s3-creds
```

Network mode — `addresses` comes from the upstream cluster's `status.crossClusterReplication.addresses` (see [`CrossClusterReplicationSpec`](neo4jenterprisecluster.md#crossclusterreplicationspec)):

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
  source:
    mode: network
    addresses: ["prod-ccdr.example.com:16000"]
```

Network mode, same Kubernetes cluster — `upstreamClusterRef` resolves addresses automatically from the upstream's `status.internalAddresses`; no `crossClusterReplication` proxy needed:

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
  source:
    mode: network
    upstreamClusterRef:
      name: prod-cluster
      namespace: prod
```
