# Neo4jDatabaseAlias

A **local database alias** — a second name for an existing database in the same DBMS.

## Overview

- **API version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jDatabaseAlias`
- **Scope**: Namespaced
- **Short names**: `n4jalias`, `n4jaliases`
- **Categories**: `neo4j`
- **Supported Neo4j Versions**: 5.26 LTS and all CalVer
- **Reconciliation**: alias existence and target drift, via `SHOW ALIASES FOR DATABASE`

Scope: **local aliases only**. Remote aliases (`... AT '<url>' USER ... PASSWORD ...`) and composite-database constituents are not modelled.

## Why this exists

Cypher has **no `RENAME DATABASE`**. The motivating case is cross-cluster replication failover: a replica created as `foo-replica` keeps that name forever, including after promotion. An alias lets applications address `foo` on the downstream cluster throughout:

| | `foo` resolves to | Clients get |
|---|---|---|
| Steady state | `foo-replica`, a read-only replica | reads |
| After promotion | `foo-replica`, now a standard database | reads **and writes** |

Because an alias may target a database that is still a replica, **create it at replica-setup time, not during the failover window** — the same connection string silently gains write capability at promotion, with nothing to run inside the outage.

## Spec

| Field | Type | Description |
|---|---|---|
| `clusterRef` | `string` | **Required.** `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone` in the same namespace hosting both alias and target. |
| `name` | `string` | Alias name in Neo4j. Defaults to `metadata.name`. 3–63 chars, `^[a-zA-Z][a-zA-Z0-9_.\-]*$`. May not be `system`. |
| `targetDatabase` | `string` | **Required.** Database the alias points at. May be a standard database or a cross-cluster replica; the alias survives promotion unchanged. May not be `system`, and may not equal the alias name. |
| `deletionPolicy` | `string` | `Delete` (default) drops the alias on CR deletion; `Retain` leaves it and only releases the finalizer. |

The target does **not** have to exist yet — the controller reports `Pending` and retries, so an alias and the replica it fronts can be applied together.

## Status

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | `Pending`, `Ready`, or `Failed`. |
| `message` | `string` | Short human-readable explanation of `phase`. |
| `observedGeneration` | `int64` | `metadata.generation` observed at the last successful reconcile. |
| `observedTarget` | `string` | Database the alias actually resolves to, read back from `SHOW ALIASES FOR DATABASE`. Differs from `spec.targetDatabase` only between an out-of-band change and the next reconcile. |
| `conditions[]` | `metav1.Condition` | `Ready`, `ClusterNotReady`. |

Drift is reconciled: if the alias is re-pointed out of band, the controller issues `ALTER ALIAS ... SET DATABASE TARGET` to restore `spec.targetDatabase`.

Dropping an alias never affects the database behind it.

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabaseAlias
metadata:
  name: foo
  namespace: dr
spec:
  clusterRef: dr-cluster
  targetDatabase: foo-replica
```

## See also

- [Neo4jReplicaDatabase](neo4jreplicadatabase.md)
- [Cross-Cluster Replication guide](../user_guide/guides/cross_cluster_replication.md)
