# Neo4jReplicaPromotion

A one-shot, **irreversible** promotion of a cross-cluster replica into an ordinary read-write database — the disaster-recovery failover action.

## Overview

- **API version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jReplicaPromotion`
- **Scope**: Namespaced
- **Short names**: `n4jpromo`, `n4jpromotions`
- **Categories**: `neo4j`
- **Supported Neo4j Versions**: 2026.08 and later
- **Reconciliation**: one-shot. `Completed` and `Failed` are terminal; there is **no finalizer**.

!!! danger "Promotion cannot be undone"

    A promoted database **cannot be re-attached** to its upstream. Any replication lag outstanding at that moment becomes **permanent data loss**. Check the replica's `status.replicationLag` first if the upstream is still reachable.

## Why a separate CR rather than a field

A `promote: true` field in a level-triggered spec could be set back to `false`, which would mean "make it a replica again" — impossible in Neo4j, and honourable only by dropping and re-seeding. A CEL immutability rule closes the interactive path but not the one that matters: **a GitOps controller re-applying the pre-promotion manifest after a failover is byte-identical to a deliberate revert.** A one-shot CR is inert to re-apply, and matches `Neo4jBackup` / `Neo4jRestore`.

## Spec

| Field | Type | Description |
|---|---|---|
| `replicaRef` | `string` | **Required.** Name of the `Neo4jReplicaDatabase` CR in the same namespace to promote. Immutable. |
| `topology` | `object` | Optionally change the database's topology as part of promotion (`primaries`, `secondaries`), passed to `dbms.promoteReplicaDatabase`. Omit to retain the replica's existing topology. Immutable. |

## Status

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | `Pending`, `Promoting`, `Completed`, or `Failed`. `Completed` and `Failed` are terminal. |
| `message` | `string` | Short human-readable explanation of `phase`. |
| `observedGeneration` | `int64` | `metadata.generation` observed at the last reconcile. |
| `completionTime` | `metav1.Time` | When the promotion reached a terminal phase. |
| `observedLagTxIds` | `int64` | The replica's replication lag immediately **before** promotion — the recovery point this promotion made permanent. Recorded, never enforced. |
| `lastCommittedTxn` | `int64` | Last transaction the replica had applied at promotion time. |
| `promotedDatabase` | `string` | The database name that was promoted. |
| `conditions[]` | `metav1.Condition` | `Ready`. |

`observedLagTxIds` exists so the RPO actually taken is auditable after an incident. The operator deliberately does **not** block a failover on a lag threshold: during a real outage it does not get to second-guess the human.

## Crash safety

The reconcile ordering is **CHECK → PROMOTE → CHECK → RECORD**. If the process dies between the procedure call and the status write, the next reconcile observes a database whose `type` is no longer `replica`, concludes the promotion succeeded, and records it — rather than calling the procedure a second time. A promotion applied against an already-promoted database is therefore recorded as an idempotent success, not an error.

## Effect on the replica CR

On completion the referenced `Neo4jReplicaDatabase` is driven to `status.phase: Promoted`, which is terminal: it stops managing the database, and its finalizer stops dropping. See [Neo4jReplicaDatabase](neo4jreplicadatabase.md).

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jReplicaPromotion
metadata:
  name: failover-2026-08-07
  namespace: dr
spec:
  replicaRef: foo-replica
  topology:
    primaries: 3
    secondaries: 1
```

```console
$ kubectl get neo4jreplicapromotion -n dr
NAME                  REPLICA       PHASE       LAGTAKEN   COMPLETED
failover-2026-08-07   foo-replica   Completed   3          2026-08-07T09:14:22Z
```
