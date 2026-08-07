# Cross-cluster database replication (CCDR)

Replicate a database from one Neo4j cluster to a read-only replica on another,
for disaster recovery or to serve reads closer to a second region.

!!! info "What this operator supports"

    **Backup-based replication only.** The replica pulls a differential backup
    chain from object storage. It needs **no network path between the two
    Kubernetes clusters** — no load balancer, no cross-cluster TLS trust, no
    NetworkPolicy changes.

    **Network replication is not supported** and is rejected by the validator.
    It requires the upstream servers' `server.cluster.advertised_address` to be
    externally routable, and the operator pins those to in-cluster DNS. Exposing
    a Service does not work around it, because the upstream hands the downstream
    the addresses to connect to. See
    `docs/design/cross-cluster-replication.md` for the full analysis.

**Requirements**

| | |
|---|---|
| Downstream (replica) cluster | Neo4j **2026.08+** — enforced |
| Upstream cluster | Neo4j 2025.01+ — not enforced (the operator cannot inspect another cluster) |
| Shared object storage | S3, GCS or Azure Blob, reachable from both clusters |

---

## How it fits together

```
UPSTREAM cluster (K8s cluster A)          DOWNSTREAM cluster (K8s cluster B)
┌──────────────────────────────┐          ┌──────────────────────────────────┐
│ Neo4jEnterpriseCluster       │          │ Neo4jEnterpriseCluster           │
│   database: foo              │          │                                  │
│                              │          │ Neo4jReplicaDatabase             │
│ Neo4jBackup                  │  s3://   │   name: foo-replica  (read-only) │
│   mode: replication-source   │ ───────► │   source.pullURI: s3://…         │
│   schedule: hourly DIFF      │  chain   │                                  │
│   status.replicationPullURI ─┼──────────┤─► paste into source.pullURI      │
└──────────────────────────────┘          │                                  │
                                          │ Neo4jDatabaseAlias               │
                                          │   name: foo → foo-replica        │
                                          └──────────────────────────────────┘
```

The two clusters never talk to each other. The only coupling is the bucket.

---

## 1. Upstream: produce the chain

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jBackup
metadata:
  name: foo-chain
  namespace: prod
spec:
  mode: replication-source        # turns chain-breaking configs into errors
  instanceRef: prod-cluster
  database: foo                   # single-database scope is required
  schedule: "0 * * * *"           # hourly; bounds the replica's staleness
  options:
    backupType: AUTO
    preferDiffAsParent: true
  storage:
    type: s3
    bucket: prod-backups
    path: foo
    cloud:
      provider: aws
      credentialsSecretRef: s3-creds
```

`mode: replication-source` enforces:

| Rule | Why |
|---|---|
| single-database scope | an instance-wide backup's layout can't be consumed by a per-database `pullURI` |
| no `spec.retention` | pruning a differential's parent breaks the chain |
| no competing writer to the same `storage` | a second CR interleaving artifacts breaks the chain |
| `schedule` required | without one the chain never advances and lag grows without bound |

Read the pull URI back once the first backup has run:

```bash
kubectl get neo4jbackup foo-chain -n prod -o jsonpath='{.status.replicationPullURI}'
# s3://prod-backups/foo/foo-chain/
```

!!! danger "The operator cannot protect the chain from your bucket lifecycle rules"

    For cloud storage the operator **never prunes** — retention is delegated to
    bucket lifecycle rules, which it can neither read nor validate. A lifecycle
    rule that expires objects in this directory **will silently break the
    differential chain** and force the replica to be rebuilt from scratch.

    Exclude the replication-source path from lifecycle expiry. No operator-side
    validation can catch this for you.

### Mixed cadence (recommended for large databases)

A daily FULL plus an hourly DIFF, sharing one directory:

```yaml
# Daily full — the chain root
spec: { mode: replication-source, schedule: "0 2 * * *", options: { backupType: FULL }, … }
---
# Hourly differential — chains off it
spec: { chainFromBackup: foo-chain, schedule: "0 * * * *", options: { backupType: DIFF }, … }
```

`chainFromBackup` makes both write into the same directory, and the operator
refuses to run them concurrently. This is *not* flagged as a competing writer.

---

## 2. Downstream: create the replica

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
  pullInterval: 1m                # bounds the recovery point objective
  source:
    mode: backup
    pullURI: s3://prod-backups/foo/foo-chain/        # from step 1
    seedURI: s3://prod-backups/foo/foo-chain/foo-2026-08-01T02-00-00.backup
    credentialsSecretRef: s3-creds
```

`spec.source` is **immutable**. Neo4j offers no way to re-point an existing
replica, so changing it would mean dropping and re-seeding — delete and recreate
the CR instead if you need to.

```bash
kubectl get neo4jreplicadatabase -n dr
# NAME          CLUSTER      UPSTREAM   PHASE         LAG   AGE
# foo-replica   dr-cluster   foo        Replicating   3     12m
```

**Replicas are read-only.** Both primaries and secondaries refuse writes.
Clients must use `AccessMode.READ` / `executeRead`, or
`cypher-shell --access-mode=read`.

---

## 3. Downstream: pre-stage the failover alias

Cypher has **no `RENAME DATABASE`**, so `foo-replica` keeps that name forever —
including after promotion. An alias lets applications address `foo` throughout:

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

| | `foo` on the DR cluster resolves to | Clients get |
|---|---|---|
| Steady state | `foo-replica`, a read-only replica | reads |
| After promotion | `foo-replica`, now a standard database | reads **and writes** |

Create this **now, not during the outage**. Aliases can target a database that
is still a replica, so there is nothing to do inside the failover window — the
same connection string silently gains write capability the moment promotion
completes.

---

## 4. Downstream: replicate users and roles

Privileges and roles are **not** replicated by CCDR. Apply the same
`Neo4jUser` / `Neo4jRole` / `Neo4jRoleBinding` CRs to the downstream cluster
that you apply upstream — with `clusterRef` pointed at the DR cluster. The
operator makes this a copy-paste rather than a manual re-creation.

---

## 5. Failover: promoting the replica

!!! danger "Promotion is irreversible"

    A promoted database **cannot be re-attached** to its upstream. Any
    replication lag outstanding at that moment becomes **permanent data loss**.
    Check `status.replicationLag` first if the upstream is still reachable.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jReplicaPromotion
metadata:
  name: failover-2026-08-07
  namespace: dr
spec:
  replicaRef: foo-replica
  topology:                       # optional; omit to retain current topology
    primaries: 3
    secondaries: 1
```

```bash
kubectl get neo4jreplicapromotion -n dr
# NAME                  REPLICA       PHASE       LAGTAKEN   COMPLETED
# failover-2026-08-07   foo-replica   Completed   3          2026-08-07T09:14:22Z
```

`status.observedLagTxIds` records the RPO the promotion actually took, so it is
auditable after the incident. The operator records it; it never blocks a
failover on it.

### Why promotion is a separate CR, not a field

A `promote: true` field in a level-triggered spec could be set back to `false`,
which would mean "make it a replica again" — impossible in Neo4j, and honourable
only by dropping and re-seeding. More importantly, a GitOps controller
re-applying the pre-promotion manifest after a failover is byte-identical to a
deliberate revert. A one-shot CR is inert to re-apply.

### What happens to the Neo4jReplicaDatabase CR

It **stays, and goes inert**:

- `status.phase: Promoted` — terminal. The controller stops touching the
  database entirely: no create, no topology reconciliation, no drift correction.
- **Deleting it will NOT drop the database.** Once promoted, the finalizer
  releases without dropping regardless of `deletionPolicy`. A promoted database
  is your live system; removing a CR that no longer describes it must not be a
  data-loss event.
- The guard is live state, not status: before any mutating action the controller
  re-reads the database's `type` from `SHOW DATABASES`. **Promoting by hand at a
  `cypher-shell` is therefore safe too** — the operator notices and goes
  terminal rather than "correcting" the drift by dropping your database.

### Handing back to declarative management

The promoted database is now an ordinary standard database. Adopt it with a
`Neo4jDatabase` CR — `ifNotExists: true` makes `CREATE DATABASE` a no-op:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata: { name: foo-replica, namespace: dr }
spec:
  clusterRef: dr-cluster
  name: foo-replica
  ifNotExists: true
```

The `Neo4jReplicaDatabase` CR can then be deleted (harmlessly) or kept as the
record of where the database came from. Both CRs may coexist safely: the replica
CR's finalizer no longer drops, so only `Neo4jDatabase` can.

---

## Hazards worth knowing

**Restoring or recreating the upstream database detaches every replica.**
A restore changes the database's internal ID; downstream replicas detach and
must be recreated from a fresh chain. `Neo4jRestore` has no way to know replicas
exist — they are in a different Kubernetes cluster with no back-reference. If
you restore an upstream, plan to rebuild its replicas.

**A broken chain requires a rebuild, not a repair.** If the differential chain
is interrupted — a lifecycle rule, a competing writer, a deleted artifact — the
replica cannot resume. Delete the `Neo4jReplicaDatabase` and recreate it from a
complete chain.

**The seed must belong to the chain.** A `seedURI` outside the `pullURI`
directory produces a replica that seeds successfully and then can never apply a
differential. The validator warns about this; it cannot verify it, because the
operator cannot see the bucket.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Failed`, "requires Neo4j 2026.08 or later" | downstream cluster predates replica support |
| `Failed`, "network replication is not supported" | set `source.mode: backup` |
| `Pending`, cluster not Ready | downstream cluster still bootstrapping |
| Lag grows without bound | upstream backup CR not running — check its schedule and `status` |
| `Promoted` unexpectedly | someone promoted out of band; the CR is now inert by design |

```bash
kubectl describe neo4jreplicadatabase foo-replica -n dr
kubectl get events -n dr --field-selector reason=ReplicaPromotionDetected
kubectl get neo4jbackup foo-chain -n prod -o jsonpath='{.status.replicationPullURI}'
```

## See also

- `docs/design/cross-cluster-replication.md` — design rationale, and why network
  replication is not supported
- `docs/user_guide/guides/backup_restore.md` — the backup machinery this builds on
- Neo4j Operations Manual → Clustering → *Replicating databases across clusters*
