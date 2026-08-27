# Cross-cluster database replication (CCDR)

Replicate a database from one Neo4j cluster to a read-only replica on another,
for disaster recovery or to serve reads closer to a second region.

!!! info "Two modes — pick per replica"

    | | Backup mode (`source.mode: backup`) | Network mode (`source.mode: network`) |
    |---|---|---|
    | Data path | Differential backup chain in object storage | Direct stream from the upstream's cluster endpoints |
    | Network path between K8s clusters | **None** — no load balancer, no cross-cluster TLS trust, no NetworkPolicy changes | **Required between separate clusters** — upstream runs a self-hosted proxy (`spec.crossClusterReplication`), CA secrets exchanged both directions. Not needed at all if both clusters are on the *same* Kubernetes cluster ([§1c](#1c-same-kubernetes-cluster-network-mode-without-the-proxy)) |
    | Recovery point objective | Bounded by `pullInterval` (default 1m) | Near-continuous |
    | Upstream setup | A `Neo4jBackup` with `mode: replication-source` | `spec.crossClusterReplication.enabled: true` on the upstream cluster (skip if same Kubernetes cluster) |
    | Cost/latency tradeoff | None | Intra-cluster secondary catch-up traffic also routes through the load balancer while enabled (only applies when the proxy is in use) |

    Both are documented below. If you don't need near-continuous replication,
    prefer backup mode — it has no operational surface on the network side at
    all. See `docs/design/cross-cluster-replication.md` for the full design
    rationale, including why network mode is a proxy rather than exposed
    per-pod Services or split-horizon DNS.

**Requirements**

| | |
|---|---|
| Downstream (replica) cluster | Neo4j **2026.08+** — enforced |
| Upstream cluster | Neo4j 2025.01+ — not enforced (the operator cannot inspect another cluster) |
| Backup mode: shared object storage | S3, GCS or Azure Blob, reachable from both clusters |
| Network mode: load balancer support | The upstream's Kubernetes cluster must support `type: LoadBalancer` Services |

---

## How it fits together

Backup mode, shown below — the two clusters never talk to each other, the
bucket is the only coupling:

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

Network mode instead has a real network path, mediated by a proxy the
operator manages — no per-pod Services, no DNS setup required of you:

```
UPSTREAM cluster (K8s cluster A)                    DOWNSTREAM cluster (K8s cluster B)
┌───────────────────────────────────────┐            ┌──────────────────────────────────┐
│ Neo4jEnterpriseCluster                │            │ Neo4jEnterpriseCluster           │
│   database: foo                       │            │                                  │
│   crossClusterReplication.enabled     │            │ Neo4jReplicaDatabase             │
│                                        │            │   name: foo-replica  (read-only) │
│ ┌────────────┐   LoadBalancer   :6000  │  stream    │   source.mode: network           │
│ │ CCDR proxy │◄──────────────── server-0│◄───────────┤   source.addresses: [...]        │
│ │ (HAProxy)  │   :16000+i       server-N│            │                                  │
│ └────────────┘                         │            │ Neo4jDatabaseAlias               │
│   status.crossClusterReplication      ─┼────────────┤─► paste one address into         │
│   .addresses                          │             │   source.addresses               │
└───────────────────────────────────────┘             └──────────────────────────────────┘
```

RAFT (7000) and routing (7688) are never exposed by the proxy — only the
tx-shipping port (6000) that CCDR catchup rides.

If both clusters happen to live on the **same** Kubernetes cluster (different
namespaces), skip the proxy entirely — see [§1c](#1c-same-kubernetes-cluster-network-mode-without-the-proxy).

---

## 1. Upstream: produce the chain (backup mode)

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

## 1b. Upstream: expose the cluster (network mode)

Skip this section if you are using backup mode.

Enable the proxy on the **upstream** `Neo4jEnterpriseCluster`:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata:
  name: prod-cluster
  namespace: prod
spec:
  crossClusterReplication:
    enabled: true
  tls:
    additionalClusterTrustCAs:
      - name: dr-cluster-ca   # the DOWNSTREAM cluster's CA — copy it here
```

`additionalClusterTrustCAs` must be set on **both** clusters, each trusting the
other's CA — copy `prod-cluster`'s CA Secret into the `dr` namespace and vice
versa. This is separate from `trustedCASecrets`: that field feeds a JVM-wide
truststore the cluster SSL policy never reads.

Wait for the proxy's load balancer to be assigned, then read the address list:

```bash
kubectl get neo4jenterprisecluster prod-cluster -n prod \
  -o jsonpath='{.status.crossClusterReplication.addresses}'
# ["prod-cluster-ccdr.prod.svc.cluster.local:16000","...:16001","...:16002"]
```

(An in-cluster hostname is shown above for illustration; in practice this is
whatever hostname or IP your cloud provider's load balancer controller
assigns — often a public-facing name unless
`spec.crossClusterReplication.loadBalancerInternal` keeps it private, which is
the default.)

Only one entry from this list is needed on the downstream side — see step 2
below.

---

## 1c. Same Kubernetes cluster? Network mode without the proxy

If the upstream and downstream are both `Neo4jEnterpriseCluster` deployments
in different namespaces **on the same Kubernetes cluster**, skip §1b
entirely — you don't need `spec.crossClusterReplication` or a proxy. Kubernetes
DNS resolves across namespaces on one cluster by default, and Neo4j's
`server.cluster.advertised_address` (the pod's own FQDN) is already reachable
from any namespace on that cluster. This is a legitimate way to run a live
replica for isolation within one cluster — a reporting replica in its own
namespace, say — not only a way to try network mode before standing up a
second cluster.

Deploy the upstream `Neo4jEnterpriseCluster` normally; nothing extra to set.
Then create the replica with `source.upstreamClusterRef` naming it — the
controller resolves the address list itself, from the upstream's
`status.internalAddresses`, no copying or constructing anything by hand:

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

`namespace` defaults to the replica's own namespace if the upstream is in the
same one — set explicitly here since it isn't. If the upstream doesn't exist
yet, or exists but hasn't published `status.internalAddresses` yet, the
replica just sits in phase `Pending` and retries; nothing to do but wait.

!!! note "Prefer typing the address by hand instead?"

    `upstreamClusterRef` is resolved via a live `Get` against this Kubernetes
    cluster's own API server — which is exactly why it only ever works
    same-cluster. If you'd rather see the address explicit in the CR (or the
    operator's RBAC to read `Neo4jEnterpriseCluster` cross-namespace is a
    concern), every server pod's address follows a fixed, constructible
    pattern — use `source.addresses` instead:

    ```
    <upstream-cluster>-server-<ordinal>.<upstream-cluster>-headless.<upstream-namespace>.svc.cluster.local:6000
    ```

    ```yaml
    source:
      mode: network
      addresses: ["prod-cluster-server-0.prod-cluster-headless.prod.svc.cluster.local:6000"]
    ```

    Both forms resolve to the same address; `upstreamClusterRef` just saves
    you computing it and keeps it correct if the upstream's naming ever
    changes.

Two things differ from the cross-cluster path, for either form above:

- **TLS trust.** `spec.tls.additionalClusterTrustCAs` is only needed if the
  two clusters use *different* cert-manager issuers. If they share one
  issuer — and therefore the same root CA — each already trusts the other's
  certificate without it.
- **NetworkPolicy.** If the upstream has `spec.networkPolicy.enabled: true`,
  this will **not** work as-is. The generated policy restricts port 6000 to
  pods carrying that cluster's own `neo4j.com/cluster` label — the
  downstream's server pods don't carry it, even though they're on the same
  physical Kubernetes cluster. Either leave NetworkPolicy off on the
  upstream, or use the full `crossClusterReplication` proxy path (§1b)
  instead, which is specifically built to admit traffic from outside that
  peer set.

Everything else — the read-only replica, the failover alias (§3), user/role
replication (§4), and promotion (§5) — works exactly as documented below;
none of it depends on whether the two clusters are on one Kubernetes cluster
or two.

---

## 2. Downstream: create the replica

=== "Backup mode"

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

=== "Network mode"

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
        addresses: ["prod-cluster-ccdr.prod.svc.cluster.local:16000"]   # one entry from step 1b
    ```

    `pullInterval` is backup-mode only and has no effect here — network mode
    streams continuously, it does not poll.

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

**Network mode: enabling `crossClusterReplication` affects the whole cluster, not just replication.** The upstream's tx-shipping port (6000) is shared by intra-cluster
secondary catch-up and CCDR catchup. Enabling the proxy routes **both** through
the load balancer while it is on — added latency and LB-dependent cost
proportional to write volume, not just replication volume. This is why the
field defaults to disabled and is documented as an explicit tradeoff, not a
free toggle.

**Network mode: the proxy's load balancer hostname isn't known immediately.**
`status.crossClusterReplication.ready` stays `false` and `addresses` stays
empty until the cloud provider assigns the LoadBalancer Service a hostname/IP.
Until then the upstream keeps advertising its internal FQDN — cluster
formation is never blocked waiting for the proxy.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Failed`, "requires Neo4j 2026.08 or later" | downstream cluster predates replica support |
| `Failed`, "network replication requires at least one upstream cluster endpoint" | `source.addresses` is empty — paste an entry from the upstream's `status.crossClusterReplication.addresses` |
| `Failed`, "must be of the form host:port" | an entry in `source.addresses` is missing its port |
| Warning: "source.pullURI is ignored in network mode" (or `seedURI`/`credentialsSecretRef`) | those fields are backup-mode only; harmless but likely a copy-paste leftover — remove them |
| Network mode never connects, upstream `status.crossClusterReplication.ready` is `false` | the upstream's proxy LoadBalancer Service has no hostname/IP yet — check `kubectl get svc <cluster>-ccdr -n <upstream-ns>` |
| Network mode TLS handshake fails | `spec.tls.additionalClusterTrustCAs` missing on one or both clusters — it must be set on **both**, each trusting the other's CA |
| Network mode connection times out, both clusters on one Kubernetes cluster | the upstream has `spec.networkPolicy.enabled: true` — its port-6000 rule only admits pods carrying its own `neo4j.com/cluster` label; either disable NetworkPolicy on the upstream or use the proxy path (§1b) instead |
| `Pending`, event `UpstreamClusterNotFound` | `source.upstreamClusterRef` names a `Neo4jEnterpriseCluster` that doesn't exist (yet) in the given namespace — check the name/namespace, or wait if it's still being created |
| `Pending`, event `UpstreamClusterNotReady` | the referenced upstream exists but hasn't published `status.internalAddresses` yet — normal briefly after the upstream is first created; check `kubectl get neo4jenterprisecluster <name> -n <ns> -o jsonpath='{.status.internalAddresses}'` if it persists |
| `Pending`, cluster not Ready | downstream cluster still bootstrapping |
| Lag grows without bound | upstream backup CR not running — check its schedule and `status` |
| `Promoted` unexpectedly | someone promoted out of band; the CR is now inert by design |

```bash
kubectl describe neo4jreplicadatabase foo-replica -n dr
kubectl get events -n dr --field-selector reason=ReplicaPromotionDetected
kubectl get neo4jbackup foo-chain -n prod -o jsonpath='{.status.replicationPullURI}'
```

## See also

- `docs/design/cross-cluster-replication.md` — design rationale for both modes,
  including why network mode is a proxy rather than exposed per-pod Services
  or split-horizon DNS
- [`Neo4jEnterpriseCluster` API reference](../../api_reference/neo4jenterprisecluster.md#crossclusterreplicationspec) — `spec.crossClusterReplication` fields
- `docs/user_guide/guides/backup_restore.md` — the backup machinery this builds on
- Neo4j Operations Manual → Clustering → *Replicating databases across clusters*
