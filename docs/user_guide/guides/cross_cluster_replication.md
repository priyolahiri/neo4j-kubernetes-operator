# Cross-cluster database replication (CCDR)

Replicate a database from one Neo4j cluster to a read-only replica on another,
for disaster recovery or to serve reads closer to a second region.

## Which configuration do you need?

Two independent questions decide your setup: **where** the two clusters live,
and **how fresh** the replica needs to be.

| Your situation | Mode | Field you set |
|---|---|---|
| Separate Kubernetes clusters, ~1 minute of lag is fine (most DR) | `source.mode: backup` | `source.pullURI` |
| Separate Kubernetes clusters, need near-continuous replication | `source.mode: network` | `source.addresses` (upstream also needs `spec.crossClusterReplication`) |
| Same Kubernetes cluster (different namespaces), ~1 minute of lag is fine | `source.mode: backup` | `source.upstreamBackupRef` |
| Same Kubernetes cluster, need near-continuous replication | `source.mode: network` | `source.upstreamClusterRef` |

**Unsure?** Start with backup mode. It never needs any network setup — same
Kubernetes cluster or not — and a bounded ~1 minute RPO is enough for most
disaster-recovery scenarios. Reach for network mode only when you specifically
need near-continuous replication.

Step 1 below is organized the same way: pick your **mode** tab, then your
**topology** paragraph inside it.

---

## How the two modes differ

| | Backup mode | Network mode |
|---|---|---|
| Data path | Differential backup chain in object storage | Direct stream from the upstream's cluster endpoints |
| Recovery point objective | Bounded by `pullInterval` (default `1m`) | Near-continuous |
| Network path, separate Kubernetes clusters | **None** — no load balancer, no cross-cluster TLS trust, no NetworkPolicy changes | A self-hosted proxy (`spec.crossClusterReplication`) plus CA exchange — see step 1 |
| Cost/latency tradeoff | None | Intra-cluster secondary catch-up traffic also shares the load balancer while the proxy is in use |

See `docs/design/cross-cluster-replication.md` for the full design rationale,
including why network mode uses a proxy rather than exposed per-pod Services
or split-horizon DNS.

## Requirements

| | |
|---|---|
| Downstream (replica) cluster | Neo4j **2026.08+** — enforced |
| Upstream cluster | Neo4j 2025.01+ — not enforced (the operator cannot inspect another cluster) |
| Backup mode | Shared object storage (S3, GCS, or Azure Blob) reachable from both clusters — **and the downstream cluster's `spec.env` must carry the credentials**, because the seed and every pull run on the Neo4j server itself. `kubectl neo4j preflight` checks this before you apply; see [step 2](#2-downstream-create-the-replica) |
| Network mode, **separate** Kubernetes clusters | The upstream's Kubernetes cluster must support `type: LoadBalancer` Services |
| Network mode, **same** Kubernetes cluster | No extra requirement — ordinary in-cluster DNS already reaches across namespaces |

---

## How it fits together

Backup mode, separate clusters — the two clusters never talk to each other,
the bucket is the only coupling:

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

Network mode, separate clusters — a real network path, mediated by a proxy
the operator manages — no per-pod Services, no DNS setup required of you:

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

**Same Kubernetes cluster, either mode:** simpler than both diagrams above —
one Kubernetes cluster, two namespaces, no bucket-only or proxy-only
choreography needed at all. Backup mode: point `upstreamBackupRef` at the
`Neo4jBackup` in the other namespace. Network mode: point `upstreamClusterRef`
at the `Neo4jEnterpriseCluster` in the other namespace. Full walkthroughs are
in step 1's "Same Kubernetes cluster" paragraphs, below.

---

## 1. Upstream: set it up

=== "Backup mode"

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

    **Separate Kubernetes clusters:** read the pull URI back once the first
    backup has run, and paste it into `source.pullURI` in step 2:

    ```bash
    kubectl get neo4jbackup foo-chain -n prod -o jsonpath='{.status.replicationPullURI}'
    # s3://prod-backups/foo/foo-chain/
    ```

    **Same Kubernetes cluster:** skip the `kubectl get` step above entirely.
    In step 2, set `source.upstreamBackupRef: {name: foo-chain, namespace:
    prod}` instead of `source.pullURI` — the controller resolves it
    automatically. Same constraint as network mode's `upstreamClusterRef`
    (see the Network mode tab): resolution is a live `Get` against this
    cluster's own API server, so it only works same-cluster. For a backup CR
    on a different Kubernetes cluster, `pullURI` (typed by hand) remains the
    only option.

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

=== "Network mode"

    **Separate Kubernetes clusters:** enable the proxy on the **upstream**
    `Neo4jEnterpriseCluster`:

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
        mode: cert-manager        # REQUIRED — see the warning below
        issuerRef:
          name: ca-cluster-issuer
          kind: ClusterIssuer
        additionalClusterTrustCAs:
          - name: dr-cluster-ca   # the DOWNSTREAM cluster's CA — copy it here
    ```

    !!! danger "Do not enable the proxy without TLS"

        The proxy is a TCP passthrough: it terminates nothing and
        authenticates nothing. **Every access control on the exposed port is
        Neo4j's cluster SSL policy** — `client_auth=REQUIRE` with
        `trust_all=false`, which is what `tls.mode: cert-manager` plus the
        default `strictPeerValidation: true` turns on. With `spec.tls` unset,
        the same load balancer fronts an unauthenticated, unencrypted
        transaction-shipping port, and anyone who can reach it can stream the
        database. The operator does not refuse this — you may be terminating
        elsewhere, or deliberately inside a private network — but it says so:
        the cluster reports `CrossClusterProxySecure=False` with reason
        `NoClusterTLS`, and emits a `CrossClusterProxyUnauthenticated` warning
        event when the load balancer address is first published.

        `additionalClusterTrustCAs` is read only when `tls.mode` is
        `cert-manager`. Set it without `mode` and the peer CAs are silently
        ignored — the field looks configured and does nothing.

    `additionalClusterTrustCAs` must be set on **both** clusters, each trusting
    the other's CA — copy `prod-cluster`'s CA Secret into the `dr` namespace
    and vice versa. This is separate from `trustedCASecrets`: that field feeds
    a JVM-wide truststore the cluster SSL policy never reads.

    Wait for the proxy's load balancer to be assigned, then read the address list:

    ```bash
    kubectl get neo4jenterprisecluster prod-cluster -n prod \
      -o jsonpath='{.status.crossClusterReplication.addresses}'
    # ["prod-cluster-ccdr.prod.svc.cluster.local:16000","...:16001","...:16002"]
    ```

    (An in-cluster hostname is shown above for illustration; in practice this
    is whatever hostname or IP your cloud provider's load balancer controller
    assigns — often a public-facing name unless
    `spec.crossClusterReplication.loadBalancerInternal` keeps it private,
    which is the default.) Only one entry from this list is needed on the
    downstream side — see step 2.

    **Same Kubernetes cluster:** skip `crossClusterReplication` and the proxy
    entirely — you don't need either. Kubernetes DNS resolves across
    namespaces on one cluster by default, and Neo4j's
    `server.cluster.advertised_address` (the pod's own FQDN) is already
    reachable from any namespace on that cluster. This is a legitimate way to
    run a live replica for isolation within one cluster — a reporting replica
    in its own namespace, say — not only a way to try network mode before
    standing up a second cluster.

    Deploy the upstream `Neo4jEnterpriseCluster` normally — nothing extra to
    set — then in step 2 use `source.upstreamClusterRef` naming it instead of
    `source.addresses`. The controller resolves the address list itself, from
    the upstream's `status.internalAddresses`, no copying or constructing
    anything by hand. `namespace` defaults to the replica's own namespace if
    the upstream is in the same one. If the upstream doesn't exist yet, or
    exists but hasn't published `status.internalAddresses` yet, the replica
    just sits in phase `Pending` and retries; nothing to do but wait.

    !!! note "Prefer typing the address by hand instead?"

        `upstreamClusterRef` is resolved via a live `Get` against this
        Kubernetes cluster's own API server — which is exactly why it only
        ever works same-cluster. If you'd rather see the address explicit in
        the CR (or the operator's RBAC to read `Neo4jEnterpriseCluster`
        cross-namespace is a concern), every server pod's address follows a
        fixed, constructible pattern — use `source.addresses` instead:

        ```
        <upstream-cluster>-server-<ordinal>.<upstream-cluster>-headless.<upstream-namespace>.svc.cluster.local:6000
        ```

        ```yaml
        source:
          mode: network
          addresses: ["prod-cluster-server-0.prod-cluster-headless.prod.svc.cluster.local:6000"]
        ```

        Both forms resolve to the same address; `upstreamClusterRef` just
        saves you computing it and keeps it correct if the upstream's naming
        ever changes.

    Two things differ from the separate-clusters path above, for either
    addressing form:

    - **TLS trust.** `spec.tls.additionalClusterTrustCAs` is only needed if
      the two clusters use *different* cert-manager issuers. If they share
      one issuer — and therefore the same root CA — each already trusts the
      other's certificate without it.
    - **NetworkPolicy.** If the upstream has `spec.networkPolicy.enabled:
      true`, this will **not** work as-is. The generated policy restricts
      port 6000 to pods carrying that cluster's own `neo4j.com/cluster`
      label — the downstream's server pods don't carry it, even though
      they're on the same physical Kubernetes cluster. Fix it with an
      explicit, opt-in allow-list entry rather than disabling NetworkPolicy
      entirely:

      ```yaml
      spec:
        networkPolicy:
          enabled: true
          allowReplicasFrom:
            - name: dr-cluster
              namespace: dr
      ```

      This admits only the named downstream cluster, on port 6000 only —
      never RAFT or routing. Nothing is admitted unless listed here.
      (Leaving NetworkPolicy off on the upstream, or using the full
      `crossClusterReplication` proxy path above, both still work too.)

Everything from step 2 onward — the read-only replica, the failover alias
(step 3), user/role replication (step 4), and promotion (step 5) — works
exactly the same regardless of mode or topology.

---

## 2. Downstream: create the replica

!!! warning "Backup mode: the DOWNSTREAM CLUSTER needs the bucket credentials, not the replica CR"

    The seed and every subsequent pull are performed by the Neo4j **server**,
    not by a Job — so the credentials have to be in the server's own
    environment, where the AWS SDK's default chain finds them. Put them on the
    downstream `Neo4jEnterpriseCluster` **before** you create the replica:

    ```yaml
    apiVersion: neo4j.neo4j.com/v1beta1
    kind: Neo4jEnterpriseCluster
    metadata:
      name: dr-cluster
      namespace: dr
    spec:
      env:
        - name: AWS_REGION
          valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_REGION}}
        - name: AWS_ACCESS_KEY_ID
          valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_ACCESS_KEY_ID}}
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_SECRET_ACCESS_KEY}}
        # S3-compatible stores (MinIO, Ceph, LocalStack) only:
        - name: AWS_ENDPOINT_URL_S3
          value: http://minio.minio.svc:9000
    ```

    **Or let the operator do it**: set `source.credentialsSecretRef` on the
    replica and it projects that Secret's keys onto the cluster for you, as
    `secretKeyRef` references rather than literals. That restarts the
    downstream servers — the replica waits for the rollout before creating
    anything — so it is still something to do when the downstream is built
    rather than mid-failover.

    Either way the operator checks before asking Neo4j: a backup-mode replica
    whose cluster has no `AWS_REGION` fails immediately, naming what is
    missing, instead of stalling in `Seeding` while the server refuses deep
    inside the AWS SDK. Two replicas naming different Secrets for one cluster
    are refused rather than fought over.

    Adding these to a live cluster restarts its servers, so set them when the
    downstream is created rather than in the middle of a failover drill.

    **Network mode needs none of this** — it reads from the upstream over the
    wire, not from a bucket.

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

    Same Kubernetes cluster as the upstream `Neo4jBackup`? Use
    `upstreamBackupRef` instead of `pullURI` and skip the `kubectl get`
    step from step 1:

    ```yaml
      source:
        mode: backup
        upstreamBackupRef: { name: foo-chain, namespace: prod }
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
        addresses: ["prod-cluster-ccdr.prod.svc.cluster.local:16000"]   # one entry from step 1
    ```

    `pullInterval` is backup-mode only and has no effect here — network mode
    streams continuously, it does not poll.

    Same Kubernetes cluster as the upstream `Neo4jEnterpriseCluster`? Use
    `upstreamClusterRef` instead of `addresses`:

    ```yaml
      source:
        mode: network
        upstreamClusterRef: { name: prod-cluster, namespace: prod }
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

Users, roles, and privileges are **not** replicated by CCDR — you apply the
same CRs to the downstream cluster yourself, with `clusterRef` pointed at the
DR cluster.

!!! danger "`Neo4jRole` privileges do NOT copy verbatim — they name the database"

    The replica database is called `foo-replica`, not `foo` (see [step 2](#2-downstream-create-the-replica):
    Cypher has no `RENAME DATABASE`, so it keeps that name permanently). And
    **privileges attach to the database, not the alias** — see
    [Aliases and privileges](database_aliases.md#aliases-and-privileges).

    So a privilege copied verbatim grants access to a database that does not
    exist on the DR cluster. **Neo4j accepts this without complaint**: the
    `Neo4jRole` reconciles to `Ready`, `enforcePrivileges: true` holds it
    there, and your dashboards stay green while DR authorization is silently
    broken — the exact failure DR exists to prevent.

    Rewrite every database name in `spec.privileges` to the replica's name.

What copies, and what does not:

| CR | Copies verbatim? | Why |
|---|---|---|
| `Neo4jRoleBinding` | ✅ Yes | names only users and roles — no database names |
| `Neo4jUser` | ✅ Yes | `spec.roles` are role names. `spec.homeDatabase` accepts a database **or an alias**, so the failover alias from [step 3](#3-downstream-pre-stage-the-failover-alias) resolves it correctly — provided you created it |
| `Neo4jRole` | ❌ **No** | `spec.privileges` are complete Cypher statements with the database name embedded. The alias does **not** help here |

Rewriting a role for the DR cluster:

```yaml
# UPSTREAM (namespace: prod) — privileges name the database `foo`
spec:
  clusterRef: prod-cluster
  name: analytics_reader
  privileges:
    - "GRANT ACCESS ON DATABASE foo TO analytics_reader"
    - "GRANT MATCH {*} ON GRAPH foo NODES * TO analytics_reader"
---
# DOWNSTREAM (namespace: dr) — clusterRef AND every database name change
spec:
  clusterRef: dr-cluster
  name: analytics_reader
  privileges:
    - "GRANT ACCESS ON DATABASE `foo-replica` TO analytics_reader"
    - "GRANT MATCH {*} ON GRAPH `foo-replica` NODES * TO analytics_reader"
```

Because the replica keeps the name `foo-replica` **after promotion too**, these
privileges stay correct through failover — there is nothing to edit during the
outage.

!!! tip "Verify, don't assume"

    A wrong database name here fails silently. After applying, confirm the
    privileges actually landed on the replica:

    ```bash
    kubectl exec -n dr <dr-cluster-server-0> -c neo4j -- \
      cypher-shell -u neo4j -p <password> --access-mode=read \
      "SHOW ROLE analytics_reader PRIVILEGES AS COMMANDS"
    ```

    Every returned statement should name `foo-replica`. A statement naming
    `foo` is the silent-failure case above.

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
exist — they may be in a different Kubernetes cluster with no back-reference. If
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
free toggle. (Irrelevant to the same-Kubernetes-cluster path, which never
enables the proxy at all.)

**Network mode, separate clusters: the proxy's load balancer hostname isn't known immediately.**
`status.crossClusterReplication.ready` stays `false` and `addresses` stays
empty until the cloud provider assigns the LoadBalancer Service a hostname/IP.
Until then the upstream keeps advertising its internal FQDN — cluster
formation is never blocked waiting for the proxy.

---

## What CCDR exposes, and what protects it

Both modes move a whole database out of one trust boundary and into another.
That is the point of them, but it is worth being explicit about what each one
puts within reach of an attacker, because the two modes fail differently.

### Network mode: a load balancer in front of the database

| | |
|---|---|
| What is exposed | Port 6000 on every upstream server, published as `<lb>:16000`, `:16001`, … one per ordinal. RAFT (7000) and routing (7688) are never proxied |
| What it gives an attacker | The transaction-shipping protocol. Anyone who can complete a connection can stream the database's contents |
| How to check | `kubectl get neo4jenterprisecluster <name> -o jsonpath='{.status.conditions[?(@.type=="CrossClusterProxySecure")]}'` — `False`/`NoClusterTLS` means nothing does |
| What protects it | **Only the cluster SSL policy.** The proxy is HAProxy in `mode tcp` — it terminates nothing, inspects nothing and authenticates nothing. With `tls.mode: cert-manager` and the default `strictPeerValidation: true`, Neo4j requires a client certificate signed by a CA in `/ssl/trusted/`, which is exactly the set you control through `additionalClusterTrustCAs`. **With `spec.tls` unset there is no authentication on that port at all** |
| Second line of defence | `crossClusterReplication.loadBalancerInternal` (default `true`) annotates the Service for an internal load balancer on AWS, Azure and GCP, so the address is reachable only from the VPC/VNet. It is a default worth keeping; set it to `false` only when the downstream is genuinely outside the network, and then only with TLS |
| Third | `networkPolicy.enabled` (default **false**). Turning it on scopes port 6000 to this cluster's own pods plus the proxy, and `networkPolicy.allowReplicasFrom` admits named downstream clusters explicitly |

Three things to hold onto: the trust anchor list *is* the access control list,
so removing a decommissioned peer's CA is a revocation step and not a tidy-up;
`additionalClusterTrustCAs` is silently ignored unless `tls.mode` is set; and
the proxy shares the port with ordinary intra-cluster secondary catch-up, so
turning it on changes the exposure of normal cluster traffic too.

### Backup mode: credentials, in the database's own environment

Backup mode opens no network path at all — the two clusters are coupled only
through the object store, which is why it needs neither TLS trust nor a load
balancer. The exposure moves to the bucket instead.

The seed and every pull are performed by the **Neo4j server process**, not by a
Job, so the server needs the bucket credentials in its own environment. Whether
you set them through `source.credentialsSecretRef` or `spec.env`, the effect is
the same: object-store credentials live in the database container's
environment, and anyone who can `kubectl exec` into that container — or run an
unrestricted procedure that reads `/proc/self/environ` — can read them. The
operator passes them as `secretKeyRef` references, never literals, so they stay
out of the StatefulSet spec and out of a support bundle; but the process
environment is the process environment.

Scope them accordingly:

- **Read-only, prefix-scoped.** The downstream never writes to the chain. A
  policy granting `s3:GetObject`/`s3:ListBucket` on the chain prefix only is
  enough, and it is the difference between a leaked credential exposing one
  database's backups and exposing the bucket.
- **Prefer workload identity where you have it.** Annotate the server pods'
  ServiceAccount with `podServiceAccountAnnotations` (IRSA, GKE Workload
  Identity, Azure Workload Identity) and the SDK's default chain finds a
  short-lived token with no long-lived secret anywhere in the cluster.
- **Remember which side holds them.** These credentials sit in the *downstream*
  cluster, which is often the less-hardened one — a DR site is not usually
  where the strictest controls live.

### Both modes

The replica is read-only in Neo4j's own sense, not in a security sense: it
carries the full contents of the upstream database, including anything an
access-control model upstream was keeping from particular users. **Roles and
privileges do not replicate** — that is what step 4 exists for. A replica
without step 4 done is a complete copy of the data with none of its
authorization, so treat the downstream cluster as holding data at the
upstream's classification from the moment the replica seeds, not from the
moment you fail over.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Failed`, "requires Neo4j 2026.08 or later" | downstream cluster predates replica support |
| **Backup mode stuck in `Seeding`, `status.message` never changes** | the downstream SERVERS have no bucket credentials. The operator's message stays optimistic; the real error is in the operator log — the AWS SDK's *"Unable to load region from any of the providers"*. Set `source.credentialsSecretRef` on the replica, or `AWS_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (and `AWS_ENDPOINT_URL_S3` for S3-compatible stores) in the downstream cluster's `spec.env` — see the warning in step 2. Either restarts the downstream servers |
| `Failed`, "network replication requires at least one upstream cluster endpoint" | `source.addresses` is empty — paste an entry from the upstream's `status.crossClusterReplication.addresses`, or use `source.upstreamClusterRef` if same-cluster |
| `Failed`, "must be of the form host:port" | an entry in `source.addresses` is missing its port |
| Warning: "source.pullURI is ignored in network mode" (or `seedURI`/`credentialsSecretRef`) | those fields are backup-mode only; harmless but likely a copy-paste leftover — remove them |
| Network mode never connects, upstream `status.crossClusterReplication.ready` is `false` | the upstream's proxy LoadBalancer Service has no hostname/IP yet — check `kubectl get svc <cluster>-ccdr -n <upstream-ns>` |
| Network mode TLS handshake fails | `spec.tls.additionalClusterTrustCAs` missing on one or both clusters — it must be set on **both**, each trusting the other's CA |
| Network mode connection times out, both clusters on one Kubernetes cluster | the upstream has `spec.networkPolicy.enabled: true` — its port-6000 rule only admits pods carrying its own `neo4j.com/cluster` label; add the downstream to `spec.networkPolicy.allowReplicasFrom` (step 1, Network mode), or disable NetworkPolicy on the upstream, or use the proxy path instead |
| `Pending`, event `UpstreamClusterNotFound` | `source.upstreamClusterRef` names a `Neo4jEnterpriseCluster` that doesn't exist (yet) in the given namespace — check the name/namespace, or wait if it's still being created |
| `Pending`, event `UpstreamClusterNotReady` | the referenced upstream exists but hasn't published `status.internalAddresses` yet — normal briefly after the upstream is first created; check `kubectl get neo4jenterprisecluster <name> -n <ns> -o jsonpath='{.status.internalAddresses}'` if it persists |
| `Pending`, event `UpstreamBackupNotFound` | `source.upstreamBackupRef` names a `Neo4jBackup` that doesn't exist (yet) in the given namespace — check the name/namespace |
| `Pending`, event `UpstreamBackupNotReady` | the referenced `Neo4jBackup` exists but hasn't run its first backup yet (empty `status.replicationPullURI`) — check `kubectl get neo4jbackup <name> -n <ns>` for its own phase/schedule |
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
- [`Neo4jReplicaDatabase` API reference](../../api_reference/neo4jreplicadatabase.md) — full `source` field reference for both modes
- `docs/user_guide/guides/backup_restore.md` — the backup machinery this builds on
- Neo4j Operations Manual → Clustering → *Replicating databases across clusters*
