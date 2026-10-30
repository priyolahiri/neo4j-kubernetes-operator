# Neo4j Enterprise Clustering

This document describes how to configure and manage Neo4j Enterprise clusters using the Neo4j Kubernetes Operator.

## Overview

The Neo4j Kubernetes Operator supports Neo4j 5.26 LTS (the final SemVer release) and any CalVer release (2025.x, 2026.x, and onward) for Enterprise clustering, with multiple discovery mechanisms and advanced features like read replicas and multi-zone deployments.

## Cluster Architecture

A Neo4j Enterprise cluster consists of:

- **Servers**: Neo4j server instances that self-organize into primary and secondary roles automatically
- **Discovery**: each server finds the others by their pod names on the `-headless` service (LIST discovery)
- **Routing service**: Routes client connections to appropriate cluster members

**Server Self-Organization**: In the server-based architecture, you deploy a number of servers and Neo4j automatically assigns primary and secondary roles based on database requirements and cluster state.

## Discovery Methods

The operator automatically uses **LIST discovery** with static pod FQDNs — the recommended approach for Kubernetes deployments. See the [Neo4j Operations Manual](https://neo4j.com/docs/operations-manual/current/clustering/setup/discovery/) for background.

### How Cluster Discovery Works

The operator uses the **LIST resolver** with pre-computed pod FQDNs from the StatefulSet headless service. Each pod's DNS name is known upfront (`{cluster}-server-{n}.{cluster}-headless.{ns}.svc.cluster.local`), so the operator injects a fixed peer list at startup — no Kubernetes API calls required for discovery.

> **Note**: Do not confuse this with the Neo4j `K8S` resolver type, which queries the Kubernetes API directly. This operator always uses the `LIST` resolver.

**Discovery ports**:

- Port **6000** (`tcp-tx`): V2 cluster communication — used by this operator for discovery endpoints
- Port **5000** (`tcp-discovery`): V1 discovery — deprecated, **never used by this operator**

### Kubernetes Services Created

The operator automatically creates:

- `{cluster-name}-headless` — StatefulSet headless service (pod FQDNs)
- `{cluster-name}-internals` — cluster-internal routing
- `{cluster-name}-client` — external Bolt/HTTP access
- `{cluster-name}-metrics` — Prometheus metrics (port 2004), when `spec.monitoring` is set and enabled

### Discovery Configuration (Injected Automatically)

The operator injects version-specific discovery settings into every pod's startup script. **Do not set these in `spec.config`** — the operator manages them.

**Neo4j 5.26.x (SemVer)**:
```properties
dbms.cluster.discovery.resolver_type=LIST
dbms.cluster.discovery.version=V2_ONLY
dbms.cluster.discovery.v2.endpoints=<cluster>-server-0.<cluster>-headless.<ns>.svc.cluster.local:6000,...
internal.dbms.cluster.discovery.system_bootstrapping_strategy=me   # server-0 only
```

**Neo4j 2025.x+ / 2026.x+ (CalVer)**:
```properties
dbms.cluster.discovery.resolver_type=LIST
dbms.cluster.endpoints=<cluster>-server-0.<cluster>-headless.<ns>.svc.cluster.local:6000,...
# No dbms.cluster.discovery.version — V2 is the only supported protocol
```

Ref: [5.26.x discovery docs](https://neo4j.com/docs/operations-manual/5/clustering/setup/discovery/) · [2025.x+ discovery docs](https://neo4j.com/docs/operations-manual/current/clustering/setup/discovery/)

## Cluster Formation

The operator uses `Parallel` pod management and a minimum-primaries floor for fast, split-brain-free cluster formation. On Neo4j 5.26 it also steers bootstrap with a **ME/OTHER** hint.

### Key Configuration

- **Bootstrap strategy** (5.26 only): server-0 uses `me` (preferred bootstrapper); all other servers use `other` (join when ready). On CalVer no hint is emitted — V2 discovery elects a bootstrapper from the endpoint list
- **Minimum primaries**: Set to `min(3, servers)` on initial formation (override with `spec.topology.minSystemPrimaries`) — that many servers must mutually discover each other before RAFT elects a leader, preventing premature solo bootstrap. Note it's a bootstrap **floor**, not a cap: all initially-joined servers become system primaries
- **On restart** (data already exists): minimum primaries check is skipped so servers rejoin immediately without blocking StatefulSet rolling updates
- **Pod Management**: `Parallel` — all pods start simultaneously

### How It Works

1. **All server pods start in parallel** — Single StatefulSet with `Parallel` pod management
2. **Servers discover each other** — Via static pod FQDNs in the LIST endpoint list (port 6000)
3. **RAFT coordination** — on 5.26, server-0's `me` hint makes it the preferred bootstrapper and others wait with the `other` hint; on CalVer the bootstrapper is elected without a hint
4. **A quorum of servers must see each other** — `dbms.cluster.minimum_initial_system_primaries_count` (default `min(3, servers)`, override via `spec.topology.minSystemPrimaries`) prevents any single node from forming a solo cluster (split-brain)
5. **Cluster forms once quorum reached** — RAFT elects a bootstrap leader (server-0 on 5.26); others join
6. **Servers self-organize** — Neo4j automatically assigns primary and secondary roles per database

### Benefits

- **Split-brain prevention** — A quorum of servers must be mutually visible before formation completes
- **Reliable formation** — Deterministic peer addresses (one FQDN per pod) unlike K8S ClusterIP which returns a single VIP
- **Fast restarts** — Minimum primaries check skipped on pod restarts so rolling updates aren't blocked

### TLS-Enabled Clusters

TLS-enabled clusters use the same parallel formation approach with additional optimizations:

- **Automatic strict peer validation** (default): The operator emits `dbms.ssl.policy.cluster.trust_all=false`, `client_auth=REQUIRE` (mutual TLS), and `verify_hostname=true`, projecting the cert-manager Secret's `ca.crt` to `/ssl/trusted/ca.crt` as the trust anchor. This matches Neo4j's canonical production guidance. Set `spec.tls.strictPeerValidation: false` to opt out into the legacy `trust_all=true` posture (only useful if your external issuer doesn't populate `ca.crt`).
- **Parallel startup maintained**: TLS doesn't change the pod startup behavior
- **Reliable formation**: With proper configuration, TLS clusters form as reliably as non-TLS clusters

For detailed TLS configuration, see the [TLS Configuration Guide](tls_configuration.md).

### Cluster Formation Strategy

The operator uses a unified clustering approach for all deployments:

#### Unified Cluster Formation
- Every `Neo4jEnterpriseCluster` (minimum 2 servers) uses Neo4j's clustering infrastructure; single-node deployments are `Neo4jEnterpriseStandalone`, which has no cluster configuration
- Automatic handling of discovery configuration based on Neo4j version
- Coordinated startup ensures data consistency

#### Parallel Pod Management
- Uses **parallel pod startup** for faster cluster formation
- All pods start simultaneously and coordinate during bootstrap
- Prevents split-brain scenarios through proper coordination

#### Formation Requirements

Initial formation waits for `dbms.cluster.minimum_initial_system_primaries_count` servers (default `min(3, servers)`, or `spec.topology.minSystemPrimaries`) to see each other:

| Cluster Size | Servers needed to form | Rationale |
|--------------|----------------------|-----------|
| 2 servers | 2 | Minimum cluster size |
| 3 servers | 3 | Odd number for optimal fault tolerance |
| 4+ servers | 3 (default) | Servers 0–2 host the `system` database as primaries; servers 3 and up as secondaries (see below) |

This keeps any single server from bootstrapping a solo cluster.

#### System primaries and secondaries

Each server hosts the `system` database (what databases, users and roles exist) either as a **primary**, which votes in its Raft group, or as a **secondary**, which keeps a copy and does not vote. The operator fixes the role at a server's first start, from its ordinal:

- servers below `spec.topology.minSystemPrimaries` (default `min(3, servers)`) are system primaries;
- the rest are system secondaries;
- the role is recorded on the server's data volume and kept across restarts.

This follows the Neo4j Operations Manual: start with three system primaries, and give servers that come and go the secondary role. It is also what lets the operator add servers without restarting the ones already running (see [Scale Up/Down](#scale-updown)).

The `system` database stays writable while a majority of its primaries is up. With the default of 3, that means two of servers 0–2, however many servers the cluster has. Raise `minSystemPrimaries` (an odd number) to make more servers system primaries.

A server created before this behaviour existed keeps the role it had: every server in such a cluster stays a system primary. Only servers added later start as secondaries.

### Cluster Formation Process

1. **Resource Creation**: The operator creates all Kubernetes resources (StatefulSets, Services, RBAC)
2. **Parallel Pod Startup**: All pods start simultaneously (not sequentially)
3. **Discovery Phase**: Pods discover each other via Kubernetes service discovery
4. **Coordination Phase**: Pods wait until the minimum number of system primaries can see each other before the cluster forms
5. **Service Ready**: Cluster accepts connections after successful formation

### Important Considerations

- **Minimum Membership**: At least `min(3, servers)` servers (or `spec.topology.minSystemPrimaries`) must be available for initial cluster formation
- **Startup Time**: Cluster formation typically completes within 2-3 minutes
- **Pod Readiness**: A server pod is `Ready` once its server is available in the cluster, as it sees itself: its `system` database is available on it, none of its databases reports a failure, and it takes part in the Raft group of every database it hosts as a primary. A roll the StatefulSet performs — a plugin install, a configuration or TLS change — therefore restarts the next server only after the previous one has rejoined, and the cluster keeps its majority. The wait is bounded: five minutes after a container starts the probe checks only the Neo4j process and the HTTP port, as it did before, so a cluster that cannot form does not hold its servers unready forever, and the liveness probe never restarts a server for being out of the cluster. After an operator upgrade each server gets the new check at its next restart; nothing is restarted for it. Pod readiness is still one server's view: check the cluster's `status.phase` for the cluster's state
- **Scaling**: After initial formation, clusters can be scaled following Neo4j's online scaling procedures

## Basic Cluster Configuration

### Simple 3-Node Cluster

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata:
  name: simple-cluster
spec:
  acceptLicenseAgreement: "eval"
  image:
    repo: neo4j
    tag: 5.26-enterprise
  topology:
    servers: 3  # 3 servers will self-organize into appropriate roles
  storage:
    className: standard
    size: 10Gi
  # LIST discovery with static pod FQDNs is configured automatically.
  # The operator never uses the K8S service-list resolver; do not set
  # dbms.kubernetes.discovery.* in spec.config (the validator rejects it).
```

**Note**: The operator automatically handles all clustering configuration including:

- LIST discovery setup with pod FQDNs on port 6000 (`tcp-tx`)
- RBAC resource creation
- Service creation for cluster communication
- Neo4j configuration for optimal clustering

### Using kubectl Commands

```bash
# Check cluster status
kubectl get neo4jenterprisecluster my-cluster -o yaml

# View cluster details
kubectl describe neo4jenterprisecluster my-cluster

# Check pod status
kubectl get pods -l app.kubernetes.io/instance=my-cluster
```

## Default Database Topology

When a Neo4j cluster starts for the first time, it automatically creates a default database called `neo4j`. This database is created with Neo4j's built-in defaults: **1 primary and 0 secondaries**, regardless of how many servers are in your cluster.

This means that on a 3-server cluster, the default `neo4j` database will only be hosted on a single server — which may be surprising if you expect it to span the entire cluster.

### Configuring Default Topology at Bootstrap

You can control the default topology for all newly created databases (including the auto-created `neo4j` database) using `initial.*` settings in `spec.config`:

```yaml
spec:
  config:
    initial.dbms.default_primaries_count: "3"
    initial.dbms.default_secondaries_count: "1"
```

> **Important**: These are bootstrap-only settings. They only take effect when the cluster is created for the first time. Adding or changing them on an existing cluster has no effect.

### Changing Database Topology After Bootstrap

To change the topology of an existing database, use the `ALTER DATABASE` Cypher command:

```bash
kubectl exec <cluster>-server-0 -c neo4j -- cypher-shell -u neo4j -p <password> \
  "ALTER DATABASE neo4j SET TOPOLOGY 3 PRIMARIES 1 SECONDARY"
```

Alternatively, you can manage database topology declaratively using the `Neo4jDatabase` CRD. See the [Neo4jDatabase API Reference](../api_reference/neo4jdatabase.md) for details. For databases on a managed **Aura** instance, use the `AuraDatabase` CRD — see the [Aura Orchestration Guide](aura_orchestration.md).

### Cannot Skip Default Database Creation

Neo4j does not provide a way to prevent the default `neo4j` database from being created. It is always created at cluster bootstrap. The only post-bootstrap options are:

- **Change its topology** using `ALTER DATABASE`
- **Rename the default** using the `dbms.setDefaultDatabase()` procedure (do not use the deprecated `dbms.default_database` config setting):
  ```bash
  kubectl exec <cluster>-server-0 -c neo4j -- cypher-shell -u neo4j -p <password> \
    "CALL dbms.setDefaultDatabase('mydb');"
  ```
  This must be executed as a database operation after bootstrap, not via `spec.config`.
- **Manage it declaratively** via a `Neo4jDatabase` CRD (the operator will warn that you are shadowing the default)

## Advanced Configuration

### Server Role Constraints

By default every server can host databases in either primary or secondary mode (`NONE`). You can optionally constrain server roles to influence how Neo4j allocates database instances — for example, dedicating some servers to secondary (read-replica) duty.

**Cluster-wide constraint** — `spec.topology.serverModeConstraint` applies to all servers. Valid values: `NONE` (default), `PRIMARY`, `SECONDARY`.

```yaml
spec:
  topology:
    servers: 3
    serverModeConstraint: PRIMARY  # all servers only host primaries
```

**Per-server constraints** — `spec.topology.serverRoles[]` overrides `serverModeConstraint` for the named servers. Each entry has a `serverIndex` (0-based) and a `modeConstraint` (`NONE`, `PRIMARY`, or `SECONDARY`):

```yaml
spec:
  topology:
    servers: 3
    serverModeConstraint: NONE
    serverRoles:
      - serverIndex: 0
        modeConstraint: PRIMARY    # server-0 only hosts primaries
      - serverIndex: 1
        modeConstraint: SECONDARY  # server-1 only hosts secondaries
      - serverIndex: 2
        modeConstraint: NONE       # server-2 can host either mode
```

**Validation rules** (enforced by the controller, not a webhook):

- `servers` must be between 2 and 100 (the 100 cap is an operator safety rail, not a Neo4j limit; realistic deployments rarely exceed ~10).
- Each `serverRoles[].serverIndex` must be in the range `[0, servers-1]`.
- `serverRoles[].serverIndex` values must be unique (no duplicates).
- You cannot constrain **all** servers to `SECONDARY` — at least one server must be able to host primaries.

### Multi-Zone Deployment

Configure topology spread and anti-affinity for high availability:

```yaml
spec:
  topology:
    servers: 5  # 5 servers will self-organize into appropriate roles
    placement:
      antiAffinity:
        enabled: true
        topologyKey: topology.kubernetes.io/zone
        type: preferred   # or "required"; any other value is treated as preferred
      topologySpread:
        enabled: true
        topologyKey: topology.kubernetes.io/zone
        maxSkew: 1
        whenUnsatisfiable: DoNotSchedule
```

> **📖 For comprehensive topology placement options**, including zone distribution strategies, anti-affinity configurations, and troubleshooting tips, see the [Topology Placement Guide](./topology_placement.md).

### TLS Configuration

Enable TLS encryption for cluster communication:

```yaml
spec:
  tls:
    mode: cert-manager
    issuerRef:
      name: neo4j-cluster-issuer
      kind: ClusterIssuer
    duration: 8760h  # 1 year
    renewBefore: 720h  # 30 days
```

## Port Configuration

The operator uses the following default ports:

| Port | Name | Purpose |
|------|------|---------|
| 7687 | bolt | Client Bolt connections |
| 7474 | http | HTTP API |
| 7473 | https | HTTPS API |
| **6000** | **tcp-tx** | **V2 cluster traffic + discovery endpoints (always used)** |
| 5000 | tcp-discovery | V1 discovery — **deprecated, not used by this operator** |
| 7688 | routing | Routing service |
| 7000 | raft | RAFT consensus |

```yaml
spec:
  config:
    server.cluster.listen_address: 0.0.0.0:6000   # V2 cluster + discovery
    server.routing.listen_address: 0.0.0.0:7688
    server.cluster.raft.listen_address: 0.0.0.0:7000
```

## Health Monitoring

The operator provides comprehensive health monitoring:

```bash
# Check cluster health
kubectl get neo4jenterprisecluster my-cluster -o jsonpath='{.status.phase}'

# Get cluster status
kubectl describe neo4jenterprisecluster my-cluster

# View cluster logs
kubectl logs -l app.kubernetes.io/instance=my-cluster
```

## Scaling Operations

### Scale Up/Down

```bash
# Scale servers by editing the resource
kubectl patch neo4jenterprisecluster my-cluster --type='merge' -p='{"spec":{"topology":{"servers":7}}}'

# Or edit the resource directly
kubectl edit neo4jenterprisecluster my-cluster
```

**Scale-up** raises the replica count. New servers join, and you rebalance databases onto them (`REALLOCATE DATABASES`, or set a per-database `TOPOLOGY`).

The operator does not check node capacity before a scale-up: whether the new pods fit is the scheduler's decision, and a node autoscaler may add the nodes they need. Check before you apply with `kubectl neo4j preflight -f <manifest>`, which reports when no Ready node can fit a server's memory request. A server that cannot be scheduled stays `Pending`; `kubectl neo4j diagnose` names it and the scheduler's reason. Lowering `servers` again removes it: a server that never started has nothing to drain.

**Servers already running are not restarted when the new servers join as system secondaries.** That covers any scale-up from `minSystemPrimaries` servers or more, for example 3→5. Each new server's discovery list names every server, and the running servers learn about it through discovery. They read the longer list at their next restart. A `ScaledWithoutRestart` event records it.

A scale-up that adds a system primary does restart the running servers one at a time, because Neo4j requires every system primary in each server's discovery list. With the default, that only happens from 2 servers to 3 or more. Scaling down never restarts the servers that stay.

**A scale-up right after a scale-down waits for the removed servers to be gone.** When a drain completes, Kubernetes stops the removed servers' pods and then deletes their volumes, which takes some seconds. A server added on one of those volumes would start as the server Neo4j has dropped, and a dropped server cannot rejoin. So if you raise `spec.topology.servers` while a removed server's pod or volume is still there, the operator adds no server at that ordinal or above until it is gone. The `TopologyChangeDeferred` condition reads `True` with reason `WaitsForRemovedServers` and names the servers, one `ScaleUpDeferred` event says so, and the cluster stays `Ready`. Once they are gone the servers are added as usual, with new identities. If something is still there after 2 minutes, for example a pod terminating on a node that is gone or a volume another tool's finalizer keeps, the message and one `ScaleUpBlocked` Warning name it and say how to remove it. The operator deletes nothing itself. A scale-up with nothing left over is not delayed. A volume that is not being deleted and that no pod owns, for example one you created ready for a new server, is used as it is.

A cluster raised too soon by an earlier release has pods running servers that SHOW SERVERS lists as `Dropped`. Its phase stays `Forming`, and the message names those pods and the way out, with one `DroppedServersReturned` Warning: set `spec.topology.servers` back to the first of them, wait for "Scale-down drain complete", then raise it again.

#### Scaling down (automated, safe-by-default)

When you **lower** `spec.topology.servers`, the operator drains the removed servers **before** their pods are stopped — it never just deletes pods out from under live data. For the removed servers (highest ordinals first) it runs, one step per reconcile:

```
DRYRUN DEALLOCATE (feasibility; nothing is cordoned if it fails) → cordon → wait 20 s → DEALLOCATE DATABASES FROM SERVER → wait until it hosts only `system` → DROP SERVER → lower replicas
```

Each step's status write starts the next pass, so the dry run and the cordon follow each other within about a second. After the cordon the drain waits 20 seconds before it deallocates, so that a scale-down you set back in that time can be undone (below). Meanwhile `ServersPendingDrain` says when the deallocation starts. Servers you cordoned yourself before the scale-down are deallocated without that wait.

Replicas are **held** at the current count (the removed pods stay running and reachable for data hand-off) until every removed server is dropped; only then are the pods stopped. Progress shows as a `ServersPendingDrain` condition and `ScaleDownDraining` events. When the drain is done the condition is `False` with "Scale-down drain complete", and stays so. Removed-ordinal PVCs are reclaimed (`PVCRetentionPolicy.WhenScaled=Delete`).

**Setting a scale-down back.** If you raise `spec.topology.servers` again before the drain begins deallocating, that is within 20 seconds of the cordon, the operator gives back what the drain took: it runs `ENABLE SERVER` for each server the drain cordoned. It records those servers on the cluster (annotation `neo4j.com/scale-down-cordoned-servers`), so a server you cordoned yourself stays cordoned. A `ScaleDownSetBack` event names the servers it enabled, and `ServersPendingDrain` reads `False` with "Scale-down not in progress". If you set it back only part of the way (5 → 3, then 4), the server the cluster keeps again is given back and the drain finishes the others.

Neo4j cannot undo a deallocation: once a removed server is `Deallocating` (or `Deallocated`, or dropped), it never hosts databases again. A set-back that comes after that is refused. The replicas stay held, nothing more is drained, and `ServersPendingDrain` reads `True` with reason `ScaleDownBlocked`, naming the servers and the value to set `spec.topology.servers` back to so the drain can finish: "Scale-down set back too late for c-server-3 (…), …: … Set spec.topology.servers to 3 so the drain can finish them, then raise it to add new servers in their place." Lower it as the message says, wait for "Scale-down drain complete", then raise it: the operator adds the new servers once the removed servers' pods and volumes are gone (`TopologyChangeDeferred` says so while it waits), and they join with new identities on new volumes. While the set-back is refused the cluster reads `Forming`, because the spec asks for more servers than are members. The message and its one Warning stay the same while the servers move on from `Deallocating` to `Deallocated` and `Dropped`; `SHOW SERVERS` and `ServersHealthy` show their states.

**A cordoned server is a member.** Neo4j keeps a cordoned server's databases on it and only stops placing new ones there, so the operator counts it as a member: a cluster with a server you cordoned (`CALL dbms.cluster.cordonServer(…)`) is `Ready`, and `ServersHealthy` names the server (`state=Cordoned`). A cordoned server that is down is reported like any other server that is down (the `Degraded` condition).

A removed server that is down when the scale-down starts — its pod being replaced, say — is drained too. `SHOW SERVERS` lists such a server with no address; the operator knows it by the address it last reported, or, failing that, because every server the scale-down keeps is accounted for. If a kept server is down too and the operator cannot tell which is which, it drains what it can name and keeps the replicas held, with `ServersPendingDrain` saying why, until a server reports its address again.

**During an upgrade.** A change of `spec.topology.servers` — up or down — waits while a rolling upgrade is in progress, or while a `Paused` or `Failed` one holds part of the servers on its target: the `TopologyChangeDeferred` condition says so, and the change is applied once the upgrade completes (see [Upgrades](guides/upgrades.md#when-an-upgrade-stops-paused-and-failed)).

**The system-database floor.** A cluster cannot be scaled below `spec.topology.minSystemPrimaries` (default **min(3, servers)** — i.e. 3 for any cluster of 3+, 2 for a 2-server cluster). This is the `system` database's voting-member quorum; Neo4j refuses to drop below it. To run 1–2 nodes, use `Neo4jEnterpriseStandalone`.

**When a scale-down is refused.** The operator sets `ServersPendingDrain` to a `ScaleDownBlocked` reason (with the exact cause) and **holds replicas — nothing is cordoned or removed** — when the cases below apply. When Neo4j refuses the deallocation, the message quotes its error, and its advice follows the cause Neo4j names: a database with its only primary on a removed server, or a database whose topology the remaining servers cannot host. When Neo4j names neither, the advice covers both. The cases:

- The target is **below the system-DB floor** (above) — keep ≥ the floor.
- A **single-primary database** (e.g. the default `neo4j` DB, which has 1 primary by default) lives on a server being removed. Neo4j refuses to move a sole primary (it would mean write-unavailability), and *no* reallocation primitive can relocate it. Give that database an additional primary first so it becomes relocatable:
  ```cypher
  ALTER DATABASE neo4j SET TOPOLOGY 2 PRIMARIES;   -- then the scale-down proceeds
  ```
- A database's `TOPOLOGY` can no longer be satisfied by the survivors (e.g. a 3-primary DB on a would-be 2-server cluster) — reduce its topology or keep the servers.
- The scale-down was **set back after the drain began deallocating** a server the cluster would now keep (above). Here the drain has already cordoned and deallocated servers; the refusal stops it from going further until the spec lets it finish them.

The operator deliberately **never auto-changes a database's `TOPOLOGY`** (that alters your durability guarantees) — it only relocates what Neo4j allows and refuses the rest with actionable guidance.

### Rolling Upgrades

```yaml
spec:
  upgradeStrategy:
    strategy: RollingUpgrade
    preUpgradeHealthCheck: true
    postUpgradeHealthCheck: true
    upgradeTimeout: 30m
    healthCheckTimeout: 5m
    stabilizationTimeout: 3m
    autoPauseOnFailure: true
```

## 5.26: writes while a server is stopped

On 5.26, writes can stall for about 30 seconds every time one server stops: a restart, a crash or a node drain. The leader waits for a connection to the stopped member before it replicates to the others, for up to `dbms.cluster.network.connect_timeout`. Neo4j documents this for Kubernetes and fixes it from **5.26.29** with `dbms.cluster.raft.async_channel_acquisition_enabled=true`. CalVer has that on by default. On Kind, restarting a follower stalled writes for 32 seconds with it off and not at all with it on.

The operator turns it on for 5.26 clusters:

- **Which servers:** only servers whose own Neo4j is 5.26.29 or later. An older patch would refuse the unknown setting and not start. The `5.26-enterprise` tag floats, so each server decides from its own version when it starts.
- **New clusters:** have it from the first start.
- **Existing clusters:** get it at each server's next restart. The operator does not restart them for it. Until then, the `RestartPending` condition names the servers still waiting:

  ```bash
  kubectl get neo4jenterprisecluster my-cluster \
    -o jsonpath='{.status.conditions[?(@.type=="RestartPending")].message}'
  ```

  To apply it sooner, delete those pods one at a time, or run `kubectl rollout restart statefulset my-cluster-server`.
- **Overriding it:** set the setting in `spec.config` (for example `"false"`). The operator then leaves it to you, and changing it restarts the servers as for any setting Neo4j reads only at startup.

!!! note "`SHOW SETTINGS` reports it as `false`"
    On 5.26.31, `SHOW SETTINGS` shows this setting as `false`, and not explicitly set, even on a server that runs with it on. 5.26.29 made an existing internal setting public, and `SHOW SETTINGS` does not reflect it. The value Neo4j actually starts with is in the configuration dump at the top of `debug.log`, and the operator's `RestartPending` condition does not rely on `SHOW SETTINGS`.

## Writes while a restarted server rejoins

A server that restarts comes back in a new pod with a new IP address. For up to 30 seconds afterwards, the other servers can still resolve its name to the old address, which nothing answers, and each attempt to connect waits out `dbms.cluster.network.connect_timeout`. Neo4j's default is 30 seconds. If Neo4j moves a database's leadership to the restarted server in that time, which it does to balance leaderships, the transfer travels over that connection, and writes to the database stop until the transfer times out. On Kind, under write load, that held writes for about 10 seconds on both 5.26 and CalVer.

The operator sets `dbms.cluster.network.connect_timeout=5s` on every cluster, and the stalls went away: no write paused longer than about 4 seconds. Servers in one Kubernetes cluster connect in milliseconds, so 5 seconds still leaves a wide margin.

- **New clusters:** have it from the first start, and nothing waits for a restart.
- **Existing clusters:** when the operator adds the setting, it applies it to the running servers without a restart (a `ConfigAppliedLive` event). Neo4j's Raft connections take it up only when each server next restarts, so the cluster's first rolling upgrade afterwards can still see the longer pauses. Until then, the `RestartPending` condition names the servers whose Raft connections still use the old value, and each drops out when it restarts. `SHOW SETTINGS` already shows `5s` on them, so it cannot tell you which servers are waiting. To take it up sooner, delete those pods one at a time.
- **Overriding it:** set `dbms.cluster.network.connect_timeout` in `spec.config`, for example back to `30s`. The change is applied live too, and `RestartPending` names the servers until each restarts. A later change names them again.

!!! note "Clusters that got the setting from v1.21.0 or v1.21.1"
    `RestartPending` records a change when the operator makes it. v1.21.0 and v1.21.1 applied the setting live without recording it, so a cluster upgraded from v1.20 under one of them, and not restarted since, is not named. Its servers take the setting up at their next restart, as the v1.21.0 upgrade note said. A cluster created on v1.21.x started with it and needs nothing.

## Troubleshooting

### Common Issues

1. **Cluster Formation Issues (Rare)**
   - The parallel startup strategy and topology-aligned minimum primaries configuration eliminate most formation issues
   - Check pod logs: `kubectl logs {cluster-name}-server-0` for any startup errors
   - Verify all pods can resolve DNS: `kubectl exec {cluster-name}-server-0 -- nslookup {cluster-name}-headless`

2. **Discovery (LIST resolver via headless service)**
   - The operator uses LIST discovery against the StatefulSet's `-headless` service.
   - Verify the headless service exists: `kubectl get service {cluster-name}-headless`
   - Check endpoints include all pods: `kubectl describe endpoints {cluster-name}-headless`
   - Check that pod FQDNs resolve: `kubectl exec {cluster-name}-server-0 -- nslookup {cluster-name}-server-0.{cluster-name}-headless.{ns}.svc.cluster.local`
   - Inspect the startup script for correct LIST endpoints: `kubectl get configmap {cluster-name}-config -o yaml | grep -A2 resolver_type`

3. **A server down, or quorum lost**
   - One server down: the cluster stays `Ready` with a `Degraded` condition, then turns phase `Degraded` after the grace period (default 5m). It keeps serving.
   - A majority down: phase `Forming` with a `ClusterQuorumLost` Warning event.
   - See [Server availability](../api_reference/neo4jenterprisecluster.md#server-availability) and [Troubleshooting](guides/troubleshooting.md#problem-a-server-is-down-degraded).

### Debug Commands

```bash
# Check cluster member status
kubectl exec -it my-cluster-server-0 -- cypher-shell -u neo4j -p password "SHOW SERVERS"

# Check database allocation
kubectl exec -it my-cluster-server-0 -- cypher-shell -u neo4j -p password "SHOW DATABASES"

# View cluster logs
kubectl logs my-cluster-server-0 -c neo4j
```

## Best Practices

1. **Use odd numbers of servers (3, 5, 7)** for optimal fault tolerance. Even numbers are allowed but may have less optimal quorum behavior.
2. **Configure proper resource limits** based on workload
3. **Use multi-zone deployment** for high availability
4. **Enable TLS** for secure cluster communication
5. **Monitor cluster health** regularly
6. **Use rolling upgrades** for zero-downtime updates
7. **Trust automatic discovery** - the operator handles all Kubernetes discovery configuration

## Migration from Neo4j 4.x

When migrating from Neo4j 4.x clustering to 5.x:

1. **Remove all manual discovery configuration** - the operator handles discovery automatically
2. **Update from `causal_clustering.*` to `dbms.cluster.*`** - but discovery settings are managed by operator
3. **Update port configurations** for new routing service if customized
4. **Remove static endpoint lists** - the operator injects its own LIST of pod FQDNs
5. **Test cluster formation** in staging environment first

**Important**: The operator automatically handles all cluster discovery. Remove any manual discovery configuration from your cluster specifications.

## References

- [Neo4j Operations Manual - Clustering](https://neo4j.com/docs/operations-manual/current/clustering/)
- [Neo4j Operations Manual - Discovery](https://neo4j.com/docs/operations-manual/current/clustering/setup/discovery/)
- [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
