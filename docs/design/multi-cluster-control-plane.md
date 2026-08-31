# Design: Multi-Kubernetes-Cluster Control Plane (hub/spoke)

> **Status:** Analysis, plus the two cheapest delivery items shipped
> (§8 items 1–2: the GitOps fan-out guide and the `k8s_cluster` metric label).
> The hub itself is unbuilt, and §5 argues that the literal reading of the
> request should not be built.
> **Source:** this repository, read at commit `33a3204`; the CCDR design
> (`cross-cluster-replication.md`), whose B1 recurs here in a worse form;
> controller-runtime v0.24.1 (`go.mod:22`).
> **Scope of this document:** what it would take for **one operator instance to
> manage Neo4j across N Kubernetes clusters**. It covers the Kubernetes API
> plane, the Bolt data plane, the CRD-surface consequences, and four delivery
> options. It does **not** cover cross-cluster *replication* — that is CCDR, it
> already shipped, and it is a different question (§1.1).
> **Headline:** the Kubernetes API half is cheap and mechanically supported —
> the operator has no `remotecommand`/exec dependency, and controller-runtime
> ships per-cluster caches. The **Bolt half is the blocker**, and it is
> [B1 from the CCDR design](cross-cluster-replication.md#b1--advertised-addresses-are-hardcoded-to-in-cluster-dns-network-only-the-real-blocker)
> again: the operator is a Neo4j client as well as a Kubernetes client, its
> connection URIs are hardcoded to in-cluster DNS, and the `neo4j://` routing
> scheme means exposing a Service does not fix it — the server hands back
> internal FQDNs. CCDR bought its way out with an opt-in L4 proxy scoped to one
> port. A hub would need the same machinery on the Bolt ports, for every
> managed cluster, mandatorily, and per-pod. **Recommended: option (c) — an
> aggregation-only hub (§5), which needs none of it.**

---

## 1. What "multi-cluster" is being asked for here

Four distinct questions hide under the phrase. This document is about the third.

1. **One Neo4j cluster stretched across Kubernetes clusters** — one DBMS, one
   RAFT group, servers in different Kubernetes clusters. Out of scope, and
   argued against in §9 Q1.
2. **Independent Neo4j clusters replicating** — shipped, as CCDR. See
   `cross-cluster-replication.md`.
3. **One operator managing N Kubernetes clusters** — a hub/spoke control
   plane. **This document.**
4. **Fleet visibility across clusters** — partially answered already by
   `spec.auraFleetManagement` (`api/v1beta1/fleet_target.go`), which registers
   deployments with Aura Fleet Manager. The gap is that it is Aura-tied; §5
   covers the non-Aura form, because it turns out to be the *same* work as the
   recommended option.

### 1.1 Why (3) is not (2)

CCDR moves **data** between Neo4j clusters. A hub moves **intent** between
Kubernetes clusters. They share exactly one thing — the discovery that Neo4j
addresses are pinned to in-cluster DNS — and that shared blocker is the reason
this document exists at all. Nothing in the shipped CCDR proxy helps a hub:
`spec.crossClusterReplication` fronts port 6000 (transaction shipping) and
deliberately leaves 7687/7688 alone.

---

## 2. What this operator already provides

The Kubernetes-API side of a hub is in better shape than expected.

- **No exec, no port-forward, no SPDY.** `rest.Config` and `clientcmd` appear
  in exactly two files — `cmd/main.go` and `internal/controller/suite_test.go`.
  There is no `remotecommand` usage in production code at all. Every controller
  interacts with Kubernetes purely through a `client.Client`, which is the one
  abstraction that retargets cleanly.
- **Uniform client injection.** All ~26 controllers are constructed with
  `Client: mgr.GetClient()` in one contiguous block (`cmd/main.go:342-497`),
  and every validator likewise (`validation.NewClusterValidator(mgr.GetClient())`
  and siblings). There is a single seam to change, not a scattering.
- **controller-runtime supports it.** `cluster.New(*rest.Config, ...) (Cluster, error)`
  exists (`pkg/cluster/cluster.go:148`), `Cluster` is a `Runnable` that can be
  added to a manager, and `source.Kind` takes an **explicit cache**
  (`pkg/source/source.go:76`) — so watching a remote cluster's objects is a
  supported composition, not a fork.
- **Namespace scoping already parameterised.** `WATCH_NAMESPACE`
  (`cmd/main.go:278`) already models "reconcile a subset", which is the right
  shape to extend per remote cluster.
- **An external Bolt hostname can already be a certificate SAN.**
  `spec.service.dnsName` is enumerated by `BuildCertificateForEnterprise`, so
  the SAN half of B8 is not a new build.

Consequence: **if the operator only ever spoke to the Kubernetes API, a hub
would be a straightforward refactor.** It does not.

---

## 3. Blockers

Ranked. B1–B3 are the data-plane blockers and are the substance of this
document. B4–B7 are structural costs that no design removes, only relocates.

### B1 — The Bolt connection is hardcoded to in-cluster DNS *(the real blocker)*

`internal/neo4j/client.go:2793-2808`:

```go
func buildConnectionURIForEnterprise(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	scheme := "neo4j"
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == "cert-manager" {
		scheme = "neo4j+s"
	}
	host := fmt.Sprintf("%s-client.%s.svc.cluster.local", cluster.Name, cluster.Namespace)
	port := 7687
	...
}
```

A hub in Kubernetes cluster A cannot resolve that name for a Neo4j deployment
in cluster B, and there is no field to override it. This is not one call site
in an obscure corner: **21 non-test files under `internal/controller/`
construct a Neo4j client**, and they fall into two groups that matter
differently.

*Data-plane CRDs* — `Neo4jDatabase`, `Neo4jDatabaseAlias`, `Neo4jUser`,
`Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jShardedDatabase`,
`Neo4jReplicaDatabase`, `Neo4jReplicaPromotion`, `Neo4jRestore`. Bolt *is*
their implementation; without it they have no behaviour at all.

*Cluster lifecycle operations* — and this is the group that is easy to miss:
`rolling_upgrade.go`, `scale_down.go`, `splitbrain_detector.go`,
`plugin_controller.go`, `neo4jbackup_sharded.go`, `diagnostics_databases.go`,
`diagnostics_users_roles.go`, and both the cluster and standalone controllers
themselves (`neo4jenterprisecluster_controller.go:639`). Draining a server
before scale-down, sequencing a rolling upgrade, and detecting split-brain are
all Cypher operations. **The infrastructure controllers are not Bolt-free
either**, which is what forecloses option (b) below.

A hub that can reach the Kubernetes API but not Bolt can create a StatefulSet
in a remote cluster and cannot create a database in it. That is Argo with extra
credentials (§4d).

### B2 — The routing scheme means exposing a Service is not sufficient

The URI above uses `neo4j://` (or `neo4j+s://`), the **routing** scheme. The
driver opens one connection, calls `dbms.routing.getRoutingTable`, and then
connects to whatever addresses the server returns. Those come from
`server.routing.advertised_address`, pinned in
`internal/resources/cluster.go:2363`:

```
server.routing.advertised_address=${HOSTNAME_FQDN}:7688
```

where `HOSTNAME_FQDN` is the internal pod FQDN. So a hub that reaches an
externally-published client Service still receives internal FQDNs in the
routing table and fails on the second hop.

**This is CCDR's B1, restated on the Bolt path.** The CCDR analysis established
the general form — "the downstream connects to a listed address, receives an
internal FQDN in reply, and fails … there is no workaround on the listing
side." The same reasoning applies unchanged here, with one aggravating
difference: CCDR made its fix **opt-in** and confined it to port 6000, so
clusters that do not replicate pay nothing. A hub needs the equivalent on the
Bolt ports for **every** cluster it manages, or it manages none of them.

`internal/validation/config_validator.go:86-89` additionally refuses any
`*.advertised_address` override through `spec.config`
(`operatorRuntimeManagedSettings`), so this cannot be worked around by
configuration — it requires operator-side support, exactly as
`spec.crossClusterReplication` did.

### B3 — Split-brain detection and diagnostics need *per-pod* Bolt, not one endpoint

`internal/controller/splitbrain_detector.go:232`:

```go
podURL := fmt.Sprintf("%s://%s.%s-headless.%s.svc.cluster.local:7687", ...)
```

The detector deliberately bypasses routing to compare each server's individual
view of the cluster — that is the whole mechanism. So the exposure requirement
is not "one reachable endpoint per cluster" but **N reachable endpoints per
cluster**, addressable per ordinal.

That is precisely the shape the CCDR proxy solves for port 6000 — one HAProxy
listening on `16000+i` per ordinal, because "a plain Kubernetes Service cannot
do per-ordinal port-to-specific-pod mapping." A hub would need the identical
construction on 7687. The work is understood and already has prior art in this
repo (`internal/resources/ccdr_proxy.go`); it is the *mandatoriness* and the
per-cluster cost that is the problem, not the novelty.

Degraded alternative: run the hub with split-brain detection and live
diagnostics disabled for remote clusters. That is a real option, and it should
be stated plainly rather than discovered — it means **the hub silently offers
less safety than a local operator**, on exactly the clusters an operator is
least able to watch by hand.

### B4 — Every `clusterRef` is same-cluster by construction

`ResolvedTarget` and `NewClient(c client.Client)`
(`internal/controller/cluster_resolver.go`) resolve a `clusterRef` with a live
`Get` against the operator's own API server, then build a Neo4j client from the
result. Adding a cluster dimension means:

- a new field on `clusterRef` for **every** dependent CRD — `Neo4jDatabase`,
  `Neo4jDatabaseAlias`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`,
  `Neo4jAuthRule`, `Neo4jBackup`, `Neo4jRestore`, `Neo4jReplicaDatabase`,
  `Neo4jReplicaPromotion`, `Neo4jShardedDatabase`;
- the same change in each corresponding validator, all of which are constructed
  with `mgr.GetClient()` today. **A validator that silently resolves against
  the hub's own cluster is the worst bug in this design space** — it reports
  success against the wrong cluster, which no user will suspect;
- for each CRD, the full hand-written surface list from `CLAUDE.md`:
  `docs/api_reference/`, `docs/index.md`, README, mkdocs nav, ArgoCD health
  checks, the CSV owned-resources list, the `helm-sync-artifacthub-crds.sh`
  `describe()` row, and `config/samples/`.

This is the cost that makes option (a) unattractive independent of B1–B3.

### B5 — Credential aggregation changes the blast radius

The hub holds N kubeconfigs **and** N Neo4j admin Secrets. Today an admin
Secret lives in the same cluster as the database it unlocks; a hub requires a
copy centrally. For the deployments that motivated much of this operator's
hardening work, "one namespace that can administer every Neo4j in the estate"
is a materially different security posture, not a deployment detail.

### B6 — Failure isolation is lost

Today a spoke operator failing takes out one Kubernetes cluster. With a hub,
hub downtime means **nothing reconciles anywhere**, including the failover
paths (`Neo4jReplicaPromotion`) most likely to be needed during an incident
that also stresses the hub. A DR mechanism whose control plane is a single
remote process is a weaker DR mechanism.

### B7 — Watch and cache cost multiplies, and `WATCH_NAMESPACE` becomes per-cluster

Each remote `cluster.Cluster` carries its own informer cache. A hub watching
Pods, StatefulSets, Services, Secrets and ConfigMaps across N clusters holds N
copies. `WATCH_NAMESPACE` (`cmd/main.go:278`) is currently global and would
have to become a per-remote-cluster scope, or the hub's memory profile becomes
a function of the largest spoke.

### B8 — TLS trust is directional and currently one-sided

The hub's Neo4j driver must trust each spoke's CA, and the spoke's certificate
must carry a SAN matching whatever external name the hub dials. The SAN half is
already expressible (`spec.service.dnsName`, §2). The trust half is the hub's
own problem — it is a Go process with a driver, not a Neo4j server, so
`spec.tls.additionalClusterTrustCAs` (which projects peer CAs into `/ssl/trusted/`
for the *database*) does not apply. The hub needs its own CA bundle
construction. New, but small, and unblocked.

---

## 4. Options

### (a) Full hub — all controllers, remote clients

The literal reading. Every controller takes a per-target client; a
`Neo4jRemoteCluster` CR supplies the kubeconfig and the Bolt endpoints.

Requires: B1 + B2 + B3 (i.e. rebuild the CCDR proxy on the Bolt ports,
mandatory and per-cluster, with per-ordinal exposure), **plus** B4 across
eleven CRDs and their validators, **plus** accepting B5–B7.

Honest assessment: this is the KubeFed shape, and it is large for the same
reasons KubeFed was.

### (b) Split hub — Kubernetes-API controllers remote, Bolt controllers local

Superficially attractive: let the hub own `Neo4jEnterpriseCluster` and
`Neo4jEnterpriseStandalone` (which mostly build StatefulSets, Services,
ConfigMaps and Certificates), and leave the Bolt-dependent controllers running
locally in each spoke.

**It does not survive contact with the code.** The infrastructure controllers
are not Bolt-free — see B1's second group. Diagnostics
(`neo4jenterprisecluster_controller.go:639`), split-brain detection, **rolling
upgrade** (`rolling_upgrade.go`) and **scale-down draining** (`scale_down.go`)
all open a Neo4j client, most of them per-pod. The split would have to be drawn
*inside* a single controller's reconcile loop rather than between controllers,
and the hub would end up able to create a cluster but not to upgrade, scale
down, or observe it. Rejected.
Rejected.

### (c) Aggregation-only hub, then hub-authored spoke CRs — **recommended**

Invert the direction. Each Kubernetes cluster keeps its own operator, with its
local Bolt path unchanged. The hub gets a read-mostly view. Detailed in §5.

### (d) Decline — document GitOps instead

A large fraction of "does it support multi-cluster?" means "can I apply the
same manifests to five clusters?" That is Argo ApplicationSets or Flux, plus
the operator installed per cluster, and it works today. This option costs a
documentation page.

**(d) is not a joke option and should ship regardless of what else does** — it
is the correct answer for most askers, and shipping it first tells you whether
the remaining demand is real.

---

## 5. The recommended design — option (c)

### 5.1 Principle

> **Spokes reconcile. The hub aggregates. The hub never speaks Bolt.**

Every blocker in §3 that is expensive — B1, B2, B3, and most of B5 — exists
only because a hub was assumed to hold the Neo4j connection. Move reconciliation
back to the spoke and they evaporate; the spoke's Bolt path is the in-cluster
one that already works.

The B1 finding that the *infrastructure* controllers also need Bolt — rolling
upgrade, scale-down draining, split-brain, diagnostics — cuts in this option's
favour rather than against it. Under (a) those become four more things needing
remote Bolt; under (c) they keep working untouched, because they never leave
the cluster they operate on.

### 5.2 Phase 1 — read-only aggregation

One new CRD, hub-side only:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRemoteCluster
metadata:
  name: eu-west-prod
  namespace: neo4j-operator
spec:
  kubeconfigSecretRef:
    name: eu-west-prod-kubeconfig
    key: config
  namespaces: [neo4j-prod, neo4j-staging]   # required; unbounded scope is rejected (B7)
  pollInterval: 30s
status:
  phase: Connected              # Pending | Connected | Unreachable | Forbidden
  observedAt: "2026-08-31T09:14:22Z"
  deployments:
    - kind: Neo4jEnterpriseCluster
      namespace: neo4j-prod
      name: prod
      phase: Ready
      version: "2026.06.0"
      servers: 3
      conditions: [{type: ServersHealthy, status: "True"}, ...]
  collectionError: ""           # non-fatal, same contract as status.diagnostics
```

Properties that make this cheap:

- **No Bolt.** The spoke operator already materialises everything interesting
  into `status` — `status.diagnostics.servers[]`, `status.diagnostics.databases[]`,
  the `ServersHealthy` / `DatabasesHealthy` conditions. The hub reads status; it
  does not re-derive it.
- **No `clusterRef` change (B4 avoided entirely).** `Neo4jRemoteCluster` is a
  new Kind that no existing CRD references. The eleven-CRD surface change does
  not happen.
- **Read-only RBAC on the spoke.** The kubeconfig needs `get`/`list`/`watch` on
  the Neo4j CRDs in named namespaces and nothing else — a far easier security
  review than B5's "can administer every database in the estate".
- **Hub downtime is a visibility outage, not a reconciliation outage** (B6
  avoided).
- **Non-fatal by construction.** An unreachable spoke sets
  `status.phase: Unreachable` and `collectionError`, never `return err` — the
  same contract `CollectDiagnostics` already uses.

Implementation notes, following house patterns: `cluster.New` per
`Neo4jRemoteCluster` added to the manager as a Runnable, with
`source.Kind(remoteCache, &Neo4jEnterpriseCluster{}, ...)` so a spoke status
change wakes the hub reconciler rather than being polled; inline validation in
`internal/validation/remotecluster_validator.go` (**never a webhook** —
invariant 1); finalizer that stops and drops the remote cluster's cache;
`retry.RetryOnConflict` on status writes; structured events from
`internal/controller/events.go`.

### 5.3 Phase 1b — the fleet metric, nearly free — *implemented*

Independently of the CRD: an operator flag labelling
`neo4j_operator_server_health` with the Kubernetes cluster it runs in, so
ordinary Prometheus federation gives a cross-cluster health view with no hub,
no kubeconfig, and no new CRD. It covers reading (4) in §1 for people who do
not use Aura Fleet Manager, and it landed before Phase 1 precisely because it
may satisfy the requirement outright.

**Shipped as `--kubernetes-cluster-name`, emitting the `k8s_cluster` label**
(`internal/metrics/metrics.go`, `cmd/main.go`,
`charts/neo4j-operator/values.yaml: kubernetesClusterName`). Two naming and
one mechanism decision are worth recording:

- **Not `--cluster-name`.** `cluster_name` is an established metric label on
  every operator metric and it means the **Neo4j** cluster. A flag by that name
  would have been read as setting it. The ambiguity is not cosmetic — a
  federated dashboard that sums across the wrong one of these two labels is
  wrong in a way nobody notices.
- **Not a Prometheus `ConstLabel`.** A constant label would be the tidier
  mechanism, but metrics are registered in `init()` (`metrics.go:243`), which
  runs before `flag.Parse()`. The name is therefore held in an `atomic.Value`
  and read at record time.
- **Empty by default, and that is the compatibility story.** Unset emits an
  empty `k8s_cluster` label, which Prometheus treats as equivalent to an absent
  label for matching, so existing queries and dashboards are unaffected. The
  Helm chart omits the flag entirely when the value is empty.

### 5.4 Phase 2 — hub-authored spoke CRs (only if writes are genuinely needed)

If declarative fan-out is required — "create database `foo` on these six
clusters" — the hub **writes a `Neo4jDatabase` manifest into the spoke's
namespace** and the spoke's local operator reconciles it over its local Bolt
connection. The hub still never opens a driver.

```yaml
kind: Neo4jFleetDatabase          # hub-side
spec:
  targets:
    clusterSelector: {matchLabels: {tier: prod}}   # selects Neo4jRemoteClusters
  template:                                        # a Neo4jDatabase spec
    name: analytics
    topology: {primaries: 3, secondaries: 1}
status:
  targets:
    - cluster: eu-west-prod
      applied: true
      observedPhase: Ready
```

This is the Open Cluster Management shape (a manifest-delivery model), and it
is the only version of hub-side *writes* that does not require re-solving
B1–B3. Two things to design carefully if it is ever built:

- **Deletion semantics.** Removing a target from the selector must not silently
  drop a database. Default to orphan-and-warn, mirroring §5.5 of the CCDR
  design ("removing a CR that no longer describes anything must not be a
  data-loss event").
- **GitOps collision.** A hub writing objects into a spoke that Argo also
  manages produces exactly the out-of-sync/prune fight the CCDR design cited
  when it rejected an operator-authored handoff. The hub's written objects need
  a documented ownership label and an Argo ignore convention, or the two
  controllers will fight.

### 5.5 What option (c) explicitly does not give you

Stated plainly, because a hub that quietly does less than expected is worse
than one that declines:

- No hub-side split-brain detection or live diagnostics for remote clusters —
  the spoke does that locally, and the hub sees only its published conclusion.
- No hub-side Cypher. `Neo4jUser` / `Neo4jRole` / ad-hoc administration remain
  spoke-local operations (Phase 2 can *deliver* the CRs, but the spoke executes).
- No single point of control during a spoke API outage; if the spoke's API
  server is down the hub can neither read nor write it, and there is no
  fallback path.

---

## 6. What full hub/spoke would additionally require

Recorded so that a future decision to build (a) does not re-derive it.

1. **A Bolt exposure mechanism per managed cluster**, per ordinal, mandatory —
   structurally the CCDR proxy (`internal/resources/ccdr_proxy.go`) retargeted
   from 6000 to 7687, plus an override for `server.routing.advertised_address`
   that the config validator currently forbids.
2. **`buildConnectionURIForEnterprise` becomes endpoint-driven**, taking a
   resolved endpoint rather than composing an in-cluster FQDN; same for the
   standalone builder and `splitbrain_detector.go`.
3. **`clusterRef.cluster` across eleven CRDs** plus their validators, plus the
   per-CRD hand-written surface list (B4).
4. **A hub CA bundle** for the driver's trust of each spoke (B8).
5. **Per-remote-cluster `WATCH_NAMESPACE` scoping** (B7).
6. **An answer to B5 and B6** that a security reviewer accepts.

Items 1 and 2 are the ones with no cheap version.

---

## 7. Testing

- **Option (c) fits the project's testing invariants; option (a) does not.**
  The hub in (c) runs no Neo4j, so a hub-plus-one-spoke test needs **one**
  Enterprise deployment — compatible with the standing rule that only one
  Enterprise deployment runs at a time (concurrent JVMs wedge Bolt on a laptop).
  A meaningful (a) test needs at least two live deployments in different
  Kubernetes clusters, which that rule forbids on a developer machine and which
  CI has no tooling for.
- **Two Kind clusters are viable.** Kind clusters share a Docker network by
  default and can route to each other — the technique the CCDR design already
  identified. The known gap is unchanged: plain Kind has no cloud provider, so
  `type: LoadBalancer` never gets an address, and this repo has no MetalLB /
  `cloud-provider-kind` wiring yet. **Option (c) does not need a LoadBalancer**
  — the hub talks to the spoke's API server, which Kind publishes — so this gap
  blocks (a) and not (c).
- **Unit:** kubeconfig parsing and rejection of an unbounded namespace scope;
  status projection from a fake spoke cluster; the `Unreachable` /
  `Forbidden` branches; validator table tests.
- **Integration:** `Label("core")` for CRD admission and the validator, no
  second cluster required. A genuine two-Kind-cluster aggregation spec is
  `Label("extended")` and dispatch-gated, mirroring
  `isPropertyShardingCompatible()`.

---

## 8. Delivery order

1. ~~**Option (d)** — a `docs/user_guide/guides/multi_cluster.md` page covering
   ApplicationSet/Flux fan-out with a per-cluster operator~~ → **done.**
   Cheapest, correct for most askers, and a demand probe for everything below.
2. ~~**§5.3** — `--cluster-name` flag + metric label + a federation example~~ →
   **done**, as `--kubernetes-cluster-name` / `k8s_cluster` (§5.3). No new API
   surface.
3. **§5.2** — `Neo4jRemoteCluster`, read-only aggregation, with the full
   hand-written-surface checklist from `CLAUDE.md` (api_reference, index, README,
   nav, ArgoCD health check keyed off the `Ready` **condition**, CSV, helm-sync
   `describe()` row, sample, `devControllerKeys`).
4. **§5.4** — `Neo4jFleetDatabase` manifest delivery, only on evidence from 1–3
   that read-only aggregation is insufficient.
5. **Option (a)** — not scheduled. §6 is its prerequisite list.

---

## 9. Open questions

**Q1 — Is a stretched single Neo4j cluster across Kubernetes clusters worth a
separate analysis? Leaning no.** It requires every port externally routable —
including `server.cluster.raft.advertised_address` (7000), which the CCDR
design pointedly refused to touch so that "RAFT leader election never depends
on the load balancer" — and it puts a WAN round-trip on the write path via RAFT
commit. The one defensible variant is Kubernetes clusters on a flat L3 network
with mutually routable pod IPs (Cilium ClusterMesh, Submariner), where the work
reduces to letting the discovery endpoint list include servers the local
StatefulSet does not own. Even then, two independent control planes each
believing they own part of one DBMS is an unsolved ownership problem. Neo4j's
own answer to geo-distribution is CCDR.

**Q2 — Does the aggregated status actually satisfy the requirement?** Everything
§5.2 surfaces is already in spoke `status`. If the real ask is cross-cluster
*actions* rather than cross-cluster *visibility*, Phase 1 is a detour and the
question becomes §5.4 vs. (d) directly. **This should be answered by a user
before item 3 in §8 is started**, because it is the difference between a
one-CRD feature and a fan-out delivery controller.

**Q3 — Should `Neo4jRemoteCluster` reuse the Aura fleet plumbing?**
`spec.auraFleetManagement` already registers deployments with a cross-cluster
fleet service, and `fleet_target.go` already abstracts "cluster or standalone"
for it. Whether the hub's aggregation should be a second consumer of that
abstraction or an independent path is an implementation question that affects
how much of §5.2 is new code.

**Q4 — Hub-side kubeconfig rotation.** Kubeconfigs expire. The design needs a
defined behaviour for credential expiry that is distinguishable in `status`
from a genuinely unreachable spoke (`Forbidden` vs. `Unreachable` above is a
first cut, not a rotation strategy).

**Q5 — Is there a customer for (a)?** §6 is a large, mostly irreversible bill.
It should not be paid on the strength of the phrase "multi-cluster" appearing
in a requirements document.
