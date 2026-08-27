# Design: Cross-Cluster Database Replication (CCDR) on Kubernetes

> **Status:** Draft for review. Not an official product or version commitment.
> **Source:** Neo4j Operations Manual, *Replicating databases across clusters* (pre-publication preview, 2026.07 docs branch). The page requires a **downstream cluster on 2026.08 or later**; the upstream minimum is stated as 2025.01 but see §9 — mixed-version support is under active test upstream and should not be hardened into operator validation yet.
> **Scope of this document:** what CCDR needs from a Kubernetes deployment, what this operator already provides, what actually blocks it, and a two-phase delivery shape. Phase 1 (backup-based) is designed to a level a contributor can implement from. Phase 2 (network-based) is deliberately left at the "what must be answered first" stage.
> **Headline:** backup-based CCDR is largely deliverable on top of machinery the operator already has, and is unaffected by everything below. Network-based CCDR is blocked on an addressing problem that no amount of Service configuration resolves (§9 Q1, confirmed), and the clean fix — a cross-cluster address distinct from the intra-cluster one — **does not exist in Neo4j's documented setting surface** (§9 Q1a). What remains is a product decision: accept a cluster-wide addressing change that degrades something working today for every non-CCDR user (§6.2 / §6.3), or ship backup-based replication only and decline the network path on Kubernetes (§6.4). This design recommends the latter.

---

## 1. What CCDR requires

CCDR replicates one database from an **upstream** cluster to a read-only **replica** on a downstream cluster. Two mechanisms, with very different infrastructure costs:

| | Network replication | Backup replication |
|---|---|---|
| Downstream needs | routable `addresses` for the upstream's **:6000** cluster endpoints | an object-store URI (`pullURI`) |
| Cross-cluster networking | required (L4), inter-cluster encryption + mutual CA trust strongly recommended | **none** |
| Upstream duty | be reachable | *"the user is responsible for creating and pushing the complete backup chain"* + ongoing differentials |
| RPO | near-zero, modulo async lag | bounded by `db.cluster.backup.pull_interval` (default `1m`) |
| Failure mode | connectivity | *"the differential backup chain must remain unbroken"* — a break requires recreating the replica |

Shared constraints, both mechanisms:

- **Replicas are read-only.** Both primaries and secondaries refuse writes. Clients must use `AccessMode.READ` / `executeRead`; `cypher-shell --access-mode=read`.
- **`system` cannot be replicated.**
- **Privileges and roles are not copied.** They must be set up separately on the downstream.
- **Promotion is one-way.** `CALL dbms.promoteReplicaDatabase(name, options)`; once promoted, *"it cannot be re-attached to the original source."*

Cypher surface:

```cypher
CREATE REPLICA DATABASE name
[[SET] TOPOLOGY n PRIMARIES [m SECONDARIES]]
OPTIONS {replicaConfig: {remote: upstreamName, addresses: [...]}}   -- network
OPTIONS {seedURI: "...", replicaConfig: {remote: upstreamName, pullURI: "..."}}  -- backup
[WAIT|NOWAIT]

CALL dbms.promoteReplicaDatabase('foo-replica', {primaries: 3, secondaries: 4});
SHOW DATABASE `foo-replica` YIELD name, type, access, address, role, writer, options;
```

---

## 2. What this operator already provides

The backup-based mechanism's upstream half — *"the user is responsible for creating and pushing the complete backup chain"* — is already automated by `Neo4jBackup`:

- `spec.options.backupType: FULL | DIFF | AUTO` (`api/v1beta1/neo4jbackup_types.go:197`)
- `spec.chainFromBackup` (`:108`) — a daily FULL CR plus an hourly DIFF CR writing into one shared per-CR directory, with a `part-of` label interlock that refuses to submit a Job while another Job in the same chain is active. This is a differential-chain generator.
- `spec.options.preferDiffAsParent` (`:238`, CalVer 2025.04+) — diffs chain off the previous diff rather than the last full.
- `spec.storage` (`StorageLocation`, `api/v1beta1/neo4jenterprisecluster_types.go:770`) — s3/gcs/azure/pvc with `credentialsSecretRef` or workload identity.
- `status.backupRuns[].backupsPath` (`api/v1beta1/neo4jbackup_types.go:365`) — *"SAME VALUE FOR EVERY RUN OF ONE CR — all runs accumulate in this directory so neo4j-admin can chain differential backups off the prior full."* **This directory is exactly what a downstream `pullURI` must point at**, and the operator already publishes it in status.

Downstream-side pieces that also already exist:

- `db.cluster.backup.pull_interval` needs no new API — `spec.config` passthrough covers it.
- `Neo4jDatabase.spec.seedURI` / `seedConfig` / `seedCredentials` already model seeding from an object-store backup.
- **The "roles are not replicated" limitation is neutralised** by applying the same `Neo4jUser` / `Neo4jRole` / `Neo4jRoleBinding` CRs to the downstream cluster. A documented CCDR caveat becomes a copy-paste. This is a real advantage of doing CCDR through the operator rather than by hand.

Consequence: **backup-based CCDR needs no cross-cluster networking, no load balancer, no advertised-address change, no mutual TLS, and no NetworkPolicy hole.** It sidesteps blockers B1–B4 below entirely.

---

## 3. Blockers

Ranked. B1–B4 apply to the **network** mechanism only; B5–B8 apply to both.

### B1 — Advertised addresses are hardcoded to in-cluster DNS *(network only; the real blocker)*

`internal/resources/cluster.go:2249,2272-2275` pins:

```
export HOSTNAME_FQDN="${HOSTNAME}.<cluster>-headless.<namespace>.svc.cluster.local"
server.default_advertised_address=${HOSTNAME_FQDN}
server.cluster.advertised_address=${HOSTNAME_FQDN}:6000
```

and `internal/validation/config_validator.go:85-91` **forbids** overriding any `*.advertised_address` via `spec.config` (`operatorRuntimeManagedSettings`).

**Confirmed behaviour (§9 Q1).** There *is* server-side routing and discovery. The downstream dials an address from `replicaConfig.addresses` — one is sufficient — and the upstream **returns the addresses the downstream then reaches out to**. Those returned addresses are the upstream servers' advertised cluster addresses. A listed address therefore need not host the upstream database; it only has to be reachable enough to answer the routing request.

The consequence is the worst case for Kubernetes:

> The downstream connects to a listed address, receives `prod-server-N.prod-headless.neo4j.svc.cluster.local:6000` in reply, and fails — that name resolves only inside the upstream's own Kubernetes cluster.

**There is no workaround on the listing side.** Enumerating every server's external address in `replicaConfig.addresses` does not help, because the addresses the downstream actually uses are the ones the upstream hands back, not the ones it was given. `server.cluster.advertised_address` must itself be externally routable.

That makes the structural conflict below unavoidable rather than merely awkward, and it is the central Phase 2 design problem:

> There is one `server.cluster.advertised_address` per server, and it serves two consumers with incompatible requirements — intra-cluster peer traffic (wants the internal FQDN; changing it re-routes all normal cluster traffic through an external hop) and cross-cluster CCDR identity (wants an externally routable address, because that value is what gets handed to the downstream).

Resolving that is §9 Q1a. **Phase 1 (backup-based) is entirely unaffected** — it opens no network path between clusters and never consults an advertised address.

### B2 — No per-server external exposure

`spec.service` builds one front-facing Service selecting all pods (ClusterIP/NodePort/LoadBalancer), plus a single `spec.service.dnsName`. CCDR wants N stable, individually-addressable endpoints on :6000. There is no per-pod Service builder.

### B3 — TLS: SANs and cross-cluster trust are both gapped, and the documented workaround does not work

- **SANs.** `BuildCertificateForEnterprise` (`internal/resources/cluster.go:651-700`) enumerates internal service and pod FQDNs plus one optional `spec.service.dnsName`. With `strictPeerValidation: true` (the default) the cluster policy sets `verify_hostname=true` (`cluster.go:151-153`), so a handshake to an external per-server address fails unless that name is a SAN.
- **Trust.** The manual requires each cluster to trust the other's CA. Neo4j's cluster policy reads its trust anchor from `/ssl/trusted/`, which the operator projects as a single item — the cluster's *own* `ca.crt` (`cluster.go:1408`). `spec.trustedCASecrets` does **not** land there: it builds a JVM JKS at `/truststore/truststore.jks`, which `dbms.ssl.policy.cluster.*` never reads — despite the field's own doc comment claiming it covers "cross-cluster replication".
- **The escape hatch is illusory.** `docs/user_guide/tls_configuration.md:469` points users at `extraVolumes`/`extraVolumeMounts` for exactly this case. `/ssl` is reserved (`internal/validation/truststore_validator.go:38`), and although `isReservedMountPath` is an exact-match check so `/ssl/trusted` would technically pass validation, **mounting there shadows the operator's own `trusted/ca.crt` and breaks intra-cluster TLS.** Mutual trust cannot be achieved through the documented path.

  *This is a docs-vs-code inconsistency worth fixing independently of CCDR.*

### B4 — NetworkPolicy drops inbound cluster-protocol traffic

`internal/resources/networkpolicy.go` rule 2 gates 6000/7000/7688/7689 on `podSelector: neo4j.com/cluster=<name>`. Traffic from a remote cluster matches no local pod and is dropped. The policy is opt-in (`spec.networkPolicy.enabled`), so this is a silent breakage for anyone who has hardened, not a universal blocker.

### B5 — No CRD or Cypher surface for replicas

`internal/neo4j/client.go` only ever emits `CREATE DATABASE`. Nothing emits `CREATE REPLICA DATABASE`, and `Neo4jDatabase.spec.options` is `map[string]string`, which cannot express the nested `replicaConfig: {remote, addresses[]}` map. A replica is not reachable through the existing CRD surface even by hand.

### B6 — The operator cannot tell a replica from a standard database

`DatabaseInfo` (`internal/neo4j/client.go:129`) carries `Name, Status, Default, Home, Role, RequestedStatus` — no `type`, no `writer`, no `access`. **This is a prerequisite for the promotion safety guard in §5.4, not a nice-to-have.**

Health checking itself is unaffected: `updateDatabasesCondition` compares `Status` against `RequestedStatus`, so an online replica reads healthy and raises no false alarm.

### B7 — Version gating is absent — *implemented, delivery item 2*

Downstream must be ≥ 2026.08. Implemented in `internal/neo4j/version.go` as
**`SupportsCCDRReplica()`**, alongside `MeetsCCDRUpstreamFloor()` and the
`MinCCDRReplicaVersion` / `MinCCDRUpstreamVersion` constants.

Named for the *role*, not the feature — the placeholder name `SupportsCCDR()`
used earlier in this document was a trap, because upstream and downstream have
different floors (2025.01 vs 2026.08) and a single predicate would silently
answer the wrong question at half its call sites.

The two gates are also enforced differently, deliberately. The operator manages
the downstream cluster and can *observe* its version, so `SupportsCCDRReplica`
is a hard gate. The upstream lives in another Kubernetes cluster the operator
cannot inspect, so any upstream version is a user-supplied claim — callers must
**warn, not reject**, on `MeetsCCDRUpstreamFloor` (§9 Q2).

Note the CI anchor CalVer is currently 2026.06, so CCDR specs will be
dispatch-gated and largely local-only until the anchor moves — the same
treatment property sharding gets via `isPropertyShardingCompatible()`.

### B8 — Restoring or recreating an upstream database silently detaches its replicas

A restore changes the database's internal ID; downstream replicas detach and must be recreated from a fresh chain. `Neo4jRestore` and the `dbms.recreateDatabase` path (`internal/neo4j/version.go:RecreateDatabaseProcedure`) have no notion that replicas exist — and cannot, since replicas live in a different Kubernetes cluster with no back-reference. Mitigation is documentation plus, optionally, an advisory annotation on the upstream `Neo4jDatabase` that the restore controller surfaces as a warning event before proceeding.

---

## 4. Phase 1 — `Neo4jReplicaDatabase` (backup-based)

### 4.1 Why a new CRD rather than fields on `Neo4jDatabase`

A replica is not a database with extra options:

- it is permanently read-only, so `initialData` and every write-path field are meaningless;
- it takes no `CREATE DATABASE` options, and `options map[string]string` cannot hold `replicaConfig` anyway (B5);
- it has a one-way terminal lifecycle (§5) that a standard database does not have;
- roughly half of `Neo4jDatabaseSpec` would have to be rejected by a validator for `kind: replica`, which is the usual signal that two things are being modelled as one.

### 4.2 Shape

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jReplicaDatabase
metadata:
  name: foo-replica
  namespace: dr
spec:
  clusterRef: dr-cluster          # downstream cluster, same namespace
  name: foo-replica               # database name — permanent, see §5.6
  upstreamDatabase: foo           # -> replicaConfig.remote
  topology:
    primaries: 3
    secondaries: 1
  source:
    mode: backup                  # backup | network   (network = Phase 2)
    pullURI: s3://backups/prod-foo/          # -> replicaConfig.pullURI
    seedURI: s3://backups/prod-foo/foo-2026-08-01T01-00-00.backup
    credentialsSecretRef: s3-creds
  pullInterval: 1m                # -> db.cluster.backup.pull_interval
status:
  phase: Replicating              # Pending | Seeding | Replicating | Promoted | Failed
  databaseType: replica           # observed from SHOW DATABASE ... YIELD type
  lastAppliedTxId: 918273
  conditions: [...]
```

`pullURI` should point at the upstream `Neo4jBackup` CR's `status.backupRuns[].backupsPath` directory (§2). Document that correspondence explicitly — it is the single most likely thing for a user to get wrong.

### 4.3 Reconcile

Standard project patterns: finalizer, `retry.RetryOnConflict`, inline validation in `internal/validation/` (never a webhook), structured events from `internal/controller/events.go`.

1. Resolve `clusterRef`; require downstream version ≥ 2026.08 via `Version.SupportsCCDRReplica()`. Quote `MinCCDRReplicaVersion` in the failure message rather than a literal, so gate and message cannot drift.
2. **Observe before acting** — `SHOW DATABASE <name> YIELD name, type, access, writer`. This drives every branch below and is the safety spine of §5.
3. Absent → `CREATE REPLICA DATABASE ... OPTIONS {seedURI, replicaConfig:{remote, pullURI}}`.
4. Present and `type = replica` → reconcile topology drift only. **Do not** attempt to change `replicaConfig`; treat `source` as immutable via a CEL `self == oldSelf` transition rule (the project uses apiserver-side CEL for immutability, consistent with invariant 1's no-webhook rule).
5. Present and `type != replica` → **promoted, out of band or otherwise.** Go terminal. Never mutate. See §5.4.

### 4.4 `Neo4jBackup` gains a replication-source mode

Decided (§9 Q5). A backup that feeds a replica has requirements a general-purpose backup does not — an unbroken differential chain and a stable directory — and today nothing stops a user from configuring a chain-breaking setup. `mode: replication-source` turns that class of footgun into validation errors at apply time.

```yaml
kind: Neo4jBackup
spec:
  mode: replication-source        # standard (default) | replication-source
  instanceRef: prod-cluster
  database: foo
  schedule: "0 * * * *"
  storage: {type: s3, bucket: backups, path: prod-foo}
status:
  replicationPullURI: s3://backups/prod-foo/<chain-dir>/
```

Validator rules when `mode: replication-source`:

- **R1 — single-database scope required.** Reject `allDatabases` (the instance-wide artifact layout is not what a per-database `pullURI` consumes). Reject `shardedDatabase` in Phase 1; sharded CCDR is its own track.
- **R2 — reject an operator-side `spec.retention`.** For PVC storage the delete-time cleanup Job would prune a diff's parent and break the chain outright.
- **R3 — reject a competing writer.** Another `Neo4jBackup` CR whose `storage` (type + bucket + path) collides, and which is not part of this chain via `chainFromBackup`, is rejected. The existing `part-of` label interlock only serialises *Jobs within one chain*; it does not stop an unrelated CR from sharing a directory.
- **R4 — require `schedule`.** A replication source with no cadence is a replica that falls arbitrarily far behind.
- **R5 — publish `status.replicationPullURI`.** The exact string to paste into the downstream `Neo4jReplicaDatabase.spec.source.pullURI`, so nobody hand-assembles bucket + path + `backupsPath`. This is the main ergonomic payoff.

Mode is declared on the chain root and inherited by members; a member declaring a conflicting mode is rejected.

**What this mode cannot protect against, and must say so.** For cloud storage the operator does not prune at all — `RetentionPolicy` delegates to bucket lifecycle rules (`api/v1beta1/neo4jbackup_types.go:162-165`), which the operator can neither read nor validate. **An S3 lifecycle rule expiring old objects will silently break the chain and force a replica rebuild, and no operator-side validation can catch it.** `replication-source` must therefore emit a warning event on first reconcile naming this risk, and the user guide must state it plainly rather than implying the mode confers protection it does not.

---

## 5. The promotion lifecycle

This is the part of the design most likely to cause data loss if it is got wrong, so it is specified in full.

### 5.1 The hazard

`dbms.promoteReplicaDatabase` strips the replicating-from configuration. The database keeps its name but its `type` changes from `replica` to a standard database, `access` becomes read-write, `writer` becomes true. It is irreversible.

A `Neo4jReplicaDatabase` spec describes a *relationship* that, after promotion, no longer exists. A naive level-triggered reconciler compares `desired: replica of foo` against `observed: standard database` and "corrects the drift" — by dropping and recreating.

**That drops the database you promoted precisely because it had become your live system.** The design must close this by construction, not by care.

### 5.2 Promotion is a separate one-shot CR, not a spec field

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jReplicaPromotion
metadata:
  name: failover-2026-08-07
  namespace: dr
spec:
  replicaRef: foo-replica         # the Neo4jReplicaDatabase CR
  topology:                       # optional; omitted = retain current topology
    primaries: 3
    secondaries: 4
status:
  phase: Completed                # Pending | Promoting | Completed | Failed
  completionTime: "2026-08-07T09:14:22Z"
  observedLagTxIds: 42            # RPO actually taken — see §5.7
```

Rejected alternative: `spec.promote: true` on the replica CR. A boolean in a level-triggered spec can be flipped back, and "false" would mean *make it a replica again*, which Neo4j cannot do — the only way to honour it is drop-and-reseed. A CEL immutability rule closes the interactive path but not the one that matters: **a GitOps controller re-applying the pre-promotion manifest**, or a manifest restored from backup, is indistinguishable from a deliberate flip. Argo/Flux drift-correction would actively re-assert `promote: false` after a failover. A one-shot CR is inert to re-apply.

It also matches the house idiom (`Neo4jBackup`, `Neo4jRestore` are both one-shot CRs with terminal phases) and gives promotion its own pollable status, which the equivalent cloud-side API design also concluded it needed.

### 5.3 What happens to the `Neo4jReplicaDatabase` CR

**It stays, and it goes inert.** Concretely:

- `status.phase: Promoted`, plus `promotedAt` and `promotedBy: <Neo4jReplicaPromotion name>`.
- The controller short-circuits at the top of `Reconcile` — same shape as `internal/controller/neo4jrestore_controller.go:257-259`. No `CREATE REPLICA`, no topology reconciliation, no `pullURI` re-application, no drift correction of any kind.
- The CR remains as the audit record of where the database came from, and as the holder of a now-declawed finalizer (§5.5).

Deleting the CR is *not* required after promotion, and nothing breaks if the user leaves it in place indefinitely.

### 5.4 The guard is observed state, not the status field

Status is a fast path, not a correctness mechanism — it can be lost to a status-subresource wipe or an etcd restore. **Before any mutating call, the controller re-checks `SHOW DATABASE <name> YIELD type`.** If `type != 'replica'`:

- refuse to act,
- set `phase: Promoted` with reason `DetectedOutOfBand`,
- emit a Warning event,
- requeue nothing.

This is what makes an out-of-band `cypher-shell` promotion safe, and it is the load-bearing guard. It also makes promotion **crash-safe without an idempotency token**: the ordering is *check → promote → check → record*, so a crash between the procedure call and the status write is recovered by the next reconcile observing `type != replica` and concluding the promotion succeeded, rather than calling the procedure a second time against an already-promoted database.

This is the concrete reason B6 (`DatabaseInfo` has no `type` field) is a prerequisite and not a nicety — **today the operator has no way to make this check at all.**

### 5.5 Deletion after promotion must never drop

- **Before promotion:** the finalizer drops the replica. Consistent with `Neo4jDatabase`, whose finalizer always drops (`internal/controller/neo4jdatabase_controller.go:326-340`).
- **After promotion:** the finalizer **releases without dropping**, and emits a Warning event naming the retained database.

Rationale: you promoted because this database became the live system. Removing a CR that no longer describes anything must not be a data-loss event. This is a deliberate asymmetry with `Neo4jDatabase` (which has no retain policy at all) and should be called out in the user guide.

### 5.6 Handing back to declarative management

After promotion the database is an ordinary standard database, so its natural long-term home is `Neo4jDatabase`.

**Recommended for Phase 1 — explicit manual handoff.** `status` publishes the exact `Neo4jDatabase` spec to apply. The user applies it with `ifNotExists: true`, so `CREATE DATABASE ... IF NOT EXISTS` is a no-op and the CR **adopts** the promoted database. The replica CR can then be deleted (harmlessly, per §5.5) or kept as history.

**Rejected for Phase 1 — operator-authored handoff.** Having the controller create the `Neo4jDatabase` CR itself fights GitOps (Argo immediately flags an undeclared object as out-of-sync, and may prune it), and the ownerRef question has no good answer: owned by the replica CR means deleting the replica CR cascades and drops the production database; not owned means an orphan.

**Double-ownership.** During handoff both CRs exist and both nominally carry a database finalizer. This is safe *because* the replica CR's finalizer is already declawed by §5.5 — only `Neo4jDatabase` can drop. Order of operations does not matter. Worth stating explicitly in the user guide, because it is the kind of thing an operator-savvy user will (correctly) worry about.

### 5.7 Naming — and why aliases are the answer

**Cypher has no `RENAME DATABASE`.** `ALTER DATABASE` covers access mode, default language, options and topology only. A replica named `foo-replica` therefore keeps that name forever, including after it becomes the production database — and applications failing over to DR would have to target `foo-replica`.

The clean fix is a **database alias** on the downstream cluster:

```cypher
CREATE ALIAS `foo` FOR DATABASE `foo-replica`;
```

**Aliases can target a database that is still a replica** (§9 Q3, answered), which is better than it first appears — it means the alias is **pre-staged at replica-creation time, not created during the failover window**:

| | Alias `foo` on the DR cluster resolves to | Client experience |
|---|---|---|
| Steady state | `foo-replica`, a read-only replica | reads succeed, writes refused |
| After promotion | `foo-replica`, now a standard database | reads and writes succeed |

So applications on the DR side address `foo` throughout and **need no reconfiguration at failover** — the same connection string silently gains write capability the moment promotion completes. Nothing has to be executed inside the outage window, which is exactly when you least want a manual `cypher-shell` step.

Two consequences for this design:

1. The failover runbook's alias step moves *left*, to replica setup. Document it there, and make the sample manifests create it by default rather than presenting it as an optional extra.
2. The operator has no alias surface today, so this is currently a manual step at setup time. Because it is now part of the recommended steady-state configuration rather than a break-glass action, **`Neo4jDatabaseAlias` has a stronger claim on Phase 1 scope than when it was filed as a follow-on** (§9 Q6). Flagged, not decided.

### 5.8 Promoting a lagging replica

Promotion freezes the current lag as permanent data loss — there is no re-attaching to catch up afterwards. The promotion CR should therefore record the observed lag at promote time (`status.observedLagTxIds`) so the RPO actually taken is auditable after an incident. The manual's suggested signal is the delta in `<prefix>.database.<db>.transaction.last_committed_tx_id` between upstream and replica; where the upstream is unreachable (the usual reason for a failover) record the replica's last applied id alone.

This is deliberately *recorded*, not *enforced*. Blocking a failover on a lag threshold is the wrong default — during a real outage the operator does not get to second-guess the human.

---

## 6. Phase 2 — network replication

**Status: implemented.** §6.1–§6.5 below is the option analysis that led to the
design, kept as background for *why* the simpler paths were not chosen. Jump
to §6.6 for what actually shipped, or to
[`spec.crossClusterReplication`](../api_reference/neo4jenterprisecluster.md#crossclusterreplicationspec)
for the API reference.

Q1's answer bounds the design space. Network replication requires all of:

1. a per-server exposure API emitting one Service per pod on :6000 (B2) — *shipped as one proxy Deployment/Service fanning out to N ordinals, not N Services (§6.6 item 1);*
2. an externally-routable `server.cluster.advertised_address`, which means carving it out of `operatorRuntimeManagedSettings` and managing it as a first-class field rather than a runtime-appended constant (B1) — *shipped simpler than this framing suggests: the setting stays in `operatorRuntimeManagedSettings` (still forbidden in `spec.config`), and only the operator's own computed value became conditional (§6.6 item 2);*
3. external per-server SANs on the cert-manager `Certificate` (B3) — *shipped as one shared SAN, not per-server (§6.6 item 3): only the port differs per ordinal, which hostname verification ignores;*
4. a supported path for the remote CA into the cluster SSL policy's trust directory — most likely projecting a new `spec.tls.additionalClusterTrustCAs` into `/ssl/trusted/` alongside the operator's own `ca.crt`, since the `extraVolumes` route is structurally unavailable (B3) — *shipped exactly as speculated (§6.6 item 4);*
5. a NetworkPolicy rule admitting remote CIDRs on :6000 (B4) — *shipped narrower: the proxy pod's own label selector, not a remote CIDR — only the proxy is ever internet-facing, never the Neo4j pods themselves (§6.6 item 5).*

Item 2 is the hard one, and Q1's full answer makes it **mandatory rather than an optimisation**. Because the upstream hands the downstream its advertised cluster addresses to connect to, those addresses must be externally routable — and the same value carries all intra-cluster peer traffic. Options (b) and (c) below are therefore not competing cost profiles for achieving the same thing; **one of them (or (a)) is a precondition for network replication working at all on Kubernetes.**

**Shared prerequisite for (b) and (c): per-pod addressing.** The StatefulSet has one pod template, so per-pod values must be derived at runtime. The operator already does exactly this — `HOSTNAME_FQDN` is built from `${HOSTNAME}` in the startup script (`internal/resources/cluster.go:2249`) — so extending it to index a per-ordinal address list is a small change. Provisioning the endpoints is the hard part, not templating them.

### 6.1 Option (a) — a distinct cross-cluster advertised address — *does not appear to exist*

The hope was a setting separate from `server.cluster.advertised_address` for cross-cluster identity, letting the operator keep advertising internal FQDNs to peers while advertising external addresses to replicas. **Three documentation pages say there is no such setting:**

- *Cluster server discovery* names exactly one advertised-address setting: `server.cluster.advertised_address`.
- *Clustering settings* enumerates the complete address surface — `server.cluster.advertised_address`, `server.cluster.raft.advertised_address`, `server.bolt.advertised_address`, plus the corresponding `listen_address` settings and `server.bolt.additional_listen_addresses`. **Nothing containing "replica", "remote" or "cross".**
- *Connectors* covers only the client-facing bolt/http/https pair.

Worse, the settings reference documents `server.cluster.advertised_address` as *"Advertised hostname/IP address and port for the **transaction shipping server**"* — transaction shipping is precisely the channel CCDR catchup rides. So it is not merely that the two consumers happen to share a setting; **cross-cluster replication and intra-cluster transaction shipping are the same subsystem, addressed by the same value.** There is no seam to split.

One near-miss worth noting: `server.bolt.additional_listen_addresses` shows Neo4j has considered multi-address binding, but it is a *listen* setting with no advertised counterpart, and there is no cluster-port equivalent. Binding to many addresses does not help when only one can be advertised.

**Treat Q1a as confirming a negative** — ask whether anything undocumented or planned exists, but design on the assumption it does not. That leaves (b), (c) and (d).

### 6.1a Note: the discovery resolver is not the fix

Neo4j supports four discovery resolver types — `LIST` (what this operator uses, with static pod FQDNs), `DNS`, `SRV`, and `K8S` (`dbms.cluster.discovery.resolver_type`, with `dbms.kubernetes.label_selector`). Seeing a Kubernetes-native resolver, the natural guess is that switching to it resolves CCDR addressing. **It does not.**

Discovery governs how a server finds *its own cluster's* peers at startup. The advertised address governs what identity a server hands out — including to a downstream replica. They are independent: a cluster using `K8S` discovery still advertises whatever `server.cluster.advertised_address` says, and a downstream replica in another Kubernetes cluster still receives an unroutable name. Changing resolver type is orthogonal to Phase 2 and should not be pursued as a fix for it.

### 6.2 Option (b) — whole-cluster external addressing

Every server advertises an externally routable address, and *all* traffic — including intra-cluster RAFT and transaction replication — routes through it.

**This is the documented purpose of `advertised_address`, not a hack.** The connectors page gives exactly this pattern as the motivating case — advertised addresses exist for when you are *"routing traffic via a proxy, or if port mappings are in use"*, with the worked example that *"if port 7687 on the Neo4j server is mapped from port 9000 on the external network, specify the `advertised_address` for the Bolt connector."* Advertising an externally-reachable address while listening internally is the intended use. The mechanism is sound; the objection to (b) is cost, not legitimacy.

**How it would work.** One stable external endpoint per pod. Three ways to get them, in descending order of practicality:

- **One load balancer, one port per server** (LB:16000 → server-0:6000, LB:16001 → server-1:6000, …). A single LB instead of N. The connectors page confirms `advertised_address` *"consists of two parts; an address … and a port number"*, so a per-server port that differs from the listen port is explicitly legal. This is the cost-sane variant and the one to design for.
- **One LoadBalancer Service per pod**, selected via `statefulset.kubernetes.io/pod-name`. Cleanest addressing, N cloud LBs, N× the standing cost.
- **NodePort.** Cheapest, but node IPs are not stable and nodes must be externally reachable. Not recommended.

**Security note.** The connectors page warns that *"Changing `server.default_listen_address` may expose cluster ports to the network."* Any implementation of (b) must widen listen addresses surgically per-connector rather than via the default, and pair the change with the NetworkPolicy work in item 5 — otherwise exposing :6000 for CCDR quietly exposes RAFT and routing too.

**A chicken-and-egg to design around.** LoadBalancer IPs are not known until the Service is provisioned, but the config referencing them is rendered before pods start. That forces a two-phase reconcile — create Services, wait for `status.loadBalancer.ingress`, re-render, restart pods — *unless* addressing is by DNS name rather than IP, in which case names are known up front. The operator already integrates with external-dns via `spec.service.dnsName`, so the DNS-name variant avoids the extra phase entirely and should be the supported path.

**What it costs, and why it is not free for non-CCDR users:**

- **Hairpin routing.** Every RAFT heartbeat and every transaction ships pod → LB → pod. RAFT is latency-sensitive; added latency shows up as slower commits and more marginal leader elections.
- **Data-processing charges.** Intra-cluster traffic that was free inside the VPC becomes billed LB throughput. For a write-heavy cluster this is the dominant new cost, and it scales with write volume rather than being a flat fee.
- **Consensus now depends on the load balancer.** Today an LB blip affects clients only. Under (b) it affects RAFT. This couples cluster availability to a component that was previously outside the availability envelope — the most serious objection to this option.
- **Hairpin NAT hazards.** A pod connecting to an LB address that routes back to itself is a well-known failure mode; behaviour varies by CNI and kube-proxy mode, and where kube-proxy short-circuits the LB IP to the backend the traffic path silently differs from the external one, which makes testing misleading.
- **NetworkPolicy rule 2 breaks.** Peer traffic no longer arrives from a pod matching `neo4j.com/cluster` (B4), so the intra-cluster rule must be rewritten in CIDR terms — losing the identity-based restriction that makes it worth having.

**The obvious cost-reduction lever mostly does not work.** `advertised_address` is per-server, so in principle only a subset need advertise externally. But the upstream returns addresses for whichever servers currently host the database, and hosting moves with topology changes and failover. Any server that could ever host the upstream database must therefore be externally routable, or replication breaks at an unpredictable later moment rather than at setup. Exposing a subset is only safe if the upstream database's placement is pinned, which is not a constraint worth taking on for this.

**Verdict.** Self-contained — no dependency the operator cannot provision — but it degrades the common case to serve a rare one, and must therefore be strictly opt-in. Appropriate for small or read-heavy clusters; poor for write-heavy ones.

### 6.3 Option (c) — split-horizon DNS

One name, e.g. `prod-server-0.ccdr.example.com`, resolving to the pod IP inside the Kubernetes cluster and to the external endpoint outside it. The advertised address becomes that globally-meaningful name instead of a `.svc.cluster.local` FQDN.

**How it would work.**

- *Outside:* public DNS (Route53 or similar) maps the name to the LB endpoint. external-dns automates this, and the operator already understands external-dns.
- *Inside:* CoreDNS must answer the same name with the pod IP — realistically a `rewrite` rule in the Corefile mapping `<cluster>-server-N.ccdr.example.com` to `<cluster>-server-N.<cluster>-headless.<ns>.svc.cluster.local`.

**Why it is better on every runtime axis.** Intra-cluster traffic resolves to pod IPs and never leaves the cluster. So: no hairpin, no added RAFT latency, no LB data-processing charges on internal traffic, **and no coupling of consensus to the load balancer** — the objection that most damns (b). NetworkPolicy rule 2 also keeps working unchanged, because peer traffic still arrives pod-to-pod; only the remote-CIDR rule needs adding. Certificate SANs are simple too: one name per server, valid from both sides.

**It also satisfies Q1's redirect requirement by construction**, which is the strongest argument for it. The address the upstream hands back is a single globally-meaningful name that is correct in *both* contexts — it resolves to a pod IP for an upstream peer and to the external endpoint for the downstream replica. Options (a) and (b) each make the returned address correct for cross-cluster use by changing what is advertised; (c) makes one advertised value simultaneously correct for both audiences, which is the only variant where the two consumers of `advertised_address` stop being in conflict at all.

**A cost the earlier revision of this section got backwards.** (c) cannot use (b)'s one-LB-many-ports trick. There is a single advertised value carrying a single port, and it must be correct from *both* sides — so if a pod listens on 6000, the external endpoint must also serve 6000. That means **N distinct external IPs or hostnames, each on port 6000** — i.e. roughly N load balancers, against (b)'s one.

So the cost comparison is not "(c) is cheaper", it is a swap of *fixed* cost for *variable* cost:

| | (b) whole-cluster external | (c) split-horizon DNS |
|---|---|---|
| Standing LB cost | one LB, N ports | ~N LBs (one IP per server on :6000) |
| Per-GB charges on intra-cluster traffic | yes — scales with write volume | none, traffic never leaves |
| RAFT latency | added hop | unchanged |
| Consensus depends on LB | yes | no |
| NetworkPolicy rule 2 | breaks, degrades to CIDR | unchanged |

(c) still wins on every runtime axis, and the variable cost is the one that grows with success. But for a small, low-write cluster (b) may genuinely be cheaper, and the guidance should say so rather than presenting (c) as uniformly better.

*(Working around this by giving each pod a distinct `server.cluster.listen_address` port to match its advertised port is possible but breaks the uniform StatefulSet and headless-Service port model. Not recommended.)*

**Why it is not obviously the right choice.** The Corefile lives in `kube-system` and is shared cluster-wide infrastructure. **The operator must not mutate it** — a bad edit breaks DNS for every workload on the cluster, not just Neo4j. So the inside half of split-horizon is a precondition the user supplies, and the operator's contract degrades to "you make the name resolve correctly on both sides; I will advertise it." When a user gets it wrong, the symptom is a cluster that will not form, at startup, with an opaque DNS error.

**That failure mode is fixable, and the fix is what makes (c) viable.** The operator can *verify* what it must not *provision*: a preflight that resolves the advertised name from inside the pod and compares the result to the pod's own IP, failing loudly with an actionable message before the cluster attempts to form. That converts the worst property of (c) — a mysterious startup failure caused by infrastructure the operator does not own — into a clear, diagnosable precondition error. Any implementation of (c) should treat this preflight as part of the feature, not a nicety.

**Verdict.** Better runtime properties than (b) on every axis; worse out-of-the-box experience, and only suitable for users with controllable DNS automation.

### 6.4 Option (d) — decline network replication on Kubernetes

Worth stating explicitly because it may be the correct *product* answer rather than a concession. Phase 1 delivers backup-based CCDR with **zero** cross-cluster networking, and the RPO difference is roughly one minute (`db.cluster.backup.pull_interval`) versus near-zero. For disaster recovery — the motivating use case behind the field demand — a one-minute RPO is very often acceptable.

The position would be: *"CCDR on Kubernetes is supported via backup replication. Network replication is possible but requires cluster-wide external addressing (§6.2) or split-horizon DNS (§6.3), and is not a supported configuration."* That is honest, ships now, and avoids committing engineering to a cluster-wide addressing change on behalf of a minority of users.

**Q1's full answer strengthens this option considerably.** Before it, network replication looked like it might be reachable by exposing services and listing the right addresses — an additive feature. It is not: the upstream hands back its advertised addresses, so supporting network replication means changing how every cluster addresses itself. That is a much larger commitment than the feature's likely demand justifies, and it is worth deciding deliberately rather than drifting into.

If network replication is later demanded by a specific customer with a hard sub-minute RPO requirement, (a)/(c) can be revisited with that requirement as justification.

### 6.5 Prior art: the official Neo4j Helm charts already do option (b)

Checked against `neo4j/helm-charts` at chart `2026.6.0` / appVersion `2026.06.0`.

**By default the charts have exactly our problem.** `neo4j/templates/neo4j-config.yaml` sets:

```
server.cluster.advertised_address: "$(bash -c 'echo ${SERVICE_NEO4J_INTERNALS}')"
```

where `SERVICE_NEO4J_INTERNALS` is `<release>-internals.<namespace>.svc.<clusterDomain>` — an in-cluster FQDN, unroutable from anywhere else, same as ours.

**But the charts ship a supported escape hatch, and we do not.** `services.neo4j.multiCluster: true` makes `neo4j-loadbalancer.yaml` open the internal cluster ports on the LoadBalancer — `tcp-tx` (6000), `tcp-raft` (7000), `tcp-discovery` (5000), `tcp-boltrouting` (7688) — and sets `publishNotReadyAddresses: true`. The `examples/multi-cluster/` values then simply override the advertised addresses to the load balancer's IP:

```yaml
services:
  neo4j:
    annotations:
      service.beta.kubernetes.io/azure-load-balancer-internal: "true"
    spec: {loadBalancerIP: 10.30.1.101}
    multiCluster: true
config:
  server.cluster.advertised_address: "10.30.1.101:6000"
  server.cluster.raft.advertised_address: "10.30.1.101:7000"
```

That is option (b), shipped and documented.

**Why it is easy there and hard here — an architectural difference, not a missing feature.** The Helm charts deploy **one Neo4j server per Helm release**: each cluster member is its own release with its own Service and its own load balancer IP. So `server.cluster.advertised_address` is a *scalar per member* and a user can override it in plain values. This operator deploys **one StatefulSet with N pods under a single CR**, so there is exactly one rendered config for all pods and no scalar override is possible. Getting the same result requires the per-ordinal address templating described in §6.2 — operator code, not a values field.

**Consequences worth taking seriously:**

- **The advice that "exposing the cluster port via Helm should just work" is essentially right — for the Helm charts.** `multiCluster: true` plus advertised-address overrides is a real, existing answer for the network path. Any field-facing CCDR-on-Kubernetes guidance should say so.
- **This operator currently has a capability gap against the Helm charts.** A user migrating from charts to the operator would *lose* the ability to run cross-K8s-cluster networking. That reframes Phase 2 from "a new feature for a minority" to "parity with the sibling deployment method", which is a stronger claim on roadmap than §6.4 assumed.
- **The proven design is (b), not (c).** The charts' example mitigates the hairpin objection by using an **internal** load balancer with a private IP routable across peered VNets, not a public one — so intra-cluster traffic hairpins within the private network. That is a meaningful and copyable mitigation of §6.2's main cost.

**Three caveats before treating the chart example as a template:**

1. **The example is 4.4-era and stale.** It sets `dbms.cluster.discovery.resolver_type: LIST` with `dbms.cluster.discovery.endpoints: "...:5000"` — V1 discovery on port 5000 — and its README links to the 4.4 manual. The chart's own current default is the `K8S` resolver on the `tcp-tx` port. The *capability* is current; the *example values* need modernising for 5.26/CalVer before anyone follows them.
2. **`multiCluster` mode forces abandoning the chart's default discovery.** `dbms.cluster.discovery.resolver_type: K8S` with `dbms.kubernetes.label_selector` queries the *local* Kubernetes API and cannot see members in another cluster, which is why the example switches to LIST. Anyone enabling multi-cluster gives up K8S discovery.
3. **It opens more than CCDR needs.** The flag exposes 5000/6000/7000/7688 together. A downstream replica needs only the cluster/catchup port. An operator-side equivalent should expose 6000 alone, and pair it with the NetworkPolicy work (B4) rather than inheriting the chart's broader surface.

### 6.6 What shipped: Option 3 — a single self-hosted proxy, port-based routing

The user-facing decision, after walking (b), (c), and a proxy+split-horizon-DNS
hybrid (below) in detail: **all of them were too complicated for the ease-of-use
bar this feature needs to clear.** (c) requires the user to own and correctly
configure split-horizon DNS, with a failure mode that surfaces as an opaque
cluster-formation error. The hybrid depends on an unverifiable assumption about
Neo4j's closed-source cluster-transport TLS behaviour (does it honour SNI on
the tx-shipping port at all?) that could not be confirmed from documentation
alone, and would have meant shipping a design whose correctness rests on an
unverified guess. (b) itself is sound but has no port-consolidation story: N
servers means N load-balanced ports or N load balancers, either way pushed onto
the user to provision and wire up correctly.

**What actually shipped is a simplification of (b): the operator, not the user
or a cloud load balancer, does the per-server port fan-out.** One
operator-managed TCP proxy (HAProxy, `mode tcp`, pure L4 — it never terminates
or inspects TLS) sits in front of the cluster. It listens on one port per
server ordinal (`16000+i`) and forwards each to that ordinal's pod on the
cluster port (6000). A single `LoadBalancer` Service fronts the proxy. This
gets (b)'s core mechanism — advertise an external endpoint, let the LB/proxy
absorb the port remapping — without requiring the user to provision N anything,
and without any DNS precondition at all. The tradeoff carried over from (b) is
the same one, made explicit and opt-in: enabling this routes intra-cluster
secondary catch-up traffic (which shares port 6000 with CCDR catchup) through
the load balancer too, adding latency and cost proportional to write volume.

**One toggle, one API round-trip.** On the upstream `Neo4jEnterpriseCluster`:

```yaml
spec:
  crossClusterReplication:
    enabled: true
  tls:
    additionalClusterTrustCAs:
      - name: dr-cluster-ca   # the downstream cluster's CA, copied here
```

Once the proxy's `LoadBalancer` Service is assigned a hostname/IP,
`status.crossClusterReplication.addresses` publishes the ready-to-paste
`<lb-hostname>:<port>` list — the same "publish the exact string the user
pastes elsewhere" ergonomics `Neo4jBackup.status.replicationPullURI` already
uses for Phase 1. On the downstream `Neo4jReplicaDatabase`:

```yaml
spec:
  source:
    mode: network
    addresses: ["<one address from the upstream's status.crossClusterReplication.addresses>"]
```

One address is sufficient — see Q1: the upstream hands back the addresses the
downstream actually uses, so the list only needs to get the first connection
made.

**Implementation, mapped onto §6's item list:**

1. *Per-server exposure* — one proxy Deployment/Service/ConfigMap
   (`internal/resources/ccdr_proxy.go`, `internal/controller/ccdr_proxy.go`),
   not N per-pod Services. A plain Kubernetes Service cannot do per-ordinal
   port-to-specific-pod mapping (every StatefulSet replica shares the same
   `containerPort`), which is why a self-hosted L4 proxy — not N Services — is
   the mechanism.
2. *Externally-routable advertised address* — `server.cluster.advertised_address`
   is overridden per-server, in `buildStartupScriptForEnterprise`
   (`internal/resources/cluster.go`), to `<proxy-lb-hostname>:$((16000 +
   SERVER_INDEX))` once the proxy's load balancer hostname is known.
   `server.cluster.raft.advertised_address` (7000) and
   `server.routing.advertised_address` (7688) are **never** touched — RAFT
   leader election never depends on the load balancer, narrowing the blast
   radius from §6.2's "all cluster traffic hairpins" framing to "tx-shipping
   traffic only, opt-in."
3. *External per-server SANs* — one SAN on the cert-manager `Certificate`
   (`BuildCertificateForEnterprise`) covering the proxy's load balancer
   hostname. One SAN suffices for every ordinal: only the port differs, and
   hostname verification ignores port.
4. *Remote CA trust* — `spec.tls.additionalClusterTrustCAs`, projecting each
   peer cluster's CA directly into `/ssl/trusted/` (confirmed
   directory-scanned, not a single fixed filename) alongside the cluster's own
   CA. Additive: the existing single-Secret `CertsVolume` mount is untouched.
5. *NetworkPolicy* — a 4th, additive ingress rule admitting the proxy pod
   (matched by its `app.kubernetes.io/component: ccdr-proxy` label, not a
   remote CIDR — the Neo4j pods are never directly internet-facing, only the
   proxy pod is) on port 6000 only.

**The proxy+split-horizon-DNS hybrid — considered, deferred.** Before
converging on Option 3, a hybrid was explored: keep the split-horizon DNS
approach from §6.3 but front it with a proxy that uses TLS SNI to route a
single external port to the correct backend pod, avoiding (c)'s N-load-balancer
cost. This depends entirely on whether Neo4j's cluster-transport TLS (the
`dbms.ssl.policy.cluster.*` handshake on port 6000) sends or honours SNI at
all — undocumented, and Neo4j's cluster transport is closed-source, so it
cannot be verified by reading code either. **Deferred, not ruled out**: if a
future customer need justifies the added complexity, a short spike against a
real cluster (capture the ClientHello on port 6000, check for an SNI
extension) would resolve the open question before any design work resumes.

---

## 7. Testing

- **Unit — landed:** Cypher construction for `CREATE REPLICA DATABASE` in both
  modes (`internal/neo4j/replicas_test.go`); version gating
  (`SupportsCCDRReplica` / `MeetsCCDRUpstreamFloor`, including a drift guard
  pinning the floor constants to the gate logic); validator table tests for
  both `mode: backup` and `mode: network` (`internal/validation/replica_validator_test.go`);
  the §5.4 terminal guard, including the out-of-band-promotion branch; the
  proxy builders and the conditional advertised-address override, pinned
  against the existing byte-identical-default-path assertions
  (`internal/resources/ccdr_proxy_test.go`, `cluster_startup_test.go`,
  `cluster_test.go`, `networkpolicy_test.go`).
- **Integration:** gated behind a dispatch input the way property sharding is (`isPropertyShardingCompatible()`), because the CI anchor CalVer is below 2026.08 (B7). Label every new spec `Label("extended")`.
- **Two-cluster testing is the hard part.** Backup-based replication is testable on a *single* Kind cluster with two Neo4j deployments and a shared MinIO bucket — but the project invariant is **one Enterprise deployment at a time** (concurrent JVMs wedge Bolt on a laptop). So this lands in the manual pre-release journey (`docs/developer_guide/release_verification.md`) rather than the automated suite, at least initially — including network mode, which needs a second, real Kubernetes cluster and so is manual-only for the foreseeable future, not merely "initially."

---

## 8. Delivery order

1. `DatabaseInfo` gains `Type` / `Access` / `Writer` (B6) — unblocks everything and is independently useful for diagnostics.
2. ~~`Version.SupportsCCDR()`~~ → **done**: `Version.SupportsCCDRReplica()` + `MeetsCCDRUpstreamFloor()` + floor constants (B7).
3. Fix the B3 docs-vs-code inconsistency in `tls_configuration.md` — currently the docs point users at a path that cannot work.
4. `Neo4jReplicaDatabase` + validator + backup-mode reconcile.
5. `Neo4jBackup` `mode: replication-source` + rules R1–R5 (§4.4). Independent of 4, so it can land in parallel.
6. `Neo4jReplicaPromotion` + the §5 terminal-guard behaviour.
7. User guide: the end-to-end backup-based runbook, the failover procedure including the alias step, the bucket-lifecycle warning from §4.4, and the B8 warning about restoring an upstream.
8. ~~Phase 2, pending §9 Q1a~~ → **done**: Option 3 (§6.6) — `spec.crossClusterReplication` on `Neo4jEnterpriseCluster`, `mode: network` on `Neo4jReplicaDatabase`, and the user guide runbook in `docs/user_guide/guides/cross_cluster_replication.md`.

---

## 9. Open questions

**Q1 — ANSWERED, in two parts.** The downstream dials only the addresses in `replicaConfig.addresses` — but there *is* server-side routing and discovery: one listed address is sufficient, and the upstream **returns the addresses the downstream then reaches out to**. Those are the upstream's advertised cluster addresses. A listed address need not host the upstream database.

Consequence: **B1 is confirmed in its strongest form.** `server.cluster.advertised_address` must be externally routable, and no amount of care on the listing side substitutes, because the downstream uses the addresses it is handed rather than the ones it was given. See B1 and §6. Phase 1 is unaffected.

**Q1a — ANSWERED from documentation; moot for the shipped design.** No distinct cross-cluster addressing setting exists (§6.1) — `server.cluster.advertised_address` is documented as the address of the *transaction shipping server*, the same subsystem CCDR catchup uses, so there is no seam between the two consumers to exploit. This is exactly why Option 3 (§6.6) does not try to find or use such a seam: it accepts that the advertised address is shared, narrows what is exposed to tx-shipping traffic only (RAFT and routing untouched), and makes the resulting tradeoff an explicit opt-in rather than a hidden default. Nothing here blocks the shipped design; it explains why the design looks the way it does.

**Q2. Upstream version floor — still open, but the code is ready for the answer.** The manual states 2025.01, and mixed-version support remains under active test upstream. Implemented as `MeetsCCDRUpstreamFloor()` + the `MinCCDRUpstreamVersion` constant, so moving the floor is a one-line change that corrects every caller at once. Callers must **warn, not reject**: the upstream is in another Kubernetes cluster the operator cannot inspect, so its version is always a user-supplied claim rather than an observation, and refusing to reconcile on an unverifiable claim is worse than surfacing a warning. Note the predicate is currently equivalent to `IsCalver` by construction (CalVer minors start at 01); it exists as its own function because this floor is the most likely thing in that file to move.

**Q3 — ANSWERED: yes.** Aliases can be created against a replica database. The alias is therefore pre-staged at replica-creation time and needs no action during the failover window; it silently gains write capability at promotion. See §5.7 — this also strengthens the case for pulling `Neo4jDatabaseAlias` (Q6) into Phase 1.

**Q4. Does `dbms.promoteReplicaDatabase` error or no-op against an already-promoted database?** The §5.4 ordering makes the operator correct either way, so this is informational — but it affects the quality of the error surfaced to a user who re-applies a promotion CR.

**Q5 — ANSWERED: yes.** `Neo4jBackup` gains `mode: replication-source`. Designed in §4.4. Note the honest limit recorded there: for cloud storage the operator delegates retention to bucket lifecycle rules it cannot see, so the mode reduces the footgun surface but cannot close it.

**Q6. Alias CRD — reopened as a Phase 1 scope question.** Filed as a follow-on, but Q3's answer moves the alias from a break-glass failover action into the *recommended steady-state configuration* for every replica (§5.7). A setup step that every user performs and the operator cannot express declaratively is a gap in the GitOps story, not a nicety. `Neo4jDatabaseAlias` would also cover ordinary non-CCDR aliasing. **Decide before the user guide is written**, since it determines whether the runbook says "apply this CR" or "exec into a pod and run Cypher".
