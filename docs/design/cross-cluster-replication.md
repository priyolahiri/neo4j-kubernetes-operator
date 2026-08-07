# Design: Cross-Cluster Database Replication (CCDR) on Kubernetes

> **Status:** Draft for review. Not an official product or version commitment.
> **Source:** Neo4j Operations Manual, *Replicating databases across clusters* (pre-publication preview, 2026.07 docs branch). The page requires a **downstream cluster on 2026.08 or later**; the upstream minimum is stated as 2025.01 but see §9 — mixed-version support is under active test upstream and should not be hardened into operator validation yet.
> **Scope of this document:** what CCDR needs from a Kubernetes deployment, what this operator already provides, what actually blocks it, and a two-phase delivery shape. Phase 1 (backup-based) is designed to a level a contributor can implement from. Phase 2 (network-based) is deliberately left at the "what must be answered first" stage.
> **Headline:** backup-based CCDR is largely deliverable on top of machinery the operator already has. Network-based CCDR is blocked on an addressing problem that no amount of Service configuration resolves, and the fix is a new operator capability.

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

So even with :6000 exposed externally, upstream members advertise names that do not resolve outside their own Kubernetes cluster. This is the classic advertised-listener problem. **Whether it actually bites depends on an unanswered question — see §9, Q1.** If the downstream only ever dials the addresses it was given, B1 evaporates and Phase 2 shrinks dramatically.

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

### B7 — Version gating is absent

Downstream must be ≥ 2026.08. `internal/neo4j/version.go` has the exact template in `SupportsAuthRules()` (gates 2026.03). Note the CI anchor CalVer is currently 2026.06, so CCDR specs will be dispatch-gated and largely local-only until the anchor moves — the same treatment property sharding gets via `isPropertyShardingCompatible()`.

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

1. Resolve `clusterRef`; require downstream version ≥ 2026.08 via a new `Version.SupportsCCDR()`.
2. **Observe before acting** — `SHOW DATABASE <name> YIELD name, type, access, writer`. This drives every branch below and is the safety spine of §5.
3. Absent → `CREATE REPLICA DATABASE ... OPTIONS {seedURI, replicaConfig:{remote, pullURI}}`.
4. Present and `type = replica` → reconcile topology drift only. **Do not** attempt to change `replicaConfig`; treat `source` as immutable via a CEL `self == oldSelf` transition rule (the project uses apiserver-side CEL for immutability, consistent with invariant 1's no-webhook rule).
5. Present and `type != replica` → **promoted, out of band or otherwise.** Go terminal. Never mutate. See §5.4.

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

The clean fix is a **database alias** on the promoted database:

```cypher
CREATE ALIAS `foo` FOR DATABASE `foo-replica`;
```

so applications keep using the original name after failover. Two consequences for this design:

1. Document the alias step as part of the failover runbook, not as an afterthought.
2. The operator has no alias CRD today. A `Neo4jDatabaseAlias` CRD is a natural follow-on and is listed in §9. Until then the alias is a manual `cypher-shell` step, which is acceptable for a break-glass DR procedure but not for a routine one.

*(To verify before this is documented as guidance: whether an alias may be created against a database that is still a replica, or only after promotion. The failover-time cost differs depending on the answer.)*

### 5.8 Promoting a lagging replica

Promotion freezes the current lag as permanent data loss — there is no re-attaching to catch up afterwards. The promotion CR should therefore record the observed lag at promote time (`status.observedLagTxIds`) so the RPO actually taken is auditable after an incident. The manual's suggested signal is the delta in `<prefix>.database.<db>.transaction.last_committed_tx_id` between upstream and replica; where the upstream is unreachable (the usual reason for a failover) record the replica's last applied id alone.

This is deliberately *recorded*, not *enforced*. Blocking a failover on a lag threshold is the wrong default — during a real outage the operator does not get to second-guess the human.

---

## 6. Phase 2 — network replication

Not designed here, because it is gated on §9 Q1. If the answer is "the downstream follows upstream advertised addresses", Phase 2 requires all of:

1. a per-server exposure API emitting one Service per pod on :6000 (B2);
2. an `advertisedAddress` override, which means carving `server.cluster.advertised_address` out of `operatorRuntimeManagedSettings` and managing it as a first-class field rather than a runtime-appended constant (B1);
3. external per-server SANs on the cert-manager `Certificate` (B3);
4. a supported path for the remote CA into the cluster SSL policy's trust directory — most likely projecting `spec.tls.additionalClusterTrustCAs` into `/ssl/trusted/` alongside the operator's own `ca.crt`, since the `extraVolumes` route is structurally unavailable (B3);
5. a NetworkPolicy rule admitting remote CIDRs on :6000 (B4).

If the answer is "only the listed addresses are ever dialled", items 1, 3, 4 and 5 remain but item 2 — by far the largest — falls away.

---

## 7. Testing

- **Unit:** Cypher construction for `CREATE REPLICA DATABASE` in both modes; `SupportsCCDR()` version gating; validator table tests; the §5.4 terminal guard, including the out-of-band-promotion branch.
- **Integration:** gated behind a dispatch input the way property sharding is (`isPropertyShardingCompatible()`), because the CI anchor CalVer is below 2026.08 (B7). Label every new spec `Label("extended")`.
- **Two-cluster testing is the hard part.** Backup-based replication is testable on a *single* Kind cluster with two Neo4j deployments and a shared MinIO bucket — but the project invariant is **one Enterprise deployment at a time** (concurrent JVMs wedge Bolt on a laptop). So this lands in the manual pre-release journey (`docs/developer_guide/release_verification.md`) rather than the automated suite, at least initially. Add the scenario there in the same PR that adds the capability.

---

## 8. Delivery order

1. `DatabaseInfo` gains `Type` / `Access` / `Writer` (B6) — unblocks everything and is independently useful for diagnostics.
2. `Version.SupportsCCDR()` (B7).
3. Fix the B3 docs-vs-code inconsistency in `tls_configuration.md` — currently the docs point users at a path that cannot work.
4. `Neo4jReplicaDatabase` + validator + backup-mode reconcile.
5. `Neo4jReplicaPromotion` + the §5 terminal-guard behaviour.
6. User guide: the end-to-end backup-based runbook, the failover procedure including the alias step, and the B8 warning about restoring an upstream.
7. Phase 2, pending §9 Q1.

---

## 9. Open questions

**Q1 (blocking Phase 2). Does the downstream only ever dial the addresses listed in `replicaConfig.addresses`, or does it follow the upstream members' advertised addresses for catchup and redirect?** The manual's note that *"there is no requirement that every remote server hosts the upstream database"* hints at redirection. This single answer determines whether network CCDR needs a new advertised-address capability (B1) or merely per-pod Services. **Needs an answer from the clustering team before any Phase 2 work starts.**

**Q2. Upstream version floor.** The manual states 2025.01, but mixed-version support is under active test upstream and the direction has not been finalised. Operator validation should therefore warn rather than reject on upstream version, and the floor should be a constant that is cheap to move.

**Q3. Alias against a replica.** Can `CREATE ALIAS` target a database whose `type` is still `replica`, or only post-promotion? Determines whether the alias is set up ahead of time or during the failover window (§5.7).

**Q4. Does `dbms.promoteReplicaDatabase` error or no-op against an already-promoted database?** The §5.4 ordering makes the operator correct either way, so this is informational — but it affects the quality of the error surfaced to a user who re-applies a promotion CR.

**Q5. Should `Neo4jBackup` learn a CCDR-aware mode?** The upstream chain for CCDR has requirements a general-purpose backup does not — an unbroken differential chain, and a stable directory. A `spec.mode: replication-source` that refuses configurations which would break the chain (e.g. a competing FULL CR writing to the same path, or a retention policy that prunes a diff's parent) would turn B8-adjacent footguns into validation errors. Deferred, but worth deciding before the user guide sets expectations.

**Q6. Alias CRD.** `Neo4jDatabaseAlias` as a follow-on (§5.7), which would also cover non-CCDR use cases.
