# Release Verification

The **pre-release verification journey**: a manual + LLM-driven walk of the real
user scenarios on a clean cluster, run from `main` before cutting any release.
It is the canonical source of truth for **what we verify, on which deployment,
at what size, and why** — keep it current as the product grows.

!!! info "Why this exists alongside the automated suites"
    Unit and integration tests ([Testing](testing.md)) validate the **code
    against itself**. This journey validates the **published instructions and
    end-to-end behaviour against a clean machine** — the only check that catches
    a doc that lies, a dead image tag, a tutorial ordering bug, or a
    CR-name/logical-name mismatch. Real passes have caught all of these
    (most recently the sharded `target.name` doc bug, a `#270` follow-up).

The [`verify-journey` skill](https://github.com/priyolahiri/neo4j-kubernetes-operator/blob/main/.claude/skills/verify-journey/SKILL.md)
executes this document. **LLM agents and developers should follow the matrix
below verbatim**; the skill holds only the mechanics (build, kind-load retry,
teardown, reporting) and defers to this page for *what* to test.

## Ground rules

- **KIND only. Enterprise images only** (`neo4j:*-enterprise`) — the project
  [invariants](../knowledge/invariants.md).
- **Follow the published docs literally** for Phases 1–2. A wrong/out-of-order
  step *is* a finding — record the exact doc location, don't silently fix it.
- **One Enterprise deployment in the cluster at a time** (the *anti-wedge*
  rule). Running standalone + cluster + sharding JVMs concurrently on a laptop
  VM wedges Bolt (the HTTP probe reports `Ready` but ports 7687/6362 time out).
  Phases run **sequentially with full teardown between them**; the Kind cluster,
  operator, and cert-manager stay up across phases.
- **Restore must be walked on BOTH standalone and cluster** — the mechanism
  differs (see the routing table) and one never covers the other.

## Capability routing — why each lands where it does

Each capability is verified on the **minimum deployment that actually exercises
its code path**. Standalone is the cheap workhorse; a capability moves to the
cluster only when it *needs* clustering.

| Capability | Where | Why |
|---|---|---|
| Database lifecycle (create/show/drop) | Standalone | Same `CREATE DATABASE` Cypher everywhere |
| Database **topology** (`n PRIMARIES m SECONDARIES`) | Cluster | Meaningless on standalone (validator only warns) |
| Users / Roles / RoleBindings | Standalone | Auth model is deployment-independent |
| Plugins (APOC, …) | Standalone (ConfigMap path) | Cluster uses the **env-var** path; ConfigMap path is the cheaper of the two to smoke |
| **Backup → restore** | **Both** | Standalone restores via the `neo4j-admin` path (supports PITR `--restore-until`); cluster restores via the in-place **Cypher** path (PITR is *rejected* on clusters). Separate code paths. |
| Property sharding | Cluster (CalVer) | Cluster-only by nature; needs a CalVer image and is memory-heavy |
| **Aura orchestration** (`Aura*` CRDs) | **Phase 4 — no Kind deployment**; needs Aura API credentials | Talks to a *cloud* API, not a local DBMS, so no phase above exercises it. Read-only checks need only a client ID/secret; write checks mutate real cloud resources — see the phase for what is and is not safe |
| **Aura Fleet Management** | Standalone (+ Aura creds for `provision`) | The plugin/registration path is deployment-independent, so the cheap phase covers it; operator-driven `provision` additionally needs Aura API credentials |
| **Cross-cluster replication** (`Neo4jReplicaDatabase`, `Neo4jReplicaPromotion`) | **Phase 5 — two deployments, run SEQUENTIALLY**; needs a `2026.08+` image | Needs an upstream *and* a downstream, which looks like it breaks the one-deployment-at-a-time rule — but backup-based replication couples the two only through a bucket, so the upstream can be torn down before the downstream comes up. See the phase |
| **Database aliases** (`Neo4jDatabaseAlias`) | Standalone | Alias DDL is deployment-independent; the CCDR failover behaviour is covered in Phase 5 |
| **Operator→Neo4j TLS verification** (CA path + `tls.crt` pinning) | Standalone | Standalone never reads `ca.crt` server-side (no cluster SSL policy), so removing that key from the TLS Secret isolates the *operator's* client verification with nothing else changing. On a cluster the same edit also breaks intra-cluster mTLS and the two failures are indistinguishable |

## The phase plan

Run **Phases 1-3 every time** (Phase 3 included — sharding regularly surfaces
issues the lighter phases miss). Phase 4 (Aura) needs cloud credentials rather
than a Kind cluster: run its **read-only sweep** whenever anything under
`internal/aura/` or an `Aura*` CRD changed. Phase 5 (CCDR) needs a `2026.08+`
image, which is above the pinned CI anchor — run it whenever anything under the
replication CRDs changed **and** such an image is available; if it is not, say
so in the log rather than recording the phase as passed.

### Phase 1 — Standalone (1 pod, ~2Gi)

`Neo4jEnterpriseStandalone` → `Ready`, then on it:

| Scenario | Verify |
|---|---|
| Standalone reconciles | `status.phase=Ready`, pod `1/1` |
| Database lifecycle | `Neo4jDatabase` Ready; `SHOW DATABASES` shows it `online` |
| Users / Roles / RoleBinding | `Neo4jUser`/`Role`/`RoleBinding` Ready; `SHOW USERS`/`SHOW ROLES`. **Reference at least one role by its hyphenated CR `metadata.name`** (not `spec.name`) and confirm the grant lands (no `RolesPending`) and a `RolesResolved` event fires. |
| Plugin (APOC) | `Neo4jPlugin` Ready; `RETURN apoc.version()` returns a version (standalone **ConfigMap** path) |
| Backup → restore | `neo4j-admin` path with `stopCluster: true`: add a marker node → back up → delete the marker → restore → confirm the marker returns |
| All-databases restore (standalone) | two user DBs → `Neo4jBackup` `allDatabases: true` → mutate both → `Neo4jRestore` `allDatabases: true` (`stopCluster: true`, `options.replaceExisting: true`); confirm both round-trip via the single offline Job and `status.databaseResults` are all `Completed` (`system` excluded) (#288) |
| Standalone recommended labels | `kubectl get pods -l app.kubernetes.io/name=neo4j` returns the standalone pod (it carries `app.kubernetes.io/{name,instance,managed-by}`) |
| `system` is not restorable | a `Neo4jRestore` with `database: system` → `Failed` with an actionable message |
| Database alias | `Neo4jDatabaseAlias` → Ready; connect via the alias name and reach the target; `SHOW ALIASES FOR DATABASE` lists it. Then re-point `spec.targetDatabase` at a second database and confirm the same connection string now reaches the new one |
| Alias drift is corrected | `ALTER ALIAS` it elsewhere by hand → next reconcile restores `spec.targetDatabase` |
| TLS: CA path | `spec.tls.mode: cert-manager` + `ca-cluster-issuer` → Ready, diagnostics collected, and **no** `PINNING` line in the operator log |
| TLS: certificate pinning | scale `cert-manager` to 0 (so it cannot restore the key), `kubectl patch secret <name>-tls-secret --type=json -p '[{"op":"remove","path":"/data/ca.crt"}]'`, then nudge a reconcile. Expect the operator to log `verifying the server by PINNING its certificate from 'tls.crt'` with a `pinnedFingerprintSHA256` equal to `openssl x509 -in tls.crt -noout -fingerprint -sha256`, **and** `status.diagnostics.lastCollected` to keep advancing — the pinned connection is a real authenticated session, not a fallback that merely logs |
| TLS: pin rejects a mismatch | with `ca.crt` still absent, replace the Secret's `tls.crt` with a *different* certificate carrying the same SANs and do **not** restart the Neo4j pod (it keeps serving the keystore it loaded at startup). Next reconcile must fail with `x509: certificate signed by unknown authority` in `status.diagnostics.collectionError`. Restore `tls.crt` → the next reconcile recovers. This is the only place the pin is proven to *reject*; CI cannot reach it because every CI Secret has `ca.crt` |

→ **Tear down the standalone fully** before Phase 2 — but **KEEP the backup PVC**
(and the `Neo4jBackup` CR that owns it, so retention does not prune the artifact).
Phase 2's cross-topology scenario restores that standalone-produced artifact into
the cluster, and the one-deployment-at-a-time rule means you cannot stand the
standalone back up alongside the cluster to re-make it. Deleting the PVC here
costs a full standalone rebuild-and-teardown cycle later, so treat "a retained
standalone backup" as one of Phase 1's outputs.

### Phase 2 — Cluster, 3 servers (~6Gi)

`Neo4jEnterpriseCluster` with `servers: 3` → `Ready`; pods
`{cluster}-server-0..2`; `SHOW SERVERS` lists 3 `Enabled`/`Available`. Then:

| Scenario | Verify |
|---|---|
| Cluster forms | 3 members `Enabled`/`Available`; server-based pod names |
| Database **with topology** | e.g. `3 PRIMARIES`; `SHOW DATABASE <db>` shows the primaries `online` |
| Backup → restore (cluster) | in-place **Cypher** path: back up one DB (`instanceRef` + `database`), restore into a **new** database, confirm the data round-trips |
| All-databases restore (cluster) | `Neo4jBackup` `allDatabases: true` → `Neo4jRestore` `allDatabases: true` (cloud-backed); confirm every user DB round-trips and `status.databaseResults` are all `Completed` (#222) |
| Cross-topology restore | restore the **retained standalone backup** from Phase 1 into the **cluster** via `instanceRef`; confirm the data round-trips. The cluster path stands up a short-lived `backup-seed-proxy-<restore>` Deployment that serves the backup PVC over HTTP, so the seed URI in `status` is an `http://…` URL, not a `file://` path |

3 servers (not 2) keeps split-brain / 3-primary quorum behaviour in the routine
walk. → **Tear down the cluster fully** before Phase 3.

### Phase 3 — Property sharding (cluster, `2026.06-enterprise`, 3 × 2Gi)

Sharding's documented floor is **4Gi + 1 core per server**. To fit a laptop we
deliberately relax it — **this phase is operator-mechanics verification, not
doc-following**. The relax is operator-side and DEV/TEST only:

```bash
# Downgrades the 4Gi/1-core hard rejects to warnings.
kubectl -n <operator-ns> set env deployment/<operator-deploy> \
  NEO4J_SHARDING_RELAX_MEMORY_MIN=true
kubectl -n <operator-ns> rollout status deployment/<operator-deploy>
```

Use **`2026.06-enterprise`** (the pinned CI anchor) so local sharding matches
what the [extended suite](ci_and_workflows.md) validates. On a `servers: 3`
cluster (~2Gi each):

| Scenario | Verify |
|---|---|
| Sharding cluster | `CALL dbms.components()` → `2026.06.x` enterprise; `status.propertyShardingReady=true` |
| Sharded database | `Neo4jShardedDatabase` whose **`metadata.name` differs from `spec.name`** (e.g. CR `products-sharded` / `spec.name: products`) → Ready; `SHOW DATABASES WHERE name STARTS WITH '<logical>'` lists the graph + property shards `online` |
| Sharded backup **by CR name** | `Neo4jBackup` `kind=ShardedDatabase` with `target.name` = the **CR metadata name** → `Succeeded`; `status.history[].shardArtifacts` lists every shard. (Using the *logical* name fails preflight — the operator resolves the logical name from the CR's `spec.name`.) |

→ **Tear down**, then delete the Kind cluster.

### Phase 4 — Aura (no Kind deployment; needs Aura API credentials)

**BETA / best-effort surface.** The `Aura*` CRDs talk to the Aura cloud API, so
nothing in Phases 1-3 touches them. This phase is **optional but strongly
recommended** whenever an `Aura*` CRD or `internal/aura/` changed, because the
published Aura OpenAPI spec is known to disagree with the live API (see
`docs/knowledge/operations.md` id 87).

**Take that disagreement seriously.** As of 2026-08-01 every Aura surface except
CMK has been driven against a real account, and three of them — fleet
provisioning, IP filters and invites — were **broken in ways the unit suite could
not see**, because the fixtures asserted the client matched the *spec* rather
than the API. A green `make test-unit` is not evidence that an Aura write path
works.

**Cheap technique worth reusing:** to learn an enum or a required field without
mutating anything, send a deliberately invalid value — the API answers with the
full list of accepted values, or names the missing field, and changes nothing.

| Surface | Live-verified | |
|---|---|---|
| Instances (v1 lifecycle), snapshots, restore | 2026-08-01 | ✅ |
| Databases + per-database backup/restore (v2beta1) | 2026-07-31 | ✅ |
| IP filters (v2beta1) | 2026-08-01 | ✅ contract corrected |
| Console RBAC: members, invites (v2beta1) | 2026-08-01 | ✅ invite contract corrected |
| Fleet provisioning (v2beta1) | 2026-07-31 | ✅ contract corrected |
| **Customer-managed keys (v1)** | — | ❌ **never exercised; needs a real cloud KMS key** |

Set up an `AuraProviderConfig` (or an inline `credentialsSecretRef`) with an API
client ID + secret from the Aura console.

**Read-only sweep — always safe, do this every time:**

| Scenario | Verify |
|---|---|
| Credentials + auth | A token from `/oauth/token` authenticates **both** v1 and v2beta1 calls |
| Org/project discovery | `GET /v2beta1/organizations` and `…/projects` return `{id,name}` |
| Read shapes still match | Org users return `user_id` + `organization_roles[]` with `organization-*` values; ip-filters return a **bare** array (no `data` envelope); the v1 CMK list returns only `id`/`name`/`tenant_id`; `DatabaseSummary` returns only `id` |
| Fleet reads | Fleet deployment single-GET **is** `data`-wrapped; token field is `auto_rotated`; server field is `mode_constraint` (singular); shard/txn/lag data appears only on `…/servers/{id}/databases` |

A GET-only sweep needs no throwaway resources and cannot damage anything. It is
what caught four real client bugs during the 2026-07-30 re-diff.

**Write checks — ONLY in a disposable Aura project, never production:**

| Scenario | Verify |
|---|---|
| `AuraInstance` lifecycle | create → Ready → resize (`memory`) → pause → resume → delete; `status.instanceId` pinned via the `neo4j.com/external-instance-id` annotation. **Set `deletionPolicy: Delete` on the throwaway instance** — the default is `Orphan`, so otherwise deleting the CR leaves the instance running and billing (see the teardown note below), and the delete path itself goes untested |
| `AuraDatabase` + backup | **Needs an instance created with `multiDatabase: true`** — a Business Critical tier alone is not enough, and the flag cannot be added later (`docs/knowledge/operations.md` id 90). Against any other instance expect `Ready=False`, reason `InstanceNotMultiDatabase`, with **no requeue**. On a multi-database instance: `AuraDatabaseBackup` reaches `Completed` (**not** on first observe — an empty status must read as `Pending`, and the backup does not appear in the backups LIST until it completes, so it must be polled by ID); `AuraDatabaseRestore` reports `Submitted`, never `Completed` |
| Multi-database create | An `AuraInstance` with `multiDatabase: true` + `organizationId` is created via **v2beta1** (v1 `type` names are translated), then remains manageable through v1; `status.atProvider.multiDatabase` reports `true`. On a v1-created instance the field reports the **true** value from the v2beta1 probe — `false` is correct there, and it stays **absent** (unknown) only when no `organizationId` is resolvable, since the v1 GET carries no such field |
| Console RBAC | `AuraProjectMember` for an existing **org** member is added without an invite; role PATCH bodies are accepted (they are `{organization_roles:[…]}` / `{project_roles:[…]}`, not a scalar) |
| Fleet `provision` | Deployment registered, token minted into the Secret, DBMS registers; deleting the Secret with the default `tokenPolicy` **refuses to rotate** and says why |

→ **Tear down** every resource created here; Aura bills by the hour. Deleting the
CRs is **not** sufficient proof: `deletionPolicy` defaults to `Orphan`, so a CR
delete can leave a running instance behind. Confirm with
`GET /v1/instances/<id>` → **404** and a project instance list of **0** before you
call the phase done.

### Phase 5 — Cross-cluster replication (two deployments, run sequentially; needs `2026.08+`)

**Run this only when a `2026.08+` enterprise image is available.** Hosting a
replica requires it, and the check is enforced — on an older image the CR fails
with `ReplicaVersionTooOld` and nothing else in this phase can proceed. That is
itself scenario 1: confirm the gate fires rather than silently doing nothing.

This phase looks like it breaks the **one Enterprise deployment at a time**
ground rule. It does not, and the reason is the point of the feature: with
backup-based replication the two clusters are coupled **only through the
object store**, never over the network. So the upstream produces its chain,
gets torn down, and *then* the downstream comes up and pulls from the bucket.
Two deployments, never concurrent.

Stand up MinIO as the shared bucket (same pattern the backup specs use).

**Part A — upstream (standalone is enough):**

| Scenario | Verify |
|---|---|
| Replication-source backup | `Neo4jBackup` `mode: replication-source` + `database` + `schedule` → `status.replicationPullURI` is populated and points at the chain directory. **Copy this value** |
| Chain-breaking configs rejected | the same CR with `retention` set, or `allDatabases: true`, or no `schedule` → phase `Invalid`, message names the rule |
| A FULL then a DIFF | let (or force) two runs so the bucket holds a full **and** a differential — a seed-only chain does not exercise the pull path |

→ **Tear the upstream down completely.** Leave the MinIO bucket intact.

**Part B — downstream (fresh deployment, `2026.08+`):**

| Scenario | Verify |
|---|---|
| Replica seeds | `Neo4jReplicaDatabase` with `source.pullURI`/`seedURI` from Part A → phase `Replicating`; `SHOW DATABASES` shows `type=replica`, `access=read-only`, `writer=false` on **every** copy including primaries |
| Replica is read-only | a write via `cypher-shell` fails; the same query with `--access-mode=read` succeeds |
| Alias pre-staged against a replica | `Neo4jDatabaseAlias` targeting the replica → `Ready` **while the target is still a replica** (this is what removes work from the failover window) |
| `network` mode rejected | `source.mode: network` → `Failed`, and the message names `advertised_address`. A bare "unsupported" would send someone off configuring Services |
| Promotion | `Neo4jReplicaPromotion` → `Completed`; `status.observedLagTxIds` recorded; `SHOW DATABASES` now shows a standard read-write type |
| Alias survives promotion | the **same** alias now resolves to a read-write database — no CR change, no client reconfiguration |
| Replica CR goes inert | the `Neo4jReplicaDatabase` is `phase: Promoted`; **delete it and confirm the database is NOT dropped** |
| Out-of-band promotion is safe | on a second replica, promote by hand at a `cypher-shell`, then let the operator reconcile → it goes `Promoted` (reason `DetectedOutOfBand`) rather than "correcting" the drift by dropping the database |

That last row is the one worth doing carefully: it is the failure mode the
whole observe-before-act design exists to prevent, and it is invisible to unit
tests.

→ **Tear down**, then delete the Kind cluster.

## Coverage at a glance

| | Standalone | Cluster (3) | Sharding (2026.06) | Aura (Phase 4) | CCDR (Phase 5, 2026.08+) |
|---|:---:|:---:|:---:|:---:|:---:|
| Reconcile → Ready | ✅ | ✅ | ✅ | ✅ (instance) | ✅ |
| Database lifecycle | ✅ | | | ✅ (`AuraDatabase`) | |
| Database topology | | ✅ | | n/a (Aura-managed) | ✅ (replica) |
| Users / Roles / Bindings | ✅ | | | n/a (console-RBAC only) | |
| Plugins (APOC) | ✅ (ConfigMap) | | | n/a | |
| Backup → restore | ✅ (neo4j-admin) | ✅ (Cypher) | ✅ (sharded) | ✅ (per-DB, API) | ✅ (chain as source) |
| Property sharding | | | ✅ | | |
| Aura Fleet Management | ✅ (plugin + token) | | | ✅ (`provision`) | |
| Aura console RBAC | | | | ✅ | |
| Aura read-contract sweep | | | | ✅ (GET-only) | |
| Cross-cluster replication | | | | | ✅ |
| Database aliases | ✅ | | | | ✅ (survives promotion) |
| Operator→Neo4j TLS (CA + pinned) | ✅ | | | n/a (HTTPS to Aura) | |

## Keeping this current

When you **add or change a capability**, update this page in the same PR:

1. Add a row to the **routing table** (which deployment, and *why* there).
2. Add the scenario + its in-DB check to the relevant **phase**.
3. Tick the **coverage** matrix.
4. If it changes the operator install or sizing, update the phase headers.

When a release fixes a specific bug, add a one-line scenario that would have
caught it (the v1.12.2 pass added the standalone-label, `system`-reject,
role-CR-name, and sharded-backup-by-CR-name checks). Record each run below.

## Verification log

| Release | Date | Result | Findings |
|---|---|---|---|
| v1.12.2 | 2026-06-14 | ✅ all phases pass | Doc bug: `backup_restore.md` sharded `target.name` said *logical name*, must be *CR name* (fixed, `#270` follow-up). v1.12.2 surfaces (#260/#268/#269/#270) verified live. |
| _(pre-merge, PR #333)_ | 2026-08-07 | ⚠️ partial — Phases 1 (alias scenarios) + 5A/5B validation only | **Bug found + fixed:** `spec.auth` is optional on Cluster/Standalone, but five call sites dereferenced `Spec.Auth.AdminSecret` unguarded — a `Neo4jDatabase` against a standalone without `spec.auth` panicked the reconciler and the CR sat with an empty status forever, with nothing on the CR explaining why. Widest site was `ResolvedTarget.NewClient`, shared by the user/role/binding/authrule and replication controllers. Fixed to use the existing nil-safe helpers. **Also:** `make deploy-dev-local` reports "deployment configured" without restarting the pod when the `:dev` tag is unchanged, so a rebuilt binary silently does not take effect — needs `kubectl rollout restart`. **Verified pass:** alias create/resolve, blue-green re-point, drift correction, alias delete does not drop the target; replica version gate (2026.08 required, refused on 2026.06), network-mode rejection naming `advertised_address`, replication-source R1/R2/R4 rejections, `status.replicationPullURI` published. **Not run:** replica seeding, promotion, and alias-survives-promotion — need a 2026.08+ image, which is not yet published (highest on Docker Hub is 2026.06). |
| _(pre-merge, PR #343)_ | 2026-08-21 | ⚠️ partial — Phase 1 TLS scenarios only | **Bug found + fixed:** the operator's "insecure TLS fallback" for Secrets without `ca.crt` had never worked. It set `InsecureSkipVerify: true`, but the Bolt driver derives that field from the URI scheme and resets it (`connector.tlsConfig()`), so Go verified against system roots and every connection failed `x509: certificate signed by unknown authority` — while the operator logged that verification was disabled. So `spec.tls.strictPeerValidation: false` could not connect at all, and the warning named a state that never existed. Replaced with pinning `tls.crt` as a one-certificate `RootCAs`. **A/B on one cluster, same Secret:** pre-pinning binary failed 6/6 reconciles; pinning binary connected and collected diagnostics. **Also verified:** CA path unchanged (no `PINNING` line, diagnostics fresh), pinned fingerprint matches `openssl`, a mismatched `tls.crt` is refused, and restoring it recovers. **Lesson:** handshake unit tests dialed the config verbatim and so could not see the driver's override — they now emulate it (`asDriverWouldUse`). |
| v1.14.0 | 2026-08-02 | ✅ all 4 phases pass (19/19 scenarios) — 3 bugs + 4 doc gaps found | **Bugs:** (1) deleting a `Neo4jPlugin` against a live deployment never finalizes — the removal Job is re-created every reconcile and fails `already exists`, so the CR stays Terminating and blocks namespace deletion; (2) `examples/property_sharding/development-property-sharding.yaml` does not boot — `spec.propertySharding.config` bypasses the memory-key exclusion that `spec.config` gets, so a user `heap.max_size` contradicts the operator-derived `heap.initial_size` and the JVM refuses to start; (3) `make dev-up` loads no `aura*` controller (the dev `-controllers` default omits all 12), so every Aura CR is accepted and then silently ignored — no status, no events, no logs. **Doc gaps:** Phase 1 teardown destroys the backup Phase 2's cross-topology scenario needs; Phase 4 teardown leaves a billing instance because `deletionPolicy` defaults to `Orphan`; the `multiDatabase`-stays-absent rule holds only with no org configured; sharding examples pin `2026.04` while the CI anchor is `2026.06`. **Also:** a crash-looping member surfaces only as `ConnectivityDegraded`, which points at Bolt rather than the container. |

## See also

- [Testing](testing.md) — the automated unit/integration suites and the
  core/extended label split.
- [CI/CD & Workflows](ci_and_workflows.md) — what runs per-PR vs. the manual extended suite.
- [Backup & Restore](../user_guide/guides/backup_restore.md),
  [Property Sharding](../user_guide/property_sharding.md) — the user docs this
  journey follows.
- [Project Invariants](../knowledge/invariants.md) — the hard constraints
  (KIND-only, Enterprise-only, etc.) every phase respects.
