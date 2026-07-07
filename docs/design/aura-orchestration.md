# Design: Aura orchestration (operator as an Aura control plane)

> **Status:** Draft for review. Grounded in the live Aura OpenAPI specs (v1 GA, v2beta1 beta) and the GA Terraform provider (`neo4j-labs/neo4jaura` v1.0.1), fetched 2026-07-06.
> **Product framing:** This operator is not PM-tracked and there is no official Neo4j roadmap decision for Aura-cloud orchestration. This is a technical design for this project — not an official product/version commitment. Distinct from (and complementary to) the existing `auraFleetManagement` field.
> **API foundation:** Core built on **Aura API v1 (GA)** — it is the only version with a complete instance lifecycle. v2beta1 (org/project hierarchy, multi-DB, RBAC, IP filters) is deferred to a later phase because it currently lacks scale/pause/resume/snapshot/restore/CMK.
> **Settled decisions:** (1) full lifecycle in Phase 1; (2) deletion defaults to **Orphan** (keep the cloud instance); (3) restore is a **separate `AuraRestore` CRD** (symmetry with `Neo4jBackup`/`Neo4jRestore`); (4) **ergonomic** Kind names (`AuraInstance`/`AuraSnapshot`/`AuraRestore`) in group `neo4j.neo4j.com`; (5) the connection Secret ships **all formats** (default `neo4j-driver`) **and full Service Binding** in Phase 1.
> **Review v2 (cloud-native) — agreed additions to Phase 1:** (6) **idempotent create + adopt** via the `neo4j.com/external-instance-id` external-name annotation; (7) **CEL** (`x-kubernetes-validations`) for immutability/enums — no webhook — with inline Go only for the live `instance_configurations` oracle; (8) an **`AuraProviderConfig`** CRD for credentials/defaults/rate-limiter (inline `credentialsSecretRef` retained as a single-account shortcut); (9) **metrics wiring** + **`publishConnectionDetailsTo`** (ConfigMap for non-secret endpoint). **Deferred to Phase 2:** `paused`/`managementPolicies`, the `status.atProvider` observed-state drift mirror, `AuraInstanceClass`, and unifying `Neo4jDatabase`/`User`/`Role` onto Aura targets. See the "Review v2" section below — where it differs from §4–§8, it wins.

---

## Review v2 — agreed cloud-native design (authoritative)

This refines §4–§8 with the decisions taken in design review. **Where this section and a base section differ, this section wins.**

### A. Idempotent create + adopt (external-name)
The Aura instance ID is the source of truth, stored in the annotation **`neo4j.com/external-instance-id`** on the `AuraInstance`, written as the *first* action after a successful create (before status). Reconcile:
- **annotation empty →** *observe-before-create*: `GET /instances?tenantId=<project>` and match by the CR's deterministic instance name; **adopt** (record the id) if found, else `POST /instances` and immediately persist the returned id to the annotation (with `RetryOnConflict`).
- **annotation set →** GET/observe that instance; never POST.

Why: Aura v1 has **no idempotency token**, so a crash between `POST` (201) and the status write would otherwise create a *second paid instance*. This makes create effectively idempotent and also enables **import**: set the annotation (or `spec.instanceId`, see below) on a CR pointing at a pre-existing instance and the operator adopts it instead of recreating. Documented limitation: the create→persist window is best-effort-minimized, not atomic (no server-side idempotency key exists).

`AuraInstance` gains an optional **`spec.instanceId`** for explicit user-driven import; the annotation remains the internal source of truth (spec value is copied to the annotation on first reconcile).

### B. Declarative validation via CEL (no webhook)
Immutable fields (`projectId`, `cloudProvider`, `region`, `type`, `version`, `source`, `customerManagedKeyId`) carry `+kubebuilder:validation:XValidation:rule="self == oldSelf"` transition rules — **apiserver-enforced** (K8s ≥1.29; repo min is 1.32), satisfying Invariant 1 with **no webhook** and no controller-bypassable gap. Enums and cross-field rules (e.g. `storage` forbidden on `free-db`; `secondariesCount`/`cdcEnrichmentMode` only for VDC) also use `XValidation`. Inline Go validation is kept ONLY for what CEL cannot express: checking the desired tier/region/memory/version/provider combo against the live `/tenants/{id}.instance_configurations` oracle.

### C. `AuraProviderConfig` CRD (credentials + defaults + rate limiter)
Namespaced CRD holding the client-credentials Secret ref + defaults, owning the per-credential **token cache + rate limiter** (25/125 rpm). Resources reference it via `spec.providerConfigRef`; an inline `spec.credentialsSecretRef` stays as a single-account shortcut (mutually exclusive). ESO-friendly (the Secret can be populated by External Secrets Operator).
```yaml
kind: AuraProviderConfig
metadata: { name: aura-prod, namespace: team-graph }
spec:
  credentialsSecretRef: { name: aura-api-creds }   # keys: clientId, clientSecret
  defaultProjectId: "abc123"
status:
  conditions: [{ type: Ready, status: "True", reason: CredentialsValidated }]
```

### G. Metrics wiring
When an instance exposes `metrics_integration_url`, surface it on `status` and (opt-in) generate a Prometheus **`ScrapeConfig`** so Aura DBs land in the same monitoring stack as self-managed. Add operator-level Prometheus metrics for Aura API calls (latency, error/rate-limit counts) via the existing `internal/metrics` package.

### H. `publishConnectionDetailsTo` (ConfigMap)
In addition to the connection Secret (§4.5), publish the **non-secret** endpoint (`NEO4J_URI`, `instanceId`, `region`, `type`) to a ConfigMap named by `spec.publishConnectionDetailsTo`, for teams that keep URIs out of Secrets. Credentials remain Secret-only.

### Spec additions (supersede §4.1)
`AuraInstance.spec` gains: `providerConfigRef` (+ retained `credentialsSecretRef` shortcut), optional `instanceId` (import), `publishConnectionDetailsTo`. Immutability moves from prose to CEL markers (B).

### Deferred to Phase 2 (explicitly out of Phase 1)
- **D/E:** `paused` annotation + `managementPolicies`; `status.atProvider` full observed-state mirror + continuous drift auto-correction/reporting. Phase 1 keeps the simpler `status` + `Ready`/`Synced` conditions from §4.1.
- **F:** `AuraInstanceClass` (StorageClass-style defaults).
- **I:** `Neo4jDatabase`/`User`/`Role` targeting an `AuraInstance` via `cluster_resolver`.

---

## 1. Why

Today the operator manages **self-managed** Neo4j (StatefulSets on the cluster) and can *register* those into the Aura console for monitoring (`auraFleetManagement`). It cannot manage **Aura-hosted** databases. This design adds a **control-plane** capability: declare an Aura instance as a Kubernetes object and have the operator provision, scale, pause/resume, snapshot, restore, and (optionally) delete it via the Aura REST API — the Crossplane/ACK pattern, specialized to Aura.

Value: one declarative surface (`kubectl` / GitOps) for both self-managed and cloud Neo4j; Aura lifecycle in the same review/audit/RBAC flow as the rest of the platform.

## 2. Scope and non-goals

**In scope (Phase 1):** full lifecycle of an Aura **instance** (create, resize, pause/resume, delete) + on-demand **snapshots** + **restore**.

**Explicit non-goal — do not conflate with `auraFleetManagement`.** They run in opposite directions:

| | Existing `auraFleetManagement` | This design (Aura orchestration) |
|---|---|---|
| Direction | Registers a self-managed cluster *into* the Aura console | Operator *provisions/manages* Aura-hosted instances |
| Mechanism | `CALL fleetManagement.registerToken` over Bolt | Aura REST API (OAuth2 client-credentials) |
| Lives on | `Neo4jEnterpriseCluster`/`Standalone` spec | New CRDs (`AuraInstance`, …) |

The new CRDs sit alongside fleet management; neither replaces the other.

**Deferred (Phase 3):** v2beta1-only features — multi-database (`AuraDatabase`), IP filters, org/project RBAC, billing/usage.

## 3. API foundation: v1 (GA), with v2beta1 in view

| Capability | Aura API **v1** (GA) | Aura API **v2beta1** (beta) |
|---|---|---|
| Model | Flat; `tenant_id` as a field | Hierarchical `/organizations/{org}/projects/{project}/…` |
| Instance create / delete | ✅ | ✅ |
| Resize (PATCH), pause, resume | ✅ | ❌ |
| Snapshots + restore | ✅ | ❌ (per-DB backups exist, different model) |
| Upgrade, overwrite/clone, CMK | ✅ | ❌ |
| New surface | — | multi-DB, IP filters, RBAC, billing, GenAI agents, GDS sessions |
| Stability | GA, stable | beta — `legacy_status` rename pending, enum-vocabulary mismatches, undocumented request bodies |

**Decision:** implement the lifecycle on **v1**. A v2beta1-only operator could create a database it cannot scale, pause, snapshot, or key-manage — unacceptable for a lifecycle controller.

**Forward-compatibility:** carry `projectId` now (v1 `tenant_id`); when v2beta1 matures, add an optional `organizationId` and a version selector without breaking existing CRs. Re-diff the CRD schema against the live spec each Aura release (v2beta1 has visible churn).

**Terminology:** the console/docs say **Project**; the API says **tenant** / `tenant_id`. CRDs expose `projectId` and map to `tenant_id` internally.

## 4. CRDs

Group `neo4j.neo4j.com`, version `v1beta1` (same as all existing CRDs). Ergonomic Kind names: `AuraInstance`, `AuraSnapshot`, `AuraRestore` (`kubectl get aurainstances`).

### 4.1 `AuraInstance`

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInstance
metadata:
  name: analytics-prod
  namespace: team-graph
spec:
  # OAuth2 client-credentials (keys: clientId, clientSecret)
  credentialsSecretRef: { name: aura-api-creds }

  # --- Placement (IMMUTABLE: a change is rejected by validation) ---
  projectId: "abc123"            # → API tenant_id
  cloudProvider: gcp             # aws | gcp | azure
  region: europe-west1
  type: professional-db          # free-db | professional-db | business-critical |
                                 # enterprise-db (VDC) | professional-ds | enterprise-ds
  version: "5"                   # coarse Aura major version

  # --- Sizing (MUTABLE via PATCH resize) ---
  memory: 4GB
  storage: 8GB                   # omit for free-db (storage is coupled/derived there)

  # --- Lifecycle / features (MUTABLE) ---
  paused: false                  # desired state → drives pause / resume
  vectorOptimized: false
  graphAnalyticsPlugin: false
  secondariesCount: 1            # VDC (enterprise-db) only
  cdcEnrichmentMode: "OFF"       # OFF | DIFF | FULL — VDC / Business Critical only
  customerManagedKeyId: ""       # VDC / AuraDS-Enterprise only (Phase 2)

  # --- Clone-from at create (IMMUTABLE, create-time only) ---
  source:
    instanceRef: analytics-staging   # or instanceId
    snapshotId: "2026-07-01T…"       # optional; requires exportable snapshot

  # --- Outputs / policy ---
  connectionSecretName: analytics-prod-conn   # operator WRITES connection details here
  connectionSecretFormat: neo4j-driver        # neo4j-driver | aura-dotenv | jdbc | servicebinding | custom (see §4.5)
  deletionPolicy: Orphan          # Orphan (default) | Delete
  deletionProtection: false       # extra guard when deletionPolicy=Delete
status:
  instanceId: "d3adb33f"
  phase: Running                  # mirrors the Aura status state machine (below)
  connectionUrl: neo4j+s://d3adb33f.databases.neo4j.io
  binding: { name: analytics-prod-conn }   # Provisioned-Service pointer (Service Binding spec)
  observedGeneration: 3
  lastSyncedAt: 2026-07-06T10:00:00Z
  conditions:
    - { type: Ready, status: "True", reason: Running }
    - { type: Synced, status: "True" }
```

**Field mutability** (enforced inline; see §6):

| Mutable via `PATCH /instances/{id}` | Immutable (change → validation error) |
|---|---|
| `memory`, `storage`, `name` (metadata), `vectorOptimized`, `graphAnalyticsPlugin`, `secondariesCount`, `cdcEnrichmentMode`, `paused` (→ pause/resume) | `projectId`, `cloudProvider`, `region`, `type`, `version`, `customerManagedKeyId`, `source` |

Notes:
- `type` has exactly one in-place transition path: `professional-db → business-critical` via `POST /instances/{id}/upgrade`. Everything else immutable → replace requires an explicit new CR (never auto-destroy a live DB — see §6).
- **Credentials are returned exactly once** by `POST /instances` (202): `username`, `password`, `connectionUrl`. The reconciler writes them to `connectionSecretName` (owner-ref'd) on first success. There is no read-back — a lost Secret cannot be recovered via this API.
- `free-db` specifics: no `storage`; adds read-only `graphNodes`/`graphRelationships` counts (surfaced in status if present).

**Printer columns:** `PHASE`, `TYPE`, `REGION`, `MEMORY`, `CONNECTION`, `AGE`.

### 4.2 `AuraSnapshot` (= backup)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraSnapshot
metadata: { name: nightly-2026-07-06, namespace: team-graph }
spec:
  instanceRef: analytics-prod        # AuraInstance in the same namespace
status:
  snapshotId: "2026-07-06T09:12:00Z-adhoc"
  profile: AdHoc                     # AdHoc | Scheduled
  status: Completed                  # Pending | InProgress | Completed | Failed
  exportable: true
  timestamp: 2026-07-06T09:12:00Z
```

- Creates an **on-demand** snapshot (`POST /instances/{id}/snapshots`, 202 → poll).
- **Documented caveat baked into the CRD:** the Aura API **cannot delete snapshots.** Deleting an `AuraSnapshot` CR releases its finalizer and drops it from cluster state; the Aura snapshot **persists**. This must be surfaced in the CRD description and status so it isn't misread as a leak. (Same behavior as the Terraform provider.)
- Scheduling: for recurring snapshots, an `AuraSnapshotSchedule` (cron → child `AuraSnapshot`s) is a Phase-2 nicety; MVP is on-demand only (Aura already takes tier-based scheduled snapshots server-side).

### 4.3 `AuraRestore` (= restore) — chosen model

Symmetric with the existing `Neo4jBackup` + `Neo4jRestore` split. Restore is an imperative one-shot action modeled as an auditable object.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraRestore
metadata: { name: prod-rollback-jul6, namespace: team-graph }
spec:
  instanceRef: analytics-prod         # target AuraInstance (restored IN PLACE)
  snapshotId: "2026-07-06T09:12:00Z-adhoc"
  # OR: snapshotRef: nightly-2026-07-06  (resolve snapshotId from an AuraSnapshot CR)
status:
  phase: Completed                    # Pending | Restoring | Completed | Failed
  startedAt:  2026-07-06T10:00:05Z
  finishedAt: 2026-07-06T10:03:40Z
  message: ""
```

- Drives `POST /instances/{id}/snapshots/{snapshotId}/restore` (202 → instance goes `restoring → running`).
- Cross-object coordination (an `AuraRestore` mutates an `AuraInstance` it does not own): gate on the instance being `Running`, treat `409 ongoing-database-operation` as requeue, and reflect progress. This is exactly the pattern the current `Neo4jRestore`↔cluster flow already implements.
- Terminal `Completed`/`Failed` CRs are kept as history (no re-fire). Optional TTL/GC later.

### 4.4 Auth

OAuth2 **client-credentials**. A namespaced Secret (`credentialsSecretRef`) holds `clientId` + `clientSecret`. The client exchanges them at `POST https://api.neo4j.io/oauth/token` (HTTP Basic + `grant_type=client_credentials`) for a **1-hour** bearer token, caches it, and **refreshes on 403** (Aura returns 403 — not 401 — for an expired token). Per-CR ref matches the operator's existing secret-ref idiom. A shared `AuraProviderConfig` CRD (bundling creds + `projectId`/defaults, Crossplane-style) is a Phase-2 convenience, not required for MVP.

### 4.5 Connection Secret & consumption helpers

The `connectionSecretName` Secret is a first-class **binding artifact**, not just a credential dump — apps should consume an Aura instance as easily as an in-cluster one.

**Secret contents** (mirror Aura's downloadable credentials file + the Neo4j driver env conventions the whole ecosystem — drivers, LangChain, the GenAI stack — already expects):

| Key | Value | Notes |
|---|---|---|
| `NEO4J_URI` | `neo4j+s://<host>` | from `connection_url`; the canonical driver/env name |
| `NEO4J_USERNAME` | `neo4j` | |
| `NEO4J_PASSWORD` | one-shot password | captured on create |
| `NEO4J_DATABASE` | `neo4j` | default database |
| `AURA_INSTANCEID` / `AURA_INSTANCENAME` | id / name | matches the console `.env` download |
| `host`, `port`, `scheme` | split components | for tools that want discrete parts |

`spec.connectionSecretFormat` selects the key layout. **All formats ship in Phase 1.**
- **`neo4j-driver`** (default) — the uppercase env keys in the table above. `envFrom`-safe; the universal driver/GenAI-stack names.
- **`aura-dotenv`** — the driver keys plus a single `credentials.env` blob (byte-identical to the console "Download credentials") for file-mount / `source` consumers.
- **`jdbc`** — adds `NEO4J_JDBC_URL=jdbc:neo4j:neo4j+s://<host>` for JVM / BI tools.
- **`servicebinding`** — the lowercase, file-style keys the [Service Binding spec](https://servicebinding.io) expects (`type=neo4j`, `provider=aura`, `uri`, `username`, `password`, `database`) with no uppercase env noise, for pure SB projection.
- **`custom`** — a user-supplied key template for apps with fixed non-standard names.

Consumption matrix:

| Format | Best consumed via | `envFrom`-safe | Notes |
|---|---|---|---|
| `neo4j-driver` (default) | `envFrom` / `secretKeyRef` | ✅ | universal driver / GenAI env names |
| `aura-dotenv` | volume mount + `source` | ⚠️ blob key | console-identical `.env` |
| `jdbc` | `secretKeyRef` | ✅ | JVM / BI |
| `servicebinding` | Service Binding projection | ⚠️ lowercase keys | needs an SB controller in-cluster |
| `custom` | as templated | depends | escape hatch |

**Consumption ergonomics:**
- Stable, owner-ref'd Secret, kept in sync (e.g. on instance rename). Apps consume via `envFrom.secretRef`, `valueFrom.secretKeyRef`, or a mounted volume — identical to any other Neo4j connection Secret.
- **Service Binding support (full, Phase 1):** `AuraInstance` is always a compliant *Provisioned Service* — `status.binding.name = <connectionSecretName>` and a `type=neo4j` key are set regardless of format, so any Service-Binding-aware workload (Spring Cloud Bindings, Quarkus, Paketo buildpacks) can bind. For teams that want the pure SB layout with no uppercase env noise, `connectionSecretFormat: servicebinding` emits exactly the SB-spec keys. We are only the *producer* of the contract; projecting the Secret into pods is done by a Service Binding controller (e.g. `servicebinding-runtime`) the cluster owner installs — no runtime dependency on our side.
- The non-secret endpoint is also on `status.connectionUrl` (visible in `kubectl get`), optionally mirrored to a ConfigMap for teams that keep URIs out of Secrets.

**Deliberate non-helpers (with reasons):**
- **No in-cluster `Service`/`ExternalName` proxy.** Aura is a public TLS endpoint; routing Bolt through a different in-cluster DNS name breaks `neo4j+s://` hostname/SNI verification (the cert CN is the Aura host). The Secret + `status.connectionUrl` is the correct handle, not a synthetic Service.
- **No credential rotation.** Aura v1 exposes no password-reset/rotate endpoint — credentials are create-once. The operator surfaces them but cannot rotate them (rotation would mean overwrite/recreate). Documented, not silently assumed.
- **Cross-namespace fan-out** is out of scope — the Secret lives in the CR's namespace; consumers elsewhere need a replicator (e.g. reflector/kubed).

## 5. Reconciler architecture

**Async, requeue-driven state machine** — there is no long-running-operation resource in the Aura API; every lifecycle op returns `202` and you poll `GET /instances/{id}.status`. This maps onto the operator's existing `rolling_upgrade_statemachine.go` shape.

Instance status state machine (v1's 13 states):

```
(absent) --create--> creating ─────────────► running ◄──────────┐
                                    ▲   │  ▲   │                  │
     resuming ◄── paused ◄── pausing│   │  │   └── updating ──────┤ (resize/secondaries/CDC)
        │                           │   │  │        restoring ────┤ (AuraRestore)
        └───────────────────────────┘   │  └────── overwriting ──┘ (clone-to-existing)
 destroying ──► (gone / 404)             └── loading / loading failed (terminal error)
```

- **Serialize per instance.** `409 ongoing-database-operation` → requeue, not error.
- **Deletion (Orphan default).** Finalizer on `AuraInstance`:
  - `deletionPolicy: Orphan` (default) → on CR delete, **do not** call the Aura API; just release the finalizer. The cloud instance keeps running. Safe against fat-fingered `kubectl delete`.
  - `deletionPolicy: Delete` → call `DELETE /instances/{id}`, poll to 404 (idempotent — 202 or 404 = done), then release. If `deletionProtection: true`, refuse and require it be cleared first.
- **Drift / immutability.** Converge mutable fields via `PATCH`. On an **immutable-field** change, set `Ready=False` + `Degraded` condition and a Warning event, and **refuse** — never destroy+recreate a live cloud DB implicitly (data + billing). Mirrors the Terraform provider's `prevent_destroy` guidance.
- **Credential capture.** On the create 202, immediately write `username`/`password`/`connectionUrl` to `connectionSecretName` (owner-ref'd), before anything can fail.
- **Inline validation (Invariant 1 — NO webhooks).** `internal/validation/aura_validator.go`, called from the reconciler:
  - immutability check on update;
  - validate `type`/`region`/`memory`/`version`/`cloudProvider` against the project's `GET /tenants/{id}.instance_configurations` (the authoritative per-project oracle) rather than hardcoded enums;
  - tier rules: `secondariesCount`/`cdcEnrichmentMode` VDC-only, `customerManagedKeyId` VDC/AuraDS-Ent only, no `storage` on `free-db`.
- **Client resilience.** Client-side throttle to the per-tier rate limit (25 rpm trial / 125 rpm paid); exponential backoff honoring `Retry-After` on 429/5xx; HTTPS-only (308 otherwise); parse **both** Aura error shapes — standard `{ "errors": [{message,reason,field}] }` and gateway `{ "error": string }` — keying decisions off `reason`.
- **Status writes** wrapped in `retry.RetryOnConflict`; **events** via new constants in `events.go`.

## 6. Fit with existing operator conventions

- **CRDs:** `api/v1beta1/aura_{instance,snapshot,restore}_types.go` + kubebuilder markers → `make manifests generate sync-all` → Helm CRDs, editor/viewer roles, OperatorHub bundle; the **`check-drift`** gate covers them. Add a description case to `scripts/helm-sync-artifacthub-crds.sh` for each new Kind.
- **Controllers:** `internal/controller/aura_{instance,snapshot,restore}_controller.go`, each with `Reconcile` + `SetupWithManager`, RBAC via `+kubebuilder:rbac` markers, finalizers via the `finalizer_deletion.go` pattern.
- **Aura API client:** new `internal/aura/` package (token manager + typed client + error/status mapping). Keep it pure/testable; unit-test the state-machine transitions and error parsing without a live API.
- **No webhooks** (Invariant 1): all validation inline. **Kind-only / enterprise-image** invariants are unaffected (Aura is external).
- **Tests:** unit tests for validator + client + state machine; an envtest suite for the controllers with a **faked Aura API** (httptest server) so no real Aura account is needed in CI. A gated, opt-in live-smoke (real client credentials via CI secret) can validate end-to-end out of band.

## 7. Strategic synergy (future, not Phase 1)

The operator already has `Neo4jDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jBackup` acting on a target via Bolt. Aura exposes Bolt (`neo4j+s://`). Extending `cluster_resolver.go`'s `ResolveClusterRef` to also resolve an `AuraInstance` would let those CRDs target Aura — one declarative surface for databases/users/roles regardless of backend. Caveats: non-`multi_database` tiers can't `CREATE DATABASE`; Aura restricts some admin. North star, not MVP.

## 8. Phasing

- **Phase 1 (MVP):** `AuraInstance` (create/resize/pause/resume/delete, Orphan default) + `AuraSnapshot` (on-demand) + `AuraRestore`; **`AuraProviderConfig`** (C); **idempotent create + adopt** via the external-name annotation (A); **CEL** immutability/enum validation + inline `instance_configurations` oracle (B); connection Secret with **all formats** (`neo4j-driver` default, `aura-dotenv`, `jdbc`, `servicebinding`, `custom`) + **full Service Binding** + **`publishConnectionDetailsTo`** ConfigMap (H); **metrics wiring** (G); faked-API tests. (See the Review v2 section — it supersedes where they differ.)
- **Phase 2 (done):** `professional-db → business-critical` upgrade (`POST /instances/{id}/upgrade`); CMK (`AuraCustomerManagedKey`) with its own `auraCMKAPI` interface, idempotent create+adopt (match on the immutable cloud `keyId`+placement), and in-use-key delete guard; D/E ergonomics — `managementPolicies`, `neo4j.com/paused`, `status.atProvider`; Aura-API Prometheus counters/histogram (`neo4j_operator_aura_api_requests_total` / `_request_duration_seconds`, route-normalized `operation` label) via an Observer hook on the client.
- **Phase 2 (deferred — documented follow-ups):** clone/overwrite (`source` + overwrite-into-existing), `AuraSnapshotSchedule`, `ScrapeConfig` auto-generation from `metricsIntegrationUrl`. Low value relative to cost; revisit on demand.
- **Phase 3 (v2beta1 — live but beta, not GA-gated):** `AuraDatabase` (multi-DB), `AuraIPFilter`, org/project RBAC are v2beta1-only (absent from v1 GA) — callable today but on an unstable beta; hold on stability, not availability. `AuraInstanceClass` and Aura-target-aware `Neo4jDatabase`/`User`/`Role` are **not** API-gated (operator-side, buildable on v1 anytime).

## 9. Risks & caveats

- **Destructive operations on real cloud resources** — mitigated by Orphan-by-default deletion, deletion-protection, and refuse-don't-recreate on immutable drift.
- **One-shot credentials** land in a K8s Secret — document RBAC + at-rest encryption expectations.
- **Snapshots are not API-deletable** — documented on the CRD; CR deletion ≠ snapshot deletion.
- **v2beta1 beta churn** — do not depend on it for lifecycle; pin to the live spec and re-diff per release.
- **Prior art is Neo4j-Labs, not first-party-supported** (Terraform provider, `aura-go-sdk`) — good references, not guarantees.
- **Rate limits** are low on trial keys (25 rpm) — the client must throttle to avoid self-inflicted 429s across many CRs.

## 10. Open questions

- Namespaced vs cluster-scoped credentials / provider config (per-CR ref for MVP; `AuraProviderConfig` later).
- Whether to expose a `sizing` helper (v1 `POST /instances/sizing` for AuraDS) as a CRD/annotation.
- GDS Sessions (`/graph-analytics/sessions`) — ephemeral compute; likely a separate future CRD, out of scope here.

## 11. References

- Aura API v1 (GA) OpenAPI: `https://neo4j.com/docs/aura/platform/api/specification/aura_api_spec_v1.yaml` (base `https://api.neo4j.io/v1`).
- Aura API v2beta1 (beta) OpenAPI: `https://api.neo4j.io/v2beta1/spec.json`.
- Aura API auth: `https://neo4j.com/docs/aura/api/authentication/` · overview: `https://neo4j.com/docs/aura/api/overview/`.
- Aura product docs: `https://neo4j.com/docs/aura/`.
- Prior art: Terraform provider `neo4j-labs/neo4jaura` (GA v1.0.1) — `neo4jaura_instance`, `neo4jaura_snapshot`, `neo4jaura_projects`; community `aura-go-sdk` (route strings).
- Existing operator context: `auraFleetManagement` in `api/v1beta1/neo4jenterprisecluster_types.go`; state-machine pattern in `internal/controller/rolling_upgrade_statemachine.go`; validation pattern in `internal/validation/`; conventions in `CLAUDE.md` + `docs/knowledge/invariants.md`.
