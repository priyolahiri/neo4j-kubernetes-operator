# Aura Orchestration

Manage **Neo4j Aura cloud instances** declaratively from Kubernetes — provision,
resize, pause/resume, snapshot, restore, and (optionally) delete them via the
Aura REST API.

!!! note "Not the same as Aura Fleet Management"
    [Aura Fleet Management](aura_fleet_management.md) registers a **self-managed**
    cluster *into* the Aura console for monitoring. **Aura Orchestration** is the
    opposite direction: the operator acts as a **control plane** that provisions
    and manages **Aura-hosted** instances. The two are complementary.

Design details and rationale: `docs/design/aura-orchestration.md`.

## Prerequisites

1. An Aura account and a **project** (the API calls it `tenant`).
2. An OAuth **client credential** (Client ID + Secret) from the Aura console
   (Account settings → API keys).
3. Store it in a Kubernetes Secret:
   ```bash
   kubectl create secret generic aura-api-creds \
     --from-literal=clientId=<CLIENT_ID> \
     --from-literal=clientSecret=<CLIENT_SECRET>
   ```

## CRDs

| Kind | Purpose |
|---|---|
| `AuraProviderConfig` | Holds the API credentials + account defaults; referenced by resources via `providerConfigRef`. |
| `AuraInstance` | A managed Aura instance (full lifecycle). |
| `AuraSnapshot` | An on-demand snapshot of an instance. |
| `AuraRestore` | An in-place restore of an instance from a snapshot. |
| `AuraCustomerManagedKey` | Registers a customer-managed encryption key (CMK) for dedicated-tier instances. |
| `AuraIPFilter` | Manages an organization-scoped network IP filter (allowlist) via the Aura API **v2beta1** (beta). |

## Quick start

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraProviderConfig
metadata: { name: aura }
spec:
  credentialsSecretRef: { name: aura-api-creds }
  defaultProjectId: "<your-project-id>"
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInstance
metadata: { name: analytics }
spec:
  providerConfigRef: { name: aura }
  cloudProvider: gcp          # aws | gcp | azure   (immutable)
  region: europe-west1        #                     (immutable)
  type: professional-db       #                     (immutable)
  version: "5"                #                     (immutable)
  memory: 2GB                 # mutable → online resize
  connectionSecretName: analytics-conn
```

The operator creates the instance and writes its connection details (URI + the
one-time credentials) to the `analytics-conn` Secret. Watch it:

```bash
kubectl get aurainstance analytics -w
```

## Connecting your app

The connection Secret is a first-class binding artifact. `connectionSecretFormat`
selects the key layout:

| Format | Keys | Best for |
|---|---|---|
| `neo4j-driver` (default) | `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`, `AURA_INSTANCEID/NAME` | `envFrom` / driver env conventions |
| `aura-dotenv` | above + a `credentials.env` blob | mounting the console-style `.env` |
| `jdbc` | above + `NEO4J_JDBC_URL` | JVM / BI tools |
| `servicebinding` | lowercase SB-spec keys | Service Binding projection |

Every format also carries `type=neo4j`/`provider=aura` and sets
`status.binding.name`, so the instance is a compliant **Service Binding**
Provisioned Service. Non-secret endpoint details can additionally be mirrored to
a ConfigMap via `spec.publishConnectionDetailsTo`.

## Lifecycle

- **Resize:** change `spec.memory` / `spec.storage` → online resize.
- **Pause/resume:** set `spec.paused: true` / `false`.
- **Upgrade tier:** change `spec.type` from `professional-db` to
  `business-critical` → in-place upgrade (see below).
- **Snapshot:** create an `AuraSnapshot` referencing the instance.
- **Restore:** create an `AuraRestore` with `instanceRef` + `snapshotId` (or
  `snapshotRef` to an `AuraSnapshot`). Restores are in place and one-shot.

## Upgrading a tier

`spec.type` is immutable **except** for the one in-place upgrade path Aura
supports: `professional-db` → `business-critical`. Change the field and the
operator issues the upgrade (the DBID and connection strings are preserved):

```yaml
spec:
  type: business-critical   # was professional-db
```

The apiserver rejects any other `type` change (a different tier requires a new
instance). Aura requires ≥ 2GB storage for Business Critical, and the GDS plugin
is removed on upgrade — size and configure the instance accordingly first.

## Customer-managed encryption keys (CMK)

Dedicated-tier instances (`enterprise-db` / `enterprise-ds`) can be encrypted
with a key you hold in your own cloud KMS. Register the key with an
`AuraCustomerManagedKey`, then reference the ID it produces from the instance:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraCustomerManagedKey
metadata: { name: analytics-cmk }
spec:
  providerConfigRef: { name: aura }
  cloudProvider: gcp            # immutable
  region: europe-west1          # immutable
  instanceType: enterprise-db   # immutable (enterprise-db | enterprise-ds)
  keyId: projects/p/locations/europe-west1/keyRings/aura/cryptoKeys/neo4j
  deletionPolicy: Orphan        # Orphan (default) | Delete
```

`keyId` is the cloud KMS key resource identifier (AWS KMS ARN, GCP KMS resource
name, or Azure Key Vault key URL) — grant Aura access to it out of band first.
Once the key is `Ready`, copy `status.customerManagedKeyId` into an
`AuraInstance`'s `spec.customerManagedKeyId`.

- **Placement is immutable.** `cloudProvider`, `region`, `instanceType`, and
  `keyId` cannot change after creation.
- **Deletion.** `Orphan` (default) leaves the key registered in Aura; `Delete`
  deregisters it. Aura refuses to delete a key that is still bound to a running
  instance — the CR then reports a `KeyInUse` condition and keeps its finalizer
  until you detach/delete those instances.

## Management policies

Every managed Aura resource honours `spec.managementPolicies` (default `["*"]`,
full management). Restrict what the operator may do — a Crossplane-style safety
knob:

- `["Observe"]` — read-only: never create, update, or delete; just reflect the
  cloud state into `status`.
- `["Observe","Create","Update"]` — manage everything except deletion.

Set the `neo4j.com/paused: "true"` annotation on any Aura CR to suspend
reconciliation entirely (including deletion) for incident response, without
deleting the CR. `AuraInstance` also mirrors the last-observed cloud state into
`status.atProvider` for drift inspection.

## Managing databases on an Aura instance

`Neo4jDatabase` can target a managed Aura instance instead of a self-managed
cluster — point it at the instance with `auraInstanceRef` (mutually exclusive
with `clusterRef`):

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jDatabase
metadata: { name: analytics-db }
spec:
  auraInstanceRef: analytics   # the AuraInstance, same namespace
  name: analytics
```

The operator connects to the instance over Bolt (`neo4j+s://…`, using the admin
credentials it captured into the connection Secret) and runs the same
`CREATE DATABASE` / `DROP DATABASE` DDL it uses for self-managed clusters.

- **Multi-database tiers only.** Additional databases require a
  multi-database-capable Aura tier (Business Critical / dedicated). On a
  single-database tier (`free-db`, `professional-db`) the operator refuses up
  front with an `AuraTierUnsupported` condition rather than looping on the Aura
  rejection.
- **Credentials.** Works when the operator holds admin credentials — i.e. an
  instance it created (the one-time password is captured at create). An imported
  instance whose password the operator never saw needs credentials supplied.
- `spec.topology` is ignored for Aura targets — Aura manages replication per
  tier.

## Managing users & roles on an Aura instance

`Neo4jUser`, `Neo4jRole`, and `Neo4jRoleBinding` accept `auraInstanceRef` too
(same mutually-exclusive-with-`clusterRef` rule). The operator manages the
instance's security graph over Bolt with the same Cypher it uses for
self-managed clusters:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRole
metadata: { name: analytics-reader }
spec:
  auraInstanceRef: analytics
  name: analytics_reader
  privileges:
    - "GRANT MATCH {*} ON GRAPH neo4j NODES * TO analytics_reader"
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jUser
metadata: { name: app-reader }
spec:
  auraInstanceRef: analytics
  username: app_reader
  passwordSecretRef: { name: app-reader-creds }
  roles: [analytics-reader]     # resolves to the role on the SAME Aura instance
```

Cross-references stay scoped to the target: a user's `roles` resolve only
against `Neo4jRole`s pointing at the *same* `auraInstanceRef`. Custom users and
roles require an Aura tier that permits them (dedicated tiers) — on a tier that
doesn't, the operator surfaces the Aura error rather than looping.

## Importing an existing instance

Point a CR at an existing Aura instance by ID — the operator **adopts** it
instead of creating a new one:

```yaml
spec:
  instanceId: "d3adb33f"   # existing Aura instance ID
```

Internally the ID is tracked in the `neo4j.com/external-instance-id` annotation,
which also makes create idempotent (a crash can't produce a duplicate instance).

## Deletion

`spec.deletionPolicy` controls what happens when the CR is deleted:

- **`Orphan`** (default) — the CR is removed; the **cloud instance keeps
  running**. Safe against accidental `kubectl delete`.
- **`Delete`** — the cloud instance is destroyed. Set `deletionProtection: true`
  to block this until explicitly cleared.

## Known limitations

- **Credentials are create-once.** Aura returns the instance password only at
  creation; the operator captures it into the connection Secret then. There is
  no password-reset endpoint, so it cannot be rotated by the operator.
- **Snapshots cannot be deleted via the API.** Deleting an `AuraSnapshot` CR
  removes it from the cluster only; the Aura snapshot persists.
- **Immutable placement.** `cloudProvider`, `region`, `version` (and `type`,
  except the `professional-db → business-critical` upgrade) cannot be changed
  after creation (enforced by the apiserver). Changing them requires a new
  instance.

## Network IP filtering (beta)

`AuraIPFilter` manages a network IP filter (allowlist). In v2beta1 a filter is
**organization-scoped** (`/organizations/{org}/ip-filters`) and *applied* to one
or more instances — model that with `instanceRefs`:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraIPFilter
metadata: { name: office-only }
spec:
  providerConfigRef: { name: aura }
  organizationId: "<your-org-id>"     # or set defaultOrganizationId on the provider config
  instanceRefs: [analytics]           # instances the filter is applied to
  allowList:                          # v2beta1 splits CIDR into address + prefixLen
    - { address: "203.0.113.0", prefixLen: 24, description: office }
  deletionPolicy: Orphan              # default; Delete removes the filter (opens access)
```

Each `allowList` entry is `{address, prefixLen, description?}` — so
`"203.0.113.0/24"` becomes `address: "203.0.113.0"`, `prefixLen: 24`. Set
`filteringDisabled: true` to turn the filter off without deleting it.

!!! warning "Beta / best-effort"
    IP filtering is only exposed on the Aura API **v2beta1**, an unstable beta
    (breaking changes are allowed without a version bump). This CRD is
    best-effort: the client shape follows the official v2beta1 `IpFilter` schema,
    but v2beta1 may change without a version bump. The rest of Aura orchestration
    uses the stable v1 API. See `docs/design/aura-orchestration.md`.

## Metrics

The operator exports Prometheus metrics for its Aura API traffic on the standard
metrics endpoint:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `neo4j_operator_aura_api_requests_total` | counter | `operation`, `result` | Aura Platform API requests, by normalized route (e.g. `POST /instances/{id}/upgrade`) and `success`/`failure`. |
| `neo4j_operator_aura_api_request_duration_seconds` | histogram | `operation` | Latency of Aura API requests, by route. |

The `operation` label is a route **template** (resource IDs collapsed to
`{id}`), so cardinality stays bounded regardless of how many instances you
manage. `result=failure` counts any non-2xx response, including the 404s that
idempotent deletes treat as success.
