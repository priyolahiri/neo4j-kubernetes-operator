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
| `AuraDatabase` | Manages a database on an Aura instance via the Aura API **v2beta1** (beta). |
| `AuraDatabaseBackup` / `AuraDatabaseRestore` | On-demand per-database backup / in-place restore, **v2beta1** (beta). |
| `AuraOrganizationMember` / `AuraProjectMember` | Manage an Aura console user's org / project role (console-RBAC), **v2beta1** (beta). |
| `AuraInvite` | Invites a user to an Aura organization or project (console-RBAC), **v2beta1** (beta). |

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

## End-to-end walkthrough

A full "day in the life": credentials → instance → database → backup → invite a
teammate → clean up. Everything is in one namespace (`neo4j`). Later steps use
CRDs on the Aura API **v2beta1** (beta) — see the notes in each section below.

**1. Store your Aura API credentials** (from the console: Account → API keys):

```bash
kubectl -n neo4j create secret generic aura-api-creds \
  --from-literal=clientId=<CLIENT_ID> \
  --from-literal=clientSecret=<CLIENT_SECRET>
```

**2. Declare the account defaults once** with an `AuraProviderConfig`, then
provision an instance:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraProviderConfig
metadata: { name: aura, namespace: neo4j }
spec:
  credentialsSecretRef: { name: aura-api-creds }
  defaultProjectId: "<project-id>"
  defaultOrganizationId: "<org-id>"     # needed for the v2beta1 CRDs below
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInstance
metadata: { name: analytics, namespace: neo4j }
spec:
  providerConfigRef: { name: aura }
  cloudProvider: gcp
  region: europe-west1
  type: business-critical               # a multi-database tier (needed for step 4)
  version: "5"
  memory: 2GB
  connectionSecretName: analytics-conn
```

**3. Wait for it to come up**, then point your app at the connection Secret:

```bash
kubectl -n neo4j wait aurainstance/analytics --for=condition=Ready --timeout=20m
# app: envFrom the analytics-conn Secret (NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD)
```

**4. Create a database** on the instance (multi-database tiers only):

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabase
metadata: { name: analytics-db, namespace: neo4j }
spec:
  instanceRef: analytics
  name: analytics
```

**5. Back it up** on demand:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabaseBackup
metadata: { name: analytics-nightly, namespace: neo4j }
spec:
  databaseRef: analytics-db
```

**6. Give a teammate read-only metrics access** to the project (console-RBAC —
this is Aura console access, not an in-database user):

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInvite
metadata: { name: invite-bob, namespace: neo4j }
spec:
  providerConfigRef: { name: aura }
  projectId: "<project-id>"
  email: bob@example.com
  role: METRICS_READER
```

Once Bob accepts, manage his role going forward with an `AuraProjectMember`
(`email: bob@example.com`, `role: METRICS_READER`).

**7. Check status** at any point:

```bash
kubectl -n neo4j get auradatabase,auradatabasebackup,aurainvite
kubectl -n neo4j describe auradatabase analytics-db
```

**8. Clean up.** Deletion honours each CR's `deletionPolicy` — e.g. `AuraDatabase`
defaults to `Delete` (drops the DB), `AuraInvite` to `Delete` (revokes a pending
invite). Delete the instance last:

```bash
kubectl -n neo4j delete auradatabase analytics-db aurainvite invite-bob
kubectl -n neo4j delete aurainstance analytics    # deletionPolicy governs the cloud instance
```

Each step is detailed in the sections that follow.

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

## Managing databases and access on an Aura instance

Aura resources are managed through **Aura-native, API-driven CRDs** — not by
pointing the self-managed `Neo4jDatabase`/`Neo4jUser`/`Neo4jRole`/`Neo4jRoleBinding`
CRDs at an instance. Those CRDs are for self-managed clusters/standalone only
(via `clusterRef`).

!!! warning "Beta / best-effort"
    The CRDs in this section use the Aura API **v2beta1**, an unstable beta whose
    contract can change without a version bump. Some request bodies are not fully
    documented upstream and are best-effort. Validate against your account before
    relying on them in production. See `docs/design/aura-orchestration.md`.

### Databases

Model a database on an instance with `AuraDatabase`. It references the
`AuraInstance` (same namespace) and derives credentials, organization, and
project from it. Aura manages replication/topology per tier, so there is **no
topology knob** (unlike `Neo4jDatabase`):

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabase
metadata: { name: analytics-db }
spec:
  instanceRef: analytics        # the AuraInstance, same namespace
  name: analytics
  deletionPolicy: Delete        # Delete drops the DB on CR delete; Orphan leaves it
```

Additional databases require a multi-database-capable Aura tier (Business
Critical / dedicated). Full field reference: [`AuraDatabase`](../api_reference/auradatabase.md).

#### Per-database backup & restore

Aura exposes per-database backups (multi-database tiers). Take an on-demand
backup and restore in place from it:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabaseBackup
metadata: { name: analytics-nightly }
spec:
  databaseRef: analytics-db     # the AuraDatabase, same namespace
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabaseRestore
metadata: { name: analytics-rollback }
spec:
  databaseRef: analytics-db
  backupRef: analytics-nightly  # or: backupId: "<aura-backup-id>"
```

Like `AuraSnapshot`, a backup is one-shot and is **not** deleted from Aura when
the CR is removed; a restore is one-shot and in place.

### Access (console-RBAC)

In-database Neo4j users/roles are **not** managed by the operator on Aura — there
is no Aura API for them. Aura governs access through **console-RBAC**:
organization/project membership and email invites (plus service accounts and
"tool authentication with Aura user", which maps a console identity to a
predefined database role). Model it with three CRDs:

```yaml
# Invite a new person to the organization (grants console access on acceptance).
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInvite
metadata: { name: invite-carol }
spec:
  providerConfigRef: { name: aura }
  organizationId: "<org-id>"    # or defaultOrganizationId on the provider config
  email: carol@example.com
  role: ORG_MEMBER              # ORG_* ; add projectId + a PROJECT_*/METRICS_READER role for a project invite
---
# Manage the org role of an EXISTING console user (by email).
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraOrganizationMember
metadata: { name: alice-org-admin }
spec:
  providerConfigRef: { name: aura }
  organizationId: "<org-id>"
  email: alice@example.com
  role: ORG_ADMIN               # ORG_OWNER | ORG_ADMIN | ORG_MEMBER
---
# Manage a user's PROJECT role (e.g. read-only metrics access).
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraProjectMember
metadata: { name: bob-metrics }
spec:
  providerConfigRef: { name: aura }
  organizationId: "<org-id>"
  projectId: "<project-id>"
  email: bob@example.com
  role: METRICS_READER          # PROJECT_ADMIN | PROJECT_MEMBER | PROJECT_VIEWER | METRICS_READER
```

`AuraOrganizationMember` / `AuraProjectMember` reconcile the role of a user who
is **already** a member — if the email isn't a member yet, the CR reports a
`NotAMember` condition; bring them in with an `AuraInvite`. Field references:
[`AuraInvite`](../api_reference/aurainvite.md),
[`AuraOrganizationMember`](../api_reference/auraorganizationmember.md),
[`AuraProjectMember`](../api_reference/auraprojectmember.md).

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
