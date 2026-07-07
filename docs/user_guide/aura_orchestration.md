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
- **Snapshot:** create an `AuraSnapshot` referencing the instance.
- **Restore:** create an `AuraRestore` with `instanceRef` + `snapshotId` (or
  `snapshotRef` to an `AuraSnapshot`). Restores are in place and one-shot.

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
- **Immutable placement.** `cloudProvider`, `region`, `type`, and `version`
  cannot be changed after creation (enforced by the apiserver). Changing them
  requires a new instance.
