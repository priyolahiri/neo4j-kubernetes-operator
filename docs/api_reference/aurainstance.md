# AuraInstance API Reference

The `AuraInstance` Custom Resource Definition (CRD) declaratively manages a Neo4j Aura cloud instance via the Aura REST API — provision, resize, pause/resume, upgrade, and (optionally) delete. It is distinct from `spec.auraFleetManagement`, which registers a self-managed cluster into the Aura console.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraInstance`
- **Scope**: Namespaced
- **Short name**: `aurainst`
- **Purpose**: Declaratively create, resize, pause/resume, upgrade, and delete a Neo4j Aura cloud instance.
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef` for API access.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`, a core `LocalObjectReference`) in the same namespace, supplying credentials, defaults, and the shared rate limiter. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account shortcut when no `AuraProviderConfig` is used. See [AuraCredentialsSecretRef](auraproviderconfig.md#auracredentialssecretref). **Mutually exclusive with `providerConfigRef`.** |
| `projectId` | `string` | Aura project (the API `tenant_id`). **Immutable.** Falls back to the provider config's `defaultProjectId` when empty. |
| `cloudProvider` | `string` | Enum `aws` / `gcp` / `azure`. **Required. Immutable.** |
| `region` | `string` | Instance region, e.g. `europe-west1`. Valid values are per-project. **Required. Immutable.** |
| `type` | `string` | Enum `free-db` / `professional-db` / `business-critical` / `enterprise-db` / `professional-ds` / `enterprise-ds`. `enterprise-db` = Virtual Dedicated Cloud (VDC). **Required. Immutable** except the in-place `professional-db` → `business-critical` upgrade. |
| `version` | `string` | Coarse Aura Neo4j major version (e.g. `"5"`). **Required. Immutable.** |
| `memory` | `string` | Instance memory, e.g. `4GB`. Mutable — drives online resize. Required except for `free-db` (size fixed by tier). |
| `storage` | `string` | Instance storage, e.g. `8GB`. Mutable. Not configurable for `free-db`. |
| `name` | `string` | Aura instance name (max 30 chars). Defaults to `metadata.name`. |
| `paused` | `bool` | Desired paused state — drives pause / resume. |
| `vectorOptimized` | `*bool` | Enables vector optimization. |
| `graphAnalyticsPlugin` | `*bool` | Enables the graph-analytics plugin. |
| `secondariesCount` | `*int32` | Number of secondaries. `enterprise-db` (VDC) only. |
| `cdcEnrichmentMode` | `string` | Enum `OFF` / `DIFF` / `FULL`. VDC / `business-critical` only. |
| `customerManagedKeyId` | `string` | Aura-assigned CMK ID (from an [`AuraCustomerManagedKey`](auracustomermanagedkey.md) status). `enterprise-db` / `enterprise-ds` only. **Immutable once set.** |
| `source` | `object` | Clone a new instance from an existing one at create time. **Immutable.** See [AuraInstanceSource](#aurainstancesource). |
| `instanceId` | `string` | Adopt/import an existing Aura instance by ID rather than creating one. **Immutable once set.** |
| `connectionSecretName` | `string` | Secret the operator writes connection details (URI + one-time credentials) to. Defaults to `<name>-conn`. |
| `connectionSecretFormat` | `string` | Enum `neo4j-driver` / `aura-dotenv` / `jdbc` / `servicebinding` / `custom`. Default `neo4j-driver`. Selects the connection Secret's key layout. |
| `publishConnectionDetailsTo` | `string` | Name of a ConfigMap to receive the non-secret endpoint details (URI, instanceId, region, type). Credentials stay in the connection Secret. |
| `deletionPolicy` | `string` | Enum `Orphan` / `Delete`. Default `Orphan` (keep the running instance on CR delete). |
| `deletionProtection` | `bool` | Blocks deletion of the cloud instance even when `deletionPolicy: Delete`, until cleared. |
| `managementPolicies` | `[]string` | Items enum `Observe` / `Create` / `Update` / `Delete` / `*`. Default `["*"]` (full management). Use a subset to observe only, never delete, etc. |

### AuraInstanceSource

Clones a new instance from an existing one at create time (immutable). Exactly one of `instanceRef` / `instanceId` identifies the source.

| Field | Type | Description |
|---|---|---|
| `instanceRef` | `string` | Another `AuraInstance` (same namespace) to clone from. |
| `instanceId` | `string` | Aura source instance ID (alternative to `instanceRef`). |
| `snapshotId` | `string` | Snapshot to clone from; requires an exportable snapshot of the source. |

## Status

| Field | Type | Description |
|---|---|---|
| `instanceId` | `string` | Aura instance ID (mirror of the external-name annotation). |
| `phase` | `string` | Mirrors the Aura instance status (`Creating`, `Running`, `Pausing`, `Paused`, `Resuming`, `Updating`, `Restoring`, `Destroying`, …). |
| `connectionUrl` | `string` | Bolt routing URL (`neo4j+s://…`). |
| `metricsIntegrationUrl` | `string` | Prometheus scrape endpoint, when available. |
| `binding` | `object` | Service Binding "Provisioned Service" pointer (`{name}` — the connection Secret). |
| `conditions` | `[]metav1.Condition` | `Ready` (instance usable) and `Synced` (operator reconciled spec). |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the instance was last observed from the Aura API. |
| `atProvider` | `object` | Full instance state last observed from the Aura API. See [AuraInstanceObservation](#aurainstanceobservation). |

### AuraInstanceObservation

Mirrors the instance state last observed from the Aura API — the source of truth for drift detection and reporting.

| Field | Type | Description |
|---|---|---|
| `status` | `string` | Observed Aura status. |
| `memory` | `string` | Observed memory. |
| `storage` | `string` | Observed storage. |
| `type` | `string` | Observed instance type. |
| `region` | `string` | Observed region. |
| `cloudProvider` | `string` | Observed cloud provider. |
| `name` | `string` | Observed instance name. |

## Immutability

The following fields are immutable and enforced declaratively by the apiserver via CEL transition rules — there is **no admission webhook** (project Invariant 1):

- `cloudProvider`, `region`, `version`
- `type` — **except** the one in-place `professional-db` → `business-critical` upgrade
- `projectId`, `customerManagedKeyId`, `instanceId` — immutable once set
- `source`

`type`/`region`/`memory`/`version` combinations are additionally validated against the live per-project `instance_configurations` inline in the reconciler (the one check CEL cannot express).

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInstance
metadata:
  name: analytics-prod
  namespace: neo4j
spec:
  providerConfigRef:
    name: aura-account
  cloudProvider: gcp
  region: europe-west1
  type: professional-db
  version: "5"
  memory: 4GB
  connectionSecretName: analytics-prod-conn
```

## Related Resources

- [`AuraProviderConfig`](auraproviderconfig.md) — Credentials + account defaults
- [`AuraSnapshot`](aurasnapshot.md) — On-demand snapshot of this instance
- [`AuraRestore`](aurarestore.md) — In-place restore from a snapshot
- [`AuraCustomerManagedKey`](auracustomermanagedkey.md) — Register a customer-managed encryption key
- [`AuraIPFilter`](auraipfilter.md) — Manage a network IP filter (BETA)
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
