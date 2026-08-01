# AuraInstance API Reference

> **✅ Live-verified 2026-08-01** — the full lifecycle (create → snapshot → restore → resize → pause → resume → tier upgrade → delete) was walked against a real Aura project. See [Verification status](../user_guide/aura_orchestration.md#verification-status).

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
| `organizationId` | `string` | Aura organization. **Immutable once set.** Needed only by the v2beta1 code paths (`multiDatabase` creation and the multi-database status probe); plain v1 management does not use it. Falls back to the provider config's `defaultOrganizationId`. |
| `cloudProvider` | `string` | Enum `aws` / `gcp` / `azure`. **Required. Immutable.** |
| `region` | `string` | Instance region, e.g. `europe-west1`. Valid values are per-project. **Required. Immutable.** |
| `type` | `string` | Enum `free-db` / `professional-db` / `business-critical` / `enterprise-db` / `professional-ds` / `enterprise-ds`. `enterprise-db` = Virtual Dedicated Cloud (VDC). **Required. Immutable** except the in-place `professional-db` → `business-critical` upgrade. |
| `version` | `string` | Coarse Aura Neo4j major version (e.g. `"5"`). **Required. Immutable.** |
| `memory` | `string` | Instance memory, e.g. `4GB`. Mutable — drives online resize. Required except for `free-db` (size fixed by tier). |
| `storage` | `string` | Instance storage, e.g. `8GB`. **In practice Aura derives storage from `memory`** — resizing memory auto-scales storage (e.g. 1GB→2GB memory takes storage 2GB→4GB), and a memory/storage pair the tier does not offer is rejected outright (`invalid-memory-size`, naming both values). **Prefer leaving this unset**: a value that disagrees with the tier makes the operator retry a permanently failing update. Not configurable for `free-db`. |
| `name` | `string` | Aura instance name (max 30 chars). Defaults to `metadata.name`. |
| `paused` | `bool` | Desired paused state — drives pause / resume. |
| `vectorOptimized` | `*bool` | Enables vector optimization. |
| `graphAnalyticsPlugin` | `*bool` | Enables the graph-analytics plugin. |
| `secondariesCount` | `*int32` | Number of secondaries. `enterprise-db` (VDC) only. |
| `cdcEnrichmentMode` | `string` | Enum `OFF` / `DIFF` / `FULL`. VDC / `business-critical` only. |
| `customerManagedKeyId` | `string` | Aura-assigned CMK ID (from an [`AuraCustomerManagedKey`](auracustomermanagedkey.md) status). `enterprise-db` / `enterprise-ds` only. **Immutable once set.** |
| `multiDatabase` | `*bool` | Requests a **multi-database** instance — the only kind that can host more than the one database Aura creates with it, and therefore the only kind [`AuraDatabase`](auradatabase.md), [`AuraDatabaseBackup`](auradatabasebackup.md) and [`AuraDatabaseRestore`](auradatabaserestore.md) can target. **Immutable**: Aura fixes it at creation and publishes no way to convert an existing instance. Only `true` changes anything, and only on `business-critical` or `enterprise-db` — Aura refuses it on every other tier. See [Multi-database instances](#multi-database-instances). |
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
| `connectionUrl` | `string` | Bolt routing URL (`neo4j+s://…`). Aura returns this as **null while an instance is resuming**, so the operator keeps the last known value rather than blanking it — a pause/resume cycle will not clear this field. |
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
| `multiDatabase` | `*bool` | Whether the instance can host more than one database. **Unset means unknown, not false** — the flag exists only on the Aura v2beta1 instance detail, which fails for instances created through v1, so the answer is not always knowable. Probed once (it can never change) and cached. |
| `defaultDatabaseId` | `string` | The database Aura creates together with a multi-database instance. Reported only by the v2beta1 create response, so populated only for instances this operator created with `multiDatabase` set. |

## Immutability

The following fields are immutable and enforced declaratively by the apiserver via CEL transition rules — there is **no admission webhook** (project Invariant 1):

- `cloudProvider`, `region`, `version`
- `type` — **except** the one in-place `professional-db` → `business-critical` upgrade
- `projectId`, `organizationId`, `customerManagedKeyId`, `instanceId` — immutable once set
- `source`, `multiDatabase`

`type`/`region`/`memory`/`version` combinations are additionally validated against the live per-project `instance_configurations` inline in the reconciler (the one check CEL cannot express).

## Multi-database instances

An Aura instance either can hold several databases or it cannot, and that is decided when the instance is created — Aura publishes no API to convert one. Set `spec.multiDatabase: true` to get one.

This has consequences worth knowing before you rely on it:

- **It changes which API creates the instance.** `multi_database` exists only in the Aura **v2beta1** API, so the operator issues the create there and then manages the instance through v1 as usual (observe, resize, pause/resume, upgrade, delete all work against a v2beta1-created instance). v2beta1 is **beta** — see the caveat in [Aura orchestration](../user_guide/aura_orchestration.md).
- **Only two tiers support it**: `business-critical` and `enterprise-db` (Virtual Dedicated Cloud). `free-db` and `professional-db` are rejected by Aura outright (`multi-database-tier-not-supported`), so the CRD rejects them on write.
- **It needs an organization ID**, because the v2beta1 paths are organization-scoped. Set `spec.organizationId` or `defaultOrganizationId` on the [`AuraProviderConfig`](auraproviderconfig.md).
- **A smaller set of fields applies.** The v2beta1 create accepts only name, type, cloudProvider, region and memory, and *silently ignores* anything else. So `storage`, `vectorOptimized`, `graphAnalyticsPlugin`, `secondariesCount`, `cdcEnrichmentMode`, `customerManagedKeyId` and `source` are **rejected** in combination with `multiDatabase` rather than quietly dropped. `version` is not sent either — Aura picks the version — although the CRD still requires it.
- **Instances created by earlier operator versions are not multi-database**, and cannot be made so. An `AuraDatabase` against one is refused with `Ready=False`, reason `InstanceNotMultiDatabase`; the fix is a new `AuraInstance` (and a data migration), not a spec edit.

```yaml
spec:
  providerConfigRef:
    name: aura-account
  organizationId: 6f2e…            # required for the v2beta1 create
  cloudProvider: gcp
  region: europe-west1
  type: business-critical
  version: "5"
  memory: 2GB
  multiDatabase: true
```

`status.atProvider.multiDatabase` reports the verdict. An **absent** value means unknown, not false: the operator probes v2beta1 once per instance, and that probe cannot succeed for instances created through v1.

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
