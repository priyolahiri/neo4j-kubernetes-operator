# AuraIPFilter API Reference

> **⚠️ BETA / best-effort.** IP filtering is only available on the Aura API **v2beta1** surface, which is an unstable beta (breaking changes are allowed without a version bump). This CRD is best-effort and its behaviour may change to track the API. The shape below is taken from the **live API**, which differs from the published `IpFilter` schema in several places — see below. See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md) before relying on it in production.
> **✅ Live-verified 2026-08-01 — and the contract was WRONG.** Until then this CRD could not create, update *or* delete a filter; all three are fixed. See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraIPFilter` Custom Resource Definition (CRD) manages a Neo4j Aura network IP filter (an allowlist) via the Aura API v2beta1.

**One filter per project.** Aura rejects a second filter targeting a project that already has one (`ip-filter-already-exists`), so two `AuraIPFilter` CRs cannot both target the same project.

**Filters are organization-scoped.** They live at the organization level and are *applied* to instances/projects, so the CR needs an organization ID — from `spec.organizationId` or the provider config's `defaultOrganizationId`.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraIPFilter`
- **Scope**: Namespaced
- **Short name**: `auraipf`
- **Purpose**: Manage a network IP filter (allowlist) on a Neo4j Aura organization and apply it to one or more instances. **BETA / best-effort.**
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

> **IP filters are organization-scoped.** In v2beta1 a filter lives at `/organizations/{org}/ip-filters` — not under a project or instance. A single filter is *applied* to instances via `filtered_entities.instances`; this CRD models that with `instanceRefs`.

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef` for API access.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`, a core `LocalObjectReference`) in the same namespace. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account shortcut when no `AuraProviderConfig` is used. See [AuraCredentialsSecretRef](auraproviderconfig.md#auracredentialssecretref). **Mutually exclusive with `providerConfigRef`.** |
| `organizationId` | `string` | Aura organization that owns the filter (filters are org-scoped in v2beta1). Falls back to the provider config's `defaultOrganizationId` when empty. |
| `name` | `string` | Filter display name in Aura (max 63 chars). Defaults to `metadata.name`. |
| `description` | `string` | Optional human-friendly description of the filter. |
| `allowList` | `[]object` | **Required.** The allowed source ranges (MinItems 1, MaxItems 1000). Each entry is `{address, prefixLen, description?}` — the v2beta1 API splits CIDR notation into an address plus a prefix length (so `"203.0.113.0/24"` → `address: "203.0.113.0"`, `prefixLen: 24`). |
| `instanceRefs` | `[]string` | Names of [`AuraInstance`](aurainstance.md)s (same namespace) the filter is applied to; the operator resolves each to its Aura instance ID (the API's `filtered_entities.instances`). Set (dedup), MaxItems 100. Omit to attach the filter out of band. |
| `filteringDisabled` | `bool` | Turns the filter off without deleting it (the API's `filtering_disabled`). Default `false` (the filter is enforced). |
| `deletionPolicy` | `string` | Enum `Orphan` / `Delete`. Default `Orphan` (leave the filter in place on CR delete — deleting it would open network access). `Delete` removes it from Aura. |
| `managementPolicies` | `[]string` | Items enum `Observe` / `Create` / `Update` / `Delete` / `*`. Default `["*"]` (full management). |

### `allowList[]` (AuraIPFilterAllowEntry)

| Field | Type | Description |
|---|---|---|
| `address` | `string` | **Required.** The IP address of the CIDR (e.g. `"203.0.113.0"`). |
| `prefixLen` | `int32` | **Required.** The CIDR prefix length (0–32 for IPv4, up to 128 for IPv6). |
| `description` | `string` | Optional human-friendly label for this entry. |

## Status

| Field | Type | Description |
|---|---|---|
| `filterId` | `string` | Aura-assigned IP-filter ID. |
| `phase` | `string` | Mirrors the reconcile outcome: `Pending`, `Ready`, `Error`. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the filter was last observed from the Aura API. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraIPFilter
metadata:
  name: office-allowlist
  namespace: neo4j
spec:
  providerConfigRef:
    name: aura-account
  organizationId: "<your-aura-org-id>"   # or defaultOrganizationId on the provider config
  instanceRefs:                          # instances the filter is applied to
    - analytics-prod
  allowList:
    - address: "203.0.113.0"
      prefixLen: 24
      description: office
    - address: "198.51.100.42"
      prefixLen: 32
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — The instance a filter can be scoped to
- [`AuraProviderConfig`](auraproviderconfig.md) — Credentials + account defaults (incl. `defaultOrganizationId`)
- [`AuraSnapshot`](aurasnapshot.md) — On-demand snapshot of an instance
- [`AuraRestore`](aurarestore.md) — In-place restore from a snapshot
- [`AuraCustomerManagedKey`](auracustomermanagedkey.md) — Register a customer-managed encryption key
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
