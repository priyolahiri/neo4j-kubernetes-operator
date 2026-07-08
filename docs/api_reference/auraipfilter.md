# AuraIPFilter API Reference

> **⚠️ BETA / best-effort.** IP filtering is only available on the Aura API **v2beta1** surface, which is an unstable beta (breaking changes are allowed without a version bump). This CRD is best-effort and its behaviour may change to track the API — the contract is reconstructed and unvalidated. See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md) before relying on it in production.

The `AuraIPFilter` Custom Resource Definition (CRD) manages a Neo4j Aura network IP filter (a CIDR allowlist) via the Aura API v2beta1.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraIPFilter`
- **Scope**: Namespaced
- **Short name**: `auraipf`
- **Purpose**: Manage a network IP filter (CIDR allowlist) on an Aura project or instance. **BETA / best-effort.**
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef` for API access.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`, a core `LocalObjectReference`) in the same namespace. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account shortcut when no `AuraProviderConfig` is used. See [AuraCredentialsSecretRef](auraproviderconfig.md#auracredentialssecretref). **Mutually exclusive with `providerConfigRef`.** |
| `organizationId` | `string` | Aura organization ID (v2beta1 hierarchy). Falls back to the provider config's `defaultOrganizationId` when empty. |
| `projectId` | `string` | Aura project (the API `tenant_id`). Falls back to the provider config's `defaultProjectId` when empty. |
| `instanceRef` | `string` | Optionally scopes the filter to a single [`AuraInstance`](aurainstance.md) (same namespace) — Aura permits at most one IP filter per instance. Omit for a project-wide filter. **Immutable once set.** |
| `name` | `string` | Filter display name in Aura (max 63 chars). Defaults to `metadata.name`. |
| `region` | `string` | Region the filter applies to, when required by the provider. |
| `cidrs` | `[]string` | **Required.** Allowlist of source ranges in CIDR notation (e.g. `"203.0.113.0/24"`). MinItems 1. |
| `deletionPolicy` | `string` | Enum `Orphan` / `Delete`. Default `Orphan` (leave the filter in place on CR delete — deleting it would open network access). `Delete` removes it from Aura. |
| `managementPolicies` | `[]string` | Items enum `Observe` / `Create` / `Update` / `Delete` / `*`. Default `["*"]` (full management). |

## Status

| Field | Type | Description |
|---|---|---|
| `filterId` | `string` | Aura-assigned IP-filter ID. |
| `phase` | `string` | Mirrors the Aura filter status: `Pending`, `Ready`, `Updating`, `Error`. |
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
  instanceRef: analytics-prod        # omit for a project-wide filter
  cidrs:
    - "203.0.113.0/24"
    - "198.51.100.42/32"
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — The instance a filter can be scoped to
- [`AuraProviderConfig`](auraproviderconfig.md) — Credentials + account defaults (incl. `defaultOrganizationId`)
- [`AuraSnapshot`](aurasnapshot.md) — On-demand snapshot of an instance
- [`AuraRestore`](aurarestore.md) — In-place restore from a snapshot
- [`AuraCustomerManagedKey`](auracustomermanagedkey.md) — Register a customer-managed encryption key
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
