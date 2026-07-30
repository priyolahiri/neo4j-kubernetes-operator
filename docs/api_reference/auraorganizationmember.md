# AuraOrganizationMember API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).

The `AuraOrganizationMember` CRD manages the **organization-level role** of an existing Aura *console* user (identified by email). This is Aura **platform** identity — **not** an in-database Neo4j user. To bring a new person in, create an [`AuraInvite`](aurainvite.md).

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraOrganizationMember`
- **Scope**: Namespaced
- **Short name**: `auraorgmember`
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef`.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`) in the same namespace. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account credentials. **Mutually exclusive with `providerConfigRef`.** |
| `organizationId` | `string` | The Aura organization. Falls back to the provider config's `defaultOrganizationId`. |
| `email` | `string` | **Required.** Identifies the org member whose role is managed. |
| `role` | `string` | **Required.** Enum `organization-owner` / `organization-admin` / `organization-member` (the Aura API's own `organization_roles` values). |
| `deletionPolicy` | `string` | Enum `Orphan` (default; leave access untouched) / `Delete` (remove from the org). |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Update`/`Delete`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `userId` | `string` | Aura user ID resolved from the email. |
| `phase` | `string` | `Pending`, `Ready`, `NotAMember`, `Error`. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the membership was last observed from the Aura API. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraOrganizationMember
metadata:
  name: alice-org-admin
  namespace: neo4j
spec:
  providerConfigRef: { name: aura }
  organizationId: "<org-id>"
  email: alice@example.com
  role: organization-admin
```

## Related Resources

- [`AuraInvite`](aurainvite.md) — invite a new user
- [`AuraProjectMember`](auraprojectmember.md) — project-level role
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
