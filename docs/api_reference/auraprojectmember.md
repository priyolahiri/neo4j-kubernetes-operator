# AuraProjectMember API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).

The `AuraProjectMember` CRD manages the **project-level role** of an existing Aura *console* user (identified by email). This is Aura **platform** identity — **not** an in-database Neo4j user. To bring a new person in, create an [`AuraInvite`](aurainvite.md).

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraProjectMember`
- **Scope**: Namespaced
- **Short name**: `auraprojmember`
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef`.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`) in the same namespace. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account credentials. **Mutually exclusive with `providerConfigRef`.** |
| `organizationId` | `string` | The Aura organization. Falls back to the provider config's `defaultOrganizationId`. |
| `projectId` | `string` | The Aura project (API `tenant_id`). Falls back to the provider config's `defaultProjectId`. |
| `email` | `string` | **Required.** Identifies the project member whose role is managed. |
| `role` | `string` | **Required.** Enum `PROJECT_ADMIN` / `PROJECT_MEMBER` / `PROJECT_VIEWER` / `METRICS_READER`. |
| `deletionPolicy` | `string` | Enum `Orphan` (default; leave access untouched) / `Delete` (remove from the project). |
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
kind: AuraProjectMember
metadata:
  name: bob-metrics-reader
  namespace: neo4j
spec:
  providerConfigRef: { name: aura }
  organizationId: "<org-id>"
  projectId: "<project-id>"
  email: bob@example.com
  role: METRICS_READER
```

## Related Resources

- [`AuraInvite`](aurainvite.md) — invite a new user
- [`AuraOrganizationMember`](auraorganizationmember.md) — organization-level role
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
