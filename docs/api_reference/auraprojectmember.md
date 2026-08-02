# AuraProjectMember API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-08-01** — request shapes and the role enum confirmed against the API's own validation errors. See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraProjectMember` CRD manages the **project-level role** of an Aura *console* user (identified by email). This is Aura **platform** identity — **not** an in-database Neo4j user.

If the email is already an **organization** member but not yet in this project, the operator adds them directly (requires `Create` in `managementPolicies`). Only a wholly unknown email needs an [`AuraInvite`](aurainvite.md) first.

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
| `role` | `string` | **Required.** Enum `project-admin` / `project-member` / `project-viewer` / `project-metrics-integration-reader` (the Aura API's own `project_roles` values). |
| `deletionPolicy` | `string` | Enum `Orphan` (default; leave access untouched) / `Delete` (remove from the project). |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`Update`/`Delete`/`*`. Default `["*"]`. `Create` permits adding an existing **organization** member to this project (the Aura API takes their user UUID); a wholly unknown email still needs an [`AuraInvite`](aurainvite.md). |

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
  role: project-metrics-integration-reader
```

## Related Resources

- [`AuraInvite`](aurainvite.md) — invite a new user
- [`AuraOrganizationMember`](auraorganizationmember.md) — organization-level role
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
