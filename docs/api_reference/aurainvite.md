# AuraInvite API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).

The `AuraInvite` CRD invites a user (by email) to a Neo4j Aura **organization** — or a **project** within it — with a role. This is how a new person is granted Aura **console** access; it is **not** an in-database Neo4j user. An existing member's role is managed with [`AuraOrganizationMember`](auraorganizationmember.md) / [`AuraProjectMember`](auraprojectmember.md).

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraInvite`
- **Scope**: Namespaced
- **Short name**: `aurainvite`
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef`.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`) in the same namespace. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account credentials. **Mutually exclusive with `providerConfigRef`.** |
| `organizationId` | `string` | The Aura organization to invite into. Falls back to the provider config's `defaultOrganizationId`. |
| `projectId` | `string` | Optionally scopes the invite to a project (a project-member invite). Omit for an org-level invite. |
| `email` | `string` | **Required.** The invitee's email address. |
| `role` | `string` | **Required.** An `ORG_*` role for an org invite, or a `PROJECT_*`/`METRICS_READER` role for a project-scoped invite (`projectId` set). |
| `deletionPolicy` | `string` | Enum `Delete` (default; revoke a still-pending invite) / `Orphan` (leave it). |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`Delete`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `inviteId` | `string` | Aura-assigned invite ID. |
| `phase` | `string` | `Pending`, `Sent`, `Accepted`, `Error`. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the invite was last observed from the Aura API. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraInvite
metadata:
  name: invite-carol
  namespace: neo4j
spec:
  providerConfigRef: { name: aura }
  organizationId: "<org-id>"
  email: carol@example.com
  role: ORG_MEMBER
```

## Related Resources

- [`AuraOrganizationMember`](auraorganizationmember.md) / [`AuraProjectMember`](auraprojectmember.md) — manage an existing member's role
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
