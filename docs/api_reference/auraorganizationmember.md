# AuraOrganizationMember API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-08-01** — request shapes and the role enum confirmed against the API's own validation errors. See [Verification status](../user_guide/aura_orchestration.md#verification-status).

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
| `lastSyncedTime` | `*metav1.Time` | When the operator last recorded the membership as observed from the Aura API: written with every change to the status, and at least every 15 minutes while nothing changes — not on every poll, since each status write starts another reconcile. |

## Deletion

`deletionPolicy: Delete` removes the member from the organization in Aura, then releases the finalizer. `Orphan` (the default), or a member whose Aura user ID was never recorded, calls nothing: it needs no credentials and the CR is removed at once (an `AuraMemberOrphaned` event names a member who keeps their role).

Removing needs the credentials and the organization. While they cannot be resolved — the credentials Secret or provider config deleted first in a namespace teardown, say — the CR waits with phase `Error`, `Ready=False`, reason `CredentialsUnavailable`, announced once. After 5 minutes it is removed anyway with a Warning `AuraMemberOrphaned`: the person keeps their organization role in Aura, so remove them in the console. Before, the finalizer was released at once and without a word.

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
