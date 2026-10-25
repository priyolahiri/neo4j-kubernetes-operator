# AuraInvite API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-08-01.** See [Verification status](../user_guide/aura_orchestration.md#verification-status).

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
| `role` | `string` | **Required.** Enum `organization-owner` / `organization-admin` / `organization-member` for an org invite (`projectId` must be unset), or `namespace-admin` / `namespace-member` / `namespace-viewer` / `namespace-metrics-integration-reader` for a project-scoped invite (`projectId` required). Enforced by CEL. The Aura invite body spells project roles `namespace-*` even though the project-members endpoint spells the same concepts `project-*`. |
| `organizationRole` | `string` | Enum `organization-owner` / `organization-admin` / `organization-member`. Grants the organization-level role on acceptance. **Required when `role` is a `namespace-*` role** — Aura demands an organization role on *every* invite (an empty role list is rejected), so a project-only invite is impossible. Enforced by CEL. |
| `deletionPolicy` | `string` | Enum `Delete` (default; revoke a still-pending invite) / `Orphan` (leave it). |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`Delete`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `inviteId` | `string` | Aura-assigned invite ID. |
| `phase` | `string` | `Sent` (invite active, awaiting acceptance), `Accepted`, `Revoked`, `Expired`, `Declined` (mirroring the Aura invite status), `Gone` (the invite is no longer listed at all — accepted, declined, revoked or expired, and Aura does not say which), `Pending` (no matching invite exists and `managementPolicies` does not permit creating one), or `Error`. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the operator last recorded the invite as observed from the Aura API: written with every change to the status, and at least every 15 minutes while nothing changes — not on every poll, since each status write starts another reconcile. |

## Deletion

`deletionPolicy: Delete` revokes the invite in Aura if it is still pending, then releases the finalizer. `Orphan`, or an invite the operator never recorded an ID for, calls nothing: it needs no credentials and the CR is removed at once (an `AuraInviteOrphaned` event names an invite left pending).

Revoking needs the credentials and the organization. While they cannot be resolved — the credentials Secret or provider config deleted first in a namespace teardown, say — the CR waits with phase `Error`, `Ready=False`, reason `CredentialsUnavailable`, announced once. After 5 minutes it is removed anyway with a Warning `AuraInviteOrphaned`: the invite stays pending in Aura and can still be accepted, so revoke it in the console. Before, the finalizer was released at once and without a word. A delete Aura refuses — anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last — is waited on the same way: `Ready=False`, reason `DeletionRefused`, announced once, asked again until 5 minutes after deletion was requested, then the CR is removed with the same Warning.

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
  role: organization-member
```

## Related Resources

- [`AuraOrganizationMember`](auraorganizationmember.md) / [`AuraProjectMember`](auraprojectmember.md) — manage an existing member's role
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
