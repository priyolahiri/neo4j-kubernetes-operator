# AuraCustomerManagedKey API Reference

> **⛔ UNPROVEN — never exercised against the live Aura API.** Every other Aura
> CRD has been driven end-to-end against a real account; this one has not,
> because it needs a real cloud KMS key (AWS KMS / GCP Cloud KMS / Azure Key
> Vault) with IAM grants to Aura, which cannot be created disposably. Its shape
> comes from the published v1 OpenAPI spec — and on this API, spec-derived
> contracts have repeatedly proven wrong.
> **Verify in a non-production project before relying on it.** See
> [Verification status](../user_guide/aura_orchestration.md#verification-status).
>
> Adoption: the v1 CMK **list** endpoint returns only
> `id`, `name` and `tenant_id`, so adoption narrows on `name` and then confirms
> with a per-key `GET` — a key can never be matched on
> `keyId`/`region`/`cloudProvider` from a list entry.

The `AuraCustomerManagedKey` Custom Resource Definition (CRD) registers a customer-managed encryption key (CMK) with Neo4j Aura for use by dedicated-tier instances. The key material lives in the customer's own cloud KMS; Aura stores only a reference to it.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraCustomerManagedKey`
- **Scope**: Namespaced
- **Short name**: `auracmk`
- **Purpose**: Register a customer-managed encryption key (CMK) for dedicated-tier `AuraInstance` encryption.
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

Once the key reaches status `Ready`, its Aura-assigned ID (`status.customerManagedKeyId`) is referenced from an [`AuraInstance`](aurainstance.md)'s `spec.customerManagedKeyId`. Every placement field is immutable: a CMK is permanently bound to one cloud provider, region, instance type, and KMS key — to change any of them, create a new CMK.

## Spec

Set exactly one of `providerConfigRef` or `credentialsSecretRef` for API access.

| Field | Type | Description |
|---|---|---|
| `providerConfigRef` | `object` | References an [`AuraProviderConfig`](auraproviderconfig.md) (`{name}`, a core `LocalObjectReference`) in the same namespace. **Mutually exclusive with `credentialsSecretRef`.** |
| `credentialsSecretRef` | `object` | Inline single-account shortcut when no `AuraProviderConfig` is used. See [AuraCredentialsSecretRef](auraproviderconfig.md#auracredentialssecretref). **Mutually exclusive with `providerConfigRef`.** |
| `projectId` | `string` | Aura project (the API `tenant_id`) the key is scoped to. **Immutable once set.** Falls back to the provider config's `defaultProjectId` when empty. |
| `name` | `string` | CMK display name in Aura (max 30 chars). Defaults to `metadata.name`. |
| `cloudProvider` | `string` | Enum `aws` / `gcp` / `azure`. Must match the instances that will use it. **Required. Immutable.** |
| `region` | `string` | Region the key applies to, e.g. `europe-west1`. **Required. Immutable.** |
| `instanceType` | `string` | Enum `enterprise-db` / `enterprise-ds` — CMKs are supported for the dedicated tiers only. **Required. Immutable.** |
| `keyId` | `string` | The cloud KMS key resource identifier Aura will use: an AWS KMS key ARN, a GCP KMS key resource name, or an Azure Key Vault key URL. The customer must grant Aura access to this key out of band. **Required. Immutable.** |
| `deletionPolicy` | `string` | Enum `Orphan` / `Delete`. Default `Orphan` (leave the key registered in Aura on CR delete). `Delete` deregisters it — refused while any instance still uses it, which is waited on for 5 minutes (see the deletion note). |
| `managementPolicies` | `[]string` | Items enum `Observe` / `Create` / `Update` / `Delete` / `*`. Default `["*"]` (full management). |

## Status

| Field | Type | Description |
|---|---|---|
| `customerManagedKeyId` | `string` | Aura-assigned key ID. Reference this value from an `AuraInstance`'s `spec.customerManagedKeyId`. |
| `phase` | `string` | Mirrors the Aura key status: `Pending`, `Ready`, `Deleting`, `Error`. `Error` also while the operator cannot reconcile the key — the `Ready` condition says why. |
| `conditions` | `[]metav1.Condition` | `Ready` (key usable) and `Synced` (operator reconciled spec). |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the operator last recorded the key as observed from the Aura API: written with every change to the status, and at least every 15 minutes while nothing changes — not on every poll, since each status write starts another reconcile. |

## Deletion note

A key still bound to an instance **cannot be deleted** — Aura returns `encryption-key-is-active`. The CR then sets `Synced=False` with reason `KeyInUse`, raises one `AuraCustomerManagedKeyDeleteBlocked`, and keeps its finalizer while it asks again — until 5 minutes after deletion was requested (the condition names the time). Then it releases the finalizer with a Warning `AuraCustomerManagedKeyOrphaned`: the key stays registered in Aura, still bound to an instance, so deregister it in the Aura console once nothing uses it. Detach or delete the instances first to have the CR deregister it. Before, a key in use held the CR — and its namespace — for as long as anything used it.

`Orphan` needs no credentials, so the CR is removed even when its credentials Secret or provider config is already gone (a namespace teardown). `Delete` needs them: while they cannot be read the CR waits, with `Ready=False`, reason `CredentialsUnavailable`, and after 5 minutes it is removed anyway with a Warning `AuraCustomerManagedKeyOrphaned` — the key stays registered in Aura. A delete Aura refuses — anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last — is waited on the same way: `Ready=False`, reason `DeletionRefused`, announced once, asked again until 5 minutes after deletion was requested, then the CR is removed with the same Warning.

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraCustomerManagedKey
metadata:
  name: prod-cmk
  namespace: neo4j
spec:
  providerConfigRef:
    name: aura-account
  cloudProvider: aws
  region: eu-west-1
  instanceType: enterprise-db
  keyId: arn:aws:kms:eu-west-1:123456789012:key/abcd1234-...
```

Then reference the resulting key from an instance:

```yaml
spec:
  type: enterprise-db
  customerManagedKeyId: <status.customerManagedKeyId of prod-cmk>
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — Consumes the key via `spec.customerManagedKeyId`
- [`AuraProviderConfig`](auraproviderconfig.md) — Credentials + account defaults
- [`AuraSnapshot`](aurasnapshot.md) — On-demand snapshot of an instance
- [`AuraRestore`](aurarestore.md) — In-place restore from a snapshot
- [`AuraIPFilter`](auraipfilter.md) — Manage a network IP filter (BETA)
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
