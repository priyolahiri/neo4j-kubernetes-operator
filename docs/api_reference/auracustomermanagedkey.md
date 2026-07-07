# AuraCustomerManagedKey API Reference

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
| `deletionPolicy` | `string` | Enum `Orphan` / `Delete`. Default `Orphan` (leave the key registered in Aura on CR delete). `Delete` deregisters it — fails while any instance still uses it. |
| `managementPolicies` | `[]string` | Items enum `Observe` / `Create` / `Update` / `Delete` / `*`. Default `["*"]` (full management). |

## Status

| Field | Type | Description |
|---|---|---|
| `customerManagedKeyId` | `string` | Aura-assigned key ID. Reference this value from an `AuraInstance`'s `spec.customerManagedKeyId`. |
| `phase` | `string` | Mirrors the Aura key status: `Pending`, `Ready`, `Deleting`, `Error`. |
| `conditions` | `[]metav1.Condition` | `Ready` (key usable) and `Synced` (operator reconciled spec). |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the key was last observed from the Aura API. |

## Deletion note

A key still bound to an instance **cannot be deleted** — Aura returns `encryption-key-is-active`. The CR then reports a `KeyInUse` condition and keeps its finalizer until the key is no longer referenced by any instance.

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
