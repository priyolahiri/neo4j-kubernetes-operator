# AuraSnapshot API Reference

> **✅ Live-verified 2026-08-01.** See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraSnapshot` Custom Resource Definition (CRD) takes an on-demand snapshot of an [`AuraInstance`](aurainstance.md) — the Aura equivalent of `Neo4jBackup`. Restore is a separate [`AuraRestore`](aurarestore.md) CR.

Three behaviours confirmed against the live API that are easy to misread:

- **`status.profile` is `AdHoc` for snapshots this CRD takes**, and `Scheduled` for the automatic ones Aura makes on its own. Listing an instance's snapshots shows both.
- **`status.exportable` means "exportable *now*"** — it is `false` while the snapshot runs and flips to `true` on completion. It is not a property of the finished snapshot decided up front.
- **Aura starts a scheduled snapshot immediately after an instance is created**, and it allows only one snapshot at a time. An `AuraSnapshot` created in that window is legitimately refused with `snapshot-not-allowed`; the operator retries, so it resolves itself.

The create call returns only the snapshot ID — no status — so a fresh CR reports `phase: Pending` until the first poll reads the real state.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraSnapshot`
- **Scope**: Namespaced
- **Short name**: `aurasnap`
- **Purpose**: Request an on-demand snapshot of an `AuraInstance`.
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

API credentials are resolved from the referenced `AuraInstance`'s provider config — an `AuraSnapshot` does not carry its own credentials.

## Spec

| Field | Type | Description |
|---|---|---|
| `instanceRef` | `string` | **Required. Immutable.** The `AuraInstance` (same namespace) to snapshot. |

## Status

| Field | Type | Description |
|---|---|---|
| `snapshotId` | `string` | Aura snapshot ID (set once the snapshot is requested). |
| `profile` | `string` | Snapshot profile: `AdHoc` or `Scheduled`. |
| `phase` | `string` | Maps the Aura snapshot status: `Pending`, `InProgress`, `Completed`, `Failed`. |
| `exportable` | `bool` | Whether the snapshot can seed a new instance. |
| `snapshotTime` | `*metav1.Time` | The snapshot's timestamp as reported by Aura. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |

## Deletion note

The Aura API **cannot DELETE snapshots**. Deleting this CR releases its finalizer and drops it from cluster state; the Aura snapshot itself persists in your account.

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraSnapshot
metadata:
  name: analytics-prod-adhoc
  namespace: neo4j
spec:
  instanceRef: analytics-prod
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — The instance being snapshotted
- [`AuraRestore`](aurarestore.md) — In-place restore from a snapshot
- [`AuraProviderConfig`](auraproviderconfig.md) — Credentials + account defaults
- [`AuraCustomerManagedKey`](auracustomermanagedkey.md) — Register a customer-managed encryption key
- [`AuraIPFilter`](auraipfilter.md) — Manage a network IP filter (BETA)
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
