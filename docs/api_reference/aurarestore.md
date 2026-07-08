# AuraRestore API Reference

The `AuraRestore` Custom Resource Definition (CRD) restores an [`AuraInstance`](aurainstance.md) in place from one of its snapshots — the Aura equivalent of `Neo4jRestore`. It is a one-shot action recorded as an auditable object: on completion the CR stays as history and does not re-fire.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraRestore`
- **Scope**: Namespaced
- **Short name**: `aurarestore`
- **Purpose**: One-shot in-place restore of an `AuraInstance` from a snapshot.
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

API credentials are resolved from the referenced `AuraInstance`'s provider config. All spec fields are immutable.

## Spec

Set **exactly one** of `snapshotId` or `snapshotRef`.

| Field | Type | Description |
|---|---|---|
| `instanceRef` | `string` | **Required. Immutable.** The target `AuraInstance` (same namespace), restored in place. |
| `snapshotId` | `string` | Aura snapshot ID to restore from. **Immutable. Mutually exclusive with `snapshotRef`.** |
| `snapshotRef` | `string` | Resolves the snapshot ID from an [`AuraSnapshot`](aurasnapshot.md) CR in the same namespace. **Immutable. Mutually exclusive with `snapshotId`.** |

## Status

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | Restore phase: `Pending`, `Restoring`, `Completed`, `Failed`. |
| `snapshotId` | `string` | Snapshot ID actually restored (resolved from `snapshotRef` when used). |
| `startTime` | `*metav1.Time` | When the restore was issued to Aura. |
| `completionTime` | `*metav1.Time` | When the instance returned to `Running` (or failed). |
| `message` | `string` | Latest human-readable detail. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraRestore
metadata:
  name: analytics-prod-restore
  namespace: neo4j
spec:
  instanceRef: analytics-prod
  snapshotRef: analytics-prod-adhoc   # or set snapshotId directly
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — The instance being restored
- [`AuraSnapshot`](aurasnapshot.md) — The snapshot source (`snapshotRef`)
- [`AuraProviderConfig`](auraproviderconfig.md) — Credentials + account defaults
- [`AuraCustomerManagedKey`](auracustomermanagedkey.md) — Register a customer-managed encryption key
- [`AuraIPFilter`](auraipfilter.md) — Manage a network IP filter (BETA)
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
