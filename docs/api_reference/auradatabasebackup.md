# AuraDatabaseBackup API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-07-31.** See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraDatabaseBackup` CRD takes an **on-demand** per-database backup on a managed Neo4j Aura instance. Like [`AuraSnapshot`](aurasnapshot.md), a backup is one-shot and is **not** deleted from Aura when the CR is removed.

> **Requires a multi-database instance**, since it backs up an [`AuraDatabase`](auradatabase.md) — see that page's prerequisite. Two behaviours of the Aura API worth knowing, both verified live: a backup does **not appear in Aura's backup list until it completes** (the operator therefore polls it by ID, so `phase` progresses normally — but do not expect to see it in the console listing immediately), and `exportable` means "exportable *now*", flipping from `false` to `true` on completion rather than describing the finished backup up front.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraDatabaseBackup`
- **Scope**: Namespaced
- **Short name**: `auradbbackup`
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

| Field | Type | Description |
|---|---|---|
| `databaseRef` | `string` | **Required.** The [`AuraDatabase`](auradatabase.md) (same namespace) to back up. Credentials, organization, project, and instance are resolved from it. |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`Delete`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `backupId` | `string` | Aura-assigned backup ID. |
| `phase` | `string` | `Pending`, `Completed`, `Failed`, `Error`. Mirrors Aura's own `status` enum (`Pending`/`InProgress`/`Completed`/`Failed`). A freshly-scheduled backup reports `Pending` — the create response carries only an ID, so no status is known until the next read. |
| `timestamp` | `string` | Backup timestamp as reported by Aura. |
| `exportable` | `bool` | Whether Aura can export/download this backup. Only populated once the backup has been read back. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the backup was last observed from the Aura API. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabaseBackup
metadata:
  name: analytics-db-backup
  namespace: neo4j
spec:
  databaseRef: analytics-db
```

## Related Resources

- [`AuraDatabase`](auradatabase.md) — the database backed up
- [`AuraDatabaseRestore`](auradatabaserestore.md) — restore from a backup
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
