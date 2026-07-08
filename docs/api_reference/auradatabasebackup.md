# AuraDatabaseBackup API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).

The `AuraDatabaseBackup` CRD takes an **on-demand** per-database backup on a managed Neo4j Aura instance (per-database backups exist only on multi-database tiers). Like [`AuraSnapshot`](aurasnapshot.md), a backup is one-shot and is **not** deleted from Aura when the CR is removed.

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
| `phase` | `string` | `Pending`, `Completed`, `Error`. |
| `timestamp` | `string` | Backup timestamp as reported by Aura. |
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
