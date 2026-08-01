# AuraDatabaseRestore API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (unstable beta). See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-07-31.** See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraDatabaseRestore` CRD performs a **one-shot, in-place** restore of a database on a managed Neo4j Aura instance from one of its per-database backups. Requires a multi-database instance — see [`AuraDatabase`](auradatabase.md).

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraDatabaseRestore`
- **Scope**: Namespaced
- **Short name**: `auradbrestore`
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

Set exactly one of `backupId` or `backupRef`.

| Field | Type | Description |
|---|---|---|
| `databaseRef` | `string` | **Required.** The [`AuraDatabase`](auradatabase.md) (same namespace) to restore in place. |
| `backupId` | `string` | The Aura backup ID to restore from. **Mutually exclusive with `backupRef`.** |
| `backupRef` | `string` | Resolves the backup ID from an [`AuraDatabaseBackup`](auradatabasebackup.md) (same namespace). **Mutually exclusive with `backupId`.** |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | `Restoring` (preparing) → `Submitting` (request in flight) → **`Submitted`** (terminal success), or `Error`. It never becomes `Completed`: Aura runs the restore asynchronously and its v2beta1 database endpoint returns only an `id` with no status, so completion is **not observable** through the API — verify in the Aura console. Once `Submitted`, the CR is not reconciled again; a repeated restore would overwrite the database a second time. A CR left in **`Submitting`** means the operator stopped while the request was in flight and cannot tell whether Aura applied it: it reports `Ready=False`, reason `RestoreOutcomeUnknown`, and deliberately does **not** retry. Check the database in the console, then delete and recreate the CR if the restore still needs to run. |
| `startedAt` | `*metav1.Time` | When the restore was submitted. |
| `finishedAt` | `*metav1.Time` | When the restore reached a terminal state. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabaseRestore
metadata:
  name: analytics-db-rollback
  namespace: neo4j
spec:
  databaseRef: analytics-db
  backupRef: analytics-db-backup
```

## Related Resources

- [`AuraDatabase`](auradatabase.md) — the database restored
- [`AuraDatabaseBackup`](auradatabasebackup.md) — the backup source
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
