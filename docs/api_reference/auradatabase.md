# AuraDatabase API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (an unstable beta — breaking changes allowed without a version bump). The database create body is not fully schema'd upstream and is mirrored from the response shape. See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-07-31** — create → backup → restore → delete walked on a real multi-database instance. See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraDatabase` CRD manages a database on a managed Neo4j Aura instance. Aura manages replication/topology per tier, so there is **no topology knob** — for databases on a self-managed cluster, use [`Neo4jDatabase`](neo4jdatabase.md) instead.

> **Requires a multi-database instance.** An Aura instance can only hold more than its own built-in database if it was created as multi-database, which is fixed at creation and cannot be changed afterwards. Create the target with [`AuraInstance`](aurainstance.md#multi-database-instances) `spec.multiDatabase: true`. Against any other instance — including every instance created by operator versions before this field existed — the API refuses with *"Only multi database Instances can add databases"* and the CR reports `Ready=False`, reason `InstanceNotMultiDatabase`. That is terminal: the operator stops retrying, because no retry can succeed. The fix is a new instance and a data migration, not a spec edit.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `AuraDatabase`
- **Scope**: Namespaced
- **Short name**: `auradb`
- **Guide**: [Aura Orchestration Guide](../user_guide/aura_orchestration.md)

## Spec

| Field | Type | Description |
|---|---|---|
| `instanceRef` | `string` | **Required.** The [`AuraInstance`](aurainstance.md) (same namespace) that hosts the database. Credentials, organization, and project are resolved from it. |
| `name` | `string` | Database name in Aura (max 63; defaults to `metadata.name`). |
| `organizationId` | `string` | Overrides the organization resolved from the instance's `AuraProviderConfig`. |
| `deletionPolicy` | `string` | Enum `Delete` (default; drop the database) / `Orphan` (leave it in place). |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`Update`/`Delete`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `databaseId` | `string` | Aura-assigned database ID. |
| `phase` | `string` | `Pending`, `Ready`, `Error`. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the database was last observed from the Aura API. |

## Example

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: AuraDatabase
metadata:
  name: analytics-db
  namespace: neo4j
spec:
  instanceRef: analytics
  name: analytics
  deletionPolicy: Delete
```

## Related Resources

- [`AuraInstance`](aurainstance.md) — the instance that hosts the database
- [`AuraDatabaseBackup`](auradatabasebackup.md) / [`AuraDatabaseRestore`](auradatabaserestore.md) — per-database backup/restore
- [Aura Orchestration Guide](../user_guide/aura_orchestration.md)
