# AuraDatabase API Reference

> **⚠️ BETA / best-effort.** Uses the Aura API **v2beta1** (an unstable beta — breaking changes allowed without a version bump). The database create body is not fully schema'd upstream and is mirrored from the response shape. See the [Aura Orchestration Guide](../user_guide/aura_orchestration.md).
> **✅ Live-verified 2026-07-31** — create → backup → restore → delete walked on a real multi-database instance. See [Verification status](../user_guide/aura_orchestration.md#verification-status).

The `AuraDatabase` CRD manages a database on a managed Neo4j Aura instance. Aura manages replication/topology per tier, so there is **no topology knob** — for databases on a self-managed cluster, use [`Neo4jDatabase`](neo4jdatabase.md) instead.

> **Requires a multi-database instance.** An Aura instance can only hold more than its own built-in database if it was created as multi-database, which is fixed at creation and cannot be changed afterwards. Create the target with [`AuraInstance`](aurainstance.md#multi-database-instances) `spec.multiDatabase: true`. Against any other instance the API refuses with *"Only multi database Instances can add databases"* and the CR reports `Ready=False`, reason `InstanceNotMultiDatabase`. That is terminal: the operator stops retrying, because no retry can succeed, and does not ask Aura again while the CR refers to the same instance. The fix is a new instance and a data migration, not a spec edit.

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
| `deletionPolicy` | `string` | Enum `Delete` (default; drop the database) / `Orphan` (leave it in place). See [Deletion](#deletion). |
| `managementPolicies` | `[]string` | Items enum `Observe`/`Create`/`Update`/`Delete`/`*`. Default `["*"]`. |

## Status

| Field | Type | Description |
|---|---|---|
| `databaseId` | `string` | Aura-assigned database ID. |
| `phase` | `string` | `Pending`, `Ready`, `Error`. |
| `conditions` | `[]metav1.Condition` | Standard readiness conditions. |
| `observedGeneration` | `int64` | The `.metadata.generation` last reconciled. |
| `lastSyncedTime` | `*metav1.Time` | When the operator last recorded the database as observed from the Aura API: written with every change to the status, and at least every 15 minutes while nothing changes — not on every poll, since each status write starts another reconcile. |

## Deletion

`Orphan` calls nothing: the CR is removed at once, with a Normal `AuraDatabaseOrphaned`. `Delete` drops the database in Aura, and needs what the database is reached through: its `AuraInstance` CR, the credentials and organization resolved from it, and the instance's ID.

- **The `AuraInstance` CR no longer exists.** Nothing can reach Aura for the database again, since the credentials come through the instance, so the CR is removed at once with a **Warning** `AuraDatabaseOrphaned`. The database went with the instance if that was deleted in Aura; otherwise it is still there with its data, so check the Aura console. Delete the `AuraDatabase` before its `AuraInstance` to have the operator drop it.
- **Anything else cannot be resolved** — the credentials Secret or provider config deleted first in a namespace teardown, say. The CR waits with phase `Error`, `Ready=False`, reason `CredentialsUnavailable`, announced once with `AuraDatabaseFailed`, and after 5 minutes is removed anyway with a Warning `AuraDatabaseOrphaned`: the database stays on the instance with its data.
- **Aura refuses the delete** — anything but a conflict, a rate limit, a 5xx or a network failure, which are retried as long as they last. The CR waits the same way, with reason `DeletionRefused`, then is removed with the same Warning.

Before, the CR was removed at once whenever the instance or the credentials could not be resolved, with a Normal event, and a refused delete was retried with error backoff for ever.

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
