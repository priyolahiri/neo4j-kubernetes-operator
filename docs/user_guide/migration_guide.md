# Upgrade Guide

**v1.13.0 is the first public release of this independent project.** There is no
upgrade path from earlier version numbers — install v1.13.0 fresh (see the
[Installation guide](installation.md)).

> This is a personally maintained, community project with no affiliation to
> Neo4j, Inc. APIs and behaviour **may change between releases** — review the
> release notes before upgrading, and validate independently before relying on
> a new version.

## v1.14 — backup & restore API cleanup (breaking)

v1.14 removes the deprecated `Neo4jBackup` / `Neo4jRestore` fields that v1.13
kept working behind deprecation warnings. `v1beta1` is still a beta API, so these
are removed in place rather than versioned. **Update your `Neo4jBackup` and
`Neo4jRestore` manifests before upgrading** — the removed fields are silently
dropped by the API server (they're no longer in the CRD schema), which would
otherwise leave a backup/restore mis-scoped or unconfirmed.

Nothing else changes: cluster, standalone, database, user/role, and plugin CRs
are untouched.

### `Neo4jBackup`

| Removed | Replacement |
|---|---|
| `spec.target.{kind,name,clusterRef}` | `spec.instanceRef` + exactly one of `spec.database` / `spec.allDatabases` / `spec.shardedDatabase` |
| `spec.cloud` (top-level) | `spec.storage.cloud` |
| `spec.options.verify` | *(removed — it was never wired to `neo4j-admin backup validate`; use `spec.options.validate`)* |
| `spec.retention.deletePolicy: Archive` | `Delete` (the only supported value; `Archive` never had archival logic) |

```yaml
# BEFORE (v1.13)                          # AFTER (v1.14)
spec:                                     spec:
  target:                                   instanceRef: my-cluster
    kind: Cluster                           allDatabases: true
    name: my-cluster                        storage:
  cloud:                                       type: s3
    provider: aws                              bucket: backups
  storage:                                     cloud:
    type: s3                                     provider: aws
    bucket: backups
```

Scope mapping: `kind: Cluster` → `allDatabases: true`; `kind: Database` (with
`name: db`, `clusterRef: c`) → `instanceRef: c` + `database: db`;
`kind: ShardedDatabase` (with `name: sd`, `clusterRef: c`) → `instanceRef: c` +
`shardedDatabase: sd`.

### `Neo4jRestore`

| Removed | Replacement |
|---|---|
| `spec.clusterRef` | `spec.instanceRef` |
| `spec.databaseName` | `spec.database` |
| `spec.force` | `spec.options.replaceExisting: true` |
| `spec.verifyBackup` | *(removed — it was never implemented)* |

```yaml
# BEFORE (v1.13)                          # AFTER (v1.14)
spec:                                     spec:
  clusterRef: my-cluster                    instanceRef: my-cluster
  databaseName: orders                      database: orders
  force: true                               options:
  source:                                     replaceExisting: true
    type: backup                            source:
    backupRef: nightly                        type: backup
                                              backupRef: nightly
```

Also new in v1.14 (additive, no action needed): `Neo4jRestore.status.stats.duration`
and `status.backupInfo` are now populated on a successful restore.

## Upgrading from v1.16.x

Apply the new release's CRDs **before** upgrading the operator (step 1
[below](#upgrading-between-future-releases)). This release needs them for more
than new fields: learn mode keeps its state in new `Neo4jRole` status fields,
and without the CRDs every `Neo4jRole` goes `Failed` saying so. No privilege is
removed in that state.

### `Neo4jRole`: privileges are matched by learn mode (behaviour change)

Neo4j stores a privilege differently from how it is written (`NODES` becomes
`NODE`, lists become one row per item, aliases resolve to their target). The
operator used to compare the text, which toggled some privileges off on every
other reconcile. It now compares against what Neo4j stores, learned from its
own grants — **learn mode**, the new default; see
[Privilege drift reconciliation](user_role_management.md#privilege-drift-reconciliation).

What you will see on upgrade: **every existing role reports
`PrivilegesSynced=Unknown` (reason `UnattributedPrivileges`)**, because learn
mode cannot attribute rows that were already there. Nothing is revoked and
access is unchanged; the difference is that out-of-band additions to those rows
are not removed until you clean them up. To restore full enforcement on a
role, remove unwanted rows by hand or delete and recreate the `Neo4jRole`. If
you would rather have exact attribution immediately and can accept short-lived
`operator_privilege_probe_*` roles in Neo4j's security log, set the Helm value
`privilegeNormalisation: probe`.

### Sharding no longer sets the server's Cypher language

Enabling property sharding used to set `db.query.default_language=CYPHER_25`
for the whole server. It no longer does: sharded databases get Cypher 25 on
their own, and the server default is the new
`spec.serverDefaultCypherLanguage`. **Existing clusters are unaffected** — the
operator records what each one already runs and keeps writing it, so the
upgrade restarts nothing. See the
[property sharding guide](property_sharding.md).

### New, no action needed

- `spec.serverDefaultCypherLanguage` on clusters and standalones — see
  [Configuration](configuration.md). Existing deployments keep their current
  language.
- `spec.privilegeRules` on `Neo4jRole`: privileges as fields instead of Cypher.
- Remote composite constituents with stored credentials now work on the 5.26
  LTS; they failed there in v1.16.0. Their URL must be `neo4j+s://` or
  `neo4j+ssc://`, which is now checked at apply time.
- `spec.graphAnalytics` on `AuraInstance`: `unavailable`, `plugin` or
  `serverless`. Serverless graph analytics is created through Aura's v2beta1
  API. See the [AuraInstance API reference](../api_reference/aurainstance.md).

### Deprecated

- `AuraInstance.spec.graphAnalyticsPlugin` — use `spec.graphAnalytics`. It keeps
  working and maps to `plugin` (true) or `unavailable` (false). Setting both is
  refused. An existing instance may switch to the equivalent `graphAnalytics`
  value; setting any other `graphAnalytics` value is refused, because Aura cannot
  change it after creation. Editing or removing the boolean itself is accepted
  but has no effect on an existing instance, which only reads it at creation.

## Upgrading between future releases

When a newer version ships:

1. **Refresh the CRDs first.** Helm does not upgrade CRDs automatically, so a new
   field or validation won't take effect until the CRDs are applied. Apply the
   bundle for the version you are upgrading to (replace the tag):

   ```bash
   kubectl apply --server-side -f \
     https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v1.17.0/neo4j-kubernetes-operator.yaml
   ```

2. **Upgrade the operator** via Helm:

   ```bash
   helm repo update
   helm upgrade neo4j-operator neo4j-operator/neo4j-operator \
     --namespace neo4j-operator-system
   ```

   (or re-apply the complete bundle if you installed with plain `kubectl` —
   see the [Installation guide](installation.md)).

3. **Read the release notes** for that version for any breaking changes or
   manual steps, and roll forward one minor version at a time if you are
   skipping several.

The operator reconciles declaratively, so once the new version is running it
converges existing `Neo4j*` resources to the new behaviour automatically.
