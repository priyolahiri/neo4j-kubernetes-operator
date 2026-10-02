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

## Upgrading from v1.17.x

Apply the new release's CRDs before upgrading the operator, as always (step 1
[below](#upgrading-between-future-releases)). Nothing fails without them, but
`status.upgradeStatus.phaseStartTime` is new: on the old CRDs the API server
drops it, and `neo4j_operator_upgrade_duration_seconds` records nothing until
the CRDs are applied.

### Behaviour changes

- **Scheduled backups report `Ready=True`** (reason `BackupScheduled`). A
  `Neo4jBackup` with a `schedule` used to stay `Ready=Unknown` for its whole
  life, so `kubectl wait --for=condition=Ready` never returned. A suspended one
  now reports `Ready=False` with reason `Suspended` instead of
  `ReconciliationFailed`.
- **If you use the [ArgoCD health checks](../gitops/README.md), re-apply
  `docs/gitops/argocd-health-checks.yaml`.** In the copy published with v1.17.0,
  a scheduled or suspended `Neo4jBackup` stayed `Progressing` forever, which
  holds up any sync wave behind it. Both now report `Healthy`.
- **`Neo4jDatabase.spec.defaultCypherLanguage` is honoured on every create
  path, and gated on the server version.** On CalVer, a database created with no
  `topology` and no `seedURI` used to ignore the field and get the server
  default; it now gets the language you set. Existing databases keep theirs,
  because a database's language is fixed when it is created. On the 5.26 LTS,
  `"25"` now fails validation with a message naming the field (it used to fail
  at `CREATE DATABASE` with a syntax error), and `"5"` is accepted and the clause
  left out, since every database there runs Cypher 5. `Neo4jCompositeDatabase` follows the same rule.
- **A standalone ignores `Neo4jDatabase.spec.topology`**, as the docs always
  said: the operator no longer sends a `TOPOLOGY` clause to a standalone, and
  `primaries: 0` there is a warning instead of an error.
- **Sizes are checked before they reach Kubernetes.** A cluster and a standalone
  now accept the same `spec.storage.size`: any Kubernetes quantity greater than
  zero, so `1.5Gi` works on a cluster and `0` is refused on both. A malformed
  size (`fifty`, `10 Gi`, or `5K`, since a capital K is not a Kubernetes suffix)
  used to pass validation and crash the operator; the CR now goes `Failed` with
  a message naming the field. `Neo4jBackup.spec.storage.pvc.size` is checked
  the same way. Every value that is newly refused either crashed the operator
  or could hold no data.
- **A dotted `Neo4jCompositeDatabase.spec.name` is refused when you apply.** It
  always failed at reconcile; the schema now says so up front.
- **Auth provider names follow the Neo4j manual.** `plugin-<name>`, for an auth
  plugin or an add-on such as Kerberos, is now accepted in
  `spec.auth.authenticationProviders` and `authorizationProviders`; it used to
  be refused. `oidc` without a provider name, `kerberos`, `jwt`, `saml` and
  `custom` are not values Neo4j documents: they still validate, and now raise a
  `ValidationWarning` event that names what to use instead. See
  [Multi-provider support](security.md#multi-provider-support).

### New warnings

A field the schema accepts but nothing in the operator acts on now raises a
`ValidationWarning` event when you set it: `… is accepted but has no effect
today: <what to do instead>`. The warnings never block a reconcile. They cover:

- `Neo4jPlugin`: `spec.resources`, `spec.source.registry` (and its `tls`),
  `spec.security.securityPolicy`;
- clusters and standalones: `spec.tls.certificateSecret`; standalones only:
  `spec.tls.strictPeerValidation: false`;
- clusters: `spec.topology.placement.nodeSelector` and `.requiredDuringScheduling`;
- `Neo4jDatabase`: `spec.initialData.source` (other than `cypher`),
  `.configMapRef`, `.secretRef` and `.storage`;
- `Neo4jBackup` and `Neo4jRestore` cloud storage: `identity.serviceAccount`, and
  `identity.autoCreate.enabled: false`;
- `AuraInstance`: `spec.connectionSecretFormat: custom`.

### Metrics

**Removed:** twelve families that were registered but that no code ever
recorded. Each was a vector with no observations, so none ever exported a
series and no dashboard or alert loses data; you can delete panels built on
them. `neo4j_operator_` followed by: `backup_size_bytes`,
`cypher_execution_duration_seconds`, `cypher_executions_total`,
`disaster_recovery_status`, `failover_total`, `replication_lag_seconds`,
`manual_scaler_enabled`, `primary_count`, `secondary_count`,
`scale_events_total`, `scaling_validation_total`, `security_operations_total`.

**New or now populated:**

- `neo4j_operator_replica_lag_transactions` and
  `neo4j_operator_replica_promotions_total` for cross-cluster replication (see
  [Cross-cluster replication metrics](guides/monitoring.md#cross-cluster-replication-metrics)).
  The lag series is removed when it can no longer be trusted (the replica is
  gone or promoted, or its lag cannot be read) instead of holding its last value.
- `neo4j_operator_upgrade_duration_seconds` is now recorded, once per ended
  rolling-upgrade phase.

### New, no action needed

- `status.upgradeStatus.phaseStartTime` on clusters.
- `Neo4jShardedDatabase` status now fills `creationTime`, `graphShard`,
  `propertyShards` and `virtualDatabase`; `Neo4jPlugin` status fills
  `installedVersion` and `installationTime`. They were in the schema but empty.
- A failed Aura Fleet Management token registration raises an
  `AuraFleetManagementFailed` Warning event, once per distinct failure message.
  It almost never fired before.

## Upgrading between future releases

When a newer version ships:

1. **Refresh the CRDs first.** Helm does not upgrade CRDs automatically, so a new
   field or validation won't take effect until the CRDs are applied. Apply the
   bundle for the version you are upgrading to (replace the tag):

   ```bash
   kubectl apply --server-side -f \
     https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v1.18.0/neo4j-kubernetes-operator.yaml
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
