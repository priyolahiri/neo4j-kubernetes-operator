# internal/validation

The **only** validation layer for the operator. Every CRD spec is checked here, inline from the
reconciler — there are no admission webhooks (invariant 1, `docs/knowledge/invariants.md`). One
file per concern; the per-CRD aggregators (`ClusterValidator`, `StandaloneValidator`,
`DatabaseValidator`, …) compose the smaller field validators.

## Key files

| File | What it does |
|---|---|
| `cluster_validator.go` | `ClusterValidator` — composes edition/topology/image/storage/tls/auth/upgrade/memory/config sub-validators; entry points `ValidateWithWarnings` (what the reconciler and `kubectl neo4j validate` run: the spec rules plus `validateAgainstRunning`), `ValidateCreateWithWarnings`/`ValidateCreate` (spec rules only), `ApplyDefaults`. |
| `standalone_validator.go` | `StandaloneValidator` — single-node equivalent: `Validate` (what the reconciler and `kubectl neo4j validate` run: `ValidateCreate`'s spec rules, the admin password's shape, and `validateAgainstRunning`, the tag against `status.version`); no update-only method (operations.md id 144). |
| `database_validator.go` | `DatabaseValidator` for `Neo4jDatabase` — tries cluster lookup first, then standalone. |
| `image_validator.go` | Rejects `-community` tags (`isCommunityTag`) + Neo4j version support check. |
| `edition_validator.go` | Intentional no-op (edition field removed; enterprise-only). |
| `topology_validator.go` | Server count, `serverModeConstraint`/`serverRoles` index/dup/all-SECONDARY checks. |
| `config_validator.go` | Rejects deprecated / operator-managed / per-pod runtime `spec.config` keys; calls `ValidateSettingValues`. |
| `setting_values.go` | `typedSettings` (setting → Neo4j value type + documented minimum) and `ValidateSettingValues`: refuses a typed value Neo4j would refuse at startup, with readers that mirror Neo4j's own parsers. Narrow on purpose — see `docs/knowledge/operations.md` id 140 before adding a setting. |
| `memory_validator.go`, `storage_validator.go` | Memory floors, PVC/storage checks; `StorageClassChangeWarning`, the advisory the reconcilers raise while `spec.storage.className` names a class the existing volumes do not use (operations.md id 102). |
| `tls_validator.go`, `truststore_validator.go` | TLS mode/issuer/strict-peer rules, trusted CA secrets. |
| `auth_validator.go`, `user_validator.go`, `role_validator.go`, `rolebinding_validator.go`, `authrule_validator.go` | Auth providers and the user/role/binding/authrule CRDs. |
| `backup_validator.go` | `BackupValidator` + standalone helper `ValidateNeo4jVersion`. |
| `upgrade_validator.go` (`ValidateVersionUpgrade`; `ValidateTagAgainstRunning`, the whole tag rule against status, shared by `validateAgainstRunning` and the upgrade state machine; `TagBelowReportedVersion`, its ordering for the machine's live check; `validateTagAgainstUpgrade`, the tag against what an unfinished upgrade's servers have started on (`upgradeFloor`: `status.upgradeStatus.startedTargetVersion`, or the target for a record an earlier operator wrote once it restarted a server), operations.md id 149; `ValidateUpgradeStrategy`), `plugin_validator.go`, `plugin_entrypoint.go` (`entrypointPluginLists`, the per-release names the Neo4j entrypoint installs; `ManagedPluginImageErrors`, operations.md id 170), `fleet_validator.go`, `mcp_validator.go`, `shardeddatabase_validator.go`, `sharding.go` | Version-upgrade rules, plugins, Aura fleet, MCP, sharded DB, `IsClusterShardingReady`. |
| `alias_validator.go`, `compositedatabase_validator.go`, `replica_validator.go`, `ccdr_proxy_validator.go`, `backup_replication_source.go` | `Neo4jDatabaseAlias`, `Neo4jCompositeDatabase`, `Neo4jReplicaDatabase`; `spec.crossClusterReplication` (refused without `spec.tls`); replication-source `Neo4jBackup` rules. |
| `admin_password_validator.go`, `license_validator.go`, `networkpolicy_validator.go` | Admin password shape (no leading `-`), `acceptLicenseAgreement` (`yes`/`eval`), `spec.networkPolicy`. |
| `*_validator_test.go`, `sharding_test.go` | Table-driven unit tests (one per validator). |

## Key types & functions

- Per-field validator pattern: `New<X>Validator()` returns a struct; `Validate(...) field.ErrorList`
  using `k8s.io/apimachinery/pkg/util/validation/field` (`field.Invalid`/`Required`/`NotSupported`).
- Aggregators wrap errors into a single `error` via `allErrs.ToAggregate()` and surface warnings
  through `ClusterValidationResult{Errors, Warnings}` (see `ValidateCreateWithWarnings`).
- `ImageValidator.isCommunityTag` / `Validate`; `ValidateNeo4jVersion` (in `backup_validator.go`);
  `IsClusterShardingReady` (in `sharding.go`, reused by the property-sharding gate).

## Conventions & gotchas

- **No webhooks** — these run inline from reconcilers, e.g. `r.Validator.ValidateWithWarnings`
  in `internal/controller/neo4jenterprisecluster_controller.go`; the standalone/backup/plugin
  controllers hold a `*validation.X` field. Don't add `_webhook.go` or webhook configs.
- `cluster_validator.go` **reports every validator, not just the first**: the early return on
  image errors was removed in #354, because it produced a fix-one-rerun loop (a broken image hid
  the `spec.config` and `spec.storage` problems behind it). Keep it that way — two tests pin it.
  The sharded validator had the same loop behind a failed cluster read (#532): its spec rules now
  run first, and a cluster that does not exist yet is `Pending` (the reconciler waits for it,
  operations.md id 145), never an error.
- `image_validator.go` rejects only the unambiguous `-community` signal; a bare/retagged Enterprise
  tag passes (runtime `CALL dbms.components()` is the backstop). Version must be 5.26.x or 2025.01.0+.
- `edition_validator.go` is a deliberate no-op — keep it; the edition field was removed.
- `DatabaseValidator.Validate` resolves cluster then standalone, and returns warnings (not errors)
  for topology/secondaries on standalone targets.
- Add new sub-validators to the right aggregator's `New...Validator` constructor and `validateCluster`
  (or `validateStandalone`) chain — a validator not wired in is dormant (cf. the previously-unwired
  `configValidator` / `PluginValidator`, and the deleted `SecurityValidator`). `TestEveryValidatorIsWiredIn`
  fails the build for a validator type nothing constructs.
- **Rules that belong to Neo4j, not to a Kind, go in a shared entry point that BOTH aggregators
  call** — `ConfigValidator.ValidateConfigMap` and `MemoryValidator.ValidateResourcesAndConfig`.
  When each aggregator carried its own copy the two drifted in both directions: `dbms.mode` was
  rejected on a standalone and accepted on a cluster, while heap-vs-limit was rejected on a
  cluster and accepted on a standalone (the v1.15.0 journey then watched that standalone
  crash-loop). `TestStandaloneAndClusterAgreeOnNeo4jRules` pins the parity.
- **A typed `spec.config` value is judged as Neo4j parses it, never stricter** (`setting_values.go`,
  id 140). A false refusal blocks a deployment that would run, so a setting joins `typedSettings` only
  when the `5.x` and `dev` settings docs agree on its type AND Neo4j's source declares it identically on
  both lines; a spelling the readers do not model is `valueUnknown` and passes. The memory rules read
  sizes with the same `readNeo4jByteSize` (their old parser refused `1GiB` and read `2Gi`).
- **There is no "old" object** (`docs/knowledge/operations.md` id 139). Without a webhook the
  reconciler sees only the current object, so a rule that needs the previous state reads what the
  operator recorded in status (`validateAgainstRunning`: `spec.image.tag` vs `status.version`). The
  update path used to fetch the cluster again and compare it with itself, so its rules never fired;
  the standalone's `ValidateUpdate` was never called at all (id 144). A rule that needs only the spec
  goes in `validateCluster`, where the CLI also runs it. `status.version` is what the servers run,
  read from a rolled StatefulSet (`rolledImageTag` in `internal/controller/running_version.go`).
  It is a tag: for one without a patch (`5.26-enterprise`), the version the servers report is in
  `status.runningVersion` (and an upgrade's in `status.upgradeStatus.resolvedTargetVersion`), and
  the tag is held to those too (`validateTagAgainstRunningVersion`, `belowReported`; id 153).
- **`ApplyDefaults` adds no field an error can be reported on.** The CLI applies it before validating,
  as the reconciler does, and a finding must name a field the user wrote or one that is missing (the
  nameless `spec.tls.issuerRef` default was removed for that, id 144).
- **`ApplyDefaults` only fills gaps, on a copy nothing writes back** (`docs/knowledge/operations.md`
  id 150). It never replaces a value the user set — a provider setting in `spec.config` counts as set —
  and a default it shares with the CRD says what the `+kubebuilder:default` marker says
  (`TestClusterValidator_ApplyDefaults_AgreesWithTheCRDDefaults` reads the CRD YAML). Adding a default
  that changes what is rendered for an existing spec restarts its servers on upgrade
  (`TestClusterValidator_ApplyDefaults_RendersAsTheDefaultsAnEarlierOperatorStored`).
- **Every published manifest is validated in CI** by `TestPublishedManifestsPassOurOwnValidators`,
  which walks `examples/` and `config/samples/`. Three shipped examples were unappliable before
  it existed, and `kubectl apply --dry-run=server` accepted all of them — the CRD schema cannot
  see these rules, which is the whole reason this package exists.

## Tests

Pure unit tests, no cluster needed:

```bash
go test ./internal/validation/...        # whole package
make test-unit                           # all unit tests
```

## See also

- `../../AGENTS.md` and root `CLAUDE.md` — invariants, domain reference.
- `docs/knowledge/invariants.md` (hard constraints, incl. NO WEBHOOKS / ENTERPRISE-IMAGES).
- `../controller/` — the reconcilers that call these validators.
- `../neo4j/` — `ParseVersion`/`IsSupported`/`IsCalver` used by image and version checks.
