# internal/controller

> controller-runtime reconcilers for every operator CRD (27 controllers, one per Kind: cluster, standalone,
> database, sharded database, composite database, alias, replica database, replica promotion, backup,
> restore, user, role, rolebinding, authrule, plugin, plus the 12 `aura*_controller.go` for the Aura CRDs),
> plus the shared helpers (events, conditions, clusterRef resolution, env-var/owned-key merging,
> split-brain detection, topology scheduling, rolling upgrade) they lean on.

## Key files

| File | What it does |
|---|---|
| `neo4jenterprisecluster_controller.go` | HA cluster reconciler. Owns the `{cluster}-server` StatefulSet, `mergeEnvVars`/`envVarsEqual`, `CollectDiagnostics`, fleet-management plugin merge. |
| `neo4jenterprisestandalone_controller.go` | Single-node reconciler (shares helpers via `standaloneAsCluster` in `util.go`). |
| `neo4jbackup_controller.go` | Job-per-`Neo4jBackup`-CR reconciler (invariant 5 — no long-running backup pod). `neo4jbackup_sharded.go`, `neo4jbackup_log_parser.go` support it. |
| `neo4jrestore_controller.go` | Restore reconciler (heaviest user of `RetryOnConflict` status writes). |
| `neo4jdatabase_controller.go` / `neo4jshardeddatabase_controller.go` | `Neo4jDatabase` / `Neo4jShardedDatabase` reconcilers. |
| `neo4juser_controller.go` / `neo4jrole_controller.go` / `neo4jrolebinding_controller.go` / `neo4jauthrule_controller.go` | Auth-model reconcilers. |
| `neo4jcompositedatabase_controller.go` / `neo4jdatabasealias_controller.go` / `neo4jreplicadatabase_controller.go` / `neo4jreplicapromotion_controller.go` | Composite DB, alias and cross-cluster replication reconcilers. |
| `aura*_controller.go`, `aura_fleet_provision.go` | Aura (cloud) reconcilers on the Aura REST API (`internal/aura/`); no Bolt. Refusals must not reach an `aura.IsTransient` branch unmarked (knowledge `operations.md` id 88). |
| `plugin_controller.go` | `Neo4jPlugin` reconciler; home of `MergeNeo4jPluginList`. Watches target StatefulSets so a `Ready` plugin restores what something removed, and nothing else (knowledge `operations.md` id 135); a write to the servers waits until the target is `Ready` with every server available and has rolled out its latest spec, and the plugin is `Ready` only once its own roll is done (id 141); it is the only writer of a `VerifiedDownload` plugin's `plugin-ca` volume, which follows the target's `spec.trustedCASecrets` (id 147). `plugin_target_gate.go` holds that gate (`targetRolloutPending`, `rolloutPending`) and its one exception, the write that repairs the plugin's own failing init container (`selfRepair`, `replaceStuckPods`; id 152). |
| `events.go` | All `EventReason*` string constants — emit these, never raw strings. |
| `conditions.go` | Most `Condition{Type,Reason}*` constants (the rest are in `events.go`) + `SetReadyCondition` / `SetNamedCondition` / `PhaseToConditionStatus`. |
| `cluster_resolver.go` | `ResolveClusterRef`, `ResolvedTarget`, `EnqueueDependentsForClusterChange`. |
| `owned_keys.go` | Owned env-var/annotation tracking (`neo4j.com/cluster-controller-env-vars`). |
| `splitbrain_detector.go` | `SplitBrainDetector` — compares per-pod cluster views, restarts orphans. |
| `topology_scheduler.go` | `TopologyScheduler` — spread constraints / anti-affinity / placement. |
| `rolling_upgrade.go` / `rolling_upgrade_statemachine.go` / `scale_down.go` / `storage_expansion.go` | In-place upgrade, drain, and PVC-expansion flows. `upgrade_hold.go`: a `Paused` or `Failed` upgrade keeps its StatefulSet partition until the state machine takes it up again (knowledge `operations.md` id 149). |
| `configmap_manager.go` / `cache_manager.go` / `connection_helper.go` / `backup_resolver.go` / `cluster_seed_creds.go` / `finalizer_deletion.go` | Supporting managers/helpers. |
| `object_patch.go` | `patchObject` / `addFinalizer` / `removeFinalizer`: the merge patches every write of one of the operator's CRs goes through (id 150). |
| `running_version.go` / `reported_version.go` / `standalone_running_version.go` | `status.version` (the tag the servers rolled out, id 144) and, for a tag without a patch, the version the servers report: `status.runningVersion` and `status.upgradeStatus.resolvedTargetVersion`, read from the SHOW SERVERS the cluster's formation check and the upgrade's availability check already run, and once per image on a standalone (id 153). |
| `suite_test.go` | envtest harness (`TestControllers` → `RunSpecs`). |

## Key types & functions

- `ResolveClusterRef(ctx, c, ns, name) (ResolvedTarget, error)` — tries cluster first, then standalone (`cluster_resolver.go`). `ResolvedTarget` exposes `IsReady`, `NewClient`.
- `EnqueueDependentsForClusterChange(...)` — watch wiring so user/role/binding reconcilers re-run on cluster status flips (used in each `SetupWithManager`).
- `SetReadyCondition` / `SetNamedCondition` (`conditions.go`) — `SetNamedCondition` for `ServersHealthy`/`DatabasesHealthy` etc.; `SetReadyCondition` ONLY for the `Ready` type.
- `(r *Neo4jEnterpriseClusterReconciler) envVarsEqual(...)` + package-level `mergeEnvVars(...)` (`neo4jenterprisecluster_controller.go`).
- `MergeNeo4jPluginList(existing, newPlugin)` (`plugin_controller.go`) — live-patch `NEO4J_PLUGINS` without clobbering other controllers.
- Each reconciler's `Reconcile` + `SetupWithManager`.

## Conventions & gotchas

- **Validation is INLINE, never a webhook (invariant 1).** Reconcilers hold a `*validation.*Validator` (e.g. cluster controller's `Validator *validation.ClusterValidator`) and call it from `Reconcile` — there is no `_webhook.go`.
- **Status writes wrap `retry.RetryOnConflict(retry.DefaultRetry, ...)`** (or `DefaultBackoff`) — required for 2025.01.0 cluster formation. See `neo4jenterprisecluster_controller.go` (`grep -n RetryOnConflict`). Re-fetch the object inside the closure.
- **Never `Update` one of the operator's CRs; patch what you change** — a finalizer, an annotation, a spec field (`object_patch.go`: `patchObject`, `addFinalizer`, `removeFinalizer`; knowledge `operations.md` id 150). An Update sends the whole object as the Go types encode it: the cluster reconcile's copy carries `ApplyDefaults`' defaults, a `false` the CRD defaults to `true` is omitted and comes back `true` (a `Neo4jRole`'s `enforcePrivileges: false` turned into revoking), and a quantity is re-spelled. `TestReconcile_LeavesTheSpecAsApplied_APIServer` and `TestFinalizers_LeaveEveryKindsSpecAsApplied_APIServer` (envtest) pin the writers. Child objects (StatefulSets, Services, ConfigMaps) are the operator's own and are not covered.
- **Per-server clients use the cluster's admin Secret with its default** (`getClusterAdminSecretName`; `dialClusterPod`, `createPodSpecificNeo4jClient`; knowledge `operations.md` id 132): never refuse a cluster without `spec.auth.adminSecret`.
- **StatefulSet existence check is `sts.UID != ""`, NOT `ResourceVersion`** (ResourceVersion is set even on never-created objects). See `createOrUpdateResourceInternal` in `neo4jenterprisecluster_controller.go`.
- **Emit events via `EventReason*` constants from `events.go`** with `corev1.EventTypeNormal` / `corev1.EventTypeWarning` — never raw strings.
- **Env vars: subset-merge, never wholesale-replace.** `envVarsEqual` checks desired vars exist with the right value but tolerates foreign extras; `mergeEnvVars` merges and enforces removals via the owned-keys annotation (`owned_keys.go`). Do not bake `NEO4J_PLUGINS` into the static StatefulSet template; use `MergeNeo4jPluginList`. (CLAUDE.md "Key Implementation Patterns".) Init containers and volumes of the server pod template follow the same model by name (`mergeOwnedByName`, `readOwnedTemplateItems`; knowledge `operations.md` id 141) — a `VerifiedDownload` plugin's init container is not the cluster's.
- **`CollectDiagnostics` is non-fatal** — surface errors to `status.diagnostics.collectionError`, never `return err`.
- **Server-based naming only**: `{cluster}-server-0…N-1`; never `primary-*`/`secondary-*` (invariant 5). `splitbrain_detector.go` enumerates pods by this scheme.

## Tests

`*_test.go` here run under the envtest suite (`suite_test.go`, no real cluster). Run the whole package with `make test-unit`, or directly: `go test ./internal/controller/...`. The core/extended ginkgo label split lives in `test/integration/`, not in this package. A single integration spec: `make test-one TEST="name"`.

## See also

- `../../AGENTS.md` and `../../CLAUDE.md` — repo-wide invariants and domain reference.
- `../../docs/knowledge/{invariants.md,operations.md,backup-restore.md}` — enforcement-tagged regression rules.
- `../validation/` — the validators these reconcilers call inline.
- `../resources/` — the K8s object builders (StatefulSet, Services, ConfigMaps) reconcilers apply.
- `../neo4j/` — the Bolt client (`NewClientForEnterprise*`, Cypher for users/privileges).
