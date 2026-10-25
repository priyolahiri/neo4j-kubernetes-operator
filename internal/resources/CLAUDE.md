# internal/resources

Pure, side-effect-free **builders** that turn a `Neo4jEnterpriseCluster` / `Neo4jEnterpriseStandalone`
CR into Kubernetes objects (StatefulSet, Services, ConfigMap, Certificate, RBAC, NetworkPolicy,
Route, MCP). No client calls, no reconcile logic — controllers in `../controller/` call these and
apply the results.

## Key files

| File | What it does |
|---|---|
| `cluster.go` | The bulk: StatefulSet, Services, ConfigMap (startup script + version-gated discovery), TLS Certificate, the server ServiceAccount (bound to no Role), Ingress, auth/audit/monitoring conf, conf merge helpers |
| `health_script.go` | `buildHealthScript`: a cluster server's `/conf/health.sh`. Readiness waits, at most `ReadyWindowSeconds` after a container starts, for the server to be available in the cluster (`/db/system/cluster/available`, `/dbms/cluster/status` on its own HTTP port); afterwards the process and the port only, so liveness never restarts a server for it. The probes are unchanged, and the ConfigMap manager never restarts a server for a `health.sh` change (`../../docs/knowledge/operations.md` id 181) |
| `memory_config.go` | Heap/pagecache sizing (`CalculateOptimalMemorySettings`, `…ForNeo4j526Plus`, `GetMemoryConfigForCluster`) |
| `plugin_init_container.go` | Verified-download init container for `Neo4jPlugin` (`BuildPluginVerifiedDownloadInitContainer`, plugin auth/CA volumes) |
| `networkpolicy.go` | `BuildNetworkPolicyForEnterprise` / `…ForStandalone` |
| `route.go` | OpenShift `Route` builders (unstructured) |
| `mcp.go` | MCP sidecar Deployment/Service/Ingress/Route builders |
| `version_label.go` | `VersionLabelValue`: the `app.kubernetes.io/version` label value for an image tag — the digest dropped, cut to a valid label value — used by every label set that carries a tag (`../../docs/knowledge/operations.md` id 149) |
| `resource_recommendation.go` | `NewResourceRecommender` resource sizing helper |
| `security_context.go` | `DefaultNeo4jPodSecurityContext` / `DefaultNeo4jContainerSecurityContext` (single source of truth) |
| `*_test.go` | Unit tests (see [Tests](#tests)); `cluster_startup_test.go` pins naming + discovery |

## Key types & functions

- `BuildServerStatefulSetForEnterprise(cluster)` — the **one** server StatefulSet (delegates to
  `buildStatefulSetForEnterprise(cluster, "server", Spec.Topology.Servers)`). The plural builder that made one StatefulSet per server was removed (#533).
- `buildVersionSpecificDiscoveryConfig(cluster)` — version-gated discovery conf string; branches on `isCalverImage(tag)`.
- `BuildConfigMapForEnterprise`, `BuildPodSpecForEnterprise` — startup script + pod spec.
- `BuildHeadlessServiceForEnterprise`, `BuildClientServiceForEnterprise`, `BuildInternalsServiceForEnterprise`, `BuildMetricsServiceForEnterprise`.
- `BuildBackupFromAddresses` (cluster) / `BuildStandaloneBackupFromAddress` — `pod-fqdn:6362` lists for `neo4j-admin database backup --from`.
- `ServerPodSelector` / `StandalonePodSelector` / `PVCSelectorByInstance` — label selectors.
- Conf helpers: `DedupeNeo4jConf`, `UpsertNeo4jConfSettings`, `Neo4jConfSettings`, `MergeConfListValues`, `IsAdditiveConfKey`, `Neo4jSettingEnvVarName`.
- Port constants: `DiscoveryPort=6000`, `RaftPort=7000`, `BackupPort=6362`, `BoltPort=7687`. Port 5000 (V1 discovery) is never used (invariant 4); the `{cluster}-discovery` Service that exposed it is gone (`LegacyDiscoveryName`, operations.md id 130).

## Conventions & gotchas

- **Invariant 5 — single server StatefulSet**: `BuildServerStatefulSetForEnterprise` builds one `{cluster}-server` StatefulSet with `replicas: N`. Pods are `{cluster}-server-0…N-1`. **Never** emit `primary-*` / `secondary-*` names, and never reintroduce a centralized `{cluster}-backup` StatefulSet or backup sidecar (backups are Job-per-`Neo4jBackup`-CR). See `../../docs/knowledge/invariants.md`.
- **Invariant 4 — V2_ONLY discovery on port 6000**: `buildVersionSpecificDiscoveryConfig` emits `dbms.cluster.discovery.version=V2_ONLY` + `dbms.cluster.discovery.v2.endpoints=…:6000` on SemVer 5.26.x, and on CalVer (2025.x+) omits the version flag and uses `dbms.cluster.endpoints=…:6000`. Endpoint FQDNs are static `{cluster}-server-{i}.{cluster}-headless.{ns}.svc.cluster.local:6000`. Both branches are pinned by tests — change both sides and the assertions together.
- **Never bake `NEO4J_PLUGINS` into the static template** (see the explicit NOTE near `cluster.go:1169`). The plugin/fleet/Aura controllers live-patch it via `MergeNeo4jPluginList` (defined in `../controller/plugin_controller.go`, not here) so they don't overwrite each other. See the plugin/env-var rules in `../../CLAUDE.md`.
- **A plugin's procedure list adds to the cluster's own** (`../../docs/knowledge/operations.md` id 179): the Neo4j entrypoint writes a `NEO4J_<setting>` variable in place of the neo4j.conf line, so the startup script's restart-neutral `buildPluginListMergeBlock` gives each shared list variable (`IsAdditiveConfKey`) the conf's entries first. Render the cluster's lists into neo4j.conf, never into the env, and keep the block between the restart-neutral markers. `ServerProcedureList` is the block in Go; keep the two agreeing (`TestServerProcedureList_AgreesWithTheBlock`). Aura Fleet Management's `fleetManagement.*` is always unrestricted, but joins the allowlist only beside one something else sets — the conf line when `spec.config` sets it, the block's entry when a plugin's variable does (id 192): an allowlist of it alone loads nothing else. The Neo4j image's entrypoint adds exactly that for `fleet-management` in `NEO4J_PLUGINS` to a conf without an allowlist (`apply_plugin_default_configuration`), so the block exports `*` (Neo4j's default) when nothing sets the list. Tests run the entrypoint's own functions through `../entrypointsim`.
- **Builders are pure**: deterministic output, no `client.Client`, no I/O. Keep them that way — reconcile decisions belong in `../controller/`.
- **Unstructured content holds JSON types only** (`../../docs/knowledge/operations.md` id 148): `Unstructured.DeepCopy` panics on a `map[string]string`, an `int` or an `int32`. Set labels and annotations with `SetLabels` / `SetAnnotations`, and write numbers as `int64` and lists as `[]any`. The Route builder broke this, and every pass of a deployment with a Route panicked (`route.go`; `jsonStringMap` in `cluster.go`).
- **No webhooks** (invariant 1): nothing here validates admission; validation is inline in `../validation/`.
- Don't hand-edit generated RBAC/CRD/Helm artifacts; `+kubebuilder:rbac:` markers live on controllers. See "Generated artifacts" in `../../CLAUDE.md`.

## Tests

All co-located here as `_test.go`. Run the package:

```bash
go test ./internal/resources/...
```

Pinned contracts to respect when editing: `cluster_startup_test.go` (`TestBuildConfigMapForEnterprise_ClusterFormation`, `TestListDiscoveryConfiguration` — assert FQDNs, `V2_ONLY` presence/absence, server-0 `me` bootstrap) and `cluster_test.go` (`TestBuildServerStatefulSetForEnterprise_EmptyStorageClassUsesDefault`, `TestBuildCertificateForEnterprise_DNSNames`). Caution: `TestBuildStatefulSetForEnterprise_WithFeatures` / `_ParallelManagement` drive the **Deprecated** plural builder, so they do NOT pin the single `{cluster}-server` StatefulSet shape — no unit test does yet (`docs/knowledge/invariants.md` INV-5).

## See also

- `../../AGENTS.md` — repo-wide agent guidance.
- `../../CLAUDE.md` — invariants, plugin/env-var rules, generated-artifacts table.
- `../../docs/knowledge/invariants.md` — hard constraints (4 and 5 above).
- `../controller/` — callers/appliers of these builders; `MergeNeo4jPluginList` lives in `plugin_controller.go`.
- `../validation/` — all CR validation (no webhooks).
