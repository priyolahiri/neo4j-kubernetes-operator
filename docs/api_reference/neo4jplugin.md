# Neo4jPlugin API Reference

The `Neo4jPlugin` Custom Resource Definition (CRD) provides automated plugin installation and management for both Neo4j Enterprise clusters and standalone deployments.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jPlugin`
- **Target Deployments**: Both `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`
- **Installation Method**: Neo4j's `NEO4J_PLUGINS` environment variable approach
- **Supported Plugins**: APOC, APOC Extended, Graph Data Science, Bloom, GenAI and, on 5.26, N10s by name (`installMode: Managed`, [the names each image's entrypoint installs](#names-the-entrypoint-installs)); any other JAR with `installMode: VerifiedDownload` or `PreBaked`
- **Note**: Fleet Management is managed via `spec.auraFleetManagement` on the cluster/standalone CRD, not via `Neo4jPlugin`. Both coexist without conflict.
- **Automatic Configuration**: Plugin-specific settings and security policies

## Architecture

**Universal Compatibility**: The `Neo4jPlugin` CRD works seamlessly with both deployment architectures:

- **Cluster Support**: Updates `{cluster-name}-server` StatefulSet with plugin configuration
- **Standalone Support**: Updates `{standalone-name}` StatefulSet with plugin configuration
- **Automatic Detection**: Controller automatically identifies target deployment type
- **Rolling Updates**: Triggers controlled restarts to apply plugin changes
- **Environment Variable Method**: Uses Neo4j's recommended `NEO4J_PLUGINS` installation approach

## Related Resources

- [`Neo4jEnterpriseCluster`](neo4jenterprisecluster.md) - Target cluster deployments
- [`Neo4jEnterpriseStandalone`](neo4jenterprisestandalone.md) - Target standalone deployments
- [`Neo4jDatabase`](neo4jdatabase.md) - Create databases that use plugin functionality
- [Plugin Examples](https://github.com/priyolahiri/neo4j-kubernetes-operator/tree/main/examples/plugins) - Detailed usage examples

## Plugin Installation Process

The `Neo4jPlugin` controller implements Neo4j's recommended installation approach:

### Installation Steps

1. **Target Discovery**: Automatically detects whether `clusterRef` points to a cluster or standalone
2. **Plugin Collection**: Gathers main plugin and all dependencies into a unified list
3. **Environment Variable Setup**: Merges plugin names into the existing `NEO4J_PLUGINS` JSON array (additive — does not overwrite entries added by other controllers such as the Aura Fleet Management reconciler)
4. **Configuration Application**: Adds plugin-specific settings as `NEO4J_*` environment variables, records their names on the StatefulSet (annotation `neo4j.com/plugin-env-vars.<plugin name>`), and removes the ones it recorded before and no longer sets
5. **StatefulSet Update**: Patches the target StatefulSet with new configuration — in one update (with a `VerifiedDownload` plugin's init container and volumes), and only when something changes. Because the update restarts every server, it waits (phase `Waiting`, a `PluginInstallDeferred` event) until the target is `Ready` with every server available: not while a cluster has a server down (`Degraded`), nor while it is forming, rolling its servers or upgrading
6. **Rolling Restart**: Triggers controlled pod restarts to apply changes
7. **Ready once rolled out**: The plugin is `Ready` once every server runs the new pod template. Every setting reaches the servers when they start (an env var, or on a standalone its `neo4j.conf`); nothing is set at runtime, and the operator does not call Neo4j once the roll is done

> **Multi-plugin coexistence**: The plugin controller, the cluster/standalone controller, and the Aura Fleet Management reconciler all use an additive merge strategy for `NEO4J_PLUGINS`. Installing APOC via `Neo4jPlugin` and enabling Fleet Management via `spec.auraFleetManagement` simultaneously results in `["apoc","fleet-management"]` — neither entry is overwritten on subsequent reconciles.

### Environment Variable Mapping

**Example Configuration (APOC)**:
```yaml
config:
  "apoc.export.file.enabled": "true"
  "apoc.import.file.enabled": "true"
```

**Applied Environment Variables (APOC)**:
```yaml
env:
- name: NEO4J_PLUGINS
  value: '["apoc"]'
- name: NEO4J_apoc_export_file_enabled
  value: 'true'
- name: NEO4J_apoc_import_file_enabled
  value: 'true'
```

**Notes**:

- Every `spec.config` key becomes a `NEO4J_<setting>` environment variable (`.` → `_`, a literal `_` doubled, case kept), which Neo4j reads at startup; APOC settings must go this way in Neo4j 5.26+
- On a standalone, security and startup-only settings go to its `neo4j.conf` instead of the env
- Neo4j refuses to start on a setting it does not know, so list only settings your Neo4j version documents
- Automatic dependency resolution and security defaults are applied as needed

### Removing settings and uninstalling

The plugin records the variables it sets on the target's StatefulSet, under `neo4j.com/plugin-env-vars.<plugin name>`. When a key leaves `spec.config`, the plugin removes its variable — one rolling restart, which waits like an install until the target is `Ready` with every server available. When every server that is down keeps failing in its `neo4j` container while running a variable the removal changes (Neo4j refused the setting), those are the highest-numbered servers, and Neo4j reports no other server unavailable, the removal does not wait: phase `Installing`, one `PluginSettingsFailing` Warning, and the restart replaces the failing servers first; on a standalone the plugin then deletes the stuck pod. A variable the cluster or standalone renders from its own spec, or one another plugin of the target sets, is never removed.

It records its `NEO4J_PLUGINS` names the same way, under `neo4j.com/plugin-entries.<plugin name>`: a dependency dropped from `spec.dependencies`, or a switch away from `Managed` mode, takes its names out of `NEO4J_PLUGINS` in the plugin's next write (not those another plugin of the target installs, nor `fleet-management` while `spec.auraFleetManagement` installs it). After a switch to `VerifiedDownload` the entrypoint therefore no longer installs the plugin beside the verified JAR.

On a cluster, each shared procedure list (`dbms.security.procedures.unrestricted`, `…allowlist`, `…denylist`, `dbms.security.http_auth_allowlist`, `server.unmanaged_extension_classes`) is exactly what the cluster's enabled plugins put there — their `spec.security`, their automatic settings, and the list keys of their `spec.config` — written with a plugin's write. A token a plugin no longer asks for goes, and a list key in `spec.config` adds to the list rather than replacing it. On a standalone the lists are in its `neo4j.conf`, where its controller unions its own `spec.config` with its plugins' settings.

Deleting the `Neo4jPlugin` — or setting `spec.enabled: false` — takes off the StatefulSet, in one write, everything the plugin wrote: its variables, its tokens in the procedure lists (not those another plugin needs), its `NEO4J_PLUGINS` names (those it recorded, and in Managed mode its own and its dependencies'; not those another plugin of the target installs, nor `fleet-management` while `spec.auraFleetManagement` installs it, nor anything when `NEO4J_PLUGINS` comes from the cluster's own `spec.env`), a `VerifiedDownload` plugin's init container and volumes, and its records.

A plugin installed by an operator older than v1.22.0 has no record of its variables, and one older than this release none of its `NEO4J_PLUGINS` names: its first reconcile records what it sets now (a metadata write, no restart) and removes nothing. What a removal left behind before the upgrade stays until removed by hand.

### Names the entrypoint installs

In `Managed` mode the plugin's name and its dependencies' go into `NEO4J_PLUGINS`, after the aliases `gds` (`graph-data-science`) and `neosemantics` (`n10s`). The Neo4j Docker entrypoint looks each name up in the image's `/startup/neo4j-plugins.json` and exits on one it does not list, so the server does not start. The lists, read from `neo4j/docker-neo4j` at every release tag and checked on the images (2026-10-24):

| Neo4j image | Names `NEO4J_PLUGINS` may hold |
|---|---|
| 5.26.0 – 5.26.10 | `apoc`, `apoc-extended`, `bloom`, `genai`, `graph-data-science`, `n10s` |
| 5.26.11 and later (also `5.26-enterprise`) | the above and `fleet-management` |
| 2025.01 – 2025.04 | `apoc`, `apoc-extended`, `bloom`, `genai`, `graph-data-science`, `n10s` |
| 2025.05 – 2025.07 | `apoc`, `apoc-extended`, `bloom`, `genai`, `graph-data-science` |
| 2025.08 and later | `apoc`, `apoc-extended`, `bloom`, `fleet-management`, `genai`, `graph-data-science` |

The operator judges a `Managed` plugin by the image tag of its target (`spec.image.tag`; a tag without a patch as its line's newest patch, a CalVer newer than the operator by the newest list it knows). A name the image does not list — `graphql` on any supported release, `n10s` on CalVer from 2025.05, a custom JAR's name — is refused: phase `Failed` with `spec.name: Invalid value: "<name>": not a plugin the neo4j:<tag> image can install: …; install its JAR with installMode: VerifiedDownload (source.url + source.checksum) instead` (or `spec.dependencies[i].name`), one `PluginUnknownToImage` Warning, nothing written, and another look every 30 seconds. What an earlier operator wrote of it — its `NEO4J_PLUGINS` names (those it recorded; without a record, its own and its dependencies' as the entrypoint knows them), its settings and records — is taken off in the write a disabled plugin's removal makes, gated the same way: at once, with one `PluginSettingsFailing` Warning, when every server that is down keeps failing on it, otherwise once the target is `Ready` with every server available. `VerifiedDownload` and `PreBaked` never touch `NEO4J_PLUGINS`, so any JAR installs that way; a disabled plugin is not judged. `kubectl neo4j validate --connect` applies the same rule; offline it warns about a name no supported release installs.

### Disabling a plugin

`spec.enabled: false` keeps the plugin off its target's servers and keeps the CR. An installed plugin is removed in the one write described above, phase `Installing` ("Removing the plugin … (spec.enabled: false)") while the servers restart without it, then `Disabled`, with one `PluginDisabled` event; a plugin created disabled is `Disabled` at once and writes nothing. Like an install, the removal waits (`Waiting`, one `PluginInstallDeferred` event, "… removing the disabled plugin restarts every server") until the target is `Ready` with every server available — except when every server that is down keeps failing in what the plugin set (its `VerifiedDownload` init container, or a setting Neo4j refuses): then it is written at once, with one `PluginInitContainerFailing` or `PluginSettingsFailing` Warning, and the restart replaces the failing servers first. Setting `spec.enabled: true` again installs the plugin again (one rolling restart). On a standalone with settings in its `neo4j.conf` (an allowlist, say), those leave with the standalone's own restart first and the plugin's write restarts it once more, as on install.

## API Version

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
```

## Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `clusterRef` | `string` | ✅ | Name of target Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone |
| `name` | `string` | ✅ | Plugin name (e.g., "apoc", "graph-data-science"). In `Managed` mode it must be a name the target's image installs — see [Names the entrypoint installs](#names-the-entrypoint-installs) |
| `version` | `string` | ✅ | Plugin version to install (must match Neo4j version compatibility) |
| `enabled` | `boolean` | ❌ | Whether the plugin is on its target's servers (default: `true`). `false` removes an installed plugin (one rolling restart) and keeps the CR, in phase `Disabled`; `true` again installs it. See [Disabling a plugin](#disabling-a-plugin) |
| `installMode` | `string` | ❌ | `Managed` (default) — operator adds plugin to `NEO4J_PLUGINS`. `PreBaked` — operator only writes config; JAR must be in a custom image. `VerifiedDownload` — operator injects an init container that downloads `source.url` and verifies it against `source.checksum` before Neo4j starts. See [Supply-chain](#supply-chain). |
| `source` | [`PluginSource`](#pluginsource) | ❌ | Plugin source configuration (default: official repository). **Required** when `installMode: VerifiedDownload`. Ignored when `installMode: PreBaked`. |
| `dependencies` | [`[]PluginDependency`](#plugindependency) | ❌ | Plugin dependencies (automatically resolved) |
| `config` | `map[string]string` | ❌ | Plugin-specific configuration (becomes `NEO4J_*` env vars) |
| `security` | [`PluginSecurity`](#pluginsecurity) | ❌ | Security settings and procedure restrictions |
| `resources` | `PluginResourceRequirements` | ❌ | **Reserved — no effect today.** Declared (`memoryLimit`, `cpuLimit`, `threadPoolSize`) and syntax-checked by the validator, but no controller or builder reads it: nothing is allocated or limited. Size the Neo4j pods through the cluster/standalone `spec.resources` instead. A `ValidationWarning` event is raised when it is set. |

### PluginSource

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | Source type: "official", "community", "custom", "url" (default: `official`) |
| `url` | `string` | Direct URL for "url" and "custom" source types. **Must be `https://`** — the controller-side validator rejects `http://`, `file://`, and every other scheme (plugin JARs are downloaded over the network and a non-https scheme has no transport integrity). Host internal plugins on an https mirror, e.g. with a cert-manager certificate. |
| `checksum` | `string` | Checksum for verification. **Required** by the controller-side validator for `type: url` and `type: custom`. Must match `^(sha256:[a-fA-F0-9]{64}\|sha512:[a-fA-F0-9]{128})$` (hex digits of either case). SHA1 and MD5 are rejected. See [Supply-chain](#supply-chain). |
| `authSecret` | `string` | Secret containing auth for private repositories/URLs. Consumed only by `installMode: VerifiedDownload` (keys `token` or `header` — see [Supply-chain](#supply-chain)). |
| `registry` | [`PluginRegistry`](#pluginregistry) | **Reserved — no effect today.** Never read by any controller; a `custom` source is fetched from `source.url`, which (with `checksum`) is required for `type: url` and `type: custom` regardless of `registry`. A `ValidationWarning` event is raised when it is set, and another when `registry.tls` is set. |

### PluginRegistry

> **Reserved — no effect today.** `source.registry` and everything under it (including `tls`) is accepted by the schema but never read, and a `ValidationWarning` event says so when it is set; see the `registry` row above.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | `string` | ✅ | Registry URL |
| `authSecret` | `string` | ❌ | Authentication secret |
| `tls` | `RegistryTLSConfig` | ❌ | TLS configuration |

### RegistryTLSConfig

| Field | Type | Description |
|-------|------|-------------|
| `insecureSkipVerify` | `boolean` | Skip TLS verification |
| `caSecret` | `string` | CA certificate secret |

### PluginDependency

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Dependency plugin name |
| `versionConstraint` | `string` | Version constraint (e.g., ">=5.26.0") |
| `optional` | `boolean` | Whether dependency is optional |

### PluginSecurity

| Field | Type | Description |
|-------|------|-------------|
| `allowedProcedures` | `[]string` | Becomes `dbms.security.procedures.allowlist` (and `…unrestricted` unless `sandbox: true`). An **allowlist**: plugin procedures and functions it does not match are not loaded at all, server-wide, and fail as *Unknown function*. |
| `deniedProcedures` | `[]string` | List of denied procedures/functions |
| `securityPolicy` | `string` | Security policy: one of `"strict"`, `"moderate"`, `"permissive"` (any other value fails validation and the plugin goes to phase `Invalid`). **Reserved — no effect today:** the value is validated but not used to change any Neo4j setting (restrict procedures with `allowedProcedures` / `deniedProcedures`). A `ValidationWarning` event is raised when it is set. |
| `sandbox` | `boolean` | Enable sandbox mode: with `allowedProcedures` set, the list is applied as an allowlist only; when `false` (default) the same list is also written to `dbms.security.procedures.unrestricted`. |

## Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `conditions` | `[]metav1.Condition` | A `Ready` condition derived from `phase` (`Ready` → `True`; `Failed`/`Invalid` → `False`; `Disabled` → `False`, reason `Disabled`; `Pending`/`Waiting`/`Installing` → `Unknown`), and `ProceduresHidden` (`True`, reason `NotInAllowlist`) while the target's effective `dbms.security.procedures.allowlist` loads none of the plugin's procedures — see [Shared procedure allowlist](#shared-procedure-allowlist) |
| `phase` | `string` | Current phase: `"Pending"`, `"Installing"`, `"Ready"`, `"Failed"`, `"Waiting"`, `"Invalid"` (spec failed validation; not retried until the CR is edited), `"Disabled"` (`spec.enabled: false`, and nothing of the plugin is on its target) |
| `message` | `string` | Human-readable status message |
| `installedVersion` | `string` | The version the operator last installed successfully: `spec.version` as it was when the plugin reached `Ready` (the version *requested*, not read back from the running plugin). Left unchanged while a later attempt is `Installing` or `Failed`, so it always names the last install that worked |
| `installationTime` | `*metav1.Time` | When the plugin was first recorded as installed at `installedVersion`: set on reaching `Ready`, not touched by later reconciles or by `Ready` → `Installing` → `Ready` cycles at the same version. A different `spec.version` is a new installation and restarts it |
| `health` | [`*PluginHealth`](#pluginhealth) | **Reserved — never populated today.** Intended: plugin health and performance information |
| `usage` | [`*PluginUsage`](#pluginusage) | **Reserved — never populated today.** Intended: plugin usage statistics |
| `observedGeneration` | `int64` | Generation of the most recently observed spec |

### PluginHealth

> **Reserved — never populated today** (also applies to `PluginPerformance` and `PluginUsage` below).

Plugin health and performance metrics.

| Field | Type | Description |
|-------|------|-------------|
| `status` | `string` | Plugin health status |
| `lastHealthCheck` | `*metav1.Time` | Last health check timestamp |
| `errors` | `[]string` | Error messages from health checks |
| `performance` | [`*PluginPerformance`](#pluginperformance) | Performance metrics |

### PluginPerformance

Plugin performance statistics.

| Field | Type | Description |
|-------|------|-------------|
| `memoryUsage` | `string` | Current memory usage |
| `cpuUsage` | `string` | Current CPU usage |
| `executionCount` | `int64` | Number of procedure executions |
| `avgExecutionTime` | `string` | Average execution time |

### PluginUsage

Plugin usage analytics.

| Field | Type | Description |
|-------|------|-------------|
| `proceduresCalled` | `map[string]int64` | Count of procedure calls by name |
| `lastUsed` | `*metav1.Time` | Last time plugin was used |
| `usageFrequency` | `string` | Usage frequency classification |

## Supply-chain

The Neo4j Enterprise Docker entrypoint resolves `NEO4J_PLUGINS` at pod
startup. For a plugin the image ships, that's deterministic — the JAR is
copied to `/plugins/`: **APOC core** from `labs/`, and from `products/`
**GenAI**, **Graph Data Science** and **Fleet Management** (the Operations
Manual lists them as pre-installed; Bloom too, on CalVer). The entrypoint
copies one of these from the image when it is there and downloads it only
when it is not. **The other plugins it knows** (`apoc-extended`, and
`n10s` on 5.26) are **downloaded from the internet on every pod start**,
which has three production consequences (a name it does not know is
refused, see [Names the entrypoint installs](#names-the-entrypoint-installs)):

- **Non-reproducibility** — a restart can pull a different artifact than
  the one originally validated.
- **Egress requirement** — air-gapped clusters cannot permit the
  outbound HTTP.
- **Supply-chain exposure** — every pod start is an unauthenticated fetch.

The operator offers two postures.

### `installMode: PreBaked` (recommended for production)

Build a custom Neo4j image with the plugin JAR copied into `/plugins/`
(or `/var/lib/neo4j/plugins/`), reference that image from
`spec.image.repo`/`tag` on the cluster or standalone CR, and set
`installMode: PreBaked` on the `Neo4jPlugin`. The operator does **not**
touch `NEO4J_PLUGINS` — no runtime fetch happens — but still writes the
plugin's required configuration (security allowlists, unrestricted
procedures, ConfigMap entries for standalone). You get the declarative
CRD UX and a pinned, signed, scannable artifact.

```dockerfile
FROM neo4j:2025.01.0-enterprise
COPY graph-data-science-2.13.0.jar /var/lib/neo4j/plugins/
```

### A checksum for `source.type: url` and `custom`

The validator requires a `source.checksum` for any `source.type: url`
or `source.type: custom`. The checksum format is enforced:

- `sha256:` followed by exactly 64 hex characters, **or**
- `sha512:` followed by exactly 128 hex characters.

SHA1, MD5, and unprefixed hex are rejected. SHA1/MD5 because they are
not collision-resistant; unprefixed hex because verification tooling
should never have to guess the algorithm.

**`source.url` and `source.checksum` are used by `VerifiedDownload`
only.** In `Managed` mode the operator writes the plugin's name to
`NEO4J_PLUGINS` and the entrypoint installs from its own list by name;
it never reads `source`, and a name it does not list is refused
([Names the entrypoint installs](#names-the-entrypoint-installs)). A
JAR fetched from a URL at runtime takes the VerifiedDownload mode
described next, which checks it before Neo4j starts.

### `installMode: VerifiedDownload` (verified download via init container)

When `installMode: VerifiedDownload` is set, the operator injects an init
container into the target StatefulSet's pod template. The init container
downloads `spec.source.url`, computes the SHA256/SHA512 of the downloaded
file, compares it against `spec.source.checksum`, and writes the JAR to
the shared `/plugins` emptyDir **before** Neo4j starts. `NEO4J_PLUGINS`
is **not** mutated for this plugin (same skip semantics as PreBaked) —
the upstream entrypoint's own download path is bypassed entirely, so it
can't race the verified JAR.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
spec:
  clusterRef: my-cluster
  name: graph-data-science
  version: "2.13.0"
  installMode: VerifiedDownload
  source:
    type: url
    url: https://github.com/neo4j/graph-data-science/releases/download/2.13.0/neo4j-graph-data-science-2.13.0.jar
    checksum: sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

**Failure mode**: on checksum mismatch the init container exits non-zero,
the pod stays Pending, and `kubectl describe pod` shows the failure on
the init container's `state.terminated.message`; the plugin stays
`Installing` with a message naming the pod and the exit code. Fix the URL
or checksum (or the target's `spec.trustedCASecrets`): the plugin writes the
fix even though its target is not `Ready`, with one
`PluginInitContainerFailing` Warning, when every server that is down is
failing in this plugin's init container, those are the highest-numbered
servers, and Neo4j reports no other server unavailable. Otherwise the fix
waits like any install. On a standalone the plugin then deletes the stuck
pod, which its StatefulSet would not replace.

**Coexistence with the cluster controller**: the init container is named
`plugin-init-<name>`, its volumes `plugin-auth-<name>` and `plugin-ca`
(shared by every VerifiedDownload plugin of the target). The cluster
controller keeps init containers and volumes it did not render across its
own template changes (an image upgrade, a config change); it records the
ones it renders in the StatefulSet annotations
`neo4j.com/cluster-controller-init-containers` and
`neo4j.com/cluster-controller-volumes`. A `Ready` plugin whose init
container or volumes were removed anyway puts them back (`PluginMissing`
event). Uninstalling the last VerifiedDownload plugin also removes
`plugin-ca`. The plugin controller is the only writer of `plugin-ca`; its
content follows the target's `spec.trustedCASecrets` (see **Internal CAs**).

**Authenticated mirrors** are supported via `spec.source.authSecret`.
The named Secret should carry either a `token` key (used as
`Authorization: Bearer <token>`) or a `header` key (used verbatim as the
full `Authorization:` header value). Mounted at `/etc/plugin-auth` and
consumed by the init script.

**Internal CAs** are supported via the owning cluster's (or standalone's)
`spec.trustedCASecrets` list. Each Secret's `ca.crt` (or the per-Secret
`Key` override) is mounted under `/etc/plugin-ca`; the init script appends
the certificates to the init image's default CA bundle and points curl at
the result via `--cacert`, so public URLs keep working. The default bundle
is `$CURL_CA_BUNDLE` when it names a file (as in `curlimages/curl`), else
the first of `/etc/ssl/certs/ca-certificates.crt`,
`/etc/pki/tls/certs/ca-bundle.crt`, `/etc/ssl/cert.pem` and `/cacert.pem`
that exists; an image with none trusts only the listed CAs and logs a
`WARNING`.

The `plugin-ca` volume follows the list: when you add, remove or replace a
Secret in `spec.trustedCASecrets`, a `Ready` plugin updates the volume (and
its init container's mount of it, when the list becomes empty or stops being
empty) without a `PluginMissing` event — phase `Installing`, message
"Updating the plugin's trusted CAs: …". That write restarts the servers, so it
waits like an install until the target is `Ready` with every server available
and has rolled out its own change (`Waiting`, one `PluginInstallDeferred`):
the target's own truststore roll comes first, then the plugin's, and the
plugin is `Ready` once its roll has reached every server. A list that
is only reordered changes nothing. The volume's projections are optional and
the init script uses the default CAs when `/etc/plugin-ca` holds no
certificate, so a Secret you delete right after removing it from the list
does not stop a restarting server; the JAR is still checked against
`spec.source.checksum`. A volume an earlier operator version wrote keeps
required projections until the list next changes, so on the first change
after upgrading, delete a removed CA's Secret only once the plugin is `Ready`
again.

**Init container image** defaults to `curlimages/curl:8.5.0`. Override
via the Helm chart's `pluginInitContainer.image` value when installing
into air-gapped clusters that need a mirrored image.

**Limitations**:

- `spec.source.type` must be `url` or `custom` — the entrypoint's
  `official`/`community` types resolve via an internal manifest the
  user can't point at a verifiable URL, so the API server rejects them
  at apply time (a CRD validation rule — the operator has no webhooks).
- `spec.dependencies` are rejected on a VerifiedDownload plugin —
  mixed install paths in a single CR confuse the supply-chain story.
  Create each dependency as its own `Neo4jPlugin` CR with its own
  `source.url` + `checksum`.
- Every pod restart re-runs the init container, which re-downloads the
  JAR. For large JARs (GDS ≈ 30 MB) this is bandwidth-wasteful.

### Duplicate-CR protection (all install modes)

Two `Neo4jPlugin` CRs in the same namespace that target the **same
`spec.clusterRef`** with the **same `spec.name`** would race on the
same `/plugins` directory and the same `NEO4J_PLUGINS` env value — one
controller adds, the other removes, restart cycle. The reconciler
detects this and refuses to install the duplicate:

- The **oldest CR (by `creationTimestamp`)** keeps reconciling
  normally. UID is the tiebreaker on identical timestamps.
- The **newer duplicate** is marked `status.phase=Failed` with a
  message naming the older CR, and a `PluginDuplicate` Warning Event
  is emitted. It stops reconciling until the older CR is deleted.
- A duplicate that's mid-delete (DeletionTimestamp set) doesn't block
  its replacement — a stuck finalizer on the older CR can't lock out
  the survivor.

Action when you see `phase=Failed` with a duplicate message: delete
the unwanted CR. The surviving CR reconciles on its next watch event
(triggered by the deletion) and takes ownership.

## Examples

### APOC Plugin for Cluster

Install APOC plugin on a Neo4jEnterpriseCluster:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: cluster-apoc-plugin
  namespace: default
spec:
  # References a Neo4jEnterpriseCluster
  clusterRef: my-cluster

  # Plugin identification
  name: apoc
  version: "5.26.0"  # Must match Neo4j version
  enabled: true

  # Plugin source (official Neo4j repository)
  source:
    type: official

  # APOC-specific configuration (becomes NEO4J_apoc_* env vars)
  config:
    "apoc.export.file.enabled": "true"
    "apoc.import.file.enabled": "true"
    "apoc.import.file.use_neo4j_config": "true"
    "apoc.trigger.enabled": "true"

  # Security configuration
  security:
    allowedProcedures:
      - "apoc.*"
    securityPolicy: "permissive"   # strict | moderate | permissive (validated only; no runtime effect today)
```

**Result**: Updates `my-cluster-server` StatefulSet with:

- `NEO4J_PLUGINS=["apoc"]`
- `NEO4J_apoc_export_file_enabled=true`
- `NEO4J_apoc_import_file_enabled=true`
- Security settings for APOC procedures

### Graph Data Science Plugin for Standalone

Install GDS plugin with dependencies on a Neo4jEnterpriseStandalone:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: standalone-gds-plugin
  namespace: default
spec:
  # References a Neo4jEnterpriseStandalone
  clusterRef: my-standalone

  # Plugin identification
  name: graph-data-science
  version: "2.10.0"
  enabled: true

  # Plugin source - official Neo4j repository
  source:
    type: official

  # Plugin dependencies (automatically included in installation)
  dependencies:
    - name: apoc
      versionConstraint: ">=5.26.0"
      optional: false

  # GDS-specific configuration. Mount the license file at /licenses/gds.license
  # via the cluster/standalone spec.extraVolumes + spec.extraVolumeMounts
  # (e.g. from a Secret named gds-license-secret).
  config:
    "gds.enterprise.license_file": "/licenses/gds.license"
    "gds.procedure.allowlist": "gds.*"
    "gds.graph.store.max_size": "2GB"

  # Security configuration
  security:
    allowedProcedures:
      - "gds.*"
      - "apoc.load.*"  # APOC dependency procedures
    securityPolicy: "strict"   # strict | moderate | permissive (validated only; no runtime effect today)
    sandbox: false  # GDS requires full access

  # Reserved — validated but not applied today. Size GDS memory/CPU via the
  # standalone's own spec.resources and spec.config instead.
  resources:
    memoryLimit: "2Gi"
    cpuLimit: "1"
    threadPoolSize: 8
```

**Result**: Updates `my-standalone` StatefulSet with:

- `NEO4J_PLUGINS=["apoc", "graph-data-science"]` (dependencies included)
- GDS-specific environment variables
- Security settings for both APOC and GDS procedures

(`spec.resources` on the `Neo4jPlugin` does not allocate anything — see the field table.)

### Custom Plugin Example

Install a plugin from a custom (private) location. `type: custom` needs an https `url` and a `checksum`; `authSecret` (keys `token` or `header`) is used by `installMode: VerifiedDownload`:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: custom-plugin
spec:
  clusterRef: my-cluster
  name: my-custom-plugin
  version: "1.0.0"
  installMode: VerifiedDownload

  # Custom source (url + checksum are required)
  source:
    type: custom
    url: "https://my-registry.example.com/plugins/my-custom-plugin-1.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
    authSecret: registry-credentials

  # Security settings
  security:
    allowedProcedures:
      - "custom.*"
    sandbox: true
```

### URL Plugin Example

Install a plugin directly from a URL (`VerifiedDownload`: in `Managed` mode the URL is not read, and the name is refused):

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: url-plugin
spec:
  clusterRef: my-cluster
  name: direct-download-plugin
  version: "2.0.0"
  installMode: VerifiedDownload

  # Direct URL source with checksum verification
  source:
    type: url
    url: "https://example.com/plugins/my-plugin-2.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
```

## Supported Plugins

### Official Neo4j Plugins

The names below are the ones `Managed` mode installs; which of them a release lists is in [Names the entrypoint installs](#names-the-entrypoint-installs). Neo4j Streams is not a plugin of any supported release, and GraphQL has none either: the [Neo4j GraphQL Library](https://neo4j.com/docs/graphql/current/) is a JavaScript library you run in your own GraphQL server, a client of the database (the old `neo4j-graphql` server extension stopped at Neo4j 3.5).

| Plugin | Name | Description | Configuration Method | Automatic Security |
|--------|------|-------------|---------------------|-------------------|
| **APOC** | `apoc` | Awesome Procedures on Cypher | Environment Variables | ❌ Manual setup |
| **APOC Extended** | `apoc-extended` | Extended APOC procedures | Environment Variables | ❌ Manual setup |
| **Graph Data Science** | `graph-data-science` | Advanced graph algorithms | Neo4j Config + Security | ✅ Auto-configured |

### Enterprise Plugins

| Plugin | Name | Description | License Required | Automatic Security |
|--------|------|-------------|------------------|-------------------|
| **Bloom** | `bloom` | Graph visualization | ✅ Commercial License | ✅ Auto-configured |
| **GenAI** | `genai` | AI/ML integration (ships with Neo4j in `products/`) | ❌ None | ❌ Manual setup |

## Automatic Security Configuration

Some plugins require specific security settings to function properly. The operator automatically applies these settings even when no user configuration is provided.

### Plugins with Automatic Security

**Bloom Plugin**:

- Automatically applies required security settings for proper operation
- No manual configuration needed for basic functionality
- Automatically configured settings:
  - `NEO4J_dbms_security_procedures_unrestricted=bloom.*`
  - `NEO4J_dbms_security_http__auth__allowlist=/,/browser.*,/bloom.*`
  - `NEO4J_server_unmanaged__extension__classes=com.neo4j.bloom.server=/bloom`

**Graph Data Science Plugin**:

- Automatically applies default security settings
- User security configuration can override defaults
- Automatically configured settings:
  - `dbms.security.procedures.unrestricted=gds.*,apoc.load.*` (a shared list: on a cluster the union of what the target's plugins put there, on a standalone merged into its `neo4j.conf`)

### Examples with Automatic Security

**Bloom Plugin (Zero Configuration)**:
```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: bloom-plugin
spec:
  clusterRef: my-cluster
  name: bloom
  version: "2.15.0"
  # No config or security section needed
  # All required security settings applied automatically
```

**GDS Plugin with Custom Security**:
```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: gds-plugin
spec:
  clusterRef: my-cluster
  name: graph-data-science
  version: "2.10.0"
  # User security settings override automatic ones
  security:
    sandbox: true  # Uses allowlist instead of unrestricted
    allowedProcedures: ["gds.*", "apoc.load.*"]
    # Results in: NEO4J_dbms_security_procedures_allowlist=gds.*,apoc.load.*
```

**Automatic vs Manual Security Configuration**:

| Configuration Type | Applies When | Environment Variables | Override Behavior |
|-------------------|--------------|----------------------|-------------------|
| **Automatic** | No user `security` section | Applied automatically | Overridden by user config |
| **User-Provided** | User defines `security` section | User settings + automatic defaults | User settings take precedence |
| **Mixed** | User partially configures security | Automatic + user settings merged | User settings override matching keys |

### Community Plugins

| Plugin | Name | Description | Configuration |
|--------|------|-------------|---------------|
| **Neo Semantics (N10s)** | `n10s` | RDF/ontology support. `Managed` on 5.26 (and CalVer before 2025.05) only; on CalVer install its JAR with `VerifiedDownload` (see the [plugins guide](../user_guide/guides/plugins.md#which-plugins-managed-installs)) | Neo4j Config |
| **Custom Plugins** | any | User-defined plugins, with `VerifiedDownload` or `PreBaked` — a custom name is refused in `Managed` mode | Flexible |

### Plugin-Specific Configuration

**APOC (Environment Variables)**:
```yaml
config:
  "apoc.export.file.enabled": "true"
  "apoc.import.file.enabled": "true"
  "apoc.trigger.enabled": "true"
  "apoc.jobs.pool.num_threads": "4"
```

**Graph Data Science (Neo4j Config)**:
```yaml
config:
  "gds.enterprise.license_file": "/licenses/gds.license"
  "gds.graph.store.max_size": "8GB"
  "gds.procedure.allowlist": "gds.*"
security:
  allowedProcedures:
    - "gds.*"
    - "apoc.load.*"
```

**Bloom (Automatic Security Configuration)**:
```yaml
# Minimal configuration - security settings applied automatically.
# Mount the license file at /licenses/bloom.license via the cluster/standalone
# spec.extraVolumes + spec.extraVolumeMounts (e.g. from a Secret named
# bloom-license-secret).
config:
  "dbms.bloom.license_file": "/licenses/bloom.license"
# Automatically applied by operator:
# - NEO4J_dbms_security_procedures_unrestricted=bloom.*
# - NEO4J_dbms_security_http__auth__allowlist=/,/browser.*,/bloom.*
# - NEO4J_server_unmanaged__extension__classes=com.neo4j.bloom.server=/bloom
```

## Plugin Status Phases

- **Pending**: Plugin resource created, waiting for processing
- **Waiting**: When the plugin has something to write to its StatefulSet, which restarts every server: waiting for the target to answer, or for it to be `Ready` with every server available: not while a cluster has a server down (`Degraded` condition `True`, or phase `Degraded`), nor while it is forming, rolling its servers or upgrading. A `PluginInstallDeferred` event (or, for a `Ready` plugin that lost something, a `PluginMissing` one) marks the start of the wait; the plugin checks again every 30 seconds
- **Installing**: Plugin installation in progress — including while the servers restart to take it (a pass waiting for that restart asks Neo4j nothing, so a standalone restarting its one pod does not turn the plugin `Waiting`), while a disabled plugin is removed (`spec.enabled: false`), while a `Ready` plugin restores what was removed from its target (`PluginMissing` event), and while a `VerifiedDownload` plugin brings its trusted CAs in line with the target's changed `spec.trustedCASecrets` ("Updating the plugin's trusted CAs: …"). While its roll is stuck, the message names the pods that cannot start ("…; cannot start: c-server-2 (container neo4j exited with code 1)")
- **Ready**: Plugin successfully installed and active. Re-checked whenever the target StatefulSet's pod template changes: if something removed what the plugin set (its `NEO4J_PLUGINS` entry, a variable it sets, a `VerifiedDownload` init container or volume), the plugin goes back to `Installing` and restores it — or `Waiting`, while the target is not `Ready` with every server available; if nothing is missing, the check writes nothing and the phase stays `Ready`, whatever state the target is in
- **Failed**: Plugin installation failed (also set on the newer of two duplicate `Neo4jPlugin` CRs, when the cluster's own spec sets a variable the plugin would have to change, and on a `Managed` plugin whose name the target's image cannot install — [Names the entrypoint installs](#names-the-entrypoint-installs); one `PluginUnknownToImage` Warning). A failed install is retried every 30 seconds; its `PluginInstallFailed` Warning is emitted once, not on each retry
- **Invalid**: The spec failed validation (for example an unsupported `securityPolicy`, a missing `source.url`/`checksum`, or a non-https URL); the controller does not requeue until the CR is edited
- **Disabled**: `spec.enabled: false`, and nothing of the plugin is on its target's servers (its removal written and rolled out, or nothing there to remove). One `PluginDisabled` event. Not a fault: the `Ready` condition is `False` with reason `Disabled`

### Shared procedure allowlist

`dbms.security.procedures.allowlist` is one server-wide list. Once any plugin of a target sets it (`spec.security.allowedProcedures`, or the key in `spec.config`) — or the target's own `spec.config` does — Neo4j loads only the procedures and functions it matches, and every other plugin's are not loaded at all: calls to them fail as unknown. Installing the GenAI example (`genai.*`, `ai.*`) beside an APOC plugin with no `security` block hides APOC that way. The operator does not widen the list for you — it is a security boundary someone set — but a plugin whose procedures the effective list loads none of carries the condition `ProceduresHidden` (`True`, reason `NotInAllowlist`), naming the list, where it is set and who sets it, with one `PluginProceduresHidden` Warning when it appears or changes. The plugin stays `Ready` (its JAR is on the servers). Add the plugin's namespace (`apoc.*`, …) to `allowedProcedures` of this plugin or of the one that sets the list; the condition goes when the list loads it again. Plugins the operator knows are judged (APOC, APOC Extended, GDS, Bloom, GenAI, N10s, Fleet Management); a custom plugin is not.

## Supported Plugin Sources

### Official and Community
`source.type: official` and `community` say only that the Neo4j entrypoint installs the plugin by name in `Managed` mode: from the image when it ships the JAR (APOC core, GDS, GenAI, Fleet Management, and Bloom on CalVer), otherwise from the plugin's published versions list (APOC Extended, and N10s on 5.26). Neither type changes what the operator writes. Which names a release installs: [Names the entrypoint installs](#names-the-entrypoint-installs).

### Custom Registry
Private plugin registries with authentication support, with `installMode: VerifiedDownload` (`source.url` + `checksum`, `authSecret`).

### Direct URL
Direct download from URLs with checksum verification, with `installMode: VerifiedDownload`.

## Installation Workflow

The `Neo4jPlugin` controller follows this comprehensive workflow:

### Phase 1: Validation and Discovery

1. **Target Validation**: Verifies `clusterRef` points to existing cluster or standalone
2. **Plugin Validation**: Checks plugin name, version compatibility, and source availability
3. **Dependency Analysis**: Resolves plugin dependencies and version constraints
4. **Conflict Detection**: Identifies conflicts with existing plugins

### Phase 2: Configuration Preparation

1. **Plugin Collection**: Assembles main plugin and dependencies into unified list
2. **Environment Variable Mapping**: Converts plugin config to `NEO4J_*` environment variables
3. **Security Configuration**: Applies plugin-specific security settings

### Phase 3: Deployment

1. **StatefulSet Update**: Patches target StatefulSet with plugin configuration
2. **Rolling Restart**: Initiates controlled pod restart sequence
3. **Health Monitoring**: Tracks pod restart progress and Neo4j startup
4. **Rolled out**: `Ready` once every server runs the new template; the operator does not query Neo4j for the plugin's procedures (the `ProceduresHidden` condition is judged from the configuration)

### Phase 4: Status and Monitoring

1. **Status Update**: Sets plugin phase to "Ready" and records installation time
2. **Health Tracking**: Reports the phase and message (the `health`/`usage` status blocks are reserved and not populated today)
3. **Error Handling**: Captures and reports installation failures
4. **Dependency Tracking**: Maintains dependency relationships

### Example Installation Timeline

```bash
# Plugin creation
kubectl apply -f apoc-plugin.yaml

# Phase progression
# 0s:  Phase: Pending
# 5s:  Phase: Installing (StatefulSet updated)
# 30s: Phase: Installing (pods restarting)
# 60s: Phase: Ready (plugin verified)
```

### Configuration Examples by Plugin Type

**APOC Plugin (Environment Variables)**:
```yaml
# Input configuration
config:
  "apoc.export.file.enabled": "true"
  "apoc.import.file.enabled": "true"
  "apoc.trigger.enabled": "true"

# Applied environment variables
env:
- name: NEO4J_PLUGINS
  value: '["apoc"]'
- name: NEO4J_apoc_export_file_enabled
  value: 'true'
- name: NEO4J_apoc_import_file_enabled
  value: 'true'
- name: NEO4J_apoc_trigger_enabled
  value: 'true'
```

**Graph Data Science (Environment + Config)**:
```yaml
# Input configuration
config:
  "gds.enterprise.license_file": "/licenses/gds.license"
  "gds.graph.store.max_size": "4GB"
security:
  allowedProcedures: ["gds.*", "apoc.*"]

# Applied configuration
env:
- name: NEO4J_PLUGINS
  value: '["apoc", "graph-data-science"]'
- name: NEO4J_gds_enterprise_license__file
  value: '/licenses/gds.license'
- name: NEO4J_gds_graph_store_max__size
  value: '4GB'
- name: NEO4J_dbms_security_procedures_unrestricted
  value: 'gds.*,apoc.*'
```

**Multiple Plugins with Dependencies**:
```yaml
# Multiple Neo4jPlugin resources
# Result: Combined environment variables
env:
- name: NEO4J_PLUGINS
  value: '["apoc", "graph-data-science", "bloom"]'
- name: NEO4J_apoc_export_file_enabled
  value: 'true'
- name: NEO4J_gds_enterprise_license__file
  value: '/licenses/gds.license'
- name: NEO4J_dbms_bloom_license__file
  value: '/licenses/bloom.license'
```

## Advanced Plugin Configurations

### Custom Plugin from URL

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: custom-plugin
spec:
  clusterRef: my-cluster
  name: my-custom-plugin
  version: "1.0.0"
  # A custom JAR: in Managed mode the name would be refused (the
  # entrypoint does not know it) and the URL never read
  installMode: VerifiedDownload

  # Custom plugin source
  source:
    type: url
    url: "https://my-registry.example.com/plugins/my-plugin-1.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
    authSecret: custom-registry-credentials

  # Custom configuration
  config:
    "custom.plugin.setting1": "value1"
    "custom.plugin.setting2": "value2"

  # Security restrictions
  security:
    allowedProcedures:
      - "custom.*"
    securityPolicy: "strict"   # strict | moderate | permissive (validated only)
    sandbox: true
```

### Plugin from a Private Mirror (VerifiedDownload)

Authenticated mirrors use `source.authSecret` with `installMode: VerifiedDownload`; the Secret carries a `token` key (sent as `Authorization: Bearer <token>`) or a `header` key. Trust for an internal CA comes from the cluster's `spec.trustedCASecrets`.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: private-registry-auth
type: Opaque
stringData:
  token: <registry-access-token>
---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: private-plugin
spec:
  clusterRef: enterprise-cluster
  name: enterprise-plugin
  version: "2.0.0"
  installMode: VerifiedDownload

  source:
    type: custom
    url: "https://private-registry.company.com/plugins/enterprise-plugin-2.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
    authSecret: private-registry-auth
```

### Production Plugin Setup with Monitoring

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: production-apoc
  labels:
    environment: production
    plugin-type: essential
spec:
  clusterRef: prod-cluster
  name: apoc
  version: "5.26.0"

  # Production configuration
  config:
    "apoc.export.file.enabled": "true"
    "apoc.import.file.enabled": "false"  # Disabled for security
    "apoc.trigger.enabled": "true"
    "apoc.jobs.pool.num_threads": "8"
    "apoc.spatial.geocode.provider": "osm"

  # Enhanced security
  security:
    allowedProcedures:
      - "apoc.export.*"
      - "apoc.trigger.*"
      - "apoc.periodic.*"
      - "apoc.meta.*"
    securityPolicy: "strict"   # strict | moderate | permissive (validated only)
    sandbox: false

  # Reserved — validated but not applied today (size the Neo4j pods via the
  # cluster's spec.resources instead)
  resources:
    memoryLimit: "512Mi"
    cpuLimit: "200m"
    threadPoolSize: 8
```

### Multi-Plugin Setup for Analytics Workload

```yaml
# APOC Foundation
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: analytics-apoc
spec:
  clusterRef: analytics-cluster
  name: apoc
  version: "5.26.0"
  config:
    "apoc.export.file.enabled": "true"
    "apoc.import.file.enabled": "true"
    "apoc.periodic.enabled": "true"

---
# Graph Data Science for Analytics
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: analytics-gds
spec:
  clusterRef: analytics-cluster
  name: graph-data-science
  version: "2.10.0"
  dependencies:
    - name: apoc
      versionConstraint: "5.26.0"
      optional: false
  config:
    # Mount the license file at /licenses/gds.license via the cluster/standalone
    # spec.extraVolumes + spec.extraVolumeMounts (e.g. from a Secret named
    # gds-enterprise-license).
    "gds.enterprise.license_file": "/licenses/gds.license"
    "gds.graph.store.max_size": "16GB"
    "gds.procedure.allowlist": "gds.*"
  resources:            # reserved — validated but not applied today
    memoryLimit: "8Gi"
    cpuLimit: "4"
    threadPoolSize: 16
```

(Neo4j Streams is not a plugin of 5.26 or CalVer: the entrypoint does not know `streams`, and the operator refuses it.)

## Troubleshooting

### Common Issues and Solutions

**Plugin Not Loading**:
```bash
# Check plugin installation status
kubectl get neo4jplugin <plugin-name> -o yaml

# Verify Neo4j logs for plugin loading
kubectl logs <pod-name> -c neo4j | grep -i "plugin\|apoc\|gds"

# Check available procedures
kubectl exec <pod-name> -c neo4j -- \
  cypher-shell -u neo4j -p password "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc'"
```

**Dependency Conflicts**:
```bash
# Check for version mismatches
kubectl describe neo4jplugin <plugin-name>

# Common conflicts:
# - APOC version doesn't match Neo4j version
# - GDS requires specific APOC version
# - Multiple plugins trying to load same dependency
```

**Resource Constraints**:
```bash
# Check pod resource usage
kubectl top pod <pod-name> --containers

# Monitor during plugin installation
kubectl logs <pod-name> -c neo4j | grep -i "outofmemory\|heap"

# Common issues:
# - Insufficient memory for GDS operations
# - CPU limits too low for parallel plugin loading
```

**License Issues (Commercial Plugins)**:
```bash
# Verify license secret exists
kubectl get secret <license-secret> -o yaml

# Check license file mounting
kubectl exec <pod-name> -c neo4j -- ls -la /licenses/

# Verify license in Neo4j
kubectl exec <pod-name> -c neo4j -- \
  cypher-shell -u neo4j -p password "SHOW PROCEDURES YIELD name WHERE name CONTAINS 'bloom'"
```

**Network and Source Issues**:
```bash
# Test plugin source connectivity
kubectl run test-plugin --rm -it --image=curlimages/curl -- \
  curl -I "https://repo1.maven.org/maven2/org/neo4j/procedure/apoc/"

# Check custom registry access
kubectl get secret <registry-auth-secret> -o yaml
```

### Performance Monitoring

> **Note:** `status.health` and `status.usage` are reserved and never populated today, so the commands below return nothing. Use `kubectl get neo4jplugin <name> -o jsonpath='{.status.phase}'` and the pod logs instead.

```bash
# Monitor plugin performance (reserved — empty today)
kubectl get neo4jplugin <plugin-name> -o jsonpath='{.status.health.performance}'

# Check plugin usage statistics
kubectl get neo4jplugin <plugin-name> -o jsonpath='{.status.usage.proceduresCalled}'

# View plugin health status
kubectl describe neo4jplugin <plugin-name> | grep -A 10 "Health:"
```

## Best Practices

1. **Version Compatibility**: Always match plugin versions with Neo4j version
2. **Dependency Management**: Let the controller handle dependency resolution
3. **Resource Planning**: Allocate sufficient memory for plugin operations (especially GDS)
4. **Security Configuration**: Use appropriate procedure allowlists and security policies
5. **License Management**: Store commercial plugin licenses in secure secrets
6. **Installation Order**: Install base plugins (APOC) before dependent plugins (GDS)
7. **Monitoring**: Regularly check the plugin `phase` and the Neo4j pod logs (the `status.health` metrics are reserved and not populated today)
8. **Updates**: Test plugin updates in development before production deployment
9. **Configuration**: Use environment variables for APOC, neo4j.conf for other plugins
10. **Troubleshooting**: Enable debug logging for plugin installation issues

For detailed plugin-specific guides, see:

- [Plugin Examples](https://github.com/priyolahiri/neo4j-kubernetes-operator/tree/main/examples/plugins)
- [Troubleshooting Guide](../user_guide/guides/troubleshooting.md)
- [Performance Tuning](../user_guide/guides/performance.md)
