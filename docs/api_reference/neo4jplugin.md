# Neo4jPlugin API Reference

The `Neo4jPlugin` Custom Resource Definition (CRD) provides automated plugin installation and management for both Neo4j Enterprise clusters and standalone deployments.

## Overview

- **API Version**: `neo4j.neo4j.com/v1beta1`
- **Kind**: `Neo4jPlugin`
- **Target Deployments**: Both `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`
- **Installation Method**: Neo4j's `NEO4J_PLUGINS` environment variable approach
- **Supported Plugins**: APOC, Graph Data Science, Bloom, GraphQL, GenAI, N10s, and custom plugins
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
4. **Configuration Application**: Adds plugin-specific settings as `NEO4J_*` environment variables
5. **StatefulSet Update**: Patches the target StatefulSet with new configuration
6. **Rolling Restart**: Triggers controlled pod restarts to apply changes
7. **Verification**: Confirms plugin installation and updates status

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
- name: NEO4J_APOC_EXPORT_FILE_ENABLED
  value: 'true'
- name: NEO4J_APOC_IMPORT_FILE_ENABLED
  value: 'true'
```

**Notes**:

- APOC settings are applied via environment variables in Neo4j 5.26+
- Bloom/GDS/GenAI settings are applied via ConfigMap (standalone) or runtime configuration (cluster)
- Automatic dependency resolution and security defaults are applied as needed

## API Version

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
```

## Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `clusterRef` | `string` | ✅ | Name of target Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone |
| `name` | `string` | ✅ | Plugin name (e.g., "apoc", "graph-data-science") |
| `version` | `string` | ✅ | Plugin version to install (must match Neo4j version compatibility) |
| `enabled` | `boolean` | ❌ | Enable the plugin (default: `true`) |
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
| `conditions` | `[]metav1.Condition` | A single `Ready` condition derived from `phase` (`Ready` → `True`; `Failed`/`Invalid` → `False`; `Pending`/`Waiting`/`Installing` → `Unknown`) |
| `phase` | `string` | Current phase: `"Pending"`, `"Installing"`, `"Ready"`, `"Failed"`, `"Waiting"`, `"Invalid"` (spec failed validation; not retried until the CR is edited) |
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
startup. For **APOC core** that's deterministic — the JAR is bundled in
the image and just gets copied to `/plugins/`. For **every other plugin**
(`graph-data-science`, `bloom`, `genai`, `n10s`, `graphql`, `apoc-extended`)
the entrypoint **downloads from the internet on every pod start**, which
has three production consequences:

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

### `installMode: Managed` with a checksum (when you must download)

When the JAR must be fetched at runtime, the validator requires a
`source.checksum` for any `source.type: url` or `source.type: custom`.
The checksum format is enforced:

- `sha256:` followed by exactly 64 hex characters, **or**
- `sha512:` followed by exactly 128 hex characters.

SHA1, MD5, and unprefixed hex are rejected. SHA1/MD5 because they are
not collision-resistant; unprefixed hex because verification tooling
should never have to guess the algorithm.

**Honest limitation**: the upstream Neo4j Docker entrypoint does **not**
consume `source.checksum` at download time. The operator records the
field for audit and exposes it on the StatefulSet, but enforcement at
the moment of download requires either (a) pinning to PreBaked or (b)
the VerifiedDownload mode described next.

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
the init container's `state.terminated.message`. Fix the URL or checksum
and reconcile — the next pod spawn re-runs the init container.

**Authenticated mirrors** are supported via `spec.source.authSecret`.
The named Secret should carry either a `token` key (used as
`Authorization: Bearer <token>`) or a `header` key (used verbatim as the
full `Authorization:` header value). Mounted at `/etc/plugin-auth` and
consumed by the init script.

**Internal CAs** are supported via the owning cluster's (or standalone's)
`spec.trustedCASecrets` list. Each Secret's `ca.crt` (or the per-Secret
`Key` override) is mounted under `/etc/plugin-ca`; the init script
concatenates the certificates into a single CA bundle and points curl at
it via `--cacert`.

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

  # APOC-specific configuration (becomes NEO4J_APOC_* env vars)
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
- `NEO4J_APOC_EXPORT_FILE_ENABLED=true`
- `NEO4J_APOC_IMPORT_FILE_ENABLED=true`
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

Install a plugin directly from a URL:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: url-plugin
spec:
  clusterRef: my-cluster
  name: direct-download-plugin
  version: "2.0.0"

  # Direct URL source with checksum verification
  source:
    type: url
    url: "https://example.com/plugins/my-plugin-2.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
```

## Supported Plugins

### Official Neo4j Plugins

| Plugin | Name | Description | Configuration Method | Automatic Security |
|--------|------|-------------|---------------------|-------------------|
| **APOC** | `apoc` | Awesome Procedures on Cypher | Environment Variables | ❌ Manual setup |
| **APOC Extended** | `apoc-extended` | Extended APOC procedures | Environment Variables | ❌ Manual setup |
| **Graph Data Science** | `graph-data-science` | Advanced graph algorithms | Neo4j Config + Security | ✅ Auto-configured |
| **Neo4j Streams** | `streams` | Kafka/Pulsar integration | Neo4j Config | ❌ Manual setup |
| **GraphQL** | `graphql` | GraphQL endpoint | Neo4j Config | ❌ Manual setup |

### Enterprise Plugins

| Plugin | Name | Description | License Required | Automatic Security |
|--------|------|-------------|------------------|-------------------|
| **Bloom** | `bloom` | Graph visualization | ✅ Commercial License | ✅ Auto-configured |
| **GenAI** | `genai` | AI/ML integration | ✅ Commercial License | ❌ Manual setup |

## Automatic Security Configuration

Some plugins require specific security settings to function properly. The operator automatically applies these settings even when no user configuration is provided.

### Plugins with Automatic Security

**Bloom Plugin**:

- Automatically applies required security settings for proper operation
- No manual configuration needed for basic functionality
- Automatically configured settings:
  - `NEO4J_DBMS_SECURITY_PROCEDURES_UNRESTRICTED=bloom.*`
  - `NEO4J_DBMS_SECURITY_HTTP_AUTH_ALLOWLIST=/,/browser.*,/bloom.*`
  - `NEO4J_SERVER_UNMANAGED_EXTENSION_CLASSES=com.neo4j.bloom.server=/bloom`

**Graph Data Science Plugin**:

- Automatically applies default security settings
- User security configuration can override defaults
- Automatically configured settings:
  - `dbms.security.procedures.unrestricted=gds.*` and `dbms.security.procedures.allowlist=gds.*` (additive — merged with, never replacing, existing values)

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
    # Results in: NEO4J_DBMS_SECURITY_PROCEDURES_ALLOWLIST=gds.*,apoc.load.*
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
| **Neo Semantics (N10s)** | `n10s` | RDF/ontology support | Neo4j Config |
| **Custom Plugins** | `custom` | User-defined plugins | Flexible |

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
# - NEO4J_DBMS_SECURITY_PROCEDURES_UNRESTRICTED=bloom.*
# - NEO4J_DBMS_SECURITY_HTTP_AUTH_ALLOWLIST=/,/browser.*,/bloom.*
# - NEO4J_SERVER_UNMANAGED_EXTENSION_CLASSES=com.neo4j.bloom.server=/bloom
```

## Plugin Status Phases

- **Pending**: Plugin resource created, waiting for processing
- **Waiting**: Waiting for the deployment to be ready — on a cluster, also while any server is unavailable (`Degraded`), since installing restarts every server
- **Installing**: Plugin installation in progress
- **Ready**: Plugin successfully installed and active
- **Failed**: Plugin installation failed (also set on the newer of two duplicate `Neo4jPlugin` CRs)
- **Invalid**: The spec failed validation (for example an unsupported `securityPolicy`, a missing `source.url`/`checksum`, or a non-https URL); the controller does not requeue until the CR is edited

## Supported Plugin Sources

### Official Repository
Neo4j's official plugin repository (recommended for production):

- APOC (Awesome Procedures On Cypher)
- Neo4j Streams
- Neo4j GraphQL

### Community Repository
Community-maintained plugins:

- Graph Data Science (GDS)
- Additional APOC extensions
- Third-party plugins

### Custom Registry
Private plugin registries with authentication support.

### Direct URL
Direct download from URLs with checksum verification.

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
4. **Installation Verification**: Confirms plugin loading via Neo4j procedures

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
- name: NEO4J_APOC_EXPORT_FILE_ENABLED
  value: 'true'
- name: NEO4J_APOC_IMPORT_FILE_ENABLED
  value: 'true'
- name: NEO4J_APOC_TRIGGER_ENABLED
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
- name: NEO4J_GDS_ENTERPRISE_LICENSE_FILE
  value: '/licenses/gds.license'
- name: NEO4J_GDS_GRAPH_STORE_MAX_SIZE
  value: '4GB'
- name: NEO4J_DBMS_SECURITY_PROCEDURES_UNRESTRICTED
  value: 'gds.*,apoc.*'
```

**Multiple Plugins with Dependencies**:
```yaml
# Multiple Neo4jPlugin resources
# Result: Combined environment variables
env:
- name: NEO4J_PLUGINS
  value: '["apoc", "graph-data-science", "streams"]'
- name: NEO4J_APOC_EXPORT_FILE_ENABLED
  value: 'true'
- name: NEO4J_GDS_ENTERPRISE_LICENSE_FILE
  value: '/licenses/gds.license'
- name: NEO4J_STREAMS_SINK_TOPIC_CYPHER_NODES
  value: 'CREATE (n:Node {id: event.id})'
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

  # Custom plugin source
  source:
    type: url
    url: "https://my-registry.example.com/plugins/my-plugin-1.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
    authSecret: custom-registry-credentials   # only used with installMode: VerifiedDownload

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

---
# Neo4j Streams for Real-time Data
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: analytics-streams
spec:
  clusterRef: analytics-cluster
  name: streams
  version: "5.26.0"
  config:
    "streams.sink.enabled": "true"
    "streams.sink.topic.nodes": "graph-nodes"
    "streams.sink.topic.relationships": "graph-relationships"
    "kafka.bootstrap.servers": "kafka.analytics.svc.cluster.local:9092"
```

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
