# Plugin Management

Install and manage Neo4j plugins (APOC, Graph Data Science, Bloom, GenAI, N10s, GraphQL, and custom JARs) declaratively via the `Neo4jPlugin` CRD. Works against both `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone` — the controller auto-detects the target from `spec.clusterRef`.

This guide is task-oriented. For the full field reference, see the [Neo4jPlugin API Reference](../../api_reference/neo4jplugin.md).

## Overview: choosing an install mode

The Neo4j Enterprise Docker entrypoint resolves the `NEO4J_PLUGINS` environment variable at pod startup. **Plugins the image ships are copied, not downloaded**: APOC core from `/var/lib/neo4j/labs/`, and GenAI, Graph Data Science and Fleet Management from `/var/lib/neo4j/products/` (the Operations Manual lists them as pre-installed; Bloom too, on CalVer) — no internet needed. The entrypoint downloads one of these only when the image does not have it. **Every other plugin** (`apoc-extended`, `n10s`) is **downloaded from the internet on every pod start** when installed this way.

`spec.installMode` controls how the JAR reaches the pod:

| Mode | How the JAR arrives | Egress required | Artifact pinned? | When to use |
|------|--------------------|-----------------|------------------|-------------|
| `Managed` (default) | Operator adds the plugin to `NEO4J_PLUGINS`; the Neo4j entrypoint installs it at pod start | Only for plugins the image does not ship (APOC core, GenAI, GDS and Fleet Management are copied from the image) | Pinned by the image for a plugin it ships; otherwise no — a restart can pull a different artifact | Plugins the image ships, anywhere; others in dev/test where re-download on restart is acceptable |
| `PreBaked` | You build a custom image with the JAR in `/plugins/`; operator writes **only** the plugin's configuration, never touches `NEO4J_PLUGINS` | None | Yes — pinned, signed, scannable image | **Recommended for production** and air-gapped clusters |
| `VerifiedDownload` | Operator injects an init container that downloads `source.url`, verifies it against `source.checksum`, and writes the JAR to `/plugins` **before** Neo4j starts | Yes (https to your mirror or the vendor) | Yes — checksum enforced at download time | Production when you can't build custom images but must pin the exact artifact |

Regardless of mode, the operator always writes the plugin's required configuration (security allowlists, unrestricted procedures, plugin settings) and triggers a rolling restart of the target StatefulSet to apply changes.

> **Multi-controller coexistence**: the plugin controller merges plugin names into `NEO4J_PLUGINS` additively — it never overwrites entries added by other controllers (e.g. the Aura Fleet Management reconciler). Installing APOC via `Neo4jPlugin` alongside `spec.auraFleetManagement` on the cluster yields `["apoc","fleet-management"]`, and neither entry is removed on subsequent reconciles. Fleet Management itself is managed via the cluster/standalone CRD, not via `Neo4jPlugin`.

## Quick starts

### APOC (works offline)

APOC core is pre-bundled in the Neo4j Enterprise image, so this works with no internet egress:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: apoc-plugin
spec:
  clusterRef: my-cluster        # Neo4jEnterpriseCluster OR Neo4jEnterpriseStandalone
  name: apoc
  version: "5.26.0"             # match your Neo4j version
  source:
    type: official
  config:
    # APOC settings become NEO4J_apoc_* environment variables (see "Plugin configuration")
    apoc.export.file.enabled: "true"
    apoc.import.file.enabled: "true"
    apoc.load.json.enabled: "true"
  security:
    allowedProcedures:
      - "apoc.load.*"
      - "apoc.export.*"
      - "apoc.import.*"
```

Wait for `status.phase: Ready`:

```bash
kubectl get neo4jplugin apoc-plugin -w
```

!!! warning "`allowedProcedures` is an allowlist, not a grant"

    It becomes `dbms.security.procedures.allowlist` (and, unless
    `sandbox: true`, `dbms.security.procedures.unrestricted` too). Neo4j then
    loads **only** the plugin procedures and functions it matches, server-wide.
    Everything else is not loaded at all and fails as *Unknown function* —
    with the example above, that includes `apoc.version()`, so a quick
    `RETURN apoc.version()` will fail even though APOC is installed. List what
    you use, or `apoc.*` for all of APOC; omit `security` to keep the operator's
    defaults.

### Graph Data Science (GDS)

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: gds-plugin
spec:
  clusterRef: my-cluster
  name: graph-data-science
  version: "2.10.0"
  source:
    type: community
  config:
    # GDS Enterprise license (optional). Mount the license file at
    # /licenses/gds.license via the cluster/standalone spec.extraVolumes +
    # spec.extraVolumeMounts (e.g. from a Secret).
    gds.enterprise.license_file: "/licenses/gds.license"
  security:
    allowedProcedures:
      - "gds.*"
```

The operator automatically adds the security configuration GDS needs: `dbms.security.procedures.unrestricted=gds.*,apoc.load.*`. With `Managed` mode the Neo4j entrypoint copies the GDS JAR the image ships in `products/` (and downloads it only from an image without one) — for a GDS version other than the image's, use `PreBaked` or `VerifiedDownload` (see [Supply-chain security](#supply-chain-security)).

### Bloom (commercial license required)

Bloom needs a commercial license. Mount the license file via the cluster/standalone CR's `spec.extraVolumes` + `spec.extraVolumeMounts` (e.g. from a Secret), then:

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: bloom-plugin
spec:
  clusterRef: my-cluster
  name: bloom
  version: "2.15.0"
  source:
    type: official
  config:
    dbms.bloom.license_file: "/licenses/bloom.license"
    # Optional: restrict Bloom access to specific roles
    dbms.bloom.authorization_role: "admin,architect"
```

Bloom's required settings are applied automatically — no manual security configuration needed:

- `dbms.security.procedures.unrestricted=bloom.*`
- `dbms.security.http_auth_allowlist=/,/browser.*,/bloom.*`
- `server.unmanaged_extension_classes=com.neo4j.bloom.server=/bloom`

After the rolling restart, Bloom is served at `http(s)://<neo4j-host>:<http-port>/bloom/`.

More examples (GenAI, standalone targets, multi-plugin setups): [examples/plugins/](https://github.com/priyolahiri/neo4j-kubernetes-operator/tree/main/examples/plugins).

## Custom and URL plugins

For plugins not in Neo4j's official/community catalogs, use `source.type: url` (direct download) or `source.type: custom` (private registry). Two rules are enforced — the https rule (and the `VerifiedDownload` gates below) also as CRD CEL validation, so `kubectl apply` of a violating CR is usually rejected by the apiserver before the operator sees it; the controller-side validator enforces them again (and the checksum rule), and a CR that reaches it goes to `status.phase: Invalid` and stops reconciling until you fix the spec:

### 1. The URL must be `https://`

`http://`, `file://`, and every other scheme is rejected. Plugin JARs are fetched over the network, and a non-https scheme has no transport integrity — a network attacker could swap the JAR in transit. If you host plugins internally, serve them from an https endpoint (an in-cluster mirror with a cert-manager certificate works).

### 2. A checksum is required

For `type: url` and `type: custom`, `source.checksum` is mandatory and must match:

- `sha256:` followed by exactly 64 hex characters, **or**
- `sha512:` followed by exactly 128 hex characters.

SHA1, MD5, and unprefixed hex are rejected — supply-chain protection demands a collision-resistant hash, and verification tooling should never have to guess the algorithm. Compute one locally:

```bash
shasum -a 256 my-plugin-2.0.0.jar
# abcd1234...  my-plugin-2.0.0.jar
# → checksum: "sha256:abcd1234..."
```

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: url-plugin
spec:
  clusterRef: my-cluster
  name: my-custom-plugin
  version: "2.0.0"
  source:
    type: url
    url: "https://artifacts.example.com/plugins/my-plugin-2.0.0.jar"
    checksum: "sha256:abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
```

> **Important — when is the checksum enforced?** In the default `Managed` mode the upstream Neo4j Docker entrypoint does **not** verify this checksum at download time. The operator records it (and surfaces it on the StatefulSet) for audit and out-of-band verification. If you need enforcement at the moment of download, use `installMode: VerifiedDownload` (below) or pin the artifact with `PreBaked`.

### Private endpoints (`authSecret`)

For authenticated mirrors or registries, reference a Secret via `source.authSecret`. With `VerifiedDownload`, the Secret should carry either a `token` key (sent as `Authorization: Bearer <token>`) or a `header` key (used verbatim as the full `Authorization:` header value). Custom registries additionally support `source.registry` with its own `authSecret` and TLS configuration — see the [API reference](../../api_reference/neo4jplugin.md#pluginregistry).

## Supply-chain security

For a security reviewer, the question is: *what artifact runs in the pod, and who verified it?* The three modes answer it differently.

### `installMode: PreBaked` (recommended for production)

Build a custom image with the JAR copied in, reference it from the cluster/standalone CR's image spec, and set `installMode: PreBaked` on the `Neo4jPlugin`:

```dockerfile
FROM neo4j:2025.01.0-enterprise
COPY graph-data-science-2.13.0.jar /var/lib/neo4j/plugins/
```

The operator does **not** touch `NEO4J_PLUGINS` — no runtime fetch ever happens — but still writes the plugin's required configuration. You keep the declarative CRD UX while the artifact is pinned, signed, and scannable through your normal image pipeline. This is also the only fully offline option for plugins the Neo4j image does not ship.

### `installMode: VerifiedDownload`

When you can't bake images, `VerifiedDownload` closes the `Managed`-mode verification gap:

- **What is verified**: the SHA-256/SHA-512 of the downloaded JAR against `spec.source.checksum`.
- **When**: at pod startup, **before** Neo4j starts.
- **By what**: an operator-injected init container on the target StatefulSet's pod template. It downloads `spec.source.url`, computes the digest, compares, and writes the JAR to the shared `/plugins` emptyDir. `NEO4J_PLUGINS` is **not** mutated for this plugin, so the entrypoint's own (unverified) download path is bypassed entirely and cannot race the verified JAR.
- **On mismatch**: the init container exits non-zero and the pod stays `Pending`. `kubectl describe pod` shows the failure on the init container's `state.terminated.message`. Fix the URL or checksum; the next pod spawn re-runs the verification.
- **Kept across the cluster's own changes**: the cluster controller keeps init containers and volumes it did not render, so an image upgrade or any other template change of the cluster leaves the plugin's init container and its `plugin-auth-<name>` / `plugin-ca` volumes in place. If something removes them anyway (a StatefulSet recreated by a storage expansion, a hand edit), a `Ready` plugin puts them back with a `PluginMissing` event. Earlier operator versions dropped the init container at the cluster's next pass, so the servers ran without the JAR while the plugin stayed `Ready`; after upgrading, such a plugin restores it — which restarts the servers once.

```yaml
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jPlugin
metadata:
  name: apoc-verified
spec:
  clusterRef: my-cluster          # running 2026.08.1
  name: apoc
  version: "2026.08.1"
  installMode: VerifiedDownload
  source:
    type: url
    url: https://github.com/neo4j/apoc/releases/download/2026.08.1/apoc-2026.08.1-core.jar
    # sha256sum of that file; pin the digest of the exact JAR you reviewed
    checksum: sha256:8607c873df867aa1b3b8c9926d55262d09088d2cccd62b53a35f43cdc22f8433
```

The validator enforces additional gates for this mode:

- `source.url` and `source.checksum` are **mandatory**.
- `source.type` must be `url` or `custom` — `official`/`community` resolve via the Neo4j entrypoint's internal manifest and cannot be pointed at a verifiable URL.
- `spec.dependencies` are **rejected** — each dependency must be its own `Neo4jPlugin` CR with its own `source.url` + `checksum`, so the entire chain is verified.

**Internal CAs**: the owning cluster's (or standalone's) `spec.trustedCASecrets` certificates are mounted under `/etc/plugin-ca` and added to the downloader's default CAs, so an internal https mirror signed by your own CA works, and a public URL (a GitHub release) still does too. The default CAs are the init image's own bundle: `$CURL_CA_BUNDLE` if it names a file, otherwise the first of `/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt`, `/etc/ssl/cert.pem` and `/cacert.pem` that exists. The init container's log names the bundle it used. A mirrored init image with none of them trusts only your CAs, and its log says so with a `WARNING`.

The CAs the downloader trusts follow that list. When you add, remove or replace a Secret in `spec.trustedCASecrets`, the cluster (or standalone) first restarts its servers for its own truststore. Meanwhile the plugin is `Waiting` ("… updating the plugin's trusted CAs restarts every server", one `PluginInstallDeferred` event). Once the target is `Ready` with every server available and done rolling, the plugin updates its `plugin-ca` volume to the new list — phase `Installing` while the restart rolls, no `PluginMissing` event — which restarts the servers once more. The plugin is `Ready` again when that restart has reached every server. Reordering the list changes nothing. A Secret you delete as soon as it leaves the list does not stop a restarting server: the volume's projections are optional, and with no certificate left in `/etc/plugin-ca` the downloader uses its default CAs (the JAR is still checked against `spec.source.checksum`). The one exception is the first list change after upgrading the operator: a volume an earlier version wrote still requires its Secrets, so delete a removed CA's Secret only once the plugin is `Ready` again.

**When the download keeps a server from starting**: if the plugin's init container fails on a server (wrong checksum, unreachable URL, a CA the mirror needs is missing), that server stays in `Init:CrashLoopBackOff`, the cluster stays `Forming`, and the plugin stays `Installing` with a message naming the pod and the init container's exit code. Fix the plugin's `source` or the target's `spec.trustedCASecrets`: the plugin writes that fix even though its target is not `Ready`, with one `PluginInitContainerFailing` Warning. It does so only when every server that is down is failing in this plugin's init container, those are the highest-numbered servers, and Neo4j reports no other server unavailable; then the restart replaces the broken servers first. If any other server is down, the fix waits like any install. On a standalone the plugin also deletes the stuck pod once the fix is written, because its StatefulSet does not replace a pod that is not ready.

**Air-gapped clusters**: the init container image defaults to `curlimages/curl:8.5.0`, pulled from a public registry. Point it at an internal mirror via the operator Helm chart value:

```yaml
# values.yaml for the neo4j-operator chart
pluginInitContainer:
  image: registry.internal.example.com/mirror/curlimages/curl:8.5.0
```

Note that every pod restart re-runs the init container and re-downloads the JAR (≈ 30 MB for GDS) — budget bandwidth accordingly.

### Duplicate-CR protection (all modes)

Two `Neo4jPlugin` CRs in the same namespace targeting the same `clusterRef` with the same plugin `name` would race on the same `/plugins` directory and `NEO4J_PLUGINS` value. The reconciler refuses the duplicate: the **oldest CR wins**, the newer one is set to `status.phase: Failed` with a message naming the older CR, and a `PluginDuplicate` Warning event is emitted. Delete the unwanted CR; the survivor takes over on its next watch event.

## Plugin configuration

`spec.config` is a string map of plugin settings. Where each entry lands depends on the plugin:

- **Every key becomes an environment variable** on the target's servers, `NEO4J_` plus the key with `_` doubled and `.` turned into `_`: `apoc.export.file.enabled: "true"` becomes `NEO4J_apoc_export_file_enabled=true` on the StatefulSet. Neo4j reads these at startup. APOC (and APOC Extended) settings must go this way: Neo4j 5.26+ does not read them from `neo4j.conf`.
- **On a standalone, security and startup-only settings** (`dbms.security.*`, `server.unmanaged_extension_classes`, `dbms.bloom.*`, license files) go to its `neo4j.conf` instead of the env.
- **Procedure allowlisting** (`dbms.security.procedures.unrestricted`, allowlists) always goes through `neo4j.conf`-level configuration, for APOC too — that's what `spec.security` drives. On a standalone these land in its `neo4j.conf`, merged with the standalone's own `spec.config` (the procedure lists are unioned), so a plugin adds to your allowlist rather than replacing it. On a cluster each list is an environment variable that is exactly what the cluster's enabled plugins put there — their `spec.security`, their automatic settings, and the same list keys in their `spec.config`, unioned — rewritten with a plugin's write: a token a plugin no longer asks for goes, and a list key in a plugin's `spec.config` adds to the list instead of replacing it.

!!! warning "The allowlist is shared"

    `dbms.security.procedures.allowlist` is one list for the whole server. Once any plugin of a target sets it, Neo4j loads only what it matches, and the procedures of every other plugin are not loaded at all — installing the GenAI example (`genai.*`, `ai.*`) beside an APOC plugin with no `security` block makes `apoc.version()` fail. The operator does not widen the list, but the hidden plugin gets the condition `ProceduresHidden` (reason `NotInAllowlist`) and one `PluginProceduresHidden` Warning naming the list and who sets it. Add the hidden plugin's namespace (`apoc.*`) to the `allowedProcedures` of either plugin.

**List only settings your Neo4j version documents.** Neo4j refuses to start on a setting it does not know ("Unrecognized setting"), so a wrong key keeps every server it reaches from starting. The GenAI plugin, for one, needs no settings: its provider credentials are procedure arguments.

**Removing a key removes its variable.** The plugin records, on the target's StatefulSet, the variables it set (annotation `neo4j.com/plugin-env-vars.<plugin name>`), and when you remove a key from `spec.config` it takes that variable off the servers — one rolling restart, which, like an install, waits until the target is `Ready` with every server available. It never removes a variable the cluster or standalone sets from its own spec, or one another plugin of the target sets. If the setting is what keeps servers from starting, the removal does not wait: when every server that is down keeps failing in its `neo4j` container while running a variable the removal changes, those are the highest-numbered servers, and Neo4j reports no other server unavailable, the plugin writes it at once, with one `PluginSettingsFailing` Warning, and the restart replaces the failing servers first. On a standalone the plugin then deletes the stuck pod, which its StatefulSet would not replace. A plugin installed by an operator before v1.22.0 has no record yet: its first reconcile records what it sets now (which restarts nothing) and removes nothing, so a variable left behind by a key you removed before upgrading has to be removed from the StatefulSet by hand.

**Its `NEO4J_PLUGINS` names are recorded too** (annotation `neo4j.com/plugin-entries.<plugin name>`): dropping a dependency from `spec.dependencies`, or switching away from `Managed` mode, takes those names out of `NEO4J_PLUGINS` in the plugin's next write, unless another plugin of the target installs them. After a switch to `VerifiedDownload` the entrypoint no longer installs the plugin beside the verified JAR. A plugin installed by an earlier operator records its names on its first reconcile and removes nothing then.

**Deleting a plugin removes what it wrote**, in one rolling restart: its variables, its tokens in the procedure lists, its `NEO4J_PLUGINS` names (the ones it recorded, and in Managed mode its own and its dependencies', unless another plugin of the target installs them), and a `VerifiedDownload` plugin's init container and volumes. What the cluster or standalone sets itself, and what another plugin of the target needs, stays.

**Disabling a plugin** (`spec.enabled: false`) removes the same things in the same single write, and keeps the CR: phase `Installing` while the servers restart without it, then `Disabled`, with one `PluginDisabled` event. Like an install, the removal waits until the target is `Ready` with every server available — unless what it removes is what keeps the down servers failing (the plugin's own init container, or a setting Neo4j refuses), in which case it is written at once. Setting `spec.enabled: true` installs the plugin again. On a standalone, a plugin with settings in its `neo4j.conf` (an allowlist, say) costs two restarts either way: the standalone's own for its `neo4j.conf`, then the plugin's write.

Validation on `spec.config`: keys may contain only letters, digits, dots, underscores and dashes; values may not contain newline or carriage-return characters (they are rendered into config lines / env vars, so a stray newline could forge an extra setting).

Some plugins also receive **automatic security configuration** even with no `security` section — Bloom and GDS get their required unrestricted-procedures and allowlist entries applied by the operator; your own `security` settings override the automatic ones. See [Automatic Security Configuration](../../api_reference/neo4jplugin.md#automatic-security-configuration).

## Troubleshooting

### Validator rejections (`status.phase: Invalid`)

A spec that fails structural validation lands in `Invalid` with a `ValidationFailed` Warning event, and is **not requeued** — edit the CR to retry. (The https and `VerifiedDownload` rules below are also CEL rules on the CRD, so on a current cluster you will normally see them as an apply-time error from `kubectl` instead, with the same intent but different wording.) Common causes:

| Symptom in `status.message` / event | Cause | Fix |
|---|---|---|
| `URL scheme must be https` | `source.url` uses `http://`, `file://`, etc. | Host the JAR on an https endpoint (in-cluster mirror with a cert-manager certificate works) |
| `checksum is required for url and custom source types` | Missing `source.checksum` | `shasum -a 256 <jar>` and set `checksum: "sha256:<digest>"` |
| `checksum must be of the form sha256:<64 hex chars> or sha512:<128 hex chars>` | SHA1/MD5, unprefixed hex, wrong length | Use a prefixed sha256/sha512 digest |
| `installMode: VerifiedDownload requires spec.source.url` / `...checksum` | VerifiedDownload without a source | Add `source.url` + `source.checksum` |
| `VerifiedDownload requires source.type=url or source.type=custom` | `official`/`community` with VerifiedDownload | Point at a downloadable URL, or switch mode |
| `VerifiedDownload does not support spec.dependencies` | Dependencies on a VerifiedDownload CR | Create each dependency as its own `Neo4jPlugin` CR |

Compatibility-matrix notes (unknown plugin name, version below the recorded minimum) are **advisory only** — they surface as `ValidationWarning` events and never block installation. So does a field the schema accepts but nothing reads (`spec.resources`, `spec.source.registry` and its `tls`, `spec.security.securityPolicy`): the event reads `spec.<path> is accepted but has no effect today: <what to do instead>`.

```bash
kubectl describe neo4jplugin <name>          # status.message + events
kubectl get events --field-selector involvedObject.name=<name>
```

### Verifying the plugin landed

For **clusters**, check the `{cluster-name}-server` StatefulSet's environment variables:

```bash
kubectl get statefulset my-cluster-server \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="neo4j")].env}' | jq .
# Expect NEO4J_PLUGINS to include your plugin, plus NEO4J_* config vars
```

For **standalone**, check the StatefulSet (named after the standalone CR) the same way, and the `{standalone-name}-config` ConfigMap for `neo4j.conf`-level plugin settings (security allowlists etc.):

```bash
kubectl get configmap my-standalone-config -o jsonpath='{.data.neo4j\.conf}' | grep -i <plugin>
```

Then confirm Neo4j actually loaded the procedures:

```bash
kubectl exec <pod-name> -c neo4j -- \
  cypher-shell -u neo4j -p <password> \
  "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc' RETURN name LIMIT 5"
```

### Events to look for

| Event reason | Type | Meaning |
|---|---|---|
| `PluginInstalled` | Normal | Plugin installed: every server runs the new pod template. Emitted once per install, from the status write that makes the plugin `Ready` |
| `PluginDisabled` | Normal | `spec.enabled: false`, and nothing of the plugin is on its target's servers any more; phase `Disabled`. Emitted once |
| `PluginProceduresHidden` | Warning | The target's effective `dbms.security.procedures.allowlist` loads none of the plugin's procedures (condition `ProceduresHidden`). Emitted when the condition appears or its message changes |
| `PluginInstallFailed` | Warning | Installation failed — see the event message. Emitted once per failure; the controller retries every 30 seconds without repeating it. A server that is restarting or briefly unreachable is not a failure: the plugin stays `Installing` |
| `PluginMissing` | Warning | A `Ready` plugin's target StatefulSet no longer carries what the plugin set (its `NEO4J_PLUGINS` entry, a variable it sets, a `VerifiedDownload` init container or its volumes); the plugin goes back to `Installing` and restores it, which restarts the servers. If the target is not `Ready` with every server available, the event says so and the plugin is `Waiting` until it is |
| `PluginInstallDeferred` | Normal | The install — or a `VerifiedDownload` plugin's update of its trusted CAs after `spec.trustedCASecrets` changed, or the removal of a disabled plugin — would restart the servers of a target that is not `Ready` with every server available (`Degraded`, or still forming, rolling or upgrading); the plugin is `Waiting` and writes once the target is. Emitted once per wait |
| `PluginInitContainerFailing` | Warning / Normal | A `VerifiedDownload` plugin writes a fix of its own failing download to a target that is not `Ready` (Warning, once), or deletes a standalone's pod stuck in that download (Normal) |
| `PluginSettingsFailing` | Warning / Normal | The plugin writes a change of its settings to a target that is not `Ready`, because every server that is down keeps failing while running them (Warning, once), or deletes a standalone's pod stuck on the old settings (Normal) |
| `PluginDuplicate` | Warning | Another CR already owns this `(clusterRef, name)` pair — delete one |
| `ValidationFailed` | Warning | Spec rejected by the validator (hard error, no requeue) |
| `ValidationWarning` | Warning | Advisory compatibility note, or a set field that nothing reads — does not block install |

### Other common issues

- **`status.phase: Waiting`**: installing a plugin restarts every server, so the plugin waits — checking again every 30 seconds — until its target is `Ready` with every server available: on a cluster, not while a server is unavailable (`Degraded` condition `True`, or phase `Degraded`), where restarting the rest could cost the cluster its majority, nor while it is still forming, rolling its servers or upgrading. A `PluginInstallDeferred` event marks the start of the wait. Before a write it also waits while the target does not answer Bolt. The wait applies only to a change the plugin would write: a plugin waiting for the servers its own install restarted stays `Installing` and asks Neo4j nothing (on a standalone it used to flip to `Waiting` whenever its one pod was down), and a `Ready` plugin with nothing to restore stays `Ready` whatever state the cluster is in.
- **A `Ready` plugin goes back to `Installing` with a `PluginMissing` event**: something removed what the plugin set from the target's StatefulSet — its `NEO4J_PLUGINS` entry, a variable it sets, or a `VerifiedDownload` init container or volume. The controller re-checks a `Ready` plugin whenever that StatefulSet's pod template changes, and restores what is gone, which restarts the servers — so while the target is not `Ready` with every server available it is `Waiting` instead, and restores it once the target is. It never overrides a value someone else gave one of its variables.
- **`Failed` with "… comes from Neo4jEnterpriseCluster … itself"**: the cluster's own spec sets a variable the plugin needs to change — typically `NEO4J_PLUGINS` in `spec.env` without this plugin in it. The cluster controller would put its value back, restarting the servers each time, so the plugin does not write it. Add the plugin to the cluster's value, or remove the variable from `spec.env`; the plugin retries every 30 seconds and installs once the conflict is gone.
- **VerifiedDownload pod stuck `Pending`**: checksum mismatch or unreachable URL — `kubectl describe pod` and read the init container's termination message.
- **Servers crash-loop after a plugin install, `Installing` "… cannot start: c-server-2 (container neo4j exited with code 1)"**: Neo4j refused to start, typically on a `spec.config` key it does not know — `kubectl logs <pod> -c neo4j --previous` names the setting. Remove the key from the plugin: the plugin writes the removal even though the cluster is not `Ready` (see [Plugin configuration](#plugin-configuration)).
- **Plugin loads but procedures fail as unknown**: look for the `ProceduresHidden` condition (`kubectl describe neo4jplugin <name>`) — another plugin's, or your own, `allowedProcedures` set the shared allowlist without this plugin's namespace (see [Plugin configuration](#plugin-configuration)). Otherwise check `spec.security.allowedProcedures` and whether the plugin needs unrestricted procedures; for license-bound plugins (Bloom, GDS Enterprise) verify the license file is mounted (`kubectl exec <pod> -c neo4j -- ls /licenses/`).
- **`status.phase: Disabled`**: the plugin has `spec.enabled: false` and is off its target's servers, as asked. Set `spec.enabled: true` to install it again.

More general debugging recipes: [Troubleshooting Guide](troubleshooting.md).

## See also

- [Neo4jPlugin API Reference](../../api_reference/neo4jplugin.md) — every field, status phase, and the full supply-chain section
- [Plugin examples](https://github.com/priyolahiri/neo4j-kubernetes-operator/tree/main/examples/plugins) — APOC, GDS, Bloom, GenAI, cluster and standalone targets
- [Aura Fleet Management](../aura_fleet_management.md) — the fleet-management plugin is managed via the cluster CRD, not `Neo4jPlugin`
- [Configuration](../configuration.md) — cluster/standalone `spec.config`, `extraVolumes`, `extraVolumeMounts` for license files
