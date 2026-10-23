# `validate`

Validate Neo4j manifests against the operator's own validators, before you apply them.


```bash
kubectl neo4j validate -f cluster.yaml
kubectl neo4j validate -f manifests/            # a directory
kubectl neo4j validate -f a.yaml -f b.yaml      # repeatable
helm template . | kubectl neo4j validate -f -   # stdin
```

Example output:

```
Neo4jEnterpriseCluster/prod (cluster.yaml):
  ✗ spec.config.dbms.default_database: Invalid value: "mydb": deprecated setting: use dbms.setDefaultDatabase() procedure instead
  ✗ spec.topology.serverRoles[0].serverIndex: Invalid value: 7: must be in range [0, 1]
  ⚠ Even number of servers (2) may reduce fault tolerance when databases specify odd-numbered server allocations. Consider using an odd number of servers for optimal fault tolerance.
  ⚠ 2 servers provide limited fault tolerance. If one server fails, databases may lose quorum. Consider using 3 or more servers for production deployments.

1 validated, 0 skipped — 2 error(s), 2 warning(s)
validated against operator rules v1.17.0
```

Errors are listed before warnings, each sorted by field path.

### Flags

| Flag | Meaning |
|---|---|
| `-f` | File, directory, or `-` for stdin. Repeatable. A directory is read one level deep (`.yaml`, `.yml`, `.json`), not recursively. |
| `--connect` | Connect to the current kubeconfig context to check cross-references. |
| `--context` | Kubeconfig context to use (implies `--connect`). |
| `--kubeconfig` | Path to the kubeconfig file (implies `--connect`). |
| `--namespace`, `-n` | Namespace for manifests that omit one. With `--connect`, it defaults to the kubeconfig context's namespace, else `default` — where `kubectl apply` would put them. |
| `--strict` | Treat warnings as errors (exit non-zero on warnings). Pending is unaffected. |
| `--quiet` | Print findings only — no per-file "ok" lines, no summary. |

### Exit codes

These are a stable contract, so you can rely on them in CI:

| Code | Meaning |
|---|---|
| `0` | No errors. Warnings alone do not fail unless `--strict`. |
| `1` | At least one validation error (or a warning under `--strict`). |
| `2` | Usage problem: bad flags, unreadable file, undecodable YAML, or (with `--connect`) a cluster that could not be reached. |

## What it checks — and what it doesn't

### Misspelled fields

A key the kind does not have is reported with its full path, one line per field:

```
$ kubectl neo4j validate -f standalone.yaml
Neo4jEnterpriseStandalone/db (standalone.yaml):
  ✗ spec.auth.secretRef: Invalid value: unknown field: this kind has no such
    field. Check the spelling against the API reference — the operator ignores
    it and the API server rejects the manifest.
```

This is the most common manifest error there is.

The check descends into lists of objects as well, and reports the index of the offending item — `spec.topology.serverRoles[0].modeContraint`, `spec.env[1].valueFrom.secretKeyRef.kee` — so a typo in `serverRoles`, `env`, a composite's `constituents` or any other list is caught the same way as one in a plain object. Lists of plain values (`[]string`, `[]int`) have no field names to misspell and are left alone.

Open maps are left alone, because every key in them is your data rather than a field name: `spec.config`, a composite constituent's `driverSettings`, labels and annotations. `metadata` and `status` are not walked either — they belong to Kubernetes, not to this operator.

This check is **offline**, so it runs on **every one of the 27 kinds** — including the ones whose cross-reference rules need `--connect`, and the ones with no operator-side validator at all. "No validator" is not "no spelling": the kinds governed only by their CRD schema can still be misspelled, and the API server is a slower place to find out. Such a document reports the typo *and* says what is still unchecked:

```
Neo4jShardedDatabase/products-sharded (sharded.yaml):
  ✗ spec.propertyShards: Invalid value: unknown field: …
  - not fully checked — Neo4jShardedDatabase validation resolves cross-references;
    re-run with --connect to check it
```

### It is not a replacement for `--dry-run=server`

`kubectl apply --dry-run=server` already enforces the **CRD schema**: field types, enums, numeric ranges, required fields, and the CEL immutability rules. `validate` does **not** duplicate that.

What it adds is everything that lives in the operator's validators *beyond* the schema — the rules the API server has no way to know about:

- cross-field topology rules, `serverRoles` index bounds and duplicate detection
- `spec.config` keys the operator rejects: deprecated 4.x settings, and keys the operator manages itself at runtime (advertised addresses, discovery, topology)
- `spec.config` values Neo4j would refuse at startup, for the settings whose type the operator knows — `db.tx_log.buffer.size: "1MiB"` (a long), `server.memory.heap.max_size: "2Gi"` (a Kubernetes suffix, not a Neo4j unit), `db.transaction.timeout: "30 sec"`; see [Value types](../configuration.md#value-types-checked-before-apply)
- TLS strictness and issuer-capability rules
- storage, image, and licence-acceptance requirements
- `spec.upgradeStrategy` timeouts (`upgradeTimeout`, `healthCheckTimeout`, `stabilizationTimeout`), which must be Go durations above zero such as `30m`, `1h30m` or `90s` — `"5 minutes"`, `"0s"` and `"-5m"` are refused, as the operator refuses them
- advisory topology warnings — even server counts, two-server clusters with
  limited fault tolerance, all-PRIMARY or all-SECONDARY mode constraints

The two are complementary. In CI, run both.

### What gets checked, and what doesn't

The operator has **27 CRD kinds, but only 13 have operator-side validators**. The command reports three distinct outcomes, and the distinction matters:

| | Kinds | Behaviour |
|---|---|---|
| **Checked offline** | `Neo4jEnterpriseCluster`, `Neo4jEnterpriseStandalone`, `Neo4jBackup`, `Neo4jPlugin`, `Neo4jDatabaseAlias`, `Neo4jCompositeDatabase`, `Neo4jReplicaDatabase` | Validated with no cluster |
| **Need `--connect`** | `Neo4jDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jShardedDatabase` | Skipped offline; validated when connected |
| **No validator at all** | the 12 Aura kinds, `Neo4jRestore`, `Neo4jReplicaPromotion` | Only the CRD schema applies — use `kubectl apply --dry-run=server` |

```
- Neo4jUser/analytics (users.yaml): skipped — Neo4jUser validation resolves cross-references; re-run with --connect to check it
- Neo4jRestore/restore-1 (restore.yaml): skipped — no operator-side validator for Neo4jRestore; its CRD schema still applies (kubectl apply --dry-run=server)
```

**Checked offline does not mean every rule ran.** One rule of a cluster, and the same rule of a standalone, compares the manifest with the deployment already running under its name: `spec.image.tag` against the version its servers run (`status.version`), which refuses a downgrade. A manifest carries no such state — a `status` in the file is ignored, as `kubectl apply` ignores it — so offline the summary says what was not checked. The same goes for a `Neo4jBackup`'s `spec.shardedDatabase`, which names an object in the cluster:

```
✓ Neo4jEnterpriseCluster/prod (cluster.yaml): ok

1 validated, 0 skipped — 0 error(s), 0 warning(s)
run with --connect to also check a Neo4jEnterpriseCluster's spec.image.tag against the version the cluster already runs
validated against operator rules v1.17.0
```

**Skipped is never a failure.** For the middle row, reporting "cluster not found" for a cluster that is merely unreachable would be a false error — worse than not checking — so the command declines to guess. For the bottom row, no amount of connecting will help, and the message says so rather than sending you after a check that does not exist.

### Checking cross-references with `--connect`

```bash
kubectl neo4j validate -f manifests/ --connect              # current kubeconfig context
kubectl neo4j validate -f manifests/ --context prod         # a specific context
kubectl neo4j validate -f manifests/ --connect -n team-a    # namespace for manifests that omit one
```

Connecting is **opt-in**. Defaulting to the current context would make a command documented as offline fail in CI, where there is usually no kubeconfig at all.

With a connection, the command resolves `clusterRef`s, password Secrets and role references, checks the admin password's shape in its Secret, and checks a cluster or standalone manifest against the deployment of the same name that already runs, as the operator will once it is applied:

```
Neo4jEnterpriseCluster/prod (cluster.yaml):
  ✗ spec.image.tag: Invalid value: "2025.10.0-enterprise": downgrades are not supported: the servers run 2026.08.1-enterprise (status.version), and Neo4j cannot go back to an earlier version. Set the tag to 2026.08.1-enterprise or later; running an earlier version means restoring a backup taken before the upgrade
```

For a standalone the message says `the standalone runs`. A deployment that does not exist yet has nothing to compare with. One the command cannot read (no `get` permission) is a warning saying the tag was not checked, rather than a silent pass.

A `Neo4jBackup`'s `spec.shardedDatabase` is resolved as the operator resolves it before a run: the `Neo4jShardedDatabase` with that CR name (`metadata.name`, not the logical `spec.name`) in the backup's namespace, on the backup's `instanceRef`. The operator fails a backup whose reference does not resolve, so the command reports it as an error, naming the CR when the value is a logical name:

```
Neo4jBackup/products-nightly (backups.yaml):
  ✗ spec.shardedDatabase: Invalid value: "products": Neo4jShardedDatabase "products" not found in namespace "neo4j"; Neo4jShardedDatabase "products-sharded" has spec.name "products", and spec.shardedDatabase takes the CR's metadata.name: set it to "products-sharded"
```

The connection also adds a third finding type:

```
Neo4jUser/analytics (users.yaml):
  … waiting for password Secret "analytics-password" in namespace "neo4j"

1 validated, 0 skipped — 0 error(s), 0 warning(s), 1 pending
```

**Pending is neither an error nor a warning.** It is the operator's own category for a dependency that is not satisfiable *yet* — a Secret you have not applied — as opposed to something wrong. Pending never affects the exit code, **including under `--strict`**: failing a pipeline because a Secret has not been created yet would punish a correct manifest.

The connection is also what makes version-skew detection possible. When connected, the CLI compares itself against the operator running in the cluster:

```
⚠ version skew: this CLI carries v1.17.0 rules, but the operator in neo4j-operator/neo4j-operator-controller-manager is v1.16.0.
  Rules added or removed between those releases are checked incorrectly here.
```

It is advisory and silent when it cannot tell — the operator may be in a namespace you cannot list, which is not worth failing over.

### Required permissions

`--connect` reads only. It needs `get` on `Neo4jEnterpriseCluster` (for a cluster manifest, the running cluster's status; for the other kinds, their `clusterRef`), `Neo4jEnterpriseStandalone` (likewise, for a standalone manifest the running standalone's status), `Neo4jRole` and `Secret` in the namespaces you are validating (plus `list` on `Neo4jUser` when validating a `Neo4jRoleBinding`, `get` on `Neo4jShardedDatabase` when validating a `Neo4jRole`, and `get` and `list` on `Neo4jShardedDatabase` when validating a `Neo4jBackup` with `spec.shardedDatabase`), plus `list` on `Deployment` for the version check (which degrades silently without it). It never writes anything.

### One run lists every error

A validator reports every error it finds, not just the first: an invalid image does not hide a bad `spec.config` key or storage size behind it, so you can fix the whole list before you re-run. The one exception is `Neo4jShardedDatabase`: when its `clusterRef` cannot be read, that is the error it reports, and its other rules wait until the cluster can be read.

A clean run covers the rules that ran. Offline, the ones that compare a manifest with what already runs did not, and the summary names them (see [above](#what-gets-checked-and-what-doesnt)).

### The operator's defaults are applied first

The operator fills in a few fields a cluster manifest may leave out before it validates one — `spec.tls.mode: disabled`, `native` authentication and authorization providers, a `ClusterIP` Service, `spec.storage.retentionPolicy: Delete`, `spec.image.pullPolicy: IfNotPresent` — and `validate` does the same, so a manifest is judged as the operator will judge it. The defaults go only into the copy that is validated: nothing is printed or written back, and a finding names a field you wrote, or one that is missing. A standalone gets no defaults from the operator, so none from `validate` either.

### Version skew

The output always names the ruleset it used:

```
validated against operator rules v1.17.0
```

The CLI carries the validation rules of the release it was built from. If your cluster runs a different operator version, a rule added later will not be checked and a rule since removed may still be enforced. **Keep the CLI on the same version as the operator you deploy.**

## In CI

Because it needs no cluster, it fits any pipeline that can run a binary:

```yaml
- name: Validate Neo4j manifests
  run: |
    curl -sSL "https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v${VERSION}/kubectl-neo4j_${VERSION}_linux_amd64.tar.gz" | tar -xz
    ./kubectl-neo4j validate -f manifests/ --strict
```

Or as a pre-commit hook:

```yaml
- repo: local
  hooks:
    - id: neo4j-validate
      name: Validate Neo4j manifests
      entry: kubectl-neo4j validate -f manifests/
      language: system
      files: '^manifests/.*\.ya?ml$'
      pass_filenames: false
```

`pass_filenames: false` matters: `validate` reads only the paths given with `-f`, and silently ignores any extra positional arguments, so a hook that let pre-commit append the staged filenames would validate just the first one.

`--strict` is the usual choice for CI: it makes advisory warnings — such as a two-server cluster that will lose quorum if one server fails — block the pipeline rather than scroll past.

## See also

- [Resource Sizing](../guides/resource_sizing.md) — the floors behind several of the warnings
- [`explain`](explain.md) — for a resource that applied cleanly but is not becoming Ready
