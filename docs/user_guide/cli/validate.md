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
validated against operator rules v1.15.0
```

Errors are listed before warnings, each sorted by field path.

### Flags

| Flag | Meaning |
|---|---|
| `-f` | File, directory, or `-` for stdin. Repeatable. |
| `--connect` | Connect to the current kubeconfig context to check cross-references. |
| `--context` | Kubeconfig context to use (implies `--connect`). |
| `--kubeconfig` | Path to the kubeconfig file (implies `--connect`). |
| `--namespace` | Namespace for manifests that omit one. |
| `--strict` | Treat warnings as errors (exit non-zero on warnings). Pending is unaffected. |
| `--quiet` | Print findings only — no per-file "ok" lines, no summary. |

### Exit codes

These are a stable contract, so you can rely on them in CI:

| Code | Meaning |
|---|---|
| `0` | No errors. Warnings alone do not fail unless `--strict`. |
| `1` | At least one validation error (or a warning under `--strict`). |
| `2` | Usage problem: bad flags, unreadable file, undecodable YAML. |

## What it checks — and what it doesn't

### It is not a replacement for `--dry-run=server`

`kubectl apply --dry-run=server` already enforces the **CRD schema**: field types, enums, numeric ranges, required fields, and the CEL immutability rules. `validate` does **not** duplicate that.

What it adds is everything that lives in the operator's validators *beyond* the schema — the rules the API server has no way to know about:

- cross-field topology rules, `serverRoles` index bounds and duplicate detection
- `spec.config` keys the operator rejects: deprecated 4.x settings, and keys the operator manages itself at runtime (advertised addresses, discovery, topology)
- TLS strictness and issuer-capability rules
- storage, image, and licence-acceptance requirements
- advisory topology warnings — even server counts, two-server clusters with
  limited fault tolerance, all-PRIMARY or all-SECONDARY mode constraints

The two are complementary. In CI, run both.

### What gets checked, and what doesn't

The operator has **26 CRD kinds, but only 12 have operator-side validators**. The command reports three distinct outcomes, and the distinction matters:

| | Kinds | Behaviour |
|---|---|---|
| **Checked offline** | `Neo4jEnterpriseCluster`, `Neo4jEnterpriseStandalone`, `Neo4jBackup`, `Neo4jPlugin`, `Neo4jDatabaseAlias`, `Neo4jReplicaDatabase` | Validated with no cluster |
| **Need `--connect`** | `Neo4jDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jShardedDatabase` | Skipped offline; validated when connected |
| **No validator at all** | the 12 Aura kinds, `Neo4jRestore`, `Neo4jReplicaPromotion` | Only the CRD schema applies — use `kubectl apply --dry-run=server` |

```
- Neo4jUser/analytics (users.yaml): skipped — Neo4jUser validation resolves cross-references; re-run with --connect to check it
- Neo4jRestore/restore-1 (restore.yaml): skipped — no operator-side validator for Neo4jRestore; its CRD schema still applies (kubectl apply --dry-run=server)
```

**Skipped is never a failure.** For the middle row, reporting "cluster not found" for a cluster that is merely unreachable would be a false error — worse than not checking — so the command declines to guess. For the bottom row, no amount of connecting will help, and the message says so rather than sending you after a check that does not exist.

### Checking cross-references with `--connect`

```bash
kubectl neo4j validate -f manifests/ --connect              # current kubeconfig context
kubectl neo4j validate -f manifests/ --context prod         # a specific context
kubectl neo4j validate -f manifests/ --connect -n team-a    # namespace for manifests that omit one
```

Connecting is **opt-in**. Defaulting to the current context would make a command documented as offline fail in CI, where there is usually no kubeconfig at all.

With a connection, the command resolves `clusterRef`s, password Secrets and role references, and adds a third finding type:

```
Neo4jUser/analytics (users.yaml):
  … waiting for password Secret "analytics-password" in namespace "neo4j"

1 validated, 0 skipped — 0 error(s), 0 warning(s), 1 pending
```

**Pending is neither an error nor a warning.** It is the operator's own category for a dependency that is not satisfiable *yet* — a Secret you have not applied — as opposed to something wrong. Pending never affects the exit code, **including under `--strict`**: failing a pipeline because a Secret has not been created yet would punish a correct manifest.

The connection is also what makes version-skew detection possible. When connected, the CLI compares itself against the operator running in the cluster:

```
⚠ version skew: this CLI carries v1.15.0 rules, but the operator in neo4j-operator/neo4j-operator-controller-manager is v1.14.0.
  Rules added or removed between those releases are checked incorrectly here.
```

It is advisory and silent when it cannot tell — the operator may be in a namespace you cannot list, which is not worth failing over.

### Required permissions

`--connect` reads only. It needs `get` on `Neo4jEnterpriseCluster`, `Neo4jEnterpriseStandalone`, `Neo4jRole` and `Secret` in the namespaces you are validating, plus `list` on `Deployment` for the version check (which degrades silently without it). It never writes anything.

### A clean run may not be the last word

The operator stops validating a resource after certain critical errors — an invalid image, for example — because later checks are not meaningful once the image is wrong. `validate` reproduces that faithfully rather than inventing its own behaviour, so:

> Fixing everything reported and re-running may surface **further** errors.

A clean run means nothing further is reachable, not that nothing was ever wrong. Re-run until two consecutive runs agree.

### Version skew

The output always names the ruleset it used:

```
validated against operator rules v1.15.0
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
      entry: kubectl-neo4j validate -f
      language: system
      files: '^manifests/.*\.ya?ml$'
```

`--strict` is the usual choice for CI: it makes advisory warnings — such as a two-server cluster that will lose quorum if one server fails — block the pipeline rather than scroll past.

## See also

- [Resource Sizing](../guides/resource_sizing.md) — the floors behind several of the warnings
- [`explain`](explain.md) — for a resource that applied cleanly but is not becoming Ready
