# `kubectl-neo4j` CLI

A small companion CLI for this operator, distributed as a `kubectl` plugin. Today it does one thing: validate your Neo4j manifests **before** you apply them.

!!! warning "Support"
    The CLI ships with the operator, on the same tag, under the **same support terms as the operator itself**: best-effort via [GitHub issues](https://github.com/priyolahiri/neo4j-kubernetes-operator/issues), no official or commercial support, no SLA, and behaviour may change between releases without notice. A downloadable binary is not a more supported artifact than the operator it comes from.

## Why it exists

This operator has **no admission webhooks** — that is a deliberate architectural invariant, not an omission. The upside is a much simpler install with no certificate plumbing and no webhook outage mode. The downside is that spec mistakes are only reported *after* you apply, through `status` and events:

```
edit YAML → apply → wait for reconcile → read status → find the mistake → repeat
```

Across 26 resource kinds and 234 top-level spec fields, that loop is slow.

`kubectl neo4j validate` closes it. It runs **the operator's own validators** against your files, locally, with no cluster connection — so you get the same errors you would have got from the reconciler, before you apply.

## Install

=== "go install"

    The simplest path if you have Go, and the only one that needs no release
    asset at all — the module is published on the Go proxy:

    ```bash
    go install github.com/priyolahiri/neo4j-kubernetes-operator/cmd/kubectl-neo4j@v1.15.0
    ```

    It installs into `$(go env GOPATH)/bin` under the name `kubectl-neo4j`,
    which is exactly what `kubectl` needs for plugin discovery. Pin the version
    to the operator release you deploy — `@latest` will drift.

=== "Install script"

    Detects your OS and architecture, downloads the matching archive, and
    **verifies its checksum before installing** — it aborts rather than
    installing something it could not verify:

    ```bash
    curl -sSL https://raw.githubusercontent.com/priyolahiri/neo4j-kubernetes-operator/main/hack/install-cli.sh | sh
    ```

    Pipe-to-shell means trusting the network round trip. Downloading first and
    reading it is the better habit:

    ```bash
    curl -sSLO https://raw.githubusercontent.com/priyolahiri/neo4j-kubernetes-operator/main/hack/install-cli.sh
    less install-cli.sh
    sh install-cli.sh
    ```

    Two knobs: `VERSION` (default: latest release) and `INSTALL_DIR`
    (default: `/usr/local/bin`). Windows is not covered — use the `.zip`
    asset.

=== "Download a release"

    Binaries are attached to every [release](https://github.com/priyolahiri/neo4j-kubernetes-operator/releases). Pick your platform:

    ```bash
    VERSION=1.15.0     # the release you want
    OS=darwin          # darwin | linux | windows
    ARCH=arm64         # arm64 | amd64

    curl -sSLO "https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v${VERSION}/kubectl-neo4j_${VERSION}_${OS}_${ARCH}.tar.gz"
    curl -sSLO "https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v${VERSION}/kubectl-neo4j_${VERSION}_checksums.txt"
    shasum -a 256 -c --ignore-missing "kubectl-neo4j_${VERSION}_checksums.txt"

    tar -xzf "kubectl-neo4j_${VERSION}_${OS}_${ARCH}.tar.gz"
    chmod +x kubectl-neo4j
    sudo mv kubectl-neo4j /usr/local/bin/
    ```

    Windows builds are published as `.zip` rather than `.tar.gz`.

=== "Build from source"

    ```bash
    git clone https://github.com/priyolahiri/neo4j-kubernetes-operator
    cd neo4j-kubernetes-operator
    make build-cli          # writes bin/kubectl-neo4j
    export PATH="$PWD/bin:$PATH"
    ```

Anything named `kubectl-*` on your `PATH` becomes a `kubectl` subcommand, so once installed:

```bash
kubectl neo4j version
kubectl plugin list        # should list kubectl-neo4j
```

It also runs standalone as `kubectl-neo4j` if you prefer.

## `validate`

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
| `--strict` | Treat warnings as errors (exit non-zero on warnings). |
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

### Kinds validated offline

Six kinds validate with no cluster connection:

`Neo4jEnterpriseCluster` · `Neo4jEnterpriseStandalone` · `Neo4jBackup` · `Neo4jPlugin` · `Neo4jDatabaseAlias` · `Neo4jReplicaDatabase`

Every other kind — `Neo4jDatabase`, `Neo4jUser`, `Neo4jRole`, `Neo4jRoleBinding`, `Neo4jAuthRule`, `Neo4jShardedDatabase` and the Aura resources — resolves cross-references (a `clusterRef`, a Secret, a role) through the Kubernetes API. Those are reported as **skipped**:

```
- Neo4jUser/analytics (users.yaml): skipped — Neo4jUser validation resolves cross-references and needs a cluster connection; not yet supported offline
```

**Skipped is not a failure.** Reporting "cluster not found" for a cluster that simply is not reachable would be a false error, which is worse than not checking at all — so the command declines to guess.

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

- [Troubleshooting](troubleshooting.md) — diagnosing a deployment that is already running
- [Resource Sizing](resource_sizing.md) — the floors behind several of the warnings
