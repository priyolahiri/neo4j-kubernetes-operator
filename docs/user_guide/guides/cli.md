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

## `status`

One view of every Neo4j resource in a namespace — what exists, what is healthy, and the message for anything that is not.

```bash
kubectl neo4j status                      # the current context's namespace
kubectl neo4j status -n neo4j
kubectl neo4j status --all-namespaces
kubectl neo4j status --problems           # only what needs attention
```

```
KIND                     NAME        PHASE     READY   AGE
Neo4jEnterpriseCluster   prod        Ready     true    3h
Neo4jDatabase            analytics   Failed    -       5m
Neo4jUser                reporting   Pending   -       2m

✗ Neo4jDatabase/analytics: topology requires 3 primaries, cluster has 2 servers
… Neo4jUser/reporting: waiting for password Secret "reporting-pw"
```

Without it, this is `kubectl get` against 26 kinds followed by `describe` on whichever looks wrong.

Three things worth knowing about how it reports:

- **Messages appear below the table, in full.** They are the part you act on, and squeezing them into a column would wrap them into noise.
- **`…` is not `✗`.** A `Pending` resource is not broken — it is waiting on something you have not created yet. Its message is still shown, because it is the line that tells you what to do next, but it is marked distinctly from a failure. This matches how `validate` separates "not yet" from "wrong".
- **An unrecognised phase is not treated as a problem.** The Aura kinds mirror Aura's own status vocabulary, which Neo4j can extend without a version bump; flagging every phase this binary predates would produce false alarms. This is the same reasoning behind the project's ArgoCD health checks.

The kind list comes from the operator's registered API scheme rather than a hardcoded list, so a CRD added to the operator appears here automatically. Kinds you lack permission to read, or whose CRD is not installed, are skipped rather than failing the whole command.

Exit code is `0` on a successful query even when resources are unhealthy — mirroring `kubectl get`. Use `--problems` and check for empty output if you want a health gate.

## `connect` and `cypher`

The most-repeated sequence in the troubleshooting guide is: find the pod, extract the password from a Secret, remember the container name, guess the right Bolt scheme. These two commands do the resolution for you.

```bash
kubectl neo4j cypher                    # the only deployment in the namespace
kubectl neo4j cypher prod -n neo4j      # a specific one
kubectl neo4j cypher prod -c "SHOW DATABASES"
```

`connect` prints the same resolution without executing anything — useful when you want the port-forward command, or to hand someone the details:

```
$ kubectl neo4j connect prod
Neo4jEnterpriseCluster/prod in namespace neo4j

In-cluster Bolt:
  bolt+s://prod-client.neo4j.svc.cluster.local:7687

From your machine:
  kubectl port-forward -n neo4j svc/prod-client 7687:7687 7474:7474
  then connect to bolt+s://localhost:7687
...
TLS is enabled: plain bolt:// is rejected by this deployment — use bolt+s://.
```

### Your password is never read, moved, or logged

This is worth stating precisely, because the obvious implementation gets it wrong.

The admin credentials are **already inside the pod** — the operator injects them via `secretKeyRef` as `DB_USERNAME` and `DB_PASSWORD`. So the command references them *by variable name* and lets the shell expand them in the container:

```bash
kubectl exec -n neo4j prod-server-0 -c neo4j -it --   sh -c 'cypher-shell -a bolt+s://localhost:7687 -u "$DB_USERNAME" -p "$DB_PASSWORD"'
```

The secret never leaves the pod. It is not in your shell history, not in `ps` output on either side, and — the one people forget — **not in the Kubernetes API audit log**, which records an exec request's command array verbatim. A version that read the Secret and passed `-p <value>` would leak it into all three.

### It hands the session to `kubectl`

`cypher` resolves the target itself, then execs `kubectl` for the interactive part. That is deliberate: terminal raw mode, window resize, signal forwarding and every kubeconfig authentication plugin are already solved there, and reimplementing them would add a large amount of fragile code for no capability you want.

Consequence: **`kubectl` must be on your `PATH`.** Invoked as `kubectl neo4j cypher` it always is. If you run the binary standalone without kubectl installed, the command says so and points you at `connect` instead of failing obscurely.

### What it will not do

`cypher` passes *your* query through unchanged; the CLI never composes Cypher of its own. Operations the operator models as resources — creating a database, promoting a replica — belong in a CR, not in a shell command, so that they are declarative, auditable and reversible. `kubectl neo4j cypher -c "..."` is you running your own query, which is a different thing from the CLI deciding to mutate your database.

## See also

- [Troubleshooting](troubleshooting.md) — diagnosing a deployment that is already running
- [Resource Sizing](resource_sizing.md) — the floors behind several of the warnings
