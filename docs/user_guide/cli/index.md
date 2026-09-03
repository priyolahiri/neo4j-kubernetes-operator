# `kubectl-neo4j` CLI

A companion CLI for this operator, distributed as a `kubectl` plugin. It exists to close a specific gap and to collapse a specific pile of `kubectl` invocations.

!!! warning "Support"
    The CLI ships with the operator, on the same tag, under the **same support terms as the operator itself**: best-effort via [GitHub issues](https://github.com/priyolahiri/neo4j-kubernetes-operator/issues), no official or commercial support, no SLA, and behaviour may change between releases without notice. A downloadable binary is not a more supported artifact than the operator it comes from.

## Why it exists

**This operator has no admission webhooks.** That is a deliberate architectural invariant, not an omission — the upside is a much simpler install with no certificate plumbing and no webhook outage mode. The downside is that spec mistakes are only reported *after* you apply, through `status` and events:

```
edit YAML → apply → wait for reconcile → read status → find the mistake → repeat
```

Across 26 resource kinds and 234 top-level spec fields, that loop is slow. [`validate`](validate.md) closes it by running **the operator's own validators** against your files, locally.

The second reason is volume. The troubleshooting guide documents 98 separate `kubectl` invocations, nearly all of them generic commands with placeholders you have to resolve from knowledge of the operator's naming scheme — `<cluster>-server-0`, `<cluster>-client`, which namespace the operator itself lives in, which container, which Bolt scheme. The CLI knows all of that.

## The commands

| | |
|---|---|
| [`validate`](validate.md) | Check manifests against the operator's validators, before applying |
| [`preflight`](preflight.md) | Check the cluster-side preconditions a manifest depends on |
| [`status`](status.md) | One view of every Neo4j resource in a namespace |
| [`diagnose`](diagnose.md) | Why a resource is unhealthy, at the Kubernetes level |
| [`connect` and `cypher`](connect.md) | Reach a deployment, or open a `cypher-shell` session |
| [`support-bundle`](support-bundle.md) | Collect a redacted diagnostic archive |
| [`explain`](explain.md) | Decode a status condition or phase, and what to do about it |
| [`export`](export.md) | Author a downstream manifest from an upstream resource's status |

Start with [Installing the CLI](install.md).

## What it will not do

Three boundaries, held deliberately:

- **It never executes Cypher for a mutating operation on your behalf.** It may author and read custom resources, and read Kubernetes state. Creating a database or promoting a replica belongs in a CR, where it is declarative, auditable and reversible. `cypher -c "..."` passes *your* query through — a different thing from the CLI deciding to change your database.
- **It never restates the operator's rules.** Validation comes from the operator's own packages, and `explain` is keyed off the operator's own condition constants, so a rename breaks the build rather than leaving the CLI confidently wrong. A second source of truth would rot.
- **It checks shape, not reachability.** [`preflight`](preflight.md) reads Kubernetes objects — a StorageClass, a Secret's key names, a ServiceAccount's annotations. It never contacts S3, GCS, Azure or a registry, and never runs a probe pod, so a clean run never means "the backup will work". Saying so on every run is the point: a check that implied more than it made would be worse than no check.
- **It admits what it does not know.** Kinds with no validator say so rather than implying a flag would help; unrecognised phases are reported as unrecognised, naming the CLI's own version. A confident wrong answer during an incident costs more than no answer.

## Flags go anywhere

Flags parse wherever you put them, before or after a positional argument, the
way `kubectl` itself behaves:

```bash
kubectl neo4j diagnose Neo4jEnterpriseCluster/prod -n neo4j   # same
kubectl neo4j diagnose -n neo4j Neo4jEnterpriseCluster/prod   # thing
```

## Version matching

The CLI carries the validation rules and status vocabulary of the release it was built from. **Keep it on the same version as the operator you deploy.** Its output always names the ruleset it used, and when connected to a cluster it warns if the two disagree.
