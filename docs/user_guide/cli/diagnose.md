# `diagnose`

Answers the question `status` raises but cannot settle: **why** is this resource not healthy?

```bash
kubectl neo4j diagnose                                    # everything in the namespace
kubectl neo4j diagnose Neo4jEnterpriseCluster/prod        # one resource
kubectl neo4j diagnose --quiet                            # only what has something to say
```

`status` reads custom resources, so it can tell you a cluster is `Pending`. But most deployments fail one layer below the CR — a pod that will not schedule, a container OOMKilled at 1Gi, an image that will not pull, a PVC that never bound. `diagnose` follows the operator's own selectors from the resource down to its StatefulSet, pods, PVCs, Jobs and events, and names the Kubernetes-level cause.

```
$ kubectl neo4j diagnose --quiet -n neo4j
Neo4jEnterpriseCluster/prod — Pending · 2/3 pods ready
  ✗ pod prod-server-2 — cannot be scheduled
      0/3 nodes are available: 3 Insufficient memory.
      → No node can satisfy the pod. Compare spec.resources.requests with node
        capacity (kubectl describe nodes), or check that the PVC's StorageClass can
        provision in the zones the nodes are in. Neo4j Enterprise will not start
        below 1.5Gi of memory, so lowering the request has a floor.

1 of 3 resource(s) have problems.
For an archive to attach to an issue: kubectl neo4j support-bundle
```

## What it looks at

| Symptom | Where it comes from |
|---|---|
| Pod cannot be scheduled | `PodScheduled=False`, reason `Unschedulable` — the scheduler's own message |
| Container OOMKilled | `lastState.terminated` (a container that restarted) or `state.terminated` (one that did not — see [below](#containers-that-finished-and-were-not-restarted)): reason `OOMKilled`, **or** exit code 137 |
| Container exited non-zero | `state.terminated` with a non-zero exit code and any reason but OOMKilled (typically `Error`), with its message. Exit 0 (`Completed`) is not a finding |
| Image will not pull | waiting reason `ImagePullBackOff` / `ErrImagePull` / `InvalidImageName` |
| Crash-looping | waiting reason `CrashLoopBackOff`, with the restart count |
| Config not resolvable | waiting reason `CreateContainerConfigError` — usually a missing Secret key |
| PVC never bound | `pvc.status.phase != Bound`, naming the StorageClass |
| No pods at all | a StatefulSet that wants replicas but has none |
| Backup Job failed | the Job's `Failed` condition being `True` (including a missed `activeDeadlineSeconds`, where no pod failed), plus its pods, container by container. The verdict is the Job's conditions, not its `status.failed` count: that counts failed *attempts* and never goes down, so a backup that retried and then completed is a success — it gets a `⚠` line noting the retries, and one still retrying gets `…`, neither of which changes the exit code |
| Nothing ever reconciled | a CR with no status at all after two minutes |
| Operator warnings | the three most recent `Warning` events on the resource |

That last-but-one row is worth calling out. A resource with **no status at all** has no other signal — nothing is broken, the CR simply sits there. That is what you see when the operator is not running, has no RBAC for the kind, or is namespace-scoped and not watching this namespace.

The pod and PVC checks apply to `Neo4jEnterpriseCluster` and `Neo4jEnterpriseStandalone`; the Job checks apply to `Neo4jBackup`. Every other kind gets only the Warning-events and no-status checks.

### Containers that finished and were not restarted

Backup Job pods run with `restartPolicy: Never`. When one of them is OOM-killed or exits non-zero, nothing restarts it, so its `lastState` stays empty and the failure lives only in the container's *current* `state.terminated`. `diagnose` reads that state too: it names the container, the reason and the exit code, and points at the log of the run that died. Nothing restarted, so that is plain `kubectl logs <pod> -c <container>` — `--previous` is only for a container that has been restarted.

```
$ kubectl neo4j diagnose Neo4jBackup/nightly -n neo4j
Neo4jBackup/nightly — Failed · 1 job(s): 0 succeeded, 1 failed
  ✗ job nightly-1 — failed (4 pod failure(s))
      BackoffLimitExceeded: Job has reached the specified backoff limit
      → Read the Job's pod log for neo4j-admin's own error: kubectl logs
        job/nightly-1. A permission error names the ServiceAccount; a path
        error names the bucket.
  ✗ container backup in nightly-1-x7k2p — was OOMKilled (exit 137)
      → Raise spec.options.resources.limits.memory on the Neo4jBackup (unset,
        the limit is 2Gi). neo4j-admin needs headroom above the largest store
        file during compaction. The log of the run that was killed: kubectl
        logs nightly-1-x7k2p -c backup
```

An OOM-killed backup is pointed at the `Neo4jBackup`'s own `spec.options.resources`, not the cluster's `spec.resources`: the backup Job's memory is sized there.

Two rules keep this from crying wolf:

- A container that exited `0` is not a finding.
- A container already reported through its previous run (a crash-loop) is not reported a second time for its current attempt.

A Job keeps the pods of its failed attempts, so a backup that was OOM-killed once and then **completed on retry** still has the killed pod. For a Job that did not fail, the Job's own verdict wins: that pod's findings are shown as `⚠` (the Job succeeded) or `…` (it is still retrying) and do not change the exit code. They are `✗` only when the Job itself failed. A pod that cannot be scheduled is not a finished attempt and stays `✗`.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | nothing wrong, or only things that may still resolve on their own |
| `1` | a problem was found |
| `2` | usage error, or the cluster could not be reached |

Things marked `…` — a readiness probe that has not passed yet — are printed but **do not** change the exit code. Neo4j Enterprise takes tens of seconds to open Bolt on first start, and a command that failed during a normal startup would be useless in the loop the exit code exists for.

## Why this does not go stale

Every rule is anchored to a fact the Kubernetes API defines. Where client-go exports a constant it is used — `PodScheduled`, `PodReasonUnschedulable`, `ClaimBound` — so an upstream rename fails the build rather than silently matching nothing. The three kubelet-owned reasons that have no constant (`CrashLoopBackOff`, `ImagePullBackOff`, `OOMKilled`) are matched as literals, and OOM is additionally matched on exit code 137 so the most common Enterprise failure does not rest on a string alone.

Workloads are found through the operator's **own exported selectors**, not a label scheme restated here — the same discipline that has `validate` call the operator's own validators.

## See also

- [`status`](status.md) — what is deployed and which resources need attention
- [`explain`](explain.md) — decode the operator's own conditions and phases
- [`support-bundle`](support-bundle.md) — package the same evidence for an issue
- [Troubleshooting](../guides/troubleshooting.md) — the long-form knowledge
