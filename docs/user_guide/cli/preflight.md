# `preflight`

Checks the **cluster-side preconditions** a manifest depends on — the things `validate` cannot see because they are not in the manifest.

```bash
kubectl neo4j preflight -f cluster.yaml -n neo4j       # before you apply
kubectl neo4j preflight Neo4jBackup/nightly -n neo4j   # for what is deployed
kubectl neo4j preflight -n neo4j                       # everything there
```

[`validate`](validate.md) took the operator's spec rules and moved them from after-apply to before-apply. But a large class of first failures is not in the manifest at all — it is in the cluster the manifest is about to land in. A StorageClass that does not exist. One that cannot expand. A credentials Secret missing a key the backup Job will mount. No node large enough for the pod. Each produces a correct-looking manifest that never becomes a running database.

```
$ kubectl neo4j preflight -f cluster.yaml -n neo4j
Neo4jEnterpriseCluster/prod (cluster.yaml)
  ✗ storageclass fast-ssd — does not exist
      → The operator will refuse to create the StatefulSet and record a
        StorageClassNotFound event. Create the class, name an existing one, or leave
        spec.storage.className empty to use the cluster default.
  ✗ node capacity — no Ready node can fit a 2Gi memory request
      largest Ready node has 1Gi allocatable
      → Every pod will stay Pending as Unschedulable. Add capacity, or lower
        spec.resources.requests.memory — but Neo4j Enterprise will not start below
        1.5Gi, so that has a floor.

Shape only: no bucket, registry or endpoint was contacted.
1 of 1 resource(s) would fail on a precondition.
```

## What it checks

**Clusters and standalones**

| Check | Why it matters |
|---|---|
| `spec.storage.className` exists | The operator refuses to build the StatefulSet and records `StorageClassNotFound` |
| That class allows volume expansion | Without it `spec.storage.size` is effectively immutable — discovered during the resize a full disk made urgent |
| A Ready node can fit the memory request | The top pod-startup failure, invisible to any manifest-level check |
| `spec.image.pullSecrets` exist | Otherwise every pod sits in `ImagePullBackOff` |

Capacity is compared against a **single node's** allocatable memory, never the cluster total: the scheduler places a pod on one node, so three nodes with 1Gi each cannot run one 2Gi pod however the total reads.

**Backups**

| Check | Why it matters |
|---|---|
| `credentialsSecretRef` exists | Named but absent means the Job cannot start |
| It carries every key the Job mounts | A missing key gives `CreateContainerConfigError`, which never mentions backups |
| Or: the backup ServiceAccount has a cloud-identity annotation | With no Secret and no IRSA / Workload Identity binding, the Job runs with no credentials at all |
| A PVC-backed backup's claim or class exists | Same failure as a cluster's storage |

This replaces the ritual the troubleshooting guide documents today — `kubectl run backup-auth-check --image=amazon/aws-cli …`, in three vendor variants — which you only reach for **after** a backup has already failed.

## What it does not check

**Shape, not reachability.** It reads Kubernetes objects: a StorageClass, a Secret's key names, a ServiceAccount's annotations, node allocatable capacity. It never contacts S3, GCS or Azure, never talks to a registry, and never runs a probe pod.

So a bucket that exists but denies access still fails at run time, and credentials that are well-formed but wrong still fail at run time. The output says so on every run rather than letting a clean result imply more than it means.

That boundary is deliberate: it keeps the command free of new images, new RBAC and anything that mutates your cluster.

A kind with no cluster-side preconditions is reported as **skipped**, not passed — a silent pass would imply a check that was never made.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | every check passed (warnings alone do not fail) |
| `1` | a check failed |
| `2` | usage error, or the cluster could not be reached |

## See also

- [`validate`](validate.md) — the manifest itself, against the operator's own validators
- [`diagnose`](diagnose.md) — the same class of failure, after it has happened
- [Storage Expansion](../guides/storage_expansion.md) — why `allowVolumeExpansion` matters before you need it
