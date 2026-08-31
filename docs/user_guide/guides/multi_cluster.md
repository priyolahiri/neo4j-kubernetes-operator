# Running Across Multiple Kubernetes Clusters

This guide covers running Neo4j under this operator in **more than one Kubernetes cluster** — how to install and configure at fleet scale, and how to get one view of health across all of them.

The short version: **install the operator in every cluster, and drive it with GitOps.** There is no hub operator that reaches into other Kubernetes clusters, and the design analysis in [`docs/design/multi-cluster-control-plane.md`](https://github.com/priyolahiri/neo4j-kubernetes-operator/blob/main/docs/design/multi-cluster-control-plane.md) explains why: the operator is a Neo4j client as well as a Kubernetes client, and its Bolt connections are in-cluster by construction.

## Which problem do you actually have?

Four different things get called "multi-cluster". They have different answers:

| What you want | Answer |
|---|---|
| The same Neo4j config applied to many clusters | **This guide** — GitOps fan-out |
| One health view across clusters | **This guide** — [`k8s_cluster` metric label](#one-health-view-across-clusters) |
| A copy of a database in another region, for DR | [Cross-Cluster Replication](cross_cluster_replication.md) |
| One Neo4j cluster whose servers span Kubernetes clusters | Not supported, and not recommended — see below |

### Why not one Neo4j cluster spanning Kubernetes clusters

A single Neo4j cluster is a RAFT group. Stretching it across Kubernetes clusters puts a wide-area round trip on every write (RAFT commit is on the write path), and requires every server's RAFT and routing addresses to be externally routable. If you need Neo4j in two regions, you want two clusters and [Cross-Cluster Replication](cross_cluster_replication.md), which is the mechanism Neo4j provides for exactly this.

## The model: one operator per cluster

Each Kubernetes cluster runs its own operator, managing only the Neo4j deployments in that cluster. This is not a limitation to work around — it is what gives you:

- **Failure isolation.** An operator outage affects one cluster. There is no shared control plane whose loss stops reconciliation everywhere.
- **Local Bolt.** Every operator reaches its Neo4j pods over in-cluster DNS, so no database endpoint is ever exposed outside its cluster just to be administered.
- **Local credentials.** A cluster's Neo4j admin Secret never leaves that cluster.

What you fan out is **manifests**, not control.

## Fan-out with Argo CD ApplicationSets

An `ApplicationSet` with a cluster generator installs the operator into every registered cluster from one definition.

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: neo4j-operator
  namespace: argocd
spec:
  generators:
    - clusters:
        selector:
          matchLabels:
            neo4j: "true"          # label your Argo cluster secrets
  template:
    metadata:
      name: 'neo4j-operator-{{name}}'
    spec:
      project: default
      source:
        repoURL: https://github.com/priyolahiri/neo4j-kubernetes-operator
        targetRevision: v1.14.0    # pin a release; never track main
        path: charts/neo4j-operator
        helm:
          values: |
            kubernetesClusterName: '{{name}}'
      destination:
        server: '{{server}}'
        namespace: neo4j-operator
      syncPolicy:
        automated: {prune: true, selfHeal: true}
        syncOptions:
          - CreateNamespace=true
          - ServerSideApply=true   # see "CRD size" below
```

Two things worth setting deliberately:

- **`kubernetesClusterName: '{{name}}'`** wires each install's metrics label to the cluster it runs in. See [below](#one-health-view-across-clusters).
- **`ServerSideApply=true`.** This operator's CRDs are large. Client-side apply stores the whole manifest in the `last-applied-configuration` annotation and can exceed the annotation size limit; server-side apply avoids it.

### Fanning out Neo4j deployments themselves

The same generator works for the workloads, with per-cluster overrides in Git:

```yaml
spec:
  generators:
    - git:
        repoURL: https://github.com/your-org/neo4j-fleet
        revision: main
        directories:
          - path: clusters/*
  template:
    metadata:
      name: 'neo4j-{{path.basename}}'
    spec:
      source:
        repoURL: https://github.com/your-org/neo4j-fleet
        path: '{{path}}'          # clusters/eu-west/, clusters/us-east/, …
      destination:
        server: '{{server}}'
        namespace: neo4j
```

Each `clusters/<name>/` directory holds that cluster's `Neo4jEnterpriseCluster`, `Neo4jDatabase`, `Neo4jUser`, `Neo4jRole` and so on. Sizing, storage class and topology differ per cluster; the CR shape does not.

!!! warning "Do not fan out `Neo4jBackup` storage paths"
    A `Neo4jBackup` written by two clusters into the same bucket path will interleave two backup chains in one directory. Give every cluster its own `spec.storage.path`. If a chain feeds cross-cluster replication, this is not merely untidy — it breaks the differential chain and forces a replica rebuild.

## Fan-out with Flux

The equivalent shape is a `HelmRelease` per cluster, or one `Kustomization` per cluster reconciled by a Flux instance in each:

```yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: neo4j-operator
  namespace: neo4j-operator
spec:
  interval: 30m
  chart:
    spec:
      chart: charts/neo4j-operator
      sourceRef: {kind: GitRepository, name: neo4j-operator, namespace: flux-system}
  values:
    kubernetesClusterName: eu-west-prod
```

Flux's usual pattern is one Flux per cluster pulling from a shared repo, which fits this operator's per-cluster model directly.

## GitOps interactions worth knowing

Three operator behaviours interact with drift correction. All three are by design, and all three will confuse a reviewer who meets them for the first time during an incident.

- **One-shot CRs are inert to re-apply.** `Neo4jBackup`, `Neo4jRestore` and `Neo4jReplicaPromotion` reach a terminal phase and stay there. Re-applying an unchanged manifest does not re-run them, which is exactly what you want from a self-healing GitOps controller.
- **Promotion is not a spec field.** Failover is a separate `Neo4jReplicaPromotion` CR precisely so that a GitOps controller re-applying the pre-promotion manifest cannot un-promote a database. See [Cross-Cluster Replication](cross_cluster_replication.md).
- **Some status fields are meant to be copied into another cluster's spec.** `status.replicationPullURI` and `status.crossClusterReplication.addresses` are published for a human or a pipeline to paste into a downstream cluster's manifest. Argo will not do this for you; treat it as a deliberate step in your fleet runbook.

## One health view across clusters

The operator exposes `neo4j_operator_server_health` per Neo4j server, labelled with `cluster_name` (the **Neo4j** cluster) and `namespace`. Across a fleet that is not enough: `cluster_name="prod"` in namespace `neo4j` very likely exists in every one of your Kubernetes clusters, and federating them into one Prometheus collapses them into a single ambiguous series.

Set `kubernetesClusterName` to disambiguate:

```yaml
# values.yaml
kubernetesClusterName: eu-west-prod
```

or directly:

```
--kubernetes-cluster-name=eu-west-prod
```

Every `neo4j_operator_server_health` series then carries `k8s_cluster="eu-west-prod"`:

```
neo4j_operator_server_health{cluster_name="prod",namespace="neo4j",server_name="srv-0",server_address="10.0.0.1:7687",k8s_cluster="eu-west-prod"} 1
```

!!! note "`k8s_cluster` is not `cluster_name`"
    `cluster_name` is the Neo4j cluster. `k8s_cluster` is the Kubernetes cluster it runs in. The flag is `--kubernetes-cluster-name` rather than `--cluster-name` to keep that distinction visible at the point of configuration.

**Backwards compatible.** The flag is unset by default, which emits an empty `k8s_cluster` label. Prometheus treats an empty label value as equivalent to the label being absent, so existing queries, dashboards and alert rules keep matching unchanged whether or not you adopt this.

### Federating

With a Prometheus per cluster and one aggregating Prometheus, federate the operator metrics:

```yaml
scrape_configs:
  - job_name: neo4j-operator-federation
    honor_labels: true
    metrics_path: /federate
    params:
      'match[]':
        - '{__name__=~"neo4j_operator_.*"}'
    static_configs:
      - targets: ['prometheus.eu-west.example.com', 'prometheus.us-east.example.com']
```

Then fleet-wide queries work directly:

```promql
# Every degraded server anywhere in the fleet
neo4j_operator_server_health == 0

# Healthy server count per Kubernetes cluster
sum by (k8s_cluster) (neo4j_operator_server_health)

# Clusters where any server is degraded
count by (k8s_cluster, cluster_name) (neo4j_operator_server_health == 0) > 0
```

An alert that names the Kubernetes cluster in its own text:

```yaml
- alert: Neo4jServerDegraded
  expr: neo4j_operator_server_health == 0
  for: 5m
  annotations:
    summary: "Neo4j server {{ $labels.server_name }} degraded in {{ $labels.cluster_name }} ({{ $labels.k8s_cluster }})"
```

If you already use Aura Fleet Management, `spec.auraFleetManagement` registers deployments with Aura's own fleet view and is an alternative to this for cross-cluster inventory.

## What this does not give you

Stated plainly, so nothing is discovered during an incident:

- **No single `kubectl` context for the fleet.** Inspecting a Neo4j deployment means talking to its own cluster's API server.
- **No cross-cluster Cypher.** `Neo4jUser`, `Neo4jRole` and `Neo4jDatabase` are reconciled by the operator local to each cluster. Fan out the CRs; the local operator executes them.
- **No aggregated status object.** The `k8s_cluster` label gives you a metrics-level fleet view. There is no CR that lists every deployment across every cluster.

## See also

- [Cross-Cluster Replication](cross_cluster_replication.md) — a read-only copy of a database in another cluster, and one-way promotion for DR
- [Monitoring](monitoring.md) — enabling metrics on a deployment
- [Prometheus and Grafana Setup](prometheus-grafana-setup.md) — the single-cluster monitoring stack this builds on
