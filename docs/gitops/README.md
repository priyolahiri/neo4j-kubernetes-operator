# GitOps Integration Guide

This directory contains configuration for integrating the Neo4j Kubernetes Operator
with GitOps tools (ArgoCD, Flux) and Prometheus monitoring.

## ArgoCD Health Checks

ArgoCD does not natively understand `status.phase` on custom resources and shows
everything as "Progressing". Apply the health check ConfigMap to teach ArgoCD how
to interpret Neo4j operator resource states.

```bash
kubectl patch configmap argocd-cm -n argocd \
  --type merge --patch-file docs/gitops/argocd-health-checks.yaml
```

Health state mapping:

| ArgoCD Status  | Neo4j Phase(s)                        |
|----------------|---------------------------------------|
| Healthy        | Ready, Completed, Succeeded, Installed |
| Degraded       | Failed, Degraded                       |
| Progressing    | Forming, Pending, Creating, or empty   |

Health checks are configured for **all 26 CRDs** in the `neo4j.neo4j.com`
group — the 14 self-managed CRDs (7 workload, 4 identity, 3 replication) and
all 12 Aura CRDs. `make check-crd-catalog` fails the build if a CRD is added
without one.

**Self-managed CRDs** key off `status.phase`, per the table above.

**Aura CRDs** key off the `Ready` **condition** instead. Their `status.phase`
largely mirrors Aura's *own* API status (`AuraInstance` copies the live instance
status verbatim), which is an open vocabulary Neo4j can extend without a version
bump — enumerating running states would silently report any new one as
`Degraded`. `phase` is still consulted first for the terminal `Error`/`Failed`
values, because the `Ready` condition can lag a failure by a reconcile.

!!! note "A promoted replica reports Healthy, not Degraded"

    `Neo4jReplicaDatabase` reaching `phase: Promoted` is a **completed
    failover**, not a fault — its health check maps that to `Healthy`. Mapping
    it to `Degraded` would show a successful DR promotion as broken, and could
    prompt an operator (or an automated remediation) to try to "fix" something
    that is working as designed. The CR is inert from that point on and will
    never modify the database again.

## Flux Health Checks

Flux automatically detects readiness via `status.conditions` when CRDs expose a
standard `Ready` condition (type `Ready`, using `metav1.Condition`). No extra
Flux configuration is needed once the operator surfaces that condition.

## Prometheus ServiceMonitor

The Helm chart includes a `ServiceMonitor` for the Prometheus Operator. Enable it
at install or upgrade time:

```bash
helm upgrade --install neo4j-operator charts/neo4j-operator \
  --set metrics.enabled=true \
  --set metrics.serviceMonitor.enabled=true
```

Or set in `values.yaml`:

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: "30s"
    scrapeTimeout: "10s"
    labels: {}        # add Prometheus instance selector labels here if needed
```

The operator exposes metrics on port `8080` at `/metrics` (Prometheus text format).
