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
| Healthy        | Ready, Completed, Succeeded, Installed; `Neo4jBackup` also `Scheduled` and `Suspended` |
| Degraded       | Failed, Degraded; `Neo4jBackup` and `Neo4jPlugin` also `Invalid` |
| Progressing    | Forming, Pending, Creating, Waiting, Running, or empty |

A `Neo4jBackup` with `spec.schedule` stays in `Scheduled` for its whole life (it
never reaches `Completed`), and `spec.suspend: true` is declared state rather
than a fault, so both map to Healthy — otherwise an Application containing a
scheduled backup would show Progressing forever and hold up any later sync wave.
The `Ready` condition (what Flux reads) agrees for `Scheduled`: it is `True`
with reason `BackupScheduled`. `Suspended` still reports `Ready=False` (a suspended
backup is not ready to run) but with reason `Suspended`, not `ReconciliationFailed`:
nothing failed.
`Invalid` (a spec the validator rejected) is terminal until the spec is edited,
so it maps to Degraded rather than Progressing.

A `Neo4jEnterpriseCluster` that loses a server stays `Ready` — Healthy — for a
grace period (default 5 minutes), so a routine pod restart does not flip the
Application. If the server is still missing after that, the phase turns
`Degraded` and so does the Application; it returns to Healthy when the server
does. A cluster that loses a majority of its servers reports `Forming`, which
ArgoCD shows as Progressing. See
[Server availability](../api_reference/neo4jenterprisecluster.md#server-availability).

Health checks are configured for **all 27 CRDs** in the `neo4j.neo4j.com`
group — the 15 self-managed CRDs (7 workload, 4 identity, 4 composite / alias /
replication) and all 12 Aura CRDs. `make check-crd-catalog` fails the build if a
CRD is added without one.

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

A `Neo4jEnterpriseCluster` keeps `Ready=True` through a short server outage
(the grace period above); a `Degraded` one reports `Ready=False` with reason
`ClusterDegraded`, so a Kustomization health-checking it fails until the
missing server is back.

## Prometheus ServiceMonitor

The Helm chart includes a `ServiceMonitor` for the Prometheus Operator. By default
(`metrics.secure: true`) the operator serves `/metrics` over **HTTPS** with
bearer-token authentication (TokenReview + SubjectAccessReview against the
`metrics-reader` ClusterRole), so the ServiceMonitor needs a token Secret for a
ServiceAccount bound to that ClusterRole — the chart refuses to render
otherwise. Enable it at install or upgrade time:

```bash
helm upgrade --install neo4j-operator charts/neo4j-operator \
  --set metrics.enabled=true \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.serviceMonitor.bearerTokenSecret.name=<token-secret>
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
    bearerTokenSecret:
      name: <token-secret>   # kubernetes.io/service-account-token Secret, key "token"
```

For a legacy scraper that cannot present a bearer token, set `metrics.secure: false`
instead (plain HTTP, no authn/authz — only behind a NetworkPolicy you trust).

The metrics Service listens on port `8080` at `/metrics` (Prometheus text format).
