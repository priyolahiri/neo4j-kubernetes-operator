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
replication) and all 12 Aura CRDs.

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

## The operator does not rewrite your spec

The stored spec of every custom resource of the operator is the one you
applied, with the CRD's own defaults the API server fills in, so a GitOps diff
shows only what you changed. The operator's defaults for a cluster (Neo4j's
`native` auth providers, TLS disabled when there is no `tls` block, a ClusterIP
Service when there is no `service` block) go on its copy of the object for each
reconcile and are never written back. What it does write to one of its custom
resources is a merge patch of what it changes:

| Written | On | When |
|---------|----|------|
| `metadata.finalizers` (its own entry) | Every kind it cleans up after | First reconcile; removed on deletion |
| `neo4j.com/scale-down-draining-servers` annotation | `Neo4jEnterpriseCluster` | While a scale-down drains servers |
| `neo4j.com/external-fleet-deployment-id` annotation | Cluster or standalone | When Aura Fleet Management provisions the deployment |
| `neo4j.com/restore-in-progress` annotation | `Neo4jEnterpriseStandalone` | While an offline restore holds it stopped |
| `neo4j.com/cypher-restore-issued`, `cypher-restore-observed-offline`, `restore-pre-hooks-ran`, `point-in-time-path`, `seed-proxy-wait-started` annotations | `Neo4jRestore` | While the restore runs |
| `neo4j.com/external-*-id`, `neo4j.com/multi-database-probed` annotations | Aura kinds | When the Aura object is created, adopted or probed |
| `spec.extraEnvFrom` entry and `neo4j.com/seed-creds-auto-inherited-from` annotation | Cluster or standalone | A database seed needs a credentials Secret the CR does not project, and the CR carries `neo4j.com/auto-inherit-seed-creds: "true"` |
| `spec.env` / `spec.extraEnvFrom` entries and the same annotation | Cluster or standalone | A `Neo4jRestore` from cloud storage needs credentials or an endpoint its target lacks, and the target carries that annotation |

The last two are spec changes you opted into with the annotation, and a GitOps
tool reports them: add them to your manifests, or tell the tool to ignore those
fields.

Releases before this one sent the whole object back with every one of these
writes, so an object they created may carry fields you did not write:

- On a cluster, the defaults above: `spec.auth.authenticationProviders` and
  `authorizationProviders: [native]`, `spec.tls: {mode: disabled,
  strictPeerValidation: true}`, `spec.service: {type: ClusterIP}`.
- `true` where you had set a field the CRD defaults to `true` to `false`:
  `spec.monitoring.enabled`, `spec.upgradeStrategy.preUpgradeHealthCheck`,
  `postUpgradeHealthCheck`, `autoPauseOnFailure` and `spec.mcp.readOnly` (cluster
  and standalone); `Neo4jRole` `spec.enforcePrivileges`; `Neo4jAuthRule`
  `spec.enforceRoles`; `Neo4jPlugin` `spec.enabled`; `Neo4jDatabase`
  `spec.ifNotExists` and `spec.wait`; a backup's or restore's cloud
  `identity.autoCreate.enabled`.
- No field at all where you had set a field with no default to `false` (an
  `AuraInstance`'s `deletionProtection: false`).
- Quantities in their canonical spelling (`cpu: "0.5"` stored as `500m`).

An operator upgrade leaves them as they are, and every object goes on doing
what its stored spec says, as it did before the upgrade: nothing restarts. Your
next apply puts back each field your manifest sets (with
`kubectl apply --server-side`, a field the operator wrote is a conflict until
you pass `--force-conflicts`); remove the others with `kubectl patch --type
json`. Removing a default changes nothing. Putting back a value of yours does
what that edit always does: `monitoring.enabled: false`, or your own
`authorizationProviders` on a cluster that named no `authenticationProviders`,
changes what the servers run and restarts them; a role's
`enforcePrivileges: false` stops it revoking privileges added outside its
spec.

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
