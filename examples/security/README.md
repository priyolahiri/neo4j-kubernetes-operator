# Security examples

Drop-in YAML for hardening Neo4j deployments. None of these are deployed
automatically by the operator — apply them per-namespace, after you've
created your `Neo4jEnterpriseCluster` / `Neo4jEnterpriseStandalone` CR.

## NetworkPolicies

**Prefer the operator-managed policy.** Set `spec.networkPolicy.enabled: true` on
the `Neo4jEnterpriseCluster` or `Neo4jEnterpriseStandalone` and the operator emits
an ingress NetworkPolicy (`<name>-server-netpol` / `<name>-standalone-netpol`):
client ports open, peer ports (6000/7000/7688/7689) restricted to the cluster's own
servers, and the backup port (6362) restricted to operator-managed backup Jobs.
`spec.networkPolicy.allowReplicasFrom` additionally admits named same-Kubernetes-cluster
CCDR replicas on port 6000. See the [Security guide](../../docs/user_guide/security.md).
It only takes effect on a CNI that enforces NetworkPolicy (Calico, Cilium, Antrea).

The files below are for when you need more than that — chiefly default-deny
**egress**, which the operator's policy does not impose. Treat them as
illustrations: extend the egress rules for what your deployment calls.

| File | Use case |
|---|---|
| [`networkpolicy-cluster.yaml`](networkpolicy-cluster.yaml) | Per-cluster ingress + egress rules. Allows Bolt/HTTP from the same namespace, the intra-cluster ports (6000/7000/7688/7689) between server pods, Bolt + metrics scrape from the operator namespace, and the backup port from operator-managed backup Jobs. Replace `MY-CLUSTER` and `MY-NAMESPACE`. |
| [`networkpolicy-standalone.yaml`](networkpolicy-standalone.yaml) | Same shape, single-pod variant. No intra-cluster ports to allow. |

Apply with `kubectl apply -f` after editing the placeholders. Both policies start from a
default-deny posture (declaring `Ingress` and `Egress` in `policyTypes` with explicit
rules means everything not listed is denied) and add back only the traffic listed.

## What the operator itself ships

The operator's own NetworkPolicy is bundled in the Helm chart and gated on
`networkPolicy.enabled` in `values.yaml`. When enabled it allows Prometheus
scrape on the metrics endpoint and the operator's egress to Neo4j workload
pods, DNS, and the K8s API — see
[`charts/neo4j-operator/templates/networkpolicy.yaml`](../../charts/neo4j-operator/templates/networkpolicy.yaml).

The operator also creates a per-deployment NetworkPolicy as a child resource of
each `Neo4jEnterpriseCluster` / `Neo4jEnterpriseStandalone` when
`spec.networkPolicy.enabled` is true (see above) — it is off by default.

## Other hardening already on by default

- Pod / container `SecurityContext` (RunAsNonRoot, drop ALL caps,
  RuntimeDefault seccomp) is applied to cluster, standalone, backup,
  restore, and plugin pods. See `internal/resources/security_context.go`.
- TLS for Bolt is opt-in via `spec.tls.mode: cert-manager` on the cluster
  / standalone CR. When enabled, the operator sets
  `server.bolt.tls_level=REQUIRED` and rejects plain `bolt://` clients.
- Plugin supply chain: `Neo4jPlugin.spec.source.checksum` is required for
  `type=url` and `type=custom`; SHA1/MD5 are rejected. See
  [`docs/api_reference/neo4jplugin.md`](../../docs/api_reference/neo4jplugin.md#supply-chain).
- Operator RBAC: the controller no longer requests `pods/exec` (stale from
  the old sidecar-exec backup architecture, removed May 2026).
