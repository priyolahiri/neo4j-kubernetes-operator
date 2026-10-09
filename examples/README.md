# Neo4j Kubernetes Operator Examples

This directory contains example configurations for deploying Neo4j Enterprise clusters using the Neo4j Kubernetes Operator.

## Prerequisites

Before deploying any examples, ensure you have:

1. **Neo4j Kubernetes Operator installed** in your cluster:
   ```bash
   # Standard deployment (uses local images)
   make deploy-dev   # or make deploy-prod

   # Registry-based deployment (requires ghcr.io access)
   make deploy-prod-registry
   ```
2. **cert-manager v1.20+ with ClusterIssuer** (automatically installed in dev/test clusters)
3. **Appropriate storage classes** available in your cluster
4. **Neo4j Enterprise Edition** (evaluation license acceptable for testing)

**Note**: Development and test clusters created with `make dev-cluster` or `make test-cluster` automatically include cert-manager v1.20.0 and a self-signed ClusterIssuer (`ca-cluster-issuer`) for TLS testing. The operator works with Neo4j Enterprise 5.26+ and 2025.x versions.

## Quick Start

### 1. Create Admin Credentials

All examples require an admin secret. Create it first:

```bash
kubectl create secret generic neo4j-admin-secret \
  --from-literal=username=neo4j \
  --from-literal=password=your-secure-password
```

### 2. Deploy a Cluster

Choose an example and deploy:

```bash
# ── Basic topologies ──
# Minimal cluster (2 servers — minimum for HA)
kubectl apply -f examples/clusters/minimal-cluster.yaml
# Three servers, TLS disabled (simplest for local testing)
kubectl apply -f examples/clusters/three-node-simple.yaml

# ── TLS (cert-manager) ──
# Two-server TLS
kubectl apply -f examples/clusters/tls-cluster.yaml
# Three-server TLS
kubectl apply -f examples/clusters/three-node-cluster.yaml
# Five-server TLS + LoadBalancer (production)
kubectl apply -f examples/clusters/multi-server-cluster.yaml
# Production-tuned (resources + monitoring)
kubectl apply -f examples/clusters/production-optimized-cluster.yaml

# ── External access (Service types) ──
kubectl apply -f examples/clusters/loadbalancer-cluster.yaml   # LoadBalancer
kubectl apply -f examples/clusters/nodeport-cluster.yaml       # NodePort
kubectl apply -f examples/clusters/ingress-cluster.yaml        # Ingress
kubectl apply -f examples/clusters/route-cluster.yaml          # OpenShift Route

# ── Placement, auth & advanced ──
# Multi-zone / per-server role placement
kubectl apply -f examples/clusters/topology-placement-cluster.yaml
# Native + LDAP/OIDC auth providers
kubectl apply -f examples/clusters/auth-example.yaml
# Secondary (read-replica) servers
kubectl apply -f examples/clusters/cluster-with-read-replicas.yaml
# Trust internal CAs (trustedCASecrets + OIDC)
kubectl apply -f examples/clusters/cluster-with-trusted-cas.yaml
# Online storage expansion
kubectl apply -f examples/clusters/storage-expansion.yaml
```

### 3. Access Neo4j

Once deployed, access Neo4j through port forwarding:

```bash
# Port forward to the cluster
kubectl port-forward svc/your-cluster-name-client 7474:7474 7687:7687

# Open Neo4j Browser
open http://localhost:7474

# On OpenShift with Routes enabled
oc get route -n <namespace>
```

## Automatic Cluster Discovery

**All clusters use V2 discovery with the `LIST` resolver and static pod FQDNs on port 6000.** The operator renders the whole discovery configuration for you:

### What the Operator Configures Automatically

1. **Neo4j configuration** (rendered into `neo4j.conf`; the endpoint list is `<cluster>-server-<i>.<cluster>-headless.<namespace>.svc.cluster.local:6000` for every server):

   ```properties
   # Neo4j 5.26.x
   dbms.cluster.discovery.resolver_type=LIST
   dbms.cluster.discovery.version=V2_ONLY
   dbms.cluster.discovery.v2.endpoints=<pod-fqdns>:6000

   # Neo4j 2025.x / 2026.x (CalVer) — V2 is the only protocol, so no version flag
   dbms.cluster.discovery.resolver_type=LIST
   dbms.cluster.endpoints=<pod-fqdns>:6000
   ```

2. **Services**:
   - Headless service: `{cluster-name}-headless` (stable pod DNS names used in the endpoint list)
   - Discovery service: `{cluster-name}-discovery` (ClusterIP with `neo4j.com/clustering=true` label)

3. **RBAC** (created alongside the cluster): ServiceAccount, Role and RoleBinding named `{cluster-name}-discovery`.

### Benefits

- ✅ **Deterministic membership** - every server is addressed by its stable pod FQDN
- ✅ **Zero configuration** - no manual setup required
- ✅ **Automatic scaling** - the endpoint list follows `spec.topology.servers`

**No manual discovery configuration is needed or accepted.** The validator rejects `dbms.cluster.discovery.resolver_type`, `dbms.cluster.discovery.v2.endpoints`, `dbms.cluster.endpoints`, `dbms.kubernetes.label_selector` and `dbms.kubernetes.discovery.service_port_name` in `spec.config` — they are managed by the operator.

## Example Configurations

### `clusters/minimal-cluster.yaml`

- **Use case**: Development, testing, minimum high availability
- **Topology**: 2 servers (minimum for clustering)
- **Mode**: Server-based clustering (servers self-organize)
- **TLS**: Disabled for simplicity
- **Resources**: 2Gi RAM, 500m CPU

### `clusters/three-node-cluster.yaml`

- **Use case**: Production, high availability
- **Topology**: 3 servers (optimal fault tolerance)
- **Mode**: Server-based clustering with TLS
- **TLS**: cert-manager enabled
- **Resources**: 4Gi RAM, 1 CPU
- **Features**: Production configuration, monitoring enabled

### `clusters/three-node-simple.yaml`

- **Use case**: Testing, development, environments without cert-manager
- **Topology**: 3 servers (optimal fault tolerance)
- **Mode**: Server-based clustering, TLS disabled
- **TLS**: Disabled for simplicity
- **Resources**: 2Gi RAM, 500m CPU
- **Features**: Testing configuration, quick deployment

### `clusters/cluster-with-read-replicas.yaml`

- **Use case**: Read-heavy workloads, horizontal scaling
- **Topology**: 5 servers (can host databases with read replicas)
- **Mode**: Server-based clustering for flexible database topologies
- **TLS**: Disabled for simplicity
- **Resources**: 3Gi RAM, 750m CPU
- **Features**: Optimized for read performance

### `clusters/multi-server-cluster.yaml`

- **Use case**: Production workload with advanced features
- **Topology**: 5 servers (automatic role organization)
- **Mode**: Server-based clustering with automatic discovery
- **TLS**: cert-manager enabled
- **Resources**: 4Gi RAM, 1 CPU
- **Features**: LoadBalancer service, automatic RBAC, production config

### `clusters/topology-placement-cluster.yaml`

- **Use case**: Multi-zone production deployment with placement constraints
- **Topology**: 3 servers with topology spread constraints
- **Mode**: Server-based clustering with anti-affinity rules
- **TLS**: cert-manager enabled
- **Resources**: 4Gi RAM, 1 CPU
- **Features**: Zone distribution, topology constraints, fault tolerance

## Fault Tolerance Considerations ⚠️

The operator allows even numbers of primary nodes but issues warnings about reduced fault tolerance. Understanding these implications is crucial for production deployments.

### Server Configuration Recommendations

| Configuration | Fault Tolerance | Use Case | Recommendation |
|---------------|----------------|----------|----------------|
| 2 Servers | None | Development/Testing | ✅ Minimum for clustering |
| 3 Servers | ✅ 1 node failure | Production | ✅ **Recommended minimum** |
| 4 Servers | ⚠️ 1 node failure (same as 3) | - | ⚠️ Consider 3 or 5 instead |
| 5 Servers | ✅ 2 node failures | High availability | ✅ Mission-critical |
| 6 Servers | ⚠️ 2 node failures (same as 5) | - | ⚠️ Consider 5 or 7 instead |
| 7+ Servers | ✅ 3+ node failures | Maximum availability | ✅ Extreme requirements |

### Operator Warnings

When deploying with even numbers of servers, the operator will emit warnings:

```
Even number of servers (4) may reduce fault tolerance when databases specify odd-numbered server allocations. Consider using an odd number of servers for optimal fault tolerance.
```

### Best Practices

1. **Use odd numbers** of servers for production
2. **2 servers minimum** for property sharding deployments (3+ recommended for HA)
3. **Scale with databases**, not excessive servers
4. **Monitor cluster health** continuously
5. **Test failover scenarios** regularly

For detailed fault tolerance analysis, see: [Fault Tolerance Guide](../docs/user_guide/guides/fault_tolerance.md)

## Customization Guide

### Storage

Update the storage configuration for your environment:

```yaml
storage:
  className: your-storage-class  # e.g., gp2, standard, fast-ssd
  size: "50Gi"                  # Adjust based on data requirements
```

### Resources

Adjust resource allocation based on your workload:

```yaml
resources:
  requests:
    memory: "4Gi"    # Initial allocation
    cpu: "1"
  limits:
    memory: "8Gi"    # Maximum allocation
    cpu: "4"
```

### TLS Configuration

For development/testing, use the automatically configured self-signed issuer:

```yaml
tls:
  mode: cert-manager
  issuerRef:
    name: ca-cluster-issuer  # Self-signed issuer for development
    kind: ClusterIssuer
```

For production, replace with your own ClusterIssuer:

```yaml
tls:
  mode: cert-manager
  issuerRef:
    name: letsencrypt-prod   # Your production issuer
    kind: ClusterIssuer
```

### Custom Configuration

Add Neo4j-specific settings:

```yaml
config:
  db.logs.query.enabled: "INFO"
  db.transaction.timeout: "60s"
  server.metrics.enabled: "true"
```

## Topology Guidelines

| Use Case | Servers | Database Topologies | Notes |
|----------|---------|-------------------|-------|
| Development | 2 | Simple databases | Minimum for clustering |
| Testing | 2-3 | Various topologies | Test different configurations |
| Small Production | 3 | 1-2 primaries, 0-1 secondaries | Minimal HA cluster |
| Large Production | 5-7 | Multiple databases with different topologies | Flexible infrastructure |
| Read-Heavy | 5+ | Databases with read replicas | Horizontal read scaling |

## Deployment Behavior

### Cluster Formation Process

Neo4j clusters use parallel pod startup with coordinated formation:

1. **Parallel Startup**: All server pods start simultaneously for faster deployment
2. **Discovery Phase**: Servers find each other through the `LIST` resolver's static pod FQDNs (port 6000) that the operator injects
3. **Self-Organization**: Servers automatically form cluster and assign roles as needed
4. **Total Time**: Typical cluster formation completes in 2-3 minutes

### Expected Timeline

| Phase | Activity | Timing |
|-------|----------|--------|
| Resource Creation | StatefulSets, Services, ConfigMaps | 0-30 seconds |
| Pod Startup | All pods start in parallel | 30-60 seconds |
| Cluster Formation | Coordination and membership | 1-3 minutes |

**Note**: The operator uses parallel pod management for efficient cluster formation while maintaining data consistency.

## Troubleshooting

### Common Issues

1. **Pod stuck in Pending**: Check storage class and PVC binding
2. **License errors**: Set `spec.acceptLicenseAgreement` to `"yes"` (you hold a Neo4j Enterprise license) or `"eval"` (30-day evaluation) — the operator refuses to deploy without it
3. **TLS issues**: Ensure cert-manager and issuer are configured
4. **Memory issues**: Increase resource limits if pods are OOMKilled
5. **Cluster formation slow**: All server pods start in parallel - expect 2-3 minutes total formation time
6. **Server pods not ready**: Check resource availability and network connectivity between pods

### Useful Commands

```bash
# Check cluster status
kubectl get neo4jenterprisecluster

# View cluster details
kubectl describe neo4jenterprisecluster your-cluster-name

# Check pod logs
kubectl logs -l neo4j.com/cluster=your-cluster-name

# Check operator logs
kubectl logs -n neo4j-operator-system -l control-plane=controller-manager --tail=-1
```

## Directory Structure

- **`clusters/`** - Production-ready cluster configurations with various topologies
- **`standalone/`** - Single-node Neo4j deployments for development
- **`backup-restore/`** - `Neo4jBackup` and `Neo4jRestore` examples (one-shot, scheduled, incremental, PITR, overwrite)
- **`databases/`** - `Neo4jDatabase` creation, topology, and seed-from-backup examples
- **`composite-databases/`** - `Neo4jCompositeDatabase`: one query endpoint over several constituent databases, local and remote
- **`cross-cluster-replication/`** - `Neo4jReplicaDatabase` and `Neo4jReplicaPromotion` for cross-cluster DR
- **`fleet-management/`** - Aura Fleet Management integration examples
- **`plugins/`** - Plugin installation examples (APOC, GDS, Bloom, etc.)
- **`property_sharding/`** - Property-sharded database examples (Neo4j 2025.12+)
- **`security/`** - NetworkPolicy and policy-as-code examples
- **`observability/`** - An OpenTelemetry Collector for Neo4j's metrics and its logs on standard output (`spec.monitoring.logs`)
- **`users-roles/`** - Declarative user, role, and privilege management via the `Neo4jUser` and `Neo4jRole` CRDs
- **`end-to-end/`** - Complete deployment scenarios for production use

## Support

For more information, see:
- [User Guide](../docs/user_guide/getting_started.md)
- [Configuration Reference](../docs/user_guide/configuration.md)
- [Troubleshooting Guide](../docs/user_guide/guides/troubleshooting.md)
