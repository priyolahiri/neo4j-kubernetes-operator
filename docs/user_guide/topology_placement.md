# Topology Placement Guide

This guide explains how to configure topology placement for Neo4j Enterprise clusters to ensure high availability, optimal performance, and fault tolerance across different failure domains in your Kubernetes cluster.

## Overview

The Neo4j Kubernetes Operator provides sophisticated topology placement capabilities to distribute Neo4j cluster nodes across different failure domains (zones, regions, or custom topology domains). This ensures your database remains available even when entire zones or racks fail.

## Key Concepts

### Failure Domains
- **Zones**: Physical or logical isolation boundaries (e.g., AWS availability zones, data center racks)
- **Nodes**: Individual Kubernetes worker nodes
- **Regions**: Larger geographical boundaries containing multiple zones

### Topology Keys
Standard Kubernetes topology labels used for placement:
- `topology.kubernetes.io/zone` - Availability zone placement
- `kubernetes.io/hostname` - Node-level anti-affinity
- `topology.kubernetes.io/region` - Regional placement
- Custom labels defined by your infrastructure team

## Basic Configuration

### Simple Zone Distribution

For most production deployments, distributing across availability zones is recommended:

```yaml
apiVersion: neo4j.io/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: production-cluster
spec:
  topology:
    primaries: 3
    secondaries: 3
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 1
        whenUnsatisfiable: "DoNotSchedule"
```

This configuration ensures:
- Primary and secondary nodes are evenly distributed across zones
- Maximum difference of 1 pod between any two zones
- Pods won't be scheduled if distribution requirements can't be met

### Pod Anti-Affinity

To prevent multiple Neo4j pods from running on the same node:

```yaml
spec:
  topology:
    primaries: 3
    secondaries: 2
    placement:
      antiAffinity:
        enabled: true
        topologyKey: "kubernetes.io/hostname"
        type: "preferred"  # or "required" for strict enforcement
```

Anti-affinity types:
- `preferred`: Best effort - scheduler tries to avoid co-location
- `required`: Strict - pods will remain unscheduled if constraints can't be met

## Advanced Configuration

### Combining Topology Spread and Anti-Affinity

For maximum resilience, combine zone distribution with node anti-affinity:

```yaml
spec:
  topology:
    primaries: 3
    secondaries: 3
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 1
        whenUnsatisfiable: "ScheduleAnyway"  # More flexible for maintenance
      antiAffinity:
        enabled: true
        topologyKey: "kubernetes.io/hostname"
        type: "preferred"
```

### Specifying Availability Zones

Explicitly define which zones to use:

```yaml
spec:
  topology:
    primaries: 3
    secondaries: 3
    availabilityZones:
      - us-east-1a
      - us-east-1b
      - us-east-1c
    enforceDistribution: true  # Ensures primaries are distributed
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
```

### Minimum Domain Requirements

Ensure scheduling only when sufficient domains are available:

```yaml
spec:
  topology:
    primaries: 3
    secondaries: 3
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 1
        minDomains: 3  # Require at least 3 zones
        whenUnsatisfiable: "DoNotSchedule"
```

## Standard Kubernetes Placement

The operator also supports standard Kubernetes placement options for fine-grained control:

### Node Selectors

Target specific node pools:

```yaml
spec:
  nodeSelector:
    node-type: "neo4j-optimized"
    storage-type: "nvme"
```

### Tolerations

Allow pods to schedule on tainted nodes:

```yaml
spec:
  tolerations:
    - key: "neo4j-dedicated"
      operator: "Equal"
      value: "true"
      effect: "NoSchedule"
```

### Custom Affinity Rules

For complex placement requirements:

```yaml
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: node.kubernetes.io/instance-type
            operator: In
            values:
            - m5.2xlarge
            - m5.4xlarge
    podAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: app
              operator: In
              values:
              - monitoring
          topologyKey: topology.kubernetes.io/zone
```

## Topology Placement Strategies

### High Availability (Recommended)

For production clusters requiring maximum availability:

```yaml
spec:
  topology:
    primaries: 3      # Odd number for quorum
    secondaries: 3    # Match primaries for zone coverage
    enforceDistribution: true
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 1
        whenUnsatisfiable: "DoNotSchedule"
      antiAffinity:
        enabled: true
        topologyKey: "kubernetes.io/hostname"
        type: "required"
```

### Cost-Optimized

For development or cost-sensitive deployments:

```yaml
spec:
  topology:
    primaries: 3
    secondaries: 1
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 2  # Allow more imbalance
        whenUnsatisfiable: "ScheduleAnyway"
      antiAffinity:
        enabled: true
        topologyKey: "kubernetes.io/hostname"
        type: "preferred"  # Soft constraint
```

### Single-Zone Development

For development environments without zone requirements:

```yaml
spec:
  topology:
    primaries: 1
    secondaries: 0
    # No placement configuration needed
```

## Troubleshooting

### Pods Stuck in Pending

Check topology constraints:

```bash
# Describe the pending pod
kubectl describe pod <pod-name>

# Check available nodes and their zones
kubectl get nodes -L topology.kubernetes.io/zone

# Verify node capacity
kubectl top nodes
```

### Uneven Distribution

Verify zone labels and capacity:

```bash
# Count pods per zone
kubectl get pods -l app.kubernetes.io/instance=<cluster-name> \
  -o custom-columns=NAME:.metadata.name,ZONE:.spec.nodeName \
  | xargs -I {} sh -c 'echo {} $(kubectl get node $(echo {} | awk "{print \$2}") -L topology.kubernetes.io/zone --no-headers | awk "{print \$6}")'

# Check topology spread constraints
kubectl get pod <pod-name> -o yaml | grep -A10 topologySpreadConstraints
```

### Validation Warnings

The operator emits warnings for suboptimal configurations:

```bash
# Check operator events
kubectl get events --field-selector reason=TopologyWarning

# View cluster status
kubectl describe neo4jenterprisecluster <cluster-name>
```

## Best Practices

1. **Use Odd Numbers for Primaries**: 3, 5, or 7 primaries provide optimal quorum behavior
2. **Match Secondary Count to Zones**: Deploy at least one secondary per zone for read availability
3. **Enable enforceDistribution**: Ensures primaries are distributed for quorum safety
4. **Start with Soft Constraints**: Use `preferred` anti-affinity and `ScheduleAnyway` during initial deployment
5. **Monitor Zone Capacity**: Ensure each zone has sufficient resources for your topology
6. **Test Failure Scenarios**: Verify cluster behavior when zones become unavailable

## Examples

### Enterprise Production Cluster

Complete example for a production-grade deployment:

```yaml
apiVersion: neo4j.io/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: production-neo4j
spec:
  edition: enterprise
  image:
    repository: neo4j
    tag: "5.26.0-enterprise"

  topology:
    primaries: 3
    secondaries: 3
    availabilityZones:
      - us-east-1a
      - us-east-1b
      - us-east-1c
    enforceDistribution: true
    placement:
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 1
        whenUnsatisfiable: "DoNotSchedule"
        minDomains: 3
      antiAffinity:
        enabled: true
        topologyKey: "kubernetes.io/hostname"
        type: "required"

  # Node selection for database workloads
  nodeSelector:
    workload-type: "database"

  # Tolerate database-dedicated nodes
  tolerations:
    - key: "dedicated"
      operator: "Equal"
      value: "database"
      effect: "NoSchedule"

  storage:
    className: "fast-ssd"
    size: "100Gi"

  resources:
    requests:
      memory: "8Gi"
      cpu: "2"
    limits:
      memory: "16Gi"
      cpu: "4"
```

## Migration Guide

If you have existing clusters without topology placement:

1. **Add Soft Constraints First**:
   ```yaml
   placement:
     antiAffinity:
       enabled: true
       type: "preferred"
   ```

2. **Gradually Introduce Zone Spreading**:
   ```yaml
   placement:
     topologySpread:
       enabled: true
       whenUnsatisfiable: "ScheduleAnyway"
   ```

3. **Tighten Constraints After Validation**:
   - Change `preferred` to `required`
   - Change `ScheduleAnyway` to `DoNotSchedule`

## Related Documentation

- [High Availability Guide](./high_availability.md)
- [Fault Tolerance Guide](./guides/fault_tolerance.md)
- [Performance Tuning](./performance.md)
- [Kubernetes Topology Documentation](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/)
