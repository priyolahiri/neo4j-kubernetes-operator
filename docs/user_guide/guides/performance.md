# Performance

This guide explains how to tune the performance of your Neo4j Enterprise clusters.

## Operator Performance Optimizations

The Neo4j Enterprise Operator includes several performance optimizations for production environments:

### Reconciliation Efficiency
- **Optimized Rate Limiting**: The cluster controller caps reconciliations at roughly 10 per minute (a token-bucket limiter of one token every 6 seconds with a burst of 10), backed by exponential backoff from 5 seconds up to 30 seconds, to prevent excessive API calls
- **Status Update Optimization**: Status updates only occur when cluster state actually changes, reducing unnecessary API server load
- **ConfigMap Debouncing**: A short debounce interval coalesces rapid successive configuration changes, preventing restart loops

### Many large clusters

A `Ready` cluster or standalone is reconciled whenever something in Kubernetes changes (its spec, a pod, the StatefulSet) and, when nothing does, again after `--ready-poll-interval` (default `30s`). That timed pass is how the operator notices what only Neo4j knows — a server or database turning unhealthy with no pod changing. Each pass queries Neo4j over Bolt, so one operator watching many clusters can raise the interval: at `2m`, a database that goes offline shows in `DatabasesHealthy` within two minutes instead of thirty seconds. A cluster that is forming, upgrading, rolling or short of a server is not affected; it keeps its own cadence.

Split-brain detection is the expensive part of a cluster's pass: it opens a Bolt connection to **every** server and compares their views of the cluster. A `Ready` cluster runs it in full at most every `--split-brain-check-interval` (default `5m`), and in between relies on `SHOW SERVERS` through the client Service. It runs at once, on that pass, when:

- a server pod has been deleted or recreated, restarted a container, or changed readiness since the last full check — the moments a server can come back on its own and form a separate cluster;
- `SHOW SERVERS` lists fewer `Enabled`, `Available` servers than `spec.topology.servers`;
- the cluster is not `Ready` (`Degraded` checks on every pass);
- the operator has restarted, so it has no clean check on record.

`--split-brain-check-interval=0s` restores a full check on every pass. With Helm, set `readyPollInterval` and `splitBrainCheckInterval`.

### Resource Management
- **Memory Validation**: Automatic validation ensures Neo4j memory settings don't exceed available resources
- **Resource Recommendations**: Built-in recommendations for optimal CPU and memory allocation based on cluster size
- **Low-overhead observability**: The operator exposes a small set of Prometheus metrics (see [Monitoring](monitoring.md)); it does not poll node or pod utilization itself

## Resource Allocation

One of the most important factors for performance is resource allocation. You can configure the CPU and memory resources for your Neo4j pods using the `spec.resources` field in the `Neo4jEnterpriseCluster` resource. It is crucial to set both `requests` and `limits` for predictable performance.

```yaml
    resources:
      requests:
        cpu: "2"
        memory: "4Gi"
      limits:
        cpu: "4"
        memory: "8Gi"
```

### Memory Validation and Recommendations

The operator includes intelligent memory validation that:

- Ensures Neo4j heap settings don't exceed available container memory
- Provides automatic recommendations for optimal memory allocation
- Validates memory ratios between heap, page cache, and system overhead

```yaml
    resources:
      requests:
        memory: "4Gi"    # Minimum for stable operation
      limits:
        memory: "8Gi"    # Allows 4-6GB for Neo4j heap + page cache
```

## JVM Tuning

For advanced use cases, you can tune the JVM settings for your Neo4j pods using environment variables in the `spec.env` field. This allows you to control settings like heap size, garbage collection, and more.

```yaml
    env:
      - name: NEO4J_server_memory_heap_initial__size
        value: "4G"
      - name: NEO4J_server_memory_heap_max__size
        value: "4G"
      - name: NEO4J_server_memory_pagecache_size
        value: "2G"
```

## Performance Monitoring

The operator does not track resource utilization or detect bottlenecks itself. Use the standard Kubernetes and Prometheus tooling for that, and the operator's own metrics for the operator's behaviour.

### Resource utilization
- **CPU, memory and storage**: observe pod and node usage with `kubectl top` or Prometheus (cAdvisor / kubelet metrics and kube-state-metrics). Compare usage with `spec.resources` and the PVC size.
- **Neo4j metrics**: transaction rates, query counts, page-cache and JVM metrics come from Neo4j's own Prometheus endpoint, which the operator enables with `spec.monitoring.enabled` (see [Monitoring](monitoring.md)).
- **Capacity before apply**: `kubectl neo4j preflight -f <manifest>` checks that a Ready node has enough allocatable memory for the requested pods.

### Operator behaviour
- **Operator metrics**: reconcile counts and durations, upgrade and backup outcomes, and resource-version conflicts. The full list, and which families are populated, is in [Monitoring](monitoring.md).
- **Cluster health**: `status.phase`, the `Ready`, `ServersHealthy` and `DatabasesHealthy` conditions, and `status.diagnostics` (live `SHOW SERVERS` / `SHOW DATABASES`).

## Best Practices

### For Production Deployments
1. **Set appropriate resource limits**: Ensure containers have sufficient CPU and memory
2. **Use persistent storage**: Configure appropriate storage classes for your workload
3. **Enable monitoring**: Monitor both Kubernetes and Neo4j metrics
4. **Plan capacity**: Monitor trends and scale manually as needed

### For Development Environments
1. **Use smaller resource allocations**: Optimize for development machine resources
2. **Enable debug logging**: Set log levels for troubleshooting
3. **Use local storage**: Consider local storage for faster development cycles
