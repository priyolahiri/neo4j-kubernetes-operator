# Auto-Scaling Guide

## Overview

The Neo4j Kubernetes Operator provides automatic scaling capabilities for read replicas based on various metrics such as CPU usage, memory consumption, query latency, and connection count. This feature ensures optimal performance and resource utilization while handling varying workloads.

## Features

- **HPA Integration**: Uses Kubernetes Horizontal Pod Autoscaler for scaling decisions
- **Multiple Metrics**: Supports CPU, memory, query latency, and connection-based scaling
- **Custom Metrics**: Integration with Prometheus for advanced metrics
- **Configurable Behavior**: Customizable scaling policies and stabilization windows
- **Read Replica Focus**: Scales secondary instances while maintaining primary stability

## Prerequisites

- Kubernetes cluster with Metrics Server installed
- Neo4j Enterprise Edition cluster
- Prometheus (for custom metrics)
- Sufficient cluster resources for scaling

## Basic Configuration

### Simple CPU-based Auto-scaling

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: my-neo4j-cluster
  namespace: default
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  topology:
    primaries: 3
    secondaries: 2
  autoScaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    metrics:
    - type: cpu
      target: "70"
  # ... other configuration
```

### Multi-metric Auto-scaling

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: advanced-neo4j-cluster
  namespace: default
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  topology:
    primaries: 3
    secondaries: 2
  autoScaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 15
    metrics:
    - type: cpu
      target: "70"
    - type: memory
      target: "80"
    - type: query_latency
      target: "2s"
    - type: connection_count
      target: "100"
    behavior:
      scaleUp:
        stabilizationWindowSeconds: 60
        policies:
        - type: Percent
          value: 100
          periodSeconds: 15
        - type: Pods
          value: 2
          periodSeconds: 60
      scaleDown:
        stabilizationWindowSeconds: 300
        policies:
        - type: Percent
          value: 50
          periodSeconds: 60
  # ... other configuration
```

## Advanced Configuration

### Custom Scaling Behavior

The `behavior` section allows fine-tuning of scaling policies:

```yaml
autoScaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: cpu
    target: "65"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60      # Wait 1 minute before scaling up
      selectPolicy: Max                   # Use the policy that allows most scaling
      policies:
      - type: Percent
        value: 100                        # Scale up by 100% (doubling)
        periodSeconds: 15
      - type: Pods
        value: 4                          # Or add 4 pods
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300     # Wait 5 minutes before scaling down
      selectPolicy: Min                   # Use the policy that allows least scaling
      policies:
      - type: Percent
        value: 50                         # Scale down by 50%
        periodSeconds: 60
      - type: Pods
        value: 2                          # Or remove 2 pods
        periodSeconds: 120
```

### Prometheus Integration

For custom metrics, ensure Prometheus is configured:

```yaml
autoScaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: custom
    name: neo4j_queries_per_second
    target: "50"
  - type: custom
    name: neo4j_slow_queries_rate
    target: "5"
  prometheusConfig:
    enabled: true
    serviceMonitor: true
    scrapeInterval: "30s"
```

## Monitoring and Observability

### Viewing HPA Status

```bash
# Check HPA status
kubectl get hpa

# Detailed HPA information
kubectl describe hpa <cluster-name>-secondary

# View scaling events
kubectl get events --field-selector involvedObject.kind=HorizontalPodAutoscaler
```

### Metrics to Monitor

1. **Current Replicas**: Number of running secondary instances
2. **Desired Replicas**: Target number based on metrics
3. **CPU/Memory Utilization**: Current resource usage
4. **Query Latency**: Average query response time
5. **Connection Count**: Active database connections

### Grafana Dashboard

Example queries for monitoring auto-scaling:

```promql
# Current replica count
neo4j_cluster_secondary_replicas{cluster="my-neo4j-cluster"}

# CPU utilization
rate(container_cpu_usage_seconds_total[5m]) * 100

# Memory utilization
container_memory_working_set_bytes / container_spec_memory_limit_bytes * 100

# Query latency
neo4j_query_execution_latency_seconds_bucket
```

## Troubleshooting

### Common Issues

#### HPA Not Scaling

**Symptoms**: Replicas remain constant despite high metrics

**Solutions**:
1. Check Metrics Server is running:
   ```bash
   kubectl get deployment metrics-server -n kube-system
   ```

2. Verify HPA can get metrics:
   ```bash
   kubectl describe hpa <cluster-name>-secondary
   ```

3. Check resource requests are set:
   ```yaml
   resources:
     requests:
       cpu: "500m"
       memory: "1Gi"
   ```

#### Scaling Too Aggressive

**Symptoms**: Frequent scaling up/down events

**Solutions**:
1. Increase stabilization window:
   ```yaml
   behavior:
     scaleUp:
       stabilizationWindowSeconds: 180
     scaleDown:
       stabilizationWindowSeconds: 600
   ```

2. Adjust metric targets:
   ```yaml
   metrics:
   - type: cpu
     target: "60"  # Lower threshold for earlier scaling
   ```

#### Resource Limits Reached

**Symptoms**: Scaling stops at certain point

**Solutions**:
1. Check cluster resource availability:
   ```bash
   kubectl describe nodes
   ```

2. Verify resource quotas:
   ```bash
   kubectl describe quota -n <namespace>
   ```

3. Adjust resource requests/limits:
   ```yaml
   resources:
     requests:
       cpu: "250m"      # Reduce requests
       memory: "512Mi"
     limits:
       cpu: "1000m"
       memory: "2Gi"
   ```

### Debugging Commands

```bash
# View HPA configuration
kubectl get hpa <cluster-name>-secondary -o yaml

# Check scaling events
kubectl get events --sort-by=.metadata.creationTimestamp

# Monitor resource usage
kubectl top pods -n <namespace>

# View operator logs
kubectl logs -f deployment/neo4j-operator-controller-manager -n neo4j-operator-system
```

## Best Practices

### 1. Set Appropriate Targets

- **CPU**: 60-80% for normal workloads
- **Memory**: 70-85% to avoid OOM kills
- **Query Latency**: Based on SLA requirements
- **Connections**: 70-80% of max connections

### 2. Configure Stabilization Windows

- **Scale Up**: 60-180 seconds (faster response)
- **Scale Down**: 300-600 seconds (avoid flapping)

### 3. Resource Planning

```yaml
resources:
  requests:
    cpu: "500m"       # Ensure predictable scheduling
    memory: "1Gi"
  limits:
    cpu: "2000m"      # Allow burst capacity
    memory: "4Gi"
```

### 4. Monitor Scaling Patterns

- Set up alerts for scaling events
- Review scaling history regularly
- Adjust thresholds based on usage patterns

### 5. Test Scaling Behavior

```bash
# Generate load to test scaling
kubectl run -i --tty load-generator --rm --image=busybox --restart=Never -- /bin/sh

# From inside the pod, generate queries
while true; do
  # Your load generation logic
  sleep 1
done
```

## Example: Complete Auto-scaling Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: production-neo4j
  namespace: production
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  
  topology:
    primaries: 3
    secondaries: 3
  
  autoScaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    
    metrics:
    - type: cpu
      target: "70"
    - type: memory
      target: "75"
    - type: query_latency
      target: "1.5s"
    - type: connection_count
      target: "80"
    
    behavior:
      scaleUp:
        stabilizationWindowSeconds: 120
        selectPolicy: Max
        policies:
        - type: Percent
          value: 50
          periodSeconds: 30
        - type: Pods
          value: 3
          periodSeconds: 60
      
      scaleDown:
        stabilizationWindowSeconds: 300
        selectPolicy: Min
        policies:
        - type: Percent
          value: 25
          periodSeconds: 60
        - type: Pods
          value: 1
          periodSeconds: 120
  
  resources:
    requests:
      cpu: "1000m"
      memory: "2Gi"
    limits:
      cpu: "4000m"
      memory: "8Gi"
  
  storage:
    className: fast-ssd
    size: 100Gi
  
  # Enable monitoring
  monitoring:
    enabled: true
    prometheus:
      enabled: true
      serviceMonitor: true
      scrapeInterval: "30s"
  
  auth:
    adminSecret: neo4j-admin-secret
```

## Security Considerations

1. **RBAC**: Ensure proper permissions for HPA operations
2. **Resource Limits**: Set appropriate limits to prevent resource exhaustion
3. **Network Policies**: Configure network access for scaled instances
4. **Monitoring**: Enable audit logging for scaling events

## Integration with Other Features

Auto-scaling works seamlessly with other enterprise features:

- **Blue-Green Deployments**: Scaling during deployments
- **Disaster Recovery**: Cross-region scaling considerations
- **Multi-tenancy**: Per-tenant scaling policies
- **Query Monitoring**: Metrics-driven scaling decisions

## Next Steps

- [Blue-Green Deployment Guide](./blue-green-deployment-guide.md)
- [Query Monitoring Guide](./query-monitoring-guide.md)
- [Performance Tuning Guide](./performance-tuning-guide.md)
- [Disaster Recovery Guide](./disaster-recovery-guide.md) 