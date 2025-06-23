# Auto-scaling Guide for Neo4j Enterprise

## Overview

The Neo4j Kubernetes Operator provides intelligent auto-scaling capabilities for both primary and secondary nodes, allowing your cluster to automatically adapt to changing workloads while maintaining performance and cost efficiency.

## 🎯 Auto-scaling Features

### Core Capabilities

- **Primary Node Scaling**: Maintains odd numbers for quorum requirements with automatic quorum protection
- **Secondary Node Scaling**: Scales based on read workload demands with zone-aware distribution
- **Multi-metric Scaling**: CPU, memory, query latency, connection count, throughput, and custom Prometheus metrics
- **Zone-aware Scaling**: Distributes replicas across availability zones with configurable skew limits
- **Quorum Protection**: Prevents scaling that would break cluster quorum with health checks
- **Custom Scaling Algorithms**: Webhook-based scaling decisions for advanced use cases

## 🏗️ Basic Auto-scaling Configuration

### Simple CPU-based Scaling

```yaml
# basic-autoscaling.yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-autoscaling
spec:
  topology:
    primaries: 3
    secondaries: 2

  autoScaling:
    enabled: true

    primaries:
      enabled: true
      minReplicas: 3
      maxReplicas: 7
      metrics:
      - type: "cpu"
        target: "70"
        weight: "1.0"

    secondaries:
      enabled: true
      minReplicas: 1
      maxReplicas: 10
      metrics:
      - type: "cpu"
        target: "60"
        weight: "1.0"

  # Other configuration...
  image:
    repo: "neo4j"
    tag: "5.26-enterprise"

  storage:
    className: "gp3"
    size: "500Gi"
```

## 🔧 Advanced Multi-metric Scaling

### Production-ready Configuration

```yaml
# advanced-autoscaling.yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-production-autoscaling
spec:
  topology:
    primaries: 3
    secondaries: 2
    enforceDistribution: true
    availabilityZones:
      - "us-west-2a"
      - "us-west-2b"
      - "us-west-2c"

  autoScaling:
    enabled: true

    # Primary nodes auto-scaling
    primaries:
      enabled: true
      minReplicas: 3  # Must be odd for quorum
      maxReplicas: 7  # Must be odd for quorum

      # Multiple scaling metrics
      metrics:
      - type: "cpu"
        target: "70"
        weight: "1.0"
      - type: "memory"
        target: "80"
        weight: "0.8"
      - type: "query_latency"
        target: "100ms"
        weight: "1.2"
      - type: "connection_count"
        target: "100"
        weight: "1.0"

      # Quorum protection
      quorumProtection:
        enabled: true
        minHealthyPrimaries: 2
        healthCheck:
          interval: "30s"
          timeout: "10s"
          failureThreshold: 3

    # Secondary nodes auto-scaling
    secondaries:
      enabled: true
      minReplicas: 1
      maxReplicas: 20

      # Read-optimized metrics
      metrics:
      - type: "cpu"
        target: "60"
        weight: "1.0"
      - type: "connection_count"
        target: "100"
        weight: "1.5"
      - type: "throughput"
        target: "1000"
        weight: "1.0"
      - type: "custom"
        target: "80"
        weight: "1.2"
        customQuery: "rate(neo4j_bolt_messages_received_total[5m])"
        source:
          type: "prometheus"
          prometheus:
            serverUrl: "http://prometheus:9090"
            query: "rate(neo4j_bolt_messages_received_total[5m])"
            interval: "30s"

      # Zone-aware scaling
      zoneAware:
        enabled: true
        minReplicasPerZone: 1
        maxZoneSkew: 2
        zonePreference:
          - "us-west-2a"
          - "us-west-2b"
          - "us-west-2c"

    # Global scaling behavior
    behavior:
      scaleUp:
        stabilizationWindow: "60s"
        policies:
        - type: "Pods"
          value: 2
          period: "60s"
        - type: "Percent"
          value: 50
          period: "60s"
        selectPolicy: "Max"

      scaleDown:
        stabilizationWindow: "300s"  # 5 minutes
        policies:
        - type: "Pods"
          value: 1
          period: "60s"
        selectPolicy: "Min"

      # Coordination between primary and secondary scaling
      coordination:
        enabled: true
        primaryPriority: 8
        secondaryPriority: 5
        scalingDelay: "30s"

    # Advanced features
    advanced:
      customAlgorithms:
      - name: "workload-predictor"
        type: "webhook"
        webhook:
          url: "http://ml-service:8080/predict"
          method: "POST"
          timeout: "30s"
```

## 📊 Supported Scaling Metrics

### Built-in Metrics

| Metric Type | Description | Target Format | Use Case |
|-------------|-------------|---------------|----------|
| `cpu` | CPU utilization percentage | `"70"` (70%) | General workload scaling |
| `memory` | Memory utilization percentage | `"80"` (80%) | Memory-intensive workloads |
| `query_latency` | Average query response time | `"100ms"` | Performance-sensitive applications |
| `connection_count` | Active Neo4j connections | `"100"` (connections) | Connection-heavy workloads |
| `throughput` | Transactions per second | `"1000"` (TPS) | High-throughput applications |

### Custom Metrics (Prometheus)

```yaml
metrics:
- type: "custom"
  target: "80"
  weight: "1.0"
  customQuery: "rate(neo4j_bolt_messages_received_total[5m])"
  source:
    type: "prometheus"
    prometheus:
      serverUrl: "http://prometheus:9090"
      query: "avg(rate(neo4j_transactions_committed_total[5m]))"
      interval: "30s"
```

## 🛡️ Quorum Protection

### Automatic Quorum Maintenance

The operator automatically ensures primary nodes maintain quorum:

```yaml
primaries:
  quorumProtection:
    enabled: true
    minHealthyPrimaries: 2  # Minimum healthy primaries before blocking scale-down
    healthCheck:
      interval: "30s"       # Health check frequency
      timeout: "10s"        # Health check timeout
      failureThreshold: 3   # Failures before marking unhealthy
```

### Quorum Rules

- **Odd Numbers**: Primary replicas are automatically adjusted to odd numbers (3, 5, 7)
- **Health Checks**: Continuous monitoring of primary node health
- **Scale-down Protection**: Prevents scaling below minimum healthy threshold
- **Emergency Override**: `allowQuorumBreak: true` for emergency situations (not recommended)

## 🌍 Zone-Aware Scaling

### Multi-Zone Distribution

```yaml
secondaries:
  zoneAware:
    enabled: true
    minReplicasPerZone: 1     # Minimum replicas per zone
    maxZoneSkew: 2            # Maximum difference between zones
    zonePreference:           # Preferred zone order
      - "us-west-2a"
      - "us-west-2b"
      - "us-west-2c"
```

### Zone Distribution Logic

1. **Even Distribution**: Attempts to distribute replicas evenly across zones
2. **Skew Limits**: Prevents too many replicas in one zone
3. **Preference Order**: Scales zones in preferred order
4. **Fault Tolerance**: Ensures availability during zone failures

## ⚙️ Scaling Behavior Configuration

### Scale-Up Behavior

```yaml
behavior:
  scaleUp:
    stabilizationWindow: "60s"    # Wait before next scale-up
    policies:
    - type: "Pods"                # Add 2 pods maximum
      value: 2
      period: "60s"
    - type: "Percent"             # Or 50% increase maximum
      value: 50
      period: "60s"
    selectPolicy: "Max"           # Use the higher value
```

### Scale-Down Behavior

```yaml
behavior:
  scaleDown:
    stabilizationWindow: "300s"   # Wait 5 minutes before scale-down
    policies:
    - type: "Pods"                # Remove 1 pod maximum
      value: 1
      period: "60s"
    selectPolicy: "Min"           # Use conservative approach
```

## 🔗 Custom Scaling Algorithms

### Webhook-Based Scaling

```yaml
advanced:
  customAlgorithms:
  - name: "ml-predictor"
    type: "webhook"
    webhook:
      url: "http://ml-service:8080/predict"
      method: "POST"
      headers:
        Authorization: "Bearer token"
      timeout: "30s"
```

### Webhook Request Format

```json
{
  "cluster": "neo4j-production",
  "namespace": "default",
  "metrics": {
    "cpu": 75.5,
    "memory": 68.2,
    "connections": 95,
    "query_latency": "125ms",
    "throughput": 850
  },
  "current_replicas": {
    "primaries": 3,
    "secondaries": 4
  }
}
```

### Webhook Response Format

```json
{
  "action": "scale_up",
  "target_replicas": {
    "primaries": 3,
    "secondaries": 6
  },
  "reason": "Predicted traffic increase based on historical patterns",
  "confidence": 0.85
}
```

## 📈 Monitoring Auto-scaling

### Key Metrics to Monitor

```yaml
# Prometheus queries for monitoring auto-scaling
queries:
  - name: "scaling_events"
    query: 'increase(neo4j_operator_scaling_events_total[5m])'

  - name: "current_replicas"
    query: 'neo4j_cluster_replicas{type="primary"}'

  - name: "scaling_decisions"
    query: 'neo4j_operator_scaling_decision_duration_seconds'

  - name: "quorum_health"
    query: 'neo4j_cluster_healthy_primaries'
```

### Grafana Dashboard Example

```json
{
  "dashboard": {
    "title": "Neo4j Auto-scaling",
    "panels": [
      {
        "title": "Replica Count",
        "targets": [
          {
            "expr": "neo4j_cluster_replicas",
            "legendFormat": "{{type}} replicas"
          }
        ]
      },
      {
        "title": "Scaling Metrics",
        "targets": [
          {
            "expr": "neo4j_cluster_cpu_utilization",
            "legendFormat": "CPU %"
          },
          {
            "expr": "neo4j_cluster_memory_utilization",
            "legendFormat": "Memory %"
          }
        ]
      }
    ]
  }
}
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Scaling Not Triggered

**Symptoms**: Metrics exceed thresholds but no scaling occurs

**Solutions**:
```bash
# Check auto-scaling status
kubectl describe neo4jenterprisecluster my-cluster

# Verify metrics collection
kubectl logs -l app.kubernetes.io/name=neo4j-operator -c manager

# Check quorum protection
kubectl get events --field-selector involvedObject.name=my-cluster
```

#### 2. Quorum Protection Blocking Scale-Down

**Symptoms**: Scale-down blocked despite low metrics

**Solutions**:
```bash
# Check primary health
kubectl get pods -l neo4j.com/cluster=my-cluster,neo4j.com/role=primary

# Review quorum protection logs
kubectl logs -l app.kubernetes.io/name=neo4j-operator | grep quorum

# Temporarily adjust protection (emergency only)
kubectl patch neo4jenterprisecluster my-cluster --type='merge' -p='
spec:
  autoScaling:
    primaries:
      quorumProtection:
        minHealthyPrimaries: 1
'
```

#### 3. Zone Distribution Issues

**Symptoms**: Uneven replica distribution across zones

**Solutions**:
```bash
# Check zone distribution
kubectl get pods -l neo4j.com/cluster=my-cluster -o wide

# Verify zone configuration
kubectl describe nodes | grep topology.kubernetes.io/zone

# Review zone-aware scaling logs
kubectl logs -l app.kubernetes.io/name=neo4j-operator | grep zone-aware
```

## 📋 Best Practices

### 1. Metric Selection

- **Primary Nodes**: Focus on CPU, memory, and query latency
- **Secondary Nodes**: Emphasize connection count and throughput
- **Custom Metrics**: Use Prometheus for application-specific metrics

### 2. Threshold Configuration

- **Conservative Thresholds**: Start with higher thresholds (70-80%)
- **Gradual Adjustment**: Lower thresholds based on observed behavior
- **Weight Balancing**: Adjust metric weights based on workload characteristics

### 3. Scaling Behavior

- **Stabilization Windows**: Use longer windows for scale-down (5+ minutes)
- **Policy Limits**: Set reasonable pod/percentage limits
- **Coordination**: Enable coordination for complex workloads

### 4. Monitoring

- **Dashboard Setup**: Create comprehensive monitoring dashboards
- **Alert Configuration**: Set up alerts for scaling failures
- **Regular Review**: Periodically review scaling patterns and adjust

## 🔧 Configuration Examples

### High-Traffic Web Application

```yaml
autoScaling:
  enabled: true
  primaries:
    enabled: true
    minReplicas: 3
    maxReplicas: 7
    metrics:
    - type: "cpu"
      target: "60"
      weight: "1.0"
    - type: "query_latency"
      target: "50ms"
      weight: "2.0"  # High priority on latency
  secondaries:
    enabled: true
    minReplicas: 2
    maxReplicas: 15
    metrics:
    - type: "connection_count"
      target: "80"
      weight: "1.5"
    - type: "throughput"
      target: "2000"
      weight: "1.0"
```

### Analytics Workload

```yaml
autoScaling:
  enabled: true
  primaries:
    enabled: true
    minReplicas: 3
    maxReplicas: 5
    metrics:
    - type: "memory"
      target: "75"
      weight: "2.0"  # Memory-intensive queries
    - type: "cpu"
      target: "70"
      weight: "1.0"
  secondaries:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    metrics:
    - type: "cpu"
      target: "65"
      weight: "1.0"
    - type: "custom"
      target: "100"
      weight: "1.2"
      customQuery: "rate(neo4j_query_execution_time_total[5m])"
```

This auto-scaling guide reflects the actual capabilities implemented in the Neo4j Kubernetes Operator, providing users with accurate information about available features and configuration options.
