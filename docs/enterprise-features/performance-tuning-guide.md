# Performance Tuning Guide

This guide provides comprehensive instructions for optimizing Neo4j performance in Kubernetes environments using the Neo4j Operator.

## Table of Contents

- [Overview](#overview)
- [System Requirements](#system-requirements)
- [JVM Configuration](#jvm-configuration)
- [Neo4j Configuration](#neo4j-configuration)
- [Kubernetes Optimization](#kubernetes-optimization)
- [Storage Performance](#storage-performance)
- [Network Optimization](#network-optimization)
- [Monitoring and Profiling](#monitoring-and-profiling)
- [Performance Testing](#performance-testing)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

Performance tuning Neo4j in Kubernetes requires optimization across multiple layers: JVM settings, Neo4j configuration, Kubernetes resources, storage, and network configuration.

### Performance Dimensions

- **Throughput**: Queries per second
- **Latency**: Response time per query
- **Scalability**: Performance under load
- **Resource Efficiency**: CPU, memory, and storage utilization

## System Requirements

### Minimum Requirements

```yaml
# Basic production setup
resources:
  requests:
    cpu: "2"
    memory: "8Gi"
    storage: "100Gi"
  limits:
    cpu: "4"
    memory: "16Gi"
```

### Recommended High-Performance Setup

```yaml
# High-performance production setup
resources:
  requests:
    cpu: "8"
    memory: "32Gi"
    storage: "1Ti"
  limits:
    cpu: "16"
    memory: "64Gi"

# Use high-performance storage
storageClassName: "fast-ssd"

# Node affinity for high-performance nodes
nodeSelector:
  node-type: "high-memory"
  storage-type: "nvme"
```

## JVM Configuration

### Heap Size Optimization

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-performance-tuned
spec:
  env:
    # Heap size - typically 50% of available memory
    - name: "NEO4J_dbms_memory_heap_initial__size"
      value: "16G"
    - name: "NEO4J_dbms_memory_heap_max__size"
      value: "16G"
    
    # GC Configuration
    - name: "NEO4J_dbms_jvm_additional"
      value: |
        -XX:+UseG1GC
        -XX:+UnlockExperimentalVMOptions
        -XX:+TrustFinalNonStaticFields
        -XX:+DisableExplicitGC
        -XX:MaxGCPauseMillis=100
        -XX:G1HeapRegionSize=32m
        -XX:InitiatingHeapOccupancyPercent=35
        -XX:G1MixedGCCountTarget=12
        -XX:G1OldCSetRegionThreshold=10
        -XX:G1MixedGCLiveThresholdPercent=85
```

### Advanced JVM Tuning

```yaml
env:
  # Large pages for better memory performance
  - name: "NEO4J_dbms_jvm_additional"
    value: |
      -XX:+UseLargePages
      -XX:LargePageSizeInBytes=2m
      
      # JIT Compilation optimization
      -XX:+UseCompressedOops
      -XX:+UseCompressedClassPointers
      -XX:ReservedCodeCacheSize=512m
      -XX:InitialCodeCacheSize=256m
      
      # String deduplication
      -XX:+UseStringDeduplication
      
      # NUMA awareness
      -XX:+UseNUMA
      
      # Aggressive optimizations
      -XX:+AggressiveOpts
      -XX:+UseFastAccessorMethods
```

## Neo4j Configuration

### Memory Configuration

```yaml
env:
  # Page cache - remainder of memory after heap
  - name: "NEO4J_dbms_memory_pagecache_size"
    value: "12G"
  
  # Transaction state memory
  - name: "NEO4J_dbms_memory_transaction_global__max__size"
    value: "2G"
  - name: "NEO4J_dbms_memory_transaction_max__size"
    value: "1G"
  
  # Query memory configuration
  - name: "NEO4J_dbms_memory_query_max__size"
    value: "2G"
  - name: "NEO4J_dbms_memory_query_global__max__size"
    value: "4G"
```

### Connection and Threading

```yaml
env:
  # Connection pool settings
  - name: "NEO4J_dbms_connector_bolt_thread__pool__min__size"
    value: "10"
  - name: "NEO4J_dbms_connector_bolt_thread__pool__max__size"
    value: "400"
  - name: "NEO4J_dbms_connector_bolt_thread__pool__keep__alive"
    value: "5m"
  
  # HTTP connector threading
  - name: "NEO4J_dbms_connector_http_thread__pool__min__size"
    value: "10"
  - name: "NEO4J_dbms_connector_http_thread__pool__max__size"
    value: "200"
  
  # Database threading
  - name: "NEO4J_dbms_threads_worker__count"
    value: "16"  # Typically number of CPU cores
```

### Query Optimization

```yaml
env:
  # Query cache configuration
  - name: "NEO4J_dbms_query_cache__size"
    value: "1000"
  - name: "NEO4J_dbms_query_cache__weak__reference__enabled"
    value: "true"
  
  # Cypher query tuning
  - name: "NEO4J_cypher_min__replan__interval"
    value: "10s"
  - name: "NEO4J_cypher_statistics__divergence__threshold"
    value: "0.5"
  - name: "NEO4J_cypher_replan__algorithm"
    value: "default"
  
  # Parallel query execution
  - name: "NEO4J_dbms_cypher_parallel__runtime__enabled"
    value: "true"
  - name: "NEO4J_dbms_cypher_parallel__runtime__worker__count"
    value: "8"
```

### Transaction Log Optimization

```yaml
env:
  # Transaction log settings
  - name: "NEO4J_dbms_tx__log_rotation_retention__policy"
    value: "100M size"
  - name: "NEO4J_dbms_tx__log_rotation_size"
    value: "25M"
  
  # Checkpoint configuration
  - name: "NEO4J_dbms_checkpoint_interval_time"
    value: "15s"
  - name: "NEO4J_dbms_checkpoint_interval_tx"
    value: "100000"
  
  # WAL configuration
  - name: "NEO4J_dbms_recovery_fail__on__missing__files"
    value: "false"
```

## Kubernetes Optimization

### Resource Requests and Limits

```yaml
# Optimal resource configuration
resources:
  requests:
    cpu: "8000m"        # Guaranteed CPU
    memory: "32Gi"      # Guaranteed memory
    ephemeral-storage: "10Gi"
  limits:
    cpu: "16000m"       # Allow burst capacity
    memory: "64Gi"      # Hard memory limit
    ephemeral-storage: "20Gi"

# Quality of Service class
priorityClassName: "high-priority"
```

### Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: neo4j-pdb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: neo4j-performance-tuned
```

### Node Affinity and Anti-Affinity

```yaml
affinity:
  # Node affinity for high-performance nodes
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: node-type
          operator: In
          values: ["high-performance"]
        - key: storage-type
          operator: In
          values: ["nvme-ssd"]
  
  # Pod anti-affinity for cluster members
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            app: neo4j-performance-tuned
        topologyKey: kubernetes.io/hostname
```

### Topology Spread Constraints

```yaml
topologySpreadConstraints:
- maxSkew: 1
  topologyKey: topology.kubernetes.io/zone
  whenUnsatisfiable: DoNotSchedule
  labelSelector:
    matchLabels:
      app: neo4j-performance-tuned
- maxSkew: 1
  topologyKey: kubernetes.io/hostname
  whenUnsatisfiable: ScheduleAnyway
  labelSelector:
    matchLabels:
      app: neo4j-performance-tuned
```

## Storage Performance

### High-Performance Storage Class

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: neo4j-high-performance
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp3
  iops: "10000"
  throughput: "1000"
  fsType: ext4
mountOptions:
- noatime
- nodiratime
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

### NVMe Local Storage

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: neo4j-local-nvme
provisioner: kubernetes.io/no-provisioner
parameters:
  fsType: ext4
mountOptions:
- noatime
- nodiratime
- nobarrier
volumeBindingMode: WaitForFirstConsumer
```

### Storage Optimization

```yaml
# Volume mount optimization
volumeMounts:
- name: data
  mountPath: /data
  mountPropagation: None
- name: logs
  mountPath: /logs
  mountPropagation: None
- name: transaction-logs
  mountPath: /transaction-logs
  mountPropagation: None

# Security context for performance
securityContext:
  fsGroup: 7474
  runAsUser: 7474
  runAsNonRoot: true
  capabilities:
    drop:
    - ALL
    add:
    - SYS_RESOURCE  # For memory locking
```

## Network Optimization

### Service Configuration

```yaml
apiVersion: v1
kind: Service
metadata:
  name: neo4j-performance-service
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: "true"
spec:
  type: LoadBalancer
  sessionAffinity: ClientIP
  sessionAffinityConfig:
    clientIP:
      timeoutSeconds: 3600
  ports:
  - name: bolt
    port: 7687
    targetPort: 7687
    protocol: TCP
  - name: http
    port: 7474
    targetPort: 7474
    protocol: TCP
```

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: neo4j-performance-network
spec:
  podSelector:
    matchLabels:
      app: neo4j-performance-tuned
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: neo4j-performance-tuned
    - namespaceSelector:
        matchLabels:
          name: application-namespace
    ports:
    - protocol: TCP
      port: 7687
    - protocol: TCP
      port: 7474
    - protocol: TCP
      port: 7473
```

## Monitoring and Profiling

### Performance Metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: neo4j-performance-metrics
spec:
  selector:
    matchLabels:
      app: neo4j-performance-tuned
  endpoints:
  - port: metrics
    interval: 15s
    scrapeTimeout: 10s
    path: /metrics
```

### Key Performance Metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: neo4j-performance-rules
spec:
  groups:
  - name: neo4j.performance
    rules:
    # Query performance
    - record: neo4j:query_duration_p95
      expr: histogram_quantile(0.95, rate(neo4j_cypher_query_duration_seconds_bucket[5m]))
    
    # Memory utilization
    - record: neo4j:memory_utilization
      expr: (neo4j_memory_pool_used_bytes / neo4j_memory_pool_max_bytes) * 100
    
    # Page cache hit ratio
    - record: neo4j:page_cache_hit_ratio
      expr: rate(neo4j_page_cache_hits_total[5m]) / (rate(neo4j_page_cache_hits_total[5m]) + rate(neo4j_page_cache_faults_total[5m])) * 100
    
    # Transaction throughput
    - record: neo4j:transaction_rate
      expr: rate(neo4j_transaction_started_total[5m])
    
    # GC performance
    - record: neo4j:gc_pause_time_p95
      expr: histogram_quantile(0.95, rate(neo4j_gc_pause_seconds_bucket[5m]))
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Neo4j Performance Dashboard",
    "panels": [
      {
        "title": "Query Performance",
        "type": "graph",
        "targets": [
          {
            "expr": "neo4j:query_duration_p95",
            "legendFormat": "P95 Query Duration"
          }
        ]
      },
      {
        "title": "Memory Utilization",
        "type": "gauge",
        "targets": [
          {
            "expr": "neo4j:memory_utilization",
            "legendFormat": "Memory %"
          }
        ]
      },
      {
        "title": "Page Cache Performance",
        "type": "stat",
        "targets": [
          {
            "expr": "neo4j:page_cache_hit_ratio",
            "legendFormat": "Hit Ratio %"
          }
        ]
      }
    ]
  }
}
```

## Performance Testing

### Load Testing Script

```bash
#!/bin/bash
# scripts/performance-test.sh

NEO4J_URI=${1:-bolt://neo4j-service:7687}
USERNAME=${2:-neo4j}
PASSWORD=${3:-password}
DURATION=${4:-300}  # 5 minutes
THREADS=${5:-10}

echo "Starting performance test..."
echo "URI: $NEO4J_URI"
echo "Duration: ${DURATION}s"
echo "Concurrent threads: $THREADS"

# Create test data
echo "Creating test data..."
python3 -c "
import time
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

driver = GraphDatabase.driver('$NEO4J_URI', auth=('$USERNAME', '$PASSWORD'))

def create_test_data():
    with driver.session() as session:
        session.run('''
        UNWIND range(1, 10000) as i
        CREATE (u:User {id: i, name: 'User' + toString(i), created: timestamp()})
        ''')
        
        session.run('''
        MATCH (u:User)
        WITH u LIMIT 10000
        UNWIND range(1, 3) as i
        WITH u, rand() as r
        MATCH (u2:User)
        WHERE u2.id = toInteger(r * 10000) + 1 AND u <> u2
        CREATE (u)-[:FOLLOWS]->(u2)
        ''')

create_test_data()
print('Test data created')

# Performance test queries
queries = [
    'MATCH (u:User) RETURN count(u)',
    'MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN count(*)',
    'MATCH (u:User {id: \$userId}) RETURN u',
    'MATCH (u:User)-[:FOLLOWS*2]->(f:User) RETURN count(*) LIMIT 1000',
]

results = {'total_queries': 0, 'total_time': 0, 'errors': 0}
lock = threading.Lock()

def run_query(query, params=None):
    try:
        start = time.time()
        with driver.session() as session:
            result = session.run(query, params or {})
            list(result)  # Consume result
        end = time.time()
        
        with lock:
            results['total_queries'] += 1
            results['total_time'] += (end - start)
    except Exception as e:
        with lock:
            results['errors'] += 1

def worker():
    end_time = time.time() + $DURATION
    while time.time() < end_time:
        import random
        query = random.choice(queries)
        params = {'userId': random.randint(1, 10000)} if '\$userId' in query else None
        run_query(query, params)

# Run performance test
start_time = time.time()
with ThreadPoolExecutor(max_workers=$THREADS) as executor:
    futures = [executor.submit(worker) for _ in range($THREADS)]
    for future in as_completed(futures):
        future.result()

end_time = time.time()
total_duration = end_time - start_time

# Results
print(f'Performance Test Results:')
print(f'Total Duration: {total_duration:.2f}s')
print(f'Total Queries: {results[\"total_queries\"]}')
print(f'Queries per Second: {results[\"total_queries\"] / total_duration:.2f}')
print(f'Average Response Time: {(results[\"total_time\"] / results[\"total_queries\"]) * 1000:.2f}ms')
print(f'Error Rate: {(results[\"errors\"] / (results[\"total_queries\"] + results[\"errors\"])) * 100:.2f}%')

driver.close()
"
```

### Benchmark Results Analysis

```python
#!/usr/bin/env python3
# scripts/analyze-benchmark.py

import json
import matplotlib.pyplot as plt
import pandas as pd

def analyze_benchmark_results(results_file):
    """Analyze benchmark results and generate performance report"""
    
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    # Create performance summary
    summary = {
        'Total Queries': results['total_queries'],
        'QPS': results['qps'],
        'Avg Response Time (ms)': results['avg_response_time_ms'],
        'P95 Response Time (ms)': results['p95_response_time_ms'],
        'Error Rate (%)': results['error_rate_percent'],
        'CPU Utilization (%)': results['cpu_utilization'],
        'Memory Utilization (%)': results['memory_utilization']
    }
    
    # Generate performance charts
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # Response time distribution
    ax1.hist(results['response_times'], bins=50, alpha=0.7)
    ax1.set_title('Response Time Distribution')
    ax1.set_xlabel('Response Time (ms)')
    ax1.set_ylabel('Frequency')
    
    # QPS over time
    ax2.plot(results['qps_timeline'])
    ax2.set_title('Queries per Second Over Time')
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('QPS')
    
    # Resource utilization
    ax3.plot(results['cpu_timeline'], label='CPU %')
    ax3.plot(results['memory_timeline'], label='Memory %')
    ax3.set_title('Resource Utilization')
    ax3.set_xlabel('Time (seconds)')
    ax3.set_ylabel('Utilization %')
    ax3.legend()
    
    # Error rate
    ax4.plot(results['error_rate_timeline'])
    ax4.set_title('Error Rate Over Time')
    ax4.set_xlabel('Time (seconds)')
    ax4.set_ylabel('Error Rate %')
    
    plt.tight_layout()
    plt.savefig('benchmark_analysis.png', dpi=300, bbox_inches='tight')
    
    # Performance recommendations
    recommendations = []
    
    if summary['Avg Response Time (ms)'] > 100:
        recommendations.append("Consider increasing memory allocation or optimizing queries")
    
    if summary['CPU Utilization (%)'] > 80:
        recommendations.append("Consider increasing CPU limits or scaling horizontally")
    
    if summary['Error Rate (%)'] > 1:
        recommendations.append("Investigate error patterns and optimize connection pooling")
    
    print("Performance Summary:")
    print("=" * 50)
    for key, value in summary.items():
        print(f"{key}: {value}")
    
    if recommendations:
        print("\nRecommendations:")
        print("=" * 50)
        for rec in recommendations:
            print(f"- {rec}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python analyze-benchmark.py <results.json>")
        sys.exit(1)
    
    analyze_benchmark_results(sys.argv[1])
```

## Troubleshooting

### Common Performance Issues

#### High Query Latency

**Diagnosis:**
```bash
# Check slow queries
kubectl exec neo4j-0 -- cypher-shell -u neo4j -p $PASSWORD "
CALL db.stats.retrieve('QUERY') YIELD section, data
WHERE section = 'slow_queries'
RETURN data ORDER BY data.duration DESC LIMIT 10"

# Check query plans
kubectl exec neo4j-0 -- cypher-shell -u neo4j -p $PASSWORD "
EXPLAIN MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN count(*)"
```

**Solutions:**
```bash
# Add missing indexes
kubectl exec neo4j-0 -- cypher-shell -u neo4j -p $PASSWORD "
CREATE INDEX user_id_index FOR (u:User) ON (u.id)"

# Optimize query
kubectl exec neo4j-0 -- cypher-shell -u neo4j -p $PASSWORD "
PROFILE MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN count(*)"
```

#### Memory Issues

**Diagnosis:**
```bash
# Check memory usage
kubectl top pod neo4j-0

# Check JVM memory
kubectl exec neo4j-0 -- jcmd 1 VM.info | grep -E "(heap|memory)"

# Check page cache statistics
kubectl exec neo4j-0 -- cypher-shell -u neo4j -p $PASSWORD "
CALL db.stats.retrieve('PAGE_CACHE')"
```

**Solutions:**
```bash
# Increase heap size
kubectl patch neo4jenterprisecluster neo4j-performance-tuned --type='merge' -p='
{
  "spec": {
    "env": [
      {
        "name": "NEO4J_dbms_memory_heap_max__size",
        "value": "32G"
      }
    ]
  }
}'
```

#### Storage Performance Issues

**Diagnosis:**
```bash
# Check I/O statistics
kubectl exec neo4j-0 -- iostat -x 1 5

# Check disk usage
kubectl exec neo4j-0 -- df -h /data

# Check storage class performance
kubectl describe storageclass neo4j-high-performance
```

## Best Practices

### Configuration Management
- Use GitOps for configuration management
- Version control all performance settings
- Document all configuration changes
- Test configuration changes in staging first

### Monitoring Strategy
- Monitor key performance indicators continuously
- Set up alerts for performance degradation
- Use distributed tracing for complex queries
- Implement capacity planning based on trends

### Testing and Validation
- Perform regular performance testing
- Use realistic data volumes and query patterns
- Test under various load conditions
- Validate performance after configuration changes

### Capacity Planning
- Monitor growth trends
- Plan for peak load scenarios
- Consider seasonal variations
- Implement auto-scaling where appropriate

## Related Documentation

- [Auto-Scaling Guide](./auto-scaling-guide.md)
- [Query Performance Monitoring](./query-monitoring-guide.md)
- [Disaster Recovery Guide](./disaster-recovery-guide.md)

---

*For additional support, please refer to the [Neo4j Operator Documentation](../README.md).* 