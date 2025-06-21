# Point-in-Time Recovery Guide

This guide provides comprehensive instructions for implementing point-in-time recovery (PITR) with the Neo4j Kubernetes Operator.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Basic Configuration](#basic-configuration)
- [Advanced Configuration](#advanced-configuration)
- [Recovery Operations](#recovery-operations)
- [Monitoring and Validation](#monitoring-and-validation)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

Point-in-Time Recovery allows you to restore your Neo4j database to any specific moment in time, providing granular data protection and recovery capabilities.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Neo4j Cluster                            │
├─────────────────────────────────────────────────────────────┤
│  Transaction Logs → Continuous Backup → Object Storage     │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Neo4j Enterprise Edition 5.0+
- Kubernetes cluster with sufficient storage
- Object storage (S3, GCS, Azure Blob)
- Backup and restore operators

## Basic Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-pitr
  namespace: neo4j-system
spec:
  pointInTimeRecovery:
    enabled: true
    
    # Transaction log shipping
    logShipping:
      enabled: true
      interval: "30s"
      destination:
        type: "s3"
        bucket: "neo4j-pitr-logs"
        path: "cluster-logs/"
      
      compression: true
      encryption: true
    
    # Base backup configuration
    baseBackup:
      schedule: "0 2 * * *"  # Daily at 2 AM
      retention: "30d"
      destination:
        type: "s3"
        bucket: "neo4j-pitr-backups"
        path: "base-backups/"
    
    # Recovery settings
    recovery:
      maxRecoveryTime: "24h"
      parallelRestore: 3
      validationEnabled: true
```

## Advanced Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-advanced-pitr
  namespace: neo4j-system
spec:
  pointInTimeRecovery:
    enabled: true
    
    # Multi-tier storage strategy
    storage:
      tiers:
        - name: "hot"
          type: "local-ssd"
          retention: "7d"
          path: "/hot-logs"
        - name: "warm"
          type: "s3"
          retention: "90d"
          bucket: "neo4j-warm-logs"
        - name: "cold"
          type: "s3-glacier"
          retention: "7y"
          bucket: "neo4j-cold-logs"
    
    # Advanced log shipping
    logShipping:
      enabled: true
      mode: "continuous"
      batchSize: "10MB"
      flushInterval: "10s"
      
      # Shipping rules
      rules:
        - name: "critical-data"
          pattern: ".*MERGE.*User.*"
          priority: "high"
          retention: "1y"
        - name: "analytics"
          pattern: ".*MATCH.*aggregation.*"
          priority: "low" 
          retention: "30d"
    
    # Cross-region replication
    replication:
      enabled: true
      regions:
        - name: "primary"
          region: "us-west-2"
        - name: "secondary"
          region: "us-east-1"
      
      consistency: "eventual"
      maxReplicationDelay: "5m"
```

## Recovery Operations

### Point-in-Time Recovery

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: pitr-restore-20240101-120000
  namespace: neo4j-system
spec:
  sourceCluster: "neo4j-pitr"
  
  # Point-in-time specification
  pointInTime:
    timestamp: "2024-01-01T12:00:00Z"
    timezone: "UTC"
  
  # Recovery options
  recovery:
    targetCluster: "neo4j-recovery-target"
    createNewCluster: true
    
    # Recovery validation
    validation:
      enabled: true
      checks:
        - type: "data-integrity"
        - type: "consistency"
        - type: "performance"
```

### Recovery Scripts

```bash
#!/bin/bash
# scripts/pitr-recovery.sh

NAMESPACE=${1:-neo4j-system}
CLUSTER_NAME=${2:-neo4j-pitr}
TARGET_TIME=${3}
RECOVERY_NAME="pitr-recovery-$(date +%Y%m%d-%H%M%S)"

if [ -z "$TARGET_TIME" ]; then
    echo "Usage: $0 <namespace> <cluster-name> <target-time>"
    echo "Example: $0 neo4j-system neo4j-pitr '2024-01-01T12:00:00Z'"
    exit 1
fi

echo "Starting point-in-time recovery..."
echo "Target time: $TARGET_TIME"
echo "Recovery name: $RECOVERY_NAME"

# Create recovery manifest
cat <<EOF | kubectl apply -f -
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: $RECOVERY_NAME
  namespace: $NAMESPACE
spec:
  sourceCluster: "$CLUSTER_NAME"
  pointInTime:
    timestamp: "$TARGET_TIME"
  recovery:
    targetCluster: "$CLUSTER_NAME-recovery"
    createNewCluster: true
    validation:
      enabled: true
EOF

# Monitor recovery progress
kubectl wait --for=condition=Complete neo4jrestore/$RECOVERY_NAME -n $NAMESPACE --timeout=3600s

echo "Recovery completed successfully"
```

## Monitoring and Validation

### Recovery Metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: neo4j-pitr-metrics
  namespace: neo4j-system
spec:
  groups:
  - name: neo4j.pitr
    rules:
    - record: neo4j:pitr_log_shipping_rate
      expr: rate(neo4j_pitr_logs_shipped_total[5m])
    
    - record: neo4j:pitr_recovery_time
      expr: histogram_quantile(0.95, rate(neo4j_pitr_recovery_duration_seconds_bucket[1h]))
    
    - alert: Neo4jPITRLogShippingLag
      expr: neo4j_pitr_log_lag_seconds > 300
      for: 2m
      labels:
        severity: warning
      annotations:
        summary: "PITR log shipping lag detected"
```

### Validation Procedures

```bash
#!/bin/bash
# scripts/validate-pitr.sh

RECOVERY_CLUSTER=$1
NAMESPACE=${2:-neo4j-system}

echo "Validating PITR recovery for cluster: $RECOVERY_CLUSTER"

# Data integrity check
kubectl exec -it $RECOVERY_CLUSTER-0 -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "
CALL db.stats.retrieve('GRAPH COUNTS') YIELD section, data
RETURN section, data"

# Consistency check  
kubectl exec -it $RECOVERY_CLUSTER-0 -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "
CALL db.checkConstraints()"

echo "✓ PITR validation completed"
```

## Troubleshooting

### Common Issues

#### Log Shipping Failures
```bash
# Check log shipping status
kubectl get neo4jenterprisecluster neo4j-pitr -o jsonpath='{.status.pointInTimeRecovery.logShipping}'

# Check shipping pod logs
kubectl logs -l app=neo4j-pitr,component=log-shipper -n neo4j-system
```

#### Recovery Failures
```bash
# Check recovery status
kubectl get neo4jrestore -n neo4j-system

# View recovery logs
kubectl describe neo4jrestore pitr-restore-name -n neo4j-system
```

## Best Practices

### Storage Management
- Use appropriate storage tiers for different retention periods
- Implement lifecycle policies for cost optimization
- Monitor storage usage and costs

### Recovery Testing
- Regularly test recovery procedures
- Validate recovery time objectives (RTO)
- Document recovery processes

### Monitoring
- Monitor log shipping lag
- Track recovery success rates
- Alert on backup failures

## Related Documentation

- [Disaster Recovery Guide](./disaster-recovery-guide.md)
- [Backup and Restore Guide](../backup-restore-guide.md)
- [Auto-Scaling Guide](./auto-scaling-guide.md)

---

*For additional support, please refer to the [Neo4j Operator Documentation](../README.md).* 