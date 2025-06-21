# Disaster Recovery Guide

## Overview

The Neo4j Kubernetes Operator provides comprehensive disaster recovery capabilities including cross-region replication, automatic failover, and point-in-time recovery. This guide covers setup, configuration, and operational procedures for ensuring business continuity.

## Features

- **Cross-Region Replication**: Automatic data replication across regions
- **Automatic Failover**: Health monitoring with automated failover
- **Manual Failover**: Controlled failover procedures
- **Point-in-Time Recovery**: Transaction log shipping and replay
- **Network Configuration**: VPC peering and cross-region networking
- **Monitoring & Alerting**: Comprehensive health monitoring

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   Primary Region │    │ Secondary Region│
│   (us-east-1)   │    │   (us-west-2)   │
│                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Primary     │ │────▶ │ Secondary   │ │
│ │ Cluster     │ │ R  │ │ Cluster     │ │
│ │             │ │ E  │ │ (Standby)   │ │
│ └─────────────┘ │ P  │ └─────────────┘ │
│                 │ L    │                 │
│ ┌─────────────┐ │ I  │ ┌─────────────┐ │
│ │ Monitoring  │ │ C  │ │ Monitoring  │ │
│ │ & Alerts    │ │ A  │ │ & Alerts    │ │
│ └─────────────┘ │ T  │ └─────────────┘ │
└─────────────────┘ I    └─────────────────┘
                    O
                    N
```

## Prerequisites

- Two Kubernetes clusters in different regions
- VPC peering or equivalent cross-region networking
- Shared storage for transaction logs (S3, GCS, etc.)
- Network connectivity between regions
- Monitoring infrastructure (Prometheus, Grafana)

## Basic Configuration

### 1. Primary Cluster Setup

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: primary-cluster
  namespace: neo4j-production
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  
  topology:
    primaries: 3
    secondaries: 2
  
  storage:
    className: fast-ssd
    size: 500Gi
  
  # Enable disaster recovery
  disasterRecovery:
    enabled: true
    role: primary
    region: us-east-1
    
  auth:
    adminSecret: neo4j-admin-secret
```

### 2. Secondary Cluster Setup

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: secondary-cluster
  namespace: neo4j-production
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  
  topology:
    primaries: 3
    secondaries: 2
  
  storage:
    className: fast-ssd
    size: 500Gi
  
  # Configure as secondary
  disasterRecovery:
    enabled: true
    role: secondary
    region: us-west-2
    primaryEndpoint: "primary-cluster.neo4j-production.svc.cluster.local:7687"
    
  auth:
    adminSecret: neo4j-admin-secret
```

### 3. Disaster Recovery Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDisasterRecovery
metadata:
  name: cross-region-dr
  namespace: neo4j-production
spec:
  # Cluster references
  primaryClusterRef: primary-cluster
  secondaryClusterRef: secondary-cluster
  
  # Cross-region configuration
  crossRegion:
    primaryRegion: us-east-1
    secondaryRegion: us-west-2
    replicationMode: async  # or sync for synchronous replication
    
    # Network configuration
    networking:
      vpcPeering:
        enabled: true
        primaryVpcId: vpc-12345678
        secondaryVpcId: vpc-87654321
      
      # Custom endpoints for cross-region access
      endpoints:
        primary: "primary.neo4j.us-east-1.example.com"
        secondary: "secondary.neo4j.us-west-2.example.com"
  
  # Replication settings
  replication:
    interval: "5m"
    timeout: "30s"
    retries: 3
    compression: true
    encryption: true
    
    # Transaction log shipping
    logShipping:
      enabled: true
      destination:
        type: s3
        bucket: neo4j-dr-logs
        region: us-east-1
        path: /transaction-logs
      interval: "1m"
      compression: gzip
      encryption:
        enabled: true
        kmsKeyId: arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
  
  # Failover configuration
  failover:
    automatic: true
    healthCheck:
      interval: "30s"
      timeout: "10s"
      consecutiveFailures: 3
      
    # Recovery objectives
    rpo: "5m"   # Recovery Point Objective
    rto: "10m"  # Recovery Time Objective
    
    # Custom health checks
    customChecks:
    - name: connectivity-check
      cypherQuery: "RETURN 1 as result"
      expectedResult: "1"
      timeout: "5s"
    
    - name: replication-lag-check
      cypherQuery: "CALL dbms.cluster.overview()"
      maxLag: "30s"
  
  # Monitoring and alerting
  monitoring:
    enabled: true
    prometheus:
      enabled: true
      serviceMonitor: true
    
    alerting:
      enabled: true
      webhooks:
      - url: "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
        events: ["failover", "lag_threshold", "health_check_failed"]
      
      email:
        enabled: true
        recipients: ["ops-team@example.com"]
        smtpServer: "smtp.example.com:587"
        
  # Backup integration
  backup:
    enabled: true
    schedule: "0 2 * * *"  # Daily at 2 AM
    retention: "30d"
    destination:
      type: s3
      bucket: neo4j-dr-backups
      path: /backups
```

## Advanced Configuration

### Synchronous Replication

For zero data loss requirements:

```yaml
spec:
  crossRegion:
    replicationMode: sync
    
  replication:
    consistency: strong
    timeout: "60s"
    
  failover:
    rpo: "0s"  # Zero data loss
    rto: "5m"
```

### Multi-Region Setup (3+ Regions)

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDisasterRecovery
metadata:
  name: multi-region-dr
spec:
  primaryClusterRef: primary-cluster
  
  # Multiple secondary regions
  secondaryClusters:
  - name: secondary-us-west
    clusterRef: secondary-cluster-west
    region: us-west-2
    priority: 1
    
  - name: secondary-eu-west
    clusterRef: secondary-cluster-eu
    region: eu-west-1
    priority: 2
  
  # Cascading replication
  replication:
    mode: cascading
    topology:
      primary: us-east-1
      secondaries:
      - region: us-west-2
        source: us-east-1
      - region: eu-west-1
        source: us-west-2
```

## Operational Procedures

### Health Monitoring

```bash
# Check disaster recovery status
kubectl get neo4jdisasterrecovery cross-region-dr -o yaml

# View replication lag
kubectl describe neo4jdisasterrecovery cross-region-dr

# Monitor cluster health
kubectl get neo4jenterprisecluster primary-cluster -o yaml
kubectl get neo4jenterprisecluster secondary-cluster -o yaml
```

### Manual Failover

#### 1. Planned Failover (Maintenance)

```bash
# 1. Stop writes to primary
kubectl patch neo4jenterprisecluster primary-cluster --type='merge' -p='{"spec":{"readOnly":true}}'

# 2. Wait for replication to catch up
kubectl wait --for=condition=ReplicationSynced neo4jdisasterrecovery cross-region-dr --timeout=300s

# 3. Promote secondary to primary
kubectl patch neo4jdisasterrecovery cross-region-dr --type='merge' -p='{"spec":{"failover":{"target":"secondary"}}}'

# 4. Update application endpoints
# Update DNS or load balancer to point to secondary region

# 5. Verify failover
kubectl get neo4jenterprisecluster secondary-cluster -o jsonpath='{.status.role}'
```

#### 2. Emergency Failover

```bash
# Immediate failover (may result in data loss)
kubectl patch neo4jdisasterrecovery cross-region-dr --type='merge' -p='{"spec":{"failover":{"emergency":true,"target":"secondary"}}}'

# Check failover status
kubectl describe neo4jdisasterrecovery cross-region-dr
```

### Recovery Procedures

#### Point-in-Time Recovery

```bash
# Create restore point
kubectl create -f - <<EOF
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: disaster-recovery-restore
  namespace: neo4j-production
spec:
  clusterRef: secondary-cluster
  source:
    type: transaction-logs
    location:
      type: s3
      bucket: neo4j-dr-logs
      path: /transaction-logs
  pointInTime: "2025-01-15T10:30:00Z"
  options:
    validateConsistency: true
    skipIndexes: false
EOF
```

#### Failback Procedure

After primary region recovery:

```bash
# 1. Ensure primary cluster is healthy
kubectl get neo4jenterprisecluster primary-cluster

# 2. Sync data from current active (secondary)
kubectl patch neo4jdisasterrecovery cross-region-dr --type='merge' -p='{"spec":{"sync":{"direction":"reverse"}}}'

# 3. Wait for sync completion
kubectl wait --for=condition=SyncComplete neo4jdisasterrecovery cross-region-dr --timeout=1800s

# 4. Failback to primary
kubectl patch neo4jdisasterrecovery cross-region-dr --type='merge' -p='{"spec":{"failover":{"target":"primary"}}}'
```

## Monitoring and Alerting

### Key Metrics

1. **Replication Lag**: Time difference between primary and secondary
2. **Health Check Status**: Success/failure of health checks
3. **Network Latency**: Cross-region communication latency
4. **Transaction Log Shipping**: Status of log shipping
5. **Failover Events**: History of failover occurrences

### Prometheus Queries

```promql
# Replication lag
neo4j_disaster_recovery_replication_lag_seconds{cluster="cross-region-dr"}

# Health check failures
increase(neo4j_disaster_recovery_health_check_failures_total[5m])

# Failover events
increase(neo4j_disaster_recovery_failover_total[1h])

# Cross-region network latency
neo4j_disaster_recovery_network_latency_seconds
```

### Grafana Dashboard

Import the disaster recovery dashboard:

```json
{
  "dashboard": {
    "title": "Neo4j Disaster Recovery",
    "panels": [
      {
        "title": "Replication Lag",
        "type": "graph",
        "targets": [
          {
            "expr": "neo4j_disaster_recovery_replication_lag_seconds"
          }
        ]
      },
      {
        "title": "Health Status",
        "type": "stat",
        "targets": [
          {
            "expr": "neo4j_disaster_recovery_health_status"
          }
        ]
      }
    ]
  }
}
```

## Testing Disaster Recovery

### 1. DR Drill Planning

```yaml
# Create test DR configuration
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDisasterRecovery
metadata:
  name: dr-test
  namespace: neo4j-test
spec:
  # Test configuration with shorter timeouts
  primaryClusterRef: test-primary
  secondaryClusterRef: test-secondary
  
  failover:
    automatic: false  # Manual only for testing
    healthCheck:
      interval: "10s"
      timeout: "5s"
      consecutiveFailures: 2
```

### 2. Automated Testing

```bash
#!/bin/bash
# DR test script

echo "Starting DR test..."

# 1. Create test data
kubectl exec -it primary-cluster-0 -- cypher-shell -u neo4j -p password \
  "CREATE (n:TestNode {id: $(date +%s), timestamp: datetime()})"

# 2. Wait for replication
sleep 30

# 3. Verify data on secondary
kubectl exec -it secondary-cluster-0 -- cypher-shell -u neo4j -p password \
  "MATCH (n:TestNode) RETURN count(n)"

# 4. Perform failover test
kubectl patch neo4jdisasterrecovery dr-test --type='merge' \
  -p='{"spec":{"failover":{"target":"secondary"}}}'

# 5. Verify application connectivity
curl -f http://secondary-endpoint:7474/db/data/ || exit 1

echo "DR test completed successfully"
```

## Troubleshooting

### Common Issues

#### Replication Lag

**Symptoms**: High replication lag between regions

**Solutions**:
1. Check network connectivity:
   ```bash
   kubectl exec -it primary-cluster-0 -- ping secondary-endpoint
   ```

2. Verify bandwidth and latency:
   ```bash
   kubectl exec -it primary-cluster-0 -- iperf3 -c secondary-endpoint
   ```

3. Adjust replication settings:
   ```yaml
   replication:
     interval: "10m"  # Increase interval
     batchSize: 1000  # Reduce batch size
   ```

#### Failover Not Triggering

**Symptoms**: Automatic failover doesn't occur despite health check failures

**Solutions**:
1. Check health check configuration:
   ```bash
   kubectl describe neo4jdisasterrecovery cross-region-dr
   ```

2. Verify network policies:
   ```bash
   kubectl get networkpolicy -n neo4j-production
   ```

3. Check operator logs:
   ```bash
   kubectl logs -f deployment/neo4j-operator-controller-manager -n neo4j-operator-system
   ```

#### Split-Brain Scenarios

**Prevention**:
```yaml
failover:
  quorum:
    enabled: true
    witnesses: 3
  fencing:
    enabled: true
    method: api  # Use Kubernetes API for fencing
```

## Security Considerations

### Network Security

```yaml
crossRegion:
  networking:
    encryption:
      inTransit: true
      atRest: true
    
    # Network policies
    networkPolicies:
      enabled: true
      allowedCIDRs:
      - "10.0.0.0/8"
      - "172.16.0.0/12"
```

### Access Control

```yaml
auth:
  rbac:
    enabled: true
    roles:
    - name: disaster-recovery-admin
      permissions:
      - "disaster-recovery:*"
      - "cluster:failover"
      - "cluster:restore"
```

## Cost Optimization

### Resource Scaling

```yaml
# Scale down secondary during normal operations
spec:
  secondaryClusters:
  - name: secondary-us-west
    resources:
      requests:
        cpu: "500m"     # Reduced CPU
        memory: "2Gi"   # Reduced memory
    topology:
      primaries: 1      # Minimum for standby
      secondaries: 0
```

### Storage Optimization

```yaml
replication:
  compression: true
  
backup:
  compression: gzip
  dedupe: true
```

## Best Practices

1. **Regular Testing**: Schedule monthly DR drills
2. **Monitoring**: Set up comprehensive alerting
3. **Documentation**: Maintain runbooks and procedures
4. **Automation**: Automate routine DR operations
5. **Security**: Encrypt all cross-region communications
6. **Compliance**: Ensure DR meets regulatory requirements

## Integration Examples

### With CI/CD

```yaml
# GitOps workflow for DR configuration
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: neo4j-dr
spec:
  source:
    repoURL: https://github.com/your-org/neo4j-dr-config
    path: disaster-recovery
    targetRevision: main
  destination:
    server: https://kubernetes.default.svc
    namespace: neo4j-production
```

### With Terraform

```hcl
resource "aws_vpc_peering_connection" "neo4j_dr" {
  vpc_id      = var.primary_vpc_id
  peer_vpc_id = var.secondary_vpc_id
  peer_region = var.secondary_region
  
  tags = {
    Name = "neo4j-disaster-recovery"
  }
}
```

## Next Steps

- [Point-in-Time Recovery Guide](./point-in-time-recovery-guide.md)
- [Multi-Tenant Setup Guide](./multi-tenant-guide.md)
- [Performance Monitoring Guide](./query-monitoring-guide.md)
- [Backup and Restore Guide](../backup-restore-guide.md) 