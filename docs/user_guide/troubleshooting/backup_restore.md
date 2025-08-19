# Backup & Restore Troubleshooting Guide

This comprehensive troubleshooting guide covers common issues with Neo4j backup and restore operations when using the Neo4j Kubernetes Operator.

## Overview

The Neo4j Kubernetes Operator provides comprehensive backup and restore capabilities including:
- **Automated backups** with scheduling and retention policies
- **Point-in-Time Recovery (PITR)** for Neo4j 2025.x
- **Multi-cloud storage** support (S3, GCS, Azure Blob)
- **Backup sidecars** automatically added to all pods
- **Automatic RBAC** management for backup operations

## Common Backup Issues

### Backup Job Failures

#### Symptom: Backup job fails to start
```bash
kubectl get jobs -l app.kubernetes.io/component=backup
# STATUS: Failed or no jobs created
```

**Diagnosis:**
```bash
# Check backup resource status
kubectl get neo4jbackup
kubectl describe neo4jbackup production-backup

# Check operator logs for backup controller errors
kubectl logs -n neo4j-operator-system deployment/neo4j-operator-controller-manager | grep -i backup

# Verify RBAC permissions
kubectl auth can-i create jobs --as=system:serviceaccount:default:production-cluster-backup
```

**Common Causes & Solutions:**

1. **Missing RBAC Permissions**:
   ```bash
   # The operator automatically creates RBAC - check if it exists
   kubectl get serviceaccount production-cluster-backup
   kubectl get role production-cluster-backup-role
   kubectl get rolebinding production-cluster-backup-binding

   # If missing, trigger operator reconciliation
   kubectl annotate neo4jenterprisecluster production-cluster operator.neo4j.com/force-reconcile="$(date +%s)"
   ```

2. **Storage Configuration Issues**:
   ```yaml
   # Verify storage configuration in backup spec
   spec:
     storage:
       s3:
         bucket: "valid-bucket-name"    # Must exist
         region: "us-west-2"            # Correct region
         # Credentials must be valid
   ```

3. **Cluster Reference Problems**:
   ```bash
   # Verify cluster exists and is ready
   kubectl get neo4jenterprisecluster production-cluster
   kubectl get pods -l neo4j.com/cluster=production-cluster
   ```

#### Symptom: Backup job starts but fails during execution

**Diagnosis:**
```bash
# Check backup job logs
kubectl logs job/production-backup-$(date +%Y%m%d)-001

# Check backup sidecar logs
kubectl logs production-cluster-server-0 -c backup-sidecar

# Check Neo4j server logs for backup-related errors
kubectl logs production-cluster-server-0 -c neo4j | grep -i backup
```

**Common Solutions:**

1. **Insufficient Disk Space**:
   ```bash
   # Check available storage
   kubectl exec production-cluster-server-0 -c backup-sidecar -- df -h /backup-staging

   # Solution: Increase backup sidecar storage or cleanup old backups
   ```

2. **Database Lock Issues**:
   ```bash
   # Check for long-running transactions
   kubectl exec production-cluster-server-0 -- cypher-shell -u neo4j -p password \
     "CALL db.listTransactions() YIELD transactionId, elapsedTimeMillis WHERE elapsedTimeMillis > 30000"

   # Solution: Wait for transactions to complete or consider using secondary for backup
   ```

3. **Memory Issues in Backup Process**:
   ```yaml
   # Increase backup sidecar resources
   spec:
     backups:
       sidecar:
         resources:
           requests:
             memory: "1Gi"      # Increase from default 512Mi
           limits:
             memory: "2Gi"      # Increase from default 1Gi
   ```

### Cloud Storage Issues

#### S3 Backup Failures

**Authentication Issues:**
```bash
# Check AWS credentials
kubectl exec production-cluster-server-0 -c backup-sidecar -- aws sts get-caller-identity

# Test S3 access
kubectl exec production-cluster-server-0 -c backup-sidecar -- aws s3 ls s3://your-backup-bucket/
```

**Solutions:**
1. **IAM Role Issues**:
   ```yaml
   # Use IAM roles for service accounts (IRSA)
   spec:
     serviceAccount:
       name: production-cluster-backup
       annotations:
         eks.amazonaws.com/role-arn: "arn:aws:iam::123456789:role/Neo4jBackupRole"
   ```

2. **Bucket Policy Problems**:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "AWS": "arn:aws:iam::123456789:role/Neo4jBackupRole"
         },
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::your-backup-bucket",
           "arn:aws:s3:::your-backup-bucket/*"
         ]
       }
     ]
   }
   ```

#### Google Cloud Storage Issues

**Service Account Problems:**
```bash
# Check GCP credentials
kubectl exec production-cluster-server-0 -c backup-sidecar -- gcloud auth list

# Test GCS access
kubectl exec production-cluster-server-0 -c backup-sidecar -- gsutil ls gs://your-backup-bucket/
```

**Solutions:**
```yaml
# Use Workload Identity
spec:
  serviceAccount:
    name: production-cluster-backup
    annotations:
      iam.gke.io/gcp-service-account: "neo4j-backup@project.iam.gserviceaccount.com"
```

#### Azure Blob Storage Issues

**Authentication Problems:**
```bash
# Check Azure credentials
kubectl exec production-cluster-server-0 -c backup-sidecar -- az account show

# Test storage access
kubectl exec production-cluster-server-0 -c backup-sidecar -- az storage blob list --account-name storageaccount --container-name backups
```

### Scheduled Backup Issues

#### Symptom: Scheduled backups not running

**Diagnosis:**
```bash
# Check CronJob status
kubectl get cronjob
kubectl describe cronjob production-backup-schedule

# Check backup schedule configuration
kubectl get neo4jbackup production-backup -o yaml | grep -A 10 schedule
```

**Common Solutions:**

1. **Invalid Cron Expression**:
   ```yaml
   # Correct cron syntax
   spec:
     schedule: "0 2 * * *"    # Daily at 2 AM
     # NOT: "0 2 * * * *"     # Invalid - too many fields
   ```

2. **Timezone Issues**:
   ```yaml
   spec:
     schedule: "0 2 * * *"
     timezone: "UTC"          # Explicitly set timezone
   ```

3. **Backup Window Conflicts**:
   ```bash
   # Check for overlapping backup jobs
   kubectl get jobs -l app.kubernetes.io/component=backup --sort-by=.metadata.creationTimestamp
   ```

## Common Restore Issues

### Restore Job Failures

#### Symptom: Restore job fails to start

**Diagnosis:**
```bash
# Check restore resource status
kubectl get neo4jrestore
kubectl describe neo4jrestore production-restore

# Check operator logs
kubectl logs -n neo4j-operator-system deployment/neo4j-operator-controller-manager | grep -i restore
```

**Common Solutions:**

1. **Invalid Backup Reference**:
   ```bash
   # Verify backup exists
   kubectl get neo4jbackup production-backup

   # Check backup completion status
   kubectl get neo4jbackup production-backup -o jsonpath='{.status.phase}'
   ```

2. **Target Cluster Issues**:
   ```bash
   # Ensure target cluster is ready
   kubectl get neo4jenterprisecluster target-cluster
   kubectl get pods -l neo4j.com/cluster=target-cluster
   ```

3. **Storage Access Problems**:
   ```bash
   # Test access to backup storage location
   kubectl exec target-cluster-server-0 -c backup-sidecar -- \
     aws s3 ls s3://backup-bucket/path/to/backup/
   ```

#### Symptom: Restore job fails during execution

**Diagnosis:**
```bash
# Check restore job logs
kubectl logs job/production-restore-$(date +%Y%m%d)

# Check target cluster logs during restore
kubectl logs target-cluster-server-0 | grep -i restore
```

**Common Solutions:**

1. **Insufficient Storage Space**:
   ```bash
   # Check available space on target cluster
   kubectl exec target-cluster-server-0 -- df -h /data

   # Solution: Increase PVC size before restore
   ```

2. **Database Already Exists**:
   ```yaml
   # Use force option to overwrite
   spec:
     options:
       force: true
   ```

3. **Version Incompatibility**:
   ```bash
   # Check Neo4j versions
   kubectl exec source-cluster-server-0 -- neo4j version
   kubectl exec target-cluster-server-0 -- neo4j version
   ```

### Point-in-Time Recovery (PITR) Issues

#### Symptom: PITR restore fails with timestamp errors

**Diagnosis:**
```bash
# Check backup logs for transaction timestamps
kubectl logs job/production-backup-latest | grep -i "restore-until"

# Verify PITR capability
kubectl exec production-cluster-server-0 -- neo4j-admin database info system
```

**Solutions:**

1. **Invalid Timestamp Format**:
   ```yaml
   # Correct ISO 8601 format
   spec:
     restoreUntil: "2025-01-15T14:30:00Z"
     # NOT: "2025-01-15 14:30:00"
   ```

2. **Timestamp Outside Backup Range**:
   ```bash
   # Check backup time range
   kubectl logs job/production-backup-20250115 | grep -E "(start|end).*time"
   ```

3. **Neo4j Version Compatibility**:
   ```yaml
   # PITR only available in Neo4j 2025.x
   spec:
     image:
       repository: "neo4j"
       tag: "2025.01.0-enterprise"
   ```

## Backup Sidecar Issues

### Sidecar Container Problems

#### Symptom: Backup sidecar fails to start

**Diagnosis:**
```bash
# Check sidecar status
kubectl get pods -l neo4j.com/cluster=production-cluster -o wide
kubectl describe pod production-cluster-server-0

# Check sidecar logs
kubectl logs production-cluster-server-0 -c backup-sidecar
```

**Common Solutions:**

1. **Resource Constraints**:
   ```yaml
   # Increase sidecar resources
   spec:
     backups:
       sidecar:
         resources:
           requests:
             memory: "512Mi"
             cpu: "200m"
           limits:
             memory: "1Gi"
             cpu: "500m"
   ```

2. **Storage Mount Issues**:
   ```bash
   # Check volume mounts
   kubectl describe pod production-cluster-server-0 | grep -A 10 "Mounts:"
   ```

3. **Permission Problems**:
   ```bash
   # Check file permissions
   kubectl exec production-cluster-server-0 -c backup-sidecar -- ls -la /backup-requests
   kubectl exec production-cluster-server-0 -c backup-sidecar -- id
   ```

### Backup Request Processing Issues

#### Symptom: Backup requests not processed by sidecar

**Diagnosis:**
```bash
# Check backup request queue
kubectl exec production-cluster-server-0 -c backup-sidecar -- ls -la /backup-requests/

# Test manual backup request
kubectl exec production-cluster-server-0 -c backup-sidecar -- sh -c \
  'echo "{\"path\":\"/data/backups/manual-test\",\"type\":\"FULL\"}" > /backup-requests/test.request'
```

**Solutions:**

1. **Request Format Issues**:
   ```json
   // Correct format
   {
     "path": "/data/backups/test",
     "type": "FULL",
     "databases": ["neo4j", "system"]
   }
   ```

2. **Sidecar Communication Problems**:
   ```bash
   # Check shared volume
   kubectl exec production-cluster-server-0 -c neo4j -- ls -la /backup-requests/
   kubectl exec production-cluster-server-0 -c backup-sidecar -- ls -la /backup-requests/
   ```

## Performance Issues

### Slow Backup Performance

**Diagnosis:**
```bash
# Monitor backup progress
kubectl logs job/production-backup-latest -f

# Check resource utilization during backup
kubectl top pod production-cluster-server-0
```

**Optimization Strategies:**

1. **Use Secondary Servers for Backup**:
   ```yaml
   spec:
     backupSource: "secondary"  # Backup from secondary to reduce primary load
   ```

2. **Parallel Backup Processing**:
   ```yaml
   spec:
     backups:
       parallelism: 2           # Multiple backup jobs can run simultaneously
   ```

3. **Storage Performance Tuning**:
   ```yaml
   # Use high-performance storage for backup staging
   spec:
     backups:
       sidecar:
         storage:
           className: "fast-ssd"
           size: "100Gi"
   ```

4. **Network Optimization**:
   ```yaml
   spec:
     config:
       # Increase buffer sizes for backup operations
       dbms.memory.off_heap.max_size: "2g"
       dbms.memory.pagecache.size: "4g"
   ```

### Slow Restore Performance

**Optimization:**

1. **Target Cluster Resources**:
   ```yaml
   spec:
     resources:
       requests:
         memory: "8Gi"
         cpu: "4"
       limits:
         memory: "16Gi"
         cpu: "8"
   ```

2. **Storage Configuration**:
   ```yaml
   spec:
     storage:
       className: "fast-ssd"
       size: "1Ti"
   ```

## Monitoring and Alerting

### Backup Health Monitoring

**Prometheus Metrics:**
```yaml
# Monitor backup success rate
neo4j_backup_success_total
neo4j_backup_failure_total
neo4j_backup_duration_seconds

# Alert rules
groups:
- name: neo4j-backup
  rules:
  - alert: BackupFailure
    expr: increase(neo4j_backup_failure_total[24h]) > 0
    labels:
      severity: critical
    annotations:
      summary: "Neo4j backup failed"
      description: "Backup for cluster {{ $labels.cluster }} failed"
```

**Log Monitoring:**
```bash
# Monitor backup logs
kubectl logs -f job/production-backup-latest | grep -E "(ERROR|WARN|SUCCESS)"

# Set up log alerts
kubectl logs -f -n neo4j-operator-system deployment/neo4j-operator-controller-manager | \
  grep -i "backup.*failed" --line-buffered | \
  while read line; do
    echo "BACKUP ALERT: $line"
    # Send to alerting system
  done
```

### Backup Validation

**Automated Validation Script:**
```bash
#!/bin/bash
# Validate backup completeness

BACKUP_NAME="production-backup"
NAMESPACE="default"

validate_backup() {
  local backup_status=$(kubectl get neo4jbackup $BACKUP_NAME -n $NAMESPACE -o jsonpath='{.status.phase}')

  if [ "$backup_status" != "Succeeded" ]; then
    echo "❌ Backup failed or incomplete: $backup_status"
    return 1
  fi

  # Check backup size
  local backup_size=$(kubectl get neo4jbackup $BACKUP_NAME -n $NAMESPACE -o jsonpath='{.status.backupSize}')
  if [ "$backup_size" -lt 1000000 ]; then  # Less than 1MB
    echo "⚠️  Backup size suspiciously small: $backup_size bytes"
  fi

  echo "✅ Backup validation passed"
  return 0
}

# Run validation
validate_backup
```

## Emergency Recovery Procedures

### Complete Database Recovery

**Scenario:** Primary database corrupted, need complete restore

```bash
# 1. Create new cluster for restoration
kubectl apply -f - <<EOF
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: recovery-cluster
spec:
  topology:
    servers: 3
  # Use same configuration as original cluster
  storage:
    className: "fast-ssd"
    size: "1Ti"
EOF

# 2. Wait for cluster to be ready
kubectl wait --for=condition=Ready neo4jenterprisecluster/recovery-cluster --timeout=600s

# 3. Restore from latest backup
kubectl apply -f - <<EOF
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: emergency-restore
spec:
  targetCluster: recovery-cluster
  source:
    backupName: production-backup-latest
  databaseName: neo4j
  force: true
EOF

# 4. Monitor restore progress
kubectl logs -f job/emergency-restore

# 5. Verify data integrity
kubectl exec recovery-cluster-server-0 -- cypher-shell -u neo4j -p password \
  "MATCH (n) RETURN count(n) as total_nodes"
```

### Point-in-Time Emergency Recovery

```bash
# Restore to specific point before corruption
kubectl apply -f - <<EOF
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: pitr-emergency-restore
spec:
  targetCluster: recovery-cluster
  source:
    backupName: production-backup-latest
  databaseName: neo4j
  options:
    restoreUntil: "2025-01-15T10:30:00Z"  # Before corruption occurred
  force: true
EOF
```

## Best Practices Summary

### Backup Best Practices
- [ ] **Regular Testing**: Test backup and restore procedures regularly
- [ ] **Multiple Storage Locations**: Store backups in multiple locations/regions
- [ ] **Retention Policies**: Implement appropriate retention policies
- [ ] **Monitoring**: Set up comprehensive backup monitoring and alerting
- [ ] **Documentation**: Document recovery procedures and test them
- [ ] **Security**: Encrypt backups and use secure storage access

### Restore Best Practices
- [ ] **Validation**: Always validate restored data integrity
- [ ] **Staging Environment**: Test restores in staging before production
- [ ] **Downtime Planning**: Plan for service interruption during restore
- [ ] **Data Consistency**: Ensure cluster consistency after restore
- [ ] **Application Testing**: Test applications after database restore

### Performance Best Practices
- [ ] **Resource Allocation**: Adequate resources for backup/restore operations
- [ ] **Storage Performance**: Use high-performance storage for operations
- [ ] **Network Optimization**: Optimize network for data transfer
- [ ] **Scheduling**: Schedule backups during low-activity periods
- [ ] **Parallel Operations**: Use parallelism where possible

For additional help, see:
- [Backup & Restore Guide](../guides/backup_restore.md)
- [Performance Tuning](../performance.md)
- [Security Best Practices](../security.md)
- [Split-Brain Recovery](split-brain-recovery.md)
