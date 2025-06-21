# Multi-Tenant Support Guide

This guide provides comprehensive instructions for implementing multi-tenant Neo4j deployments with the Kubernetes Operator.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Basic Configuration](#basic-configuration)
- [Advanced Configuration](#advanced-configuration)
- [Tenant Management](#tenant-management)
- [Resource Isolation](#resource-isolation)
- [Security and Access Control](#security-and-access-control)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

Multi-tenant support enables hosting multiple isolated tenant workloads on a shared Neo4j infrastructure while maintaining security, performance isolation, and cost efficiency.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                Shared Infrastructure                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Tenant A   │  │  Tenant B   │  │     Tenant C        │  │
│  │             │  │             │  │                     │  │
│  │ Database A  │  │ Database B  │  │   Database C        │  │
│  │ Resources   │  │ Resources   │  │   Resources         │  │
│  │ Security    │  │ Security    │  │   Security          │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Neo4j Enterprise Edition 5.0+
- Kubernetes cluster with RBAC enabled
- Network policies support
- Resource quota management
- Storage classes for isolation

## Basic Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-multitenant
  namespace: neo4j-system
spec:
  multiTenant:
    enabled: true
    
    # Tenant isolation strategy
    isolation:
      level: "database"  # database, cluster, or namespace
      
    # Resource allocation
    resources:
      defaultQuota:
        cpu: "2"
        memory: "4Gi"
        storage: "100Gi"
      
      tenantLimits:
        maxCpu: "8"
        maxMemory: "16Gi"
        maxStorage: "1Ti"
        maxDatabases: 10
    
    # Tenant management
    tenantManagement:
      autoProvisioning: true
      defaultRetention: "30d"
      billing:
        enabled: true
        meteringInterval: "1h"
    
    # Security settings
    security:
      networkIsolation: true
      rbacEnabled: true
      auditingEnabled: true
```

## Advanced Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-advanced-multitenant
  namespace: neo4j-system
spec:
  multiTenant:
    enabled: true
    
    # Advanced isolation
    isolation:
      level: "cluster"
      
      # Namespace-based isolation
      namespaceTemplate: "neo4j-tenant-{{tenant-id}}"
      
      # Network isolation
      networkPolicies:
        enabled: true
        allowCrossTenant: false
        allowedPorts: [7474, 7687]
      
      # Storage isolation
      storageClasses:
        default: "ssd-encrypted"
        tenantSpecific: true
        encryption: "at-rest"
    
    # Resource management
    resources:
      # Tier-based resource allocation
      tiers:
        basic:
          cpu: "1"
          memory: "2Gi"
          storage: "50Gi"
          price: "$50/month"
        
        standard:
          cpu: "4"
          memory: "8Gi"
          storage: "200Gi"
          price: "$200/month"
        
        premium:
          cpu: "8"
          memory: "32Gi"
          storage: "1Ti"
          price: "$800/month"
      
      # Auto-scaling per tenant
      autoScaling:
        enabled: true
        minReplicas: 1
        maxReplicas: 5
        targetCPUUtilization: 70
        targetMemoryUtilization: 80
    
    # SLA configuration
    sla:
      availabilityTargets:
        basic: "99.5%"
        standard: "99.9%"
        premium: "99.95%"
      
      performanceTargets:
        responseTime:
          basic: "100ms"
          standard: "50ms"
          premium: "20ms"
    
    # Compliance and governance
    compliance:
      dataResidency:
        enabled: true
        regions: ["us-west-2", "eu-central-1"]
      
      encryption:
        atRest: true
        inTransit: true
        keyRotation: "30d"
      
      auditing:
        enabled: true
        retention: "7y"
        compliance: ["SOC2", "GDPR", "HIPAA"]
```

## Tenant Management

### Tenant Provisioning

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jTenant
metadata:
  name: tenant-acme-corp
  namespace: neo4j-system
spec:
  tenantId: "acme-corp"
  displayName: "ACME Corporation"
  
  # Resource allocation
  resources:
    tier: "standard"
    databases:
      - name: "production"
        size: "large"
      - name: "staging"
        size: "small"
  
  # Access configuration
  access:
    users:
      - username: "admin@acme.com"
        role: "admin"
      - username: "analyst@acme.com"
        role: "reader"
    
    # Network access
    allowedIPs:
      - "10.0.0.0/8"
      - "192.168.1.100/32"
  
  # SLA configuration
  sla:
    tier: "standard"
    backupRetention: "90d"
    supportLevel: "business"
  
  # Billing information
  billing:
    accountId: "acme-billing-001"
    costCenter: "engineering"
    purchaseOrder: "PO-2024-001"
```

### Tenant Management Scripts

```bash
#!/bin/bash
# scripts/manage-tenant.sh

OPERATION=$1
TENANT_ID=$2
NAMESPACE=${3:-neo4j-system}

case $OPERATION in
  "create")
    create_tenant $TENANT_ID $NAMESPACE
    ;;
  "delete")
    delete_tenant $TENANT_ID $NAMESPACE
    ;;
  "scale")
    scale_tenant $TENANT_ID $3 $NAMESPACE
    ;;
  "status")
    tenant_status $TENANT_ID $NAMESPACE
    ;;
  *)
    echo "Usage: $0 {create|delete|scale|status} <tenant-id> [namespace]"
    exit 1
    ;;
esac

create_tenant() {
  local tenant_id=$1
  local namespace=$2
  
  echo "Creating tenant: $tenant_id"
  
  # Create tenant namespace
  kubectl create namespace "neo4j-tenant-$tenant_id"
  
  # Apply resource quotas
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ResourceQuota
metadata:
  name: tenant-quota
  namespace: neo4j-tenant-$tenant_id
spec:
  hard:
    requests.cpu: "4"
    requests.memory: "8Gi"
    persistentvolumeclaims: "10"
    requests.storage: "200Gi"
EOF

  # Create tenant cluster
  cat <<EOF | kubectl apply -f -
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-$tenant_id
  namespace: neo4j-tenant-$tenant_id
spec:
  replicas: 3
  resources:
    requests:
      cpu: "1"
      memory: "2Gi"
    limits:
      cpu: "2"
      memory: "4Gi"
EOF

  echo "✓ Tenant $tenant_id created successfully"
}

delete_tenant() {
  local tenant_id=$1
  local namespace=$2
  
  echo "Deleting tenant: $tenant_id"
  
  # Delete tenant cluster
  kubectl delete neo4jenterprisecluster neo4j-$tenant_id -n neo4j-tenant-$tenant_id
  
  # Delete namespace (this removes all resources)
  kubectl delete namespace neo4j-tenant-$tenant_id
  
  echo "✓ Tenant $tenant_id deleted successfully"
}
```

## Resource Isolation

### Namespace Isolation

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: neo4j-tenant-{{tenant-id}}
  labels:
    tenant-id: "{{tenant-id}}"
    isolation-level: "namespace"
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: tenant-isolation
  namespace: neo4j-tenant-{{tenant-id}}
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: neo4j-tenant-{{tenant-id}}
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: neo4j-tenant-{{tenant-id}}
```

### Resource Quotas

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: tenant-compute-quota
  namespace: neo4j-tenant-{{tenant-id}}
spec:
  hard:
    requests.cpu: "4"
    requests.memory: "8Gi"
    limits.cpu: "8"
    limits.memory: "16Gi"
    pods: "10"
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: tenant-storage-quota
  namespace: neo4j-tenant-{{tenant-id}}
spec:
  hard:
    persistentvolumeclaims: "10"
    requests.storage: "200Gi"
```

## Security and Access Control

### RBAC Configuration

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: tenant-admin
  namespace: neo4j-tenant-{{tenant-id}}
rules:
- apiGroups: [""]
  resources: ["pods", "services", "persistentvolumeclaims"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["neo4j.neo4j.com"]
  resources: ["neo4jenterpriseclusters", "neo4jbackups", "neo4jrestores"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: tenant-admin-binding
  namespace: neo4j-tenant-{{tenant-id}}
subjects:
- kind: User
  name: "admin@{{tenant-domain}}"
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: Role
  name: tenant-admin
  apiGroup: rbac.authorization.k8s.io
```

### Database-Level Security

```bash
#!/bin/bash
# scripts/setup-tenant-security.sh

TENANT_ID=$1
NAMESPACE="neo4j-tenant-$TENANT_ID"

# Create tenant-specific users and roles
kubectl exec -it neo4j-$TENANT_ID-0 -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "
-- Create tenant database
CREATE DATABASE ${TENANT_ID}_production;
CREATE DATABASE ${TENANT_ID}_staging;

-- Create tenant roles
CREATE ROLE ${TENANT_ID}_admin;
CREATE ROLE ${TENANT_ID}_reader;
CREATE ROLE ${TENANT_ID}_writer;

-- Grant database access
GRANT ACCESS ON DATABASE ${TENANT_ID}_production TO ${TENANT_ID}_admin;
GRANT ACCESS ON DATABASE ${TENANT_ID}_staging TO ${TENANT_ID}_admin;

-- Create tenant users
CREATE USER ${TENANT_ID}_admin_user SET PASSWORD '$ADMIN_PASSWORD';
CREATE USER ${TENANT_ID}_app_user SET PASSWORD '$APP_PASSWORD';

-- Assign roles
GRANT ROLE ${TENANT_ID}_admin TO ${TENANT_ID}_admin_user;
GRANT ROLE ${TENANT_ID}_writer TO ${TENANT_ID}_app_user;
"
```

## Monitoring and Metrics

### Tenant-Specific Metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: multitenant-metrics
  namespace: neo4j-system
spec:
  groups:
  - name: neo4j.multitenant
    rules:
    # Resource utilization by tenant
    - record: neo4j:tenant_cpu_usage
      expr: sum by(tenant_id) (rate(container_cpu_usage_seconds_total{namespace=~"neo4j-tenant-.*"}[5m]))
    
    - record: neo4j:tenant_memory_usage
      expr: sum by(tenant_id) (container_memory_usage_bytes{namespace=~"neo4j-tenant-.*"})
    
    - record: neo4j:tenant_storage_usage
      expr: sum by(tenant_id) (kubelet_volume_stats_used_bytes{namespace=~"neo4j-tenant-.*"})
    
    # Query metrics by tenant
    - record: neo4j:tenant_query_rate
      expr: sum by(tenant_id) (rate(neo4j_cypher_queries_total[5m]))
    
    # SLA compliance metrics
    - record: neo4j:tenant_availability
      expr: avg_over_time(up{namespace=~"neo4j-tenant-.*"}[24h]) * 100
    
    # Billing metrics
    - record: neo4j:tenant_cost_per_hour
      expr: (neo4j:tenant_cpu_usage * 0.05) + (neo4j:tenant_memory_usage / 1024 / 1024 / 1024 * 0.01)
```

### Tenant Dashboard

```json
{
  "dashboard": {
    "title": "Multi-Tenant Neo4j Overview",
    "panels": [
      {
        "title": "Tenant Resource Usage",
        "type": "table",
        "targets": [
          {
            "expr": "neo4j:tenant_cpu_usage",
            "format": "table"
          }
        ]
      },
      {
        "title": "SLA Compliance",
        "type": "stat",
        "targets": [
          {
            "expr": "neo4j:tenant_availability",
            "legendFormat": "{{tenant_id}} Availability"
          }
        ]
      },
      {
        "title": "Cost by Tenant",
        "type": "piechart",
        "targets": [
          {
            "expr": "sum by(tenant_id) (neo4j:tenant_cost_per_hour)",
            "legendFormat": "{{tenant_id}}"
          }
        ]
      }
    ]
  }
}
```

## Troubleshooting

### Common Issues

#### Tenant Isolation Problems
```bash
# Check network policies
kubectl get networkpolicies -A | grep tenant

# Verify resource quotas
kubectl describe quota -n neo4j-tenant-example

# Check RBAC permissions
kubectl auth can-i --list --as=user@tenant.com -n neo4j-tenant-example
```

#### Resource Contention
```bash
# Check resource usage by tenant
kubectl top pods -A | grep neo4j-tenant

# Verify resource limits
kubectl describe limits -A | grep neo4j-tenant
```

## Best Practices

### Tenant Design
- Use appropriate isolation levels based on security requirements
- Implement proper resource quotas and limits
- Design for horizontal scaling

### Security
- Implement defense in depth
- Regular security audits
- Principle of least privilege

### Cost Management
- Implement chargeback/showback
- Monitor resource utilization
- Use appropriate instance sizing

### Operations
- Automate tenant provisioning
- Implement proper monitoring
- Document tenant procedures

## Related Documentation

- [Auto-Scaling Guide](./auto-scaling-guide.md)
- [Security Best Practices](../security-guide.md)
- [Query Performance Monitoring](./query-monitoring-guide.md)

---

*For additional support, please refer to the [Neo4j Operator Documentation](../README.md).* 