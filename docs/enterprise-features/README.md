# Neo4j Kubernetes Operator: Enterprise Features

## Overview

The Neo4j Kubernetes Operator provides a comprehensive suite of enterprise-grade features designed for production workloads. These features enable organizations to deploy, scale, and manage Neo4j clusters with confidence, ensuring high availability, performance, and security.

## 🚀 **Available Enterprise Features**

### 1. [Auto-Scaling](./auto-scaling-guide.md)
Automatically scale read replicas based on CPU, memory, query latency, and connection metrics.

**Key Benefits:**
- HPA integration with custom metrics
- Configurable scaling policies
- Prometheus integration
- Resource optimization

```yaml
autoScaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: cpu
    target: "70"
```

### 2. [Blue-Green Deployments](./blue-green-deployment-guide.md)
Zero-downtime deployments with automated traffic switching and rollback capabilities.

**Key Benefits:**
- Zero-downtime updates
- Automated testing and validation
- Traffic splitting and canary deployments
- Automatic rollback on failure

```yaml
blueGreen:
  enabled: true
  traffic:
    mode: automatic
    canaryPercentage: 20
```

### 3. [Plugin Management](./plugin-management-guide.md)
Dynamic installation and management of Neo4j plugins including APOC, GDS, and custom plugins.

**Key Benefits:**
- Official and community plugin support
- Security sandboxing
- Dependency resolution
- Hot deployment without restarts

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jPlugin
metadata:
  name: apoc-plugin
spec:
  clusterRef: my-cluster
  name: apoc
  version: "5.26.0"
```

### 4. [Disaster Recovery](./disaster-recovery-guide.md)
Cross-region replication with automatic failover and point-in-time recovery.

**Key Benefits:**
- Cross-region networking
- Automatic failover
- Transaction log shipping
- RTO/RPO guarantees

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDisasterRecovery
metadata:
  name: cross-region-dr
spec:
  primaryClusterRef: primary-cluster
  secondaryClusterRef: secondary-cluster
```

### 5. [Query Performance Monitoring](./query-monitoring-guide.md)
Advanced query monitoring with slow query detection and performance insights.

**Key Benefits:**
- Slow query detection
- Query sampling
- Prometheus metrics export
- Index recommendations

```yaml
queryMonitoring:
  enabled: true
  slowQueryThreshold: "2s"
  explainPlan: true
  indexRecommendations: true
```

### 6. [Point-in-Time Recovery](./point-in-time-recovery-guide.md)
Transaction log shipping with the ability to restore to any point in time.

**Key Benefits:**
- Continuous transaction log backup
- Cloud storage integration
- Precise recovery points
- Automated retention policies

```yaml
pointInTimeRecovery:
  enabled: true
  transactionLogRetention: "7d"
  logShipping:
    enabled: true
    destination:
      type: s3
      bucket: neo4j-pitr-logs
```

### 7. [Multi-Tenant Support](./multi-tenant-guide.md)
Database-level or namespace-level isolation with resource quotas and tenant management.

**Key Benefits:**
- Tenant isolation
- Resource quotas
- Billing separation
- Security boundaries

```yaml
multiTenant:
  enabled: true
  isolation: database
  tenants:
  - name: tenant-a
    databases: ["tenant_a_db"]
    resources:
      cpu: "1000m"
      memory: "2Gi"
```

## 🏗 **Architecture Overview**

```
┌─────────────────────────────────────────────────────────────┐
│                     Neo4j Operator                         │
├─────────────────────────────────────────────────────────────┤
│  Auto-Scaling  │  Blue-Green  │  Plugin Mgmt │ Multi-Tenant │
│  Controller    │  Controller  │  Controller  │ Manager      │
├─────────────────────────────────────────────────────────────┤
│  Query Monitor │  DR Manager  │  PITR Manager│ Cache Manager│
├─────────────────────────────────────────────────────────────┤
│              Core Neo4j Enterprise Cluster                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │
│  │ Primary 1   │ │ Primary 2   │ │ Primary 3   │          │
│  └─────────────┘ └─────────────┘ └─────────────┘          │
│  ┌─────────────┐ ┌─────────────┐                          │
│  │Secondary 1  │ │Secondary 2  │ (Auto-scaled)            │
│  └─────────────┘ └─────────────┘                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Monitoring & Storage                        │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│ │ Prometheus  │ │ Cloud Store │ │ Grafana     │           │
│ │ Metrics     │ │ (S3/GCS)    │ │ Dashboards  │           │
│ └─────────────┘ └─────────────┘ └─────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## 🚦 **Quick Start**

### Prerequisites

Before using enterprise features, ensure you have:

- Kubernetes cluster (v1.21+)
- Neo4j Enterprise license
- Sufficient cluster resources
- cert-manager (for TLS)
- Prometheus (for monitoring)

### Basic Enterprise Cluster

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: enterprise-cluster
  namespace: default
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  
  topology:
    primaries: 3
    secondaries: 2
  
  # Enable enterprise features
  autoScaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    metrics:
    - type: cpu
      target: "70"
  
  blueGreen:
    enabled: true
  
  queryMonitoring:
    enabled: true
    slowQueryThreshold: "5s"
  
  pointInTimeRecovery:
    enabled: true
    transactionLogRetention: "7d"
  
  multiTenant:
    enabled: true
    isolation: database
  
  auth:
    adminSecret: neo4j-admin-secret
```

### Deploy the Cluster

```bash
# Apply the configuration
kubectl apply -f enterprise-cluster.yaml

# Check deployment status
kubectl get neo4jenterprisecluster enterprise-cluster -w

# View enterprise features status
kubectl describe neo4jenterprisecluster enterprise-cluster
```

## 📊 **Monitoring Dashboard**

### Grafana Dashboard Import

Use our pre-built Grafana dashboard for comprehensive monitoring:

```bash
# Import enterprise features dashboard
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: neo4j-enterprise-dashboard
  namespace: monitoring
data:
  dashboard.json: |
    {
      "dashboard": {
        "title": "Neo4j Enterprise Features",
        "panels": [
          // Auto-scaling metrics
          // Blue-green deployment status
          // Plugin health
          // DR replication lag
          // Query performance
        ]
      }
    }
EOF
```

### Key Metrics to Monitor

1. **Auto-Scaling**: Replica count, CPU/memory usage, scaling events
2. **Blue-Green**: Deployment status, traffic split, validation results
3. **Plugins**: Installation status, procedure calls, errors
4. **Disaster Recovery**: Replication lag, failover events, health checks
5. **Query Performance**: Slow queries, latency percentiles, throughput
6. **PITR**: Log shipping status, backup success rate, retention
7. **Multi-Tenant**: Per-tenant resource usage, quota enforcement

## 🔧 **Configuration Examples**

### Production Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: production-cluster
  namespace: production
spec:
  edition: enterprise
  image:
    repo: neo4j
    tag: 5.26-enterprise
  
  # High availability topology
  topology:
    primaries: 3
    secondaries: 3
  
  # Production-grade auto-scaling
  autoScaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    metrics:
    - type: cpu
      target: "65"
    - type: memory
      target: "75"
    - type: query_latency
      target: "1s"
    behavior:
      scaleUp:
        stabilizationWindowSeconds: 120
      scaleDown:
        stabilizationWindowSeconds: 300
  
  # Zero-downtime deployments
  blueGreen:
    enabled: true
    traffic:
      mode: automatic
      canaryPercentage: 10
      waitDuration: "10m"
    validation:
      healthChecks:
      - name: connectivity
        cypherQuery: "RETURN 1"
        timeout: "30s"
  
  # Comprehensive monitoring
  queryMonitoring:
    enabled: true
    slowQueryThreshold: "1s"
    explainPlan: true
    indexRecommendations: true
    sampling:
      rate: "0.1"
    metricsExport:
      prometheus: true
      interval: "30s"
  
  # Disaster recovery
  pointInTimeRecovery:
    enabled: true
    transactionLogRetention: "30d"
    logShipping:
      enabled: true
      interval: "1m"
      destination:
        type: s3
        bucket: production-neo4j-logs
        encryption:
          enabled: true
  
  # Multi-tenancy
  multiTenant:
    enabled: true
    isolation: database
    resourceQuotas:
      defaultCPUQuota: "1000m"
      defaultMemoryQuota: "2Gi"
      maxTenantsPerCluster: 50
  
  # Resource allocation
  resources:
    requests:
      cpu: "2000m"
      memory: "8Gi"
    limits:
      cpu: "8000m"
      memory: "32Gi"
  
  # Storage
  storage:
    className: fast-ssd
    size: 1000Gi
  
  # Security
  auth:
    adminSecret: neo4j-admin-secret
  
  tls:
    enabled: true
    issuerRef:
      name: letsencrypt-prod
```

## 🧪 **Testing & Validation**

### Integration Tests

```bash
# Run enterprise feature tests
./scripts/run-tests.sh enterprise

# Disaster recovery drills
./scripts/run-tests.sh e2e --cleanup
```

### Health Checks

```bash
# Check all enterprise features
kubectl get neo4jenterprisecluster -o yaml | grep -A 10 status

# Verify auto-scaling
kubectl get hpa

# Check plugin status
kubectl get neo4jplugin

# Disaster recovery status
kubectl get neo4jdisasterrecovery
```

## 🚨 **Troubleshooting**

### Common Issues

#### Auto-Scaling Not Working
```bash
# Check HPA status
kubectl describe hpa <cluster-name>-secondary

# Verify metrics server
kubectl get deployment metrics-server -n kube-system
```

#### Plugin Installation Failed
```bash
# Check plugin status
kubectl describe neo4jplugin <plugin-name>

# View operator logs
kubectl logs -f deployment/neo4j-operator-controller-manager -n neo4j-operator-system
```

#### Blue-Green Deployment Stuck
```bash
# Check deployment status
kubectl get events --field-selector involvedObject.kind=Neo4jEnterpriseCluster

# View validation logs
kubectl logs -l app=<cluster-name>-validation
```

### Support Resources

- [Troubleshooting Guide](../troubleshooting.md)
- [Performance Tuning](./performance-tuning-guide.md)
- [Security Best Practices](../security-guide.md)
- [Backup and Restore](../backup-restore-guide.md)

## 📚 **Documentation Index**

| Feature | Guide | Configuration | Examples |
|---------|-------|---------------|----------|
| Auto-Scaling | [Guide](./auto-scaling-guide.md) | HPA, Metrics | Production scaling |
| Blue-Green | [Guide](./blue-green-deployment-guide.md) | Traffic, Validation | Zero-downtime updates |
| Plugin Management | [Guide](./plugin-management-guide.md) | APOC, GDS, Custom | Plugin ecosystem |
| Disaster Recovery | [Guide](./disaster-recovery-guide.md) | Cross-region, Failover | Business continuity |
| Query Monitoring | [Guide](./query-monitoring-guide.md) | Performance, Alerts | Query optimization |
| Point-in-Time Recovery | [Guide](./point-in-time-recovery-guide.md) | Log shipping, Restore | Data protection |
| Multi-Tenant | [Guide](./multi-tenant-guide.md) | Isolation, Quotas | SaaS deployments |

## 🔮 **Roadmap**

### Upcoming Features

- **Advanced Security**: RBAC integration, audit logging
- **Cost Optimization**: Intelligent resource scheduling
- **AI/ML Integration**: Automated performance tuning
- **Global Distribution**: Multi-cloud deployments
- **Compliance**: SOC2, GDPR, HIPAA certifications

### Feature Requests

We welcome feedback and feature requests! Please:

1. Check existing [issues](https://github.com/neo4j-labs/neo4j-operator/issues)
2. Submit new [feature requests](https://github.com/neo4j-labs/neo4j-operator/issues/new)
3. Join our [community discussions](https://community.neo4j.com)

## 🤝 **Contributing**

Interested in contributing to the enterprise features?

1. Read the [contributing guide](../../CONTRIBUTING.md)
2. Check the [development setup](../../README-DEV.md)
3. Review [architecture docs](../development/architecture.md)
4. Submit a [pull request](https://github.com/neo4j-labs/neo4j-operator/pulls)

## 📄 **License**

Enterprise features require a valid Neo4j Enterprise license. Contact [Neo4j Sales](https://neo4j.com/contact-us/) for licensing information.

---

**Ready to get started?** Choose your enterprise feature and dive into the detailed guides above! 🚀 