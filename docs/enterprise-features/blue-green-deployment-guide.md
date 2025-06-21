# Blue-Green Deployment Guide

This guide provides comprehensive instructions for implementing zero-downtime blue-green deployments with the Neo4j Kubernetes Operator.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Basic Configuration](#basic-configuration)
- [Advanced Configuration](#advanced-configuration)
- [Deployment Process](#deployment-process)
- [Traffic Management](#traffic-management)
- [Validation and Testing](#validation-and-testing)
- [Rollback Procedures](#rollback-procedures)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Integration Examples](#integration-examples)

## Overview

Blue-green deployment is a technique that reduces downtime and risk by running two identical production environments called Blue and Green. At any time, only one environment is live, serving all production traffic.

### Key Benefits

- **Zero-Downtime Updates**: Seamless switching between environments
- **Risk Mitigation**: Easy rollback if issues are detected
- **Testing in Production**: Validate changes in production-like environment
- **Performance Validation**: Compare performance between versions

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer                            │
├─────────────────────────────────────────────────────────────┤
│              Traffic Switching Logic                        │
└─────────────┬─────────────────────────┬─────────────────────┘
              │                         │
    ┌─────────▼────────┐    ┌──────────▼─────────┐
    │   Blue Environment│    │  Green Environment │
    │                  │    │                    │
    │  Neo4j v5.12     │    │   Neo4j v5.13     │
    │  (Production)    │    │   (Staging)        │
    │                  │    │                    │
    └──────────────────┘    └────────────────────┘
```

## Prerequisites

### Infrastructure Requirements

- Kubernetes cluster with sufficient resources for two environments
- Load balancer or ingress controller supporting traffic splitting
- Persistent storage that can be shared or migrated between environments
- Network policies configured for environment isolation

### Dependencies

```yaml
# Required operators and tools
dependencies:
  - name: nginx-ingress
    version: ">=4.0"
  - name: cert-manager
    version: ">=1.8"
  - name: prometheus-operator
    version: ">=0.60"
```

### Permissions

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: neo4j-blue-green-manager
rules:
- apiGroups: [""]
  resources: ["services", "endpoints"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["networking.k8s.io"]
  resources: ["ingresses"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
```

## Basic Configuration

### Simple Blue-Green Setup

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-production
  namespace: neo4j-system
spec:
  # Blue-Green deployment configuration
  blueGreenDeployment:
    enabled: true
    strategy: "manual"  # or "automatic"
    
    # Environment specifications
    blue:
      replicas: 3
      image: "neo4j:5.12-enterprise"
      resources:
        requests:
          memory: "4Gi"
          cpu: "2"
        limits:
          memory: "8Gi"
          cpu: "4"
      
    green:
      replicas: 3
      image: "neo4j:5.13-enterprise"
      resources:
        requests:
          memory: "4Gi"
          cpu: "2"
        limits:
          memory: "8Gi"
          cpu: "4"
    
    # Traffic configuration
    traffic:
      initialTarget: "blue"
      switchMode: "gradual"  # or "immediate"
      gradualSteps:
        - percentage: 10
          duration: "5m"
        - percentage: 50
          duration: "10m"
        - percentage: 100
          duration: "0"
    
    # Validation configuration
    validation:
      enabled: true
      healthChecks:
        - type: "connectivity"
          timeout: "30s"
        - type: "cluster-status"
          timeout: "60s"
        - type: "custom-query"
          query: "MATCH (n) RETURN count(n) LIMIT 1"
          timeout: "30s"
      
      performanceTests:
        enabled: true
        duration: "5m"
        queries:
          - "MATCH (n:User) RETURN count(n)"
          - "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN count(*)"
        thresholds:
          responseTime: "100ms"
          errorRate: "1%"
```

### Traffic Management Configuration

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: neo4j-blue-green-ingress
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "0"
    nginx.ingress.kubernetes.io/upstream-hash-by: "$arg_session_id"
spec:
  ingressClassName: nginx
  rules:
  - host: neo4j.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: neo4j-blue-service
            port:
              number: 7474
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: neo4j-green-ingress
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "100"
spec:
  ingressClassName: nginx
  rules:
  - host: neo4j.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: neo4j-green-service
            port:
              number: 7474
```

## Advanced Configuration

### Automated Blue-Green with GitOps

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-automated-bg
  namespace: neo4j-system
spec:
  blueGreenDeployment:
    enabled: true
    strategy: "gitops"
    
    # GitOps configuration
    gitops:
      repository: "https://github.com/company/neo4j-config"
      branch: "main"
      path: "clusters/production"
      syncInterval: "5m"
      
    # Automated validation pipeline
    automation:
      enabled: true
      triggers:
        - type: "git-commit"
        - type: "schedule"
          cron: "0 2 * * 0"  # Weekly Sunday 2 AM
      
      pipeline:
        stages:
          - name: "deploy-green"
            tasks:
              - type: "apply-manifest"
              - type: "wait-ready"
                timeout: "10m"
          
          - name: "validation"
            tasks:
              - type: "health-check"
              - type: "performance-test"
              - type: "integration-test"
                testSuite: "regression"
          
          - name: "traffic-switch"
            tasks:
              - type: "gradual-switch"
                steps:
                  - traffic: 5
                    monitor: "2m"
                  - traffic: 25
                    monitor: "5m"
                  - traffic: 100
                    monitor: "1m"
          
          - name: "cleanup"
            tasks:
              - type: "terminate-blue"
                delay: "30m"
    
    # Advanced monitoring
    monitoring:
      metrics:
        - name: "deployment_success_rate"
          type: "counter"
        - name: "switch_duration"
          type: "histogram"
        - name: "rollback_count"
          type: "counter"
      
      alerts:
        - name: "high-error-rate"
          condition: "error_rate > 0.05"
          action: "rollback"
        - name: "performance-degradation"
          condition: "response_time_p95 > 500ms"
          action: "hold"
```

### Multi-Region Blue-Green

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-multi-region-bg
  namespace: neo4j-system
spec:
  blueGreenDeployment:
    enabled: true
    strategy: "multi-region"
    
    regions:
      - name: "us-west-2"
        blue:
          replicas: 3
          nodeSelector:
            topology.kubernetes.io/region: "us-west-2"
        green:
          replicas: 3
          nodeSelector:
            topology.kubernetes.io/region: "us-west-2"
      
      - name: "eu-central-1"
        blue:
          replicas: 3
          nodeSelector:
            topology.kubernetes.io/region: "eu-central-1"
        green:
          replicas: 3
          nodeSelector:
            topology.kubernetes.io/region: "eu-central-1"
    
    # Cross-region coordination
    coordination:
      mode: "sequential"  # or "parallel"
      sequence:
        - region: "us-west-2"
          traffic: 0.3
        - region: "eu-central-1"
          traffic: 0.7
      
      rollback:
        automatic: true
        threshold: "any_region_failure"
```

## Deployment Process

### Manual Deployment Steps

1. **Prepare Green Environment**
```bash
# Update cluster specification
kubectl apply -f cluster-with-green-update.yaml

# Wait for green environment to be ready
kubectl wait --for=condition=Ready pod -l app=neo4j-green -n neo4j-system --timeout=600s
```

2. **Validate Green Environment**
```bash
# Run health checks
kubectl exec -it neo4j-green-0 -n neo4j-system -- cypher-shell -u neo4j -p password "CALL dbms.cluster.overview()"

# Performance validation
kubectl exec -it neo4j-green-0 -n neo4j-system -- cypher-shell -u neo4j -p password "CALL db.stats.retrieve('GRAPH COUNTS')"
```

3. **Switch Traffic Gradually**
```bash
# Start with 10% traffic to green
kubectl patch ingress neo4j-green-ingress -n neo4j-system -p '{"metadata":{"annotations":{"nginx.ingress.kubernetes.io/canary-weight":"10"}}}'

# Monitor for 5 minutes, then increase to 50%
kubectl patch ingress neo4j-green-ingress -n neo4j-system -p '{"metadata":{"annotations":{"nginx.ingress.kubernetes.io/canary-weight":"50"}}}'

# Finally switch 100% traffic
kubectl patch ingress neo4j-green-ingress -n neo4j-system -p '{"metadata":{"annotations":{"nginx.ingress.kubernetes.io/canary-weight":"100"}}}'
```

### Automated Deployment with CI/CD

```yaml
# .github/workflows/blue-green-deploy.yml
name: Blue-Green Deployment
on:
  push:
    branches: [main]
    paths: ['k8s/neo4j/**']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup kubectl
      uses: azure/setup-kubectl@v3
      with:
        version: 'v1.24.0'
    
    - name: Deploy to Green
      run: |
        # Update green environment
        envsubst < k8s/neo4j/cluster-template.yaml | kubectl apply -f -
        
        # Wait for deployment
        kubectl rollout status statefulset/neo4j-green -n neo4j-system --timeout=600s
    
    - name: Run Validation Tests
      run: |
        # Health checks
        ./scripts/health-check.sh green
        
        # Performance tests
        ./scripts/performance-test.sh green
        
        # Integration tests
        ./scripts/integration-test.sh green
    
    - name: Switch Traffic
      run: |
        # Gradual traffic switch
        ./scripts/traffic-switch.sh --target=green --strategy=gradual
    
    - name: Monitor and Rollback if Needed
      run: |
        # Monitor for 10 minutes
        if ! ./scripts/monitor-health.sh --duration=10m --target=green; then
          echo "Health check failed, rolling back"
          ./scripts/traffic-switch.sh --target=blue --strategy=immediate
          exit 1
        fi
    
    - name: Cleanup Blue Environment
      run: |
        # Terminate old blue environment
        kubectl delete statefulset neo4j-blue -n neo4j-system
        kubectl delete pvc -l app=neo4j-blue -n neo4j-system
```

## Traffic Management

### Load Balancer Configuration

```yaml
apiVersion: v1
kind: Service
metadata:
  name: neo4j-blue-green-lb
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
    service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: "true"
spec:
  type: LoadBalancer
  selector:
    app: neo4j-active
  ports:
  - name: bolt
    port: 7687
    targetPort: 7687
  - name: http
    port: 7474
    targetPort: 7474
  - name: https
    port: 7473
    targetPort: 7473
```

### Istio Traffic Management

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: neo4j-blue-green
spec:
  hosts:
  - neo4j.example.com
  http:
  - match:
    - headers:
        canary:
          exact: "true"
    route:
    - destination:
        host: neo4j-green-service
        port:
          number: 7474
  - route:
    - destination:
        host: neo4j-blue-service
        port:
          number: 7474
      weight: 90
    - destination:
        host: neo4j-green-service
        port:
          number: 7474
      weight: 10
---
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: neo4j-blue-green-destinations
spec:
  host: neo4j-service
  subsets:
  - name: blue
    labels:
      version: blue
  - name: green
    labels:
      version: green
```

## Validation and Testing

### Health Check Scripts

```bash
#!/bin/bash
# scripts/health-check.sh

ENVIRONMENT=$1
NAMESPACE=${2:-neo4j-system}

echo "Running health checks for $ENVIRONMENT environment..."

# Check pod readiness
if ! kubectl get pods -l app=neo4j-$ENVIRONMENT -n $NAMESPACE -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | grep -v True; then
    echo "✓ All pods are ready"
else
    echo "✗ Some pods are not ready"
    exit 1
fi

# Check Neo4j cluster status
NEO4J_POD=$(kubectl get pods -l app=neo4j-$ENVIRONMENT -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
CLUSTER_STATUS=$(kubectl exec $NEO4J_POD -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "CALL dbms.cluster.overview() YIELD role, addresses, groups RETURN role, addresses[0] as address" --format plain)

if echo "$CLUSTER_STATUS" | grep -q "LEADER"; then
    echo "✓ Cluster has a leader"
else
    echo "✗ No cluster leader found"
    exit 1
fi

# Check database connectivity
if kubectl exec $NEO4J_POD -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "RETURN 1" >/dev/null 2>&1; then
    echo "✓ Database connectivity verified"
else
    echo "✗ Database connectivity failed"
    exit 1
fi

echo "✓ All health checks passed for $ENVIRONMENT environment"
```

### Performance Test Scripts

```bash
#!/bin/bash
# scripts/performance-test.sh

ENVIRONMENT=$1
NAMESPACE=${2:-neo4j-system}
DURATION=${3:-300}  # 5 minutes

echo "Running performance tests for $ENVIRONMENT environment..."

NEO4J_POD=$(kubectl get pods -l app=neo4j-$ENVIRONMENT -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')

# Create test data if needed
kubectl exec $NEO4J_POD -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "
MERGE (u:User {id: 1, name: 'TestUser'})
MERGE (p:Product {id: 1, name: 'TestProduct'})
MERGE (u)-[:PURCHASED]->(p)
"

# Performance test queries
QUERIES=(
    "MATCH (n:User) RETURN count(n)"
    "MATCH (u:User)-[r:PURCHASED]->(p:Product) RETURN count(r)"
    "MATCH (n) RETURN count(n) LIMIT 1000"
)

START_TIME=$(date +%s)
TOTAL_QUERIES=0
TOTAL_TIME=0
ERRORS=0

while [ $(($(date +%s) - START_TIME)) -lt $DURATION ]; do
    for QUERY in "${QUERIES[@]}"; do
        QUERY_START=$(date +%s%N)
        if kubectl exec $NEO4J_POD -n $NAMESPACE -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "$QUERY" >/dev/null 2>&1; then
            QUERY_END=$(date +%s%N)
            QUERY_TIME=$((($QUERY_END - $QUERY_START) / 1000000))  # Convert to milliseconds
            TOTAL_TIME=$((TOTAL_TIME + QUERY_TIME))
            TOTAL_QUERIES=$((TOTAL_QUERIES + 1))
        else
            ERRORS=$((ERRORS + 1))
        fi
    done
    sleep 1
done

# Calculate metrics
AVG_RESPONSE_TIME=$((TOTAL_TIME / TOTAL_QUERIES))
ERROR_RATE=$(echo "scale=4; $ERRORS / ($TOTAL_QUERIES + $ERRORS) * 100" | bc)
QPS=$(echo "scale=2; $TOTAL_QUERIES / $DURATION" | bc)

echo "Performance Test Results for $ENVIRONMENT:"
echo "  Total Queries: $TOTAL_QUERIES"
echo "  Average Response Time: ${AVG_RESPONSE_TIME}ms"
echo "  Error Rate: ${ERROR_RATE}%"
echo "  Queries Per Second: $QPS"

# Validate against thresholds
if [ $AVG_RESPONSE_TIME -gt 100 ]; then
    echo "✗ Average response time exceeds threshold (100ms)"
    exit 1
fi

if [ $(echo "$ERROR_RATE > 1" | bc) -eq 1 ]; then
    echo "✗ Error rate exceeds threshold (1%)"
    exit 1
fi

echo "✓ Performance tests passed for $ENVIRONMENT environment"
```

## Rollback Procedures

### Immediate Rollback

```bash
#!/bin/bash
# scripts/rollback.sh

NAMESPACE=${1:-neo4j-system}

echo "Initiating immediate rollback..."

# Switch traffic back to blue immediately
kubectl patch ingress neo4j-green-ingress -n $NAMESPACE -p '{"metadata":{"annotations":{"nginx.ingress.kubernetes.io/canary-weight":"0"}}}'

# Verify traffic switch
sleep 10
if kubectl get ingress neo4j-green-ingress -n $NAMESPACE -o jsonpath='{.metadata.annotations.nginx\.ingress\.kubernetes\.io/canary-weight}' | grep -q "0"; then
    echo "✓ Traffic successfully switched back to blue environment"
else
    echo "✗ Failed to switch traffic back"
    exit 1
fi

# Update cluster status
kubectl patch neo4jenterprisecluster neo4j-production -n $NAMESPACE --type='merge' -p='{"spec":{"blueGreenDeployment":{"activeEnvironment":"blue"}}}'

echo "✓ Rollback completed successfully"
```

### Gradual Rollback

```bash
#!/bin/bash
# scripts/gradual-rollback.sh

NAMESPACE=${1:-neo4j-system}

echo "Initiating gradual rollback..."

# Gradual traffic reduction from green to blue
WEIGHTS=(90 50 10 0)
for WEIGHT in "${WEIGHTS[@]}"; do
    echo "Setting green traffic weight to $WEIGHT%..."
    kubectl patch ingress neo4j-green-ingress -n $NAMESPACE -p "{\"metadata\":{\"annotations\":{\"nginx.ingress.kubernetes.io/canary-weight\":\"$WEIGHT\"}}}"
    
    # Monitor for 2 minutes
    sleep 120
    
    # Check health
    if ! ./scripts/health-check.sh blue; then
        echo "✗ Blue environment health check failed during rollback"
        exit 1
    fi
done

echo "✓ Gradual rollback completed successfully"
```

## Monitoring and Metrics

### Prometheus Metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: neo4j-blue-green-metrics
spec:
  selector:
    matchLabels:
      app: neo4j-blue-green
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: neo4j-blue-green-alerts
spec:
  groups:
  - name: neo4j-blue-green
    rules:
    - alert: BlueGreenDeploymentFailed
      expr: neo4j_blue_green_deployment_status != 1
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "Blue-green deployment failed"
        description: "Blue-green deployment for {{ $labels.cluster }} has failed"
    
    - alert: HighErrorRateDuringSwitch
      expr: rate(neo4j_requests_total{status!~"2.."}[5m]) / rate(neo4j_requests_total[5m]) > 0.05
      for: 2m
      labels:
        severity: warning
      annotations:
        summary: "High error rate during traffic switch"
        description: "Error rate is {{ $value | humanizePercentage }} during traffic switch"
    
    - alert: PerformanceDegradation
      expr: histogram_quantile(0.95, rate(neo4j_request_duration_seconds_bucket[5m])) > 0.5
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "Performance degradation detected"
        description: "95th percentile response time is {{ $value }}s"
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Neo4j Blue-Green Deployment",
    "panels": [
      {
        "title": "Traffic Distribution",
        "type": "stat",
        "targets": [
          {
            "expr": "neo4j_blue_green_traffic_blue_percentage",
            "legendFormat": "Blue"
          },
          {
            "expr": "neo4j_blue_green_traffic_green_percentage", 
            "legendFormat": "Green"
          }
        ]
      },
      {
        "title": "Deployment Timeline",
        "type": "graph",
        "targets": [
          {
            "expr": "neo4j_blue_green_deployment_events",
            "legendFormat": "Deployment Events"
          }
        ]
      },
      {
        "title": "Response Time Comparison",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(neo4j_request_duration_seconds_bucket{environment=\"blue\"}[5m]))",
            "legendFormat": "Blue P95"
          },
          {
            "expr": "histogram_quantile(0.95, rate(neo4j_request_duration_seconds_bucket{environment=\"green\"}[5m]))",
            "legendFormat": "Green P95"
          }
        ]
      }
    ]
  }
}
```

## Troubleshooting

### Common Issues

#### Traffic Not Switching

**Problem**: Traffic remains on blue environment despite configuration changes.

**Solution**:
```bash
# Check ingress annotations
kubectl get ingress neo4j-green-ingress -n neo4j-system -o yaml

# Verify ingress controller is running
kubectl get pods -n ingress-nginx

# Check ingress controller logs
kubectl logs -n ingress-nginx deployment/ingress-nginx-controller

# Force ingress reload
kubectl delete pod -n ingress-nginx -l app.kubernetes.io/name=ingress-nginx
```

#### Green Environment Not Ready

**Problem**: Green environment pods are not becoming ready.

**Solution**:
```bash
# Check pod status
kubectl get pods -l app=neo4j-green -n neo4j-system

# Check pod logs
kubectl logs neo4j-green-0 -n neo4j-system

# Check resource constraints
kubectl describe pod neo4j-green-0 -n neo4j-system

# Check persistent volume claims
kubectl get pvc -l app=neo4j-green -n neo4j-system
```

#### Performance Degradation

**Problem**: Green environment shows poor performance.

**Solution**:
```bash
# Check resource utilization
kubectl top pods -l app=neo4j-green -n neo4j-system

# Verify JVM settings
kubectl exec neo4j-green-0 -n neo4j-system -- java -XX:+PrintFlagsFinal -version | grep -i heap

# Check database statistics
kubectl exec neo4j-green-0 -n neo4j-system -- cypher-shell -u neo4j -p $NEO4J_PASSWORD "CALL db.stats.retrieve('GRAPH COUNTS')"

# Compare configurations
diff <(kubectl get neo4jenterprisecluster neo4j-production -o yaml) <(kubectl get neo4jenterprisecluster neo4j-production-green -o yaml)
```

### Debug Commands

```bash
# Check blue-green deployment status
kubectl get neo4jenterprisecluster neo4j-production -o jsonpath='{.status.blueGreenDeployment}'

# View deployment events
kubectl get events --field-selector involvedObject.name=neo4j-production -n neo4j-system

# Check controller logs
kubectl logs -n neo4j-operator-system deployment/neo4j-operator-controller-manager

# Validate traffic distribution
curl -H "Host: neo4j.example.com" http://ingress-nginx-controller/debug/config | grep -A 20 neo4j
```

## Best Practices

### Resource Management

1. **Resource Allocation**
   - Ensure sufficient cluster resources for both environments
   - Use resource quotas to prevent resource contention
   - Monitor resource usage during deployment process

2. **Storage Management**
   - Use separate PVCs for blue and green environments
   - Implement backup before switching traffic
   - Consider storage class performance characteristics

### Security Considerations

1. **Network Policies**
   - Isolate blue and green environments
   - Restrict cross-environment communication
   - Use separate service accounts

2. **Secret Management**
   - Rotate secrets between deployments
   - Use different secret versions for environments
   - Implement secret scanning in CI/CD

### Operational Excellence

1. **Monitoring Setup**
   - Monitor both environments continuously
   - Set up alerts for deployment failures
   - Track deployment metrics and success rates

2. **Testing Strategy**
   - Implement comprehensive validation tests
   - Use canary analysis for performance validation
   - Maintain rollback procedures

## Integration Examples

### ArgoCD Integration

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: neo4j-blue-green
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/company/neo4j-config
    targetRevision: HEAD
    path: k8s/neo4j
  destination:
    server: https://kubernetes.default.svc
    namespace: neo4j-system
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
    - CreateNamespace=true
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m
```

### Flagger Integration

```yaml
apiVersion: flagger.app/v1beta1
kind: Canary
metadata:
  name: neo4j-canary
  namespace: neo4j-system
spec:
  targetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: neo4j-green
  progressDeadlineSeconds: 60
  service:
    port: 7474
    targetPort: 7474
  analysis:
    interval: 1m
    threshold: 5
    maxWeight: 50
    stepWeight: 10
    metrics:
    - name: request-success-rate
      thresholdRange:
        min: 99
      interval: 1m
    - name: request-duration
      thresholdRange:
        max: 500
      interval: 30s
    webhooks:
    - name: load-test
      url: http://flagger-loadtester.test/
      timeout: 5s
      metadata:
        cmd: "hey -z 1m -q 10 -c 2 http://neo4j-canary.neo4j-system:7474/db/data/"
```

### Tekton Pipeline Integration

```yaml
apiVersion: tekton.dev/v1beta1
kind: Pipeline
metadata:
  name: neo4j-blue-green-pipeline
spec:
  params:
  - name: image-tag
    type: string
  - name: environment
    type: string
    default: "green"
  
  tasks:
  - name: deploy-environment
    taskRef:
      name: kubectl-deploy
    params:
    - name: manifest
      value: |
        apiVersion: neo4j.neo4j.com/v1alpha1
        kind: Neo4jEnterpriseCluster
        metadata:
          name: neo4j-$(params.environment)
        spec:
          image: neo4j:$(params.image-tag)
          replicas: 3
  
  - name: health-check
    taskRef:
      name: health-check
    params:
    - name: environment
      value: "$(params.environment)"
    runAfter:
    - deploy-environment
  
  - name: performance-test
    taskRef:
      name: performance-test
    params:
    - name: environment
      value: "$(params.environment)"
    runAfter:
    - health-check
  
  - name: switch-traffic
    taskRef:
      name: traffic-switch
    params:
    - name: target-environment
      value: "$(params.environment)"
    runAfter:
    - performance-test
```

## Related Documentation

- [Auto-Scaling Guide](./auto-scaling-guide.md)
- [Disaster Recovery Guide](./disaster-recovery-guide.md) 
- [Plugin Management Guide](./plugin-management-guide.md)
- [Query Performance Monitoring](./query-monitoring-guide.md)
- [Backup and Restore Guide](../backup-restore-guide.md)

---

*For additional support and advanced configurations, please refer to the [Neo4j Operator Documentation](../README.md) or contact the platform engineering team.* 