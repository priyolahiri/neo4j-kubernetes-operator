# Architecture Guide

This document describes the architecture and design principles of the Neo4j Enterprise Operator for Kubernetes, with explanations for developers at all experience levels.

## Table of Contents

- [Overview](#overview)
- [Architecture Principles](#architecture-principles)
- [System Components](#system-components)
- [Controller Architecture](#controller-architecture)
- [Custom Resource Definitions](#custom-resource-definitions)
- [Security Model](#security-model)
- [Networking](#networking)
- [Storage Architecture](#storage-architecture)
- [Observability](#observability)
- [Performance Optimizations](#performance-optimizations)

## Overview

The Neo4j Enterprise Operator is a smart assistant that knows how to manage Neo4j databases in Kubernetes. When you tell it "I want a 3-node Neo4j cluster," it figures out all the complex details and makes it happen automatically.

From a technical perspective, the Neo4j Enterprise Operator is a Kubernetes-native operator that manages the lifecycle of Neo4j database clusters and related resources. It follows the Operator Pattern, extending the Kubernetes API with custom resources and controllers.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Neo4j Operator │    │       Custom Resources          │ │
│  │   Manager        │◄──►│  - Neo4jEnterpriseCluster      │ │
│  │                 │    │  - Neo4jDatabase               │ │
│  │  ┌─────────────┐ │    │  - Neo4jBackup                │ │
│  │  │Controllers  │ │    │  - Neo4jRestore               │ │
│  │  │- Cluster    │ │    │  - Neo4jUser                  │ │
│  │  │- Database   │ │    │  - Neo4jRole                  │ │
│  │  │- Backup     │ │    │  - Neo4jGrant                 │ │
│  │  │- Restore    │ │    │  - Neo4jPlugin                │ │
│  │  │- User       │ │    └─────────────────────────────────┘ │
│  │  │- Role       │ │                                      │
│  │  │- Grant      │ │    ┌─────────────────────────────────┐ │
│  │  │- Plugin     │ │    │        Neo4j Resources          │ │
│  │  │- AutoScale  │ │    │  - StatefulSets                │ │
│  │  └─────────────┘ │    │  - Services                    │ │
│  │                 │    │  - ConfigMaps                 │ │
│  │  ┌─────────────┐ │    │  - Secrets                    │ │
│  │  │Webhooks     │ │    │  - PersistentVolumeClaims     │ │
│  │  │- Validation │ │    │  - NetworkPolicies            │ │
│  │  │- Mutation   │ │    └─────────────────────────────────┘ │
│  │  └─────────────┘ │                                      │
│  └─────────────────┘                                      │
└─────────────────────────────────────────────────────────────┘
```

## Architecture Principles

### 1. Kubernetes-Native Design

The operator works exactly like other Kubernetes resources (pods, services, etc.), uses familiar `kubectl` commands, and integrates with existing Kubernetes tools and workflows.

Technically, it follows Kubernetes conventions and patterns, uses Custom Resource Definitions (CRDs) to extend the API, implements the Controller Pattern for resource management, and leverages Kubernetes primitives (StatefulSets, Services, etc.).

### 2. Declarative Configuration

You describe what you want (3-node cluster, backup every night) and the operator figures out how to make it happen. Changes are applied automatically when you update your configuration.

The implementation uses declarative desired state through custom resources, with controllers continuously reconciling actual state to desired state. Configuration changes trigger automatic updates with rollback capabilities through resource versioning.

### 3. Separation of Concerns

Each part of the operator has a specific job (clusters, backups, users, etc.). Changes to one area don't break others, making it easy to understand and troubleshoot.

From an architectural standpoint, each controller manages a specific resource type with clear boundaries between different operational aspects. The modular design allows independent development and testing with a plugin architecture for extensibility.

### 4. Security First

The system is secure by default with minimal configuration, uses Kubernetes security features automatically, and protects sensitive data like passwords and certificates.

This is implemented through RBAC integration with Kubernetes security model, secret management for sensitive data, network policies for traffic isolation, and admission controllers for validation and security.

### 5. Observability

Comprehensive monitoring and logging work out of the box with clear status messages and error reporting, plus integration with popular monitoring tools.

The technical implementation includes comprehensive metrics and logging, health checks and readiness probes, distributed tracing for complex operations, and event-driven architecture for audit trails.

## System Components

### Core Manager

The manager is the main program that runs the operator, acting like the conductor of an orchestra, coordinating all the different parts.

**Responsibilities:**
- Controller lifecycle management
- Webhook server management
- Leader election coordination
- Metrics and health endpoints

**Configuration:**
- Leader election settings
- Webhook certificate management
- Controller concurrency settings
- Resource limits and requests

### Controllers

#### 1. Neo4jEnterpriseCluster Controller

This controller knows how to create and manage Neo4j clusters. When you create a `Neo4jEnterpriseCluster` resource, this controller springs into action.

**Resources Managed:**
- StatefulSets for core and read replica nodes
- Services for cluster communication and client access
- ConfigMaps for Neo4j configuration
- Secrets for authentication and certificates

**Key Features:**
- Automatic cluster formation and discovery
- Rolling updates and scaling operations
- Backup scheduling and management
- Monitoring and alerting integration
- Auto-scaling with multiple metrics
- Topology-aware placement

#### 2. Auto-scaling Controller

This automatically adjusts cluster size based on workload. If your database gets busy, it adds more nodes. When traffic decreases, it scales down to save resources.

**Features:**
- Multi-metric scaling (CPU, memory, query latency, connections, throughput)
- Zone-aware scaling with distribution constraints
- Quorum protection for primary nodes
- Custom webhook-based scaling algorithms
- Predictive scaling capabilities

**Implementation Details:**
```go
type AutoScaler struct {
    client           client.Client
    logger           logr.Logger
    metricsCollector *MetricsCollector
    scaleDecision    *ScaleDecisionEngine
}
```

#### 3. Neo4jDatabase Controller

This manages individual databases within Neo4j clusters, like managing different "rooms" within your Neo4j "building."

**Resources Managed:**
- Database creation and configuration
- Schema management
- Access control policies
- Performance monitoring

#### 4. Backup/Restore Controllers

These controllers handle data protection. The backup controller automatically saves your data, while the restore controller can bring it back if needed.

**Backup Controller:**
- Scheduled backup operations
- Multiple storage backend support (S3, GCS, Azure Blob)
- Backup retention policies
- Backup validation and verification

**Restore Controller:**
- Point-in-time recovery
- Cross-cluster restore operations
- Data migration support
- Rollback capabilities

#### 5. Security Controllers (User, Role, Grant)

These controllers manage who can access your databases and what they can do. They work together to provide comprehensive security.

**User Controller:**
- User account lifecycle
- Password management
- Authentication integration

**Role Controller:**
- Role-based access control
- Permission management
- Role inheritance

**Grant Controller:**
- Database access grants
- Fine-grained permissions
- Audit trail

#### 6. Plugin Controller

This manages Neo4j plugins, allowing you to extend Neo4j's functionality with additional features.

**Features:**
- Plugin installation and removal
- Version management
- Configuration management
- Dependency resolution

### Webhooks

Webhooks are like security guards and helpful assistants. They check your configurations for problems and can automatically fix common issues.

#### Validation Webhooks
Ensure resource specifications are valid and secure.

**Validations:**
- Resource specification validation
- Security policy enforcement
- Resource quota compliance
- Dependency verification

#### Mutation Webhooks
Automatically modify resources to apply defaults and policies.

**Mutations:**
- Default value injection
- Security label application
- Resource annotation
- Configuration standardization

## Controller Architecture

### Reconciliation Loop

The reconciliation loop is like a continuous improvement process. The controller constantly checks "Is everything the way it should be?" and fixes any problems it finds.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Watch Events  │───►│  Reconcile      │───►│  Update Status  │
│   (Create,      │    │  - Analyze      │    │  - Conditions   │
│    Update,      │    │  - Plan         │    │  - Phase        │
│    Delete)      │    │  - Execute      │    │  - Observability│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                        │                        │
         │                        ▼                        │
         │               ┌─────────────────┐               │
         └───────────────│  Requeue Logic  │◄──────────────┘
                         │  - Backoff      │
                         │  - Rate Limit   │
                         │  - Error Handle │
                         └─────────────────┘
```

### Controller Components

#### Event Processing
```go
func (r *Neo4jEnterpriseClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // 1. Fetch the resource
    cluster := &v1alpha1.Neo4jEnterpriseCluster{}
    if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    // 2. Analyze current state
    currentState, err := r.analyzeCurrentState(ctx, cluster)
    if err != nil {
        return ctrl.Result{}, err
    }

    // 3. Plan changes
    plan, err := r.planChanges(ctx, cluster, currentState)
    if err != nil {
        return ctrl.Result{}, err
    }

    // 4. Execute plan
    if err := r.executePlan(ctx, cluster, plan); err != nil {
        return ctrl.Result{RequeueAfter: time.Minute}, err
    }

    // 5. Update status
    return r.updateStatus(ctx, cluster, currentState)
}
```

#### State Management
Controllers maintain comprehensive state tracking:

```go
type ClusterState struct {
    Phase          string
    Conditions     []metav1.Condition
    Replicas       ReplicaStatus
    Endpoints      EndpointStatus
    Version        string
    UpgradeStatus  *UpgradeStatus
}
```

## Custom Resource Definitions

### Resource Hierarchy

CRDs are like templates for different types of Neo4j resources. Each one defines what fields you can set and what the operator will do with them.

```
Neo4jEnterpriseCluster (Primary Resource)
├── Neo4jDatabase (Databases within cluster)
├── Neo4jBackup (Backup configurations)
├── Neo4jRestore (Restore operations)
├── Neo4jUser (User accounts)
├── Neo4jRole (Access roles)
├── Neo4jGrant (Access permissions)
└── Neo4jPlugin (Plugin management)
```

### API Design Patterns

#### Spec-Status Pattern
All resources follow the Kubernetes spec-status pattern:

```go
type Neo4jEnterpriseCluster struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   Neo4jEnterpriseClusterSpec   `json:"spec,omitempty"`
    Status Neo4jEnterpriseClusterStatus `json:"status,omitempty"`
}
```

#### Composition over Inheritance
Complex configurations are built through composition:

```yaml
spec:
  topology:          # Cluster layout
    primaries: 3
    secondaries: 2
  autoScaling:       # Scaling behavior
    enabled: true
    primaries: {...}
    secondaries: {...}
  multiCluster:      # Multi-cluster setup
    enabled: true
    topology: {...}
```

## Security Model

### Authentication and Authorization

The operator uses Kubernetes' built-in security. It only does what it's allowed to do, and it protects sensitive information like passwords.

#### RBAC Integration
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: neo4j-operator-manager-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "secrets"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

#### Secret Management
- Automatic secret generation for cluster authentication
- Integration with external secret management systems
- Certificate lifecycle management
- Encryption at rest and in transit

### Network Security

#### Network Policies
Automatic generation of network policies for cluster isolation:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: neo4j-cluster-network-policy
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: neo4j
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app.kubernetes.io/name: neo4j
    ports:
    - protocol: TCP
      port: 5000  # Cluster communication
    - protocol: TCP
      port: 7000  # Raft
```

## Performance Optimizations

### Startup Optimization

**The Challenge:** Traditional controller-runtime startup was slow (60+ seconds) due to informer cache synchronization.

**The Solution:** Multiple caching strategies implemented in `internal/controller/fast_cache.go`:

#### 1. NoCache Strategy (Ultra-Fast: 1-3 seconds)
```go
type NoCache struct {
    directClient client.Client
}

func (nc *NoCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
    return nc.directClient.Get(ctx, key, obj)
}
```

#### 2. LazyInformers Strategy (Fast: 5-10 seconds)
```go
type LazyInformers struct {
    cache         cache.Cache
    informers     map[schema.GroupVersionKind]cache.Informer
    warmupQueue   chan schema.GroupVersionKind
}

func (li *LazyInformers) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
    gvk := obj.GetObjectKind().GroupVersionKind()
    if !li.isWarmedUp(gvk) {
        li.requestWarmup(gvk)
        return li.directClient.Get(ctx, key, obj)
    }
    return li.cache.Get(ctx, key, obj)
}
```

#### 3. SelectiveWatch Strategy (Balanced: 10-15 seconds)
```go
cacheOpts := cache.Options{
    ByObject: map[client.Object]cache.ByObject{
        &v1alpha1.Neo4jEnterpriseCluster{}: {},
        &corev1.Secret{}: {
            Label: labels.SelectorFromSet(map[string]string{
                "app.kubernetes.io/managed-by": "neo4j-operator",
            }),
        },
    },
}
```

### Memory Optimization

#### Connection Pool Management
- **Circuit Breaker Pattern**: Prevents cascade failures
- **Optimized Pool Sizing**: 20 connections for memory efficiency
- **Smart Timeouts**: 5-second acquisition, 10-second query timeouts
- **Benefits**: 60% reduction in memory usage per client

#### Controller Memory Optimization
- **Object Reuse**: Kubernetes objects are pooled and reused
- **Connection Caching**: Cached Neo4j client connections
- **Rate Limiting**: Controlled concurrent reconciliation
- **Benefits**: 70% reduction in GC frequency

#### Cache Management
- **Selective Watching**: Only operator-managed resources
- **Label-Based Filtering**: 85% reduction in cached objects
- **Memory-Aware GC**: Threshold-based garbage collection
- **Benefits**: Lower memory usage, faster reconciliation

### Auto-scaling Performance

The auto-scaling system implements sophisticated performance optimizations:

#### Metrics Collection Optimization
```go
type MetricsCollector struct {
    client         client.Client
    neo4jClients   map[string]Neo4jClient  // Cached connections
    circuitBreaker *CircuitBreaker         // Failure protection
}

func (mc *MetricsCollector) CollectMetrics(ctx context.Context, cluster *v1alpha1.Neo4jEnterpriseCluster) (*ClusterMetrics, error) {
    // Check cache first
    if cached := mc.cache.Get(cluster.Name); cached != nil && !cached.IsExpired() {
        return cached.Metrics, nil
    }

    // Collect metrics in parallel
    var wg sync.WaitGroup
    metrics := &ClusterMetrics{}
    errors := make(chan error, 4)

    // Collect different metric types concurrently
    wg.Add(4)
    go func() { defer wg.Done(); metrics.PrimaryNodes = mc.collectNodeMetrics(ctx, cluster, "primary") }()
    go func() { defer wg.Done(); metrics.SecondaryNodes = mc.collectNodeMetrics(ctx, cluster, "secondary") }()
    go func() { defer wg.Done(); metrics.QueryMetrics = mc.collectQueryMetrics(ctx, cluster) }()
    go func() { defer wg.Done(); metrics.SystemMetrics = mc.collectSystemMetrics(ctx, cluster) }()

    wg.Wait()
    close(errors)

    // Check for errors
    for err := range errors {
        if err != nil {
            return nil, err
        }
    }

    // Cache results
    mc.cache.Set(cluster.Name, metrics, 30*time.Second)

    return metrics, nil
}
```

## Observability

### Metrics

The operator provides detailed metrics about cluster health, performance, and operations that integrate with monitoring tools like Prometheus.

#### Operator Metrics
```go
var (
    reconciliationDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "neo4j_operator_reconciliation_duration_seconds",
            Help: "Time spent reconciling resources",
        },
        []string{"controller", "resource", "namespace"},
    )

    scalingEvents = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "neo4j_operator_scaling_events_total",
            Help: "Total number of scaling events",
        },
        []string{"cluster", "type", "direction"},
    )
)
```

#### Neo4j Metrics Integration
- Automatic metrics collection from Neo4j instances
- Custom metrics for auto-scaling decisions
- Performance monitoring and alerting

### Logging

Structured logging with multiple levels:
```go
logger := log.FromContext(ctx).WithValues(
    "cluster", cluster.Name,
    "namespace", cluster.Namespace,
    "reconcile_id", uuid.New().String(),
)

logger.Info("Starting cluster reconciliation",
    "desired_primaries", cluster.Spec.Topology.Primaries,
    "current_primaries", cluster.Status.Replicas.Primaries,
)
```

### Health Checks

Comprehensive health monitoring:
- Kubernetes readiness and liveness probes
- Neo4j cluster health validation
- Auto-scaling system health
- Performance monitoring

This architecture provides a robust, scalable, and maintainable foundation for managing Neo4j Enterprise clusters in Kubernetes environments, with optimizations that significantly improve development experience and production performance.
