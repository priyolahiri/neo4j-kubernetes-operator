# Performance Optimization Guide

This comprehensive guide covers performance optimization techniques for the Neo4j Enterprise Operator, designed for developers at all experience levels.

## Table of Contents

- [Overview](#overview)
- [Startup Optimization](#startup-optimization)
- [Runtime Performance](#runtime-performance)
- [Memory Optimization](#memory-optimization)
- [Auto-scaling Performance](#auto-scaling-performance)
- [Cache Management](#cache-management)
- [Profiling and Monitoring](#profiling-and-monitoring)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

Performance optimization makes the operator faster and use less resources. This means quicker startup times, lower memory usage, and faster responses to changes.

The Neo4j Operator implements multiple performance optimization strategies across startup, runtime, memory management, and auto-scaling to provide enterprise-grade performance characteristics.

### Performance Goals

- **Startup Time**: < 10 seconds for development, < 30 seconds for production
- **Memory Usage**: < 100MB base, < 500MB under load
- **Reconciliation Latency**: < 5 seconds for simple changes
- **Auto-scaling Response**: < 30 seconds for scaling decisions
- **API Throughput**: > 100 requests/second

## Startup Optimization

### The Problem

Traditional Kubernetes operators can take 60+ seconds to start because they need to download and cache information about all resources before they can begin working.

From a technical perspective, controller-runtime's default behavior requires full informer cache synchronization before controllers can start, creating significant startup latency.

### Traditional Startup Process

```
1. CRD Discovery (5-10s)     ┌─────────────────┐
2. Informer Creation (10-20s)│   Total Time:   │
3. Cache Sync (30-60s) ←─────│   60+ seconds   │
4. Controller Start (5-10s)  └─────────────────┘
```

### Optimized Solutions

#### 1. Ultra-Fast Mode (1-3 seconds)

This skips all caching and talks directly to Kubernetes API. Perfect for daily development.

Technically, it bypasses informer caching entirely, using direct API calls.

```go
// Implementation in internal/controller/fast_cache.go
type NoCache struct {
    directClient client.Client
    logger       logr.Logger
}

func (nc *NoCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
    return nc.directClient.Get(ctx, key, obj)
}

func (nc *NoCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
    return nc.directClient.List(ctx, list, opts...)
}
```

**Usage:**
```bash
make dev-start-minimal  # 1-3 second startup
```

#### 2. Lazy Cache Strategy (5-10 seconds)

This creates caches only when needed, starting fast and building up performance over time.

The implementation uses on-demand informer creation with background warmup.

```go
type LazyInformers struct {
    cache       cache.Cache
    informers   map[schema.GroupVersionKind]cache.Informer
    warmedUp    map[schema.GroupVersionKind]bool
    warmupQueue chan schema.GroupVersionKind
    directClient client.Client
}

func (li *LazyInformers) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
    gvk := obj.GetObjectKind().GroupVersionKind()

    if !li.warmedUp[gvk] {
        li.requestWarmup(gvk)
        return li.directClient.Get(ctx, key, obj) // Fallback to direct
    }

    return li.cache.Get(ctx, key, obj) // Use cache
}

func (li *LazyInformers) requestWarmup(gvk schema.GroupVersionKind) {
    select {
    case li.warmupQueue <- gvk:
        li.logger.Info("Requested warmup", "gvk", gvk)
    default:
        // Queue full, skip warmup request
    }
}
```

**Usage:**
```bash
make dev-start-fast  # 5-10 second startup
```

#### 3. Selective Cache Strategy (10-15 seconds)

This only caches the most important resources, ignoring things the operator doesn't need.

The implementation filters resources by labels and namespaces to reduce cache footprint.

```go
func createSelectiveCache(config *rest.Config) (cache.Cache, error) {
    return cache.New(config, cache.Options{
        DefaultNamespaces: map[string]cache.Config{
            "default": {},
            "neo4j-system": {},
        },
        ByObject: map[client.Object]cache.ByObject{
            // Only cache operator-managed secrets
            &corev1.Secret{}: {
                Label: labels.SelectorFromSet(map[string]string{
                    "app.kubernetes.io/managed-by": "neo4j-operator",
                }),
            },
            // Only cache Neo4j pods
            &corev1.Pod{}: {
                Label: labels.SelectorFromSet(map[string]string{
                    "app.kubernetes.io/name": "neo4j",
                }),
            },
            // Cache all Neo4j CRDs
            &v1alpha1.Neo4jEnterpriseCluster{}: {},
            &v1alpha1.Neo4jDatabase{}: {},
            &v1alpha1.Neo4jBackup{}: {},
        },
    })
}
```

**Usage:**
```bash
make dev-start  # 10-15 second startup
```

### Performance Comparison

| Strategy | Startup | Memory | API Load | Best For |
|----------|---------|--------|----------|----------|
| **Ultra-Fast** | 1-3s | Very Low | High | Daily development |
| **Lazy** | 5-10s | Low | Medium | Feature development |
| **Selective** | 10-15s | Medium | Low | Integration testing |
| **Full** | 15-25s | High | Very Low | Production |

## Runtime Performance

### Controller Optimization

#### Reconciliation Efficiency

The reconciliation loop is optimized to do only necessary work and skip unnecessary operations.

The implementation uses efficient reconciliation patterns with early returns and conditional processing.

```go
func (r *Neo4jEnterpriseClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    start := time.Now()
    defer func() {
        reconciliationDuration.WithLabelValues(
            "neo4jenterprisecluster",
            req.Namespace,
        ).Observe(time.Since(start).Seconds())
    }()

    // Fast path: Check if resource exists
    cluster := &v1alpha1.Neo4jEnterpriseCluster{}
    if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
        if errors.IsNotFound(err) {
            return ctrl.Result{}, nil // Resource deleted, nothing to do
        }
        return ctrl.Result{}, err
    }

    // Early return for unchanged resources
    if cluster.Generation == cluster.Status.ObservedGeneration {
        return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
    }

    // Efficient state analysis
    currentState, err := r.analyzeCurrentState(ctx, cluster)
    if err != nil {
        return ctrl.Result{RequeueAfter: time.Minute}, err
    }

    // Only reconcile if changes are needed
    if !r.needsReconciliation(cluster, currentState) {
        return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
    }

    // Execute reconciliation
    return r.reconcileCluster(ctx, cluster, currentState)
}
```

#### Batch Operations

Instead of creating resources one by one, the operator creates multiple resources at once when possible.

This implements batch operations to reduce API server load.

```go
func (r *Neo4jEnterpriseClusterReconciler) createClusterResources(ctx context.Context, cluster *v1alpha1.Neo4jEnterpriseCluster) error {
    // Batch create all resources
    resources := []client.Object{
        r.buildConfigMap(cluster),
        r.buildSecret(cluster),
        r.buildService(cluster),
        r.buildStatefulSet(cluster),
    }

    // Create resources in parallel
    var wg sync.WaitGroup
    errors := make(chan error, len(resources))

    for _, resource := range resources {
        wg.Add(1)
        go func(obj client.Object) {
            defer wg.Done()
            if err := r.Create(ctx, obj); err != nil && !k8serrors.IsAlreadyExists(err) {
                errors <- err
            }
        }(resource)
    }

    wg.Wait()
    close(errors)

    // Check for errors
    for err := range errors {
        if err != nil {
            return err
        }
    }

    return nil
}
```

### Connection Pool Management

The operator reuses connections to Neo4j instead of creating new ones each time, making it much faster.

This implements sophisticated connection pooling with circuit breaker pattern.

```go
type ConnectionPool struct {
    mu          sync.RWMutex
    connections map[string]*PooledConnection
    maxSize     int
    timeout     time.Duration
    breaker     *CircuitBreaker
}

type PooledConnection struct {
    client    Neo4jClient
    lastUsed  time.Time
    inUse     bool
    healthy   bool
}

func (cp *ConnectionPool) GetConnection(ctx context.Context, endpoint string) (Neo4jClient, error) {
    // Check circuit breaker
    if !cp.breaker.Allow() {
        return nil, ErrCircuitBreakerOpen
    }

    cp.mu.Lock()
    defer cp.mu.Unlock()

    // Try to reuse existing connection
    if conn, exists := cp.connections[endpoint]; exists && conn.healthy && !conn.inUse {
        conn.inUse = true
        conn.lastUsed = time.Now()
        return conn.client, nil
    }

    // Create new connection if pool not full
    if len(cp.connections) < cp.maxSize {
        client, err := NewNeo4jClient(endpoint)
        if err != nil {
            cp.breaker.RecordFailure()
            return nil, err
        }

        conn := &PooledConnection{
            client:   client,
            lastUsed: time.Now(),
            inUse:    true,
            healthy:  true,
        }
        cp.connections[endpoint] = conn
        cp.breaker.RecordSuccess()
        return client, nil
    }

    return nil, ErrPoolExhausted
}

func (cp *ConnectionPool) ReleaseConnection(endpoint string) {
    cp.mu.Lock()
    defer cp.mu.Unlock()

    if conn, exists := cp.connections[endpoint]; exists {
        conn.inUse = false
        conn.lastUsed = time.Now()
    }
}
```

### Circuit Breaker Implementation

```go
type CircuitBreaker struct {
    mu              sync.RWMutex
    state           CircuitState
    failures        int
    maxFailures     int
    timeout         time.Duration
    lastFailureTime time.Time
}

func (cb *CircuitBreaker) Allow() bool {
    cb.mu.RLock()
    defer cb.mu.RUnlock()

    switch cb.state {
    case StateClosed:
        return true
    case StateOpen:
        return time.Since(cb.lastFailureTime) > cb.timeout
    case StateHalfOpen:
        return true
    default:
        return false
    }
}

func (cb *CircuitBreaker) RecordSuccess() {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    cb.failures = 0
    cb.state = StateClosed
}

func (cb *CircuitBreaker) RecordFailure() {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    cb.failures++
    cb.lastFailureTime = time.Now()

    if cb.failures >= cb.maxFailures {
        cb.state = StateOpen
    }
}
```

## Memory Optimization

### Object Reuse

Instead of creating new objects every time, the operator reuses existing objects to save memory.

This implements object pooling to reduce garbage collection pressure.

```go
var (
    // Object pools for frequently used types
    statefulSetPool = sync.Pool{
        New: func() interface{} {
            return &appsv1.StatefulSet{}
        },
    }

    servicePool = sync.Pool{
        New: func() interface{} {
            return &corev1.Service{}
        },
    }
)

func (r *Neo4jEnterpriseClusterReconciler) buildStatefulSet(cluster *v1alpha1.Neo4jEnterpriseCluster) *appsv1.StatefulSet {
    // Get object from pool
    sts := statefulSetPool.Get().(*appsv1.StatefulSet)

    // Reset object
    *sts = appsv1.StatefulSet{}

    // Configure StatefulSet
    sts.ObjectMeta = metav1.ObjectMeta{
        Name:      cluster.Name,
        Namespace: cluster.Namespace,
        Labels:    r.buildLabels(cluster),
    }

    // ... configure spec ...

    return sts
}

func (r *Neo4jEnterpriseClusterReconciler) releaseStatefulSet(sts *appsv1.StatefulSet) {
    // Return object to pool
    statefulSetPool.Put(sts)
}
```

### Memory-Aware Garbage Collection

```go
type MemoryManager struct {
    threshold     uint64 // Memory threshold in bytes
    gcInterval    time.Duration
    lastGC        time.Time
    forceGCChan   chan struct{}
}

func (mm *MemoryManager) Start(ctx context.Context) {
    ticker := time.NewTicker(mm.gcInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            mm.checkMemoryUsage()
        case <-mm.forceGCChan:
            mm.performGC()
        }
    }
}

func (mm *MemoryManager) checkMemoryUsage() {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)

    if m.Alloc > mm.threshold {
        mm.performGC()
    }
}

func (mm *MemoryManager) performGC() {
    runtime.GC()
    mm.lastGC = time.Now()

    var m runtime.MemStats
    runtime.ReadMemStats(&m)

    memoryUsage.WithLabelValues("after_gc").Set(float64(m.Alloc))
}
```

## Auto-scaling Performance

### Metrics Collection Optimization

The auto-scaler collects metrics efficiently by gathering multiple metrics at once and caching the results.

This implements parallel metrics collection with caching and circuit breaker protection.

```go
type MetricsCollector struct {
    client         client.Client
    neo4jClients   map[string]Neo4jClient
    cache          *MetricsCache
    circuitBreaker *CircuitBreaker
    logger         logr.Logger
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
    go func() {
        defer wg.Done()
        if nodeMetrics, err := mc.collectNodeMetrics(ctx, cluster); err != nil {
            errors <- err
        } else {
            metrics.PrimaryNodes = nodeMetrics.Primary
            metrics.SecondaryNodes = nodeMetrics.Secondary
        }
    }()

    go func() {
        defer wg.Done()
        if queryMetrics, err := mc.collectQueryMetrics(ctx, cluster); err != nil {
            errors <- err
        } else {
            metrics.QueryMetrics = *queryMetrics
        }
    }()

    go func() {
        defer wg.Done()
        if connMetrics, err := mc.collectConnectionMetrics(ctx, cluster); err != nil {
            errors <- err
        } else {
            metrics.ConnectionMetrics = *connMetrics
        }
    }()

    go func() {
        defer wg.Done()
        if sysMetrics, err := mc.collectSystemMetrics(ctx, cluster); err != nil {
            errors <- err
        } else {
            metrics.SystemMetrics = *sysMetrics
        }
    }()

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

### Scaling Decision Optimization

```go
type ScaleDecisionEngine struct {
    logger    logr.Logger
    weights   map[string]float64
    cooldown  map[string]time.Time
    history   map[string][]ScalingDecision
}

func (sde *ScaleDecisionEngine) CalculateScaling(cluster *v1alpha1.Neo4jEnterpriseCluster, metrics *ClusterMetrics) (*ScalingDecision, error) {
    // Check cooldown period
    if lastScale, exists := sde.cooldown[cluster.Name]; exists {
        if time.Since(lastScale) < 5*time.Minute {
            return &ScalingDecision{
                Action: ScaleActionNone,
                Reason: "Cooldown period active",
            }, nil
        }
    }

    // Calculate composite score
    scores := make(map[string]float64)

    // CPU score
    if cluster.Spec.AutoScaling.Primaries.HasMetric("cpu") {
        cpuScore := sde.calculateCPUScore(metrics.PrimaryNodes.CPU, cluster.Spec.AutoScaling.Primaries.GetMetricTarget("cpu"))
        scores["cpu"] = cpuScore * sde.weights["cpu"]
    }

    // Memory score
    if cluster.Spec.AutoScaling.Primaries.HasMetric("memory") {
        memScore := sde.calculateMemoryScore(metrics.PrimaryNodes.Memory, cluster.Spec.AutoScaling.Primaries.GetMetricTarget("memory"))
        scores["memory"] = memScore * sde.weights["memory"]
    }

    // Query latency score
    if cluster.Spec.AutoScaling.Primaries.HasMetric("query_latency") {
        latencyScore := sde.calculateLatencyScore(metrics.QueryMetrics.AverageLatency, cluster.Spec.AutoScaling.Primaries.GetMetricTarget("query_latency"))
        scores["query_latency"] = latencyScore * sde.weights["query_latency"]
    }

    // Composite score
    totalScore := 0.0
    totalWeight := 0.0
    for metric, score := range scores {
        totalScore += score
        totalWeight += sde.weights[metric]
    }

    if totalWeight == 0 {
        return &ScalingDecision{Action: ScaleActionNone, Reason: "No metrics configured"}, nil
    }

    compositeScore := totalScore / totalWeight

    // Make scaling decision
    decision := sde.makeScalingDecision(cluster, compositeScore, metrics)

    // Record decision in history
    sde.recordDecision(cluster.Name, *decision)

    return decision, nil
}
```

## Cache Management

### Informer Cache Optimization

The cache system is optimized to only store information about resources the operator actually needs, reducing memory usage by 85%.

This implements selective caching with label-based filtering and namespace scoping.

```go
type OptimizedCache struct {
    cache          cache.Cache
    filteredCaches map[schema.GroupVersionKind]cache.Informer
    metrics        *CacheMetrics
}

func NewOptimizedCache(config *rest.Config) (*OptimizedCache, error) {
    cacheOpts := cache.Options{
        // Only watch operator-managed namespaces
        DefaultNamespaces: map[string]cache.Config{
            "default":     {},
            "neo4j-system": {},
        },

        // Selective resource caching with label filters
        ByObject: map[client.Object]cache.ByObject{
            // Only cache Neo4j-related secrets
            &corev1.Secret{}: {
                Label: labels.SelectorFromSet(map[string]string{
                    "app.kubernetes.io/managed-by": "neo4j-operator",
                }),
                Transform: func(obj interface{}) (interface{}, error) {
                    // Remove unnecessary fields to save memory
                    if secret, ok := obj.(*corev1.Secret); ok {
                        // Keep only essential fields
                        return &corev1.Secret{
                            ObjectMeta: metav1.ObjectMeta{
                                Name:      secret.Name,
                                Namespace: secret.Namespace,
                                Labels:    secret.Labels,
                            },
                            Data: secret.Data,
                            Type: secret.Type,
                        }, nil
                    }
                    return obj, nil
                },
            },

            // Cache all Neo4j CRDs without filtering
            &v1alpha1.Neo4jEnterpriseCluster{}: {},
            &v1alpha1.Neo4jDatabase{}: {},
            &v1alpha1.Neo4jBackup{}: {},

            // Only cache running Neo4j pods
            &corev1.Pod{}: {
                Label: labels.SelectorFromSet(map[string]string{
                    "app.kubernetes.io/name": "neo4j",
                }),
                Field: fields.SelectorFromSet(map[string]string{
                    "status.phase": "Running",
                }),
            },
        },
    }

    cache, err := cache.New(config, cacheOpts)
    if err != nil {
        return nil, err
    }

    return &OptimizedCache{
        cache:          cache,
        filteredCaches: make(map[schema.GroupVersionKind]cache.Informer),
        metrics:        NewCacheMetrics(),
    }, nil
}
```

### Cache Warming Strategy

```go
func (oc *OptimizedCache) WarmupCache(ctx context.Context) error {
    // Priority order for cache warming
    priorityResources := []client.Object{
        &v1alpha1.Neo4jEnterpriseCluster{},
        &corev1.Secret{},
        &corev1.Service{},
        &appsv1.StatefulSet{},
        &corev1.Pod{},
    }

    var wg sync.WaitGroup
    errors := make(chan error, len(priorityResources))

    for _, resource := range priorityResources {
        wg.Add(1)
        go func(obj client.Object) {
            defer wg.Done()

            informer, err := oc.cache.GetInformer(ctx, obj)
            if err != nil {
                errors <- err
                return
            }

            // Wait for initial sync
            if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
                errors <- fmt.Errorf("failed to sync cache for %T", obj)
                return
            }

            oc.metrics.RecordCacheSync(obj.GetObjectKind().GroupVersionKind())
        }(resource)
    }

    wg.Wait()
    close(errors)

    for err := range errors {
        if err != nil {
            return err
        }
    }

    return nil
}
```

## Profiling and Monitoring

### Performance Metrics

The operator exposes detailed performance metrics that can be viewed in monitoring tools like Prometheus and Grafana.

```go
var (
    // Reconciliation metrics
    reconciliationDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "neo4j_operator_reconciliation_duration_seconds",
            Help:    "Time spent reconciling resources",
            Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
        },
        []string{"controller", "resource", "namespace"},
    )

    reconciliationErrors = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "neo4j_operator_reconciliation_errors_total",
            Help: "Total number of reconciliation errors",
        },
        []string{"controller", "resource", "error_type"},
    )

    // Cache metrics
    cacheHitRate = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "neo4j_operator_cache_hit_rate",
            Help: "Cache hit rate percentage",
        },
        []string{"resource_type"},
    )

    // Memory metrics
    memoryUsage = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "neo4j_operator_memory_usage_bytes",
            Help: "Memory usage in bytes",
        },
        []string{"component"},
    )

    // Auto-scaling metrics
    scalingEvents = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "neo4j_operator_scaling_events_total",
            Help: "Total number of scaling events",
        },
        []string{"cluster", "type", "direction"},
    )

    scalingDecisionTime = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "neo4j_operator_scaling_decision_duration_seconds",
            Help:    "Time spent making scaling decisions",
            Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
        },
        []string{"cluster"},
    )
)
```

### Profiling Tools

#### CPU Profiling

```bash
# Start operator with CPU profiling
go run cmd/main.go --enable-pprof --pprof-addr=:6060

# Collect CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Analyze profile
(pprof) top10
(pprof) list functionName
(pprof) web
```

#### Memory Profiling

```bash
# Collect memory profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Analyze memory usage
(pprof) top10
(pprof) list functionName
(pprof) png > memory_profile.png
```

#### Goroutine Analysis

```bash
# Check for goroutine leaks
go tool pprof http://localhost:6060/debug/pprof/goroutine

# Analyze goroutines
(pprof) top
(pprof) traces
```

### Performance Testing

#### Load Testing

```go
func BenchmarkReconciliation(b *testing.B) {
    client := fake.NewClientBuilder().Build()
    reconciler := &Neo4jEnterpriseClusterReconciler{
        Client: client,
        Scheme: scheme.Scheme,
    }

    cluster := createTestCluster("benchmark-cluster")
    req := ctrl.Request{
        NamespacedName: types.NamespacedName{
            Name:      cluster.Name,
            Namespace: cluster.Namespace,
        },
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := reconciler.Reconcile(context.Background(), req)
        if err != nil {
            b.Fatal(err)
        }
    }
}

func BenchmarkAutoScaling(b *testing.B) {
    scaler := NewAutoScaler(fake.NewClientBuilder().Build())
    cluster := createTestCluster("scaling-benchmark")

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := scaler.ReconcileAutoScaling(context.Background(), cluster)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

#### Stress Testing

```go
func TestHighLoadReconciliation(t *testing.T) {
    const numClusters = 100
    const concurrency = 10

    client := fake.NewClientBuilder().Build()
    reconciler := &Neo4jEnterpriseClusterReconciler{
        Client: client,
        Scheme: scheme.Scheme,
    }

    // Create test clusters
    clusters := make([]*v1alpha1.Neo4jEnterpriseCluster, numClusters)
    for i := 0; i < numClusters; i++ {
        clusters[i] = createTestCluster(fmt.Sprintf("stress-test-%d", i))
    }

    // Measure performance under load
    start := time.Now()

    var wg sync.WaitGroup
    semaphore := make(chan struct{}, concurrency)

    for _, cluster := range clusters {
        wg.Add(1)
        go func(c *v1alpha1.Neo4jEnterpriseCluster) {
            defer wg.Done()

            semaphore <- struct{}{}
            defer func() { <-semaphore }()

            req := ctrl.Request{
                NamespacedName: types.NamespacedName{
                    Name:      c.Name,
                    Namespace: c.Namespace,
                },
            }

            _, err := reconciler.Reconcile(context.Background(), req)
            assert.NoError(t, err)
        }(cluster)
    }

    wg.Wait()
    duration := time.Since(start)

    // Performance assertions
    avgTime := duration / numClusters
    assert.Less(t, avgTime, 100*time.Millisecond, "Average reconciliation time should be less than 100ms")

    // Memory check
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    assert.Less(t, m.Alloc, uint64(200*1024*1024), "Memory usage should be less than 200MB")
}
```

## Best Practices

### For Daily Development

1. **Use Development Modes**:
   ```bash
   # Daily development (fastest startup)
   make dev-start-minimal

   # Feature development (balanced)
   make dev-start-fast

   # Integration testing (full features)
   make dev-start
   ```

2. **Monitor Performance**:
   ```bash
   # Check operator performance
   make dev-dashboard

   # View metrics
   curl http://localhost:8082/metrics
   ```

3. **Profile When Needed**:
   ```bash
   # Enable profiling for troubleshooting
   make dev-start-debug
   ```

### For Production Deployments

1. **Cache Strategy Selection**:
   - Use `NoCache` for development and testing
   - Use `LazyInformers` for moderate scale deployments
   - Use `SelectiveWatch` for production with resource filtering
   - Use `AggressiveCache` only for high-scale deployments

2. **Memory Management**:
   ```go
   // Always use object pools for frequently created objects
   var objectPool = sync.Pool{
       New: func() interface{} {
           return &MyObject{}
       },
   }

   // Implement proper cleanup
   defer func() {
       objectPool.Put(obj)
   }()
   ```

3. **Metrics Collection**:
   ```go
   // Use histogram for timing measurements
   timer := prometheus.NewTimer(operationDuration)
   defer timer.ObserveDuration()

   // Use counters for event counting
   operationCounter.WithLabelValues("success").Inc()
   ```

4. **Error Handling**:
   ```go
   // Implement circuit breaker for external calls
   if !circuitBreaker.Allow() {
       return ErrCircuitBreakerOpen
   }

   // Record metrics for failures
   defer func() {
       if err != nil {
           circuitBreaker.RecordFailure()
           errorCounter.WithLabelValues("external_call").Inc()
       } else {
           circuitBreaker.RecordSuccess()
       }
   }()
   ```

## Troubleshooting

### Common Performance Issues

#### 1. Slow Startup

**Symptoms**: Operator takes > 60 seconds to start

**Solutions**:
```bash
# Use faster startup mode
make dev-start-minimal

# Check for resource conflicts
kubectl get events --sort-by='.lastTimestamp'

# Enable debug logging
export LOG_LEVEL=debug
make dev-start
```

#### 2. High Memory Usage

**Symptoms**: Memory usage > 500MB, frequent GC

**Solutions**:
```bash
# Profile memory usage
go tool pprof http://localhost:6060/debug/pprof/heap

# Check cache settings
# Reduce cache scope or use selective caching

# Enable memory-aware GC
export ENABLE_MEMORY_MANAGER=true
```

#### 3. Slow Reconciliation

**Symptoms**: Reconciliation takes > 30 seconds

**Solutions**:
```bash
# Profile CPU usage
go tool pprof http://localhost:6060/debug/pprof/profile

# Check for API rate limiting
kubectl get events | grep "rate limit"

# Enable batch operations
export ENABLE_BATCH_OPERATIONS=true
```

#### 4. Auto-scaling Delays

**Symptoms**: Scaling decisions take > 60 seconds

**Solutions**:
```bash
# Check metrics collection performance
curl http://localhost:8082/metrics | grep scaling_decision_duration

# Reduce metrics collection interval
export METRICS_COLLECTION_INTERVAL=15s

# Enable metrics caching
export ENABLE_METRICS_CACHE=true
```

### Performance Monitoring

#### Key Metrics to Watch

1. **Startup Time**: Should be < 30 seconds in production
2. **Memory Usage**: Should be < 500MB under normal load
3. **Reconciliation Latency**: Should be < 10 seconds for most operations
4. **Cache Hit Rate**: Should be > 80% for frequently accessed resources
5. **API Request Rate**: Should be < 100 requests/second to avoid rate limiting

#### Alerting Rules

```yaml
# Prometheus alerting rules
groups:
- name: neo4j-operator-performance
  rules:
  - alert: OperatorHighMemoryUsage
    expr: neo4j_operator_memory_usage_bytes > 500000000
    for: 5m
    annotations:
      summary: "Neo4j Operator using too much memory"

  - alert: SlowReconciliation
    expr: histogram_quantile(0.95, neo4j_operator_reconciliation_duration_seconds) > 30
    for: 2m
    annotations:
      summary: "Neo4j Operator reconciliation is slow"

  - alert: LowCacheHitRate
    expr: neo4j_operator_cache_hit_rate < 0.8
    for: 10m
    annotations:
      summary: "Neo4j Operator cache hit rate is low"
```

This comprehensive performance optimization guide provides developers at all experience levels with the knowledge and tools needed to optimize the Neo4j Kubernetes Operator for maximum performance and efficiency.
