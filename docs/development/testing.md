# Testing Guide

This comprehensive guide covers testing best practices and methodologies for the Neo4j Enterprise Operator, designed for developers at all experience levels.

## Table of Contents

- [Testing Philosophy](#testing-philosophy)
- [Quick Start Testing](#quick-start-testing)
- [Test Types](#test-types)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
- [Test Environment Setup](#test-environment-setup)
- [Advanced Testing](#advanced-testing)
- [Performance Testing](#performance-testing)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Testing Philosophy

Testing ensures your code works correctly and doesn't break existing functionality. Think of tests as safety nets that catch problems before users see them.

Our testing approach follows the testing pyramid with emphasis on fast feedback and comprehensive coverage.

### Testing Pyramid

```
        🔺 E2E Tests (10%)
       🔺🔺 Integration Tests (20%)
    🔺🔺🔺🔺 Unit Tests (70%)
```

### Test Principles

- **Fast Feedback** - Tests should run quickly and provide immediate feedback
- **Reliable** - Tests should be deterministic and not flaky
- **Maintainable** - Tests should be easy to understand and modify
- **Comprehensive** - Critical paths must be covered
- **Isolated** - Tests should not depend on external systems when possible

## Quick Start Testing

### Getting Started

```bash
# 🚀 Run all tests (comprehensive)
make test-all

# ⚡ Quick test suite (for daily development)
make test-quick

# 📊 Generate test report with coverage
make test-report
```

### Advanced Testing

```bash
# Comprehensive test suite with detailed reporting
make test-all-comprehensive

# Performance and load testing
make test-performance

# Security and chaos testing
make test-security test-chaos
```

## Test Types

### 1. Unit Tests

Unit tests check individual functions and methods in isolation. They're like testing each gear in a watch separately.

**Location**: `*_test.go` files alongside source code
**Framework**: Go's built-in testing + testify assertions
**Speed**: < 1 second per test

#### Example Unit Test

```go
func TestNewAutoScaler(t *testing.T) {
    tests := []struct {
        name        string
        client      client.Client
        want        *AutoScaler
        wantErr     bool
    }{
        {
            name:   "successful creation",
            client: fake.NewClientBuilder().Build(),
            want: &AutoScaler{
                client: fake.NewClientBuilder().Build(),
                logger: logr.Discard(),
            },
            wantErr: false,
        },
        {
            name:    "nil client",
            client:  nil,
            want:    nil,
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := NewAutoScaler(tt.client)
            if tt.wantErr {
                assert.Nil(t, got)
                return
            }
            assert.NotNil(t, got)
            assert.Equal(t, tt.client, got.client)
        })
    }
}
```

#### Testing Auto-scaling Logic

```go
func TestScaleDecisionEngine_CalculatePrimaryScaling(t *testing.T) {
    tests := []struct {
        name     string
        cluster  *v1alpha1.Neo4jEnterpriseCluster
        metrics  *ClusterMetrics
        expected *ScalingDecision
    }{
        {
            name: "scale up due to high CPU",
            cluster: &v1alpha1.Neo4jEnterpriseCluster{
                Spec: v1alpha1.Neo4jEnterpriseClusterSpec{
                    AutoScaling: &v1alpha1.AutoScalingSpec{
                        Primaries: &v1alpha1.PrimaryAutoScalingConfig{
                            MinReplicas: 3,
                            MaxReplicas: 7,
                            Metrics: []v1alpha1.AutoScalingMetric{
                                {Type: "cpu", Target: "70", Weight: "1.0"},
                            },
                        },
                    },
                },
            },
            metrics: &ClusterMetrics{
                PrimaryNodes: NodeMetrics{
                    Total: 3,
                    CPU:   MetricValue{Current: 85.0},
                },
            },
            expected: &ScalingDecision{
                Action:         ScaleActionUp,
                TargetReplicas: 5,
                Reason:         "CPU utilization 85.0% exceeds target 70%",
                Confidence:     0.8,
            },
        },
    }

    engine := NewScaleDecisionEngine(logr.Discard())
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := engine.CalculatePrimaryScaling(tt.cluster, tt.metrics)
            assert.Equal(t, tt.expected.Action, result.Action)
            assert.Equal(t, tt.expected.TargetReplicas, result.TargetReplicas)
            assert.Contains(t, result.Reason, "CPU utilization")
        })
    }
}
```

### 2. Integration Tests

Integration tests check how different parts work together. They're like testing if all the gears in a watch work together to keep time.

**Location**: `test/integration/`
**Framework**: Ginkgo + Gomega + envtest
**Environment**: Kubernetes control plane (etcd + API server)

#### Example Integration Test

```go
var _ = Describe("Neo4jEnterpriseCluster Controller", func() {
    var (
        ctx     context.Context
        cluster *v1alpha1.Neo4jEnterpriseCluster
    )

    BeforeEach(func() {
        ctx = context.Background()
        cluster = &v1alpha1.Neo4jEnterpriseCluster{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "test-cluster",
                Namespace: "default",
                Labels: map[string]string{
                    "test": "integration",
                },
            },
            Spec: v1alpha1.Neo4jEnterpriseClusterSpec{
                Image: v1alpha1.ImageSpec{
                    Repo: "neo4j",
                    Tag:  "5.26-enterprise",
                },
                Topology: v1alpha1.TopologyConfiguration{
                    Primaries:   3,
                    Secondaries: 1,
                },
                Storage: v1alpha1.StorageSpec{
                    ClassName: "standard",
                    Size:      "10Gi",
                },
            },
        }
    })

    Context("When creating a Neo4jEnterpriseCluster", func() {
        It("Should create the cluster successfully", func() {
            Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

            Eventually(func() bool {
                err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)
                return err == nil && cluster.Status.Phase == "Ready"
            }, timeout, interval).Should(BeTrue())

            // Verify StatefulSets are created
            var statefulSets appsv1.StatefulSetList
            Expect(k8sClient.List(ctx, &statefulSets, client.InNamespace("default"))).To(Succeed())
            Expect(len(statefulSets.Items)).To(BeNumerically(">=", 1))

            // Verify Services are created
            var services corev1.ServiceList
            Expect(k8sClient.List(ctx, &services, client.InNamespace("default"))).To(Succeed())
            Expect(len(services.Items)).To(BeNumerically(">=", 1))
        })

        It("Should handle auto-scaling configuration", func() {
            cluster.Spec.AutoScaling = &v1alpha1.AutoScalingSpec{
                Enabled: true,
                Primaries: &v1alpha1.PrimaryAutoScalingConfig{
                    Enabled:     true,
                    MinReplicas: 3,
                    MaxReplicas: 7,
                    Metrics: []v1alpha1.AutoScalingMetric{
                        {Type: "cpu", Target: "70", Weight: "1.0"},
                    },
                },
            }

            Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

            Eventually(func() bool {
                err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)
                return err == nil && cluster.Status.Phase == "Ready"
            }, timeout, interval).Should(BeTrue())

            // Verify auto-scaling is configured
            Expect(cluster.Status.Conditions).To(ContainElement(
                MatchFields(IgnoreExtras, Fields{
                    "Type":   Equal("AutoScalingReady"),
                    "Status": Equal(metav1.ConditionTrue),
                }),
            ))
        })
    })

    Context("When updating cluster configuration", func() {
        BeforeEach(func() {
            Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
            Eventually(func() bool {
                err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)
                return err == nil && cluster.Status.Phase == "Ready"
            }, timeout, interval).Should(BeTrue())
        })

        It("Should handle scaling operations", func() {
            // Scale up secondaries
            cluster.Spec.Topology.Secondaries = 3
            Expect(k8sClient.Update(ctx, cluster)).To(Succeed())

            Eventually(func() bool {
                err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)
                return err == nil && cluster.Status.Replicas.Secondaries == 3
            }, timeout, interval).Should(BeTrue())
        })
    })

    AfterEach(func() {
        // Cleanup
        Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
    })
})
```

### 3. End-to-End Tests

E2E tests check the entire system working together in a real environment. They're like testing if the complete watch works correctly when worn.

**Location**: `test/e2e/`
**Framework**: Ginkgo + Gomega
**Environment**: Real Kubernetes cluster (kind, minikube, etc.)

#### Example E2E Test

```go
var _ = Describe("Neo4j Operator E2E", func() {
    var (
        namespace string
        cluster   *v1alpha1.Neo4jEnterpriseCluster
    )

    BeforeEach(func() {
        namespace = "e2e-test-" + randomString(8)

        // Create test namespace
        ns := &corev1.Namespace{
            ObjectMeta: metav1.ObjectMeta{Name: namespace},
        }
        Expect(k8sClient.Create(context.Background(), ns)).To(Succeed())

        cluster = &v1alpha1.Neo4jEnterpriseCluster{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "e2e-cluster",
                Namespace: namespace,
            },
            Spec: v1alpha1.Neo4jEnterpriseClusterSpec{
                Image: v1alpha1.ImageSpec{
                    Repo: "neo4j",
                    Tag:  "5.26-enterprise",
                },
                Topology: v1alpha1.TopologyConfiguration{
                    Primaries:   3,
                    Secondaries: 2,
                },
                Storage: v1alpha1.StorageSpec{
                    ClassName: "standard",
                    Size:      "10Gi",
                },
                Auth: &v1alpha1.AuthSpec{
                    Provider:  "native",
                    SecretRef: "neo4j-auth",
                },
            },
        }
    })

    It("Should deploy a complete Neo4j cluster", func() {
        By("Creating authentication secret")
        secret := &corev1.Secret{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "neo4j-auth",
                Namespace: namespace,
            },
            Data: map[string][]byte{
                "username": []byte("neo4j"),
                "password": []byte("test-password"),
            },
        }
        Expect(k8sClient.Create(context.Background(), secret)).To(Succeed())

        By("Creating Neo4j cluster")
        Expect(k8sClient.Create(context.Background(), cluster)).To(Succeed())

        By("Waiting for cluster to be ready")
        Eventually(func() bool {
            err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), cluster)
            return err == nil && cluster.Status.Phase == "Ready"
        }, 10*time.Minute, 30*time.Second).Should(BeTrue())

        By("Verifying all pods are running")
        var pods corev1.PodList
        Expect(k8sClient.List(context.Background(), &pods,
            client.InNamespace(namespace),
            client.MatchingLabels{"app.kubernetes.io/name": "neo4j"},
        )).To(Succeed())

        runningPods := 0
        for _, pod := range pods.Items {
            if pod.Status.Phase == corev1.PodRunning {
                runningPods++
            }
        }
        Expect(runningPods).To(Equal(5)) // 3 primaries + 2 secondaries

        By("Testing Neo4j connectivity")
        // Port forward and test connection
        testNeo4jConnectivity(namespace, "e2e-cluster")
    })

    AfterEach(func() {
        // Cleanup namespace
        ns := &corev1.Namespace{
            ObjectMeta: metav1.ObjectMeta{Name: namespace},
        }
        Expect(k8sClient.Delete(context.Background(), ns)).To(Succeed())
    })
})

func testNeo4jConnectivity(namespace, clusterName string) {
    // Implementation to test actual Neo4j connectivity
    // This would use port-forwarding and Neo4j driver
}
```

## Running Tests

### Quick Commands

```bash
# Basic test commands
make test           # Unit tests only
make test-integration  # Integration tests only
make test-e2e       # End-to-end tests only
make test-all       # All tests

# With coverage
make coverage       # Generate coverage report
make coverage-html  # HTML coverage report

# Comprehensive testing
make test-all-comprehensive    # All tests with detailed reports
make test-quick-comprehensive  # Essential tests only
```

### Test Configuration

#### Environment Variables

```bash
# Test parallelization
export PARALLEL=true
export GINKGO_NODES=4

# Test timeouts
export TEST_TIMEOUT=30m
export INTEGRATION_TIMEOUT=10m
export E2E_TIMEOUT=20m

# Performance testing
export PERF_DURATION=10m
export PERF_CONCURRENT=20

# Debug mode
export DEBUG=true
export VERBOSE=true
```

#### Custom Test Runs

```bash
# Run specific test suites
go test ./internal/controller/... -v

# Run tests with custom flags
go test -run TestAutoScaler ./internal/controller/autoscaler_test.go -v

# Integration tests with custom environment
KUBECONFIG=~/.kube/config make test-integration

# E2E tests with specific cluster
KIND_CLUSTER_NAME=test-cluster make test-e2e
```

### Test Reports

```bash
# Generate comprehensive test report
make test-report

# View test summary
make test-summary

# Generate performance report
make test-performance-report
```

## Test Environment Setup

### Local Development

#### Using envtest (Integration Tests)

```go
var (
    cfg       *rest.Config
    k8sClient client.Client
    testEnv   *envtest.Environment
    ctx       context.Context
    cancel    context.CancelFunc
)

var _ = BeforeSuite(func() {
    ctx, cancel = context.WithCancel(context.TODO())

    By("bootstrapping test environment")
    testEnv = &envtest.Environment{
        CRDDirectoryPaths: []string{
            filepath.Join("..", "..", "config", "crd", "bases"),
        },
        ErrorIfCRDPathMissing: true,
    }

    var err error
    cfg, err = testEnv.Start()
    Expect(err).NotTo(HaveOccurred())
    Expect(cfg).NotTo(BeNil())

    // Add scheme
    err = v1alpha1.AddToScheme(scheme.Scheme)
    Expect(err).NotTo(HaveOccurred())

    k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
    Expect(err).NotTo(HaveOccurred())
    Expect(k8sClient).NotTo(BeNil())
})

var _ = AfterSuite(func() {
    cancel()
    By("tearing down the test environment")
    err := testEnv.Stop()
    Expect(err).NotTo(HaveOccurred())
})
```

#### Using kind (E2E Tests)

```bash
# Create test cluster
kind create cluster --name neo4j-test --config hack/kind-config.yaml

# Install operator
make install
make deploy IMG=neo4j-operator:test

# Run E2E tests
make test-e2e CLUSTER_NAME=neo4j-test

# Cleanup
kind delete cluster --name neo4j-test
```

### CI/CD Testing

#### GitHub Actions Integration

```yaml
name: Tests
on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    - name: Run unit tests
      run: make test-unit
    - name: Upload coverage
      uses: codecov/codecov-action@v3

  integration-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    - name: Run integration tests
      run: make test-integration

  e2e-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    - name: Create kind cluster
      uses: helm/kind-action@v1.4.0
    - name: Run E2E tests
      run: make test-e2e
```

## Advanced Testing

### Performance Testing

#### Load Testing

```go
func TestAutoScalerPerformance(t *testing.T) {
    scaler := NewAutoScaler(fake.NewClientBuilder().Build())

    // Create test clusters
    clusters := make([]*v1alpha1.Neo4jEnterpriseCluster, 100)
    for i := 0; i < 100; i++ {
        clusters[i] = createTestCluster(fmt.Sprintf("cluster-%d", i))
    }

    // Measure performance
    start := time.Now()

    var wg sync.WaitGroup
    for _, cluster := range clusters {
        wg.Add(1)
        go func(c *v1alpha1.Neo4jEnterpriseCluster) {
            defer wg.Done()
            _, err := scaler.ReconcileAutoScaling(context.Background(), c)
            assert.NoError(t, err)
        }(cluster)
    }

    wg.Wait()
    duration := time.Since(start)

    // Performance assertions
    assert.Less(t, duration, 10*time.Second, "Processing 100 clusters should take less than 10 seconds")

    // Memory usage check
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    assert.Less(t, m.Alloc, uint64(100*1024*1024), "Memory usage should be less than 100MB")
}
```

#### Benchmarking

```go
func BenchmarkAutoScalerReconcile(b *testing.B) {
    scaler := NewAutoScaler(fake.NewClientBuilder().Build())
    cluster := createTestCluster("benchmark-cluster")

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := scaler.ReconcileAutoScaling(context.Background(), cluster)
        if err != nil {
            b.Fatal(err)
        }
    }
}

func BenchmarkMetricsCollection(b *testing.B) {
    collector := NewMetricsCollector(fake.NewClientBuilder().Build(), logr.Discard())
    cluster := createTestCluster("metrics-cluster")

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := collector.CollectMetrics(context.Background(), cluster)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

### Chaos Testing

```go
func TestClusterResilience(t *testing.T) {
    // Test cluster behavior under various failure conditions
    tests := []struct {
        name     string
        scenario func(*v1alpha1.Neo4jEnterpriseCluster)
        expected string
    }{
        {
            name: "pod deletion",
            scenario: func(cluster *v1alpha1.Neo4jEnterpriseCluster) {
                // Simulate pod deletion
                deletePod(cluster, "primary-0")
            },
            expected: "cluster should recover automatically",
        },
        {
            name: "network partition",
            scenario: func(cluster *v1alpha1.Neo4jEnterpriseCluster) {
                // Simulate network partition
                createNetworkPolicy(cluster, "deny-all")
            },
            expected: "cluster should maintain quorum",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            cluster := createTestCluster("chaos-test")

            // Apply chaos scenario
            tt.scenario(cluster)

            // Wait and verify recovery
            Eventually(func() bool {
                return isClusterHealthy(cluster)
            }, 5*time.Minute, 10*time.Second).Should(BeTrue())
        })
    }
}
```

## Best Practices

### Test Organization

#### Test Structure
```go
func TestControllerFunction(t *testing.T) {
    // Arrange - Set up test data
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    mockClient := mocks.NewMockClient(ctrl)
    mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

    // Act - Execute the function under test
    result, err := functionUnderTest(mockClient, input)

    // Assert - Verify the results
    assert.NoError(t, err)
    assert.Equal(t, expected, result)
}
```

#### Test Data Management

```go
// Use factory functions for test data
func createTestCluster(name string) *v1alpha1.Neo4jEnterpriseCluster {
    return &v1alpha1.Neo4jEnterpriseCluster{
        ObjectMeta: metav1.ObjectMeta{
            Name:      name,
            Namespace: "default",
        },
        Spec: v1alpha1.Neo4jEnterpriseClusterSpec{
            Image: v1alpha1.ImageSpec{
                Repo: "neo4j",
                Tag:  "5.26-enterprise",
            },
            Topology: v1alpha1.TopologyConfiguration{
                Primaries: 3,
            },
        },
    }
}

// Use builders for complex test data
type ClusterBuilder struct {
    cluster *v1alpha1.Neo4jEnterpriseCluster
}

func NewClusterBuilder(name string) *ClusterBuilder {
    return &ClusterBuilder{
        cluster: createTestCluster(name),
    }
}

func (b *ClusterBuilder) WithAutoScaling() *ClusterBuilder {
    b.cluster.Spec.AutoScaling = &v1alpha1.AutoScalingSpec{
        Enabled: true,
    }
    return b
}

func (b *ClusterBuilder) WithReplicas(primaries, secondaries int32) *ClusterBuilder {
    b.cluster.Spec.Topology.Primaries = primaries
    b.cluster.Spec.Topology.Secondaries = secondaries
    return b
}

func (b *ClusterBuilder) Build() *v1alpha1.Neo4jEnterpriseCluster {
    return b.cluster
}

// Usage
cluster := NewClusterBuilder("test").
    WithAutoScaling().
    WithReplicas(3, 2).
    Build()
```

### Mocking and Fakes

#### Using testify/mock

```go
type MockNeo4jClient struct {
    mock.Mock
}

func (m *MockNeo4jClient) ExecuteQuery(ctx context.Context, query string) (string, error) {
    args := m.Called(ctx, query)
    return args.String(0), args.Error(1)
}

func TestMetricsCollection(t *testing.T) {
    mockClient := new(MockNeo4jClient)
    mockClient.On("ExecuteQuery", mock.Anything, mock.AnythingOfType("string")).
        Return("result", nil)

    collector := &MetricsCollector{neo4jClient: mockClient}

    result, err := collector.CollectMetrics(context.Background(), cluster)

    assert.NoError(t, err)
    assert.NotNil(t, result)
    mockClient.AssertExpectations(t)
}
```

#### Using controller-runtime fakes

```go
func TestClusterController(t *testing.T) {
    scheme := runtime.NewScheme()
    v1alpha1.AddToScheme(scheme)

    client := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(cluster).
        Build()

    controller := &Neo4jEnterpriseClusterReconciler{
        Client: client,
        Scheme: scheme,
    }

    result, err := controller.Reconcile(context.Background(), ctrl.Request{
        NamespacedName: types.NamespacedName{
            Name:      cluster.Name,
            Namespace: cluster.Namespace,
        },
    })

    assert.NoError(t, err)
    assert.Equal(t, ctrl.Result{}, result)
}
```

## Troubleshooting

### Common Issues

#### 1. Flaky Tests

**Problem**: Tests pass sometimes but fail other times.

**Solutions**:
```go
// Use Eventually for async operations
Eventually(func() bool {
    err := k8sClient.Get(ctx, key, object)
    return err == nil && object.Status.Phase == "Ready"
}, timeout, interval).Should(BeTrue())

// Add proper cleanup
AfterEach(func() {
    Expect(k8sClient.Delete(ctx, object)).To(Succeed())
    Eventually(func() bool {
        err := k8sClient.Get(ctx, key, object)
        return errors.IsNotFound(err)
    }, timeout, interval).Should(BeTrue())
})

// Use unique names
testName := "test-" + uuid.New().String()[:8]
```

#### 2. Resource Conflicts

**Problem**: Tests interfere with each other.

**Solutions**:
```go
// Use unique namespaces
BeforeEach(func() {
    namespace = "test-" + randomString(8)
    ns := &corev1.Namespace{
        ObjectMeta: metav1.ObjectMeta{Name: namespace},
    }
    Expect(k8sClient.Create(ctx, ns)).To(Succeed())
})

// Proper resource cleanup
AfterEach(func() {
    // Delete all test resources
    Expect(k8sClient.DeleteAllOf(ctx, &v1alpha1.Neo4jEnterpriseCluster{},
        client.InNamespace(namespace))).To(Succeed())
})
```

#### 3. Test Timeouts

**Problem**: Tests timeout waiting for conditions.

**Solutions**:
```go
// Increase timeouts for complex operations
const (
    timeout  = 10 * time.Minute
    interval = 10 * time.Second
)

// Use context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

// Add debug information
Eventually(func() bool {
    err := k8sClient.Get(ctx, key, cluster)
    if err != nil {
        GinkgoWriter.Printf("Error getting cluster: %v\n", err)
        return false
    }
    GinkgoWriter.Printf("Cluster phase: %s\n", cluster.Status.Phase)
    return cluster.Status.Phase == "Ready"
}, timeout, interval).Should(BeTrue())
```

### Debugging Tests

#### Verbose Output

```bash
# Run with verbose output
go test -v ./...

# Ginkgo verbose mode
ginkgo -v ./test/...

# Debug specific test
go test -run TestSpecificFunction -v ./internal/controller/
```

#### Test Debugging

```go
// Add debug prints
func TestDebugExample(t *testing.T) {
    t.Logf("Starting test with input: %+v", input)

    result, err := functionUnderTest(input)

    t.Logf("Function returned: result=%+v, err=%v", result, err)

    assert.NoError(t, err)
}

// Use testify's require for immediate failure
func TestWithRequire(t *testing.T) {
    result, err := functionUnderTest(input)
    require.NoError(t, err) // Stops test immediately on failure
    require.NotNil(t, result)

    // Continue with assertions...
}
```

This comprehensive testing guide provides developers at all experience levels with the tools and knowledge needed to write effective tests for the Neo4j Kubernetes Operator, ensuring code quality and reliability.
