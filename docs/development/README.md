# Neo4j Operator Development Guide

Welcome to the Neo4j Kubernetes Operator development guide! This comprehensive documentation helps developers at all levels contribute to the operator codebase.

## 🎯 Quick Navigation

| I want to... | Go to... | Time Required |
|--------------|----------|---------------|
| **Get started quickly** | [Quick Start](#-quick-start) | 5 minutes |
| **Understand the architecture** | [Architecture Overview](#-architecture-overview) | 15 minutes |
| **Write my first test** | [Testing Guide](#-testing-guide) | 10 minutes |
| **Optimize development workflow** | [Development Optimization](#-development-optimization) | 5 minutes |
| **Ensure code quality** | [Code Quality](#-code-quality--static-analysis) | 5 minutes |
| **Debug performance issues** | [Performance Optimization](#-performance-optimization) | 20 minutes |

## 📋 Development Paths

### 🟢 **Getting Started Path** (New to Kubernetes/Operators)
- **Start here**: [Quick Start](#-quick-start) → [Architecture Basics](#architecture-basics) → [Your First Contribution](#your-first-contribution)
- **Focus on**: Understanding concepts, following established patterns, learning best practices
- **Tools**: Use provided scripts and make targets, follow step-by-step guides

### 🟡 **Feature Development Path** (Some Kubernetes experience)
- **Start here**: [Quick Start](#-quick-start) → [Testing Guide](#-testing-guide) → [Advanced Development](#advanced-development)
- **Focus on**: Feature development, testing strategies, optimization techniques
- **Tools**: Custom development workflows, performance profiling, advanced debugging

### 🔴 **Architecture & Optimization Path** (Kubernetes/Operator experience)
- **Start here**: [Architecture Deep Dive](#architecture-deep-dive) → [Performance Optimization](#-performance-optimization) → [Contributing Guidelines](#contributing-guidelines)
- **Focus on**: Architecture decisions, performance optimization, mentoring others
- **Tools**: Custom tooling, advanced profiling, system design

## 🚀 Quick Start

### Prerequisites

**Required Tools:**
- **Go 1.21+** - [Install Go](https://golang.org/doc/install)
- **Docker** - [Install Docker](https://docs.docker.com/get-docker/)
- **kubectl** - [Install kubectl](https://kubernetes.io/docs/tasks/tools/)
- **kind** - For local Kubernetes clusters: `go install sigs.k8s.io/kind@latest`

**Optional but Recommended:**
- **Git** - Version control
- **VS Code** with Go extension - IDE setup
- **k9s** - Kubernetes CLI dashboard: `brew install k9s`

### One-Command Setup

```bash
# 🚀 Initialize complete development environment
make dev-init
```

**What this does:**
- ✅ Installs all development tools
- ✅ Creates a kind Kubernetes cluster
- ✅ Sets up git hooks and pre-commit checks
- ✅ Configures IDE settings
- ✅ Starts monitoring stack (Prometheus, Grafana)

### Development Modes

Choose your development mode based on your needs:

#### 🟢 **Guided Mode** (Recommended for newcomers)
```bash
make dev-start-guided
```
- Step-by-step startup with explanations
- Built-in help and tips
- Automatic error detection and suggestions

#### ⚡ **Fast Mode** (Daily development)
```bash
make dev-start-fast
```
- 5-10 second startup time
- Essential controllers only
- Perfect for feature development

#### 🔧 **Full Mode** (Complete testing)
```bash
make dev-start
```
- All controllers and features
- Production-like environment
- Comprehensive testing capabilities

### Your First Contribution

**Getting Started with Contributions:**

1. **Find an appropriate issue**:
   ```bash
   # Look for issues labeled "good first issue" or "help wanted"
   open https://github.com/neo4j-labs/neo4j-kubernetes-operator/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22
   ```

2. **Set up development environment**:
   ```bash
   git clone https://github.com/neo4j-labs/neo4j-kubernetes-operator.git
   cd neo4j-kubernetes-operator
   make dev-init
   ```

3. **Make your changes**:
   ```bash
   # Create a feature branch
   git checkout -b fix/my-first-fix

   # Make your changes
   # ... edit files ...

   # Test your changes
   make test-quick

   # Check code quality
   make lint-fix
   ```

4. **Submit your contribution**:
   ```bash
   git add .
   git commit -m "fix: description of your fix"
   git push origin fix/my-first-fix
   # Create PR through GitHub UI
   ```

## 🏗️ Architecture Overview

### Architecture Basics

The Neo4j Operator follows the **Kubernetes Operator Pattern**:

```
User creates → Custom Resource → Controller watches → Creates/Updates → Kubernetes Resources
     ↓              ↓                    ↓                    ↓                    ↓
   YAML file    Neo4jCluster      Cluster Controller     StatefulSets      Running Neo4j Pods
```

**Key Concepts:**
- **Custom Resources (CRs)**: YAML definitions of what you want (like `Neo4jEnterpriseCluster`)
- **Controllers**: Go code that makes it happen (reads CRs, creates Kubernetes resources)
- **Reconciliation**: Continuously ensuring actual state matches desired state

### Architecture Deep Dive

**System Architecture:**

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
│  │  │- Cluster    │ │    │  - Neo4jUser/Role/Grant       │ │
│  │  │- Database   │ │    └─────────────────────────────────┘ │
│  │  │- Backup     │ │                                      │
│  │  │- Security   │ │    ┌─────────────────────────────────┐ │
│  │  │- AutoScale  │ │    │        Neo4j Resources          │ │
│  │  └─────────────┘ │    │  - StatefulSets                │ │
│  │                 │    │  - Services                    │ │
│  │  ┌─────────────┐ │    │  - ConfigMaps/Secrets         │ │
│  │  │Webhooks     │ │    │  - PVCs/NetworkPolicies       │ │
│  │  │- Validation │ │    └─────────────────────────────────┘ │
│  │  │- Mutation   │ │                                      │
│  │  └─────────────┘ │                                      │
│  └─────────────────┘                                      │
└─────────────────────────────────────────────────────────────┘
```

**Core Components:**

1. **Manager**: Main process hosting controllers and webhooks
2. **Controllers**: Reconciliation logic for each resource type
3. **Webhooks**: Validation and mutation of resources
4. **Cache System**: Optimized Kubernetes API interaction

**Key Files:**
- `cmd/main.go` - Entry point and manager setup
- `api/v1alpha1/` - Custom Resource Definitions
- `internal/controller/` - Controller implementations
- `internal/webhooks/` - Admission webhooks

## 🧪 Testing Guide

### Testing Philosophy

We follow the **testing pyramid**:

```
        🔺 E2E Tests (10%)
       🔺🔺 Integration Tests (20%)
    🔺🔺🔺🔺 Unit Tests (70%)
```

### Quick Testing Commands

```bash
# 🚀 Run all tests (comprehensive)
make test-all

# ⚡ Quick test suite (for daily development)
make test-quick

# 🔍 Specific test types
make test-unit          # Unit tests only
make test-integration   # Integration tests only
make test-e2e           # End-to-end tests only

# 📊 With coverage report
make coverage
```

### Writing Your First Test

**Basic Unit Test Example**:
```go
func TestNewAutoScaler(t *testing.T) {
    // Arrange
    client := fake.NewClientBuilder().Build()

    // Act
    scaler := NewAutoScaler(client)

    // Assert
    assert.NotNil(t, scaler)
    assert.Equal(t, client, scaler.client)
}
```

**Integration Test Example**:
```go
var _ = Describe("Neo4jCluster Controller", func() {
    It("should create a cluster successfully", func() {
        cluster := &v1alpha1.Neo4jEnterpriseCluster{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "test-cluster",
                Namespace: "default",
            },
            Spec: v1alpha1.Neo4jEnterpriseClusterSpec{
                // ... spec
            },
        }

        Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

        Eventually(func() bool {
            // Check cluster is ready
            return cluster.Status.Phase == "Ready"
        }, timeout, interval).Should(BeTrue())
    })
})
```

### Advanced Testing

**For complex scenarios:**

- **Performance Testing**: `make test-performance`
- **Chaos Testing**: `make test-chaos`
- **Security Testing**: `make test-security`
- **Load Testing**: `make test-load`

## 🔧 Development Optimization

### Startup Time Optimization

**The Challenge**: Traditional operator startup takes 60+ seconds due to informer cache synchronization.

**Our Solution**: Multiple caching strategies:

#### ⚡ Ultra-Fast Mode (1-3 seconds)
```bash
make dev-start-minimal
```
- **Best for**: Daily development, quick testing
- **Trade-off**: Slightly slower individual operations
- **How**: Bypasses all informer caching, uses direct API calls

#### 🚀 Fast Mode (5-10 seconds)
```bash
make dev-start-fast
```
- **Best for**: Feature development, debugging
- **Trade-off**: Balanced performance
- **How**: Lazy cache loading, selective resource watching

#### 🔧 Full Mode (15-25 seconds)
```bash
make dev-start
```
- **Best for**: Integration testing, production simulation
- **Trade-off**: Slower startup, full functionality
- **How**: Complete cache prewarming, all controllers

### Performance Comparison

| Mode | Startup | Memory | API Load | Controllers | Best For |
|------|---------|--------|----------|-------------|----------|
| **Ultra-Fast** | 1-3s | Very Low | High | Essential | Daily dev |
| **Fast** | 5-10s | Low | Medium | Core | Feature dev |
| **Full** | 15-25s | Medium | Low | All | Testing |

### Hot Reload Development

```bash
# Terminal 1: Start services
make dev-services

# Terminal 2: Start operator with hot reload
HOT_RELOAD=true make dev-start
```

**Benefits:**
- Automatic restart on code changes
- Preserves cluster state
- Faster iteration cycles

### Development Dashboard

```bash
# Real-time development environment status
make dev-dashboard
```

**Provides:**
- Cluster health status
- Service endpoints
- Resource usage
- Quick action buttons

## 🔍 Code Quality & Static Analysis

### Quick Quality Checks

```bash
# 🚀 One-command quality check
make quality

# ⚡ Quick checks (for daily development)
make lint-fix

# 🔍 Comprehensive analysis
make lint-comprehensive
```

### Static Analysis Tools

#### golangci-lint v2.1.6
- **45+ modern linters** for comprehensive analysis
- **Security detection**: Unicode attacks, unsafe operations
- **Performance optimization**: Memory leaks, inefficient patterns
- **Modern Go patterns**: Go 1.22+ features, best practices

#### staticcheck 2025.1.1
- **Advanced static analysis** for Go
- **Unused code detection**
- **Bug pattern recognition**
- **Performance issue identification**

### Pre-commit Hooks

```bash
# Install pre-commit hooks
make install-hooks

# Manual run
pre-commit run --all-files
```

**Automatically runs:**
- Code formatting (gofmt, goimports)
- Linting (golangci-lint)
- Static analysis (staticcheck)
- Security checks

### Code Quality Metrics

```bash
# Generate comprehensive quality report
make quality-report

# View code metrics
make metrics
```

## ⚡ Performance Optimization

### Operator Performance Features

#### Connection Pool Management
- **Circuit Breaker Pattern**: Prevents cascade failures
- **Optimized Pool Sizing**: 20 connections for memory efficiency
- **Smart Timeouts**: 5-second acquisition, 10-second query timeouts
- **Benefits**: 60% reduction in memory usage, improved reliability

#### Controller Memory Optimization
- **Object Reuse**: Kubernetes objects are pooled and reused
- **Connection Caching**: Cached Neo4j client connections
- **Rate Limiting**: Controlled concurrent reconciliation
- **Benefits**: 70% reduction in GC frequency, better performance

#### Cache Management
- **Selective Watching**: Only operator-managed resources
- **Label-Based Filtering**: 85% reduction in cached objects
- **Memory-Aware GC**: Threshold-based garbage collection
- **Benefits**: Lower memory usage, faster reconciliation

### Performance Profiling

```bash
# CPU profiling
make profile-cpu

# Memory profiling
make profile-memory

# Goroutine analysis
make profile-goroutines

# Generate performance report
make performance-report
```

### Benchmarking

```bash
# Run performance benchmarks
make benchmark

# Compare with baseline
make benchmark-compare

# Load testing
make load-test
```

## 🛠️ Advanced Development

### Custom Controller Development

**Controller Structure**:
```go
type MyController struct {
    client.Client
    Scheme *runtime.Scheme
    logger logr.Logger
}

func (r *MyController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // Reconciliation logic
}
```

**Advanced Patterns**:
- **Finalizers**: Resource cleanup
- **Owner References**: Resource relationships
- **Status Conditions**: State communication
- **Events**: User feedback

### Debugging Techniques

#### Local Debugging
```bash
# Debug mode with delve
make debug

# Debug specific controller
DEBUG_CONTROLLER=cluster make debug
```

#### Remote Debugging
```bash
# Debug operator in cluster
make debug-remote

# Port forward for debugging
kubectl port-forward deployment/neo4j-operator 40000:40000
```

#### Troubleshooting Tools
```bash
# Operator logs
make logs

# Event analysis
make events

# Resource status
make status

# Health check
make health
```

### Contributing Guidelines

#### Code Style
- Follow Go conventions and best practices
- Use meaningful variable and function names
- Add comprehensive comments for public APIs
- Write tests for all new functionality

#### Pull Request Process
1. **Fork and clone** the repository
2. **Create feature branch** with descriptive name
3. **Write tests** for your changes
4. **Run quality checks** (`make quality`)
5. **Update documentation** if needed
6. **Submit PR** with clear description

#### Review Criteria
- **Functionality**: Does it work as intended?
- **Testing**: Adequate test coverage?
- **Performance**: No performance regressions?
- **Documentation**: Clear and up-to-date?
- **Code Quality**: Follows project standards?

## 📚 Additional Resources

### Documentation
- [API Reference](../api-reference.md) - Complete API documentation
- [User Guides](../README.md) - User-facing documentation
- [Architecture Deep Dive](architecture.md) - Detailed system design
- [Testing Guide](testing.md) - Comprehensive testing documentation
- [Performance Guide](performance.md) - Performance optimization details

### External Resources
- [Kubernetes Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Controller Runtime](https://pkg.go.dev/sigs.k8s.io/controller-runtime)
- [Kubebuilder Book](https://book.kubebuilder.io/)
- [Neo4j Documentation](https://neo4j.com/docs/)

### Community
- [GitHub Issues](https://github.com/neo4j-labs/neo4j-kubernetes-operator/issues) - Bug reports and feature requests
- [GitHub Discussions](https://github.com/neo4j-labs/neo4j-kubernetes-operator/discussions) - Community discussions
- [Neo4j Community Forum](https://community.neo4j.com/) - General Neo4j support

---

**Ready to contribute?** Start with the [Quick Start](#-quick-start) section and follow the development path that matches your experience level. We welcome contributions from developers at all skill levels!
