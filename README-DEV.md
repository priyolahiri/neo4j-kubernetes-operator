# Neo4j Operator Development Environment

This repository contains a comprehensive development environment for the Neo4j Enterprise Operator for Kubernetes, designed to provide an excellent developer experience with modern tooling and automation.

## 🆕 Recent Features

### Topology-Aware Placement (NEW)

The operator now supports automatic distribution of Neo4j primaries across availability zones:

- **Automatic zone discovery** - Discovers available zones in the cluster
- **Smart constraint generation** - Creates appropriate Kubernetes constraints
- **Validation and monitoring** - Ensures proper distribution
- **Flexible configuration** - Supports both hard and soft constraints

**Development workflow:**
```bash
# Test topology features locally
make dev-start-multizone     # Start multi-zone kind cluster
kubectl apply -f config/samples/topology-aware-cluster.yaml
./scripts/validate-topology.sh my-cluster
```

**Key files for topology development:**
- `api/v1alpha1/neo4jenterprisecluster_types.go` - API definitions
- `internal/controller/topology_scheduler.go` - Core scheduling logic
- `internal/controller/neo4jenterprisecluster_controller.go` - Controller integration
- `docs/topology-aware-placement.md` - Complete feature documentation

## 🚀 Quick Start

### One-Command Setup

```bash
# Initialize complete development environment
make dev-init
```

This will:

- ✅ Install all development tools
- ✅ Create a kind Kubernetes cluster
- ✅ Setup git hooks and pre-commit checks
- ✅ Configure IDE settings
- ✅ Start monitoring stack

### Development Dashboard

```bash
# View development environment status
make dev-dashboard
```

## 🛠️ Development Tools & Features

### 📊 Development Dashboard

- Real-time status of cluster, services, and tools
- Quick access to common development tasks
- Environment health checks

### 🔧 Comprehensive Testing Framework

- **Unit Tests** - Fast, isolated tests
- **Integration Tests** - Controller integration testing
- **E2E Tests** - End-to-end workflow testing
- **Ginkgo BDD Tests** - Behavior-driven development

```bash
make test-all-comprehensive  # Run all tests
make test-quick-comprehensive # Quick test suite
make test-report            # Generate HTML test report
```

### 🔍 Code Quality Analysis

- **Advanced Linting** - golangci-lint, staticcheck, errcheck
- **Security Scanning** - gosec, govulncheck, gitleaks
- **Code Formatting** - gofmt, gofumpt, goimports, golines
- **Dependency Analysis** - License checking, vulnerability scanning
- **Code Metrics** - Complexity, coverage, performance

```bash
make quality                # Complete quality analysis
make quality-quick          # Quick quality checks
make quality-report         # Generate HTML quality report
```

### 🐳 Development Services

- **Neo4j Enterprise** - Single instance and cluster modes
- **Prometheus** - Metrics collection and monitoring
- **Grafana** - Dashboards and visualization
- **Redis** - Caching for development
- **MinIO** - S3-compatible storage testing
- **LocalStack** - AWS services simulation
- **Jaeger** - Distributed tracing

```bash
make dev-services           # Start core services
make dev-services-cluster   # Start Neo4j cluster
make dev-services-monitoring # Start monitoring stack
```

### 🔥 Hot Reload & Debug

- **Air** - Hot reload for Go applications
- **Delve** - Go debugger integration
- **Tilt** - Kubernetes development environment
- **VS Code** - Complete IDE integration

```bash
make dev-start              # Normal development mode
make dev-debug              # Debug mode with delve
HOT_RELOAD=true make dev-start # Hot reload mode
tilt up                     # Tilt development mode
```

### 🧪 Advanced Testing

- **Table-driven tests** - Comprehensive test coverage
- **Benchmark tests** - Performance profiling
- **Memory leak detection** - goleak integration
- **Race condition detection** - Built-in race detector
- **Parallel testing** - Faster test execution
- **Topology testing** - Validate placement across zones

```bash
# Test topology-aware placement
./scripts/validate-topology.sh my-cluster
make test-topology
```

### 📊 Performance Profiling

- **CPU Profiling** - Identify performance bottlenecks
- **Memory Profiling** - Track memory usage and leaks
- **Execution Tracing** - Detailed execution analysis
- **Blocking Profiling** - Find blocking operations
- **Mutex Profiling** - Analyze lock contention

```bash
make profile-trace          # Execution tracing
```

### 🔐 Security & Compliance

- **Pre-commit hooks** - Automated security checks
- **Secret detection** - Prevent credential leaks
- **Vulnerability scanning** - Dependency security
- **License compliance** - Legal compliance checking
- **RBAC validation** - Kubernetes security

### 📝 VS Code Integration

- **Code snippets** - Neo4j operator-specific snippets
- **Debug configurations** - Pre-configured debugging
- **Task definitions** - Integrated build and test tasks
- **Extension recommendations** - Curated extension list
- **IntelliSense** - Enhanced code completion

### 🎯 Git Workflow Enhancement

- **Conventional commits** - Standardized commit messages
- **Pre-commit hooks** - Automated quality checks
- **Commit linting** - Enforce commit standards
- **Changelog generation** - Automated release notes

## 📁 Project Structure

```text
├── scripts/                    # Development scripts
│   ├── dev-environment.sh     # Main development environment manager
│   ├── code-quality.sh        # Code quality and analysis
│   └── testing-framework.sh   # Comprehensive testing framework
├── .vscode/                   # VS Code configuration
│   ├── settings.json          # Editor settings
│   ├── tasks.json             # Build and test tasks
│   ├── launch.json            # Debug configurations
│   ├── extensions.json        # Recommended extensions
│   └── snippets.json          # Code snippets
├── dev-data/                  # Development configuration
│   ├── prometheus/            # Prometheus configuration
│   └── grafana/               # Grafana dashboards
├── docs/development/          # Development documentation
├── docker-compose.dev.yml     # Development services
├── Dockerfile.dev-tools       # Development tools container
├── .air.toml                  # Hot reload configuration
└── README-DEV.md              # This file
```

## 🌟 Key Features

### 1. **One-Command Environment Setup**

Initialize everything with a single command - no more complex setup procedures.

### 2. **Multi-Mode Development**

- Normal mode for standard development
- Debug mode with breakpoints and inspection
- Hot reload mode for rapid iteration
- Tilt mode for Kubernetes-native development

### 3. **Comprehensive Testing**

- All test types in one framework
- Parallel execution for speed
- Detailed reporting and metrics
- Unit, integration, and E2E testing

### 4. **Advanced Code Quality**

- Multiple linting tools
- Security vulnerability scanning
- Code formatting and organization
- Dependency analysis and license checking

### 5. **Full Observability**

- Prometheus metrics collection
- Grafana dashboards
- Jaeger distributed tracing
- Comprehensive logging

### 6. **IDE Integration**

- Complete VS Code setup
- Debug configurations
- Code snippets for common patterns
- Integrated tasks and commands

## 🚦 Development Workflow

### Daily Development

```bash
# 1. Start development environment
make dev-dashboard              # Check status
make dev-services               # Start services
make dev-start                  # Start operator

# 2. During development
make test-quick-comprehensive   # Run quick tests
make quality-quick             # Check code quality

# 3. Before committing
make test-all-comprehensive    # Full test suite
make quality                   # Complete quality check
git add . && git commit        # Pre-commit hooks run automatically
```

### Feature Development

```bash
# 1. Create feature branch
git checkout -b feature/your-feature

# 2. Development cycle
make dev-start                 # Start development
# ... make changes ...
make test-quick-comprehensive  # Quick validation

# 3. Pre-commit validation
make test-all-comprehensive    # Full test suite
make quality                   # Code quality check
make security                  # Security scan

# 4. Commit and push
git add .
git commit -m "feat: your feature description"
git push origin feature/your-feature
```

### Bug Fixing

```bash
# 1. Reproduce issue in debug mode
make dev-debug                 # Start with debugger

# 2. Fix and validate
make test-unit                 # Unit tests
make test-integration          # Integration tests

# 3. Regression testing
make test-all-comprehensive    # Full validation
```

## 🐛 Debugging

### Debug Mode

```bash
# Start operator in debug mode
make dev-debug

# Connect with VS Code or use CLI
dlv connect :2345
```

### Profiling

```bash
# CPU profiling
make profile-cpu

# Memory profiling
make profile-memory

# Trace profiling
make profile-trace
```

### Log Analysis

```bash
# View operator logs
make logs-operator

# View cluster logs
make logs-cluster

# View all service logs
make logs-all
```

## 🧪 Testing

### Test Categories

1. **Unit Tests**: Fast, isolated component tests
2. **Integration Tests**: Controller and Kubernetes API integration
3. **E2E Tests**: Complete workflow testing
4. **Performance Tests**: Load and stress testing
5. **Security Tests**: Security validation

### Running Tests

```bash
# Quick test suite (< 30 seconds)
make test-quick-comprehensive

# Full test suite (comprehensive)
make test-all-comprehensive

# Specific test categories
make test-unit
make test-integration
make test-e2e
make test-performance
make test-security

# Generate test reports
make test-report
```

## 🔒 Security

### Security Checks

```bash
# Full security analysis
make security

# Vulnerability scanning
make security-vulnerabilities

# Secret detection
make security-secrets

# License compliance
make security-licenses
```

### Pre-commit Security

All commits are automatically scanned for:

- Hardcoded secrets
- Vulnerable dependencies
- License violations
- Security policy compliance

## 📊 Metrics & Monitoring

### Development Metrics

- Build times and success rates
- Test execution times and coverage
- Code quality metrics
- Security scan results

### Monitoring Stack

- **Prometheus**: Metrics collection
- **Grafana**: Visualization dashboards
- **Jaeger**: Distributed tracing
- **Neo4j**: Database monitoring

```bash
# Access monitoring UIs
make monitoring-open           # Open all monitoring UIs
make monitoring-prometheus     # Open Prometheus
make monitoring-grafana        # Open Grafana
make monitoring-jaeger         # Open Jaeger
```

## 🚀 Performance

### Optimization Features

- Parallel test execution
- Incremental builds
- Cached dependencies
- Optimized container images
- Resource usage monitoring

### Performance Benchmarks

```bash
# Run performance benchmarks
make benchmark

# Profile performance hotspots
make profile-performance

# Generate performance reports
make performance-report
```

## 🤝 Contributing

See the main [CONTRIBUTING.md](CONTRIBUTING.md) for detailed contribution guidelines.

### Quick Contribution Flow

1. Fork and clone the repository
2. Run `make dev-init` to setup environment
3. Create feature branch
4. Make changes with `make dev-start`
5. Validate with `make test-all-comprehensive quality`
6. Submit pull request

## 🆘 Troubleshooting

### Common Issues

#### Development Environment Issues

```bash
# Reset development environment
make dev-cleanup-all
make dev-init

# Check environment status
make dev-dashboard
make dev-doctor
```

#### Test Issues

```bash
# Clean test cache
make test-clean

# Run tests with verbose output
make test-verbose

# Debug specific test
make test-debug TEST=YourSpecificTest
```

#### Build Issues

```bash
# Clean build cache
make clean

# Rebuild from scratch
make build-clean

# Check dependencies
make deps-check
```

## 📚 Additional Resources

- [Architecture Guide](docs/development/architecture.md)
- [Testing Guide](docs/development/testing-guide.md)
- [Developer Guide](docs/development/developer-guide.md)
- [API Reference](docs/api-reference.md)
- [Contributing Guidelines](CONTRIBUTING.md)

## 🎉 Happy Coding!

This development environment is designed to make your development experience as smooth and productive as possible. If you encounter any issues or have suggestions for improvements, please create an issue or submit a pull request. 