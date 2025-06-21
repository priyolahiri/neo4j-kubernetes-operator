# Quick Start Guide

This guide helps you get started with Neo4j Operator development in under 5 minutes.

## 🚀 One-Line Setup

```bash
make setup-dev && make install-hooks && make dev-cluster && make install
```

## 📋 Essential Commands

### Development Setup

```bash
make setup-dev         # Install all development tools
make install-hooks     # Install pre-commit hooks
make dev-cluster       # Create Kind development cluster
make install           # Install CRDs
```

### Daily Development

```bash
make dev-run           # Run operator locally
make dev-run-hot       # Run with hot reload
make dev-run-debug     # Run with debugger
make test              # Run tests
make lint              # Run linter
make fmt               # Format code
```

### Testing

```bash
make test-unit         # Unit tests only
make test-integration  # Integration tests
make test-e2e          # End-to-end tests
make test-samples      # Test sample configurations
make coverage          # Generate coverage report
```

### Cleanup

```bash
make dev-cleanup       # Clean development files
make dev-cleanup-all   # Clean everything including cluster
make clean             # Clean build artifacts
```

## 🛠️ Advanced Development

### Using Tilt (Recommended)

```bash
# Install Tilt
curl -fsSL https://raw.githubusercontent.com/tilt-dev/tilt/master/scripts/install.sh | bash

# Start development environment
tilt up

# Access Tilt UI
open http://localhost:10350
```

### Debug Mode

```bash
# Local debugging
make debug

# Connect with VS Code or:
dlv connect :2345
```

### Hot Reload Development

```bash
# Option 1: Using our script
make dev-run-hot

# Option 2: Using Tilt
tilt up
```

### Webhook Development

```bash
make dev-run-webhooks
```

## 📁 Project Structure

```text
neo4j-operator/
├── api/v1alpha1/          # CRD definitions
├── cmd/                   # Main entry point
├── internal/
│   ├── controller/        # Controllers
│   ├── resources/         # Resource builders
│   ├── neo4j/            # Neo4j client
│   └── webhooks/         # Admission webhooks
├── config/               # Kubernetes manifests
├── test/                 # Tests
├── hack/                 # Development scripts
└── docs/                 # Documentation
```

## 🧪 Testing Your Changes

### 1. Basic Validation

```bash
make fmt lint vet test
```

### 2. Test Sample Configurations

```bash
make test-samples
```

### 3. Manual Testing

```bash
# Create a basic cluster
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: neo4j-auth
  namespace: default
type: Opaque
data:
  password: bmVvNGpwYXNzd29yZA==  # neo4jpassword
---
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: quickstart-cluster
  namespace: default
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 0
  storage:
    className: "standard"
    size: "10Gi"
  auth:
    provider: native
    secretRef: neo4j-auth
EOF

# Check status
kubectl get neo4jenterprisecluster quickstart-cluster -o yaml

# Port forward to access Neo4j
kubectl port-forward service/quickstart-cluster-client 7474:7474 7687:7687

# Access Neo4j Browser at http://localhost:7474
```

## 🔍 Debugging Common Issues

### Operator Not Starting

```bash
# Check operator logs
kubectl logs deployment/neo4j-operator-controller-manager -n neo4j-operator-system -f

# Check if CRDs are installed
kubectl get crd | grep neo4j

# Check RBAC permissions
kubectl auth can-i create statefulsets --as=system:serviceaccount:neo4j-operator-system:neo4j-operator-controller-manager
```

### Cluster Not Creating

```bash
# Check cluster status
kubectl describe neo4jenterprisecluster quickstart-cluster

# Check pod status
kubectl get pods -l app.kubernetes.io/name=quickstart-cluster

# Check events
kubectl get events --sort-by=.metadata.creationTimestamp
```

### Build Issues

```bash
# Clean and rebuild
make clean
go mod tidy
make build
```

## 📊 Monitoring & Metrics

### Local Development

```bash
# Metrics endpoint
curl http://localhost:8080/metrics

# Health check
curl http://localhost:8081/healthz

# pprof (debug mode)
go tool pprof http://localhost:6060/debug/pprof/profile
```

## 🚀 Next Steps

After completing this quickstart:

1. Read the [Development Guide](development.md) for detailed development workflows
2. Check out [Testing Guide](development/testing-guide.md) for comprehensive testing
3. Explore [API Reference](api-reference.md) for complete API documentation
4. Join our [community discussions](https://github.com/neo4j-labs/neo4j-operator/discussions)

## 📝 Additional Resources

- [Architecture Overview](development/architecture.md)
- [Contributing Guidelines](../CONTRIBUTING.md)
- [Performance Optimizations](performance-optimizations.md)
- [Troubleshooting Guide](development.md#troubleshooting) 