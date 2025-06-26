# Neo4j Kubernetes Operator Testing Guide

This document provides comprehensive information about the testing infrastructure for the Neo4j Kubernetes Operator.

## Table of Contents

- [Overview](#overview)
- [Test Types](#test-types)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Test Environment Setup](#test-environment-setup)
- [Running Tests](#running-tests)
- [Test Scripts](#test-scripts)
- [CI/CD Integration](#cicd-integration)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

The Neo4j Kubernetes Operator includes a comprehensive testing infrastructure designed to ensure reliability, stability, and correctness of the operator functionality. The testing framework supports multiple test types, parallel execution, coverage reporting, and automated CI/CD integration.

## Test Types

### 1. Unit Tests
- **Purpose**: Test individual functions and components in isolation
- **Location**: `./internal/...`
- **Execution**: Fast, no external dependencies
- **Coverage**: High code coverage for core logic

### 2. Integration Tests
- **Purpose**: Test component interactions and Kubernetes API integration
- **Location**: `./test/integration/`
- **Execution**: Requires Kubernetes cluster (kind/minikube)
- **Coverage**: API interactions, CRD operations, controller logic

### 3. End-to-End (E2E) Tests
- **Purpose**: Test complete operator workflows and user scenarios
- **Location**: `./test/e2e/`
- **Execution**: Full Kubernetes environment, longer runtime
- **Coverage**: Complete user workflows, edge cases

### 4. Simple Integration Tests
- **Purpose**: Quick validation of basic operator functionality
- **Location**: `./test/integration/`
- **Execution**: Lightweight, focused on core features
- **Coverage**: Essential operator features

### 5. Smoke Tests
- **Purpose**: Basic functionality verification
- **Location**: `./test/integration/`
- **Execution**: Minimal setup, fast execution
- **Coverage**: Critical path validation

## Prerequisites

### System Requirements
- **Go**: 1.21 or higher
- **Docker**: Running Docker daemon
- **kubectl**: Kubernetes command-line tool
- **kind**: Kubernetes in Docker (for local testing)
- **Memory**: 4GB+ available RAM
- **Disk**: 10GB+ available space

### Optional Tools
- **ginkgo**: Test framework (auto-installed if missing)
- **helm**: Package manager (for some test scenarios)

## Quick Start

### 1. Setup Test Environment
```bash
# Setup test environment
./scripts/setup-test-environment.sh setup

# Or check current environment
./scripts/setup-test-environment.sh check
```

### 2. Run All Tests
```bash
# Run all test types
./scripts/run-tests.sh

# Run with coverage
./scripts/run-tests.sh --coverage

# Run with verbose output
./scripts/run-tests.sh --verbose
```

### 3. Run Specific Test Types
```bash
# Unit tests only
./scripts/run-tests.sh unit

# Integration tests only
./scripts/run-tests.sh integration

# E2E tests only
./scripts/run-tests.sh e2e

# Simple integration tests
./scripts/run-tests.sh simple

# Smoke tests
./scripts/run-tests.sh smoke
```

## Test Environment Setup

### Automated Setup
The test environment setup script handles:
- System requirement validation
- Tool installation (kubectl, kind, ginkgo)
- Directory creation
- Environment variable configuration
- Manifest generation

```bash
./scripts/setup-test-environment.sh setup --verbose
```

### Manual Setup
If you prefer manual setup:

1. **Install Dependencies**
   ```bash
   # Install kubectl
   curl -LO "https://dl.k8s.io/release/v1.30.0/bin/$(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m | sed 's/x86_64/amd64/')/kubectl"
   chmod +x kubectl
   sudo mv kubectl /usr/local/bin/

   # Install kind
   curl -Lo ./kind "https://kind.sigs.k8s.io/dl/v0.22.0/kind-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m | sed 's/x86_64/amd64/')"
   chmod +x kind
   sudo mv kind /usr/local/bin/

   # Install ginkgo
   go install github.com/onsi/ginkgo/v2/ginkgo@latest
   ```

2. **Create Test Cluster**
   ```bash
   # Create kind cluster
   kind create cluster --name neo4j-operator-test

   # Or use existing cluster
   kubectl cluster-info
   ```

3. **Install CRDs**
   ```bash
   make install
   ```

4. **Deploy Operator**
   ```bash
   make deploy-test-with-webhooks
   ```

## Running Tests

### Test Runner Script
The main test runner provides a unified interface:

```bash
# Basic usage
./scripts/run-tests.sh [TEST_TYPE] [OPTIONS]

# Examples
./scripts/run-tests.sh                    # Run all tests
./scripts/run-tests.sh integration        # Run integration tests
./scripts/run-tests.sh --coverage --verbose  # Run with coverage and verbose output
./scripts/run-tests.sh e2e --timeout 30m  # Run e2e tests with 30min timeout
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-v, --verbose` | Enable verbose output | false |
| `-p, --parallel` | Run tests in parallel | false |
| `-c, --coverage` | Generate coverage reports | false |
| `--no-cleanup` | Skip cleanup after tests | false |
| `--no-setup` | Skip test environment setup | false |
| `--fail-fast` | Stop on first failure | false |
| `--retain-logs` | Keep test logs after completion | false |
| `--timeout DURATION` | Set test timeout | 10m |
| `--namespace NAME` | Set test namespace | neo4j-operator-system |

### Individual Test Scripts

#### Integration Tests
```bash
# Run full integration test suite with webhooks and cert-manager
make test-integration

# Run integration tests directly
go test -v ./test/integration/...

# Run simple integration tests
go test -v ./test/integration/... -ginkgo.focus="Simple"

# Run smoke tests
go test -v ./test/integration/... -ginkgo.focus="Smoke"
```

#### Unit Tests
```bash
# Run all unit tests
go test ./...

# Run with coverage
go test -coverprofile=coverage.out ./...

# Run specific package
go test ./internal/controller/...
```

#### E2E Tests
```bash
# Run e2e tests with webhooks and cert-manager
make test-e2e

# Run e2e tests with ginkgo
ginkgo -v -timeout=30m ./test/e2e/...

# Run with coverage
ginkgo -v -coverprofile=coverage-e2e.out ./test/e2e/...
```

## Test Scripts

### setup-test-environment.sh
Comprehensive test environment management:

```bash
# Commands
./scripts/setup-test-environment.sh setup     # Setup environment
./scripts/setup-test-environment.sh check     # Check requirements
./scripts/setup-test-environment.sh cleanup   # Cleanup environment
./scripts/setup-test-environment.sh validate  # Validate environment

# Options
./scripts/setup-test-environment.sh setup --verbose  # Verbose output
./scripts/setup-test-environment.sh setup --force    # Force setup
```

### run-tests.sh
Unified test runner for all test types with webhooks and cert-manager:

```bash
# Features
- Multiple test type support (unit, integration, e2e, smoke)
- Webhooks enabled with cert-manager for all tests
- Parallel execution
- Coverage reporting
- Comprehensive result reporting
- Signal handling and cleanup
- Configurable options

# Usage
./scripts/run-tests.sh [TEST_TYPE] [OPTIONS]

# Test Types
unit         # Unit tests only
integration  # Integration tests with webhooks
e2e          # End-to-end tests with webhooks
smoke        # Smoke tests with webhooks
all          # All test types
simple       # Simple integration tests with webhooks

# Options
-v, --verbose     # Enable verbose output
-p, --parallel    # Enable parallel execution
-c, --coverage    # Generate coverage report
--no-cleanup      # Skip cleanup
--no-setup        # Skip environment setup
--fail-fast       # Stop on first failure
--retain-logs     # Keep test logs
--timeout DURATION # Set test timeout
```

## CI/CD Integration

### GitHub Actions
The project includes optimized CI workflows with webhooks and cert-manager:

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v4
        with:
          go-version: '1.24'

      - name: Setup test environment
        run: ./scripts/setup-test-environment.sh setup

      - name: Run tests with webhooks
        run: make test-ci

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

### Local CI Simulation
```bash
# Run CI-like test suite with webhooks
make test-ci

# Run comprehensive test suite
make test-comprehensive

# Run all tests with coverage
./scripts/run-tests.sh all --coverage --verbose --timeout 30m
```

## Troubleshooting

### Common Issues

#### 1. Cluster Connection Issues
```bash
# Check cluster status
kubectl cluster-info

# Verify kind cluster
kind get clusters
kind export kubeconfig --name neo4j-operator-test

# Check nodes
kubectl get nodes
```

#### 2. CRD Installation Issues
```bash
# Check CRD status
kubectl get crd | grep neo4j

# Reinstall CRDs
make uninstall
make install

# Wait for CRDs to be established
kubectl wait --for=condition=established --timeout=60s crd/neo4jenterpriseclusters.neo4j.neo4j.com
```

#### 3. Operator Deployment Issues
```bash
# Check operator pods
kubectl get pods -n neo4j-operator-system

# Check operator logs
kubectl logs -n neo4j-operator-system deployment/controller-manager

# Check webhooks
kubectl get validatingwebhookconfigurations | grep neo4j
kubectl get mutatingwebhookconfigurations | grep neo4j

# Check cert-manager
kubectl get pods -n cert-manager
kubectl get certificates -n neo4j-operator-system

# Redeploy operator with webhooks
make deploy-test-with-webhooks
```

#### 4. Webhook Issues
```bash
# Check webhook status
kubectl get validatingwebhookconfigurations
kubectl get mutatingwebhookconfigurations

# Check webhook certificates
kubectl get certificates -n neo4j-operator-system
kubectl get secrets -n neo4j-operator-system | grep webhook

# Verify cert-manager is working
kubectl get pods -n cert-manager
kubectl logs -n cert-manager deployment/cert-manager
```

#### 5. Test Timeout Issues
```bash
# Increase timeout
./scripts/run-tests.sh --timeout 30m

# Run with verbose output for debugging
./scripts/run-tests.sh --verbose

# Check resource usage
kubectl top nodes
kubectl top pods -n neo4j-operator-system
```

#### 6. Resource Issues
```bash
# Check available resources
free -h
df -h

# Clean up resources
./scripts/setup-test-environment.sh cleanup
docker system prune -f
```

### Debug Mode
```bash
# Enable debug output
export TEST_DEBUG=true
make test-debug

# Run with verbose logging
./scripts/run-tests.sh all --verbose --retain-logs
```

### Test Artifacts
Test artifacts are stored in:
- `logs/` - Test execution logs
- `coverage/` - Coverage reports
- `test-results/` - Test result files
- `tmp/` - Temporary files

## Best Practices

### 1. Test Development
- Write focused, isolated tests
- Use descriptive test names
- Include both positive and negative test cases
- Mock external dependencies appropriately
- Use table-driven tests for multiple scenarios

### 2. Test Execution
- Run tests locally before pushing
- Use appropriate test types for changes
- Monitor test execution time
- Review coverage reports regularly
- Keep test data and fixtures up to date

### 3. CI/CD Integration
- Run full test suite on pull requests
- Use parallel execution for faster feedback
- Generate and store coverage reports
- Set appropriate timeouts for different test types
- Implement test result notifications

### 4. Environment Management
- Use isolated test environments
- Clean up resources after tests
- Version control test configurations
- Document environment requirements
- Automate environment setup

### 5. Performance Considerations
- Use appropriate resource limits
- Monitor test execution time
- Optimize slow tests
- Use parallel execution where possible
- Implement test result caching

## Contributing

When contributing to the test infrastructure:

1. **Follow existing patterns** - Maintain consistency with current test structure
2. **Add appropriate tests** - Ensure new features have corresponding tests
3. **Update documentation** - Keep this guide current with changes
4. **Test locally** - Verify changes work in your environment
5. **Consider CI impact** - Ensure changes don't significantly impact CI performance

## Support

For issues with the testing infrastructure:

1. Check the troubleshooting section above
2. Review test logs in the `logs/` directory
3. Verify your environment meets prerequisites
4. Check recent changes that might affect tests
5. Open an issue with detailed information about the problem

## Additional Resources

- [Go Testing Documentation](https://golang.org/pkg/testing/)
- [Ginkgo Testing Framework](https://onsi.github.io/ginkgo/)
- [Kubernetes Testing Best Practices](https://kubernetes.io/docs/concepts/testing/)
- [Kind Documentation](https://kind.sigs.k8s.io/)
