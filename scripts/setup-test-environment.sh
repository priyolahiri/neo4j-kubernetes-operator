#!/bin/bash

# Neo4j Operator Test Environment Setup Script
# This script sets up and validates the test environment for Neo4j Operator tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
VERBOSE=${VERBOSE:-false}
FORCE_SETUP=${FORCE_SETUP:-false}
CHECK_ONLY=${CHECK_ONLY:-false}

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to print verbose output
verbose() {
    if [[ "$VERBOSE" == "true" ]]; then
        echo -e "${BLUE}[VERBOSE] $1${NC}"
    fi
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check system requirements
check_system_requirements() {
    print_status $BLUE "🔍 Checking system requirements..."

    local requirements_met=true

    # Check Go version
    if command_exists go; then
        local go_version=$(go version | awk '{print $3}' | sed 's/go//')
        verbose "Found Go version: $go_version"

        # Check if Go version is 1.21 or higher
        if [[ "$go_version" < "1.21" ]]; then
            print_status $RED "❌ Go version $go_version is too old. Required: 1.21 or higher"
            requirements_met=false
        else
            print_status $GREEN "✅ Go version $go_version is compatible"
        fi
    else
        print_status $RED "❌ Go is not installed"
        requirements_met=false
    fi

    # Check Docker
    if command_exists docker; then
        local docker_version=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "unknown")
        verbose "Found Docker version: $docker_version"
        print_status $GREEN "✅ Docker is available"
    else
        print_status $RED "❌ Docker is not installed or not running"
        requirements_met=false
    fi

    # Check kubectl
    if command_exists kubectl; then
        local kubectl_version=$(kubectl version --client --short 2>/dev/null | awk '{print $3}' || echo "unknown")
        verbose "Found kubectl version: $kubectl_version"
        print_status $GREEN "✅ kubectl is available"
    else
        print_status $YELLOW "⚠️  kubectl is not installed (will be installed if needed)"
    fi

    # Check kind
    if command_exists kind; then
        local kind_version=$(kind version 2>/dev/null | head -1 || echo "unknown")
        verbose "Found kind version: $kind_version"
        print_status $GREEN "✅ kind is available"
    else
        print_status $YELLOW "⚠️  kind is not installed (will be installed if needed)"
    fi

    # Check available memory
    if command_exists free; then
        local available_memory=$(free -m | awk 'NR==2{printf "%.0f", $7/1024}')
        verbose "Available memory: ${available_memory}GB"

        if [[ "$available_memory" -lt 4 ]]; then
            print_status $YELLOW "⚠️  Low memory available: ${available_memory}GB (recommended: 4GB+)"
        else
            print_status $GREEN "✅ Sufficient memory available: ${available_memory}GB"
        fi
    fi

    # Check available disk space
    if command_exists df; then
        local available_disk=$(df -BG . | awk 'NR==2{print $4}' | sed 's/G//')
        verbose "Available disk space: ${available_disk}GB"

        if [[ "$available_disk" -lt 10 ]]; then
            print_status $YELLOW "⚠️  Low disk space available: ${available_disk}GB (recommended: 10GB+)"
        else
            print_status $GREEN "✅ Sufficient disk space available: ${available_disk}GB"
        fi
    fi

    if [[ "$requirements_met" == "false" ]]; then
        print_status $RED "❌ System requirements not met. Please install missing dependencies."
        return 1
    fi

    print_status $GREEN "✅ System requirements check completed"
    return 0
}

# Function to install missing tools
install_missing_tools() {
    print_status $BLUE "🔧 Installing missing tools..."

    # Install kubectl if not present
    if ! command_exists kubectl; then
        print_status $YELLOW "📦 Installing kubectl..."
        local kubectl_version="v1.30.0"
        local os=$(uname -s | tr '[:upper:]' '[:lower:]')
        local arch=$(uname -m)

        if [[ "$arch" == "x86_64" ]]; then
            arch="amd64"
        elif [[ "$arch" == "aarch64" ]]; then
            arch="arm64"
        fi

        curl -LO "https://dl.k8s.io/release/${kubectl_version}/bin/${os}/${arch}/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/
        print_status $GREEN "✅ kubectl installed"
    fi

    # Install kind if not present
    if ! command_exists kind; then
        print_status $YELLOW "📦 Installing kind..."
        local kind_version="v0.22.0"
        local os=$(uname -s | tr '[:upper:]' '[:lower:]')
        local arch=$(uname -m)

        if [[ "$arch" == "x86_64" ]]; then
            arch="amd64"
        elif [[ "$arch" == "aarch64" ]]; then
            arch="arm64"
        fi

        curl -Lo ./kind "https://kind.sigs.k8s.io/dl/${kind_version}/kind-${os}-${arch}"
        chmod +x kind
        sudo mv kind /usr/local/bin/
        print_status $GREEN "✅ kind installed"
    fi

    # Install ginkgo if not present
    if ! command_exists ginkgo; then
        print_status $YELLOW "📦 Installing ginkgo..."
        go install github.com/onsi/ginkgo/v2/ginkgo@latest
        print_status $GREEN "✅ ginkgo installed"
    fi
}

# Function to setup test environment
setup_test_environment() {
    print_status $BLUE "🚀 Setting up test environment..."

    # Create necessary directories
    verbose "Creating test directories..."
    mkdir -p "$PROJECT_ROOT/test-results"
    mkdir -p "$PROJECT_ROOT/coverage"
    mkdir -p "$PROJECT_ROOT/logs"
    mkdir -p "$PROJECT_ROOT/tmp"

    # Set up environment variables
    export TEST_MODE=true
    export TEST_TIMEOUT=10m
    export TEST_PARALLEL_JOBS=2
    export TEST_VERBOSE=false
    export TEST_CLEANUP_ON_FAILURE=true

    # Generate manifests
    verbose "Generating manifests..."
    cd "$PROJECT_ROOT"
    make manifests

    print_status $GREEN "✅ Test environment setup completed"
}

# Function to validate test environment
validate_test_environment() {
    print_status $BLUE "🔍 Validating test environment..."

    local validation_passed=true

    # Check if project structure is correct
    if [[ ! -f "$PROJECT_ROOT/Makefile" ]]; then
        print_status $RED "❌ Makefile not found in project root"
        validation_passed=false
    fi

    if [[ ! -d "$PROJECT_ROOT/test/integration" ]]; then
        print_status $RED "❌ Integration test directory not found"
        validation_passed=false
    fi

    if [[ ! -d "$PROJECT_ROOT/config/crd/bases" ]]; then
        print_status $RED "❌ CRD bases directory not found"
        validation_passed=false
    fi

    # Check if CRDs exist
    local crd_count=$(find "$PROJECT_ROOT/config/crd/bases" -name "*.yaml" 2>/dev/null | wc -l)
    if [[ "$crd_count" -eq 0 ]]; then
        print_status $YELLOW "⚠️  No CRD files found in config/crd/bases"
    else
        verbose "Found $crd_count CRD files"
        print_status $GREEN "✅ CRD files found"
    fi

    # Check if test scripts exist
    if [[ ! -f "$PROJECT_ROOT/scripts/run-tests.sh" ]]; then
        print_status $YELLOW "⚠️  Unified test runner script not found"
    else
        print_status $GREEN "✅ Test runner scripts found"
    fi

    # Check if webhook-enabled test configuration exists
    if [[ ! -f "$PROJECT_ROOT/config/test-with-webhooks/kustomization.yaml" ]]; then
        print_status $YELLOW "⚠️  Webhook-enabled test configuration not found"
    else
        print_status $GREEN "✅ Webhook-enabled test configuration found"
    fi

    # Check if Go modules are properly set up
    if [[ ! -f "$PROJECT_ROOT/go.mod" ]]; then
        print_status $RED "❌ go.mod file not found"
        validation_passed=false
    else
        print_status $GREEN "✅ Go modules configured"
    fi

    # Check if dependencies are downloaded
    if [[ ! -d "$PROJECT_ROOT/vendor" ]] && [[ ! -f "$PROJECT_ROOT/go.sum" ]]; then
        print_status $YELLOW "⚠️  Go dependencies not downloaded, running go mod download..."
        cd "$PROJECT_ROOT"
        go mod download
    fi

    if [[ "$validation_passed" == "false" ]]; then
        print_status $RED "❌ Test environment validation failed"
        return 1
    fi

    print_status $GREEN "✅ Test environment validation completed"
    return 0
}

# Function to clean up test environment
cleanup_test_environment() {
    print_status $BLUE "🧹 Cleaning up test environment..."

    # Remove test artifacts
    verbose "Removing test artifacts..."
    rm -rf "$PROJECT_ROOT/test-results"/*
    rm -rf "$PROJECT_ROOT/coverage"/*
    rm -rf "$PROJECT_ROOT/logs"/*
    rm -rf "$PROJECT_ROOT/tmp"/*

    # Remove test output files
    rm -f "$PROJECT_ROOT/test-output.log"
    rm -f "$PROJECT_ROOT/coverage-integration.out"
    rm -f "$PROJECT_ROOT/coverage-integration.html"

    # Clean up any existing test clusters
    if command_exists kind; then
        verbose "Cleaning up existing kind clusters..."
        kind delete cluster --name neo4j-operator-test 2>/dev/null || true
        kind delete cluster --name neo4j-operator-dev 2>/dev/null || true
    fi

    print_status $GREEN "✅ Test environment cleanup completed"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS] COMMAND"
    echo ""
    echo "Commands:"
    echo "  setup     - Set up the test environment"
    echo "  check     - Check system requirements and validate environment"
    echo "  cleanup   - Clean up test artifacts and clusters"
    echo "  validate  - Validate the test environment"
    echo ""
    echo "Options:"
    echo "  -v, --verbose    - Enable verbose output"
    echo "  -f, --force      - Force setup even if environment exists"
    echo "  -h, --help       - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 setup                    # Set up test environment"
    echo "  $0 check                    # Check requirements"
    echo "  $0 setup --verbose          # Set up with verbose output"
    echo "  $0 cleanup --force          # Force cleanup"
}

# Main function
main() {
    local command=""

    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            setup|check|cleanup|validate)
                command="$1"
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -f|--force)
                FORCE_SETUP=true
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done

    if [[ -z "$command" ]]; then
        echo "Error: No command specified"
        show_usage
        exit 1
    fi

    # Change to project root
    cd "$PROJECT_ROOT"

    case "$command" in
        setup)
            print_status $BLUE "🚀 Setting up Neo4j Operator test environment..."
            check_system_requirements
            install_missing_tools
            setup_test_environment
            validate_test_environment
            print_status $GREEN "🎉 Test environment setup completed successfully!"
            ;;
        check)
            print_status $BLUE "🔍 Checking Neo4j Operator test environment..."
            check_system_requirements
            validate_test_environment
            print_status $GREEN "✅ Environment check completed!"
            ;;
        cleanup)
            print_status $BLUE "🧹 Cleaning up Neo4j Operator test environment..."
            cleanup_test_environment
            print_status $GREEN "✅ Environment cleanup completed!"
            ;;
        validate)
            print_status $BLUE "🔍 Validating Neo4j Operator test environment..."
            validate_test_environment
            print_status $GREEN "✅ Environment validation completed!"
            ;;
        *)
            echo "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
