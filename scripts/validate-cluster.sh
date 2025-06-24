#!/bin/bash

# Cluster Connectivity Validation Script
# This script validates cluster connectivity and health for various cluster types

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CLUSTER_TYPE="auto"
TIMEOUT=300
VERBOSE=false
SKIP_CLEANUP=false

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] ${message}${NC}"
}

# Function to print usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Cluster Connectivity Validation Script

OPTIONS:
    -t, --type TYPE        Cluster type (kind, openshift, remote, auto)
    -n, --name NAME        Cluster name (for kind clusters)
    -s, --server SERVER    OpenShift server URL
    -k, --token TOKEN      OpenShift token
    --timeout SECONDS      Timeout for operations (default: 300)
    -v, --verbose          Enable verbose output
    --skip-cleanup         Skip cleanup operations
    -h, --help             Show this help message

EXAMPLES:
    # Validate kind cluster
    $0 --type kind --name neo4j-operator-test

    # Validate OpenShift cluster
    $0 --type openshift --server https://api.cluster.example.com:6443 --token sha256~...

    # Auto-detect cluster type
    $0 --verbose

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--type)
            CLUSTER_TYPE="$2"
            shift 2
            ;;
        -n|--name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        -s|--server)
            OPENSHIFT_SERVER="$2"
            shift 2
            ;;
        -k|--token)
            OPENSHIFT_TOKEN="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --skip-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_status $RED "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Function to detect cluster type
detect_cluster_type() {
    if command -v oc &> /dev/null && [ -n "${OPENSHIFT_SERVER:-}" ] && [ -n "${OPENSHIFT_TOKEN:-}" ]; then
        echo "openshift"
    elif command -v kind &> /dev/null && [ -n "${CLUSTER_NAME:-}" ]; then
        echo "kind"
    elif command -v kubectl &> /dev/null; then
        echo "remote"
    else
        echo "unknown"
    fi
}

# Function to validate kubectl is available
validate_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        print_status $RED "kubectl is not installed or not in PATH"
        exit 1
    fi
    print_status $GREEN "kubectl is available: $(kubectl version --client --short)"
}

# Function to validate kind cluster
validate_kind_cluster() {
    local cluster_name=${CLUSTER_NAME:-neo4j-operator-test}

    print_status $BLUE "Validating Kind cluster: $cluster_name"

    # Check if cluster exists
    if ! kind get clusters | grep -q "$cluster_name"; then
        print_status $RED "Kind cluster '$cluster_name' does not exist"
        exit 1
    fi

    # Set kubectl context
    kubectl config use-context "kind-$cluster_name"

    # Test connectivity
    print_status $BLUE "Testing cluster connectivity..."
    kubectl cluster-info

    # Wait for nodes to be ready
    print_status $BLUE "Waiting for nodes to be ready (timeout: ${TIMEOUT}s)..."
    kubectl wait --for=condition=ready nodes --all --timeout=${TIMEOUT}s

    # Check node status
    print_status $BLUE "Checking node status..."
    kubectl get nodes -o wide

    # Check system pods
    print_status $BLUE "Checking system pods..."
    kubectl get pods -n kube-system

    # Test API server health
    print_status $BLUE "Testing API server health..."
    kubectl get --raw /healthz

    print_status $GREEN "Kind cluster validation completed successfully"
}

# Function to validate OpenShift cluster
validate_openshift_cluster() {
    print_status $BLUE "Validating OpenShift cluster"

    # Check if oc is available
    if ! command -v oc &> /dev/null; then
        print_status $RED "OpenShift CLI (oc) is not installed"
        exit 1
    fi

    # Check credentials
    if [ -z "${OPENSHIFT_SERVER:-}" ] || [ -z "${OPENSHIFT_TOKEN:-}" ]; then
        print_status $RED "OpenShift credentials not provided"
        exit 1
    fi

    # Login to cluster
    print_status $BLUE "Logging into OpenShift cluster..."
    oc login --token="$OPENSHIFT_TOKEN" --server="$OPENSHIFT_SERVER"

    # Test connectivity
    print_status $BLUE "Testing cluster connectivity..."
    oc cluster-info

    # Check cluster status
    print_status $BLUE "Checking cluster status..."
    oc get nodes
    oc get projects

    # Test API server health
    print_status $BLUE "Testing API server health..."
    oc get --raw /healthz

    print_status $GREEN "OpenShift cluster validation completed successfully"
}

# Function to validate remote cluster
validate_remote_cluster() {
    print_status $BLUE "Validating remote cluster"

    # Test connectivity
    print_status $BLUE "Testing cluster connectivity..."
    kubectl cluster-info

    # Check cluster status
    print_status $BLUE "Checking cluster status..."
    kubectl get nodes -o wide

    # Check namespaces
    print_status $BLUE "Checking namespaces..."
    kubectl get namespaces

    # Test API server health
    print_status $BLUE "Testing API server health..."
    kubectl get --raw /healthz

    print_status $GREEN "Remote cluster validation completed successfully"
}

# Function to run comprehensive health checks
run_health_checks() {
    local cluster_type=$1

    print_status $BLUE "Running comprehensive health checks..."

    case $cluster_type in
        "kind"|"remote")
            # Check core components
            kubectl get pods -n kube-system --no-headers | grep -E "(kube-apiserver|kube-controller-manager|kube-scheduler|etcd)" || true

            # Check DNS
            kubectl get pods -n kube-system --no-headers | grep coredns || true

            # Check CNI
            kubectl get pods -n kube-system --no-headers | grep -E "(calico|flannel|weave)" || true
            ;;
        "openshift")
            # Check OpenShift components
            oc get pods -n openshift-apiserver --no-headers || true
            oc get pods -n openshift-controller-manager --no-headers || true
            oc get pods -n openshift-scheduler --no-headers || true
            ;;
    esac

    print_status $GREEN "Health checks completed"
}

# Function to cleanup resources
cleanup() {
    if [ "$SKIP_CLEANUP" = true ]; then
        print_status $YELLOW "Skipping cleanup as requested"
        return
    fi

    print_status $BLUE "Cleaning up resources..."

    case $CLUSTER_TYPE in
        "kind")
            if [ -n "${CLUSTER_NAME:-}" ]; then
                kind delete cluster --name "$CLUSTER_NAME" || true
            fi
            ;;
        "openshift")
            # Clean up any test resources if needed
            echo "OpenShift cleanup completed"
            ;;
        *)
            echo "No cleanup needed for cluster type: $CLUSTER_TYPE"
            ;;
    esac
}

# Main execution
main() {
    print_status $BLUE "Starting cluster connectivity validation"

    # Auto-detect cluster type if not specified
    if [ "$CLUSTER_TYPE" = "auto" ]; then
        CLUSTER_TYPE=$(detect_cluster_type)
        print_status $BLUE "Auto-detected cluster type: $CLUSTER_TYPE"
    fi

    # Validate kubectl availability
    validate_kubectl

    # Set trap for cleanup
    if [ "$SKIP_CLEANUP" = false ]; then
        trap cleanup EXIT
    fi

    # Validate cluster based on type
    case $CLUSTER_TYPE in
        "kind")
            validate_kind_cluster
            ;;
        "openshift")
            validate_openshift_cluster
            ;;
        "remote")
            validate_remote_cluster
            ;;
        "unknown")
            print_status $RED "Could not determine cluster type. Please specify with --type"
            exit 1
            ;;
        *)
            print_status $RED "Unsupported cluster type: $CLUSTER_TYPE"
            exit 1
            ;;
    esac

    # Run health checks
    run_health_checks "$CLUSTER_TYPE"

    print_status $GREEN "Cluster validation completed successfully!"
}

# Run main function
main "$@"
