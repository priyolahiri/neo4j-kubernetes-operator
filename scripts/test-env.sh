#!/bin/bash
# Simple test environment manager for Neo4j Operator

set -euo pipefail

CLUSTER_NAME="neo4j-operator-test"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Required-env validator. Called by cluster() — the only subcommand that
# actually creates a Kind cluster and installs cert-manager. setup() and
# cleanup() must NOT require these vars because `make test-setup` /
# `test-cleanup` / `test-destroy` invoke the script without passing them
# (they don't create clusters; setup just makes directories + runs
# manifests, cleanup just deletes directories + tears down any existing
# cluster). Putting the `${VAR:?msg}` checks at script-top would block
# those harmless paths.
require_cluster_env() {
    # KIND_NODE_IMAGE pins the K8s version. Single source of truth = the
    # Makefile (`KIND_NODE_IMAGE` variable, passed via `make
    # test-cluster`). No fallback default here on purpose — a default
    # would silently shadow Makefile bumps and let test clusters drift
    # from dev clusters / CI / envtest.
    : "${KIND_NODE_IMAGE:?KIND_NODE_IMAGE must be set (e.g. via 'make test-cluster' or 'KIND_NODE_IMAGE=kindest/node:v1.34.0 scripts/test-env.sh cluster')}"

    # CERT_MANAGER_VERSION pins the cert-manager release applied to the
    # test cluster. Same single-source-of-truth contract as KIND_NODE_IMAGE.
    : "${CERT_MANAGER_VERSION:?CERT_MANAGER_VERSION must be set (e.g. via 'make test-cluster' or 'CERT_MANAGER_VERSION=v1.20.0 scripts/test-env.sh cluster')}"
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

cluster() {
    # Validate required env BEFORE any side effects (cluster create, etc).
    # Other subcommands (setup, cleanup, clean_cluster) don't call this —
    # they don't need the K8s / cert-manager pins.
    require_cluster_env

    log "Setting up test cluster..."

    # Clean up any existing cluster
    if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
        log "Deleting existing cluster: ${CLUSTER_NAME}"
        kind delete cluster --name "${CLUSTER_NAME}" 2>/dev/null || true
    fi

    # Create new cluster
    log "Creating cluster: ${CLUSTER_NAME} (image: ${KIND_NODE_IMAGE})"
    kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}" --wait 10m

    # Export kubeconfig
    kind export kubeconfig --name "${CLUSTER_NAME}"

    # Install cert-manager
    log "Installing cert-manager ${CERT_MANAGER_VERSION}..."
    kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=300s

    # Create self-signed ClusterIssuer for testing.
    #
    # The TLS specs issue certificates from ca-cluster-issuer, which is THREE
    # objects deep:
    #
    #   selfsigned-cluster-issuer (selfSigned)
    #     -> Certificate selfsigned-ca
    #       -> Secret selfsigned-ca-secret
    #         -> ClusterIssuer ca-cluster-issuer
    #
    # This previously waited only on the Certificate, with `|| log warning` so a
    # timeout was swallowed and the suite started anyway. Both parts were wrong:
    #
    #  1. A Ready Certificate does NOT imply a Ready ClusterIssuer — the issuer
    #     goes Ready only after cert-manager observes the resulting Secret, a
    #     further reconcile.
    #  2. Continuing past a timeout turns a setup problem into confusing red TLS
    #     specs 20 minutes later. Observed on a slow runner (main @5f5d17c,
    #     integration run 32226204116): both TLS specs failed with
    #     "IssuerNotReady: Referenced issuer does not have a Ready status
    #     condition" while every non-TLS spec passed.
    #
    # So: wait for what the tests actually depend on, with a timeout that suits a
    # loaded CI runner, and FAIL LOUDLY rather than handing back a cluster whose
    # TLS is broken.
    # The apply is RETRIED because cert-manager's validating webhook rejects
    # with `Error from server (InternalError)` for a few seconds after its pod
    # reports Ready: the readiness probe passes before the webhook endpoint is
    # actually serving, and a Deployment being Available says nothing about the
    # Service behind it having endpoints. Observed on main @d1a3a5a
    # (integration run 33762424276), where the pod met its condition at
    # 13:44:12 and the apply failed at 13:44:43 — the whole 5.26 job lost to a
    # race that clears on its own within seconds.
    #
    # Bounded, and still FAILS LOUDLY at the end: a genuinely broken issuer must
    # not be handed to the suite as red TLS specs twenty minutes later.
    log "Creating self-signed ClusterIssuer for testing..."
    issuer_applied=false
    for attempt in $(seq 1 12); do
        if kubectl apply -f "${PROJECT_ROOT}/config/dev/self-signed-issuer.yaml"; then
            issuer_applied=true
            break
        fi
        log "  attempt ${attempt}/12 failed (cert-manager webhook may not be serving yet); retrying in 10s"
        sleep 10
    done
    if [ "${issuer_applied}" != "true" ]; then
        log "ERROR: could not apply ${PROJECT_ROOT}/config/dev/self-signed-issuer.yaml after 12 attempts"
        log "       Every TLS spec depends on it; refusing to hand back a half-configured cluster."
        kubectl get pods -n cert-manager 2>&1 | tail -10 || true
        kubectl get endpoints -n cert-manager cert-manager-webhook 2>&1 | tail -5 || true
        exit 1
    fi

    log "Waiting for the bootstrap CA certificate (selfsigned-ca)..."
    if ! kubectl wait --for=condition=Ready certificate/selfsigned-ca -n cert-manager --timeout=180s; then
        log "ERROR: bootstrap CA certificate selfsigned-ca never became Ready."
        kubectl describe certificate/selfsigned-ca -n cert-manager 2>&1 | tail -30 || true
        exit 1
    fi

    log "Waiting for ca-cluster-issuer to become Ready..."
    if ! kubectl wait --for=condition=Ready clusterissuer/ca-cluster-issuer --timeout=180s; then
        log "ERROR: ca-cluster-issuer never became Ready — every TLS spec would fail against this cluster."
        kubectl get clusterissuer,certificate -A 2>&1 || true
        kubectl describe clusterissuer/ca-cluster-issuer 2>&1 | tail -30 || true
        exit 1
    fi
    log "ca-cluster-issuer is Ready."

    log "Test cluster ready!"
}

setup() {
    log "Setting up test environment..."

    # Create directories
    mkdir -p "${PROJECT_ROOT}/test-results"
    mkdir -p "${PROJECT_ROOT}/coverage"
    mkdir -p "${PROJECT_ROOT}/logs"

    # Generate manifests
    cd "${PROJECT_ROOT}"
    make manifests

    log "Test environment setup complete!"
}

cleanup() {
    log "Cleaning up test environment..."

    # Clean up directories
    rm -rf "${PROJECT_ROOT}/test-results"
    rm -rf "${PROJECT_ROOT}/coverage"
    rm -rf "${PROJECT_ROOT}/logs"
    rm -rf "${PROJECT_ROOT}/tmp"

    # Clean up files
    rm -f "${PROJECT_ROOT}/test-output.log"
    rm -f "${PROJECT_ROOT}/coverage-*.out"
    rm -f "${PROJECT_ROOT}/coverage-*.html"

    # Delete test cluster
    if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
        log "Deleting test cluster: ${CLUSTER_NAME}"
        kind delete cluster --name "${CLUSTER_NAME}" 2>/dev/null || true
    fi

    log "Test environment cleanup complete!"
}

clean_cluster() {
    log "Cleaning test cluster resources..."

    if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
        log "Switching to test cluster context..."
        kind export kubeconfig --name "${CLUSTER_NAME}"

        log "Removing operator deployment..."
        kubectl delete namespace neo4j-operator-system --ignore-not-found=true --timeout=60s

        log "Removing test resources..."
        kubectl delete namespace neo4j --ignore-not-found=true --timeout=60s

        log "Removing CRDs..."
        kubectl delete crd --selector=app.kubernetes.io/name=neo4j-operator --ignore-not-found=true

        log "Test cluster resources cleaned!"
    else
        log "Test cluster not found, skipping cleanup"
    fi
}

case "${1:-}" in
    cluster)
        cluster
        ;;
    setup)
        setup
        ;;
    cleanup)
        cleanup
        ;;
    clean-cluster)
        clean_cluster
        ;;
    *)
        echo "Usage: $0 {cluster|setup|cleanup|clean-cluster}"
        exit 1
        ;;
esac
