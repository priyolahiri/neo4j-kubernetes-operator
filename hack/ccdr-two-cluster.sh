#!/usr/bin/env bash
# Stand up the two-Kubernetes-cluster cross-cluster replication rig, with TLS
# on and mutual CA trust — release_verification.md Phase 5 Part E.
#
# Why this exists: Part E is the configuration CCDR is actually sold with, and
# it is the only one that exercises the proxy, the certificate SANs, the
# advertised-address override and cross-cluster trust together. The first time
# anyone ran it, on 2026-09-14, three defects fell out — one of which (a peer
# CA mounted inside the certs volume's own directory) made the feature
# unusable, and another of which made the resulting cluster unrepairable.
#
# It had never been run because the bring-up is fiddly: two clusters, two
# operators, MetalLB on one side only, and a CA exchange in both directions
# that has to happen AFTER cert-manager has issued each cluster's certificate.
# Thirty minutes of careful kubectl is thirty minutes nobody spends before a
# release. This makes it one command.
#
# Usage:
#   hack/ccdr-two-cluster.sh up      # create both clusters + operators + MetalLB
#   hack/ccdr-two-cluster.sh trust   # exchange CAs (run once both are Ready)
#   hack/ccdr-two-cluster.sh status  # where everything is
#   hack/ccdr-two-cluster.sh down    # delete both clusters
#
# It deliberately does NOT create the Neo4jEnterpriseClusters or the replica.
# Those are the thing under test: applying them by hand, from the published
# docs, is the point of the journey.
set -euo pipefail

UPSTREAM="${CCDR_UPSTREAM_CLUSTER:-neo4j-operator-dev}"
DOWNSTREAM="${CCDR_DOWNSTREAM_CLUSTER:-neo4j-dr}"
UP_CTX="kind-${UPSTREAM}"
DOWN_CTX="kind-${DOWNSTREAM}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.20.0}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-neo4j-operator:dev}"
OPERATOR_NS="${OPERATOR_NS:-neo4j-operator-dev}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() { printf '\033[1;36m[ccdr]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[ccdr]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[ccdr]\033[0m %s\n' "$*" >&2; exit 1; }

# cluster_config renders hack/kind-config.yaml for one cluster.
#
# That config pins `apiServerPort: 6443`, which is right for the single dev
# cluster and fatal for the second one here: both would publish 127.0.0.1:6443
# and the second create dies with "Bind for 127.0.0.1:6443 failed: port is
# already allocated". The shared config has pinned the port since the initial
# commit, and this script reused it for both clusters from the day it was
# written — so `ccdr-e2e-up` could never create the downstream, and the Part E
# walk it exists to automate was still being done by hand.
#
# The upstream keeps 6443, so existing habits and any tooling that assumes it
# still work. The downstream drops the line entirely and lets Kind pick a free
# port — no second hardcoded number to collide with something else later.
cluster_config() {
    local name="$1" out="$2"
    if [ "${name}" = "${UPSTREAM}" ]; then
        cp "${REPO_ROOT}/hack/kind-config.yaml" "${out}"
    else
        grep -v '^  apiServerPort:' "${REPO_ROOT}/hack/kind-config.yaml" > "${out}"
    fi
}

create_cluster() {
    local name="$1" ctx="kind-$1"
    if kind get clusters 2>/dev/null | grep -qx "${name}"; then
        log "${name} already exists"
    else
        log "creating ${name}"
        local cfg
        cfg="$(mktemp -t kind-config-XXXXXX.yaml)"
        cluster_config "${name}" "${cfg}"
        kind create cluster --name "${name}" --image "${KIND_NODE_IMAGE}" --config "${cfg}"
        rm -f "${cfg}"
        kubectl --context "${ctx}" wait --for=condition=ready node --all --timeout=300s
    fi

    log "${name}: cert-manager ${CERT_MANAGER_VERSION}"
    kubectl --context "${ctx}" apply -f \
        "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    kubectl --context "${ctx}" wait --for=condition=ready pod \
        -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=300s

    # The webhook reports Ready before it reliably answers, so the first apply
    # of a cert-manager CR can still be refused. Retry rather than fail the
    # whole bring-up on a race — same pattern the integration fixtures use.
    log "${name}: ca-cluster-issuer"
    local i
    for i in 1 2 3 4 5; do
        if kubectl --context "${ctx}" apply -f "${REPO_ROOT}/config/dev/self-signed-issuer.yaml"; then
            break
        fi
        warn "${name}: issuer apply refused (attempt ${i}); the cert-manager webhook may still be warming up"
        sleep 10
    done
    kubectl --context "${ctx}" wait --for=condition=Ready \
        clusterissuer/ca-cluster-issuer --timeout=180s ||
        warn "${name}: ca-cluster-issuer is not Ready — TLS deployments will hang waiting for a Secret"
}

load_and_deploy_operator() {
    local name="$1" ctx="kind-$1"
    log "${name}: loading ${OPERATOR_IMAGE}"
    # containerd restarts inside the node just after the cluster reports Ready,
    # so the first load often fails with a connection refused or a missing
    # content digest. Retry; this is the documented Kind race.
    local i
    for i in 1 2 3; do
        if kind load docker-image "${OPERATOR_IMAGE}" --name "${name}"; then
            break
        fi
        warn "${name}: image load failed (attempt ${i}), retrying"
        sleep 10
    done

    # kubectl, not make: every make target here runs against the CURRENT
    # context, and `kind create cluster` has just switched it. Targeting the
    # wrong cluster is the single easiest mistake in a two-cluster rig — it
    # cost a false blocker on the first Part E walk — so nothing in this
    # script omits --context.
    log "${name}: CRDs + operator (dev overlay → ${OPERATOR_NS})"
    kubectl --context "${ctx}" apply -f "${REPO_ROOT}/config/crd/bases/" ||
        die "${name}: could not install the CRDs"
    kubectl --context "${ctx}" apply -k "${REPO_ROOT}/config/overlays/dev" ||
        die "${name}: could not apply the dev overlay"
    kubectl --context "${ctx}" -n "${OPERATOR_NS}" rollout status \
        deployment/neo4j-operator-controller-manager --timeout=300s
}

cmd_up() {
    command -v kind >/dev/null || die "kind is not installed"
    command -v docker >/dev/null || die "docker is not installed"

    log "building ${OPERATOR_IMAGE}"
    docker build -t "${OPERATOR_IMAGE}" "${REPO_ROOT}"

    create_cluster "${UPSTREAM}"
    create_cluster "${DOWNSTREAM}"

    # MetalLB on the UPSTREAM only: it is the side that publishes the proxy.
    # The pool comes from the shared `kind` Docker network, so the downstream
    # cluster can reach it with no host routing.
    log "MetalLB on ${UPSTREAM} (the proxy side)"
    "${REPO_ROOT}/hack/metallb-setup.sh" "${UPSTREAM}" 200

    load_and_deploy_operator "${UPSTREAM}"
    load_and_deploy_operator "${DOWNSTREAM}"

    cat <<EOF

$(log "both clusters are up")

  upstream    ${UP_CTX}      (MetalLB pool on the shared kind network)
  downstream  ${DOWN_CTX}

Next, by hand and from the published docs — that is the point of the journey:

  1. Apply a Neo4jEnterpriseCluster to EACH cluster, with
       spec.tls.mode: cert-manager
       spec.tls.issuerRef: {name: ca-cluster-issuer, kind: ClusterIssuer}
     and spec.crossClusterReplication.enabled: true on the UPSTREAM only.

  2. Once both are Ready:  hack/ccdr-two-cluster.sh trust
     (it exchanges the CAs and patches both spec.tls.additionalClusterTrustCAs)

  3. Read the proxy address:
       kubectl --context ${UP_CTX} get neo4jenterprisecluster <name> \\
         -o jsonpath='{.status.crossClusterReplication.addresses}'

  4. Apply a Neo4jReplicaDatabase on ${DOWN_CTX} with source.mode: network
     and that address.

Every scenario and what to check: docs/developer_guide/release_verification.md,
Phase 5 Part E.
EOF
}

# cluster_name_on <context> — the single Neo4jEnterpriseCluster there.
cluster_name_on() {
    local ctx="$1"
    kubectl --context "${ctx}" get neo4jenterprisecluster -A \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

cluster_ns_on() {
    local ctx="$1"
    kubectl --context "${ctx}" get neo4jenterprisecluster -A \
        -o jsonpath='{.items[0].metadata.namespace}' 2>/dev/null
}

cmd_trust() {
    local up_name down_name up_ns down_ns
    up_name="$(cluster_name_on "${UP_CTX}")"
    down_name="$(cluster_name_on "${DOWN_CTX}")"
    [ -n "${up_name}" ] || die "no Neo4jEnterpriseCluster on ${UP_CTX} — apply one first"
    [ -n "${down_name}" ] || die "no Neo4jEnterpriseCluster on ${DOWN_CTX} — apply one first"
    up_ns="$(cluster_ns_on "${UP_CTX}")"
    down_ns="$(cluster_ns_on "${DOWN_CTX}")"

    # The CA only exists once cert-manager has issued the certificate, which is
    # why this is a separate command rather than part of `up`.
    local up_ca down_ca
    up_ca="$(kubectl --context "${UP_CTX}" -n "${up_ns}" get secret "${up_name}-tls-secret" \
        -o jsonpath='{.data.ca\.crt}' 2>/dev/null)" ||
        die "${up_name}-tls-secret has no ca.crt yet on ${UP_CTX} — is spec.tls.mode cert-manager, and is the cluster issued?"
    down_ca="$(kubectl --context "${DOWN_CTX}" -n "${down_ns}" get secret "${down_name}-tls-secret" \
        -o jsonpath='{.data.ca\.crt}' 2>/dev/null)" ||
        die "${down_name}-tls-secret has no ca.crt yet on ${DOWN_CTX}"
    [ -n "${up_ca}" ] || die "${up_name}-tls-secret carries an empty ca.crt — the issuer did not populate it"
    [ -n "${down_ca}" ] || die "${down_name}-tls-secret carries an empty ca.crt"

    log "upstream CA → ${DOWN_CTX}/upstream-ca, downstream CA → ${UP_CTX}/downstream-ca"
    kubectl --context "${DOWN_CTX}" -n "${down_ns}" create secret generic upstream-ca \
        --from-literal=placeholder=x --dry-run=client -o json |
        python3 -c "import json,sys; d=json.load(sys.stdin); d['data']={'ca.crt':'${up_ca}'}; print(json.dumps(d))" |
        kubectl --context "${DOWN_CTX}" apply -f -
    kubectl --context "${UP_CTX}" -n "${up_ns}" create secret generic downstream-ca \
        --from-literal=placeholder=x --dry-run=client -o json |
        python3 -c "import json,sys; d=json.load(sys.stdin); d['data']={'ca.crt':'${down_ca}'}; print(json.dumps(d))" |
        kubectl --context "${UP_CTX}" apply -f -

    log "patching spec.tls.additionalClusterTrustCAs on both"
    kubectl --context "${UP_CTX}" -n "${up_ns}" patch neo4jenterprisecluster "${up_name}" --type=merge \
        -p '{"spec":{"tls":{"additionalClusterTrustCAs":[{"name":"downstream-ca","key":"ca.crt"}]}}}'
    kubectl --context "${DOWN_CTX}" -n "${down_ns}" patch neo4jenterprisecluster "${down_name}" --type=merge \
        -p '{"spec":{"tls":{"additionalClusterTrustCAs":[{"name":"upstream-ca","key":"ca.crt"}]}}}'

    cat <<EOF

$(log "CAs exchanged")

Verify the trust actually crossed — this is the check that caught the peer-CA
mount bug, and a fingerprint comparison is the only thing that proves it:

  kubectl --context ${UP_CTX} -n ${up_ns} exec ${up_name}-server-0 -c neo4j -- md5sum /ssl/trusted/*.crt
  kubectl --context ${DOWN_CTX} -n ${down_ns} exec ${down_name}-server-0 -c neo4j -- md5sum /ssl/trusted/*.crt

The upstream's peer-ca-0.crt must equal the downstream's own ca.crt, and vice
versa. Both clusters must still reach Ready: a peer CA that stops a pod
starting is a blocker, not a warning.
EOF
}

cmd_status() {
    local ctx
    for ctx in "${UP_CTX}" "${DOWN_CTX}"; do
        printf '\n\033[1m=== %s ===\033[0m\n' "${ctx}"
        kubectl --context "${ctx}" get neo4jenterprisecluster -A 2>/dev/null ||
            echo "(cluster unreachable)"
        kubectl --context "${ctx}" get neo4jreplicadatabase -A 2>/dev/null | grep -v '^No resources' || true
        kubectl --context "${ctx}" get svc -A -o wide 2>/dev/null | grep -E 'LoadBalancer|NAME' || true
    done
    echo
}

cmd_down() {
    log "deleting ${UPSTREAM} and ${DOWNSTREAM}"
    kind delete cluster --name "${UPSTREAM}" || true
    kind delete cluster --name "${DOWNSTREAM}" || true
}

case "${1:-}" in
    up) cmd_up ;;
    trust) cmd_trust ;;
    status) cmd_status ;;
    down) cmd_down ;;
    *) die "usage: $0 {up|trust|status|down}" ;;
esac
