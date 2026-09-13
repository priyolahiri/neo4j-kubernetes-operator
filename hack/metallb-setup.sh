#!/usr/bin/env bash
# Give a Kind cluster a working LoadBalancer, so the cross-cluster replication
# proxy can be exercised for real.
#
# Why this exists: spec.crossClusterReplication puts the upstream's
# transaction-shipping port behind a type: LoadBalancer Service, and plain Kind
# has no cloud provider, so that Service never gets an address. The proxy path
# was therefore the one part of cross-cluster replication that no test could
# reach — and it was hiding a live defect (loadBalancerInternal produced a
# public load balancer while claiming otherwise).
#
# The trick is that Kind clusters share one Docker network, so an address pool
# carved from the top of that network's subnet is routable from any other Kind
# cluster on the same host. No host routes, no tunnels.
#
# Usage: hack/metallb-setup.sh <cluster-name> [pool-offset]
#   pool-offset picks a distinct /24 tail so two clusters never hand out the
#   same address. Default 200 → .200-.250.
set -euo pipefail

CLUSTER="${1:?usage: metallb-setup.sh <kind-cluster-name> [pool-offset]}"
OFFSET="${2:-200}"
METALLB_VERSION="${METALLB_VERSION:-v0.14.9}"
CTX="kind-${CLUSTER}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

SUBNET="$(docker network inspect kind -f '{{range .IPAM.Config}}{{if and .Subnet (not (eq (index (split .Subnet "") 0) ":"))}}{{.Subnet}} {{end}}{{end}}' 2>/dev/null | tr ' ' '\n' | grep -v ':' | head -1)"
if [ -z "${SUBNET}" ]; then
    echo "ERROR: could not read the 'kind' Docker network subnet. Is a Kind cluster up?" >&2
    exit 1
fi
PREFIX="$(echo "${SUBNET}" | cut -d. -f1-2)"
POOL_START="${PREFIX}.255.${OFFSET}"
POOL_END="${PREFIX}.255.$((OFFSET + 50))"
log "kind network ${SUBNET} → MetalLB pool ${POOL_START}-${POOL_END} on ${CLUSTER}"

kubectl --context "${CTX}" apply -f \
    "https://raw.githubusercontent.com/metallb/metallb/${METALLB_VERSION}/config/manifests/metallb-native.yaml"

log "waiting for MetalLB to be ready..."
kubectl --context "${CTX}" wait --namespace metallb-system \
    --for=condition=ready pod --selector=app=metallb --timeout=300s

# The webhook refuses configuration until its endpoints are actually serving —
# Ready pods are not sufficient, the same race the cert-manager issuer hits in
# scripts/test-env.sh. Retry rather than fail the whole setup on a few seconds.
log "configuring the address pool..."
for attempt in $(seq 1 12); do
    if kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kind-pool
  namespace: metallb-system
spec:
  addresses:
    - ${POOL_START}-${POOL_END}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kind-l2
  namespace: metallb-system
spec:
  ipAddressPools:
    - kind-pool
EOF
    then
        log "MetalLB ready on ${CLUSTER}; LoadBalancer Services will get ${POOL_START}-${POOL_END}"
        exit 0
    fi
    log "  attempt ${attempt}/12 failed (webhook not serving yet); retrying in 10s"
    sleep 10
done

echo "ERROR: could not configure the MetalLB address pool after 12 attempts" >&2
kubectl --context "${CTX}" get pods -n metallb-system >&2 || true
exit 1
