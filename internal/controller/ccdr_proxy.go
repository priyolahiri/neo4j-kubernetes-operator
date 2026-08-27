/*
Copyright 2025 Priyo Lahiri.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// ccdrConfigHashAnnotation stamps the rendered haproxy.cfg's content hash
// onto the proxy Deployment's pod template, so a ConfigMap change (e.g. the
// server count changing the frontend/backend list) triggers a rollout the
// same way Neo4j's own ConfigMapManager forces a StatefulSet restart on
// config drift.
const ccdrConfigHashAnnotation = "neo4j.com/ccdr-proxy-config-hash"

// reconcileCrossClusterReplicationProxy creates/updates the network-mode
// CCDR exposure proxy's ConfigMap, Deployment, and Service, and returns the
// status to publish on the cluster CR. Called BEFORE the server ConfigMap
// is reconciled, so a freshly-observed load balancer hostname is available
// in-memory (via the returned status, which the caller assigns onto
// cluster.Status before continuing) when the startup script renders
// server.cluster.advertised_address.
//
// When spec.crossClusterReplication is unset or disabled, tears down any
// previously-created proxy resources and returns (nil, nil).
func (r *Neo4jEnterpriseClusterReconciler) reconcileCrossClusterReplicationProxy(
	ctx context.Context,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (*neo4jv1beta1.CrossClusterReplicationStatus, error) {
	logger := log.FromContext(ctx)

	if cluster.Spec.CrossClusterReplication == nil || !cluster.Spec.CrossClusterReplication.Enabled {
		if err := r.teardownCrossClusterReplicationProxy(ctx, cluster); err != nil {
			return nil, fmt.Errorf("teardown CCDR proxy: %w", err)
		}
		if cluster.Status.CrossClusterReplication != nil {
			// Clear a stale status left over from when this was enabled —
			// same re-fetch-inside-retry rationale as the persist call below.
			if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
				if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
					return err
				}
				latest.Status.CrossClusterReplication = nil
				return r.Status().Update(ctx, latest)
			}); err != nil {
				return nil, fmt.Errorf("clear CCDR proxy status: %w", err)
			}
		}
		return nil, nil
	}

	cm := resources.BuildCCDRProxyConfigMap(cluster)
	if err := r.applyCCDRConfigMap(ctx, cm, cluster); err != nil {
		return nil, fmt.Errorf("reconcile CCDR proxy ConfigMap: %w", err)
	}
	configHash := hashCCDRConfig(cm.Data["haproxy.cfg"])

	dep := resources.BuildCCDRProxyDeployment(cluster)
	dep.Spec.Template.Annotations = ccdrMergeAnnotations(dep.Spec.Template.Annotations, map[string]string{
		ccdrConfigHashAnnotation: configHash,
	})
	if err := r.applyCCDRDeployment(ctx, dep, cluster); err != nil {
		return nil, fmt.Errorf("reconcile CCDR proxy Deployment: %w", err)
	}

	svc := resources.BuildCCDRProxyService(cluster)
	if err := r.createOrUpdateResource(ctx, svc, cluster); err != nil {
		return nil, fmt.Errorf("reconcile CCDR proxy Service: %w", err)
	}

	// Re-fetch to read the load balancer's assigned ingress — createOrUpdate
	// doesn't return the live object.
	live := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, live); err != nil {
		return nil, fmt.Errorf("get CCDR proxy Service: %w", err)
	}

	status := &neo4jv1beta1.CrossClusterReplicationStatus{}
	if len(live.Status.LoadBalancer.Ingress) > 0 {
		ing := live.Status.LoadBalancer.Ingress[0]
		hostname := ing.Hostname
		if hostname == "" {
			hostname = ing.IP
		}
		if hostname != "" {
			status.Ready = true
			status.LoadBalancerHostname = hostname
			for i := int32(0); i < cluster.Spec.Topology.Servers; i++ {
				status.Addresses = append(status.Addresses,
					fmt.Sprintf("%s:%d", hostname, resources.CCDRProxyBasePort+i))
			}
		}
	}
	if !status.Ready {
		logger.Info("CCDR proxy Service load balancer not yet assigned; server.cluster.advertised_address stays internal until it is")
	}

	// Persist explicitly against a freshly-Get'd object: the caller's
	// in-memory `cluster` may already be stale by the time a later
	// updateClusterStatusWithVersion call re-Gets and overwrites status from
	// the API server (that function never carries this field forward — see
	// its own re-fetch-inside-retry comment). This mirrors that same
	// re-fetch-inside-retry pattern rather than fighting it.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		latest.Status.CrossClusterReplication = status
		return r.Status().Update(ctx, latest)
	}); err != nil {
		return nil, fmt.Errorf("persist CCDR proxy status: %w", err)
	}

	return status, nil
}

// teardownCrossClusterReplicationProxy deletes the proxy Deployment,
// Service, and ConfigMap. Idempotent; NotFound is success. Owner-ref GC
// would eventually catch these too, but this makes the transition when a
// user flips crossClusterReplication.enabled from true to false immediate
// rather than waiting for the cluster CR's own deletion.
func (r *Neo4jEnterpriseClusterReconciler) teardownCrossClusterReplicationProxy(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	name := resources.CCDRProxyName(cluster.Name)
	om := metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}
	for _, obj := range []client.Object{
		&appsv1.Deployment{ObjectMeta: om},
		&corev1.Service{ObjectMeta: om},
		&corev1.ConfigMap{ObjectMeta: om},
	} {
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete %T %s: %w", obj, name, err)
		}
	}
	return nil
}

func (r *Neo4jEnterpriseClusterReconciler) applyCCDRConfigMap(ctx context.Context, cm *corev1.ConfigMap, owner client.Object) error {
	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: cm.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(owner, cm, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cm)
	} else if err != nil {
		return err
	}
	if existing.Data["haproxy.cfg"] == cm.Data["haproxy.cfg"] {
		return nil
	}
	existing.Data = cm.Data
	return r.Update(ctx, existing)
}

func (r *Neo4jEnterpriseClusterReconciler) applyCCDRDeployment(ctx context.Context, dep *appsv1.Deployment, owner client.Object) error {
	existing := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(owner, dep, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, dep)
	} else if err != nil {
		return err
	}
	// Selector is immutable and never changes for this Deployment (the label
	// set is derived only from the cluster name, not topology), so a plain
	// pod-template + port-list update is always safe here.
	existing.Spec.Template = dep.Spec.Template
	return r.Update(ctx, existing)
}

func hashCCDRConfig(cfg string) string {
	sum := sha256.Sum256([]byte(cfg))
	return hex.EncodeToString(sum[:])[:16]
}

func ccdrMergeAnnotations(base, add map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(add))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range add {
		out[k] = v
	}
	return out
}
