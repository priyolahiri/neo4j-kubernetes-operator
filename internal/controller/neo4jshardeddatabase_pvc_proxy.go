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
	stderrors "errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// pvcSeedProxyURLForShard is the sharded-DB convenience wrapper around
// pvcSeedProxyURL. The proxy owner is the sharded DB CR, so its name is
// the URL suffix.
func pvcSeedProxyURLForShard(shardedDBName, namespace, backupsPath, filename string) string {
	return pvcSeedProxyURL(shardedDBName, namespace, backupsPath, filename)
}

// ensurePVCSeedProxy is the sharded-DB convenience wrapper around
// ensurePVCSeedProxyResources. Delegates to the generic helper with the
// sharded DB CR as the owner.
func (r *Neo4jShardedDatabaseReconciler) ensurePVCSeedProxy(
	ctx context.Context,
	shardedDB *neo4jv1beta1.Neo4jShardedDatabase,
	backupPVCName string,
) (proxyAvailable bool, err error) {
	available, err := ensurePVCSeedProxyResources(ctx, r.Client, r.Scheme, shardedDB, shardedDB.Name, backupPVCName, nil)
	if err == nil {
		// Restrict the proxy to the target cluster's server pods (#219).
		if npErr := ensurePVCSeedProxyNetworkPolicy(ctx, r.Client, r.Scheme, shardedDB, shardedDB.Name, clusterPodLabels(shardedDB.Spec.ClusterRef)); npErr != nil {
			log.FromContext(ctx).Error(npErr, "Failed to ensure seed-proxy NetworkPolicy (non-fatal)")
		}
	}
	return available, err
}

// ensureClusterSeedConfig makes a cloud seed reachable from the cluster's
// server pods, which fetch it themselves: the backup's credentials Secret and
// — for MinIO and other S3-compatible stores — its endpoint, projected as a
// Neo4jRestore does it (both in one update, so one rolling restart, under the
// neo4j.com/auto-inherit-seed-creds annotation), and seeding only once the
// pods carry them. wait=true means return res: the database is Pending or
// Failed with the reason. PVC seeds need nothing here (in-cluster HTTP).
func (r *Neo4jShardedDatabaseReconciler) ensureClusterSeedConfig(
	ctx context.Context,
	shardedDB *neo4jv1beta1.Neo4jShardedDatabase,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	resolved *ResolvedShardedSeed,
) (res ctrl.Result, wait bool) {
	customEndpoint := resolved.Cloud != nil && resolved.Cloud.EndpointURL != ""
	if resolved.CredsSecretName == "" && !customEndpoint {
		return ctrl.Result{}, false
	}
	logger := log.FromContext(ctx)
	requeue := ctrl.Result{RequeueAfter: r.RequeueAfter}
	setStatus := func(phase, msg string) {
		if err := r.updateStatus(ctx, shardedDB, phase, msg, nil); err != nil {
			logger.Error(err, "Failed to update status", "phase", phase)
		}
	}
	seed := r.seedConfig()
	target := restoreTarget{cluster: cluster}

	projected, missing, err := seed.projectSeedConfig(ctx, target, resolved.CredsSecretName, resolved.Cloud)
	if err != nil {
		if stderrors.Is(err, errSeedConfigNotAutoInherited) {
			msg := fmt.Sprintf("cluster %q's server pods can't reach the seed source: missing %s. The server JVM fetches the seed itself. Provide these on the cluster CR, or set annotation %s=\"true\" to let the operator inject them (triggers one rolling restart).",
				cluster.Name, strings.Join(missing, "; "), AutoInheritSeedCredsAnnotation)
			r.Recorder.Event(shardedDB, corev1.EventTypeWarning, "SeedCredsMissing", msg)
			setStatus("Failed", msg)
			return requeue, true
		}
		logger.Error(err, "Failed to project seed configuration onto the cluster")
		setStatus("Pending", fmt.Sprintf("Retrying projection of seed configuration onto cluster %q: %v", cluster.Name, err))
		return requeue, true
	}
	if projected {
		r.Recorder.Event(shardedDB, corev1.EventTypeNormal, "SeedCredsAutoInherited",
			fmt.Sprintf("Projected seed configuration (credentials/endpoint) onto cluster %q; waiting for the rolling restart", cluster.Name))
		setStatus("Pending", fmt.Sprintf("Projected seed configuration onto cluster %q; waiting for the rolling restart", cluster.Name))
		return requeue, true
	}
	rolledOut := true
	if resolved.CredsSecretName != "" {
		if ok, err := seed.seedCredsRolledOut(ctx, target, resolved.CredsSecretName); err != nil || !ok {
			rolledOut = false
		}
	}
	if customEndpoint && envHasSeedEndpoint(cluster.Spec.Env) {
		if ok, err := seed.specEnvEndpointRolledOut(ctx, target); err != nil || !ok {
			rolledOut = false
		}
	}
	if !rolledOut {
		setStatus("Pending", fmt.Sprintf("Waiting for cluster %q pods to roll out the seed configuration", cluster.Name))
		return requeue, true
	}
	return ctrl.Result{}, false
}

// seedConfig reuses the restore path's projection of seed credentials and
// endpoint onto a cluster, and its rollout checks: they need only a client.
func (r *Neo4jShardedDatabaseReconciler) seedConfig() *Neo4jRestoreReconciler {
	return &Neo4jRestoreReconciler{Client: r.Client, Scheme: r.Scheme}
}

// teardownPVCSeedProxy removes the proxy stack once the sharded database has
// finished seeding (#219) — the proxy otherwise serves the whole backup PVC
// for the lifetime of the (long-lived) sharded DB CR.
func (r *Neo4jShardedDatabaseReconciler) teardownPVCSeedProxy(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) error {
	return teardownPVCSeedProxyResources(ctx, r.Client, shardedDB.Namespace, shardedDB.Name)
}
