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

// A server pod that cannot start and is due for the update is replaced by the
// operator when the StatefulSet controller does not replace it
// (operations.md id 204).
//
// Every repair the operator makes on a cluster that is short of a server — a
// plugin's own init container or settings (ids 152, 155), a held upgrade's
// new tag (id 162) — writes the server StatefulSet's template and relies on
// the StatefulSet controller to replace the pod that cannot start: it is on
// an older revision, at or above the partition, so the rolling update deletes
// it and creates it again from the new template. Which StatefulSet controller
// does that depends on the MaxUnavailableStatefulSet feature gate
// (pkg/controller/statefulset/stateful_set_control.go, kubernetes/kubernetes):
//
//   - off — Kubernetes up to 1.34, 1.35.4 and later 1.35 patches, 1.36: the
//     update deletes the highest pod in the partition's range that is not on
//     the update revision, available or not, then waits for it;
//   - on, as it is by default in 1.35.0 to 1.35.3 (beta, on), then turned off
//     in 1.35.4 and 1.36: updateStatefulSetAfterInvariantEstablished counts
//     every pod that is not available, at any ordinal, and deletes nothing while
//     that count reaches maxUnavailable (1 when unset, the API server's default
//     with the gate on). The pod that cannot start is one: nothing is ever
//     replaced, and the repair waits for ever — Kubernetes' "forced rollback",
//     which the standalone, an OrderedReady StatefulSet, already needed
//     (replaceStuckPods);
//   - on again in 1.37: for Parallel pod management it first deletes every pod
//     at or above the partition that is on an older revision and not
//     available, then spends what is left of maxUnavailable on available ones.
//
// So on a cluster the operator deletes such a pod itself, as 1.37's
// controller, and the one without the gate, would: one at or above the
// partition, below the replica count, on another revision than the update
// revision of a spec the StatefulSet controller has seen, not being deleted,
// not Ready, and failing to start (podCannotStart: an image it cannot pull, a
// container that keeps exiting, an init container that failed). It is down, so
// nothing it served is lost, and it comes back from the update revision. Where
// the StatefulSet controller deletes it too, one of the two deletes finds it
// gone. A pod below the partition is never touched: it would come back on the
// current revision.

import (
	"context"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// stuckServerPods returns the pods of live that its rolling update is to
// replace and that cannot start, in ordinal order, described: at or above the
// partition, below the replica count, on another revision than
// status.updateRevision, not being deleted, not Ready, and failing to start
// (podCannotStart). None while the StatefulSet controller has not seen the
// latest spec: its update revision could be the template the pod fails on.
func stuckServerPods(live *appsv1.StatefulSet, pods []corev1.Pod) ([]*corev1.Pod, []string) {
	update := live.Status.UpdateRevision
	if live.Spec.Replicas == nil || update == "" || live.Status.ObservedGeneration < live.Generation {
		return nil, nil
	}
	partition := 0
	if p := statefulSetPartition(live); p != nil {
		partition = int(*p)
	}
	var stuck []*corev1.Pod
	var why []string
	for ordinal := partition; ordinal < int(*live.Spec.Replicas); ordinal++ {
		for i := range pods {
			pod := &pods[i]
			if o, ok := statefulSetOrdinal(live.Name, pod.Name); !ok || o != ordinal {
				continue
			}
			revision := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
			if revision == "" || revision == update || pod.DeletionTimestamp != nil || isPodReady(pod) {
				continue
			}
			if described, ok := podCannotStart(pod); ok {
				stuck = append(stuck, pod)
				why = append(why, described)
			}
		}
	}
	return stuck, why
}

// replaceStuckServerPods deletes the pods stuckServerPods returns, each with
// its UID as a precondition, so that the StatefulSet controller creates them
// again from the update revision, and announces each with a Normal
// ServerPodReplaced event. A pod already gone or replaced is passed over.
func (r *Neo4jEnterpriseClusterReconciler) replaceStuckServerPods(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	live *appsv1.StatefulSet, pods []corev1.Pod) {
	logger := log.FromContext(ctx)
	stuck, why := stuckServerPods(live, pods)
	for i, pod := range stuck {
		revision := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				continue // the StatefulSet controller replaced it first
			}
			logger.Info("Could not delete the server pod that cannot start", "pod", pod.Name, "error", err.Error())
			continue
		}
		logger.Info("Deleted a server pod that cannot start so that the StatefulSet creates it again from its update revision",
			"pod", pod.Name, "why", why[i], "revision", revision, "updateRevision", live.Status.UpdateRevision)
		cause := strings.TrimSuffix(strings.TrimPrefix(why[i], pod.Name+" ("), ")")
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonServerPodReplaced,
			"Deleted pod %s, which cannot start on revision %s (%s), so that StatefulSet %s creates it again from its update revision %s; "+
				"a StatefulSet does not replace such a pod itself on every Kubernetes version (MaxUnavailableStatefulSet in 1.35.0-1.35.3)",
			pod.Name, revision, cause, live.Name, live.Status.UpdateRevision)
	}
}

// replaceStuckServerPodsUncached is replaceStuckServerPods on the server pods
// read from the API server; live was read from it too.
func (r *Neo4jEnterpriseClusterReconciler) replaceStuckServerPodsUncached(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, live *appsv1.StatefulSet) {
	pods, err := r.serverPodsUncached(ctx, cluster)
	if err != nil {
		log.FromContext(ctx).V(1).Info("Could not read the server pods to replace one that cannot start", "error", err.Error())
		return
	}
	r.replaceStuckServerPods(ctx, cluster, live, pods)
}

// reconcileStuckServerPods is the normal pass's: when the cached server
// StatefulSet shows an update that has not reached every pod with one not
// ready — the only time a pod can be stuck so — it reads the StatefulSet and
// the pods from the API server and replaces the stuck ones. A stale cache only
// delays it to the next pass.
func (r *Neo4jEnterpriseClusterReconciler) reconcileStuckServerPods(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	cached := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}, cached); err != nil {
		return
	}
	if cached.Spec.Replicas == nil || cached.Status.ReadyReplicas >= *cached.Spec.Replicas ||
		(cached.Status.UpdateRevision == cached.Status.CurrentRevision && cached.Status.ObservedGeneration >= cached.Generation) {
		return
	}
	live, err := r.serverStatefulSetUncached(ctx, cluster)
	if err != nil {
		return
	}
	r.replaceStuckServerPodsUncached(ctx, cluster, live)
}
