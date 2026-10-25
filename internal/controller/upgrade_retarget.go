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

// A server the upgrade has moved on is never put back below the partition
// (operations.md id 180).
//
// A StatefulSet creates a pod at or above its partition from its update
// revision (the template) and one below it from its current revision — the
// version every server ran before the upgrade, until Kubernetes records the
// rolled-out template as current once every pod runs it. Staging froze a new
// target at the replica count. On a retarget (A to B), mid-walk or of a held
// upgrade, the servers the walk had rolled onto A then sat below the
// partition: one whose pod was recreated before the walk reached it again — a
// node drain, an eviction, a crash and a delete — came back on the version
// before the upgrade, over a store A had opened. Neo4j does not support
// downgrades.
//
// So Staging never puts a server that runs another revision than the
// StatefulSet's current one below the partition: it stages the new target at
// the lowest such server. Those servers are then in the update range — the
// StatefulSet controller moves them on to the new target, highest first, each
// once the one before is Ready — and a pod of theirs recreated meanwhile comes
// back on the target. The walk's own checks (the server available in SHOW
// SERVERS, its databases caught up, id 127) apply from the lowest of them down;
// between those servers there is Kubernetes readiness only, as for the servers
// above the lowest one a repair replaces (id 162). A forward move, unchecked
// for a server that had already passed them on A, is the price of never going
// back: deferring the retarget until the walk to A completes would keep every
// check but take every server through A — a target the user is leaving, and
// one a walk that cannot finish (a server that cannot start on A) never
// completes — and making A the current revision is only possible once every
// server runs it.
//
// Two waits keep that roll from starting at a bad moment. The write that
// stages a new template at such a partition restarts servers at once, so it
// waits until every server below the partition is Ready and every one at or
// above is Ready or among the servers a repair replaces (id 162) — a retarget
// mid-walk would otherwise restart them while another server is down. And when
// every server runs the template being left, Kubernetes is about to record it
// as current: Staging waits for that, then freezes the new target at the
// replica count, and the walk checks every server, with no unchecked roll at
// all.

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// serverPodsUncached lists the cluster's server pods from the API server: a
// pod a node drain has just recreated must not be read from a cache that still
// holds the one before.
func (r *Neo4jEnterpriseClusterReconciler) serverPodsUncached(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.apiReader().List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"neo4j.com/cluster":    cluster.Name,
		"neo4j.com/clustering": "true",
	}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// retargetPartition returns the partition Staging writes so that no server
// running another revision than the StatefulSet's current one is below it:
// partition, or the lowest such server's ordinal. With a reason to wait
// instead: every server runs the StatefulSet's update revision, which
// Kubernetes is about to record as current (not while a repair replaces
// servers that cannot start: such a cluster never gets there).
func retargetPartition(live *appsv1.StatefulSet, pods []corev1.Pod, partition int32, repairing bool) (int32, string) {
	current, update := live.Status.CurrentRevision, live.Status.UpdateRevision
	if current == "" || live.Spec.Replicas == nil {
		return partition, ""
	}
	replicas := int(*live.Spec.Replicas)
	lowest, onUpdate, counted := -1, 0, 0
	for i := range pods {
		pod := &pods[i]
		ordinal, ok := statefulSetOrdinal(live.Name, pod.Name)
		revision := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
		if !ok || ordinal >= replicas || pod.DeletionTimestamp != nil || revision == "" {
			continue
		}
		counted++
		if revision == current {
			continue
		}
		if revision == update {
			onUpdate++
		}
		if lowest < 0 || ordinal < lowest {
			lowest = ordinal
		}
	}
	if lowest < 0 {
		return partition, ""
	}
	if !repairing && counted == replicas && onUpdate == replicas && update != "" {
		return partition, "every server runs the template the upgrade leaves; the StatefulSet controller records it as the current revision once they are all Ready"
	}
	if int32(lowest) < partition {
		return int32(lowest), ""
	}
	return partition, ""
}

// serverNotReadyBelow names a server that keeps a write staging a new
// template at partition from restarting servers now: one below the partition
// that is missing, being deleted or not Ready, or one at or above it that is
// not Ready and not among the servers a repair replaces (down). "" when there
// is none.
func serverNotReadyBelow(live *appsv1.StatefulSet, pods []corev1.Pod, partition int32, down *serversDown) string {
	if live.Spec.Replicas == nil {
		return ""
	}
	byOrdinal := map[int]*corev1.Pod{}
	for i := range pods {
		if ordinal, ok := statefulSetOrdinal(live.Name, pods[i].Name); ok {
			byOrdinal[ordinal] = &pods[i]
		}
	}
	for ordinal := 0; ordinal < int(*live.Spec.Replicas); ordinal++ {
		pod := byOrdinal[ordinal]
		switch {
		case pod == nil || pod.DeletionTimestamp != nil:
			if int32(ordinal) < partition {
				return fmt.Sprintf("%s-%d", live.Name, ordinal)
			}
		case isPodReady(pod):
		case int32(ordinal) >= partition && down != nil && down.pods[pod.Name]:
		default:
			return pod.Name
		}
	}
	return ""
}
