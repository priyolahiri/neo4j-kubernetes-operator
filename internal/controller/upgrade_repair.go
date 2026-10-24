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

// A held upgrade whose restarted servers cannot start on its target goes on
// towards a new tag (operations.md id 162).
//
// An upgrade stops (Paused or Failed) when a server it restarted does not
// come back: the target image cannot be pulled (a typo, a tag not published
// yet), or the server crash-loops on it. Those servers run the target
// template, so they are down until the template changes, and only the state
// machine writes it while the upgrade holds its partition (id 149). But the
// machine started only on a Ready cluster with every server available, which
// such a cluster never is again; the image hold pinned the new tag to the
// template; and had the machine started, Staging froze every pod and the walk
// waited for all of them to be ready. Changing the tag, the documented way to
// resume a Paused upgrade, did nothing, towards any tag.
//
// So the machine takes such an upgrade up, towards a new tag, when the only
// servers down are the ones it restarted, each failing to start (an image
// pull that fails, a container that keeps exiting), and they are the highest
// ordinals, at or above the partition, with every server below them up and
// no other server unavailable in Neo4j's last view. Staging then freezes at
// the lowest of them instead of at the replica count, so the StatefulSet
// replaces them on the new template first — they are down, so nothing that
// was up restarts — and the walk goes on from there through its gates. The
// pre-upgrade checks are skipped: the cluster is short of exactly the servers
// being replaced. It is id 152's exception for a plugin, applied to the
// upgrade.

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// serversDown names the servers at or above a StatefulSet's partition that
// cannot start on its template: partition is the lowest of their ordinals,
// failing describes each.
type serversDown struct {
	partition int32
	failing   []string
}

// heldUpgradeRepair reports whether a Paused or Failed upgrade goes on towards
// spec.image.tag although the cluster is not Ready with every server
// available: the cluster has formed, the tag is a new one (the server
// StatefulSet's template carries another image), and the servers that are
// down are exactly the ones the upgrade restarted that cannot start on the
// image they run (serversDownOnTarget). nil otherwise.
func (r *Neo4jEnterpriseClusterReconciler) heldUpgradeRepair(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *serversDown {
	if !upgradeHeld(cluster) || !clusterHasFormed(cluster) {
		return nil
	}
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil || len(sts.Spec.Template.Spec.Containers) == 0 {
		return nil
	}
	if sts.Spec.Template.Spec.Containers[0].Image == fmt.Sprintf("%s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag) {
		// The same image cannot start there either; a Failed upgrade retries
		// it once every server is available, a Paused one waits for the user.
		return nil
	}
	return r.serversDownOnTarget(ctx, cluster, sts)
}

// serversDownOnTarget returns the servers of sts at or above its partition that
// cannot start on the image they run (podCannotStart), when they are the only
// servers down: the highest ordinals, every pod below them Ready, none
// missing or being deleted (podsDownOnlyIn), and Neo4j's last view naming no
// other server unavailable (unavailableServerOutside; no diagnostics leaves the
// pods to decide). nil otherwise, and with no partition between zero and the
// replica count: such pods run the template of an upgrade that has restarted
// a server.
func (r *Neo4jEnterpriseClusterReconciler) serversDownOnTarget(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) *serversDown {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	partition := int32(0)
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
		partition = *ru.Partition
	}
	if partition <= 0 || partition >= replicas {
		return nil
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"neo4j.com/cluster":    cluster.Name,
		"neo4j.com/clustering": "true",
	}); err != nil {
		return nil
	}
	failing, down := podsDownOnlyIn(sts, pods.Items, podCannotStart)
	if len(down) == 0 {
		return nil
	}
	if other := unavailableServerOutside(cluster, down); other != "" {
		log.FromContext(ctx).Info("Not replacing the servers that cannot start on the upgrade's target: another server is unavailable",
			"server", other, "down", failing)
		return nil
	}
	lowest := replicas
	for name := range down {
		if ordinal, ok := statefulSetOrdinal(sts.Name, name); ok && int32(ordinal) < lowest {
			lowest = int32(ordinal)
		}
	}
	return &serversDown{partition: lowest, failing: failing}
}

// imagePullFailures are the waiting reasons of a container whose image cannot
// be pulled.
var imagePullFailures = map[string]bool{"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true}

// podCannotStart reports whether a server pod that is not Ready cannot start on
// the image it was given, described for a message: its neo4j container or an
// init container waits on an image it cannot pull, its neo4j container exited
// non-zero and has not run since (containerCrashLooping), or an init container
// failed (initContainerFailure). A pod still starting does not count.
func podCannotStart(pod *corev1.Pod) (string, bool) {
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, s := range statuses {
			if w := s.State.Waiting; w != nil && imagePullFailures[w.Reason] {
				return fmt.Sprintf("%s (container %s: %s)", pod.Name, s.Name, w.Reason), true
			}
		}
	}
	if code, ok := containerCrashLooping(pod, resources.Neo4jContainer); ok {
		return failingPod(pod.Name, "container "+resources.Neo4jContainer, code), true
	}
	for _, s := range pod.Status.InitContainerStatuses {
		if code, ok := initContainerFailure(pod, s.Name); ok {
			return failingPod(pod.Name, "init container "+s.Name, code), true
		}
	}
	return "", false
}

// imageChanges reports whether desired's server container runs another image
// than live's.
func imageChanges(live, desired *appsv1.StatefulSet) bool {
	l, d := live.Spec.Template.Spec.Containers, desired.Spec.Template.Spec.Containers
	return len(l) > 0 && len(d) > 0 && l[0].Image != d[0].Image
}
