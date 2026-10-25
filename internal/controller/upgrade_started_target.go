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

// What the servers of an upgrade have started on (operations.md id 149).
//
// The version rule refuses a tag below a version a server has run: Neo4j does
// not support downgrades, and a server that started on a target has had its
// store opened by it. It read that from the walk's progress, which says a
// server was released onto the target — the partition lowered for it — not
// that it started. A server whose image cannot be pulled never runs the
// target, and the #562 walk could not point an upgrade paused on an
// unpublished tag (2026.09.9-enterprise, server-2 in ErrImagePull) at the
// earlier fixed one: "already restarted servers on it", and the cluster went
// Failed.
//
// So the operator records what the servers started on, from their containers,
// in status.upgradeStatus.startedTargetVersion: previousVersion when the
// upgrade starts, the target once a server's Neo4j container has started on
// it — running, or exited after running (a crash loop may have touched the
// store), never one still waiting on an image pull or a container it could not
// create. Progress is kept for what it says; the record is a separate field
// because progress is reset at each retarget, and the version an earlier
// target started a server on must survive it. It is read on every pass of the
// walk, before a retarget, before the machine takes a held upgrade up, and on
// the passes that hold one (the image may become pullable).

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// progressReleasedAServer reports whether an upgrade record's progress says
// the walk released a server onto its target (the partition was lowered for
// it): the only evidence a record an earlier operator wrote carries.
func progressReleasedAServer(us *neo4jv1beta1.UpgradeStatus) bool {
	return us != nil && us.Progress != nil && (us.Progress.Upgraded > 0 || us.Progress.InProgress > 0)
}

// containerStartedOn reports whether the pod's Neo4j container runs the image
// tag and has started on it: it is running, or has exited after running
// (terminated now or before, or restarted). A container waiting on an image
// it cannot pull, or one that could not be created, has not.
func containerStartedOn(pod *corev1.Pod, tag string) bool {
	image := ""
	for _, c := range pod.Spec.Containers {
		if c.Name == resources.Neo4jContainer {
			image = c.Image
		}
	}
	if image == "" || imageTag(image) != tag {
		return false
	}
	for _, s := range pod.Status.ContainerStatuses {
		if s.Name != resources.Neo4jContainer {
			continue
		}
		return s.State.Running != nil || s.State.Terminated != nil ||
			s.LastTerminationState.Terminated != nil || s.RestartCount > 0
	}
	return false
}

// targetStartedOnAServer reports whether a server pod of the cluster has
// started on the image tag.
func (r *Neo4jEnterpriseClusterReconciler) targetStartedOnAServer(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, tag string) (bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"neo4j.com/cluster":    cluster.Name,
		"neo4j.com/clustering": "true",
	}); err != nil {
		return false, err
	}
	for i := range pods.Items {
		if containerStartedOn(&pods.Items[i], tag) {
			return true, nil
		}
	}
	return false, nil
}

// recordStartedTarget records in status.upgradeStatus.startedTargetVersion
// that a server has started on the upgrade's target, once one has; the
// pass's copy follows. Nothing is written while none has, or when it is
// recorded already. Non-fatal: a failed read or write is retried on the next
// pass, and until then the record says less than the servers ran — the
// callers that act on it (a retarget, taking a held upgrade up) read it first.
//
// With an earlier release's CRDs installed the API server drops the field, so
// every pass sends it again: that write changes nothing (the stored object
// keeps its resourceVersion) and is logged at V(1), not at info on every pass
// (as for the reported versions, operations.md id 153).
func (r *Neo4jEnterpriseClusterReconciler) recordStartedTarget(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	logger := log.FromContext(ctx)
	us := cluster.Status.UpgradeStatus
	if us == nil || us.TargetVersion == "" || us.StartedTargetVersion == us.TargetVersion ||
		us.Phase == upgradePhaseCompleted {
		return
	}
	target := us.TargetVersion
	started, err := r.targetStartedOnAServer(ctx, cluster, target)
	if err != nil {
		logger.V(1).Info("Could not read whether a server started on the upgrade's target", "error", err.Error())
		return
	}
	if !started {
		return
	}
	written, kept := false, false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		written, kept = false, false
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		l := latest.Status.UpgradeStatus
		if l == nil || l.TargetVersion != target {
			return nil // the record moved on meanwhile
		}
		before := latest.ResourceVersion
		l.StartedTargetVersion = target
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		written = true
		// The update returns the object as stored.
		kept = latest.ResourceVersion != before && latest.Status.UpgradeStatus != nil &&
			latest.Status.UpgradeStatus.StartedTargetVersion == target
		return nil
	})
	if err != nil {
		logger.V(1).Info("Could not record that a server started on the upgrade's target", "error", err.Error())
		return
	}
	if !written {
		return
	}
	us.StartedTargetVersion = target
	if !kept {
		logger.V(1).Info("A server has started on the upgrade's target; the API server did not keep status.upgradeStatus.startedTargetVersion (apply this release's CRDs)",
			"target", target)
		return
	}
	logger.Info("A server has started on the upgrade's target; a tag below it is refused from now on", "target", target)
}

// forgetAbandonedUpgrade drops the record of a Paused or Failed upgrade that
// no server ever started on and whose target spec.image.tag no longer names,
// when the server StatefulSet's template does not carry that target: a check
// refused it before Staging, or the user moved the tag back before it went on.
// Nothing will take it up — the image change it was for is gone — and its
// message said "failed …, and the operator takes it up again" for as long as
// it stayed (#562 walk). A record with no target (an earlier operator's pause
// before the pre-upgrade check) waits for the user as before, and one whose
// template carries its target is the state machine's to finish.
func (r *Neo4jEnterpriseClusterReconciler) forgetAbandonedUpgrade(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	us := cluster.Status.UpgradeStatus
	if !upgradeHeld(cluster) || us.TargetVersion == "" || us.TargetVersion == cluster.Spec.Image.Tag ||
		us.ResolvedTargetVersion != "" {
		return
	}
	// Nothing says a server started on it: this operator's record says so
	// from the containers; an earlier operator's from a walk that released
	// no server.
	if us.StartedTargetVersion != "" && us.StartedTargetVersion != us.PreviousVersion ||
		us.StartedTargetVersion == "" && progressReleasedAServer(us) {
		return
	}
	place, found := r.serverImageInPlace(ctx, cluster)
	if !found || imageTag(place.image) == us.TargetVersion {
		return
	}
	target, phase := us.TargetVersion, us.Phase
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		if l := latest.Status.UpgradeStatus; l == nil || l.TargetVersion != target || l.Phase != phase {
			return nil
		}
		latest.Status.UpgradeStatus = nil
		syncStalledCondition(latest) // nothing stopped any more (operations.md id 149)
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		log.FromContext(ctx).V(1).Info("Could not drop the record of an abandoned upgrade", "error", err.Error())
		return
	}
	cluster.Status.UpgradeStatus = nil
	log.FromContext(ctx).Info("Dropped the record of an upgrade no server started on and spec.image.tag no longer asks for",
		"target", target, "phase", phase, "tag", cluster.Spec.Image.Tag)
}
