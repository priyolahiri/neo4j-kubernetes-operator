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

// A Paused or Failed upgrade holds the servers where the rolling-upgrade state
// machine left them (operations.md id 149).
//
// Once an upgrade stops, the normal reconcile pass runs again, and it applies
// the builder's StatefulSet, whose RollingUpdate partition is unset. It used to
// keep the live partition only while the machine was active, so the first pass
// after a Paused or Failed upgrade set it to 0, and the StatefulSet controller
// restarted every server the machine had not rolled onto the target image,
// one after the other with only the Kubernetes readiness probe between them:
// none of the machine's gates (#488's database catch-up, SHOW SERVERS). A
// Paused upgrade was not "left as-is", and moving the tag back after a Failed
// one was a downgrade of servers that had just been rolled forward.
//
// So the partition stays where the machine left it until the upgrade goes on,
// and only the machine moves it:
//
//   - Failed: the operator retries the upgrade itself, through the machine,
//     once the cluster is Ready with every server available (isUpgradeRequired),
//     as before; the walk re-verifies the servers already rolled and rolls the
//     rest one at a time through its gates.
//   - Paused (autoPauseOnFailure): the upgrade waits for the user. Changing
//     spec.image.tag resumes it towards the new tag; setting
//     spec.upgradeStrategy.autoPauseOnFailure to false lets the operator retry
//     it as a Failed one. A tag below the target is refused once the upgrade
//     has restarted a server on the target (validateAgainstRunning in
//     internal/validation), so a tag can go back to status.version only while
//     no server runs the target. For a target without a patch, "below" is
//     below what a restarted server reports (resolvedTargetVersion, recorded
//     at the next Ready while the upgrade is held, operations.md id 153).
//
// While an upgrade is held, the rest of the normal pass runs: services,
// configuration, diagnostics, and the Ready status that users, roles, databases
// and backups wait for. A cluster whose servers run two versions because an
// upgrade stopped half-way is formed and serving, so it is Ready when every
// server is available; status.message and status.upgradeStatus say the upgrade
// is held.

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// upgradeHeld reports whether the cluster's upgrade stopped Paused or Failed,
// so the servers stay where the upgrade state machine left them.
func upgradeHeld(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	if cluster == nil || cluster.Status.UpgradeStatus == nil {
		return false
	}
	switch cluster.Status.UpgradeStatus.Phase {
	case upgradePhasePaused, upgradePhaseFailed:
		return true
	}
	return false
}

// upgradeWaitsForUser reports whether a Paused upgrade still waits for the
// user: spec.upgradeStrategy.autoPauseOnFailure is still set, and
// spec.image.tag still names the version it paused on. A record that names no
// target was written by an operator that did not record one when an upgrade
// paused before it started; it waits too, for autoPauseOnFailure to be
// turned off.
func upgradeWaitsForUser(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	us := cluster.Status.UpgradeStatus
	if us == nil || us.Phase != upgradePhasePaused || !autoPauseOnFailure(cluster) {
		return false
	}
	return us.TargetVersion == "" || us.TargetVersion == cluster.Spec.Image.Tag
}

// autoPauseOnFailure reports whether a failed upgrade step pauses the upgrade
// rather than failing it.
func autoPauseOnFailure(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	return cluster.Spec.UpgradeStrategy != nil && cluster.Spec.UpgradeStrategy.AutoPauseOnFailure
}

// keptUpgradePartition returns the RollingUpdate partition a held upgrade
// keeps on the server StatefulSet, or 0 when nothing is held: the servers
// below it run the version they ran before the upgrade.
func keptUpgradePartition(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) int32 {
	if !upgradeHeld(cluster) || sts == nil || sts.Spec.UpdateStrategy.RollingUpdate == nil ||
		sts.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
		return 0
	}
	if p := *sts.Spec.UpdateStrategy.RollingUpdate.Partition; p > 0 {
		return p
	}
	return 0
}

// upgradeStaged reports whether the server StatefulSet's template carries the
// upgrade's target, which Staging writes: a record that names no target, or
// one a check refused before Staging, has rolled no server.
func upgradeStaged(us *neo4jv1beta1.UpgradeStatus, sts *appsv1.StatefulSet) bool {
	return us != nil && us.TargetVersion != "" && len(sts.Spec.Template.Spec.Containers) > 0 &&
		imageTag(sts.Spec.Template.Spec.Containers[0].Image) == us.TargetVersion
}

// heldRolloutPending reports whether a StatefulSet whose partition a held
// upgrade keeps has not finished rolling what the partition lets it roll: the
// StatefulSet controller has not seen the current spec, or a server at or
// above the partition is not on the current template. The servers below the
// partition are not a rollout in flight: they stay where they are until the
// upgrade goes on.
func heldRolloutPending(sts *appsv1.StatefulSet, partition int32) (bool, string) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if sts.Status.ObservedGeneration < sts.Generation {
		return true, "the StatefulSet controller has not seen the current template"
	}
	if want := replicas - partition; want > 0 && sts.Status.UpdatedReplicas < want {
		return true, fmt.Sprintf("%d/%d pods from ordinal %d up updated (the upgrade holds the rest)",
			sts.Status.UpdatedReplicas, want, partition)
	}
	return false, ""
}

// partitionForNewServerStatefulSet is the partition a server StatefulSet
// created while an upgrade is held starts with: where the machine left it, or
// every pod frozen if it had not staged yet. A storage expansion deletes the
// StatefulSet (orphaning its pods) and the next pass creates it again; created
// without a partition, it would restart every server below the held one onto
// its template. Nil when nothing is held for spec.image.tag.
func partitionForNewServerStatefulSet(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, replicas *int32) *int32 {
	if !upgradeHeld(cluster) || cluster.Status.UpgradeStatus.TargetVersion != cluster.Spec.Image.Tag {
		return nil
	}
	if p := cluster.Status.UpgradeStatus.CurrentPartition; p != nil {
		v := *p
		return &v
	}
	if replicas == nil {
		return nil
	}
	v := *replicas
	return &v
}

// sameHeldUpgrade reports whether a new upgrade record for previousVersion
// continues the held one in us — the same upgrade resumed or retargeted, from
// the version the servers were last verified to run — rather than a new one.
func sameHeldUpgrade(us *neo4jv1beta1.UpgradeStatus, previousVersion string) bool {
	return us != nil && (us.Phase == upgradePhasePaused || us.Phase == upgradePhaseFailed) &&
		us.PreviousVersion == previousVersion
}

// newUpgradeRecord is the status.upgradeStatus an upgrade from previousVersion
// to targetVersion starts with. One that continues a held upgrade
// (sameHeldUpgrade) keeps its start time and its progress: the progress says
// whether a server has been restarted on a target, which is what the version
// rule reads to refuse a tag below it (operations.md id 149), and a fresh
// record would forget it. It keeps the version a floating target resolved to
// for the same reason (resolvedTargetVersion, operations.md id 153): the
// servers the earlier target restarted still run it.
func newUpgradeRecord(held *neo4jv1beta1.UpgradeStatus, previousVersion, targetVersion string, servers int32, now metav1.Time) neo4jv1beta1.UpgradeStatus {
	record := neo4jv1beta1.UpgradeStatus{
		Phase:           upgradePhaseStaging,
		StartTime:       &now,
		StepStartTime:   &now,
		CurrentStep:     "Staging target pod template",
		PreviousVersion: previousVersion,
		TargetVersion:   targetVersion,
		Progress:        &neo4jv1beta1.UpgradeProgress{Total: servers, Pending: servers},
		// No server has started on anything newer yet (recordStartedTarget).
		StartedTargetVersion: previousVersion,
	}
	if !sameHeldUpgrade(held, previousVersion) {
		return record
	}
	// The servers the held upgrade started still run what they started on: a
	// tag below it stays refused. A record an earlier operator wrote says only
	// that the walk released a server, which is taken as having run the
	// target, as that operator's rule did.
	switch {
	case held.StartedTargetVersion != "":
		record.StartedTargetVersion = held.StartedTargetVersion
	case progressReleasedAServer(held):
		record.StartedTargetVersion = held.TargetVersion
	}
	if held.StartTime != nil {
		start := *held.StartTime
		record.StartTime = &start
	}
	if held.Progress != nil {
		progress := held.Progress.DeepCopy()
		progress.Total = servers
		record.Progress = progress
	}
	record.ResolvedTargetVersion = held.ResolvedTargetVersion
	return record
}

// clusterReadyMessage is the status message of a Ready cluster; it says so
// when an upgrade is in progress or held, which status.upgradeStatus details.
// The machine writes it when it starts or retargets an upgrade
// (noteUpgradeInClusterMessage): a resumed upgrade used to say "is paused"
// until it completed (#553).
func clusterReadyMessage(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	const ready = "Neo4j cluster is fully formed and ready"
	us := cluster.Status.UpgradeStatus
	if us == nil || us.TargetVersion == "" || us.TargetVersion == cluster.Status.Version {
		return ready
	}
	switch {
	case upgradeStateMachineActive(cluster):
		return fmt.Sprintf("%s; its upgrade to %s is in progress (status.upgradeStatus)", ready, us.TargetVersion)
	case us.Phase == upgradePhasePaused:
		return fmt.Sprintf("%s; its upgrade to %s is paused (status.upgradeStatus): the servers it has not rolled keep the version they run until spec.image.tag changes or spec.upgradeStrategy.autoPauseOnFailure is set to false",
			ready, us.TargetVersion)
	case us.Phase == upgradePhaseFailed:
		return fmt.Sprintf("%s; its upgrade to %s failed (status.upgradeStatus), and the operator takes it up again while every server is available",
			ready, us.TargetVersion)
	}
	return ready
}

// imageInPlace is the image a cluster's servers run where the image hold reads
// it, and how many of them are ready.
type imageInPlace struct {
	image string
	ready int32
}

// serverImageInPlace returns the image the server StatefulSet's template
// carries and how many of its replicas are ready. While the StatefulSet does
// not exist but its pods do — a storage expansion orphan-deletes it and the
// next pass creates it again — it reads the orphaned server pods: the image of
// the highest ordinal, which carries the template the StatefulSet rolled out
// last (it updates pods from the highest ordinal down), and how many are
// ready. found is false when there is neither (a new cluster) or they cannot
// be read.
//
// Reading only the StatefulSet let a held image change through after an
// expansion: the hold found nothing, so the StatefulSet was created again with
// the spec's image and no partition, and every server rolled onto it with
// only the readiness probe between them, on a cluster that was not Ready
// (#538).
func (r *Neo4jEnterpriseClusterReconciler) serverImageInPlace(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (imageInPlace, bool) {
	stsName := fmt.Sprintf("%s-server", cluster.Name)
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: cluster.Namespace}, sts)
	switch {
	case err == nil:
		if len(sts.Spec.Template.Spec.Containers) == 0 {
			return imageInPlace{}, false
		}
		return imageInPlace{image: sts.Spec.Template.Spec.Containers[0].Image, ready: sts.Status.ReadyReplicas}, true
	case !apierrors.IsNotFound(err):
		return imageInPlace{}, false
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"neo4j.com/cluster":    cluster.Name,
		"neo4j.com/clustering": "true",
	}); err != nil {
		return imageInPlace{}, false
	}
	var place imageInPlace
	highest := -1
	for i := range pods.Items {
		pod := &pods.Items[i]
		ordinal, ok := statefulSetOrdinal(stsName, pod.Name)
		if !ok || pod.DeletionTimestamp != nil {
			continue
		}
		if isPodReady(pod) {
			place.ready++
		}
		if ordinal < highest {
			continue
		}
		for _, c := range pod.Spec.Containers {
			if c.Name == resources.Neo4jContainer {
				highest, place.image = ordinal, c.Image
			}
		}
	}
	return place, place.image != ""
}

// heldImageChange is what deferredUpgradeTargets keeps per cluster (under
// clusterKey): the image change last held back, why (holdCause*), and whether
// its UpgradeDeferred event is out. A drain's deferral is kept under
// drainDeferralKey (announceUpgradeDeferredByDrain).
type heldImageChange struct {
	image, cause string
	announced    bool
}

// holdCauseNotReady: the cluster is not Ready with every server available
// (holdImageDriftUntilReady).
const holdCauseNotReady = "not-ready"

// announceHeldImageChange emits the UpgradeDeferred event for an image change
// the normal pass holds back until the cluster is Ready with every server
// available, once per change, and only from the second pass that holds it.
// A hold the pass that met it ends is not a deferral worth an event: after a
// refused tag is fixed, the cluster's phase is still the refusal's Failed, so
// the first pass holds the new image, writes Ready, and the next pass starts
// the upgrade — it used to announce a deferral every time (#550). Nor is a
// change an upgrade the machine stopped already explains: its record names
// that target, announced with its UpgradePaused or UpgradeFailed.
func (r *Neo4jEnterpriseClusterReconciler) announceHeldImageChange(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, desiredImage string) {
	key := clusterKey(cluster)
	if upgradeHeld(cluster) && cluster.Status.UpgradeStatus.TargetVersion == cluster.Spec.Image.Tag {
		r.deferredUpgradeTargets.Store(key, heldImageChange{image: desiredImage, cause: holdCauseNotReady, announced: true})
		return
	}
	prev, _ := r.deferredUpgradeTargets.Load(key)
	held, _ := prev.(heldImageChange)
	switch {
	case held.image != desiredImage || held.cause != holdCauseNotReady:
		r.deferredUpgradeTargets.Store(key, heldImageChange{image: desiredImage, cause: holdCauseNotReady})
	case !held.announced:
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonUpgradeDeferred,
			"Image change to %s deferred until the cluster is Ready with every server available; the rolling-upgrade state machine will perform it", desiredImage)
		r.deferredUpgradeTargets.Store(key, heldImageChange{image: desiredImage, cause: holdCauseNotReady, announced: true})
	}
}
