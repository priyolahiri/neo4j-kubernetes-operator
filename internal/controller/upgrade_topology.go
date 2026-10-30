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

// A change of spec.topology.servers waits for an upgrade (operations.md id
// 163).
//
// Neo4j's rolling upgrade restarts the servers it has, one at a time; it
// changes no topology. The operator surfaced a scale-down requested while the
// state machine rolled and ran its drain after the upgrade, but nothing else
// held a topology change back:
//
//   - Staging applied the builder's StatefulSet, whose replica count is
//     spec.topology.servers. A scale-down in the same apply as an image change
//     (or before a retarget) removed servers with no drain at all; a scale-up
//     added servers on the target mid-upgrade.
//   - Once the upgrade stopped Paused or Failed, the normal pass ran again: a
//     scale-up added servers that started on the held upgrade's target (the
//     pods at or above a StatefulSet partition get its update revision), and a
//     scale-down drain cordoned and deallocated servers, the rolled ones first,
//     during the hold. A server added while the upgrade had staged but not
//     restarted any server ran the target without the record knowing, so the
//     version rule still let the tag go back below it.
//
// So Staging keeps the replica count, and while a held upgrade keeps a
// partition, the normal pass works on the server count the StatefulSet has
// (the same in-memory hold holdImageDriftUntilReady applies to the image): the
// ConfigMap's discovery endpoints, the drain and the formation check all see
// the servers there are. The TopologyChangeDeferred condition says what waits.
// Once the upgrade completes (or the partition is gone), the change is applied
// as it would have been.

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// holdTopologyChange is the normal pass's hold on a change of
// spec.topology.servers: while a held upgrade keeps a partition
// (holdTopologyWhileUpgradeHolds, id 163), or, for a raise, while the pods and
// volumes of servers a scale-down removed are still there
// (holdRaiseForRemovedServers, id 229). Either keeps the in-memory
// spec.topology.servers at what the StatefulSet can take now and says so in
// the TopologyChangeDeferred condition; with nothing held, a condition that
// said a change waits is cleared.
func (r *Neo4jEnterpriseClusterReconciler) holdTopologyChange(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	requested := cluster.Spec.Topology.Servers
	if r.holdTopologyWhileUpgradeHolds(ctx, cluster) || r.holdRaiseForRemovedServers(ctx, cluster) {
		return
	}
	r.reportTopologyDeferral(ctx, cluster, requested, requested)
}

// holdTopologyWhileUpgradeHolds keeps the in-memory spec.topology.servers at
// the server StatefulSet's replica count while a Paused or Failed upgrade
// keeps a partition on it (keptUpgradePartition), so the rest of the normal
// pass changes no topology, records the wait in the TopologyChangeDeferred
// condition, and reports whether it held. A scale-down drain already in
// progress is left to finish (it holds the replicas itself).
func (r *Neo4jEnterpriseClusterReconciler) holdTopologyWhileUpgradeHolds(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	requested := cluster.Spec.Topology.Servers
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil || sts.Spec.Replicas == nil {
		return false
	}
	servers := *sts.Spec.Replicas
	if requested == servers || keptUpgradePartition(cluster, sts) == 0 || scaleDownDrainInProgress(cluster) {
		return false
	}
	log.FromContext(ctx).Info("Topology change deferred until the held upgrade completes",
		"requested", requested, "servers", servers, "upgrade", cluster.Status.UpgradeStatus.Phase,
		"target", cluster.Status.UpgradeStatus.TargetVersion)
	r.reportTopologyDeferral(ctx, cluster, requested, servers)
	cluster.Spec.Topology.Servers = servers
	return true
}

// topologyDeferral is the TopologyChangeDeferred condition for a cluster
// whose spec asks for requested servers while the server StatefulSet keeps
// servers: True while they differ, False otherwise.
func topologyDeferral(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, requested, servers int32) metav1.Condition {
	if requested == servers {
		return noTopologyChangeWaits(findCondition(cluster.Status.Conditions, ConditionTypeTopologyChangeDeferred))
	}
	target, phase := "", ""
	if us := cluster.Status.UpgradeStatus; us != nil {
		target, phase = us.TargetVersion, us.Phase
	}
	return metav1.Condition{
		Type:   ConditionTypeTopologyChangeDeferred,
		Status: metav1.ConditionTrue,
		Reason: ConditionReasonWaitsForUpgrade,
		Message: fmt.Sprintf("spec.topology.servers is %d; the server StatefulSet keeps %d servers until the upgrade to %s (%s, status.upgradeStatus) completes: a rolling upgrade changes no topology",
			requested, servers, target, phase),
	}
}

// noTopologyChangeWaits is the TopologyChangeDeferred condition once nothing
// waits. Its message names the wait that ended, from the condition it
// replaces (have): "for an upgrade", or "for the pods and volumes of removed
// servers" (id 229). The walk of the #619 fixes read "No change … waits for
// an upgrade" after a wait for removed servers. A condition that is False
// already keeps its message, so no pass rewrites it.
func noTopologyChangeWaits(have *metav1.Condition) metav1.Condition {
	message := "No change of spec.topology.servers waits for an upgrade"
	switch {
	case have != nil && have.Status == metav1.ConditionFalse && have.Message != "":
		message = have.Message
	case have != nil && have.Reason == ConditionReasonWaitsForRemovedServers:
		message = "No change of spec.topology.servers waits for the pods and volumes of removed servers"
	}
	return metav1.Condition{
		Type:    ConditionTypeTopologyChangeDeferred,
		Status:  metav1.ConditionFalse,
		Reason:  ConditionReasonNoTopologyChangeWaits,
		Message: message,
	}
}

// reportTopologyDeferral writes the TopologyChangeDeferred condition when it
// changes (a status write re-queues the cluster): True with the counts while
// they differ; once they agree, False, and only if the condition was there.
// It is written on a copy re-read inside RetryOnConflict and mirrored onto the
// pass's copy. Non-fatal.
func (r *Neo4jEnterpriseClusterReconciler) reportTopologyDeferral(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, requested, servers int32) {
	r.writeTopologyDeferral(ctx, cluster, topologyDeferral(cluster, requested, servers))
}

// writeTopologyDeferral writes want as the TopologyChangeDeferred condition
// when the pass's copy differs from it (a False one only over a condition
// that is there), on a copy re-read inside RetryOnConflict, and mirrors it
// onto the pass's copy. It returns the condition as stored before the write,
// nil when absent, and whether it wrote: what a caller announces from.
// Non-fatal.
func (r *Neo4jEnterpriseClusterReconciler) writeTopologyDeferral(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, want metav1.Condition) (*metav1.Condition, bool) {
	have := findCondition(cluster.Status.Conditions, ConditionTypeTopologyChangeDeferred)
	if have == nil && want.Status == metav1.ConditionFalse {
		return nil, false
	}
	if have != nil && have.Status == want.Status && have.Reason == want.Reason && have.Message == want.Message {
		return have.DeepCopy(), false
	}
	var before *metav1.Condition
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		before = nil
		if stored := findCondition(latest.Status.Conditions, want.Type); stored != nil {
			before = stored.DeepCopy()
		}
		SetNamedCondition(&latest.Status.Conditions, want.Type, cluster.Generation, want.Status, want.Reason, want.Message)
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to record the TopologyChangeDeferred condition (non-fatal)")
		return nil, false
	}
	SetNamedCondition(&cluster.Status.Conditions, want.Type, cluster.Generation, want.Status, want.Reason, want.Message)
	return before, true
}
