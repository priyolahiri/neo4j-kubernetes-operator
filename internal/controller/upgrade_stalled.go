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

// The Stalled condition of a cluster whose upgrade stopped (operations.md id
// 149).
//
// A stopped upgrade leaves the cluster's phase as it is — Ready while every
// server is available, since it serves — and its Ready condition True (#565).
// kstatus, which Flux's health checks use, reads Ready=True as Current, so a
// Flux user no longer saw that the image change did not land; v1.22.0 turned
// the phase Failed, and the Ready condition False. kstatus reads a Stalled
// condition first: Stalled=True is Failed, whatever Ready says, and a Stalled
// condition that is False, or none, leaves the decision to Ready
// (checkGenericProperties in pkg/kstatus/status/generic.go of
// sigs.k8s.io/cli-utils, and the same in Flux's fork, github.com/fluxcd/
// cli-utils). So the cluster carries Stalled=True while its upgrade record is
// Paused or Failed, and the condition goes False when the upgrade goes on,
// completes or is dropped. It is set in the status write that records the
// upgrade's state, so it costs no write and no event of its own (id 136).

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// syncStalledCondition sets cluster's Stalled condition from its upgrade
// record: True while the record is Paused or Failed, with the stopped upgrade's
// cause; False once it is not, if the condition was set; absent on a cluster
// whose upgrade never stopped. It reports whether the condition changed.
func syncStalledCondition(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	us := cluster.Status.UpgradeStatus
	conditions := &cluster.Status.Conditions
	if us != nil && (us.Phase == upgradePhaseFailed || us.Phase == upgradePhasePaused) {
		reason, verb, next := ConditionReasonUpgradeFailed, "failed",
			"the operator retries it while every server is available; spec.image.tag set to another target goes on at once"
		if us.Phase == upgradePhasePaused {
			reason, verb, next = ConditionReasonUpgradePaused, "is paused",
				"set spec.image.tag to another target, or spec.upgradeStrategy.autoPauseOnFailure to false to retry it"
		}
		message := fmt.Sprintf("Upgrade to %s %s (status.upgradeStatus): %s", us.TargetVersion, verb, next)
		if us.LastError != "" {
			message = fmt.Sprintf("Upgrade to %s %s: %s (status.upgradeStatus.lastError); %s", us.TargetVersion, verb, us.LastError, next)
		}
		return SetNamedCondition(conditions, ConditionTypeStalled, cluster.Generation, metav1.ConditionTrue, reason, message)
	}
	if findCondition(*conditions, ConditionTypeStalled) == nil {
		return false
	}
	return SetNamedCondition(conditions, ConditionTypeStalled, cluster.Generation, metav1.ConditionFalse,
		ConditionReasonUpgradeNotStopped, "No upgrade is stopped")
}
