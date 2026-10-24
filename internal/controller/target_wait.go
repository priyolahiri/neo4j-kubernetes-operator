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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// recordTargetWait writes, into the status a Neo4jUser, Neo4jRole,
// Neo4jRoleBinding or Neo4jAuthRule reconcile re-read for its write, that the
// object's clusterRef target is not Ready: the ClusterNotReady condition, and
// phase Pending with a Ready condition False/ClusterNotReady — unless the
// object is already Ready at its generation, which it stays.
//
// What such an object reports — a user, a role, a grant — lives in the target's
// system database and does not change while the target restarts, and the
// target announces its own state. Demoting a Ready object to Pending for the
// restart made the pass that found the target back a transition into Ready, so
// every restart of a standalone was a UserReady (RoleReady, BindingCreated)
// event: 28 UserReady in 40 minutes of restarts on the v1.22.0 re-walk. An
// object not yet Ready at its generation — a new one, one whose spec changed,
// one that failed — still waits in Pending (knowledge rule 158).
func recordTargetWait(conditions *[]metav1.Condition, phase, message *string, generation int64, msg string) {
	SetNamedCondition(conditions, ConditionTypeClusterNotReady, generation, metav1.ConditionTrue,
		ConditionReasonClusterNotReady, msg)
	if readyAtGeneration(*phase, *conditions, generation) {
		return
	}
	SetReadyCondition(conditions, generation, metav1.ConditionFalse, ConditionReasonClusterNotReady, msg)
	*phase = "Pending"
	*message = msg
}

// readyAtGeneration reports whether the persisted status says Ready for the
// object's current generation: phase Ready, and a Ready condition True that a
// Ready write stamped at this generation (only the phase writers set the Ready
// condition, so a named condition's write at a new generation does not count).
func readyAtGeneration(phase string, conditions []metav1.Condition, generation int64) bool {
	if phase != "Ready" {
		return false
	}
	ready := findCondition(conditions, ConditionTypeReady)
	return ready != nil && ready.Status == metav1.ConditionTrue && ready.ObservedGeneration == generation
}
