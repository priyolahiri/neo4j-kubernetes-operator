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
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// targetUnreachableGrace is how long an object that depends on a clusterRef
// target — a Neo4jUser, Neo4jRole, Neo4jRoleBinding or Neo4jAuthRule, and a
// Neo4jDatabase, Neo4jCompositeDatabase, Neo4jDatabaseAlias,
// Neo4jReplicaDatabase or Neo4jShardedDatabase — waits quietly on a target that
// reads Ready but cannot be reached. A standalone reads Ready a few seconds
// before its Bolt port accepts connections, and a stopCluster restore stops it
// for longer; past this, the connection failure is the object's own and is
// reported (knowledge rules 172 and 183).
const targetUnreachableGrace = 5 * time.Minute

// recordTargetWait writes, into the status a Neo4jUser, Neo4jRole,
// Neo4jRoleBinding, Neo4jAuthRule, Neo4jCompositeDatabase or
// Neo4jDatabaseAlias reconcile re-read for its write, that the object's
// clusterRef target cannot serve it: the ClusterNotReady condition with reason
// (ClusterNotReady: the target is not Ready; ClusterUnreachable: it reads Ready
// but cannot be reached), and phase Pending with a Ready condition False for
// that reason — unless the object's status already records an outcome of its
// own for its generation, which it keeps (settledAtGeneration).
//
// What such an object reports — a user, a role, a grant, a database, an alias —
// lives in the target and does not change while the target restarts, and the
// target announces its own state. Demoting a Ready object to Pending for the
// restart made the pass that found the target back a transition into Ready, so
// every restart of a standalone was a UserReady (RoleReady, BindingCreated)
// event: 28 UserReady in 40 minutes of restarts on the v1.22.0 re-walk
// (knowledge rule 158). A Failed one was demoted the same way, so the pass
// that met the same failure again was a transition back into Failed: a
// UserSyncFailed after every standalone restart (#573), and a binding's
// UserNotFound likewise (knowledge rule 189). An object with no outcome at its
// generation — a new one, one whose spec changed, one already waiting on its
// target — still waits in Pending (knowledge rule 183).
//
// It reports lasting when the target has been unreachable for longer than
// targetUnreachableGrace, measured from the condition's LastTransitionTime; it
// then leaves the phase alone, for the caller to record the failure.
func recordTargetWait(conditions *[]metav1.Condition, phase, message *string, generation int64,
	reason, msg string, now time.Time,
) (lasting bool) {
	if lasting, _ = noteTargetWait(conditions, *phase, generation, neo4jv1beta1.PhaseReady, reason, msg, now); lasting {
		return true
	}
	if settledAtGeneration(*phase, *conditions, generation) {
		return false
	}
	SetReadyCondition(conditions, generation, metav1.ConditionFalse, reason, msg)
	*phase = neo4jv1beta1.PhasePending
	*message = msg
	return false
}

// noteTargetWait is the part of a target wait every dependent kind shares: it
// sets the ClusterNotReady condition, and reports whether the object keeps its
// phase, message and Ready condition through the wait (keepsReady: it is Ready
// at its generation in readyPhase, the phase its kind is Ready in — Ready, or
// Replicating for a Neo4jReplicaDatabase) and whether an unreachable target has
// outlasted targetUnreachableGrace (lasting, checked first). A caller whose
// object does not keep its phase records the wait in its kind's own waiting
// phase: Pending, or Waiting for a Neo4jShardedDatabase.
func noteTargetWait(conditions *[]metav1.Condition, phase string, generation int64,
	readyPhase, reason, msg string, now time.Time,
) (lasting, keepsReady bool) {
	SetNamedCondition(conditions, ConditionTypeClusterNotReady, generation, metav1.ConditionTrue, reason, msg)
	if reason == ConditionReasonClusterUnreachable {
		if c := findCondition(*conditions, ConditionTypeClusterNotReady); c != nil &&
			now.Sub(c.LastTransitionTime.Time) > targetUnreachableGrace {
			return true, false
		}
	}
	return false, readyAtGeneration(readyPhase, phase, *conditions, generation)
}

// targetAnswered records, after a connection to the target succeeded, that the
// target serves the object: ClusterNotReady False. It reports whether that
// changed the conditions, so a caller writes only then. Written before the
// connection, it restarted the clock of an unreachable target's wait on every
// pass (knowledge rule 172).
func targetAnswered(conditions *[]metav1.Condition, generation int64) bool {
	before := findCondition(*conditions, ConditionTypeClusterNotReady)
	if before != nil && before.Status == metav1.ConditionFalse && before.Reason == "ClusterReady" &&
		before.Message == "" && before.ObservedGeneration == generation {
		return false
	}
	SetNamedCondition(conditions, ConditionTypeClusterNotReady, generation, metav1.ConditionFalse, "ClusterReady", "")
	return true
}

// settledAtGeneration reports whether the persisted status records an outcome
// of the object's own for its current generation, which a target restart does
// not change: Ready at this generation (readyAtGeneration), or a Ready
// condition the phase writers stamped at this generation for a cause of the
// object's own — a failure (UserSyncFailed, a refused grant, a lasting
// ConnectionFailed) or a wait on something other than the target (a binding's
// UserNotFound, roles pending). A wait on the target itself (ClusterNotReady,
// ClusterUnreachable, ClusterNotFound), and the password Secret wait that runs
// before the target is looked at, are not outcomes.
func settledAtGeneration(phase string, conditions []metav1.Condition, generation int64) bool {
	if readyAtGeneration(neo4jv1beta1.PhaseReady, phase, conditions, generation) {
		return true
	}
	if phase == "" || phase == "Ready" {
		return false
	}
	ready := findCondition(conditions, ConditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.ObservedGeneration != generation {
		return false
	}
	switch ready.Reason {
	case ConditionReasonClusterNotReady, ConditionReasonClusterUnreachable, EventReasonClusterNotFound, ConditionReasonSecretPending:
		return false
	}
	return true
}

// readyAtGeneration reports whether the persisted status says Ready for the
// object's current generation: phase readyPhase (Ready, or Replicating for a
// Neo4jReplicaDatabase), and a Ready condition True that a Ready write stamped
// at this generation (only the phase writers set the Ready condition, so a
// named condition's write at a new generation does not count).
func readyAtGeneration(readyPhase, phase string, conditions []metav1.Condition, generation int64) bool {
	if phase != readyPhase {
		return false
	}
	ready := findCondition(conditions, ConditionTypeReady)
	return ready != nil && ready.Status == metav1.ConditionTrue && ready.ObservedGeneration == generation
}

// targetUnreachable reports an error that says the target cannot serve the
// operator right now: its Bolt port refuses, resets or times out
// (IsConnectivityError), its address does not resolve, or its system database
// is not available yet. A target that reads Ready a few seconds before it
// accepts connections — a standalone after a restart, during a stopCluster
// restore — produces these, and they are the rest of the same restart, not a
// failure of the object being reconciled (knowledge rule 172).
func targetUnreachable(err error) bool {
	if err == nil {
		return false
	}
	return neo4jclient.IsConnectivityError(err) || neo4jclient.IsHostUnresolvableError(err) ||
		strings.Contains(err.Error(), "Neo.TransientError.General.DatabaseUnavailable")
}

// targetUnreachableMessage is the ClusterNotReady (and, for an object not yet
// Ready, the status) message while the target cannot be reached.
func targetUnreachableMessage(clusterRef string, err error) string {
	return fmt.Sprintf("%s reads Ready but cannot be reached: %v", targetRefDisplay(clusterRef), err)
}

// targetAway reports an error that says the target cannot serve a database
// kind's request for now: it cannot be reached (targetUnreachable), or Neo4j
// answered with an error it asks to be retried (statementTransient) — what a
// cluster that has lost the majority of its servers, and still reads Ready,
// answers: Neo.TransientError.Statement.RemoteExecutionTransientError,
// "Failed to obtain connection towards READ server". A Neo.TransientError
// whose type a wrap lost still counts, by its code, except the two the driver
// reclassifies as client errors (Transaction.Terminated, LockClientStopped).
// The Neo4jDatabase, Neo4jDatabaseAlias, Neo4jCompositeDatabase,
// Neo4jReplicaDatabase and Neo4jShardedDatabase wait such an error out as they
// wait out an unreachable target (knowledge rules 183, 210).
func targetAway(err error) bool {
	if err == nil {
		return false
	}
	if targetUnreachable(err) || statementTransient(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "Neo.TransientError.") &&
		!strings.Contains(msg, "Neo.TransientError.Transaction.Terminated") &&
		!strings.Contains(msg, "Neo.TransientError.Transaction.LockClientStopped")
}

// targetAwayMessage is the ClusterNotReady (and, for an object not yet Ready,
// the status) message while the target cannot serve the object (targetAway).
func targetAwayMessage(clusterRef string, err error) string {
	if targetUnreachable(err) {
		return targetUnreachableMessage(clusterRef, err)
	}
	return fmt.Sprintf("%s reads Ready but cannot serve the request: %v", targetRefDisplay(clusterRef), err)
}
