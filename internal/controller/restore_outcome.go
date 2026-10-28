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

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// restoreContainerName is the restore Job's one container.
const restoreContainerName = "neo4j-restore"

// restoreRefusalError marks an error with which a restore's preflight refuses
// the spec as it is — a database that exists without replaceExisting, an
// offline restore into a running standalone without stopCluster — as opposed
// to one it met reading what it needs.
type restoreRefusalError struct{ error }

func (e restoreRefusalError) Unwrap() error { return e.error }

// restoreRefusal marks err as a refusal of the restore's spec.
func restoreRefusal(err error) error { return restoreRefusalError{err} }

// isRestoreRefusal reports whether err is a refusal of the restore's spec.
func isRestoreRefusal(err error) bool {
	var refusal restoreRefusalError
	return stderrors.As(err, &refusal)
}

// apiReadFailure reports whether err carries an API error other than NotFound:
// a read the validation could not make, which is the controller's error, not a
// refusal of the spec.
func apiReadFailure(err error) bool {
	var status apierrors.APIStatus
	return stderrors.As(err, &status) && !apierrors.IsNotFound(err)
}

// refuseRestore ends an attempt its spec refuses, or its target as the spec
// finds it: phase Failed with statusMsg, logged by logRefusal from that write
// — at info when the write records the refusal, at V(1) after. It reports
// whether the write announced it. The caller returns no error: the next pass
// returns on the Failed restore at its generation, so a returned error was
// only a second ERROR line ("Reconciler error") and a retry that did nothing
// (knowledge operations.md id 215).
func (r *Neo4jRestoreReconciler) refuseRestore(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, logMsg, statusMsg string, err error) (announced bool) {
	announced = r.transitionRestoreStatus(ctx, restore, StatusFailed, statusMsg)
	logRefusal(log.FromContext(ctx), announced, logMsg, err)
	return announced
}

// restoreEndedAtGeneration reports whether the restore ended — Completed or
// Failed — at its current generation, and in which phase. Such a restore is
// not run again until its spec changes (a new generation) or it is created
// again; Reconcile returns on it before it looks the target up, with no
// requeue, no write and nothing logged at info (knowledge ids 198, 215).
func restoreEndedAtGeneration(restore *neo4jv1beta1.Neo4jRestore) (bool, string) {
	phase := restore.Status.Phase
	if phase != StatusCompleted && phase != StatusFailed {
		return false, ""
	}
	return restore.Status.ObservedGeneration == restore.Generation, phase
}

// restoreReadyCondition is the Ready condition of a restore in phase: reasons
// of a restore, not the backup's BackupSucceeded or the generic
// ReconciliationFailed PhaseToConditionStatus gives (knowledge id 215).
func restoreReadyCondition(phase string) (metav1.ConditionStatus, string) {
	switch phase {
	case StatusCompleted:
		return metav1.ConditionTrue, ConditionReasonRestoreComplete
	case StatusFailed:
		return metav1.ConditionFalse, ConditionReasonRestoreFailed
	case StatusRunning:
		return metav1.ConditionUnknown, ConditionReasonRestoreRunning
	default:
		return PhaseToConditionStatus(phase)
	}
}

// restoreJobFailure says why a restore Job failed, for status.message and the
// RestoreFailed event (jobFailureMessage). "Restore job failed" alone left the
// reader to find the pod before its TTL removed it (knowledge id 215).
func (r *Neo4jRestoreReconciler) restoreJobFailure(ctx context.Context, job *batchv1.Job) string {
	return jobFailureMessage(ctx, r.Client, "Restore job", job, restoreContainerName)
}
