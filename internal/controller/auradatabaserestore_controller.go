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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabaserestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabaserestores/status,verbs=get;update;patch

// AuraDatabaseRestoreReconciler performs a one-shot in-place restore of a
// database on a Neo4j Aura instance from one of its per-database backups via the
// Aura API v2beta1. BETA / best-effort.
type AuraDatabaseRestoreReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraDatabaseClientFactory
}

func (r *AuraDatabaseRestoreReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives the one-shot restore.
func (r *AuraDatabaseRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	rs := &neo4jv1beta1.AuraDatabaseRestore{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if rs.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraDatabaseRestore reconciliation paused via annotation")
		return ctrl.Result{}, nil
	}
	if !rs.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// One-shot: never re-run after a terminal outcome.
	//
	// "Submitted" is the terminal success phase — see markSubmitted, which cannot
	// honestly write "Completed" because v2beta1 exposes no way to observe a
	// restore finishing. This guard previously listed only "Completed", a phase
	// nothing ever writes, so it never fired: any later reconcile (operator
	// restart, cache resync, any watch event) submitted the SAME restore again,
	// silently overwriting a database that had already been restored.
	switch rs.Status.Phase {
	case auraRestorePhaseSubmitted, "Completed", auraRestorePhaseError:
		return ctrl.Result{}, nil
	case auraRestorePhaseSubmitting:
		// The process stopped between persisting this marker and learning the
		// outcome, so we do not know whether Aura accepted the restore. Re-sending
		// it could overwrite a database a second time; assuming it succeeded could
		// leave a requested restore undone. Aura offers no idempotency key and no
		// way to read a restore's state, so this cannot be resolved automatically —
		// stop and say so, rather than guess with someone's data.
		return ctrl.Result{}, r.markOutcomeUnknown(ctx, req)
	}
	if !managementAllows(rs.Spec.ManagementPolicies, auraPolicyCreate) {
		return ctrl.Result{}, nil
	}

	// Resolve target coordinates via the referenced AuraDatabase.
	db := &neo4jv1beta1.AuraDatabase{}
	if err := r.Get(ctx, types.NamespacedName{Name: rs.Spec.DatabaseRef, Namespace: rs.Namespace}, db); err != nil {
		return r.fail(ctx, req, rs, "DatabaseRefUnresolved", fmt.Errorf("resolving databaseRef %q: %w", rs.Spec.DatabaseRef, err))
	}
	databaseID := db.Annotations[AuraExternalDatabaseAnnotation]
	if databaseID == "" {
		databaseID = db.Status.DatabaseID
	}
	if databaseID == "" {
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}

	// Resolve the backup ID (explicit, or from an AuraDatabaseBackup).
	backupID := rs.Spec.BackupID
	if backupID == "" && rs.Spec.BackupRef != "" {
		bk := &neo4jv1beta1.AuraDatabaseBackup{}
		if err := r.Get(ctx, types.NamespacedName{Name: rs.Spec.BackupRef, Namespace: rs.Namespace}, bk); err != nil {
			return r.fail(ctx, req, rs, "BackupRefUnresolved", fmt.Errorf("resolving backupRef %q: %w", rs.Spec.BackupRef, err))
		}
		backupID = bk.Status.BackupID
		if backupID == "" {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
	}

	creds, orgID, projectID, instanceID, err := resolveAuraDBCoords(ctx, r.Client, rs.Namespace, db.Spec.InstanceRef, db.Spec.OrganizationID)
	if err != nil {
		return r.fail(ctx, req, rs, "TargetUnresolved", err)
	}
	apiClient := resolveDatabaseClient(r.ClientFactory, creds)

	r.markStarted(ctx, req)
	r.Recorder.Event(rs, corev1.EventTypeNormal, EventReasonAuraDatabaseRestoreStarted,
		fmt.Sprintf("Restoring database %s from backup %s", databaseID, backupID))

	// Persist the ambiguous marker BEFORE the call, so a crash mid-flight is
	// recognisable rather than replayed. If even this write fails, do not call:
	// an unrecorded attempt is exactly what we are protecting against.
	if err := r.markSubmitting(ctx, req); err != nil {
		return ctrl.Result{}, err
	}
	accepted, err := apiClient.RestoreDatabase(ctx, orgID, projectID, instanceID, databaseID, aura.RestoreDatabaseRequest{BackupID: backupID})
	if err != nil {
		// 409 here is retryable, not terminal: Aura returns it for "ongoing
		// operation" and "backup not in a completed state".
		if aura.IsConflict(err) || aura.IsTransient(err) {
			// Aura reported a failure, so clear the marker to allow the retry. A 5xx
			// is the residual risk — it says the server erred, not that nothing was
			// applied — but this is the behaviour that already shipped, and the case
			// the marker exists for is process death, where no error is seen at all.
			r.markPhase(ctx, req, auraRestorePhasePending)
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, rs, "RestoreFailed", err)
	}
	r.Recorder.Event(rs, corev1.EventTypeNormal, EventReasonAuraDatabaseRestoreDone,
		fmt.Sprintf("Submitted restore of database %s from backup %s", databaseID, backupID))
	return ctrl.Result{}, r.markSubmitted(ctx, req, accepted)
}

// Restore phases. "Submitting" is the ambiguous one: written before the API call
// and cleared only by a known outcome, so finding it on a fresh reconcile means an
// attempt was in flight when the operator stopped.
const (
	auraRestorePhasePending    = "Pending"
	auraRestorePhaseRestoring  = "Restoring"
	auraRestorePhaseSubmitting = "Submitting"
	auraRestorePhaseSubmitted  = "Submitted"
	auraRestorePhaseError      = "Error"
)

// markPhase sets the phase, best-effort. Used for the retry-allowing reset, where
// a failed write simply leaves the safe (blocking) marker in place.
func (r *AuraDatabaseRestoreReconciler) markPhase(ctx context.Context, req ctrl.Request, phase string) {
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.Phase = phase
		return r.Status().Update(ctx, latest)
	})
}

// markSubmitting persists the ambiguous marker. Unlike the other status writers
// this one's error is propagated: the whole point is that the marker exists before
// the destructive call.
func (r *AuraDatabaseRestoreReconciler) markSubmitting(ctx context.Context, req ctrl.Request) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.Phase = auraRestorePhaseSubmitting
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

// markOutcomeUnknown reports an attempt whose result cannot be determined. It is
// terminal on purpose: only a human can check the Aura console and decide whether
// to re-run, and re-running is destructive.
func (r *AuraDatabaseRestoreReconciler) markOutcomeUnknown(ctx context.Context, req ctrl.Request) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: "RestoreOutcomeUnknown",
			Message: "A restore was in flight when the operator stopped, and the Aura v2beta1 API offers no way " +
				"to read a restore's state, so it is unknown whether it was applied. Not retrying: a repeated " +
				"restore overwrites the database again. Check the database in the Aura console, then delete this " +
				"CR and recreate it if the restore still needs to run.",
		})
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

func (r *AuraDatabaseRestoreReconciler) markStarted(ctx context.Context, req ctrl.Request) {
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		if latest.Status.StartedAt == nil {
			now := metav1.Now()
			latest.Status.StartedAt = &now
		}
		latest.Status.Phase = auraRestorePhaseRestoring
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

// markSubmitted records that the restore was accepted by Aura.
//
// The phase is deliberately "Submitted", NOT "Completed": the restore runs
// asynchronously and v2beta1 gives us nothing to poll — the spec suggests
// polling the database GET endpoint, but DatabaseSummary carries only an `id`,
// with no status field, so completion is not observable through this API.
// Claiming "Completed" here would assert something we cannot know.
func (r *AuraDatabaseRestoreReconciler) markSubmitted(ctx context.Context, req ctrl.Request, accepted *aura.DatabaseRestoreAccepted) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		now := metav1.Now()
		latest.Status.FinishedAt = &now
		latest.Status.Phase = auraRestorePhaseSubmitted
		latest.Status.ObservedGeneration = latest.Generation
		// The 202 body is the ONLY state this API ever reports for a restore, so
		// fold what it said into the message instead of discarding it. GET and
		// LIST return just an id, even mid-restore, which is why completion still
		// has to be confirmed in the console.
		msg := "Restore accepted by Aura. Completion is not observable: the v2beta1 database " +
			"endpoint exposes no status field, so verify in the Aura console (v2beta1, beta)"
		if accepted != nil && accepted.Status != "" {
			msg = fmt.Sprintf("Restore accepted by Aura; database reported %q at acceptance "+
				"(%d nodes, %d relationships). Completion is not observable — the v2beta1 database "+
				"endpoint exposes no status field, so verify in the Aura console (v2beta1, beta)",
				accepted.Status, accepted.Nodes, accepted.Relationships)
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Submitted",
			Message: msg,
		})
		return r.Status().Update(ctx, latest)
	})
}

func (r *AuraDatabaseRestoreReconciler) fail(ctx context.Context, req ctrl.Request, rs *neo4jv1beta1.AuraDatabaseRestore, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraDatabaseRestore reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(rs, corev1.EventTypeWarning, EventReasonAuraDatabaseRestoreFailed, fmt.Sprintf("%s: %v", reason, cause))
	terminal := reason == "RestoreFailed"
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: cause.Error(),
		})
		if terminal {
			latest.Status.Phase = auraRestorePhaseError
			now := metav1.Now()
			latest.Status.FinishedAt = &now
		}
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
	if terminal {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraDatabaseRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraDatabaseRestore{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
