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

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabasebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabasebackups/status,verbs=get;update;patch

// AuraDatabaseBackupReconciler takes an on-demand per-database backup on a Neo4j
// Aura instance via the Aura API v2beta1. BETA / best-effort. Like AuraSnapshot,
// backups are one-shot and are not deleted from Aura on CR deletion.
type AuraDatabaseBackupReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraDatabaseClientFactory
}

func (r *AuraDatabaseBackupReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the backup state machine.
func (r *AuraDatabaseBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	b := &neo4jv1beta1.AuraDatabaseBackup{}
	if err := r.Get(ctx, req.NamespacedName, b); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if b.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraDatabaseBackup reconciliation paused via annotation")
		return ctrl.Result{}, nil
	}
	if !b.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Resolve the target AuraDatabase → coordinates + database ID.
	db := &neo4jv1beta1.AuraDatabase{}
	if err := r.Get(ctx, types.NamespacedName{Name: b.Spec.DatabaseRef, Namespace: b.Namespace}, db); err != nil {
		return r.fail(ctx, req, b, "DatabaseRefUnresolved", fmt.Errorf("resolving databaseRef %q: %w", b.Spec.DatabaseRef, err))
	}
	databaseID := db.Annotations[AuraExternalDatabaseAnnotation]
	if databaseID == "" {
		databaseID = db.Status.DatabaseID
	}
	if databaseID == "" {
		_ = r.setCondition(ctx, req, "Ready", metav1.ConditionFalse, "DatabaseNotReady",
			fmt.Sprintf("AuraDatabase %q has no database ID yet", b.Spec.DatabaseRef))
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}
	creds, orgID, projectID, instanceID, err := resolveAuraDBCoords(ctx, r.Client, b.Namespace, db.Spec.InstanceRef, db.Spec.OrganizationID)
	if err != nil {
		return r.fail(ctx, req, b, "TargetUnresolved", err)
	}
	apiClient := resolveDatabaseClient(r.ClientFactory, creds)

	// Already taken? Observe it.
	backupID := b.Annotations[AuraExternalBackupAnnotation]
	if backupID == "" {
		backupID = b.Status.BackupID
	}
	if backupID != "" {
		observed, err := apiClient.GetDatabaseBackup(ctx, orgID, projectID, instanceID, databaseID, backupID)
		if err != nil {
			if aura.IsNotFound(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, b, "ObserveFailed", err)
		}
		return r.syncStatus(ctx, req, observed)
	}

	// Take it once.
	if !managementAllows(b.Spec.ManagementPolicies, auraPolicyCreate) {
		_ = r.setCondition(ctx, req, "Ready", metav1.ConditionFalse, "CreateNotAllowed",
			"managementPolicies does not permit Create")
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}
	created, err := apiClient.CreateDatabaseBackup(ctx, orgID, projectID, instanceID, databaseID)
	if err != nil {
		return r.fail(ctx, req, b, "BackupFailed", err)
	}
	if err := r.patch(ctx, req, func(o *neo4jv1beta1.AuraDatabaseBackup) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalBackupAnnotation] = created.ID
	}); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(b, corev1.EventTypeNormal, EventReasonAuraDatabaseBackupCreated,
		fmt.Sprintf("Created database backup %s", created.ID))
	if _, err := r.syncStatus(ctx, req, created); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraDatabaseBackupReconciler) syncStatus(ctx context.Context, req ctrl.Request, observed *aura.DatabaseBackup) (ctrl.Result, error) {
	// v2beta1 DatabaseBackup.status is a required enum:
	// Pending | InProgress | Completed | Failed.
	//
	// An EMPTY status must NOT be treated as done: the CREATE response
	// (CreateDatabaseBackupResponse) carries only `id`, so a just-scheduled
	// backup arrives here with no status at all. Treating "" as Completed is what
	// made every backup — including failed ones — report success.
	phase := "Pending"
	condStatus := metav1.ConditionFalse
	reason := "InProgress"
	message := "Database backup in progress"
	switch observed.Status {
	case aura.BackupStatusCompleted:
		phase, condStatus, reason = "Completed", metav1.ConditionTrue, "Completed"
		message = "Database backup completed (v2beta1, beta)"
	case aura.BackupStatusFailed:
		phase, condStatus, reason = "Failed", metav1.ConditionFalse, "Failed"
		message = "Aura reported the database backup as Failed"
	case aura.BackupStatusInProgress:
		reason, message = "InProgress", "Database backup in progress"
	case aura.BackupStatusPending, "":
		reason, message = "Pending", "Database backup scheduled, awaiting Aura status"
	default:
		reason = "Unknown"
		message = fmt.Sprintf("Aura reported an unrecognized backup status %q", observed.Status)
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseBackup{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.BackupID = observed.ID
		latest.Status.Timestamp = observed.Timestamp
		latest.Status.Exportable = observed.Exportable
		now := metav1.Now()
		latest.Status.LastSyncedTime = &now
		latest.Status.ObservedGeneration = latest.Generation
		latest.Status.Phase = phase
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: condStatus, Reason: reason, Message: message,
		})
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	// Completed and Failed are both terminal — stop requeueing. Anything else
	// (Pending, InProgress, empty, unrecognized) is still in flight.
	if phase == "Completed" || phase == "Failed" {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraDatabaseBackupReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraDatabaseBackup)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseBackup{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

func (r *AuraDatabaseBackupReconciler) setCondition(ctx context.Context, req ctrl.Request, condType string, status metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseBackup{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: condType, Status: status, Reason: reason, Message: msg,
		})
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

func (r *AuraDatabaseBackupReconciler) fail(ctx context.Context, req ctrl.Request, b *neo4jv1beta1.AuraDatabaseBackup, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraDatabaseBackup reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(b, corev1.EventTypeWarning, EventReasonAuraDatabaseBackupFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabaseBackup{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: cause.Error(),
		})
		latest.Status.Phase = "Error"
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraDatabaseBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraDatabaseBackup{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
