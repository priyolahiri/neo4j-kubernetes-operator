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
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// AuraDatabaseFinalizer guards the operator's chance to delete (or orphan) the
// Aura database before the CR is removed.
const AuraDatabaseFinalizer = "neo4j.com/auradatabase-finalizer"

// auraDatabaseTarget carries the resolved coordinates for an Aura database.
type auraDatabaseTarget struct {
	creds      auraCredentials
	orgID      string
	projectID  string
	instanceID string
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabases/finalizers,verbs=update

// AuraDatabaseReconciler manages a database on a Neo4j Aura instance via the Aura
// API v2beta1. BETA / best-effort (see internal/aura/database_v2beta1.go).
type AuraDatabaseReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraDatabaseClientFactory
}

func (r *AuraDatabaseReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *AuraDatabaseReconciler) dbName(d *neo4jv1beta1.AuraDatabase) string {
	if d.Spec.Name != "" {
		return d.Spec.Name
	}
	return d.Name
}

// resolveTarget derives credentials + org/project/instance IDs from the
// referenced AuraInstance.
func (r *AuraDatabaseReconciler) resolveTarget(ctx context.Context, d *neo4jv1beta1.AuraDatabase) (auraDatabaseTarget, error) {
	creds, orgID, projectID, instanceID, err := resolveAuraDBCoords(ctx, r.Client, d.Namespace, d.Spec.InstanceRef, d.Spec.OrganizationID)
	return auraDatabaseTarget{creds: creds, orgID: orgID, projectID: projectID, instanceID: instanceID}, err
}

// Reconcile drives one pass of the database state machine.
func (r *AuraDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	d := &neo4jv1beta1.AuraDatabase{}
	if err := r.Get(ctx, req.NamespacedName, d); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if d.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraDatabase reconciliation paused via annotation")
		return ctrl.Result{}, nil
	}

	tgt, err := r.resolveTarget(ctx, d)
	if err != nil {
		// Deletion can still proceed best-effort even if the target is unresolved.
		if !d.DeletionTimestamp.IsZero() {
			return r.handleDeletion(ctx, req, d, nil, tgt)
		}
		return r.fail(ctx, req, d, "TargetUnresolved", err)
	}
	apiClient := resolveDatabaseClient(r.ClientFactory, tgt.creds)

	if !d.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, d, apiClient, tgt)
	}

	if !controllerutil.ContainsFinalizer(d, AuraDatabaseFinalizer) {
		if err := r.patch(ctx, req, func(o *neo4jv1beta1.AuraDatabase) {
			controllerutil.AddFinalizer(o, AuraDatabaseFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	externalID := d.Annotations[AuraExternalDatabaseAnnotation]
	if externalID == "" {
		externalID = d.Status.DatabaseID
	}
	if externalID == "" {
		allowCreate := managementAllows(d.Spec.ManagementPolicies, auraPolicyCreate)
		id, adopted, err := r.observeOrCreate(ctx, req, d, apiClient, tgt, allowCreate)
		if err != nil {
			return r.fail(ctx, req, d, "CreateFailed", err)
		}
		if id == "" {
			_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "AwaitingDatabase",
				"no matching database found and managementPolicies does not permit Create")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		externalID = id
		if adopted {
			r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseAdopted,
				fmt.Sprintf("Adopted existing database %s", externalID))
		}
	}

	observed, err := apiClient.GetDatabase(ctx, tgt.orgID, tgt.projectID, tgt.instanceID, externalID)
	if err != nil {
		if aura.IsNotFound(err) || aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, d, "ObserveFailed", err)
	}

	if err := r.syncStatus(ctx, req, observed); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseReady,
		fmt.Sprintf("Database %s reconciled", observed.ID))
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// observeOrCreate creates the database unless the external-ID annotation already
// pins one.
//
// NOTE: adoption-by-name is impossible on this API. The v2beta1 DatabaseSummary
// carries ONLY an `id` — no name, no status — so a pre-existing Aura database
// cannot be matched against spec.name. The external-ID annotation set by
// setExternalID is therefore the ONLY adoption mechanism; if it is lost, this
// will create a second database rather than re-adopt. We still list first, so we
// can warn when creating into a non-empty instance instead of doing it silently.
func (r *AuraDatabaseReconciler) observeOrCreate(
	ctx context.Context, req ctrl.Request, d *neo4jv1beta1.AuraDatabase,
	apiClient auraDatabaseAPI, tgt auraDatabaseTarget, allowCreate bool,
) (id string, adopted bool, err error) {
	name := r.dbName(d)
	existing, err := apiClient.ListDatabases(ctx, tgt.orgID, tgt.projectID, tgt.instanceID)
	if err != nil {
		return "", false, fmt.Errorf("listing databases before create: %w", err)
	}
	if !allowCreate {
		return "", false, nil
	}
	if len(existing) > 0 {
		r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseCreated,
			fmt.Sprintf("Creating database %q on an instance that already has %d database(s): the Aura API does not "+
				"return database names, so an existing database cannot be adopted by name. Verify no duplicate was created.",
				name, len(existing)))
	}
	created, err := apiClient.CreateDatabase(ctx, tgt.orgID, tgt.projectID, tgt.instanceID, aura.CreateDatabaseRequest{Name: name})
	if err != nil {
		return "", false, err
	}
	if err := r.setExternalID(ctx, req, created.ID); err != nil {
		return "", false, err
	}
	r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseCreated,
		fmt.Sprintf("Created database %s", created.ID))
	return created.ID, false, nil
}

func (r *AuraDatabaseReconciler) handleDeletion(
	ctx context.Context, req ctrl.Request, d *neo4jv1beta1.AuraDatabase, apiClient auraDatabaseAPI, tgt auraDatabaseTarget,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(d, AuraDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}
	externalID := d.Annotations[AuraExternalDatabaseAnnotation]
	if externalID == "" {
		externalID = d.Status.DatabaseID
	}
	deleteCloud := d.Spec.DeletionPolicy != "Orphan" && managementAllows(d.Spec.ManagementPolicies, auraPolicyDelete)
	if deleteCloud && externalID != "" && apiClient != nil && tgt.orgID != "" && tgt.projectID != "" && tgt.instanceID != "" {
		if err := apiClient.DeleteDatabase(ctx, tgt.orgID, tgt.projectID, tgt.instanceID, externalID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if !aura.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseDeleted,
			fmt.Sprintf("Deleted database %s", externalID))
	} else if externalID != "" {
		r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseOrphaned,
			fmt.Sprintf("Orphaning database %s; it stays in place in Aura", externalID))
	}
	return ctrl.Result{}, r.patch(ctx, req, func(o *neo4jv1beta1.AuraDatabase) {
		controllerutil.RemoveFinalizer(o, AuraDatabaseFinalizer)
	})
}

func (r *AuraDatabaseReconciler) setExternalID(ctx context.Context, req ctrl.Request, id string) error {
	return r.patch(ctx, req, func(o *neo4jv1beta1.AuraDatabase) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalDatabaseAnnotation] = id
	})
}

func (r *AuraDatabaseReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraDatabase)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabase{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

func (r *AuraDatabaseReconciler) setCondition(ctx context.Context, req ctrl.Request, condType string, status metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabase{}
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

func (r *AuraDatabaseReconciler) syncStatus(ctx context.Context, req ctrl.Request, observed *aura.Database) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabase{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.DatabaseID = observed.ID
		latest.Status.Phase = "Ready"
		now := metav1.Now()
		latest.Status.LastSyncedTime = &now
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Reconciled",
			Message: "Database reconciled against the Aura API (v2beta1, beta)",
		})
		return r.Status().Update(ctx, latest)
	})
}

func (r *AuraDatabaseReconciler) fail(ctx context.Context, req ctrl.Request, d *neo4jv1beta1.AuraDatabase, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraDatabase reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabase{}
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
func (r *AuraDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraDatabase{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
