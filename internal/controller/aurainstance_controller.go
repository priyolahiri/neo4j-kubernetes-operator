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

// AuraInstanceFinalizer guards the operator's chance to delete (or deliberately
// orphan) the cloud instance before the CR is removed.
const AuraInstanceFinalizer = "neo4j.com/aurainstance-finalizer"

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainstances/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraproviderconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// AuraInstanceReconciler provisions and manages a Neo4j Aura cloud instance via
// the Aura REST API. Lifecycle is a status-poll state machine (Aura has no
// long-running-operation resource): create/adopt, then observe, resize,
// pause/resume, and on delete either orphan (default) or destroy the instance.
type AuraInstanceReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
}

func (r *AuraInstanceReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the instance state machine.
func (r *AuraInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	inst := &neo4jv1beta1.AuraInstance{}
	if err := r.Get(ctx, req.NamespacedName, inst); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Resolve credentials + client (needed for both the delete and the sync path).
	creds, err := resolveAuraCredentials(ctx, r.Client, inst.Namespace, inst.Spec.ProviderConfigRef, inst.Spec.CredentialsSecretRef)
	if err != nil {
		return r.fail(ctx, req, inst, "CredentialsUnavailable", err)
	}
	projectID := inst.Spec.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	apiClient := auraClientForCreds(creds)

	if !inst.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, inst, apiClient)
	}

	if !controllerutil.ContainsFinalizer(inst, AuraInstanceFinalizer) {
		if err := r.patchInstance(ctx, req, func(o *neo4jv1beta1.AuraInstance) {
			controllerutil.AddFinalizer(o, AuraInstanceFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	if projectID == "" {
		return r.fail(ctx, req, inst, "ProjectIDMissing",
			fmt.Errorf("spec.projectId is empty and the provider config has no defaultProjectId"))
	}

	// --- Idempotent create + adopt via the external-name annotation ---
	externalID := inst.Annotations[AuraExternalIDAnnotation]
	if externalID == "" && inst.Spec.InstanceID != "" {
		externalID = inst.Spec.InstanceID // explicit user import
	}

	if externalID == "" {
		// Observe-before-create: never POST if an instance with our name already
		// exists (avoids a duplicate paid instance after a crash between create
		// and the annotation write).
		id, adopted, err := r.observeOrCreate(ctx, req, inst, apiClient, projectID)
		if err != nil {
			return r.fail(ctx, req, inst, "CreateFailed", err)
		}
		externalID = id
		if adopted {
			r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceAdopted,
				fmt.Sprintf("Adopted existing Aura instance %s", externalID))
		}
	}

	// --- Observe ---
	observed, err := apiClient.GetInstance(ctx, externalID)
	if err != nil {
		if aura.IsNotFound(err) {
			// Just created and not yet visible, or externally deleted. Requeue.
			logger.Info("Aura instance not found yet; requeuing", "instanceId", externalID)
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, inst, "ObserveFailed", err)
	}

	// --- Reconcile connection Secret + ConfigMap (URI etc.; password preserved) ---
	if err := r.reconcileConnectionOutputs(ctx, inst, observed, nil); err != nil {
		logger.Error(err, "failed to reconcile connection outputs")
	}

	// --- Drive drift only when the instance is in a stable, running state ---
	if aura.IsInstanceRunning(observed.Status) {
		if handled, res, err := r.reconcileDrift(ctx, inst, apiClient, observed); handled {
			return res, err
		}
	}
	// Pause/resume desired-state transitions.
	if res, handled, err := r.reconcilePauseResume(ctx, inst, apiClient, observed); handled {
		return res, err
	}

	// --- Status ---
	if err := r.syncStatus(ctx, req, inst, observed); err != nil {
		return ctrl.Result{}, err
	}

	// Keep polling while a transition is in flight.
	if aura.IsInstanceTransient(observed.Status) {
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}
	// Periodic re-observe to catch out-of-band drift.
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// observeOrCreate implements the observe-before-create idempotency guard.
func (r *AuraInstanceReconciler) observeOrCreate(
	ctx context.Context, req ctrl.Request, inst *neo4jv1beta1.AuraInstance,
	apiClient *aura.Client, projectID string,
) (id string, adopted bool, err error) {
	name := r.instanceName(inst)

	existing, err := apiClient.ListInstances(ctx, projectID)
	if err != nil {
		return "", false, fmt.Errorf("listing instances before create: %w", err)
	}
	for i := range existing {
		if existing[i].Name == name {
			id = existing[i].ID
			if err := r.setExternalID(ctx, req, id); err != nil {
				return "", false, err
			}
			return id, true, nil
		}
	}

	// Inline oracle (item B's Go half): validate the desired combo against the
	// project's allowed instance_configurations — the one check CEL can't do.
	// Best-effort: if the tenant can't be fetched, let CreateInstance surface
	// any rejection rather than blocking on a transient read.
	if tenant, terr := apiClient.GetTenant(ctx, projectID); terr == nil && len(tenant.InstanceConfigurations) > 0 {
		match := false
		for i := range tenant.InstanceConfigurations {
			c := tenant.InstanceConfigurations[i]
			if c.Type == inst.Spec.Type && c.Region == inst.Spec.Region && c.CloudProvider == inst.Spec.CloudProvider {
				match = true
				break
			}
		}
		if !match {
			return "", false, fmt.Errorf(
				"no Aura instance configuration for type=%s region=%s cloudProvider=%s in project %s; check the project's supported combinations",
				inst.Spec.Type, inst.Spec.Region, inst.Spec.CloudProvider, projectID)
		}
	}

	// Create.
	createReq := aura.CreateInstanceRequest{
		Name:          name,
		Version:       inst.Spec.Version,
		Region:        inst.Spec.Region,
		Memory:        inst.Spec.Memory,
		Type:          inst.Spec.Type,
		TenantID:      projectID,
		CloudProvider: inst.Spec.CloudProvider,
		Storage:       inst.Spec.Storage,
	}
	if inst.Spec.CustomerManagedKeyID != "" {
		createReq.CustomerManagedKeyID = inst.Spec.CustomerManagedKeyID
	}
	if inst.Spec.VectorOptimized != nil {
		createReq.VectorOptimized = inst.Spec.VectorOptimized
	}
	if inst.Spec.GraphAnalyticsPlugin != nil {
		createReq.GraphAnalyticsPlugin = inst.Spec.GraphAnalyticsPlugin
	}
	if inst.Spec.Source != nil {
		createReq.SourceInstanceID = inst.Spec.Source.InstanceID
		createReq.SourceSnapshotID = inst.Spec.Source.SnapshotID
	}

	resp, err := apiClient.CreateInstance(ctx, createReq)
	if err != nil {
		return "", false, err
	}
	id = resp.ID
	// Persist the external ID BEFORE anything else — this is the idempotency
	// guard: a crash after this point re-observes instead of re-creating.
	if err := r.setExternalID(ctx, req, id); err != nil {
		return "", false, err
	}
	// Capture the one-time credentials (create is the only time the password is
	// returned) into the connection Secret immediately.
	if err := r.reconcileConnectionOutputs(ctx, inst, nil, resp); err != nil {
		log.FromContext(ctx).Error(err, "failed to persist one-time connection credentials")
	}
	r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceCreated,
		fmt.Sprintf("Created Aura instance %s", id))
	return id, false, nil
}

// reconcileDrift converges mutable fields (memory, storage, name) via PATCH.
// Returns handled=true when it issued a change (caller should requeue).
func (r *AuraInstanceReconciler) reconcileDrift(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient *aura.Client, observed *aura.Instance,
) (handled bool, res ctrl.Result, err error) {
	patch := aura.PatchInstanceRequest{}
	changed := false
	if inst.Spec.Memory != "" && inst.Spec.Memory != observed.Memory {
		patch.Memory = &inst.Spec.Memory
		changed = true
	}
	if inst.Spec.Storage != "" && inst.Spec.Storage != observed.Storage {
		patch.Storage = &inst.Spec.Storage
		changed = true
	}
	if desired := r.instanceName(inst); desired != observed.Name {
		patch.Name = &desired
		changed = true
	}
	if !changed {
		return false, ctrl.Result{}, nil
	}
	if _, err := apiClient.PatchInstance(ctx, observed.ID, patch); err != nil {
		if aura.IsConflict(err) || aura.IsTransient(err) {
			return true, ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return true, ctrl.Result{}, err
	}
	r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceUpdated,
		fmt.Sprintf("Updated Aura instance %s", observed.ID))
	return true, ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// reconcilePauseResume drives the desired paused state.
func (r *AuraInstanceReconciler) reconcilePauseResume(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient *aura.Client, observed *aura.Instance,
) (res ctrl.Result, handled bool, err error) {
	switch {
	case inst.Spec.Paused && aura.IsInstanceRunning(observed.Status):
		if err := apiClient.PauseInstance(ctx, observed.ID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, true, nil
			}
			return ctrl.Result{}, true, err
		}
		r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstancePaused,
			fmt.Sprintf("Pausing Aura instance %s", observed.ID))
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, true, nil
	case !inst.Spec.Paused && aura.IsInstancePaused(observed.Status):
		if err := apiClient.ResumeInstance(ctx, observed.ID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, true, nil
			}
			return ctrl.Result{}, true, err
		}
		r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceResumed,
			fmt.Sprintf("Resuming Aura instance %s", observed.ID))
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// handleDeletion honours the deletion policy: Orphan (default) keeps the cloud
// instance; Delete destroys it (unless deletionProtection is set).
func (r *AuraInstanceReconciler) handleDeletion(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient *aura.Client,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(inst, AuraInstanceFinalizer) {
		return ctrl.Result{}, nil
	}

	externalID := inst.Annotations[AuraExternalIDAnnotation]
	if externalID == "" {
		externalID = inst.Status.InstanceID
	}

	switch inst.Spec.DeletionPolicy {
	case "Delete":
		if inst.Spec.DeletionProtection {
			logger.Info("deletionProtection is set; refusing to delete the Aura instance", "instanceId", externalID)
			r.Recorder.Event(inst, corev1.EventTypeWarning, EventReasonAuraInstanceFailed,
				"deletionProtection is set: clear it to allow deletion of the cloud instance")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		if externalID != "" {
			if err := apiClient.DeleteInstance(ctx, externalID); err != nil && !aura.IsNotFound(err) {
				if aura.IsConflict(err) || aura.IsTransient(err) {
					return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
				}
				return ctrl.Result{}, err
			}
			r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceDeleted,
				fmt.Sprintf("Deleted Aura instance %s", externalID))
		}
	default: // Orphan
		if externalID != "" {
			r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceOrphaned,
				fmt.Sprintf("Orphaning Aura instance %s (deletionPolicy=Orphan); the cloud instance keeps running", externalID))
		}
	}

	// Release the finalizer.
	return ctrl.Result{}, r.patchInstance(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(inst)}, func(o *neo4jv1beta1.AuraInstance) {
		controllerutil.RemoveFinalizer(o, AuraInstanceFinalizer)
	})
}

// instanceName is the Aura instance name (spec.name or the CR name, ≤30 chars).
func (r *AuraInstanceReconciler) instanceName(inst *neo4jv1beta1.AuraInstance) string {
	name := inst.Spec.Name
	if name == "" {
		name = inst.Name
	}
	if len(name) > 30 {
		name = name[:30]
	}
	return name
}

// setExternalID writes the Aura instance ID to the external-name annotation.
func (r *AuraInstanceReconciler) setExternalID(ctx context.Context, req ctrl.Request, id string) error {
	return r.patchInstance(ctx, req, func(o *neo4jv1beta1.AuraInstance) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalIDAnnotation] = id
	})
}

// patchInstance applies a mutation to the latest object with conflict retry.
func (r *AuraInstanceReconciler) patchInstance(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraInstance)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInstance{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

// syncStatus writes observed state + conditions.
func (r *AuraInstanceReconciler) syncStatus(ctx context.Context, req ctrl.Request, inst *neo4jv1beta1.AuraInstance, observed *aura.Instance) error {
	running := aura.IsInstanceRunning(observed.Status)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInstance{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.InstanceID = observed.ID
		latest.Status.Phase = observed.Status
		if observed.ConnectionURL != "" {
			latest.Status.ConnectionURL = observed.ConnectionURL
		}
		latest.Status.MetricsIntegrationURL = observed.MetricsIntegrationURL
		if cn := r.connectionSecretName(latest); cn != "" {
			latest.Status.Binding = &neo4jv1beta1.AuraServiceBinding{Name: cn}
		}
		now := metav1.Now()
		latest.Status.LastSyncedTime = &now
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: auraCondStatus(running),
			Reason:  readyReason(observed.Status),
			Message: fmt.Sprintf("Aura instance status: %s", observed.Status),
		})
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Synced", Status: metav1.ConditionTrue, Reason: "Observed",
			Message: "Reconciled against the Aura API",
		})
		return r.Status().Update(ctx, latest)
	})
}

func readyReason(status string) string {
	switch {
	case aura.IsInstanceRunning(status):
		return "Running"
	case aura.IsInstancePaused(status):
		return "Paused"
	default:
		return "NotReady"
	}
}

// fail records a failure on status and requeues without a hard error when the
// cause is external (missing creds/config), so we don't hot-loop on user error.
func (r *AuraInstanceReconciler) fail(ctx context.Context, req ctrl.Request, inst *neo4jv1beta1.AuraInstance, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraInstance reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(inst, corev1.EventTypeWarning, EventReasonAuraInstanceFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInstance{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: cause.Error(),
		})
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraInstance{}).
		Owns(&corev1.Secret{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
