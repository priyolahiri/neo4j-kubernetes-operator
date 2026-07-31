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
	// ClientFactory builds the Aura API client from resolved credentials; nil
	// uses the real shared cached client. Tests inject a fake.
	ClientFactory auraClientFactory
	// InstanceV2ClientFactory builds the Aura v2beta1 instance client, used only
	// by the multi-database paths (creating one, and probing whether an existing
	// instance is one). nil uses the real shared cached client.
	InstanceV2ClientFactory auraInstanceV2ClientFactory
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

	// Paused: suspend all reconciliation (including deletion) until cleared.
	if inst.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraInstance reconciliation paused via annotation")
		_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "Paused",
			"Reconciliation paused via the neo4j.com/paused annotation")
		return ctrl.Result{}, nil
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
	apiClient := resolveClient(r.ClientFactory, creds)
	v2Client := resolveInstanceV2Client(r.InstanceV2ClientFactory, creds)

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
		// and the annotation write). Creation is gated by managementPolicies.
		allowCreate := managementAllows(inst.Spec.ManagementPolicies, auraPolicyCreate)
		id, adopted, err := r.observeOrCreate(ctx, req, inst, apiClient, v2Client, projectID, allowCreate)
		if err != nil {
			// A deliberate refusal, or Aura refusing multi_database on this tier,
			// describes something no retry can change (type is immutable). Name it
			// and stop, rather than rewriting the same status every 30s.
			if isAuraRefusal(err) || aura.IsMultiDatabaseTierUnsupported(err) {
				return r.failTerminal(ctx, req, inst, "MultiDatabaseUnsupported", err)
			}
			return r.fail(ctx, req, inst, "CreateFailed", err)
		}
		if id == "" {
			// Observe-only (no Create policy) and no matching instance exists yet.
			_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "AwaitingInstance",
				"no matching Aura instance found and managementPolicies does not permit Create")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
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

	// --- Drift + pause/resume, gated by the Update management policy ---
	if managementAllows(inst.Spec.ManagementPolicies, auraPolicyUpdate) {
		if aura.IsInstanceRunning(observed.Status) {
			// Tier upgrade first (professional-db → business-critical), then resize.
			if handled, res, err := r.reconcileUpgrade(ctx, inst, apiClient, observed); handled {
				return res, err
			}
			if handled, res, err := r.reconcileDrift(ctx, inst, apiClient, observed); handled {
				return res, err
			}
		}
		if res, handled, err := r.reconcilePauseResume(ctx, inst, apiClient, observed); handled {
			return res, err
		}
	}

	// --- Multi-database verdict (one-shot, non-fatal) ---
	// Reported for the user's benefit and consumed by the AuraDatabase family,
	// which can only target a multi-database instance.
	r.reconcileMultiDatabaseStatus(ctx, req, inst, v2Client, projectID, externalID)

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
	apiClient auraAPI, v2Client auraInstanceV2API, projectID string, allowCreate bool,
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

	// Observe-only: no matching instance exists and Create is not permitted.
	if !allowCreate {
		return "", false, nil
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

	// A multi-database instance can only be created through v2beta1 — v1 has no
	// such field — so that create diverges here. Everything afterwards (this
	// method's caller included) stays on v1, which does see v2beta1-created
	// instances.
	if wantsMultiDatabase(inst) {
		return r.createMultiDatabaseInstance(ctx, req, inst, projectID, name)
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
		createReq.SourceSnapshotID = inst.Spec.Source.SnapshotID
		// instanceRef points at another AuraInstance in this namespace; resolve it
		// to the Aura instance ID. Previously only source.instanceId was read, so
		// setting instanceRef alone silently produced a plain (non-clone) create.
		switch {
		case inst.Spec.Source.InstanceID != "":
			createReq.SourceInstanceID = inst.Spec.Source.InstanceID
		case inst.Spec.Source.InstanceRef != "":
			srcID, err := r.resolveSourceInstanceRef(ctx, inst.Namespace, inst.Spec.Source.InstanceRef)
			if err != nil {
				return "", false, err
			}
			createReq.SourceInstanceID = srcID
		}
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

// resolveSourceInstanceRef resolves a clone source given as another AuraInstance
// in the same namespace, returning its Aura instance ID. The source must already
// have been provisioned (status.instanceId populated).
func (r *AuraInstanceReconciler) resolveSourceInstanceRef(ctx context.Context, namespace, name string) (string, error) {
	src := &neo4jv1beta1.AuraInstance{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, src); err != nil {
		return "", fmt.Errorf("resolving source.instanceRef %q: %w", name, err)
	}
	if src.Status.InstanceID == "" {
		return "", fmt.Errorf("source AuraInstance %q has no instanceId yet (not provisioned)", name)
	}
	return src.Status.InstanceID, nil
}

// reconcileDrift converges mutable fields (memory, storage, name, secondaries
// count, CDC enrichment mode) via PATCH. Returns handled=true when it issued a
// change (caller should requeue).
func (r *AuraInstanceReconciler) reconcileDrift(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient auraAPI, observed *aura.Instance,
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
	// secondariesCount and cdcEnrichmentMode were previously declared on the CRD
	// (with CEL tier guards) but never sent, so setting them did nothing at all.
	// v1's PATCH accepts both — they are absent from the schema's `properties` but
	// named in the endpoint description and its "Update Secondary Count" /
	// "Update CDC Enrichment Mode" examples.
	if inst.Spec.SecondariesCount != nil {
		want := int(*inst.Spec.SecondariesCount)
		if observed.SecondariesCount == nil || want != *observed.SecondariesCount {
			patch.SecondariesCount = &want
			changed = true
		}
	}
	if inst.Spec.CDCEnrichmentMode != "" && inst.Spec.CDCEnrichmentMode != observed.CDCEnrichmentMode {
		patch.CDCEnrichmentMode = &inst.Spec.CDCEnrichmentMode
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

// Instance tier values involved in the one supported in-place upgrade path.
const (
	auraTypeProfessionalDB   = "professional-db"
	auraTypeBusinessCritical = "business-critical"
)

// reconcileUpgrade performs the single supported in-place tier upgrade
// (professional-db → business-critical) when the spec requests it. The CRD's CEL
// transition rule already rejects every other type change, so this only ever
// sees the one valid path. Returns handled=true when it issued the upgrade (the
// caller requeues to poll the resulting "updating" → "running" transition).
func (r *AuraInstanceReconciler) reconcileUpgrade(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient auraAPI, observed *aura.Instance,
) (handled bool, res ctrl.Result, err error) {
	if observed.Type != auraTypeProfessionalDB || inst.Spec.Type != auraTypeBusinessCritical {
		return false, ctrl.Result{}, nil
	}
	if err := apiClient.UpgradeInstance(ctx, observed.ID); err != nil {
		if aura.IsConflict(err) || aura.IsTransient(err) {
			return true, ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return true, ctrl.Result{}, err
	}
	r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceUpgraded,
		fmt.Sprintf("Upgrading Aura instance %s from professional-db to business-critical", observed.ID))
	return true, ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// reconcilePauseResume drives the desired paused state.
func (r *AuraInstanceReconciler) reconcilePauseResume(
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient auraAPI, observed *aura.Instance,
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
	ctx context.Context, inst *neo4jv1beta1.AuraInstance, apiClient auraAPI,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(inst, AuraInstanceFinalizer) {
		return ctrl.Result{}, nil
	}

	externalID := inst.Annotations[AuraExternalIDAnnotation]
	if externalID == "" {
		externalID = inst.Status.InstanceID
	}

	// Delete the cloud instance only when the policy says Delete AND the Delete
	// management policy permits it; otherwise orphan (the safe default).
	deleteCloud := inst.Spec.DeletionPolicy == "Delete" && managementAllows(inst.Spec.ManagementPolicies, auraPolicyDelete)
	switch {
	case deleteCloud && inst.Spec.DeletionProtection:
		logger.Info("deletionProtection is set; refusing to delete the Aura instance", "instanceId", externalID)
		r.Recorder.Event(inst, corev1.EventTypeWarning, EventReasonAuraInstanceFailed,
			"deletionProtection is set: clear it to allow deletion of the cloud instance")
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	case deleteCloud:
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
	default: // Orphan (policy Orphan, or Delete not permitted by managementPolicies)
		if externalID != "" {
			r.Recorder.Event(inst, corev1.EventTypeNormal, EventReasonAuraInstanceOrphaned,
				fmt.Sprintf("Orphaning Aura instance %s; the cloud instance keeps running", externalID))
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

// setCondition sets a status condition, writing only when it actually changes,
// so a steady terminal state (e.g. Paused) doesn't self-trigger a reconcile loop.
func (r *AuraInstanceReconciler) setCondition(ctx context.Context, req ctrl.Request, condType string, status metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInstance{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		existing := meta.FindStatusCondition(latest.Status.Conditions, condType)
		if existing != nil && existing.Status == status && existing.Reason == reason &&
			existing.Message == msg && latest.Status.ObservedGeneration == latest.Generation {
			return nil
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: condType, Status: status, Reason: reason, Message: msg,
		})
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
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
		// The multi-database facts are NOT part of the v1 observation this method
		// rebuilds from — they come from the v2beta1 create response or the
		// one-shot probe — and they can never change, so carry them across the
		// wholesale rebuild instead of dropping them every reconcile.
		var (
			priorMultiDB  *bool
			priorDefaultD string
		)
		if prior := latest.Status.AtProvider; prior != nil {
			priorMultiDB = prior.MultiDatabase
			priorDefaultD = prior.DefaultDatabaseID
		}
		latest.Status.AtProvider = &neo4jv1beta1.AuraInstanceObservation{
			Status:            observed.Status,
			Memory:            observed.Memory,
			Storage:           observed.Storage,
			Type:              observed.Type,
			Region:            observed.Region,
			CloudProvider:     observed.CloudProvider,
			Name:              observed.Name,
			MultiDatabase:     priorMultiDB,
			DefaultDatabaseID: priorDefaultD,
		}
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

// failTerminal records a failure that no retry can clear — the spec asks for
// something Aura will never do — so it does NOT requeue. The spec edit that
// fixes it triggers a fresh reconcile on its own.
func (r *AuraInstanceReconciler) failTerminal(ctx context.Context, req ctrl.Request, inst *neo4jv1beta1.AuraInstance, reason string, cause error) (ctrl.Result, error) {
	_, _ = r.fail(ctx, req, inst, reason, cause)
	return ctrl.Result{}, nil
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
