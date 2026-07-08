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
	"reflect"
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

// AuraIPFilterFinalizer guards the operator's chance to delete (or orphan) the
// Aura IP filter before the CR is removed.
const AuraIPFilterFinalizer = "neo4j.com/auraipfilter-finalizer"

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraipfilters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraipfilters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraipfilters/finalizers,verbs=update

// AuraIPFilterReconciler manages a Neo4j Aura network IP filter via the Aura API
// v2beta1. BETA / best-effort: v2beta1 is an unstable beta and the exact API
// contract is reconstructed (see internal/aura/ipfilter_v2beta1.go).
type AuraIPFilterReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	// ClientFactory builds the Aura v2beta1 client; nil uses the real shared
	// cached client. Tests inject a fake.
	ClientFactory auraIPFilterClientFactory
}

func (r *AuraIPFilterReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the IP-filter state machine.
func (r *AuraIPFilterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	f := &neo4jv1beta1.AuraIPFilter{}
	if err := r.Get(ctx, req.NamespacedName, f); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if f.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraIPFilter reconciliation paused via annotation")
		_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "Paused",
			"Reconciliation paused via the neo4j.com/paused annotation")
		return ctrl.Result{}, nil
	}

	creds, err := resolveAuraCredentials(ctx, r.Client, f.Namespace, f.Spec.ProviderConfigRef, f.Spec.CredentialsSecretRef)
	if err != nil {
		return r.fail(ctx, req, f, "CredentialsUnavailable", err)
	}
	orgID := f.Spec.OrganizationID
	if orgID == "" {
		orgID = r.providerDefaultOrgID(ctx, f)
	}
	projectID := f.Spec.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	apiClient := resolveIPFilterClient(r.ClientFactory, creds)

	if !f.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, f, apiClient, orgID, projectID)
	}

	if !controllerutil.ContainsFinalizer(f, AuraIPFilterFinalizer) {
		if err := r.patch(ctx, req, func(o *neo4jv1beta1.AuraIPFilter) {
			controllerutil.AddFinalizer(o, AuraIPFilterFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	if orgID == "" || projectID == "" {
		return r.fail(ctx, req, f, "ScopeMissing",
			fmt.Errorf("organizationId and projectId are required (set them on the CR or as defaults on the AuraProviderConfig)"))
	}

	// Resolve the optional instance association to its Aura instance ID.
	instanceID, err := r.resolveInstanceID(ctx, f)
	if err != nil {
		return r.fail(ctx, req, f, "InstanceRefUnresolved", err)
	}

	// --- Idempotent create + adopt ---
	externalID := f.Annotations[AuraExternalIPFilterAnnotation]
	if externalID == "" {
		externalID = f.Status.FilterID
	}
	if externalID == "" {
		allowCreate := managementAllows(f.Spec.ManagementPolicies, auraPolicyCreate)
		id, adopted, err := r.observeOrCreate(ctx, req, f, apiClient, orgID, projectID, instanceID, allowCreate)
		if err != nil {
			return r.fail(ctx, req, f, "CreateFailed", err)
		}
		if id == "" {
			_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "AwaitingFilter",
				"no matching IP filter found and managementPolicies does not permit Create")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		externalID = id
		if adopted {
			r.Recorder.Event(f, corev1.EventTypeNormal, EventReasonAuraIPFilterAdopted,
				fmt.Sprintf("Adopted existing IP filter %s", externalID))
		}
	}

	observed, err := apiClient.GetIPFilter(ctx, orgID, projectID, externalID)
	if err != nil {
		if aura.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, f, "ObserveFailed", err)
	}

	// --- Drift: converge the CIDR set + name (Update-gated) ---
	if managementAllows(f.Spec.ManagementPolicies, auraPolicyUpdate) {
		if handled, res, err := r.reconcileDrift(ctx, f, apiClient, orgID, projectID, observed); handled {
			return res, err
		}
	}

	if err := r.syncStatus(ctx, req, f, observed); err != nil {
		return ctrl.Result{}, err
	}
	if aura.IsIPFilterReady(observed.Status) {
		r.Recorder.Event(f, corev1.EventTypeNormal, EventReasonAuraIPFilterReady,
			fmt.Sprintf("IP filter %s is ready", observed.ID))
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// observeOrCreate adopts an IP filter with the same name (and instance
// association) before creating, so a crash can't produce a duplicate.
func (r *AuraIPFilterReconciler) observeOrCreate(
	ctx context.Context, req ctrl.Request, f *neo4jv1beta1.AuraIPFilter,
	apiClient auraIPFilterAPI, orgID, projectID, instanceID string, allowCreate bool,
) (id string, adopted bool, err error) {
	name := r.filterName(f)
	existing, err := apiClient.ListIPFilters(ctx, orgID, projectID)
	if err != nil {
		return "", false, fmt.Errorf("listing ip filters before create: %w", err)
	}
	for i := range existing {
		e := existing[i]
		if e.Name == name && e.InstanceID == instanceID {
			if err := r.setExternalID(ctx, req, e.ID); err != nil {
				return "", false, err
			}
			return e.ID, true, nil
		}
	}
	if !allowCreate {
		return "", false, nil
	}
	created, err := apiClient.CreateIPFilter(ctx, orgID, projectID, aura.CreateIPFilterRequest{
		Name:       name,
		InstanceID: instanceID,
		Region:     f.Spec.Region,
		CIDRs:      f.Spec.CIDRs,
	})
	if err != nil {
		return "", false, err
	}
	if err := r.setExternalID(ctx, req, created.ID); err != nil {
		return "", false, err
	}
	r.Recorder.Event(f, corev1.EventTypeNormal, EventReasonAuraIPFilterCreated,
		fmt.Sprintf("Created IP filter %s", created.ID))
	return created.ID, false, nil
}

// reconcileDrift converges the mutable CIDR set + name.
func (r *AuraIPFilterReconciler) reconcileDrift(
	ctx context.Context, f *neo4jv1beta1.AuraIPFilter, apiClient auraIPFilterAPI,
	orgID, projectID string, observed *aura.IPFilter,
) (handled bool, res ctrl.Result, err error) {
	patch := aura.UpdateIPFilterRequest{}
	changed := false
	if desired := r.filterName(f); desired != observed.Name {
		patch.Name = &desired
		changed = true
	}
	if !equalStringSet(f.Spec.CIDRs, observed.CIDRs) {
		cidrs := f.Spec.CIDRs
		patch.CIDRs = &cidrs
		changed = true
	}
	if !changed {
		return false, ctrl.Result{}, nil
	}
	if _, err := apiClient.UpdateIPFilter(ctx, orgID, projectID, observed.ID, patch); err != nil {
		if aura.IsConflict(err) || aura.IsTransient(err) {
			return true, ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return true, ctrl.Result{}, err
	}
	r.Recorder.Event(f, corev1.EventTypeNormal, EventReasonAuraIPFilterUpdated,
		fmt.Sprintf("Updated IP filter %s", observed.ID))
	return true, ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraIPFilterReconciler) handleDeletion(
	ctx context.Context, req ctrl.Request, f *neo4jv1beta1.AuraIPFilter, apiClient auraIPFilterAPI, orgID, projectID string,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(f, AuraIPFilterFinalizer) {
		return ctrl.Result{}, nil
	}
	externalID := f.Annotations[AuraExternalIPFilterAnnotation]
	if externalID == "" {
		externalID = f.Status.FilterID
	}
	deleteCloud := f.Spec.DeletionPolicy == "Delete" && managementAllows(f.Spec.ManagementPolicies, auraPolicyDelete)
	if deleteCloud && externalID != "" && orgID != "" && projectID != "" {
		if err := apiClient.DeleteIPFilter(ctx, orgID, projectID, externalID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if !aura.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.Recorder.Event(f, corev1.EventTypeNormal, EventReasonAuraIPFilterDeleted,
			fmt.Sprintf("Deleted IP filter %s", externalID))
	} else if externalID != "" {
		r.Recorder.Event(f, corev1.EventTypeNormal, EventReasonAuraIPFilterOrphaned,
			fmt.Sprintf("Orphaning IP filter %s; it stays in place in Aura", externalID))
	}
	return ctrl.Result{}, r.patch(ctx, req, func(o *neo4jv1beta1.AuraIPFilter) {
		controllerutil.RemoveFinalizer(o, AuraIPFilterFinalizer)
	})
}

// resolveInstanceID resolves spec.instanceRef to its Aura instance ID (via the
// referenced AuraInstance's external-id annotation / status). Empty when no
// instanceRef is set (a project-wide filter).
func (r *AuraIPFilterReconciler) resolveInstanceID(ctx context.Context, f *neo4jv1beta1.AuraIPFilter) (string, error) {
	if f.Spec.InstanceRef == "" {
		return "", nil
	}
	inst := &neo4jv1beta1.AuraInstance{}
	if err := r.Get(ctx, types.NamespacedName{Name: f.Spec.InstanceRef, Namespace: f.Namespace}, inst); err != nil {
		return "", fmt.Errorf("resolving instanceRef %q: %w", f.Spec.InstanceRef, err)
	}
	id := inst.Annotations[AuraExternalIDAnnotation]
	if id == "" {
		id = inst.Status.InstanceID
	}
	if id == "" {
		return "", fmt.Errorf("AuraInstance %q has no external instance ID yet", f.Spec.InstanceRef)
	}
	return id, nil
}

// providerDefaultOrgID reads defaultOrganizationId off the referenced provider
// config, if any.
func (r *AuraIPFilterReconciler) providerDefaultOrgID(ctx context.Context, f *neo4jv1beta1.AuraIPFilter) string {
	if f.Spec.ProviderConfigRef == nil {
		return ""
	}
	pc := &neo4jv1beta1.AuraProviderConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: f.Spec.ProviderConfigRef.Name, Namespace: f.Namespace}, pc); err != nil {
		return ""
	}
	return pc.Spec.DefaultOrganizationID
}

func (r *AuraIPFilterReconciler) filterName(f *neo4jv1beta1.AuraIPFilter) string {
	if f.Spec.Name != "" {
		return f.Spec.Name
	}
	return f.Name
}

func (r *AuraIPFilterReconciler) setExternalID(ctx context.Context, req ctrl.Request, id string) error {
	return r.patch(ctx, req, func(o *neo4jv1beta1.AuraIPFilter) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalIPFilterAnnotation] = id
	})
}

func (r *AuraIPFilterReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraIPFilter)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraIPFilter{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

func (r *AuraIPFilterReconciler) setCondition(ctx context.Context, req ctrl.Request, condType string, status metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraIPFilter{}
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

func (r *AuraIPFilterReconciler) syncStatus(ctx context.Context, req ctrl.Request, f *neo4jv1beta1.AuraIPFilter, observed *aura.IPFilter) error {
	ready := aura.IsIPFilterReady(observed.Status)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraIPFilter{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.FilterID = observed.ID
		latest.Status.Phase = observed.Status
		now := metav1.Now()
		latest.Status.LastSyncedTime = &now
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: auraCondStatus(ready), Reason: ipFilterReadyReason(observed.Status),
			Message: fmt.Sprintf("IP filter status: %s", observed.Status),
		})
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Synced", Status: metav1.ConditionTrue, Reason: "Observed",
			Message: "Reconciled against the Aura API (v2beta1, beta)",
		})
		return r.Status().Update(ctx, latest)
	})
}

func ipFilterReadyReason(status string) string {
	if aura.IsIPFilterReady(status) {
		return "Ready"
	}
	return "NotReady"
}

func (r *AuraIPFilterReconciler) fail(ctx context.Context, req ctrl.Request, f *neo4jv1beta1.AuraIPFilter, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraIPFilter reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(f, corev1.EventTypeWarning, EventReasonAuraIPFilterFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraIPFilter{}
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

// equalStringSet reports whether two string slices contain the same elements
// (order-insensitive), used to detect CIDR-set drift.
func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	am := make(map[string]int, len(a))
	for _, s := range a {
		am[s]++
	}
	bm := make(map[string]int, len(b))
	for _, s := range b {
		bm[s]++
	}
	return reflect.DeepEqual(am, bm)
}

// SetupWithManager wires the controller.
func (r *AuraIPFilterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraIPFilter{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
