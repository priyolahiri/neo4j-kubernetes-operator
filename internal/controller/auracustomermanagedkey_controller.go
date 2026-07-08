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

// AuraCMKFinalizer guards the operator's chance to deregister (or deliberately
// orphan) the customer-managed key in Aura before the CR is removed.
const AuraCMKFinalizer = "neo4j.com/auracustomermanagedkey-finalizer"

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auracustomermanagedkeys,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auracustomermanagedkeys/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auracustomermanagedkeys/finalizers,verbs=update

// AuraCustomerManagedKeyReconciler registers and manages a customer-managed
// encryption key with Aura via the Aura REST API. Lifecycle is a status-poll
// state machine: create/adopt, poll pending → ready, and on delete either orphan
// (default) or deregister the key (which the API refuses while any instance
// still uses it).
type AuraCustomerManagedKeyReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	// ClientFactory builds the Aura CMK client from resolved credentials; nil
	// uses the real shared cached client. Tests inject a fake.
	ClientFactory auraCMKClientFactory
}

func (r *AuraCustomerManagedKeyReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the key state machine.
func (r *AuraCustomerManagedKeyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cmk := &neo4jv1beta1.AuraCustomerManagedKey{}
	if err := r.Get(ctx, req.NamespacedName, cmk); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Paused: suspend all reconciliation (including deletion) until cleared.
	if cmk.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraCustomerManagedKey reconciliation paused via annotation")
		_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "Paused",
			"Reconciliation paused via the neo4j.com/paused annotation")
		return ctrl.Result{}, nil
	}

	creds, err := resolveAuraCredentials(ctx, r.Client, cmk.Namespace, cmk.Spec.ProviderConfigRef, cmk.Spec.CredentialsSecretRef)
	if err != nil {
		return r.fail(ctx, req, cmk, "CredentialsUnavailable", err)
	}
	projectID := cmk.Spec.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	apiClient := resolveCMKClient(r.ClientFactory, creds)

	if !cmk.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, cmk, apiClient)
	}

	if !controllerutil.ContainsFinalizer(cmk, AuraCMKFinalizer) {
		if err := r.patchCMK(ctx, req, func(o *neo4jv1beta1.AuraCustomerManagedKey) {
			controllerutil.AddFinalizer(o, AuraCMKFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	if projectID == "" {
		return r.fail(ctx, req, cmk, "ProjectIDMissing",
			fmt.Errorf("spec.projectId is empty and the provider config has no defaultProjectId"))
	}

	// --- Idempotent create + adopt via the external-name annotation ---
	externalID := cmk.Annotations[AuraExternalCMKAnnotation]
	if externalID == "" {
		externalID = cmk.Status.CustomerManagedKeyID
	}

	if externalID == "" {
		allowCreate := managementAllows(cmk.Spec.ManagementPolicies, auraPolicyCreate)
		id, adopted, err := r.observeOrCreate(ctx, req, cmk, apiClient, projectID, allowCreate)
		if err != nil {
			return r.fail(ctx, req, cmk, "CreateFailed", err)
		}
		if id == "" {
			_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "AwaitingKey",
				"no matching customer-managed key found and managementPolicies does not permit Create")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		externalID = id
		if adopted {
			r.Recorder.Event(cmk, corev1.EventTypeNormal, EventReasonAuraCMKAdopted,
				fmt.Sprintf("Adopted existing customer-managed key %s", externalID))
		}
	}

	// --- Observe ---
	observed, err := apiClient.GetCustomerManagedKey(ctx, externalID)
	if err != nil {
		if aura.IsNotFound(err) {
			logger.Info("customer-managed key not found yet; requeuing", "keyId", externalID)
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, cmk, "ObserveFailed", err)
	}

	if err := r.syncStatus(ctx, req, cmk, observed); err != nil {
		return ctrl.Result{}, err
	}
	if aura.IsCMKReady(observed.Status) {
		r.Recorder.Event(cmk, corev1.EventTypeNormal, EventReasonAuraCMKReady,
			fmt.Sprintf("Customer-managed key %s is ready", observed.ID))
		// Ready is terminal; re-observe periodically to catch out-of-band drift.
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}
	// Still pending/validating — keep polling.
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// observeOrCreate lists the project's keys and adopts a match (by the immutable
// cloud KMS key identifier + placement) before ever creating, so a crash between
// create and the annotation write cannot register a duplicate. Creation is gated
// by the Create management policy.
func (r *AuraCustomerManagedKeyReconciler) observeOrCreate(
	ctx context.Context, req ctrl.Request, cmk *neo4jv1beta1.AuraCustomerManagedKey,
	apiClient auraCMKAPI, projectID string, allowCreate bool,
) (id string, adopted bool, err error) {
	existing, err := apiClient.ListCustomerManagedKeys(ctx, projectID)
	if err != nil {
		return "", false, fmt.Errorf("listing customer-managed keys before create: %w", err)
	}
	for i := range existing {
		e := existing[i]
		if e.KeyID == cmk.Spec.KeyID && e.Region == cmk.Spec.Region &&
			e.CloudProvider == cmk.Spec.CloudProvider && e.InstanceType == cmk.Spec.InstanceType {
			if err := r.setExternalID(ctx, req, e.ID); err != nil {
				return "", false, err
			}
			return e.ID, true, nil
		}
	}

	if !allowCreate {
		return "", false, nil
	}

	createReq := aura.CreateCMKRequest{
		Name:          r.keyName(cmk),
		TenantID:      projectID,
		InstanceType:  cmk.Spec.InstanceType,
		CloudProvider: cmk.Spec.CloudProvider,
		KeyID:         cmk.Spec.KeyID,
		Region:        cmk.Spec.Region,
	}
	created, err := apiClient.CreateCustomerManagedKey(ctx, createReq)
	if err != nil {
		return "", false, err
	}
	// Persist the external ID BEFORE status — the idempotency guard.
	if err := r.setExternalID(ctx, req, created.ID); err != nil {
		return "", false, err
	}
	r.Recorder.Event(cmk, corev1.EventTypeNormal, EventReasonAuraCMKCreated,
		fmt.Sprintf("Registered customer-managed key %s", created.ID))
	return created.ID, false, nil
}

// handleDeletion honours the deletion policy: Orphan (default) leaves the key
// registered in Aura; Delete deregisters it — which the API refuses (400
// encryption-key-is-active) while any instance still uses it, so we surface a
// blocking condition and requeue rather than dropping the finalizer.
func (r *AuraCustomerManagedKeyReconciler) handleDeletion(
	ctx context.Context, req ctrl.Request, cmk *neo4jv1beta1.AuraCustomerManagedKey, apiClient auraCMKAPI,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cmk, AuraCMKFinalizer) {
		return ctrl.Result{}, nil
	}

	externalID := cmk.Annotations[AuraExternalCMKAnnotation]
	if externalID == "" {
		externalID = cmk.Status.CustomerManagedKeyID
	}

	deleteCloud := cmk.Spec.DeletionPolicy == "Delete" && managementAllows(cmk.Spec.ManagementPolicies, auraPolicyDelete)
	if deleteCloud && externalID != "" {
		if err := apiClient.DeleteCustomerManagedKey(ctx, externalID); err != nil {
			if aura.IsCMKActive(err) {
				r.Recorder.Event(cmk, corev1.EventTypeWarning, EventReasonAuraCMKDeleteBlocked,
					fmt.Sprintf("Cannot delete customer-managed key %s: still in use by one or more instances", externalID))
				_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "KeyInUse",
					"the customer-managed key is still bound to at least one instance; detach or delete those instances first")
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if !aura.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.Recorder.Event(cmk, corev1.EventTypeNormal, EventReasonAuraCMKDeleted,
			fmt.Sprintf("Deregistered customer-managed key %s", externalID))
	} else if externalID != "" {
		r.Recorder.Event(cmk, corev1.EventTypeNormal, EventReasonAuraCMKOrphaned,
			fmt.Sprintf("Orphaning customer-managed key %s; it stays registered in Aura", externalID))
	}

	return ctrl.Result{}, r.patchCMK(ctx, req, func(o *neo4jv1beta1.AuraCustomerManagedKey) {
		controllerutil.RemoveFinalizer(o, AuraCMKFinalizer)
	})
}

// keyName is the CMK's Aura display name (spec.name or the CR name, ≤30 chars).
func (r *AuraCustomerManagedKeyReconciler) keyName(cmk *neo4jv1beta1.AuraCustomerManagedKey) string {
	name := cmk.Spec.Name
	if name == "" {
		name = cmk.Name
	}
	if len(name) > 30 {
		name = name[:30]
	}
	return name
}

// setExternalID writes the Aura key ID to the external-name annotation.
func (r *AuraCustomerManagedKeyReconciler) setExternalID(ctx context.Context, req ctrl.Request, id string) error {
	return r.patchCMK(ctx, req, func(o *neo4jv1beta1.AuraCustomerManagedKey) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalCMKAnnotation] = id
	})
}

// patchCMK applies a mutation to the latest object with conflict retry.
func (r *AuraCustomerManagedKeyReconciler) patchCMK(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraCustomerManagedKey)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraCustomerManagedKey{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

// setCondition sets a status condition, writing only when it actually changes,
// so a steady terminal state (e.g. Paused) doesn't self-trigger a reconcile loop.
func (r *AuraCustomerManagedKeyReconciler) setCondition(ctx context.Context, req ctrl.Request, condType string, status metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraCustomerManagedKey{}
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

// syncStatus writes observed key state + conditions.
func (r *AuraCustomerManagedKeyReconciler) syncStatus(ctx context.Context, req ctrl.Request, cmk *neo4jv1beta1.AuraCustomerManagedKey, observed *aura.CustomerManagedKey) error {
	ready := aura.IsCMKReady(observed.Status)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraCustomerManagedKey{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.CustomerManagedKeyID = observed.ID
		latest.Status.Phase = observed.Status
		now := metav1.Now()
		latest.Status.LastSyncedTime = &now
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: auraCondStatus(ready),
			Reason:  cmkReadyReason(observed.Status),
			Message: fmt.Sprintf("Customer-managed key status: %s", observed.Status),
		})
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Synced", Status: metav1.ConditionTrue, Reason: "Observed",
			Message: "Reconciled against the Aura API",
		})
		return r.Status().Update(ctx, latest)
	})
}

func cmkReadyReason(status string) string {
	switch {
	case aura.IsCMKReady(status):
		return "Ready"
	case aura.IsCMKPending(status):
		return "Pending"
	default:
		return "NotReady"
	}
}

// fail records a failure on status and requeues without a hard error when the
// cause is external (missing creds/config), so we don't hot-loop on user error.
func (r *AuraCustomerManagedKeyReconciler) fail(ctx context.Context, req ctrl.Request, cmk *neo4jv1beta1.AuraCustomerManagedKey, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraCustomerManagedKey reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(cmk, corev1.EventTypeWarning, EventReasonAuraCMKFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraCustomerManagedKey{}
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
func (r *AuraCustomerManagedKeyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraCustomerManagedKey{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
