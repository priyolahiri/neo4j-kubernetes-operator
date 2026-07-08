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
	"strings"
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

// AuraInviteFinalizer guards revoking a pending invite on CR deletion.
const AuraInviteFinalizer = "neo4j.com/aurainvite-finalizer"

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainvites,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainvites/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainvites/finalizers,verbs=update

// AuraInviteReconciler manages an organization/project invite via the Aura API
// v2beta1. BETA / best-effort.
type AuraInviteReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraMemberClientFactory
}

func (r *AuraInviteReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the invite state machine.
func (r *AuraInviteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	inv := &neo4jv1beta1.AuraInvite{}
	if err := r.Get(ctx, req.NamespacedName, inv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if inv.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraInvite reconciliation paused via annotation")
		return ctrl.Result{}, nil
	}

	creds, err := resolveAuraCredentials(ctx, r.Client, inv.Namespace, inv.Spec.ProviderConfigRef, inv.Spec.CredentialsSecretRef)
	if err != nil {
		if !inv.DeletionTimestamp.IsZero() {
			return r.releaseFinalizer(ctx, req)
		}
		return r.fail(ctx, req, inv, "CredentialsUnavailable", err)
	}
	orgID := resolveProviderOrgID(ctx, r.Client, inv.Namespace, inv.Spec.ProviderConfigRef, inv.Spec.OrganizationID)
	apiClient := resolveMemberClient(r.ClientFactory, creds)

	if !inv.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, inv, apiClient, orgID)
	}
	if !controllerutil.ContainsFinalizer(inv, AuraInviteFinalizer) {
		if err := r.patch(ctx, req, func(o *neo4jv1beta1.AuraInvite) {
			controllerutil.AddFinalizer(o, AuraInviteFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}
	if orgID == "" {
		return r.fail(ctx, req, inv, "OrganizationMissing",
			fmt.Errorf("organizationId is required (set it on the CR or as defaultOrganizationId on the AuraProviderConfig)"))
	}

	externalID := inv.Annotations[AuraExternalInviteAnnotation]
	if externalID == "" {
		externalID = inv.Status.InviteID
	}
	if externalID == "" {
		id, adopted, err := r.observeOrCreate(ctx, req, inv, apiClient, orgID)
		if err != nil {
			return r.fail(ctx, req, inv, "CreateFailed", err)
		}
		if id == "" {
			_ = r.setStatus(ctx, req, "", "Pending", metav1.ConditionFalse, "AwaitingInvite",
				"no matching invite found and managementPolicies does not permit Create")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		externalID = id
		if adopted {
			r.Recorder.Event(inv, corev1.EventTypeNormal, EventReasonAuraInviteAdopted,
				fmt.Sprintf("Adopted existing invite %s", externalID))
		}
	}

	observed, err := apiClient.GetInvite(ctx, orgID, externalID)
	if err != nil {
		if aura.IsNotFound(err) {
			// The invite is gone (accepted or revoked out of band) — treat as done.
			_ = r.setStatus(ctx, req, externalID, "Accepted", metav1.ConditionTrue, "Gone",
				"invite is no longer pending (accepted or revoked)")
			return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
		}
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, inv, "ObserveFailed", err)
	}
	if err := r.setStatus(ctx, req, observed.ID, "Sent", metav1.ConditionTrue, "Reconciled",
		"Invite reconciled against the Aura API (v2beta1, beta)"); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(inv, corev1.EventTypeNormal, EventReasonAuraInviteReady,
		fmt.Sprintf("Invite for %q reconciled", inv.Spec.Email))
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *AuraInviteReconciler) observeOrCreate(ctx context.Context, req ctrl.Request, inv *neo4jv1beta1.AuraInvite, apiClient auraMemberAPI, orgID string) (id string, adopted bool, err error) {
	existing, err := apiClient.ListInvites(ctx, orgID)
	if err != nil {
		return "", false, fmt.Errorf("listing invites before create: %w", err)
	}
	for i := range existing {
		if strings.EqualFold(existing[i].Email, inv.Spec.Email) && existing[i].ProjectID == inv.Spec.ProjectID {
			if err := r.setExternalID(ctx, req, existing[i].ID); err != nil {
				return "", false, err
			}
			return existing[i].ID, true, nil
		}
	}
	if !managementAllows(inv.Spec.ManagementPolicies, auraPolicyCreate) {
		return "", false, nil
	}
	created, err := apiClient.CreateInvite(ctx, orgID, aura.CreateInviteRequest{
		Email:     inv.Spec.Email,
		Role:      inv.Spec.Role,
		ProjectID: inv.Spec.ProjectID,
	})
	if err != nil {
		return "", false, err
	}
	if err := r.setExternalID(ctx, req, created.ID); err != nil {
		return "", false, err
	}
	r.Recorder.Event(inv, corev1.EventTypeNormal, EventReasonAuraInviteCreated,
		fmt.Sprintf("Invited %q (%s)", inv.Spec.Email, inv.Spec.Role))
	return created.ID, false, nil
}

func (r *AuraInviteReconciler) handleDeletion(ctx context.Context, req ctrl.Request, inv *neo4jv1beta1.AuraInvite, apiClient auraMemberAPI, orgID string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(inv, AuraInviteFinalizer) {
		return ctrl.Result{}, nil
	}
	externalID := inv.Annotations[AuraExternalInviteAnnotation]
	if externalID == "" {
		externalID = inv.Status.InviteID
	}
	revoke := inv.Spec.DeletionPolicy != "Orphan" && managementAllows(inv.Spec.ManagementPolicies, auraPolicyDelete)
	if revoke && externalID != "" && orgID != "" {
		if err := apiClient.DeleteInvite(ctx, orgID, externalID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if !aura.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.Recorder.Event(inv, corev1.EventTypeNormal, EventReasonAuraInviteDeleted,
			fmt.Sprintf("Revoked invite %s", externalID))
	} else if externalID != "" {
		r.Recorder.Event(inv, corev1.EventTypeNormal, EventReasonAuraInviteOrphaned,
			fmt.Sprintf("Orphaning invite %s; it stays pending in Aura", externalID))
	}
	return r.releaseFinalizer(ctx, req)
}

func (r *AuraInviteReconciler) releaseFinalizer(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, r.patch(ctx, req, func(o *neo4jv1beta1.AuraInvite) {
		controllerutil.RemoveFinalizer(o, AuraInviteFinalizer)
	})
}

func (r *AuraInviteReconciler) setExternalID(ctx context.Context, req ctrl.Request, id string) error {
	return r.patch(ctx, req, func(o *neo4jv1beta1.AuraInvite) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalInviteAnnotation] = id
	})
}

func (r *AuraInviteReconciler) setStatus(ctx context.Context, req ctrl.Request, inviteID, phase string, condStatus metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInvite{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		if inviteID != "" {
			latest.Status.InviteID = inviteID
		}
		latest.Status.Phase = phase
		now := metav1.Now()
		latest.Status.LastSyncedTime = &now
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: condStatus, Reason: reason, Message: msg,
		})
		return r.Status().Update(ctx, latest)
	})
}

func (r *AuraInviteReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraInvite)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInvite{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

func (r *AuraInviteReconciler) fail(ctx context.Context, req ctrl.Request, inv *neo4jv1beta1.AuraInvite, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraInvite reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(inv, corev1.EventTypeWarning, EventReasonAuraInviteFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = r.setStatus(ctx, req, "", "Error", metav1.ConditionFalse, reason, cause.Error())
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraInviteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraInvite{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
