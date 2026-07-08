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

// AuraOrgMemberFinalizer guards member removal on CR deletion.
const AuraOrgMemberFinalizer = "neo4j.com/auraorgmember-finalizer"

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraorganizationmembers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraorganizationmembers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraorganizationmembers/finalizers,verbs=update

// AuraOrganizationMemberReconciler reconciles the organization role of an
// existing Aura console user via the Aura API v2beta1. BETA / best-effort.
type AuraOrganizationMemberReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraMemberClientFactory
}

func (r *AuraOrganizationMemberReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the org-membership state machine.
func (r *AuraOrganizationMemberReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	m := &neo4jv1beta1.AuraOrganizationMember{}
	if err := r.Get(ctx, req.NamespacedName, m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if m.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraOrganizationMember reconciliation paused via annotation")
		return ctrl.Result{}, nil
	}

	creds, err := resolveAuraCredentials(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.CredentialsSecretRef)
	if err != nil {
		if !m.DeletionTimestamp.IsZero() {
			return r.releaseFinalizer(ctx, req)
		}
		return r.fail(ctx, req, m, "CredentialsUnavailable", err)
	}
	orgID := resolveProviderOrgID(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.OrganizationID)
	apiClient := resolveMemberClient(r.ClientFactory, creds)

	if !m.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, m, apiClient, orgID)
	}
	if !controllerutil.ContainsFinalizer(m, AuraOrgMemberFinalizer) {
		if err := r.patch(ctx, req, func(o *neo4jv1beta1.AuraOrganizationMember) {
			controllerutil.AddFinalizer(o, AuraOrgMemberFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}
	if orgID == "" {
		return r.fail(ctx, req, m, "OrganizationMissing",
			fmt.Errorf("organizationId is required (set it on the CR or as defaultOrganizationId on the AuraProviderConfig)"))
	}

	member, err := r.findByEmail(ctx, apiClient, orgID, m.Spec.Email)
	if err != nil {
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, m, "LookupFailed", err)
	}
	if member == nil {
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberNotFound,
			fmt.Sprintf("%q is not an organization member; invite them with an AuraInvite", m.Spec.Email))
		_ = r.setStatus(ctx, req, "", "NotAMember", metav1.ConditionFalse, "NotAMember",
			fmt.Sprintf("%q is not an organization member yet", m.Spec.Email))
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}

	if member.Role != m.Spec.Role && managementAllows(m.Spec.ManagementPolicies, auraPolicyUpdate) {
		if _, err := apiClient.UpdateOrgMemberRole(ctx, orgID, member.ID, m.Spec.Role); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, m, "UpdateFailed", err)
		}
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberUpdated,
			fmt.Sprintf("Set %q org role to %s", m.Spec.Email, m.Spec.Role))
	}
	if err := r.setStatus(ctx, req, member.ID, "Ready", metav1.ConditionTrue, "Reconciled",
		"Organization membership reconciled (v2beta1, beta)"); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberReady,
		fmt.Sprintf("Organization member %q reconciled", m.Spec.Email))
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *AuraOrganizationMemberReconciler) findByEmail(ctx context.Context, apiClient auraMemberAPI, orgID, email string) (*aura.Member, error) {
	members, err := apiClient.ListOrgMembers(ctx, orgID)
	if err != nil {
		return nil, err
	}
	for i := range members {
		if strings.EqualFold(members[i].Email, email) {
			return &members[i], nil
		}
	}
	return nil, nil
}

func (r *AuraOrganizationMemberReconciler) handleDeletion(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraOrganizationMember, apiClient auraMemberAPI, orgID string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(m, AuraOrgMemberFinalizer) {
		return ctrl.Result{}, nil
	}
	remove := m.Spec.DeletionPolicy == "Delete" && managementAllows(m.Spec.ManagementPolicies, auraPolicyDelete)
	if remove && orgID != "" && m.Status.UserID != "" {
		if err := apiClient.DeleteOrgMember(ctx, orgID, m.Status.UserID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if !aura.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberRemoved,
			fmt.Sprintf("Removed %q from the organization", m.Spec.Email))
	}
	return r.releaseFinalizer(ctx, req)
}

func (r *AuraOrganizationMemberReconciler) releaseFinalizer(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, r.patch(ctx, req, func(o *neo4jv1beta1.AuraOrganizationMember) {
		controllerutil.RemoveFinalizer(o, AuraOrgMemberFinalizer)
	})
}

func (r *AuraOrganizationMemberReconciler) setStatus(ctx context.Context, req ctrl.Request, userID, phase string, condStatus metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraOrganizationMember{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		if userID != "" {
			latest.Status.UserID = userID
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

func (r *AuraOrganizationMemberReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraOrganizationMember)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraOrganizationMember{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

func (r *AuraOrganizationMemberReconciler) fail(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraOrganizationMember, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraOrganizationMember reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = r.setStatus(ctx, req, "", "Error", metav1.ConditionFalse, reason, cause.Error())
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraOrganizationMemberReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraOrganizationMember{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
