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

// AuraProjectMemberFinalizer guards member removal on CR deletion.
const AuraProjectMemberFinalizer = "neo4j.com/auraprojectmember-finalizer"

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraprojectmembers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraprojectmembers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auraprojectmembers/finalizers,verbs=update

// AuraProjectMemberReconciler reconciles the project role of an existing Aura
// console user via the Aura API v2beta1. BETA / best-effort.
type AuraProjectMemberReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraMemberClientFactory
}

func (r *AuraProjectMemberReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives one pass of the project-membership state machine.
func (r *AuraProjectMemberReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	m := &neo4jv1beta1.AuraProjectMember{}
	if err := r.Get(ctx, req.NamespacedName, m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if m.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraProjectMember reconciliation paused via annotation")
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
	projectID := m.Spec.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	apiClient := resolveMemberClient(r.ClientFactory, creds)

	if !m.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, m, apiClient, orgID, projectID)
	}
	if !controllerutil.ContainsFinalizer(m, AuraProjectMemberFinalizer) {
		if err := r.patch(ctx, req, func(o *neo4jv1beta1.AuraProjectMember) {
			controllerutil.AddFinalizer(o, AuraProjectMemberFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}
	if orgID == "" || projectID == "" {
		return r.fail(ctx, req, m, "TargetMissing",
			fmt.Errorf("organizationId and projectId are required (set them on the CR or as defaults on the AuraProviderConfig)"))
	}

	member, err := r.findByEmail(ctx, apiClient, orgID, projectID, m.Spec.Email)
	if err != nil {
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, m, "LookupFailed", err)
	}
	// Not yet a project member: if the person already exists at ORGANIZATION
	// level we can add them directly (POST project users takes the Aura user
	// UUID + a project role). Only a wholly unknown email needs an AuraInvite.
	if member == nil {
		orgMember, err := r.findOrgMemberByEmail(ctx, apiClient, orgID, m.Spec.Email)
		if err != nil {
			if aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, m, "LookupFailed", err)
		}
		if orgMember == nil {
			r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberNotFound,
				fmt.Sprintf("%q is not an organization member; invite them with an AuraInvite", m.Spec.Email))
			_ = r.setStatus(ctx, req, "", "NotAMember", metav1.ConditionFalse, "NotAMember",
				fmt.Sprintf("%q is not an organization member yet", m.Spec.Email))
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		if !managementAllows(m.Spec.ManagementPolicies, auraPolicyCreate) {
			_ = r.setStatus(ctx, req, orgMember.UserID, "NotAMember", metav1.ConditionFalse, "CreateNotPermitted",
				fmt.Sprintf("%q is not a project member and managementPolicies does not permit Create", m.Spec.Email))
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		if err := apiClient.AddProjectMember(ctx, orgID, projectID, orgMember.UserID, m.Spec.Role); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, m, "AddFailed", err)
		}
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberUpdated,
			fmt.Sprintf("Added %q to the project as %s", m.Spec.Email, m.Spec.Role))
		if err := r.setStatus(ctx, req, orgMember.UserID, "Ready", metav1.ConditionTrue, "Reconciled",
			"Project membership created (v2beta1, beta)"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	if member.Role() != m.Spec.Role && managementAllows(m.Spec.ManagementPolicies, auraPolicyUpdate) {
		if _, err := apiClient.UpdateProjectMemberRole(ctx, orgID, projectID, member.UserID, m.Spec.Role); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, m, "UpdateFailed", err)
		}
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberUpdated,
			fmt.Sprintf("Set %q project role to %s", m.Spec.Email, m.Spec.Role))
	}
	if err := r.setStatus(ctx, req, member.UserID, "Ready", metav1.ConditionTrue, "Reconciled",
		"Project membership reconciled (v2beta1, beta)"); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberReady,
		fmt.Sprintf("Project member %q reconciled", m.Spec.Email))
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *AuraProjectMemberReconciler) findByEmail(ctx context.Context, apiClient auraMemberAPI, orgID, projectID, email string) (*aura.Member, error) {
	members, err := apiClient.ListProjectMembers(ctx, orgID, projectID)
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

// findOrgMemberByEmail resolves an email to the ORGANIZATION-level member, whose
// UserID is what POST project users requires (it takes a UUID, never an email).
func (r *AuraProjectMemberReconciler) findOrgMemberByEmail(ctx context.Context, apiClient auraMemberAPI, orgID, email string) (*aura.Member, error) {
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

func (r *AuraProjectMemberReconciler) handleDeletion(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraProjectMember, apiClient auraMemberAPI, orgID, projectID string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(m, AuraProjectMemberFinalizer) {
		return ctrl.Result{}, nil
	}
	remove := m.Spec.DeletionPolicy == "Delete" && managementAllows(m.Spec.ManagementPolicies, auraPolicyDelete)
	if remove && orgID != "" && projectID != "" && m.Status.UserID != "" {
		if err := apiClient.DeleteProjectMember(ctx, orgID, projectID, m.Status.UserID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			if !aura.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberRemoved,
			fmt.Sprintf("Removed %q from the project", m.Spec.Email))
	}
	return r.releaseFinalizer(ctx, req)
}

func (r *AuraProjectMemberReconciler) releaseFinalizer(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, r.patch(ctx, req, func(o *neo4jv1beta1.AuraProjectMember) {
		controllerutil.RemoveFinalizer(o, AuraProjectMemberFinalizer)
	})
}

func (r *AuraProjectMemberReconciler) setStatus(ctx context.Context, req ctrl.Request, userID, phase string, condStatus metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraProjectMember{}
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

func (r *AuraProjectMemberReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraProjectMember)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraProjectMember{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Update(ctx, latest)
	})
}

func (r *AuraProjectMemberReconciler) fail(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraProjectMember, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraProjectMember reconcile deferred", "reason", reason, "error", cause.Error())
	r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberFailed, fmt.Sprintf("%s: %v", reason, cause))
	_ = r.setStatus(ctx, req, "", "Error", metav1.ConditionFalse, reason, cause.Error())
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraProjectMemberReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraProjectMember{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
