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

	// A deletion resolves the credentials only to call Aura, and waits for
	// them a bounded time (knowledge rules 157, 178).
	if !m.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, m)
	}
	creds, err := resolveAuraCredentials(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.CredentialsSecretRef)
	if err != nil {
		return r.fail(ctx, req, m, "CredentialsUnavailable", err)
	}
	orgID := resolveProviderOrgID(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.OrganizationID)
	projectID := m.Spec.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	apiClient := resolveMemberClient(r.ClientFactory, creds)
	if !controllerutil.ContainsFinalizer(m, AuraProjectMemberFinalizer) {
		// A tested JSON patch, no resourceVersion lock: a copy older than the
		// object is no Conflict (knowledge rule 199).
		if err := addFinalizer(ctx, r.Client, m, AuraProjectMemberFinalizer); err != nil {
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
			// Announced when the wait starts, not on every requeue until the
			// person accepts an invite (knowledge rule 145).
			if changed, _ := r.setStatusChanged(ctx, req, "", "NotAMember", metav1.ConditionFalse, "NotAMember",
				fmt.Sprintf("%q is not an organization member yet", m.Spec.Email)); changed {
				r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberNotFound,
					fmt.Sprintf("%q is not an organization member; invite them with an AuraInvite", m.Spec.Email))
			}
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
	changed, err := r.setStatusChanged(ctx, req, member.UserID, neo4jv1beta1.PhaseReady, metav1.ConditionTrue, "Reconciled",
		"Project membership reconciled (v2beta1, beta)")
	if err != nil {
		return ctrl.Result{}, err
	}
	// Announced when the membership becomes Ready, not on every re-observe
	// while it stays so (knowledge rule 136).
	if changed {
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberReady,
			fmt.Sprintf("Project member %q reconciled", m.Spec.Email))
	}
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

func (r *AuraProjectMemberReconciler) handleDeletion(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraProjectMember) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(m, AuraProjectMemberFinalizer) {
		return ctrl.Result{}, nil
	}
	remove := m.Spec.DeletionPolicy == "Delete" && managementAllows(m.Spec.ManagementPolicies, auraPolicyDelete)
	if !remove || m.Status.UserID == "" {
		if m.Status.UserID != "" {
			r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberOrphaned,
				fmt.Sprintf("Orphaning project member %q; they keep the role %s in Aura", m.Spec.Email, m.Spec.Role))
		}
		return r.releaseFinalizer(ctx, req)
	}
	// Removing calls Aura. Credentials, an organization or a project that
	// cannot be resolved used to release the finalizer at once, without an
	// event, and the person kept their project role. The deletion waits for
	// them, then says what it left (knowledge rules 157, 178).
	what := fmt.Sprintf("project member %q", m.Spec.Email)
	apiClient, orgID, projectID, targetErr := r.deletionTarget(ctx, m)
	if targetErr != nil {
		if !deletionGracePeriodExceeded(m) {
			return r.fail(ctx, req, m, "CredentialsUnavailable",
				auraDeletionNeedsCredentials(m, what, m.Status.UserID, targetErr))
		}
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberOrphaned,
			auraOrphanedWithoutCredentials(what, m.Status.UserID,
				fmt.Sprintf("they keep the role %s in the project", m.Spec.Role), targetErr))
		return r.releaseFinalizer(ctx, req)
	}
	// A DELETE Aura refused is waited on, bounded, then the member is left
	// and named — it was returned as an error, with backoff for ever
	// (knowledge rule 201).
	err := apiClient.DeleteProjectMember(ctx, orgID, projectID, m.Status.UserID)
	switch {
	case err == nil || aura.IsNotFound(err):
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberRemoved,
			fmt.Sprintf("Removed %q from the project", m.Spec.Email))
	case !auraDeleteRefused(err):
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	case !deletionGracePeriodExceeded(m):
		return r.fail(ctx, req, m, auraReasonDeletionRefused,
			auraDeletionRefusedWait(m, what, m.Status.UserID, err))
	default:
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberOrphaned,
			auraOrphanedAfterRefusal(what, m.Status.UserID,
				fmt.Sprintf("they keep the role %s in the project", m.Spec.Role), err))
	}
	return r.releaseFinalizer(ctx, req)
}

// deletionTarget resolves what removing the member needs: the client, from
// the credentials, the organization and the project.
func (r *AuraProjectMemberReconciler) deletionTarget(ctx context.Context, m *neo4jv1beta1.AuraProjectMember) (auraMemberAPI, string, string, error) {
	creds, err := resolveAuraCredentials(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.CredentialsSecretRef)
	if err != nil {
		return nil, "", "", err
	}
	orgID := resolveProviderOrgID(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.OrganizationID)
	projectID := m.Spec.ProjectID
	if projectID == "" {
		projectID = creds.projectID
	}
	if orgID == "" || projectID == "" {
		return nil, "", "", fmt.Errorf("organizationId and projectId are required (set them on the CR or as defaults on the AuraProviderConfig)")
	}
	return resolveMemberClient(r.ClientFactory, creds), orgID, projectID, nil
}

func (r *AuraProjectMemberReconciler) releaseFinalizer(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// One removal a stale cache or a status write just before cannot turn
	// into a Conflict, and that an object already gone has done already
	// (knowledge rule 189).
	return ctrl.Result{}, removeFinalizerAt(ctx, r.Client, req.NamespacedName, &neo4jv1beta1.AuraProjectMember{}, AuraProjectMemberFinalizer)
}

func (r *AuraProjectMemberReconciler) setStatus(ctx context.Context, req ctrl.Request, userID, phase string, condStatus metav1.ConditionStatus, reason, msg string) error {
	_, err := r.setStatusChanged(ctx, req, userID, phase, condStatus, reason, msg)
	return err
}

// setStatusChanged is setStatus that also reports whether the write changed the
// phase or the Ready condition's status, reason or message (LastSyncedTime is
// bookkeeping), from the attempt whose write succeeded, so a caller can announce
// a state once (knowledge rule 145).
//
// It writes only when something changed or the stored lastSyncedTime is due a
// refresh: a status write is a watch event, and stamping lastSyncedTime on
// every pass started the next one at once, about twice every 30s while a
// member was failing or waiting (auraSyncedTimeRefresh).
func (r *AuraProjectMemberReconciler) setStatusChanged(ctx context.Context, req ctrl.Request, userID, phase string, condStatus metav1.ConditionStatus, reason, msg string) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		latest := &neo4jv1beta1.AuraProjectMember{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		before := latest.Status.DeepCopy()
		want := metav1.Condition{Type: "Ready", Status: condStatus, Reason: reason, Message: msg}
		wouldChange := latest.Status.Phase != phase || auraConditionChanged(latest.Status.Conditions, want)
		if userID != "" {
			latest.Status.UserID = userID
		}
		latest.Status.Phase = phase
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, want)
		now := metav1.Now()
		if !auraStatusWriteNeeded(*before, latest.Status, before.LastSyncedTime, now.Time) {
			return nil
		}
		latest.Status.LastSyncedTime = &now
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		changed = wouldChange
		return nil
	})
	return changed, err
}

func (r *AuraProjectMemberReconciler) fail(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraProjectMember, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraProjectMember reconcile deferred", "reason", reason, "error", cause.Error())
	// Announced when the failure starts or its message changes, not on every
	// requeue while it stays (knowledge rule 145).
	if changed, _ := r.setStatusChanged(ctx, req, "", "Error", metav1.ConditionFalse, reason, aura.MessageWithoutRequestID(cause)); changed {
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberFailed, fmt.Sprintf("%s: %v", reason, cause))
	}
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
		Complete(endsWithItsNamespace(r))
}
