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
	apiClient := resolveMemberClient(r.ClientFactory, creds)
	if !controllerutil.ContainsFinalizer(m, AuraOrgMemberFinalizer) {
		// A tested JSON patch, no resourceVersion lock: a copy older than the
		// object is no Conflict (knowledge rule 199).
		if err := addFinalizer(ctx, r.Client, m, AuraOrgMemberFinalizer); err != nil {
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
		// Announced when the wait starts, not on every requeue until the person
		// accepts an invite (knowledge rule 145).
		if changed, _ := r.setStatusChanged(ctx, req, "", "NotAMember", metav1.ConditionFalse, "NotAMember",
			fmt.Sprintf("%q is not an organization member yet", m.Spec.Email)); changed {
			r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberNotFound,
				fmt.Sprintf("%q is not an organization member; invite them with an AuraInvite", m.Spec.Email))
		}
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}

	if member.Role() != m.Spec.Role && managementAllows(m.Spec.ManagementPolicies, auraPolicyUpdate) {
		if _, err := apiClient.UpdateOrgMemberRole(ctx, orgID, member.UserID, m.Spec.Role); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, m, "UpdateFailed", err)
		}
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberUpdated,
			fmt.Sprintf("Set %q org role to %s", m.Spec.Email, m.Spec.Role))
	}
	changed, err := r.setStatusChanged(ctx, req, member.UserID, neo4jv1beta1.PhaseReady, metav1.ConditionTrue, "Reconciled",
		"Organization membership reconciled (v2beta1, beta)")
	if err != nil {
		return ctrl.Result{}, err
	}
	// Announced when the membership becomes Ready, not on every re-observe
	// while it stays so (knowledge rule 136).
	if changed {
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberReady,
			fmt.Sprintf("Organization member %q reconciled", m.Spec.Email))
	}
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

func (r *AuraOrganizationMemberReconciler) handleDeletion(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraOrganizationMember) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(m, AuraOrgMemberFinalizer) {
		return ctrl.Result{}, nil
	}
	remove := m.Spec.DeletionPolicy == "Delete" && managementAllows(m.Spec.ManagementPolicies, auraPolicyDelete)
	if !remove || m.Status.UserID == "" {
		if m.Status.UserID != "" {
			r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberOrphaned,
				fmt.Sprintf("Orphaning organization member %q; they keep the role %s in Aura", m.Spec.Email, m.Spec.Role))
		}
		return r.releaseFinalizer(ctx, req)
	}
	// Removing calls Aura. Credentials or an organization that cannot be
	// resolved used to release the finalizer at once, without an event, and
	// the person kept their organization role. The deletion waits for them,
	// then says what it left (knowledge rules 157, 178).
	what := fmt.Sprintf("organization member %q", m.Spec.Email)
	apiClient, orgID, targetErr := r.deletionTarget(ctx, m)
	if targetErr != nil {
		if !deletionGracePeriodExceeded(m) {
			return r.fail(ctx, req, m, "CredentialsUnavailable",
				auraDeletionNeedsCredentials(m, what, m.Status.UserID, targetErr))
		}
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberOrphaned,
			auraOrphanedWithoutCredentials(what, m.Status.UserID,
				fmt.Sprintf("they keep the role %s in the organization", m.Spec.Role), targetErr))
		return r.releaseFinalizer(ctx, req)
	}
	// A DELETE Aura refused is waited on, bounded, then the member is left
	// and named — it was returned as an error, with backoff for ever
	// (knowledge rule 201).
	err := apiClient.DeleteOrgMember(ctx, orgID, m.Status.UserID)
	switch {
	case err == nil || aura.IsNotFound(err):
		r.Recorder.Event(m, corev1.EventTypeNormal, EventReasonAuraMemberRemoved,
			fmt.Sprintf("Removed %q from the organization", m.Spec.Email))
	case !auraDeleteRefused(err):
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	case !deletionGracePeriodExceeded(m):
		return r.fail(ctx, req, m, auraReasonDeletionRefused,
			auraDeletionRefusedWait(m, what, m.Status.UserID, err))
	default:
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberOrphaned,
			auraOrphanedAfterRefusal(what, m.Status.UserID,
				fmt.Sprintf("they keep the role %s in the organization", m.Spec.Role), err))
	}
	return r.releaseFinalizer(ctx, req)
}

// deletionTarget resolves what removing the member needs: the client, from
// the credentials, and the organization.
func (r *AuraOrganizationMemberReconciler) deletionTarget(ctx context.Context, m *neo4jv1beta1.AuraOrganizationMember) (auraMemberAPI, string, error) {
	creds, err := resolveAuraCredentials(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.CredentialsSecretRef)
	if err != nil {
		return nil, "", err
	}
	orgID := resolveProviderOrgID(ctx, r.Client, m.Namespace, m.Spec.ProviderConfigRef, m.Spec.OrganizationID)
	if orgID == "" {
		return nil, "", fmt.Errorf("organizationId is required (set it on the CR or as defaultOrganizationId on the AuraProviderConfig)")
	}
	return resolveMemberClient(r.ClientFactory, creds), orgID, nil
}

func (r *AuraOrganizationMemberReconciler) releaseFinalizer(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// One removal a stale cache or a status write just before cannot turn
	// into a Conflict, and that an object already gone has done already
	// (knowledge rule 189).
	return ctrl.Result{}, removeFinalizerAt(ctx, r.Client, req.NamespacedName, &neo4jv1beta1.AuraOrganizationMember{}, AuraOrgMemberFinalizer)
}

// setStatusChanged writes the phase, the user ID and the Ready condition, and
// reports whether the write changed the phase or the Ready condition's status,
// reason or message (LastSyncedTime is bookkeeping), from the attempt whose
// write succeeded, so a caller can announce a state once (knowledge rule 145).
//
// It writes only when something changed or the stored lastSyncedTime is due a
// refresh: a status write is a watch event, and stamping lastSyncedTime on
// every pass started the next one at once, about twice every 30s while a
// member was failing or waiting (auraSyncedTimeRefresh).
func (r *AuraOrganizationMemberReconciler) setStatusChanged(ctx context.Context, req ctrl.Request, userID, phase string, condStatus metav1.ConditionStatus, reason, msg string) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		latest := &neo4jv1beta1.AuraOrganizationMember{}
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

func (r *AuraOrganizationMemberReconciler) fail(ctx context.Context, req ctrl.Request, m *neo4jv1beta1.AuraOrganizationMember, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraOrganizationMember reconcile deferred", "reason", reason, "error", cause.Error())
	// Announced when the failure starts or its message changes, not on every
	// requeue while it stays (knowledge rule 145).
	if changed, _ := r.setStatusChanged(ctx, req, "", "Error", metav1.ConditionFalse, reason, aura.MessageWithoutRequestID(cause)); changed {
		r.Recorder.Event(m, corev1.EventTypeWarning, EventReasonAuraMemberFailed, fmt.Sprintf("%s: %v", reason, cause))
	}
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
