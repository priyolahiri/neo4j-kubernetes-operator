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
		// The refusal depends on the spec alone, so once status records it for
		// this generation the pass stops before listing the invites again
		// (knowledge rule 156).
		if _, refusal := buildInviteRequest(inv); refusal != nil &&
			auraRefusalRecorded(inv.Status.Conditions, inv.Status.ObservedGeneration, inv.Generation,
				auraReasonOrganizationRoleRequired, refusal.Error()) {
			return ctrl.Result{}, nil
		}
		id, adopted, err := r.observeOrCreate(ctx, req, inv, apiClient, orgID)
		if err != nil {
			// A refusal describes a spec that Aura can never accept (see
			// buildInviteRequest), so retrying it every 30s only rewrites the same
			// status. The spec edit that fixes it triggers its own reconcile.
			if isAuraRefusal(err) {
				res, ferr := r.fail(ctx, req, inv, auraReasonOrganizationRoleRequired, err)
				res.RequeueAfter = 0
				return res, ferr
			}
			// A 429, a 5xx or a transport failure of an Aura call is retried as it
			// is, not recorded as a failure (knowledge rule 88).
			if isRetryableAuraCall(err) {
				logger.Info("invite create deferred by a transient API failure; retrying", "error", err.Error())
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
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

	// v2beta1 has no GET /invites/{id} (only DELETE), so a single-invite read
	// goes through the LIST endpoint. A nil result means the invite is no longer
	// listed at all.
	observed, err := apiClient.FindInvite(ctx, orgID, externalID)
	if err != nil {
		if aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, inv, "ObserveFailed", err)
	}
	if observed == nil {
		// Dropped off the list entirely — accepted or revoked out of band. We
		// cannot tell which without a status, so report it neutrally as Gone.
		_ = r.setStatus(ctx, req, externalID, "Gone", metav1.ConditionTrue, "Gone",
			"invite is no longer listed (accepted, declined, revoked or expired)")
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// The invite IS listed, so use its own status field rather than inferring
	// from existence. Only `active` is genuinely still pending.
	phase, condition, reason, message := inviteStatusToPhase(observed.Status)
	changed, err := r.setStatusChanged(ctx, req, observed.ID, phase, condition, reason, message)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Announced when the invite's state changes (sent, accepted, expired, …),
	// not on every re-observe while it stays (knowledge rule 136).
	if changed {
		r.Recorder.Event(inv, corev1.EventTypeNormal, EventReasonAuraInviteReady,
			fmt.Sprintf("Invite for %q reconciled (%s)", inv.Spec.Email, phase))
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// auraReasonOrganizationRoleRequired is the Ready reason of the terminal
// refusal of an invite without an organization role, and what the refusal
// guard looks for (knowledge rule 91: guard and writer share the name).
const auraReasonOrganizationRoleRequired = "OrganizationRoleRequired"

// inviteStatusToPhase maps the v2beta1 OrganizationInvite.status enum onto the
// CR's phase + Ready condition. An empty status (older/partial payload) is
// treated as still-pending rather than assumed accepted.
func inviteStatusToPhase(status string) (phase string, cond metav1.ConditionStatus, reason, message string) {
	switch status {
	case aura.InviteStatusAccepted:
		return "Accepted", metav1.ConditionTrue, "Accepted", "invite was accepted"
	case aura.InviteStatusRevoked:
		return "Revoked", metav1.ConditionFalse, "Revoked", "invite was revoked"
	case aura.InviteStatusExpired:
		return "Expired", metav1.ConditionFalse, "Expired", "invite expired before it was accepted"
	case aura.InviteStatusDeclined:
		return "Declined", metav1.ConditionFalse, "Declined", "invite was declined by the invitee"
	case aura.InviteStatusActive, "":
		return "Sent", metav1.ConditionTrue, "Reconciled", "invite is pending acceptance (v2beta1, beta)"
	default:
		return "Sent", metav1.ConditionTrue, "Reconciled",
			fmt.Sprintf("invite reconciled; Aura reported status %q", status)
	}
}

// buildInviteRequest maps the CR onto the two-slot v2beta1 invite body: an
// `organization-*` role fills the org slot, a `namespace-*` role fills a
// project_invites entry.
//
// Aura requires at least one ORGANIZATION role on every invite and rejects a
// null/absent project_invites, so both slots are always populated — an empty
// project_invites list is the normal shape for an organization-only invite.
// Before this was verified live, BOTH branches produced a body the API rejected:
// an organization-role invite omitted project_invites ("Field required"), and a
// namespace-role invite omitted roles. Only the combination of a namespace role
// AND spec.organizationRole happened to work.
//
// A namespace-only invite is not expressible, so it is refused with an
// explanation rather than silently granting an organization role nobody asked
// for — that is a privilege decision for the user, not the operator.
func buildInviteRequest(inv *neo4jv1beta1.AuraInvite) (aura.CreateInviteRequest, error) {
	out := aura.CreateInviteRequest{
		Email:          inv.Spec.Email,
		ProjectInvites: []aura.ProjectInvite{}, // never nil: null is rejected
	}
	if strings.HasPrefix(inv.Spec.Role, "namespace-") {
		out.ProjectInvites = []aura.ProjectInvite{{
			ProjectID:    inv.Spec.ProjectID,
			ProjectRoles: []string{inv.Spec.Role},
		}}
		if inv.Spec.OrganizationRole == "" {
			return aura.CreateInviteRequest{}, refusef(
				"invite for %q requests only the project role %q, but Aura requires an organization role on "+
					"every invite; set spec.organizationRole (organization-owner, organization-admin or "+
					"organization-member)", inv.Spec.Email, inv.Spec.Role)
		}
		out.Roles = []string{inv.Spec.OrganizationRole}
		return out, nil
	}
	out.Roles = []string{inv.Spec.Role}
	return out, nil
}

// inviteCoversProject reports whether an existing invite is scoped to projectID,
// so an adopt only matches an invite of the same shape. projectID == "" means an
// organization-level invite, which must carry no project_invites.
func inviteCoversProject(existing aura.Invite, projectID string) bool {
	if projectID == "" {
		return len(existing.ProjectInvites) == 0
	}
	for _, pi := range existing.ProjectInvites {
		if pi.ProjectID == projectID {
			return true
		}
	}
	return false
}

func (r *AuraInviteReconciler) observeOrCreate(ctx context.Context, req ctrl.Request, inv *neo4jv1beta1.AuraInvite, apiClient auraMemberAPI, orgID string) (id string, adopted bool, err error) {
	existing, err := apiClient.ListInvites(ctx, orgID)
	if err != nil {
		return "", false, retryableAuraCall(fmt.Errorf("listing invites before create: %w", err))
	}
	for i := range existing {
		if strings.EqualFold(existing[i].Email, inv.Spec.Email) && inviteCoversProject(existing[i], inv.Spec.ProjectID) {
			if err := r.setExternalID(ctx, req, existing[i].ID); err != nil {
				return "", false, err
			}
			return existing[i].ID, true, nil
		}
	}
	if !managementAllows(inv.Spec.ManagementPolicies, auraPolicyCreate) {
		return "", false, nil
	}
	body, err := buildInviteRequest(inv)
	if err != nil {
		return "", false, err
	}
	created, err := apiClient.CreateInvite(ctx, orgID, body)
	if err != nil {
		return "", false, retryableAuraCall(err)
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
	_, err := r.setStatusChanged(ctx, req, inviteID, phase, condStatus, reason, msg)
	return err
}

// setStatusChanged is setStatus that also reports whether the write changed the
// phase or the Ready condition's status, reason or message (LastSyncedTime is
// bookkeeping), from the attempt whose write succeeded, so a caller can announce
// a state once (knowledge rule 145).
//
// It writes only when something changed or the stored lastSyncedTime is due a
// refresh: a status write is a watch event, and stamping lastSyncedTime on
// every pass started the next one at once, about twice every 30s while an
// invite was failing (auraSyncedTimeRefresh).
func (r *AuraInviteReconciler) setStatusChanged(ctx context.Context, req ctrl.Request, inviteID, phase string, condStatus metav1.ConditionStatus, reason, msg string) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		latest := &neo4jv1beta1.AuraInvite{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		before := latest.Status.DeepCopy()
		want := metav1.Condition{Type: "Ready", Status: condStatus, Reason: reason, Message: msg}
		wouldChange := latest.Status.Phase != phase || auraConditionChanged(latest.Status.Conditions, want)
		if inviteID != "" {
			latest.Status.InviteID = inviteID
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

// patch applies a mutation to the latest object with conflict retry, and
// writes only what it changed: a merge patch, never an Update (operations.md
// id 150).
func (r *AuraInviteReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraInvite)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraInvite{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return patchObject(ctx, r.Client, latest, func() { mutate(latest) })
	})
}

func (r *AuraInviteReconciler) fail(ctx context.Context, req ctrl.Request, inv *neo4jv1beta1.AuraInvite, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraInvite reconcile deferred", "reason", reason, "error", cause.Error())
	// Announced when the failure starts or its message changes, not on every
	// requeue while it stays (knowledge rule 145).
	if changed, _ := r.setStatusChanged(ctx, req, "", "Error", metav1.ConditionFalse, reason, aura.MessageWithoutRequestID(cause)); changed {
		r.Recorder.Event(inv, corev1.EventTypeWarning, EventReasonAuraInviteFailed, fmt.Sprintf("%s: %v", reason, cause))
	}
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
