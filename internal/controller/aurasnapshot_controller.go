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
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurasnapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurasnapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainstances,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// AuraSnapshotReconciler takes an on-demand snapshot of an Aura instance and
// polls it to completion. No finalizer: the Aura API cannot delete snapshots,
// so deleting the CR only removes it from cluster state (the snapshot persists).
type AuraSnapshotReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	// ClientFactory builds the Aura API client; nil uses the real shared client.
	ClientFactory auraClientFactory
}

func (r *AuraSnapshotReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile requests the snapshot (once) and polls its status.
func (r *AuraSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	snap := &neo4jv1beta1.AuraSnapshot{}
	if err := r.Get(ctx, req.NamespacedName, snap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A snapshot in a terminal phase is not observed again: Aura changes nothing
	// about it, and a resync or an operator restart used to GET it once more —
	// and, with the credentials since revoked, turn a Completed snapshot into a
	// failure retried every 30s (knowledge rule 156).
	if snapshotTerminal(snap.Status.Phase) {
		return ctrl.Result{}, nil
	}

	creds, externalID, _, err := resolveInstanceCredsAndID(ctx, r.Client, snap.Namespace, snap.Spec.InstanceRef)
	if err != nil {
		logger.Info("snapshot deferred: instance not resolvable", "error", err.Error())
		r.setSnapshotCondition(ctx, req, false, "InstanceNotReady", err.Error())
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}
	apiClient := resolveClient(r.ClientFactory, creds)

	// Request the snapshot exactly once.
	if snap.Status.SnapshotID == "" {
		created, err := apiClient.CreateSnapshot(ctx, externalID)
		if err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, snap, "CreateFailed", err)
		}
		if err := r.patchStatus(ctx, req, func(s *neo4jv1beta1.AuraSnapshot) {
			// The create response carries ONLY snapshot_id — no status, no
			// profile, no exportable (verified live 2026-08-01: a 202 whose body
			// is exactly {"snapshot_id": …}). Copying those fields wrote an empty
			// phase into the CR, so `kubectl get aurasnapshot` showed a blank
			// PHASE until the first poll. Record the ID and the honest initial
			// state; GetSnapshot fills the rest.
			s.Status.SnapshotID = created.SnapshotID
			s.Status.Phase = aura.SnapshotStatusPending
		}); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Event(snap, corev1.EventTypeNormal, EventReasonAuraSnapshotCreated,
			"Requested Aura snapshot "+created.SnapshotID)
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}

	// Poll the snapshot to completion.
	s, err := apiClient.GetSnapshot(ctx, externalID, snap.Status.SnapshotID)
	if err != nil {
		if aura.IsNotFound(err) || aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, snap, "ObserveFailed", err)
	}

	done := snapshotTerminal(s.Status)
	if err := r.patchStatus(ctx, req, func(sn *neo4jv1beta1.AuraSnapshot) {
		sn.Status.Phase = s.Status
		sn.Status.Profile = s.Profile
		sn.Status.Exportable = s.Exportable
		if s.Timestamp != "" {
			if t, perr := time.Parse(time.RFC3339, s.Timestamp); perr == nil {
				mt := metav1.NewTime(t)
				sn.Status.SnapshotTime = &mt
			}
		}
		sn.Status.ObservedGeneration = sn.Generation
		meta.SetStatusCondition(&sn.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  auraCondStatus(s.Status == aura.SnapshotStatusCompleted),
			Reason:  "Snapshot" + s.Status,
			Message: "Aura snapshot status: " + s.Status,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}

	if !done {
		return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
	}
	return ctrl.Result{}, nil
}

// snapshotTerminal reports whether an Aura snapshot status is final:
// Completed, Failed or Cancelled. Cancelled is terminal too; omitting it made a
// cancelled snapshot requeue forever. (The v1 spec's snapshot LIST status enum
// omits Cancelled while the single-GET enum includes it — handled either way.)
func snapshotTerminal(status string) bool {
	return status == aura.SnapshotStatusCompleted ||
		status == aura.SnapshotStatusFailed ||
		status == aura.SnapshotStatusCancelled
}

// fail records a failure the operator retries — phase Error and the Ready
// condition, as every Aura fail() does — and announces it with a Warning when
// that write changed the phase or the condition: once when it starts or its
// message changes, not on every 30s retry, nor after an operator restart
// (knowledge rules 145 and 156). It used to write the condition alone, so the
// phase stayed empty or Pending and `kubectl neo4j status` read a failing
// snapshot as healthy, with no event at all. The message leaves Aura's request
// ID out, so a refusal that repeats is the same status (rule 184).
func (r *AuraSnapshotReconciler) fail(ctx context.Context, req ctrl.Request, snap *neo4jv1beta1.AuraSnapshot, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraSnapshot reconcile deferred", "reason", reason, "error", cause.Error())
	announce := false
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		announce = false
		latest := &neo4jv1beta1.AuraSnapshot{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		want := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: aura.MessageWithoutRequestID(cause)}
		changed := auraConditionChanged(latest.Status.Conditions, want) || latest.Status.Phase != neo4jv1beta1.PhaseError
		meta.SetStatusCondition(&latest.Status.Conditions, want)
		latest.Status.Phase = neo4jv1beta1.PhaseError
		latest.Status.ObservedGeneration = latest.Generation
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		announce = changed
		return nil
	})
	if announce {
		r.Recorder.Event(snap, corev1.EventTypeWarning, EventReasonAuraSnapshotFailed, fmt.Sprintf("%s: %v", reason, cause))
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraSnapshotReconciler) setSnapshotCondition(ctx context.Context, req ctrl.Request, ready bool, reason, msg string) {
	_ = r.patchStatus(ctx, req, func(s *neo4jv1beta1.AuraSnapshot) {
		s.Status.ObservedGeneration = s.Generation
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: auraCondStatus(ready), Reason: reason, Message: msg,
		})
	})
}

func (r *AuraSnapshotReconciler) patchStatus(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraSnapshot)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraSnapshot{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Status().Update(ctx, latest)
	})
}

// SetupWithManager wires the controller.
func (r *AuraSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraSnapshot{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(endsWithItsNamespace(r))
}
