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
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

const (
	auraRestorePending   = "Pending"
	auraRestoreRestoring = "Restoring"
	auraRestoreCompleted = "Completed"
	auraRestoreFailed    = "Failed"
)

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurarestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurarestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurainstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=aurasnapshots,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// AuraRestoreReconciler restores an Aura instance in place from a snapshot. It's
// a one-shot action: a Completed/Failed CR stays as history and never re-fires.
type AuraRestoreReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
}

func (r *AuraRestoreReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// Reconcile drives the restore state machine.
func (r *AuraRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	restore := &neo4jv1beta1.AuraRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal: never re-fire a completed/failed restore.
	if restore.Status.Phase == auraRestoreCompleted || restore.Status.Phase == auraRestoreFailed {
		return ctrl.Result{}, nil
	}

	apiClient, externalID, _, err := resolveInstanceClientAndID(ctx, r.Client, restore.Namespace, restore.Spec.InstanceRef)
	if err != nil {
		return r.pending(ctx, req, restore, "InstanceNotReady", err.Error())
	}

	// Resolve the snapshot ID (direct or via an AuraSnapshot CR).
	snapshotID := restore.Spec.SnapshotID
	if snapshotID == "" && restore.Spec.SnapshotRef != "" {
		snapObj := &neo4jv1beta1.AuraSnapshot{}
		if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.SnapshotRef, Namespace: restore.Namespace}, snapObj); err != nil {
			return r.pending(ctx, req, restore, "SnapshotRefUnresolved", err.Error())
		}
		snapshotID = snapObj.Status.SnapshotID
		if snapshotID == "" {
			return r.pending(ctx, req, restore, "SnapshotNotReady",
				fmt.Sprintf("AuraSnapshot %q has no snapshotId yet", restore.Spec.SnapshotRef))
		}
	}

	observed, err := apiClient.GetInstance(ctx, externalID)
	if err != nil {
		if aura.IsNotFound(err) || aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, restore, "ObserveFailed", err.Error())
	}

	switch restore.Status.Phase {
	case "", auraRestorePending:
		// If a restore is already underway (e.g. issued before a crash), advance.
		if aura.IsInstanceTransient(observed.Status) {
			return r.setPhase(ctx, req, restore, auraRestoreRestoring, snapshotID, "Restore in progress")
		}
		if !aura.IsInstanceRunning(observed.Status) {
			return r.pending(ctx, req, restore, "InstanceNotRunning",
				fmt.Sprintf("waiting for instance to be Running before restore (status: %s)", observed.Status))
		}
		if err := apiClient.RestoreSnapshot(ctx, externalID, snapshotID); err != nil {
			if aura.IsConflict(err) || aura.IsTransient(err) {
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, restore, "RestoreRejected", err.Error())
		}
		r.Recorder.Event(restore, corev1.EventTypeNormal, EventReasonAuraRestoreStarted,
			fmt.Sprintf("Restoring instance %s from snapshot %s", externalID, snapshotID))
		return r.setPhase(ctx, req, restore, auraRestoreRestoring, snapshotID, "Restore issued")

	case auraRestoreRestoring:
		switch {
		case aura.IsInstanceTransient(observed.Status):
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		case aura.IsInstanceRunning(observed.Status):
			r.Recorder.Event(restore, corev1.EventTypeNormal, EventReasonAuraRestoreCompleted,
				fmt.Sprintf("Restore of instance %s completed", externalID))
			return r.complete(ctx, req, restore)
		default:
			r.Recorder.Event(restore, corev1.EventTypeWarning, EventReasonAuraRestoreFailed,
				fmt.Sprintf("instance %s entered status %s during restore", externalID, observed.Status))
			return r.fail(ctx, req, restore, "RestoreFailed",
				fmt.Sprintf("instance status %s", observed.Status))
		}
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraRestoreReconciler) setPhase(ctx context.Context, req ctrl.Request, restore *neo4jv1beta1.AuraRestore, phase, snapshotID, msg string) (ctrl.Result, error) {
	err := r.patchStatus(ctx, req, func(o *neo4jv1beta1.AuraRestore) {
		o.Status.Phase = phase
		o.Status.SnapshotID = snapshotID
		o.Status.Message = msg
		if o.Status.StartTime == nil {
			now := metav1.Now()
			o.Status.StartTime = &now
		}
		o.Status.ObservedGeneration = o.Generation
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraRestoreReconciler) complete(ctx context.Context, req ctrl.Request, restore *neo4jv1beta1.AuraRestore) (ctrl.Result, error) {
	err := r.patchStatus(ctx, req, func(o *neo4jv1beta1.AuraRestore) {
		o.Status.Phase = auraRestoreCompleted
		o.Status.Message = "Restore completed"
		now := metav1.Now()
		o.Status.CompletionTime = &now
		o.Status.ObservedGeneration = o.Generation
		meta.SetStatusCondition(&o.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Completed", Message: "Restore completed",
		})
	})
	return ctrl.Result{}, err
}

func (r *AuraRestoreReconciler) fail(ctx context.Context, req ctrl.Request, restore *neo4jv1beta1.AuraRestore, reason, msg string) (ctrl.Result, error) {
	err := r.patchStatus(ctx, req, func(o *neo4jv1beta1.AuraRestore) {
		o.Status.Phase = auraRestoreFailed
		o.Status.Message = msg
		now := metav1.Now()
		o.Status.CompletionTime = &now
		o.Status.ObservedGeneration = o.Generation
		meta.SetStatusCondition(&o.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: msg,
		})
	})
	return ctrl.Result{}, err
}

func (r *AuraRestoreReconciler) pending(ctx context.Context, req ctrl.Request, restore *neo4jv1beta1.AuraRestore, reason, msg string) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraRestore pending", "reason", reason, "detail", msg)
	if err := r.patchStatus(ctx, req, func(o *neo4jv1beta1.AuraRestore) {
		if o.Status.Phase == "" {
			o.Status.Phase = auraRestorePending
		}
		o.Status.Message = msg
		o.Status.ObservedGeneration = o.Generation
		meta.SetStatusCondition(&o.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: msg,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

func (r *AuraRestoreReconciler) patchStatus(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraRestore)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraRestore{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.Status().Update(ctx, latest)
	})
}

// SetupWithManager wires the controller.
func (r *AuraRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraRestore{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(r)
}
