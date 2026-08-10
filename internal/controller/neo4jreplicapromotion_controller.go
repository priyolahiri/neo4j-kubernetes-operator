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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Neo4jReplicaPromotionReconciler reconciles a Neo4jReplicaPromotion resource.
//
// One-shot, like Neo4jBackup / Neo4jRestore. There is deliberately NO
// finalizer: promotion is irreversible, so there is no cleanup to perform, and
// deleting the CR must never imply undoing it.
type Neo4jReplicaPromotionReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicapromotions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicapromotions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicapromotions/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicadatabases,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jreplicadatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile performs a one-way replica promotion.
//
// Crash safety without an idempotency token comes from the ordering
// CHECK → PROMOTE → CHECK → RECORD. If the process dies between the procedure
// call and the status write, the next reconcile observes a database whose type
// is no longer "replica", concludes the promotion succeeded, and records it —
// rather than calling the procedure a second time against an already-promoted
// database.
func (r *Neo4jReplicaPromotionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jreplicapromotion", req.NamespacedName)

	promo := &neo4jv1beta1.Neo4jReplicaPromotion{}
	if err := r.Get(ctx, req.NamespacedName, promo); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Terminal phases are final. A one-shot CR must not re-run because
	// something touched it, and re-running a promotion is meaningless anyway.
	if promo.Status.Phase == neo4jv1beta1.PromotionPhaseCompleted ||
		promo.Status.Phase == neo4jv1beta1.PromotionPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Deletion needs no cleanup — nothing to undo, no finalizer held.
	if promo.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	requeue := r.requeueAfter()

	// Resolve the replica CR this promotion targets.
	replica := &neo4jv1beta1.Neo4jReplicaDatabase{}
	key := types.NamespacedName{Name: promo.Spec.ReplicaRef, Namespace: promo.Namespace}
	if err := r.Get(ctx, key, replica); err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Neo4jReplicaDatabase %q not found in namespace %q",
				promo.Spec.ReplicaRef, promo.Namespace)
			r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePending, metav1.ConditionFalse,
				EventReasonClusterNotFound, msg, nil)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		return ctrl.Result{RequeueAfter: requeue}, err
	}

	dbName := effectiveReplicaName(replica)

	target, err := ResolveClusterRef(ctx, r.Client, replica.Namespace, replica.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found || !target.IsReady() {
		msg := fmt.Sprintf("%s is not ready to accept a promotion", targetRefDisplay(replica.Spec.ClusterRef))
		r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePending, metav1.ConditionFalse,
			ConditionReasonClusterNotReady, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := target.NewClient(r.Client)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePending, metav1.ConditionFalse,
			EventReasonConnectionFailed, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()

	// CHECK — observe live state before doing anything irreversible.
	before, err := nc.GetDatabaseInfo(ctx, dbName)
	if err != nil {
		msg := fmt.Sprintf("could not read database %q before promoting: %v", dbName, err)
		r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePending, metav1.ConditionFalse,
			EventReasonPromotionFailed, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if before == nil {
		msg := fmt.Sprintf("database %q does not exist", dbName)
		r.fail(ctx, promo, msg)
		return ctrl.Result{}, nil
	}
	if before.Type != neo4jclient.DatabaseTypeReplica {
		// Already promoted — by an earlier attempt of this same CR that
		// crashed before recording, or out of band. Idempotent success, not
		// an error: the requested end state holds.
		msg := fmt.Sprintf("database %q is already type %q, not a replica; recording promotion as complete",
			dbName, before.Type)
		r.complete(ctx, promo, replica, before, msg)
		return ctrl.Result{}, nil
	}

	// Capture the lag BEFORE promoting — this is the recovery point the
	// promotion is about to make permanent, and after the call it is no
	// longer observable.
	lagTaken := before.ReplicationLag
	lastTxn := before.LastCommittedTxn

	r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePromoting, metav1.ConditionFalse,
		"Promoting", fmt.Sprintf("promoting replica %q (lag %d)", dbName, lagTaken), nil)
	r.Recorder.Eventf(promo, corev1.EventTypeWarning, EventReasonPromotionStarted,
		"Promoting replica %q. THIS IS IRREVERSIBLE: the database cannot be re-attached to its upstream, "+
			"and the %d transactions it is currently behind become permanent data loss.", dbName, lagTaken)

	// PROMOTE.
	var primaries, secondaries int32
	if t := promo.Spec.Topology; t != nil {
		primaries = t.Primaries
		secondaries = t.Secondaries
	}
	if err := nc.PromoteReplicaDatabase(ctx, dbName, primaries, secondaries); err != nil {
		// Do NOT go terminal on a call failure — re-check on the next pass.
		// The call may have succeeded and the response been lost.
		msg := fmt.Sprintf("promote call failed for %q: %v", dbName, err)
		r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePromoting, metav1.ConditionFalse,
			EventReasonPromotionFailed, msg, nil)
		r.Recorder.Event(promo, corev1.EventTypeWarning, EventReasonPromotionFailed, msg)
		return ctrl.Result{RequeueAfter: requeue}, err
	}

	// CHECK again — confirm from live state rather than trusting the call.
	after, err := nc.GetDatabaseInfo(ctx, dbName)
	if err != nil || after == nil {
		msg := fmt.Sprintf("promote issued for %q but could not confirm; will re-check", dbName)
		r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePromoting, metav1.ConditionFalse,
			"Confirming", msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if after.Type == neo4jclient.DatabaseTypeReplica {
		msg := fmt.Sprintf("promote issued for %q but it is still a replica; will re-check", dbName)
		r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhasePromoting, metav1.ConditionFalse,
			"Confirming", msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// RECORD.
	after.ReplicationLag = lagTaken
	after.LastCommittedTxn = lastTxn
	msg := fmt.Sprintf("replica %q promoted to type %q; %d transactions of lag were made permanent",
		dbName, after.Type, lagTaken)
	r.complete(ctx, promo, replica, after, msg)
	return ctrl.Result{}, nil
}

// complete records the terminal Completed phase on the promotion CR and drives
// the replica CR into its own terminal Promoted phase.
func (r *Neo4jReplicaPromotionReconciler) complete(
	ctx context.Context,
	promo *neo4jv1beta1.Neo4jReplicaPromotion,
	replica *neo4jv1beta1.Neo4jReplicaDatabase,
	info *neo4jclient.DatabaseInfo,
	msg string,
) {
	r.Recorder.Eventf(promo, corev1.EventTypeNormal, EventReasonPromotionCompleted, "%s", msg)

	update := func() error {
		latest := &neo4jv1beta1.Neo4jReplicaPromotion{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(promo), latest); err != nil {
			return err
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, metav1.ConditionTrue,
			"Promoted", msg)
		latest.Status.Phase = neo4jv1beta1.PromotionPhaseCompleted
		latest.Status.Message = msg
		latest.Status.ObservedGeneration = latest.Generation
		latest.Status.ObservedLagTxIds = info.ReplicationLag
		latest.Status.LastCommittedTxn = info.LastCommittedTxn
		latest.Status.PromotedDatabase = info.Name
		if latest.Status.CompletionTime == nil {
			now := metav1.Now()
			latest.Status.CompletionTime = &now
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to record promotion completion")
	}

	// Drive the replica CR terminal so it stops managing the database, and so
	// its finalizer stops dropping. Best-effort: the replica controller's own
	// observe step reaches the same conclusion on its next pass, so a failure
	// here delays the transition rather than losing it.
	replicaUpdate := func() error {
		latest := &neo4jv1beta1.Neo4jReplicaDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(replica), latest); err != nil {
			return err
		}
		if latest.Status.Phase == neo4jv1beta1.ReplicaPhasePromoted {
			return nil
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, metav1.ConditionTrue, "Promoted", msg)
		latest.Status.Phase = neo4jv1beta1.ReplicaPhasePromoted
		latest.Status.Message = msg
		latest.Status.DatabaseType = info.Type
		latest.Status.ReplicationLag = info.ReplicationLag
		latest.Status.LastCommittedTxn = info.LastCommittedTxn
		if latest.Status.PromotedAt == nil {
			now := metav1.Now()
			latest.Status.PromotedAt = &now
		}
		if latest.Status.PromotedBy == "" {
			latest.Status.PromotedBy = promo.Name
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, replicaUpdate); err != nil {
		log.FromContext(ctx).Error(err, "failed to mark Neo4jReplicaDatabase promoted")
	}
}

func (r *Neo4jReplicaPromotionReconciler) fail(ctx context.Context, promo *neo4jv1beta1.Neo4jReplicaPromotion, msg string) {
	r.Recorder.Event(promo, corev1.EventTypeWarning, EventReasonPromotionFailed, msg)
	r.setStatus(ctx, promo, neo4jv1beta1.PromotionPhaseFailed, metav1.ConditionFalse,
		EventReasonPromotionFailed, msg, nil)
}

func (r *Neo4jReplicaPromotionReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jReplicaPromotionReconciler) setStatus(
	ctx context.Context,
	promo *neo4jv1beta1.Neo4jReplicaPromotion,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message string,
	completionTime *metav1.Time,
) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jReplicaPromotion{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(promo), latest); err != nil {
			return err
		}
		if latest.Status.Phase == neo4jv1beta1.PromotionPhaseCompleted {
			return nil
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, readyReason, message)
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if completionTime != nil {
			latest.Status.CompletionTime = completionTime
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jReplicaPromotion status")
	}
}

// SetupWithManager registers the controller and re-enqueues a promotion when
// the replica it targets changes — so a promotion applied before its replica
// finishes seeding starts as soon as the replica is ready, rather than waiting
// out the requeue interval. During a failover that latency is the thing being
// optimised.
func (r *Neo4jReplicaPromotionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueue := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		replicaName := obj.GetName()
		ns := obj.GetNamespace()
		if replicaName == "" || ns == "" {
			return nil
		}
		var promotions neo4jv1beta1.Neo4jReplicaPromotionList
		if err := mgr.GetClient().List(ctx, &promotions, client.InNamespace(ns)); err != nil {
			return nil
		}
		var reqs []reconcile.Request
		for i := range promotions.Items {
			p := &promotions.Items[i]
			if p.Spec.ReplicaRef != replicaName {
				continue
			}
			// Terminal promotions never need re-running.
			if p.Status.Phase == neo4jv1beta1.PromotionPhaseCompleted ||
				p.Status.Phase == neo4jv1beta1.PromotionPhaseFailed {
				continue
			}
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: p.Namespace, Name: p.Name},
			})
		}
		return reqs
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jReplicaPromotion{}).
		Watches(&neo4jv1beta1.Neo4jReplicaDatabase{}, enqueue).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}
