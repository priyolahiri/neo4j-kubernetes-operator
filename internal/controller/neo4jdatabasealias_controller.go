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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jDatabaseAliasFinalizer ensures the controller can drop the alias before
// the CR disappears.
const Neo4jDatabaseAliasFinalizer = "neo4j.com/databasealias-finalizer"

// Neo4jDatabaseAliasReconciler reconciles a Neo4jDatabaseAlias resource.
type Neo4jDatabaseAliasReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	Validator               *validation.AliasValidator
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdatabasealiases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdatabasealiases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jdatabasealiases/finalizers,verbs=update
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterpriseclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jenterprisestandalones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Neo4jDatabaseAlias toward its desired state.
func (r *Neo4jDatabaseAliasReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jdatabasealias", req.NamespacedName)

	alias := &neo4jv1beta1.Neo4jDatabaseAlias{}
	if err := r.Get(ctx, req.NamespacedName, alias); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	aliasName := effectiveAliasName(alias)
	requeue := r.requeueAfter()

	if alias.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, alias, aliasName)
	}

	if !controllerutil.ContainsFinalizer(alias, Neo4jDatabaseAliasFinalizer) {
		controllerutil.AddFinalizer(alias, Neo4jDatabaseAliasFinalizer)
		if err := r.Update(ctx, alias); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if r.Validator != nil {
		res := r.Validator.Validate(ctx, alias)
		for _, w := range res.Warnings {
			r.Recorder.Eventf(alias, corev1.EventTypeWarning, EventReasonValidationWarning, "%s", w)
		}
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonValidationFailed, msg, "")
			r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	target, err := ResolveClusterRef(ctx, r.Client, alias.Namespace, alias.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve cluster ref")
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(alias.Spec.ClusterRef))
		r.setStatus(ctx, alias, "Pending", metav1.ConditionFalse, EventReasonClusterNotFound, msg, "")
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		msg := fmt.Sprintf("%s is not Ready", targetRefDisplay(alias.Spec.ClusterRef))
		r.setNamedCondition(ctx, alias, ConditionTypeClusterNotReady, metav1.ConditionTrue, ConditionReasonClusterNotReady, msg)
		r.setStatus(ctx, alias, "Pending", metav1.ConditionFalse, ConditionReasonClusterNotReady, msg, "")
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, alias, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	nc, err := target.NewClient(r.Client)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonConnectionFailed, msg, "")
		r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()

	// The target database does not have to exist yet. An alias and the replica
	// it fronts are typically applied together, and CREATE ALIAS against a
	// missing database fails — so wait rather than flapping into Failed.
	dbInfo, err := nc.GetDatabaseInfo(ctx, alias.Spec.TargetDatabase)
	if err != nil {
		return r.fail(ctx, alias, "target database lookup failed", err, requeue)
	}
	if dbInfo == nil {
		msg := fmt.Sprintf("target database %q does not exist yet", alias.Spec.TargetDatabase)
		r.setStatus(ctx, alias, "Pending", metav1.ConditionFalse, ConditionReasonClusterNotReady, msg, "")
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	existing, err := nc.ShowAlias(ctx, aliasName)
	if err != nil {
		return r.fail(ctx, alias, "alias lookup failed", err, requeue)
	}

	switch {
	case existing == nil:
		if err := nc.CreateAlias(ctx, aliasName, alias.Spec.TargetDatabase); err != nil {
			return r.fail(ctx, alias, "create alias failed", err, requeue)
		}
		r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasCreated,
			"Alias %q created for database %q", aliasName, alias.Spec.TargetDatabase)

	case existing.Database != alias.Spec.TargetDatabase:
		// Drift: someone re-pointed the alias out of band, or spec.targetDatabase
		// changed. Spec wins.
		if err := nc.AlterAliasTarget(ctx, aliasName, alias.Spec.TargetDatabase); err != nil {
			return r.fail(ctx, alias, "retarget alias failed", err, requeue)
		}
		r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasRetargeted,
			"Alias %q re-pointed from %q to %q", aliasName, existing.Database, alias.Spec.TargetDatabase)
	}

	if alias.Status.Phase != "Ready" {
		r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasReady,
			"Alias %q resolves to %q", aliasName, alias.Spec.TargetDatabase)
	}

	msg := fmt.Sprintf("alias %q resolves to %q", aliasName, alias.Spec.TargetDatabase)
	r.setStatus(ctx, alias, "Ready", metav1.ConditionTrue, "AliasReady", msg, alias.Spec.TargetDatabase)
	return ctrl.Result{RequeueAfter: requeue}, nil
}

func (r *Neo4jDatabaseAliasReconciler) handleDeletion(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, aliasName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(alias, Neo4jDatabaseAliasFinalizer) {
		return ctrl.Result{}, nil
	}

	requeue := r.requeueAfter()

	if strings.EqualFold(alias.Spec.DeletionPolicy, "Retain") {
		controllerutil.RemoveFinalizer(alias, Neo4jDatabaseAliasFinalizer)
		return ctrl.Result{}, r.Update(ctx, alias)
	}

	target, err := ResolveClusterRef(ctx, r.Client, alias.Namespace, alias.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		controllerutil.RemoveFinalizer(alias, Neo4jDatabaseAliasFinalizer)
		return ctrl.Result{}, r.Update(ctx, alias)
	}
	if !target.IsReady() {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := target.NewClient(r.Client)
	if err != nil {
		logger.Error(err, "cannot connect during alias deletion; releasing finalizer")
		controllerutil.RemoveFinalizer(alias, Neo4jDatabaseAliasFinalizer)
		return ctrl.Result{}, r.Update(ctx, alias)
	}
	defer func() { _ = nc.Close() }()

	// Dropping an alias never affects the database behind it, so a failure
	// here is low-stakes — release rather than wedge the deletion.
	if err := nc.DropAliasIfExists(ctx, aliasName); err != nil {
		r.Recorder.Eventf(alias, corev1.EventTypeWarning, EventReasonAliasFailed,
			"DROP ALIAS %q failed; releasing finalizer to avoid wedging deletion: %v", aliasName, err)
		controllerutil.RemoveFinalizer(alias, Neo4jDatabaseAliasFinalizer)
		return ctrl.Result{}, r.Update(ctx, alias)
	}

	r.Recorder.Eventf(alias, corev1.EventTypeNormal, EventReasonAliasDropped, "Alias %q dropped", aliasName)
	controllerutil.RemoveFinalizer(alias, Neo4jDatabaseAliasFinalizer)
	return ctrl.Result{}, r.Update(ctx, alias)
}

func (r *Neo4jDatabaseAliasReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jDatabaseAliasReconciler) fail(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, label string, err error, requeue time.Duration) (ctrl.Result, error) {
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	r.setStatus(ctx, alias, "Failed", metav1.ConditionFalse, EventReasonAliasFailed, msg, "")
	r.Recorder.Event(alias, corev1.EventTypeWarning, EventReasonAliasFailed, msg)
	return ctrl.Result{RequeueAfter: requeue}, err
}

func (r *Neo4jDatabaseAliasReconciler) setStatus(
	ctx context.Context,
	alias *neo4jv1beta1.Neo4jDatabaseAlias,
	phase string,
	readyStatus metav1.ConditionStatus,
	readyReason, message, observedTarget string,
) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jDatabaseAlias{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(alias), latest); err != nil {
			return err
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, readyReason, message)
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if observedTarget != "" {
			latest.Status.ObservedTarget = observedTarget
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jDatabaseAlias status")
	}
}

func (r *Neo4jDatabaseAliasReconciler) setNamedCondition(ctx context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias, condType string, status metav1.ConditionStatus, reason, message string) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jDatabaseAlias{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(alias), latest); err != nil {
			return err
		}
		SetNamedCondition(&latest.Status.Conditions, condType, latest.Generation, status, reason, message)
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jDatabaseAlias", "condition", condType)
	}
}

// effectiveAliasName returns spec.name if set, else metadata.name.
func effectiveAliasName(alias *neo4jv1beta1.Neo4jDatabaseAlias) string {
	if alias.Spec.Name != "" {
		return alias.Spec.Name
	}
	return alias.Name
}

// SetupWithManager registers the controller and re-enqueues aliases when their
// target cluster flips to Ready.
func (r *Neo4jDatabaseAliasReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueue := EnqueueDependentsForClusterChange(
		mgr.GetClient(),
		func() client.ObjectList { return &neo4jv1beta1.Neo4jDatabaseAliasList{} },
		func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
			aliases, ok := list.(*neo4jv1beta1.Neo4jDatabaseAliasList)
			if !ok {
				return
			}
			for i := range aliases.Items {
				a := &aliases.Items[i]
				emit(a.Name, a.Namespace, a.Spec.ClusterRef)
			}
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jDatabaseAlias{}).
		Watches(&neo4jv1beta1.Neo4jEnterpriseCluster{}, enqueue).
		Watches(&neo4jv1beta1.Neo4jEnterpriseStandalone{}, enqueue).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}
