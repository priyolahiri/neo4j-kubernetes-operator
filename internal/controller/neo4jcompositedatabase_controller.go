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
	"sort"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Neo4jCompositeDatabaseFinalizer guards the composite so its Cypher-side
// teardown runs before the CR disappears.
const Neo4jCompositeDatabaseFinalizer = "neo4j.com/compositedatabase-finalizer"

// Neo4jCompositeDatabaseReconciler reconciles a Neo4jCompositeDatabase.
type Neo4jCompositeDatabaseReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	Validator    *validation.CompositeDatabaseValidator
	RequeueAfter time.Duration
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jcompositedatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jcompositedatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jcompositedatabases/finalizers,verbs=update

// Reconcile drives a composite database and its constituents to spec.
//
// The ordering below is the whole point of owning both in one controller.
// Neo4j accepts `CREATE ALIAS <composite>.<name>` when no composite of that
// name exists — it becomes an ordinary alias that merely has a dot in it — and
// the composite can then NEVER be created:
//
//	42N87: The database or alias name `x` conflicts with the name `x.y`
//	       of an existing database or alias.
//
// So the composite is created first, always, and constituents only afterwards.
func (r *Neo4jCompositeDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("neo4jcompositedatabase", req.NamespacedName)

	cd := &neo4jv1beta1.Neo4jCompositeDatabase{}
	if err := r.Get(ctx, req.NamespacedName, cd); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	name := compositeName(cd)
	requeue := r.requeueAfter()

	if cd.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, cd, name)
	}

	if !controllerutil.ContainsFinalizer(cd, Neo4jCompositeDatabaseFinalizer) {
		controllerutil.AddFinalizer(cd, Neo4jCompositeDatabaseFinalizer)
		if err := r.Update(ctx, cd); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if r.Validator != nil {
		res := r.Validator.Validate(ctx, cd)
		for _, w := range res.Warnings {
			r.Recorder.Eventf(cd, corev1.EventTypeWarning, EventReasonValidationWarning, "%s", w)
		}
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
				EventReasonValidationFailed, msg, nil)
			r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	target, err := ResolveClusterRef(ctx, r.Client, cd.Namespace, cd.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve cluster ref")
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(cd.Spec.ClusterRef))
		r.setStatus(ctx, cd, neo4jv1beta1.PhasePending, metav1.ConditionFalse,
			EventReasonClusterNotFound, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		msg := fmt.Sprintf("%s is not Ready", targetRefDisplay(cd.Spec.ClusterRef))
		r.setNamedCondition(ctx, cd, ConditionTypeClusterNotReady, metav1.ConditionTrue,
			ConditionReasonClusterNotReady, msg)
		r.setStatus(ctx, cd, neo4jv1beta1.PhasePending, metav1.ConditionFalse,
			ConditionReasonClusterNotReady, msg, nil)
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	r.setNamedCondition(ctx, cd, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	nc, err := target.NewClient(r.Client)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
			EventReasonConnectionFailed, msg, nil)
		r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		return ctrl.Result{RequeueAfter: requeue}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()

	// STEP 1 — the composite itself, before any constituent.
	info, err := nc.ShowCompositeDatabase(ctx, name)
	if err != nil {
		// Includes the "exists but is not a composite" refusal, which is a
		// spec-vs-reality conflict no reconcile should paper over.
		return r.fail(ctx, cd, "composite lookup failed", err, requeue)
	}
	if info == nil {
		if blocker := r.namespaceBlocker(ctx, nc, name); blocker != "" {
			msg := fmt.Sprintf(
				"cannot create composite %q: the alias %q already occupies its namespace. "+
					"Neo4j accepts an alias named `<composite>.<constituent>` even when no such "+
					"composite exists, and that alias then blocks the composite permanently. "+
					"Drop %q, then this will converge", name, blocker, blocker)
			r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
				EventReasonCompositeNameBlocked, msg, nil)
			r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonCompositeNameBlocked, msg)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		r.setStatus(ctx, cd, neo4jv1beta1.PhaseCreating, metav1.ConditionFalse,
			"Creating", fmt.Sprintf("creating composite database %q", name), nil)
		if err := nc.CreateCompositeDatabase(ctx, name, compositeWait(cd), true,
			cd.Spec.DefaultCypherLanguage); err != nil {
			return r.fail(ctx, cd, "create composite database failed", err, requeue)
		}
		r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeCreated,
			"Composite database %q created", name)
	} else if cd.Spec.DefaultCypherLanguage != "" {
		// The only alterable property. Applied unconditionally because the
		// server does not report it on SHOW DATABASE, so there is nothing to
		// diff against; the statement is idempotent.
		if err := nc.AlterCompositeDatabaseLanguage(ctx, name, cd.Spec.DefaultCypherLanguage); err != nil {
			return r.fail(ctx, cd, "set default Cypher language failed", err, requeue)
		}
	}

	// STEP 2 — constituents, now that the namespace exists.
	observed, err := r.reconcileConstituents(ctx, nc, cd, name, requeue)
	if err != nil {
		return r.fail(ctx, cd, "reconcile constituents failed", err, requeue)
	}

	if cd.Status.Phase != neo4jv1beta1.PhaseReady {
		r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeReady,
			"Composite database %q is ready with %d constituent(s)", name, len(observed))
	}
	msg := fmt.Sprintf("composite database %q exposes %d constituent(s)", name, len(observed))
	r.setStatus(ctx, cd, neo4jv1beta1.PhaseReady, metav1.ConditionTrue,
		EventReasonCompositeReady, msg, observed)
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// reconcileConstituents adds, re-points and (when enforcing) removes
// constituent aliases, returning the fully-qualified list the server reports
// afterwards.
func (r *Neo4jCompositeDatabaseReconciler) reconcileConstituents(
	ctx context.Context, nc *neo4jclient.Client,
	cd *neo4jv1beta1.Neo4jCompositeDatabase, composite string, _ time.Duration,
) ([]string, error) {
	live, err := nc.ShowAliases(ctx)
	if err != nil {
		return nil, fmt.Errorf("list aliases: %w", err)
	}
	// Only aliases inside this composite's namespace are ours to manage.
	existing := map[string]string{} // short name → current target
	prefix := composite + "."
	for _, a := range live {
		if strings.HasPrefix(a.Name, prefix) {
			existing[strings.TrimPrefix(a.Name, prefix)] = a.Database
		}
	}

	desired := map[string]string{}
	for _, c := range cd.Spec.Constituents {
		desired[c.Name] = c.TargetDatabase
	}

	for _, c := range cd.Spec.Constituents {
		current, present := existing[c.Name]
		switch {
		case !present:
			// The target need not exist yet — but CREATE ALIAS against a
			// missing database fails, so skip rather than fail the whole
			// composite. The next reconcile picks it up.
			dbInfo, err := nc.GetDatabaseInfo(ctx, c.TargetDatabase)
			if err != nil {
				return nil, fmt.Errorf("look up constituent target %q: %w", c.TargetDatabase, err)
			}
			if dbInfo == nil {
				continue
			}
			if err := nc.CreateCompositeConstituent(ctx, composite, c.Name, c.TargetDatabase); err != nil {
				return nil, err
			}
			r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentAdded,
				"Constituent %q now resolves to %q",
				neo4jclient.QualifyConstituent(composite, c.Name), c.TargetDatabase)

		case current != c.TargetDatabase:
			if err := nc.AlterCompositeConstituent(ctx, composite, c.Name, c.TargetDatabase); err != nil {
				return nil, err
			}
			r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentMoved,
				"Constituent %q re-pointed from %q to %q",
				neo4jclient.QualifyConstituent(composite, c.Name), current, c.TargetDatabase)
		}
	}

	if compositeEnforce(cd) {
		for name := range existing {
			if _, wanted := desired[name]; wanted {
				continue
			}
			if err := nc.DropCompositeConstituent(ctx, composite, name); err != nil {
				return nil, err
			}
			r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentRemoved,
				"Constituent %q removed — not in spec.constituents",
				neo4jclient.QualifyConstituent(composite, name))
		}
	}

	// Read back what the DBMS actually exposes rather than echoing spec: with
	// enforceConstituents false the two legitimately differ, and the status
	// should say what is true.
	info, err := nc.ShowCompositeDatabase(ctx, composite)
	if err != nil || info == nil {
		return nil, err
	}
	out := append([]string(nil), info.Constituents...)
	sort.Strings(out)
	return out, nil
}

// namespaceBlocker reports an existing alias sitting in the composite's
// namespace when the composite itself does not exist — the state that makes
// creation impossible.
func (r *Neo4jCompositeDatabaseReconciler) namespaceBlocker(
	ctx context.Context, nc *neo4jclient.Client, composite string,
) string {
	aliases, err := nc.ShowAliases(ctx)
	if err != nil {
		// Not being able to look is not evidence of a blocker; let the create
		// attempt speak for itself.
		return ""
	}
	prefix := composite + "."
	for _, a := range aliases {
		if strings.HasPrefix(a.Name, prefix) {
			return a.Name
		}
	}
	return ""
}

// handleDeletion drops the composite when the policy says to, then releases
// the finalizer.
func (r *Neo4jCompositeDatabaseReconciler) handleDeletion(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase, name string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(cd, Neo4jCompositeDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}

	if strings.EqualFold(cd.Spec.DeletionPolicy, "Retain") {
		logger.Info("deletionPolicy Retain — leaving the composite in place", "composite", name)
	} else {
		target, err := ResolveClusterRef(ctx, r.Client, cd.Namespace, cd.Spec.ClusterRef)
		switch {
		case err != nil:
			logger.Error(err, "resolve cluster ref during deletion; releasing finalizer")
		case !target.Found || !target.IsReady():
			// The deployment is gone or down. Blocking here would wedge the CR
			// in Terminating forever for a database that may no longer exist.
			logger.Info("deployment not available during deletion; releasing finalizer",
				"clusterRef", cd.Spec.ClusterRef)
		default:
			nc, err := target.NewClient(r.Client)
			if err != nil {
				logger.Error(err, "connect during deletion; releasing finalizer")
				break
			}
			defer func() { _ = nc.Close() }()
			// CASCADE is required, not optional: a plain drop is REFUSED while
			// constituents exist. It removes the constituent ALIASES only —
			// their target databases are untouched.
			if err := nc.DropCompositeDatabase(ctx, name, true); err != nil {
				return r.fail(ctx, cd, "drop composite database failed", err, r.requeueAfter())
			}
			r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeDropped,
				"Composite database %q dropped (constituent aliases removed; their target databases kept)", name)
		}
	}

	controllerutil.RemoveFinalizer(cd, Neo4jCompositeDatabaseFinalizer)
	if err := r.Update(ctx, cd); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Neo4jCompositeDatabaseReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *Neo4jCompositeDatabaseReconciler) fail(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	label string, err error, requeue time.Duration,
) (ctrl.Result, error) {
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
		EventReasonCompositeFailed, msg, nil)
	r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonCompositeFailed, msg)
	return ctrl.Result{RequeueAfter: requeue}, err
}

func (r *Neo4jCompositeDatabaseReconciler) setStatus(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	phase string, readyStatus metav1.ConditionStatus, reason, message string, constituents []string,
) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jCompositeDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cd), latest); err != nil {
			return err
		}
		SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, reason, message)
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if constituents != nil {
			latest.Status.ObservedConstituents = constituents
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jCompositeDatabase status")
	}
}

func (r *Neo4jCompositeDatabaseReconciler) setNamedCondition(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	condType string, status metav1.ConditionStatus, reason, message string,
) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jCompositeDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cd), latest); err != nil {
			return err
		}
		SetNamedCondition(&latest.Status.Conditions, condType, latest.Generation, status, reason, message)
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to set condition on Neo4jCompositeDatabase",
			"condition", condType)
	}
}

// compositeName returns spec.name if set, else metadata.name.
func compositeName(cd *neo4jv1beta1.Neo4jCompositeDatabase) string {
	if cd.Spec.Name != "" {
		return cd.Spec.Name
	}
	return cd.Name
}

// compositeWait defaults to true.
func compositeWait(cd *neo4jv1beta1.Neo4jCompositeDatabase) bool {
	if cd.Spec.Wait == nil {
		return true
	}
	return *cd.Spec.Wait
}

// compositeEnforce defaults to true, matching Neo4jRole.enforcePrivileges.
func compositeEnforce(cd *neo4jv1beta1.Neo4jCompositeDatabase) bool {
	if cd.Spec.EnforceConstituents == nil {
		return true
	}
	return *cd.Spec.EnforceConstituents
}

// SetupWithManager wires the controller, including a watch on the referenced
// deployment so a composite applied before its cluster converges when the
// cluster goes Ready.
func (r *Neo4jCompositeDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jCompositeDatabase{}).
		Watches(
			&neo4jv1beta1.Neo4jEnterpriseCluster{},
			EnqueueDependentsForClusterChange(
				mgr.GetClient(),
				func() client.ObjectList { return &neo4jv1beta1.Neo4jCompositeDatabaseList{} },
				func(list client.ObjectList, emit func(name, namespace, clusterRef string)) {
					l, ok := list.(*neo4jv1beta1.Neo4jCompositeDatabaseList)
					if !ok {
						return
					}
					for i := range l.Items {
						emit(l.Items[i].Name, l.Items[i].Namespace, l.Items[i].Spec.ClusterRef)
					}
				},
			),
		).
		Named("neo4jcompositedatabase").
		Complete(r)
}
