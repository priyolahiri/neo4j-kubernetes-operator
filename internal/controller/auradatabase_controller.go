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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// AuraDatabaseFinalizer guards the operator's chance to delete (or orphan) the
// Aura database before the CR is removed.
const AuraDatabaseFinalizer = "neo4j.com/auradatabase-finalizer"

// auraDatabaseTarget carries the resolved coordinates for an Aura database.
type auraDatabaseTarget struct {
	creds      auraCredentials
	orgID      string
	projectID  string
	instanceID string
	// multiDatabase is the referenced AuraInstance's recorded verdict on whether
	// it can host additional databases. nil means UNKNOWN (see
	// aura_multidatabase.go) — only an explicit false is grounds to refuse.
	multiDatabase *bool
}

// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neo4j.neo4j.com,resources=auradatabases/finalizers,verbs=update

// AuraDatabaseReconciler manages a database on a Neo4j Aura instance via the Aura
// API v2beta1. BETA / best-effort (see internal/aura/database_v2beta1.go).
type AuraDatabaseReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
	RequeueAfter            time.Duration
	ClientFactory           auraDatabaseClientFactory
}

func (r *AuraDatabaseReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

func (r *AuraDatabaseReconciler) dbName(d *neo4jv1beta1.AuraDatabase) string {
	if d.Spec.Name != "" {
		return d.Spec.Name
	}
	return d.Name
}

// resolveTarget derives credentials + org/project/instance IDs from the
// referenced AuraInstance.
func (r *AuraDatabaseReconciler) resolveTarget(ctx context.Context, d *neo4jv1beta1.AuraDatabase) (auraDatabaseTarget, error) {
	creds, orgID, projectID, instanceID, err := resolveAuraDBCoords(ctx, r.Client, d.Namespace, d.Spec.InstanceRef, d.Spec.OrganizationID)
	return auraDatabaseTarget{
		creds:         creds,
		orgID:         orgID,
		projectID:     projectID,
		instanceID:    instanceID,
		multiDatabase: auraInstanceMultiDatabase(ctx, r.Client, d.Namespace, d.Spec.InstanceRef),
	}, err
}

// Reconcile drives one pass of the database state machine.
func (r *AuraDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	d := &neo4jv1beta1.AuraDatabase{}
	if err := r.Get(ctx, req.NamespacedName, d); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if d.Annotations[AuraPausedAnnotation] == "true" {
		logger.Info("AuraDatabase reconciliation paused via annotation")
		return ctrl.Result{}, nil
	}

	// Deletion first: it resolves the instance and the credentials only to
	// call Aura (knowledge rules 157, 202).
	if !d.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, d)
	}

	tgt, err := r.resolveTarget(ctx, d)
	if err != nil {
		return r.fail(ctx, req, d, "TargetUnresolved", err)
	}
	apiClient := resolveDatabaseClient(r.ClientFactory, tgt.creds)

	if !controllerutil.ContainsFinalizer(d, AuraDatabaseFinalizer) {
		// A tested JSON patch, no resourceVersion lock: a copy older than the
		// object is no Conflict (knowledge rule 199).
		if err := addFinalizer(ctx, r.Client, d, AuraDatabaseFinalizer); err != nil {
			return ctrl.Result{}, err
		}
	}

	externalID := d.Annotations[AuraExternalDatabaseAnnotation]
	if externalID == "" {
		externalID = d.Status.DatabaseID
	}
	if externalID == "" {
		// A refusal already on status for this spec and this Aura instance is not
		// asked again: it would only repeat the list and, when the instance's
		// verdict is unknown, the create Aura refuses (knowledge rule 156). The
		// message names the instance's ID, so a replaced instance is looked at
		// afresh.
		if auraRefusalRecorded(d.Status.Conditions, d.Status.ObservedGeneration, d.Generation,
			auraReasonInstanceNotMultiDatabase, notMultiDatabaseRefusal(d.Spec.InstanceRef, tgt.instanceID).Error()) {
			return ctrl.Result{}, nil
		}
		allowCreate := managementAllows(d.Spec.ManagementPolicies, auraPolicyCreate)
		id, adopted, err := r.observeOrCreate(ctx, req, d, apiClient, tgt, allowCreate)
		if err != nil {
			// A deliberate refusal (see auraRefusalError) describes a permanent
			// property of the target, so name it as such and stop retrying.
			if isAuraRefusal(err) {
				return r.fail(ctx, req, d, auraReasonInstanceNotMultiDatabase, err)
			}
			// A 429, a 5xx or a transport failure of an Aura call is retried as it
			// is, not recorded as a failure (knowledge rule 88).
			if isRetryableAuraCall(err) {
				logger.Info("Aura database create deferred by a transient API failure; retrying", "error", err.Error())
				return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
			}
			return r.fail(ctx, req, d, "CreateFailed", err)
		}
		if id == "" {
			_ = r.setCondition(ctx, req, "Synced", metav1.ConditionFalse, "AwaitingDatabase",
				"no matching database found and managementPolicies does not permit Create")
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		externalID = id
		if adopted {
			r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseAdopted,
				fmt.Sprintf("Adopted existing database %s", externalID))
		}
	}

	observed, err := apiClient.GetDatabase(ctx, tgt.orgID, tgt.projectID, tgt.instanceID, externalID)
	if err != nil {
		if aura.IsNotFound(err) || aura.IsTransient(err) {
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		}
		return r.fail(ctx, req, d, "ObserveFailed", err)
	}

	changed, err := r.syncStatus(ctx, req, observed)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Announced when the database becomes Ready, not on every re-observe while
	// it stays so (knowledge rule 136).
	if changed {
		r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseReady,
			fmt.Sprintf("Database %s reconciled", observed.ID))
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// auraReasonInstanceNotMultiDatabase is the Ready reason of the terminal
// refusal to create a database on an instance that is not multi-database, and
// what the refusal guard looks for (knowledge rule 91: guard and writer share
// the name).
const auraReasonInstanceNotMultiDatabase = "InstanceNotMultiDatabase"

// observeOrCreate creates the database unless the external-ID annotation already
// pins one.
//
// NOTE: adoption-by-name is impossible on this API. The v2beta1 DatabaseSummary
// carries ONLY an `id` — no name, no status — so a pre-existing Aura database
// cannot be matched against spec.name. The external-ID annotation set by
// setExternalID is therefore the ONLY adoption mechanism; if it is lost, this
// will create a second database rather than re-adopt. We still list first, so we
// can warn when creating into a non-empty instance instead of doing it silently.
func (r *AuraDatabaseReconciler) observeOrCreate(
	ctx context.Context, req ctrl.Request, d *neo4jv1beta1.AuraDatabase,
	apiClient auraDatabaseAPI, tgt auraDatabaseTarget, allowCreate bool,
) (id string, adopted bool, err error) {
	name := r.dbName(d)
	existing, err := apiClient.ListDatabases(ctx, tgt.orgID, tgt.projectID, tgt.instanceID)
	if err != nil {
		return "", false, retryableAuraCall(fmt.Errorf("listing databases before create: %w", err))
	}
	if !allowCreate {
		return "", false, nil
	}
	// Refuse before spending a call we know will be rejected. Only an explicit
	// false counts: an unknown verdict must still attempt the create, because
	// the verdict is unknowable for many instances.
	if tgt.multiDatabase != nil && !*tgt.multiDatabase {
		return "", false, notMultiDatabaseRefusal(d.Spec.InstanceRef, tgt.instanceID)
	}
	if len(existing) > 0 {
		r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseCreated,
			fmt.Sprintf("Creating database %q on an instance that already has %d database(s): the Aura API does not "+
				"return database names, so an existing database cannot be adopted by name. Verify no duplicate was created.",
				name, len(existing)))
	}
	created, err := apiClient.CreateDatabase(ctx, tgt.orgID, tgt.projectID, tgt.instanceID, aura.CreateDatabaseRequest{Name: name})
	if err != nil {
		// The API's own refusal, for the instances whose verdict we could not
		// learn in advance. Translate it: on its own it is a bare 409 that reads
		// as "try again later", and retrying it can never succeed.
		if aura.IsMultiDatabaseOnly(err) {
			return "", false, notMultiDatabaseRefusal(d.Spec.InstanceRef, tgt.instanceID)
		}
		return "", false, retryableAuraCall(err)
	}
	if err := r.setExternalID(ctx, req, created.ID); err != nil {
		return "", false, err
	}
	r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseCreated,
		fmt.Sprintf("Created database %s", created.ID))
	return created.ID, false, nil
}

// handleDeletion honours the deletion policy: Delete (the default) deletes the
// database, Orphan leaves it. Only Delete calls Aura, so only it resolves the
// AuraInstance the database is on and the credentials, which come through it.
//
// It used to release the finalizer whenever they could not be resolved, with a
// Normal "Orphaning database" event: a database, with its data, left in Aura
// and announced as routine. Now (knowledge rule 202):
//   - the AuraInstance CR is gone: nothing can reach Aura for the database any
//     more, and nothing will (its credentials came through it), so the
//     finalizer is released at once — with a Warning, since the database is
//     still there unless the instance was deleted in Aura too;
//   - anything else (the credentials Secret, the provider config, the
//     organization or project, an instance not yet created): the deletion
//     waits finalizerDeletionGracePeriod for it, then releases with a Warning
//     naming what is left (rule 157);
//   - a DELETE Aura refuses: the same bounded wait (rule 201).
func (r *AuraDatabaseReconciler) handleDeletion(ctx context.Context, req ctrl.Request, d *neo4jv1beta1.AuraDatabase) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(d, AuraDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}
	externalID := d.Annotations[AuraExternalDatabaseAnnotation]
	if externalID == "" {
		externalID = d.Status.DatabaseID
	}
	deleteCloud := d.Spec.DeletionPolicy != "Orphan" && managementAllows(d.Spec.ManagementPolicies, auraPolicyDelete)
	remains := fmt.Sprintf("it stays on Aura instance %q with its data", d.Spec.InstanceRef)
	switch {
	case externalID == "":
	case !deleteCloud:
		r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseOrphaned,
			fmt.Sprintf("Orphaning database %s; it stays in place in Aura", externalID))
	case r.instanceGone(ctx, d):
		r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseOrphaned, fmt.Sprintf(
			"Orphaning database %s although deletionPolicy is Delete: its AuraInstance %q no longer exists, so the operator "+
				"cannot reach Aura to delete it. It went with the instance if that was deleted in Aura; otherwise it is still "+
				"there with its data, so check the Aura console", externalID, d.Spec.InstanceRef))
	default:
		tgt, err := r.resolveTarget(ctx, d)
		if err != nil {
			if !deletionGracePeriodExceeded(d) {
				return r.fail(ctx, req, d, "CredentialsUnavailable",
					auraDeletionNeedsCredentials(d, "Aura database", externalID, err))
			}
			r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseOrphaned,
				auraOrphanedWithoutCredentials("Aura database", externalID, remains, err))
			break
		}
		err = resolveDatabaseClient(r.ClientFactory, tgt.creds).DeleteDatabase(ctx, tgt.orgID, tgt.projectID, tgt.instanceID, externalID)
		switch {
		case err == nil || aura.IsNotFound(err):
			r.Recorder.Event(d, corev1.EventTypeNormal, EventReasonAuraDatabaseDeleted,
				fmt.Sprintf("Deleted database %s", externalID))
		case !auraDeleteRefused(err):
			return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
		case !deletionGracePeriodExceeded(d):
			return r.fail(ctx, req, d, auraReasonDeletionRefused,
				auraDeletionRefusedWait(d, "Aura database", externalID, err))
		default:
			r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseOrphaned,
				auraOrphanedAfterRefusal("Aura database", externalID, remains, err))
		}
	}
	// One removal a stale cache or a status write just before cannot turn
	// into a Conflict, and that an object already gone has done already
	// (knowledge rule 189).
	return ctrl.Result{}, removeFinalizerAt(ctx, r.Client, req.NamespacedName, &neo4jv1beta1.AuraDatabase{}, AuraDatabaseFinalizer)
}

// instanceGone reports whether the AuraInstance the database is on no longer
// exists as a CR. Another read error is not "gone": the deletion waits on it.
func (r *AuraDatabaseReconciler) instanceGone(ctx context.Context, d *neo4jv1beta1.AuraDatabase) bool {
	err := r.Get(ctx, types.NamespacedName{Name: d.Spec.InstanceRef, Namespace: d.Namespace}, &neo4jv1beta1.AuraInstance{})
	return apierrors.IsNotFound(err)
}

func (r *AuraDatabaseReconciler) setExternalID(ctx context.Context, req ctrl.Request, id string) error {
	return r.patch(ctx, req, func(o *neo4jv1beta1.AuraDatabase) {
		if o.Annotations == nil {
			o.Annotations = map[string]string{}
		}
		o.Annotations[AuraExternalDatabaseAnnotation] = id
	})
}

// patch applies a mutation to the latest object with conflict retry, and
// writes only what it changed: a merge patch, never an Update (operations.md
// id 150).
func (r *AuraDatabaseReconciler) patch(ctx context.Context, req ctrl.Request, mutate func(*neo4jv1beta1.AuraDatabase)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabase{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return patchObject(ctx, r.Client, latest, func() { mutate(latest) })
	})
}

func (r *AuraDatabaseReconciler) setCondition(ctx context.Context, req ctrl.Request, condType string, status metav1.ConditionStatus, reason, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.AuraDatabase{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: condType, Status: status, Reason: reason, Message: msg,
		})
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	})
}

// syncStatus records the database as Ready, only when that changes the status
// or the stored lastSyncedTime is due a refresh (auraSyncedTimeRefresh). It
// reports whether the write changed the phase or the Ready condition, from the
// attempt whose write succeeded, so the caller announces Ready once (knowledge
// rule 136).
func (r *AuraDatabaseReconciler) syncStatus(ctx context.Context, req ctrl.Request, observed *aura.Database) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		latest := &neo4jv1beta1.AuraDatabase{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		before := latest.Status.DeepCopy()
		readyCond := metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "Reconciled",
			Message: "Database reconciled against the Aura API (v2beta1, beta)",
		}
		wouldChange := latest.Status.Phase != neo4jv1beta1.PhaseReady || auraConditionChanged(latest.Status.Conditions, readyCond)
		latest.Status.DatabaseID = observed.ID
		latest.Status.Phase = neo4jv1beta1.PhaseReady
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, readyCond)
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

func (r *AuraDatabaseReconciler) fail(ctx context.Context, req ctrl.Request, d *neo4jv1beta1.AuraDatabase, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("AuraDatabase reconcile deferred", "reason", reason, "error", cause.Error())
	announce := false
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		announce = false
		latest := &neo4jv1beta1.AuraDatabase{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		want := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: aura.MessageWithoutRequestID(cause)}
		changed := auraConditionChanged(latest.Status.Conditions, want) || latest.Status.Phase != "Error"
		meta.SetStatusCondition(&latest.Status.Conditions, want)
		latest.Status.Phase = "Error"
		latest.Status.ObservedGeneration = latest.Generation
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		announce = changed
		return nil
	})
	// Announced when the failure starts or its message changes, not on every
	// requeue while it stays (knowledge rule 145).
	if announce {
		r.Recorder.Event(d, corev1.EventTypeWarning, EventReasonAuraDatabaseFailed, fmt.Sprintf("%s: %v", reason, cause))
	}
	// A refusal is terminal: the condition it reports cannot change on its own,
	// so requeueing would only rewrite the same status every 30s and bury the
	// explanation under repeated events. The spec change that fixes it triggers a
	// fresh reconcile by itself.
	if isAuraRefusal(cause) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter()}, nil
}

// SetupWithManager wires the controller.
func (r *AuraDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mcr := r.MaxConcurrentReconciles
	if mcr <= 0 {
		mcr = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.AuraDatabase{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mcr}).
		Complete(endsWithItsNamespace(r))
}
