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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings

	// newDeletionClient connects the deletion to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newDeletionClient func(ResolvedTarget) (compositeDeletionClient, error)

	// newNeo4jClient connects Reconcile to the resolved target; nil uses
	// ResolvedTarget.NewClient. Tests script the server here.
	newNeo4jClient func(ResolvedTarget) (compositeNeo4jClient, error)
}

// compositeNeo4jClient is what the Neo4jCompositeDatabase reconcile asks of
// Neo4j. *neo4jclient.Client is one.
type compositeNeo4jClient interface {
	ShowCompositeDatabase(ctx context.Context, name string) (*neo4jclient.CompositeInfo, error)
	ShowAliases(ctx context.Context) ([]neo4jclient.AliasInfo, error)
	GetDatabaseInfo(ctx context.Context, databaseName string) (*neo4jclient.DatabaseInfo, error)
	CreateCompositeDatabase(ctx context.Context, name string, wait bool, ifNotExists bool, cypherVersion string) error
	AlterCompositeDatabaseLanguage(ctx context.Context, name, cypherVersion string) error
	CreateCompositeConstituent(ctx context.Context, composite, constituent, targetDatabase string) error
	AlterCompositeConstituent(ctx context.Context, composite, constituent, targetDatabase string) error
	DropCompositeConstituent(ctx context.Context, composite, constituent string) error
	CreateRemoteConstituent(ctx context.Context, composite, constituent, targetDatabase, url string, auth neo4jclient.RemoteConstituentAuth) error
	AlterRemoteConstituent(ctx context.Context, composite, constituent, targetDatabase, url string, auth neo4jclient.RemoteConstituentAuth) error
	Close() error
}

// connect returns the Neo4j client Reconcile uses for the resolved target.
func (r *Neo4jCompositeDatabaseReconciler) connect(target ResolvedTarget) (compositeNeo4jClient, error) {
	if r.newNeo4jClient != nil {
		return r.newNeo4jClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

// compositeDeletionClient is what the Neo4jCompositeDatabase deletion asks of
// Neo4j. *neo4jclient.Client is one.
type compositeDeletionClient interface {
	DropCompositeDatabase(ctx context.Context, name string, cascadeAliases bool) error
	Close() error
}

func (r *Neo4jCompositeDatabaseReconciler) connectForDeletion(target ResolvedTarget) (compositeDeletionClient, error) {
	if r.newDeletionClient != nil {
		return r.newDeletionClient(target)
	}
	nc, err := target.NewClient(r.Client)
	if err != nil {
		return nil, err
	}
	return nc, nil
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
		if err := addFinalizer(ctx, r.Client, cd, Neo4jCompositeDatabaseFinalizer); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if r.Validator != nil {
		res := r.Validator.Validate(ctx, cd)
		r.advisoryWarnings.record(r.Recorder, cd, EventReasonValidationWarning, res.Warnings)
		if len(res.Errors) > 0 {
			msg := fmt.Sprintf("validation failed: %s", res.Errors.ToAggregate().Error())
			// Announced when the failure starts or its message changes, not on
			// every pass while the spec stays invalid (knowledge rule 136).
			if r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
				EventReasonValidationFailed, msg, nil) {
				r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	target, err := ResolveClusterRef(ctx, r.Client, cd.Namespace, cd.Spec.ClusterRef)
	if err != nil {
		logger.Error(err, "failed to resolve cluster ref")
		return ctrl.Result{}, err
	}
	if !target.Found {
		msg := fmt.Sprintf("%s not found", targetRefDisplay(cd.Spec.ClusterRef))
		// A wait, announced once: when it starts, not on every pass until the
		// deployment exists (operations.md ids 145 and 182).
		if r.setStatus(ctx, cd, neo4jv1beta1.PhasePending, metav1.ConditionFalse,
			EventReasonClusterNotFound, msg, nil) {
			r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonClusterNotFound, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if !target.IsReady() {
		// A composite Ready at its generation stays Ready while its target
		// restarts (knowledge rule 183).
		r.waitForTarget(ctx, cd, ConditionReasonClusterNotReady,
			fmt.Sprintf("%s is not Ready", targetRefDisplay(cd.Spec.ClusterRef)))
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	nc, err := r.connect(target)
	if err != nil {
		if targetUnreachable(err) {
			return r.unreachable(ctx, cd, err, requeue)
		}
		msg := fmt.Sprintf("failed to connect to Neo4j: %v", err)
		// Retried with backoff; the same failure is announced once (knowledge
		// rule 145).
		if r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
			EventReasonConnectionFailed, msg, nil) {
			r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{}, err
	}
	defer func() {
		if err := nc.Close(); err != nil {
			logger.Error(err, "failed to close Neo4j client")
		}
	}()
	// Only a target that answered serves this composite: written after the
	// connection, so an unreachable one keeps the time its outage began.
	r.setNamedCondition(ctx, cd, ConditionTypeClusterNotReady, metav1.ConditionFalse, "ClusterReady", "")

	// The DEFAULT LANGUAGE clause exists only on CalVer. On the 5.26 LTS a
	// requested "5" is left out (every database there runs Cypher 5 already).
	language := compositeCypherLanguage(cd, target)
	if cd.Spec.DefaultCypherLanguage != "" && language == "" {
		logger.V(1).Info("Omitting DEFAULT LANGUAGE CYPHER: the target server (5.26 LTS) has no such clause and runs Cypher 5",
			"composite", name, "defaultCypherLanguage", cd.Spec.DefaultCypherLanguage)
	}

	// STEP 1 — the composite itself, before any constituent.
	info, err := nc.ShowCompositeDatabase(ctx, name)
	if err != nil {
		// Includes the "exists but is not a composite" refusal, which is a
		// spec-vs-reality conflict no reconcile should paper over.
		return r.failPass(ctx, cd, "composite lookup failed", err, requeue)
	}
	if info == nil {
		if blocker := r.namespaceBlocker(ctx, nc, name); blocker != "" {
			r.reportNameBlocked(ctx, cd, name, blocker)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		r.setStatus(ctx, cd, neo4jv1beta1.PhaseCreating, metav1.ConditionFalse,
			"Creating", fmt.Sprintf("creating composite database %q", name), nil)
		if err := nc.CreateCompositeDatabase(ctx, name, compositeWait(cd), true, language); err != nil {
			return r.failPass(ctx, cd, "create composite database failed", err, requeue)
		}
		r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeCreated,
			"Composite database %q created", name)
	} else if language != "" {
		// The only alterable property. Applied unconditionally because the
		// server does not report it on SHOW DATABASE, so there is nothing to
		// diff against; the statement is idempotent.
		if err := nc.AlterCompositeDatabaseLanguage(ctx, name, language); err != nil {
			return r.failPass(ctx, cd, "set default Cypher language failed", err, requeue)
		}
	}

	// STEP 2 — constituents, now that the namespace exists.
	observed, lookalikes, err := r.reconcileConstituents(ctx, nc, cd, name, requeue)
	if err != nil {
		return r.failPass(ctx, cd, "reconcile constituents failed", err, requeue)
	}

	msg := fmt.Sprintf("composite database %q exposes %d constituent(s)", name, len(observed))
	// An ordinary alias named like a constituent is NOT one and is not ours to
	// remove, but it is indistinguishable from a constituent in any listing
	// that shows only names — so say so here rather than leaving the user to
	// wonder why enforceConstituents appears to ignore it. In status rather
	// than as an event: this is a standing condition of the DBMS, and an event
	// per reconcile would be noise forever.
	if len(lookalikes) > 0 {
		msg += fmt.Sprintf("; note %d alias(es) named like constituents but not part of "+
			"this composite and so not managed by it: %s",
			len(lookalikes), strings.Join(lookalikes, ", "))
	}
	// Announced from the write that moves the composite into Ready, never from
	// this pass's copy, which may predate the last write (knowledge rules 145
	// and 183).
	if _, entered := r.writeStatus(ctx, cd, neo4jv1beta1.PhaseReady, metav1.ConditionTrue,
		EventReasonCompositeReady, msg, observed); entered {
		r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeReady,
			"Composite database %q is ready with %d constituent(s)", name, len(observed))
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// reconcileConstituents adds, re-points and (when enforcing) removes
// constituent aliases, returning the fully-qualified list the server reports
// afterwards, plus any ordinary alias whose name merely looks like one of
// ours.
func (r *Neo4jCompositeDatabaseReconciler) reconcileConstituents(
	ctx context.Context, nc compositeNeo4jClient,
	cd *neo4jv1beta1.Neo4jCompositeDatabase, composite string, _ time.Duration,
) (constituents []string, lookalikes []string, err error) {
	live, err := nc.ShowAliases(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list aliases: %w", err)
	}
	existing, lookalikes := classifyAliases(live, composite)

	for _, c := range cd.Spec.Constituents {
		// existing is keyed lower-cased: Neo4j lists the constituent spec
		// calls `Latest` as `latest`.
		current, present := existing[strings.ToLower(c.Name)]

		if c.Remote != nil {
			auth, err := r.remoteAuth(ctx, cd.Namespace, c.Remote)
			if err != nil {
				return nil, nil, err
			}
			switch {
			case !present:
				// No existence check on the target: it lives in another DBMS
				// the operator cannot see. If it is wrong, the remote server
				// says so when a query is routed there.
				if err := nc.CreateRemoteConstituent(ctx, composite, c.Name,
					c.TargetDatabase, c.Remote.URL, auth); err != nil {
					return nil, nil, err
				}
				r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentAdded,
					"Constituent %q now resolves to %q on %s",
					neo4jclient.QualifyConstituent(composite, c.Name), c.TargetDatabase, c.Remote.URL)
			case remoteConstituentDrifted(current, c, auth):
				if err := nc.AlterRemoteConstituent(ctx, composite, c.Name,
					c.TargetDatabase, c.Remote.URL, auth); err != nil {
					return nil, nil, err
				}
				r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentMoved,
					"Constituent %q re-pointed to %q on %s",
					neo4jclient.QualifyConstituent(composite, c.Name), c.TargetDatabase, c.Remote.URL)
			}
			continue
		}

		switch {
		case !present:
			// The target need not exist yet — but CREATE ALIAS against a
			// missing database fails, so skip rather than fail the whole
			// composite. The next reconcile picks it up.
			dbInfo, err := nc.GetDatabaseInfo(ctx, c.TargetDatabase)
			if err != nil {
				return nil, nil, fmt.Errorf("look up constituent target %q: %w", c.TargetDatabase, err)
			}
			if dbInfo == nil {
				continue
			}
			if err := nc.CreateCompositeConstituent(ctx, composite, c.Name, c.TargetDatabase); err != nil {
				return nil, nil, err
			}
			r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentAdded,
				"Constituent %q now resolves to %q",
				neo4jclient.QualifyConstituent(composite, c.Name), c.TargetDatabase)

		case localConstituentDrifted(current, c):
			if err := nc.AlterCompositeConstituent(ctx, composite, c.Name, c.TargetDatabase); err != nil {
				return nil, nil, err
			}
			r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeConstituentMoved,
				"Constituent %q re-pointed from %q to %q",
				neo4jclient.QualifyConstituent(composite, c.Name), current.Database, c.TargetDatabase)
		}
	}

	if compositeEnforce(cd) {
		for _, name := range undesiredConstituents(existing, cd.Spec.Constituents) {
			if err := nc.DropCompositeConstituent(ctx, composite, name); err != nil {
				return nil, nil, err
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
		return nil, lookalikes, err
	}
	out := append([]string(nil), info.Constituents...)
	sort.Strings(out)
	return out, lookalikes, nil
}

// classifyAliases splits every alias in the DBMS into the constituents of this
// composite and the ordinary aliases that merely look like them.
//
// The `composite` column is the only thing that tells them apart. Matching on
// the name prefix instead treats a plain alias as a constituent: Neo4j accepts
// CREATE ALIAS `x.y` with no composite `x` in sight, and the result is listed
// identically. The operator then tried to drop it as a constituent — DROP
// ALIAS `x`.`y`, which matches nothing — and IF EXISTS swallowed the miss, so
// it announced a removal that had not happened, on every reconcile, forever.
//
// A lookalike is not ours to drop: nothing in this CR's spec claims it, and
// deleting a resource the user created out of band on the strength of a name
// collision would be worse than leaving it. It is returned so the caller can
// say it out loud, because it is invisible in any listing that shows only
// names.
//
// Names are compared ignoring case, and constituents are keyed by their short
// name lower-cased. Neo4j normalises composite and alias names to lower case,
// so SHOW ALIASES lists the constituent `Latest` of composite `Cineasts` as
// `cineasts.latest` with composite `cineasts`, while the validator admits
// capitals in spec. An exact match found none of a capitalised composite's
// constituents: CREATE ALIAS was re-sent for each on every pass, and with
// enforceConstituents a capitalised constituent was dropped as "not in
// spec.constituents" and created again on the next.
func classifyAliases(live []neo4jclient.AliasInfo, composite string) (
	map[string]neo4jclient.AliasInfo, []string,
) {
	existing := map[string]neo4jclient.AliasInfo{}
	var lookalikes []string
	prefix := strings.ToLower(composite + ".")
	for _, a := range live {
		switch {
		case strings.EqualFold(a.Composite, composite):
			existing[strings.ToLower(neo4jclient.UnqualifyConstituent(composite, a.Name))] = a
		case strings.HasPrefix(strings.ToLower(a.Name), prefix):
			// Belongs to no composite, or to a different one — either way it
			// is not a constituent of ours, however much the name suggests it.
			lookalikes = append(lookalikes, a.Name)
		}
	}
	sort.Strings(lookalikes)
	return existing, lookalikes
}

// remoteAuth resolves a remote constituent's credentials into the form the
// Cypher builder takes.
//
// For OIDC forwarding there is nothing to read: no credential is stored, which
// is the whole point of that mode.
func (r *Neo4jCompositeDatabaseReconciler) remoteAuth(
	ctx context.Context, namespace string, remote *neo4jv1beta1.RemoteConstituent,
) (neo4jclient.RemoteConstituentAuth, error) {
	auth := neo4jclient.RemoteConstituentAuth{DriverSettings: remote.DriverSettings}
	if remote.OIDCCredentialForwarding {
		auth.OIDCForwarding = true
		return auth, nil
	}

	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{
		Name: remote.CredentialsSecretRef, Namespace: namespace,
	}, &secret); err != nil {
		return auth, fmt.Errorf("read credentials Secret %q: %w", remote.CredentialsSecretRef, err)
	}
	user, okUser := secret.Data["username"]
	pass, okPass := secret.Data["password"]
	if !okUser || !okPass {
		return auth, fmt.Errorf(
			"secret %q must contain both `username` and `password` keys for a remote constituent",
			remote.CredentialsSecretRef)
	}
	auth.Username = string(user)
	auth.Password = string(pass)
	return auth, nil
}

// undesiredConstituents returns, sorted, the keys of the live constituents
// (classifyAliases) that spec.constituents does not list — the ones
// enforceConstituents drops. Spec names are lower-cased to match the keys: an
// exact comparison dropped a constituent spec spells `Latest` on every other
// pass, since Neo4j lists it as `latest`, and the next pass created it again.
func undesiredConstituents(existing map[string]neo4jclient.AliasInfo, constituents []neo4jv1beta1.CompositeConstituent) []string {
	desired := map[string]bool{}
	for _, c := range constituents {
		desired[strings.ToLower(c.Name)] = true
	}
	var out []string
	for name := range existing {
		if !desired[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// localConstituentDrifted reports whether a live constituent no longer matches
// a local constituent in spec: it resolves to another database, or it is a
// remote alias. Location changing from remote to local is a real transition:
// the alias has to be re-pointed at a local database.
//
// The target is compared ignoring case. SHOW ALIASES lists the database
// lower-cased, so an exact match against a target spelled `Movies` re-pointed
// the constituent, and announced it, on every pass.
func localConstituentDrifted(current neo4jclient.AliasInfo, c neo4jv1beta1.CompositeConstituent) bool {
	return !strings.EqualFold(current.Database, c.TargetDatabase) || current.Location == "remote"
}

// remoteConstituentDrifted reports whether a live remote alias no longer
// matches spec.
//
// The credential itself is deliberately NOT compared, because it cannot be:
// SHOW ALIASES never returns the password on any alias. So a password rotated
// in the Secret alone is invisible here. Rather than let that silently persist,
// the reconcile re-issues ALTER whenever anything else about the alias moved,
// and a password-only change is handled by the operator re-applying on the
// user's next spec touch — documented in the guide, because a check that
// cannot see the field must not pretend otherwise.
func remoteConstituentDrifted(
	current neo4jclient.AliasInfo, c neo4jv1beta1.CompositeConstituent,
	auth neo4jclient.RemoteConstituentAuth,
) bool {
	if current.Location != "remote" {
		// It is a LOCAL alias today and spec wants it remote.
		return true
	}
	// The target is a database name, compared ignoring case as every Neo4j
	// database name is.
	if !strings.EqualFold(current.Database, c.TargetDatabase) || current.URL != c.Remote.URL {
		return true
	}
	// OIDC forwarding stores no user; stored credentials do. A change between
	// the two modes shows up exactly there.
	return current.User != auth.Username
}

// namespaceBlocker reports an existing alias sitting in the composite's
// namespace when the composite itself does not exist — the state that makes
// creation impossible.
func (r *Neo4jCompositeDatabaseReconciler) namespaceBlocker(
	ctx context.Context, nc compositeNeo4jClient, composite string,
) string {
	aliases, err := nc.ShowAliases(ctx)
	if err != nil {
		// Not being able to look is not evidence of a blocker; let the create
		// attempt speak for itself.
		return ""
	}
	return aliasInNamespace(aliases, composite)
}

// aliasInNamespace returns the first alias named `<composite>.<something>`,
// or "". The prefix is matched ignoring case: SHOW ALIASES lists names
// lower-cased, and a composite spelled with capitals has the same namespace.
func aliasInNamespace(aliases []neo4jclient.AliasInfo, composite string) string {
	prefix := strings.ToLower(composite + ".")
	for _, a := range aliases {
		if strings.HasPrefix(strings.ToLower(a.Name), prefix) {
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
	if !controllerutil.ContainsFinalizer(cd, Neo4jCompositeDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}
	release := func() error { return removeFinalizer(ctx, r.Client, cd, Neo4jCompositeDatabaseFinalizer) }

	if strings.EqualFold(cd.Spec.DeletionPolicy, "Retain") {
		log.FromContext(ctx).Info("deletionPolicy Retain — leaving the composite in place", "composite", name)
		return ctrl.Result{}, release()
	}

	target, err := ResolveClusterRef(ctx, r.Client, cd.Namespace, cd.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	// A deployment that is down, refuses the connection or refuses the drop
	// used to release the finalizer at once (or, for a refused drop, wait for
	// ever, #526). It waits, bounded, as every other cleanup: the composite
	// still exposes its constituents (knowledge rule 178).
	cleanup := deletionCleanup{
		obj: cd, recorder: r.Recorder, reason: EventReasonCompositeFailed,
		leftBehind: fmt.Sprintf("composite database %q and its constituent aliases on %s", name,
			targetRefDisplay(cd.Spec.ClusterRef)),
		byHand: fmt.Sprintf("DROP COMPOSITE DATABASE `%s` CASCADE ALIASES", neo4jclient.EscapeBackticks(name)),
		record: func(msg string) bool {
			return r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse, EventReasonCompositeFailed, msg, nil)
		},
		release: release, requeue: r.requeueAfter(),
	}
	if done, res, err := cleanup.settleByTarget(ctx, r.Client, cd.Namespace, cd.Spec.ClusterRef, target); done {
		return res, err
	}
	if !target.IsReady() {
		return cleanup.blocked(ctx, targetDownCause(cd.Spec.ClusterRef))
	}
	nc, err := r.connectForDeletion(target)
	if err != nil {
		return cleanup.blocked(ctx, connectFailedCause(cd.Spec.ClusterRef, err))
	}
	defer func() { _ = nc.Close() }()
	// CASCADE is required, not optional: a plain drop is REFUSED while
	// constituents exist. It removes the constituent ALIASES only — their
	// target databases are untouched.
	if err := nc.DropCompositeDatabase(ctx, name, true); err != nil {
		return cleanup.blocked(ctx, statementFailedCause(cd.Spec.ClusterRef, "DROP COMPOSITE DATABASE", err))
	}
	r.Recorder.Eventf(cd, corev1.EventTypeNormal, EventReasonCompositeDropped,
		"Composite database %q dropped (constituent aliases removed; their target databases kept)", name)
	return ctrl.Result{}, release()
}

func (r *Neo4jCompositeDatabaseReconciler) requeueAfter() time.Duration {
	if r.RequeueAfter > 0 {
		return r.RequeueAfter
	}
	return 30 * time.Second
}

// reportNameBlocked records that a dotted alias occupies the composite's
// namespace and announces CompositeDatabaseNameBlocked when that starts (or
// names a different alias), not on every pass until the alias is dropped
// (knowledge rule 145).
func (r *Neo4jCompositeDatabaseReconciler) reportNameBlocked(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase, name, blocker string,
) {
	msg := fmt.Sprintf(
		"cannot create composite %q: the alias %q already occupies its namespace. "+
			"Neo4j accepts an alias named `<composite>.<constituent>` even when no such "+
			"composite exists, and that alias then blocks the composite permanently. "+
			"Drop %q, then this will converge", name, blocker, blocker)
	if r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
		EventReasonCompositeNameBlocked, msg, nil) {
		r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonCompositeNameBlocked, msg)
	}
}

func (r *Neo4jCompositeDatabaseReconciler) fail(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	label string, err error, requeue time.Duration,
) (ctrl.Result, error) {
	msg := label
	if err != nil {
		msg = fmt.Sprintf("%s: %v", label, err)
	}
	// Announced when the failure starts or its message changes, not on every
	// retry of the same failure (knowledge rule 145).
	if r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse,
		EventReasonCompositeFailed, msg, nil) {
		r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonCompositeFailed, msg)
	}
	// A rejection the server will repeat (Neo.ClientError.*: a malformed
	// statement, a missing role or user, an argument it refuses) cannot
	// resolve without someone changing something, and error backoff would
	// retry it within milliseconds and bury the one event that explains it.
	// It is already in status and the event above; retry it on the fixed
	// interval. Anything else — a connectivity blip, a transient conflict —
	// goes back as an error, to controller-runtime's backoff. Same rule as
	// Neo4jReplicaDatabase's fail(). Never return a requeue WITH an error:
	// controller-runtime ignores it (TestNoRequeueWithError).
	if isPermanentServerRejection(err) {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	return ctrl.Result{}, err
}

// setStatus writes the phase, message and Ready condition. It reports whether
// the write changed the phase, the message or the Ready condition's status or
// reason — a state transition — from the attempt whose write succeeded, so a
// caller can emit the matching event once, on the transition.
func (r *Neo4jCompositeDatabaseReconciler) setStatus(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	phase string, readyStatus metav1.ConditionStatus, reason, message string, constituents []string,
) bool {
	transitioned, _ := r.writeStatus(ctx, cd, phase, readyStatus, reason, message, constituents)
	return transitioned
}

// writeStatus is setStatus that also reports whether the write moved the
// persisted phase into the one given (entered), the transition
// CompositeDatabaseReady announces.
func (r *Neo4jCompositeDatabaseReconciler) writeStatus(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	phase string, readyStatus metav1.ConditionStatus, reason, message string, constituents []string,
) (transitioned, entered bool) {
	update := func() error {
		transitioned, entered = false, false
		latest := &neo4jv1beta1.Neo4jCompositeDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cd), latest); err != nil {
			return err
		}
		phaseMoved := latest.Status.Phase != phase
		changed := phaseMoved || latest.Status.Message != message
		if SetReadyCondition(&latest.Status.Conditions, latest.Generation, readyStatus, reason, message) {
			changed = true
		}
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if constituents != nil {
			latest.Status.ObservedConstituents = constituents
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		transitioned, entered = changed, phaseMoved
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to update Neo4jCompositeDatabase status")
		return false, false
	}
	return transitioned, entered
}

// failPass is fail for the reconcile pass: an error that says the target
// cannot be reached is the target's outage, waited through (unreachable), not
// a failure of this composite. The deletion keeps calling fail itself.
func (r *Neo4jCompositeDatabaseReconciler) failPass(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
	label string, err error, requeue time.Duration,
) (ctrl.Result, error) {
	if targetUnreachable(err) {
		return r.unreachable(ctx, cd, err, requeue)
	}
	return r.fail(ctx, cd, label, err, requeue)
}

// waitForTarget records that the target cannot serve the composite — reason
// ClusterNotReady (not Ready) or ClusterUnreachable (reads Ready, does not
// answer): the ClusterNotReady condition, and phase Pending unless the
// composite is already Ready at its generation, which it stays. The composite
// lives on the target and does not change while the target restarts, and the
// target announces its own state; demoting it made the pass that found the
// target back a CompositeDatabaseReady (#565; knowledge rule 183). The status
// is written only when it changes. It reports lasting when an unreachable
// target has outlasted targetUnreachableGrace.
func (r *Neo4jCompositeDatabaseReconciler) waitForTarget(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase, reason, msg string,
) (lasting bool) {
	update := func() error {
		latest := &neo4jv1beta1.Neo4jCompositeDatabase{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cd), latest); err != nil {
			return err
		}
		before := latest.Status.DeepCopy()
		lasting = recordTargetWait(&latest.Status.Conditions, &latest.Status.Phase, &latest.Status.Message,
			latest.Generation, reason, msg, time.Now())
		latest.Status.ObservedGeneration = latest.Generation
		if apiequality.Semantic.DeepEqual(*before, latest.Status) {
			return nil
		}
		return r.Status().Update(ctx, latest)
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to record the target wait on Neo4jCompositeDatabase")
		return false
	}
	return lasting
}

// unreachable handles a target that reads Ready but cannot be reached:
// waitForTarget with reason ClusterUnreachable, logged at info and retried on
// the fixed interval. Only an outage that outlasts targetUnreachableGrace is
// this composite's failure: Failed, ConnectionFailed, announced once
// (knowledge rules 172 and 183).
func (r *Neo4jCompositeDatabaseReconciler) unreachable(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase, err error, requeue time.Duration,
) (ctrl.Result, error) {
	if r.waitForTarget(ctx, cd, ConditionReasonClusterUnreachable, targetUnreachableMessage(cd.Spec.ClusterRef, err)) {
		msg := fmt.Sprintf("failed to connect to Neo4j for more than %s: %v", targetUnreachableGrace, err)
		if r.setStatus(ctx, cd, neo4jv1beta1.PhaseFailed, metav1.ConditionFalse, EventReasonConnectionFailed, msg, nil) {
			r.Recorder.Event(cd, corev1.EventTypeWarning, EventReasonConnectionFailed, msg)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	log.FromContext(ctx).Info("Target reads Ready but cannot be reached; retrying",
		"clusterRef", cd.Spec.ClusterRef, "error", err.Error())
	return ctrl.Result{RequeueAfter: requeue}, nil
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

// compositeCypherLanguage returns the value to put in the composite's DEFAULT
// LANGUAGE CYPHER clause, or "" when the clause must be left out.
//
// The 5.26 LTS does not parse that clause on CREATE COMPOSITE DATABASE or
// ALTER DATABASE, and every database there already runs Cypher 5, so a
// requested "5" is dropped (the outcome is what the user asked for) — the same
// rule Neo4jDatabase follows via DefaultLanguageForImage. A requested "25" on
// the LTS never gets this far: the validator refuses it first. A target whose
// image tag cannot be read passes the request through unchanged.
func compositeCypherLanguage(cd *neo4jv1beta1.Neo4jCompositeDatabase, target ResolvedTarget) string {
	var tag string
	switch {
	case target.Cluster != nil:
		tag = target.Cluster.Spec.Image.Tag
	case target.Standalone != nil:
		tag = target.Standalone.Spec.Image.Tag
	}
	return neo4jclient.DefaultLanguageForImage(tag, cd.Spec.DefaultCypherLanguage)
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
