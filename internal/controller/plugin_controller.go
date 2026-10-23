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
	"encoding/json"
	stderrors "errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	// DefaultAdminSecretNamePlugin is the default name for admin credentials secret
	DefaultAdminSecretNamePlugin = "neo4j-admin-secret"

	// PluginInstallModeManaged is the default — operator adds the plugin to
	// NEO4J_PLUGINS so the Neo4j Docker entrypoint resolves and installs the
	// JAR at pod startup.
	PluginInstallModeManaged = "Managed"

	// PluginInstallModePreBaked — operator does not touch NEO4J_PLUGINS. The
	// JAR must already be in /plugins (baked into a custom Neo4j image).
	// Operator still applies plugin configuration (security allowlists,
	// unrestricted procedures, ConfigMap entries).
	PluginInstallModePreBaked = "PreBaked"

	// PluginInstallModeVerifiedDownload — operator injects an init
	// container into the target StatefulSet that downloads spec.source.url,
	// verifies the SHA256/SHA512 against spec.source.checksum, and drops
	// the JAR into the shared /plugins emptyDir before Neo4j starts. As
	// with PreBaked, NEO4J_PLUGINS is NOT mutated — the entrypoint's own
	// download path is bypassed entirely so it can't race the verified
	// JAR. Configuration env vars / ConfigMap entries still flow.
	PluginInstallModeVerifiedDownload = "VerifiedDownload"

	// PluginInitContainersAnnotation tracks the names of init containers
	// the plugin controller owns on a given StatefulSet, so the controller
	// can remove only its own containers on plugin uninstall without
	// disturbing init containers added by other controllers / users.
	// Value format: comma-separated list of container names.
	PluginInitContainersAnnotation = "neo4j.com/plugin-init-containers"
)

// isPreBakedInstallMode reports whether the operator should skip the
// NEO4J_PLUGINS env mutation for this plugin and only apply configuration.
func isPreBakedInstallMode(plugin *neo4jv1beta1.Neo4jPlugin) bool {
	return plugin.Spec.InstallMode == PluginInstallModePreBaked
}

// isVerifiedDownloadInstallMode reports whether the operator should
// inject an init container for this plugin AND skip the NEO4J_PLUGINS
// env mutation (same env-skip semantics as PreBaked, but with a new
// JAR-delivery path).
func isVerifiedDownloadInstallMode(plugin *neo4jv1beta1.Neo4jPlugin) bool {
	return plugin.Spec.InstallMode == PluginInstallModeVerifiedDownload
}

// shouldSkipNeo4jPluginsEnv reports whether either of the
// JAR-already-in-/plugins install modes is active. Both PreBaked and
// VerifiedDownload share the "operator must NOT add this plugin to
// NEO4J_PLUGINS" semantics — wrapping both into one predicate keeps
// the callsite in installPluginViaEnvironment readable.
func shouldSkipNeo4jPluginsEnv(plugin *neo4jv1beta1.Neo4jPlugin) bool {
	return isPreBakedInstallMode(plugin) || isVerifiedDownloadInstallMode(plugin)
}

// getAdminSecretName safely extracts the admin secret name from cluster spec with fallback to default
func getClusterAdminSecretName(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.AdminSecret != "" {
		return cluster.Spec.Auth.AdminSecret
	}
	return DefaultAdminSecretNamePlugin
}

// getStandaloneAdminSecretName safely extracts the admin secret name from standalone spec with fallback to default
func getStandaloneAdminSecretName(standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) string {
	if standalone.Spec.Auth != nil && standalone.Spec.Auth.AdminSecret != "" {
		return standalone.Spec.Auth.AdminSecret
	}
	return DefaultAdminSecretNamePlugin
}

// Neo4jPluginReconciler reconciles a Neo4jPlugin object
type Neo4jPluginReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	RequeueAfter time.Duration

	// Validator performs structural validation of the Neo4jPlugin spec
	// (name/version/source/checksum/dependencies/security/resources) inline
	// before any install work. Hard errors short-circuit the reconcile into
	// the "Invalid" phase; compatibility-matrix advisories are surfaced as
	// Warning events only. Always set via NewPluginValidator() in cmd/main.go.
	Validator *validation.PluginValidator

	// PluginInitImage overrides the default init container image
	// (resources.DefaultPluginInitContainerImage) used for
	// VerifiedDownload mode. Wired from the operator's
	// PLUGIN_INIT_CONTAINER_IMAGE env var which the helm chart sets
	// from .Values.pluginInitContainer.image. Empty = use the default
	// hardcoded in the resource builder.
	PluginInitImage string

	// advisoryWarnings announces each advisory warning when it starts to apply
	// and every advisoryWarningRefresh after, not on every pass.
	advisoryWarnings advisoryWarnings

	// bolt replaces the network in tests; nil in production.
	bolt *pluginBolt
}

// pluginBolt stands in, in tests, for the two steps of a pass that need a
// running Neo4j.
type pluginBolt struct {
	functional func(ctx context.Context, deployment *DeploymentInfo) bool
	configure  func(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error
}

func (r *Neo4jPluginReconciler) deploymentFunctional(ctx context.Context, deployment *DeploymentInfo) bool {
	if r.bolt != nil && r.bolt.functional != nil {
		return r.bolt.functional(ctx, deployment)
	}
	return r.isDeploymentFunctional(ctx, deployment)
}

func (r *Neo4jPluginReconciler) configure(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	if r.bolt != nil && r.bolt.configure != nil {
		return r.bolt.configure(ctx, plugin, deployment)
	}
	return r.configurePlugin(ctx, plugin, deployment)
}

// PluginFinalizer is the finalizer for Neo4j plugin resources
const PluginFinalizer = "neo4j.neo4j.com/plugin-finalizer"

// checkForDuplicatePlugin lists every Neo4jPlugin in the same namespace
// and returns the metadata.name of the CR that should "own" the
// (clusterRef, plugin name) tuple — i.e. the one this controller will
// actually reconcile. The newer duplicate is expected to drop itself
// into Failed state and stop reconciling.
//
// Selection rules:
//   - Candidates with a different clusterRef or a different plugin name
//     are ignored.
//   - Candidates currently being deleted (DeletionTimestamp != nil)
//     are ignored — they're on their way out, no point handing them
//     ownership.
//   - Among the remaining candidates (including the plugin under
//     reconciliation), the oldest CreationTimestamp wins; UID is the
//     tiebreaker when timestamps collide (which is rare but possible
//     in fast test loops).
//
// Returns the empty string if no other live CR contests the tuple —
// the caller treats that the same as "this CR is the winner".
func (r *Neo4jPluginReconciler) checkForDuplicatePlugin(
	ctx context.Context,
	plugin *neo4jv1beta1.Neo4jPlugin,
) (string, error) {
	list := &neo4jv1beta1.Neo4jPluginList{}
	if err := r.List(ctx, list, client.InNamespace(plugin.Namespace)); err != nil {
		return "", fmt.Errorf("failed to list Neo4jPlugin in namespace %q: %w", plugin.Namespace, err)
	}

	winner := plugin
	contested := false
	for i := range list.Items {
		candidate := &list.Items[i]
		if candidate.UID == plugin.UID {
			continue
		}
		if candidate.Spec.ClusterRef != plugin.Spec.ClusterRef {
			continue
		}
		if candidate.Spec.Name != plugin.Spec.Name {
			continue
		}
		if candidate.DeletionTimestamp != nil {
			continue
		}
		contested = true
		// Older CreationTimestamp wins. If timestamps tie, lower UID
		// wins — gives a deterministic ordering across replicas of
		// the operator.
		if candidate.CreationTimestamp.Before(&winner.CreationTimestamp) {
			winner = candidate
		} else if candidate.CreationTimestamp.Equal(&winner.CreationTimestamp) &&
			string(candidate.UID) < string(winner.UID) {
			winner = candidate
		}
	}

	if !contested {
		return "", nil
	}
	return winner.Name, nil
}

//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jplugins,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jplugins/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=neo4j.neo4j.com,resources=neo4jplugins/finalizers,verbs=update
// Pods: read for the target's servers, and a standalone's pod deleted when its
// OrderedReady StatefulSet will not replace it (replaceStuckPods).
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

// Reconcile handles the reconciliation of Neo4jPlugin resources
func (r *Neo4jPluginReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Neo4jPlugin instance
	plugin := &neo4jv1beta1.Neo4jPlugin{}
	if err := r.Get(ctx, req.NamespacedName, plugin); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Neo4jPlugin resource not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Neo4jPlugin")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if plugin.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, plugin)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(plugin, PluginFinalizer) {
		if err := addFinalizer(ctx, r.Client, plugin, PluginFinalizer); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		// The patch above triggers a watch event that re-queues this object.
		return ctrl.Result{}, nil
	}

	// Structural validation gate. Hard errors (missing name/version, bad
	// source/checksum, conflicting security, VerifiedDownload gaps) leave the
	// plugin in "Invalid" with no requeue — re-edit the CR to retry. The
	// compatibility matrix produces advisory Warnings only (the operator
	// installs arbitrary, incl. URL-sourced, plugins and is forward-compatible
	// across Neo4j versions), so those never block install.
	if r.Validator != nil {
		result := r.Validator.Validate(plugin)
		r.advisoryWarnings.record(r.Recorder, plugin, EventReasonValidationWarning, result.Warnings)
		if len(result.Errors) > 0 {
			msg := fmt.Sprintf("Neo4jPlugin spec is invalid: %s", result.Errors.ToAggregate().Error())
			// Announced when the failure starts or its message changes, not on
			// every pass (a resync, any status write) while the spec stays
			// invalid (knowledge rule 136).
			if r.updatePluginStatus(ctx, plugin, "Invalid", msg) {
				r.Recorder.Event(plugin, corev1.EventTypeWarning, EventReasonValidationFailed, msg)
			}
			return ctrl.Result{}, nil
		}
	}

	// Refuse to reconcile if another Neo4jPlugin CR already owns this
	// (clusterRef, plugin name) tuple. Two reconcilers racing on the
	// same /plugins directory + NEO4J_PLUGINS env var produces flapping
	// state (one CR adds, the other removes, restart cycle). Validator
	// can't catch this — it's structural-only, no cross-CR visibility.
	// Resolved here at reconcile time with a deterministic "oldest
	// wins" tiebreaker; newer duplicates surface the conflict via
	// status + Event and stop reconciling. When the winner is deleted,
	// the watch on Neo4jPlugin fires for the survivor — at which point
	// the previously-failed duplicate becomes the new winner on its
	// own next reconcile.
	winner, dupErr := r.checkForDuplicatePlugin(ctx, plugin)
	if dupErr != nil {
		logger.Error(dupErr, "Failed to check for duplicate Neo4jPlugin CRs")
		return ctrl.Result{}, dupErr
	}
	if winner != "" && winner != plugin.Name {
		msg := fmt.Sprintf(
			"duplicate Neo4jPlugin: %q (older) already targets cluster %q with plugin name %q; delete one CR before this one can reconcile",
			winner, plugin.Spec.ClusterRef, plugin.Spec.Name,
		)
		// Announced when the conflict starts, not again on the pass its own
		// status write starts, nor on every resync (knowledge rule 145).
		if r.updatePluginStatus(ctx, plugin, "Failed", msg) {
			r.Recorder.Event(plugin, corev1.EventTypeWarning, EventReasonPluginDuplicate, msg)
		}
		return ctrl.Result{}, nil
	}

	// Get target deployment (cluster or standalone)
	deployment, err := r.getTargetDeployment(ctx, plugin)
	if err != nil {
		logger.Error(err, "Failed to get target deployment",
			"clusterRef", plugin.Spec.ClusterRef, "namespace", plugin.Namespace)
		r.updatePluginStatus(
			ctx,
			plugin,
			"Failed",
			fmt.Sprintf(
				"Target deployment not found (clusterRef=%q, namespace=%q): %v",
				plugin.Spec.ClusterRef,
				plugin.Namespace,
				err,
			),
		)
		return ctrl.Result{}, nil // Don't return error - status is set correctly
	}

	// Standalone neo4j.conf (plugin security settings) is owned by the standalone
	// controller, which folds in the union of all Neo4jPlugin CRs for the target
	// and rolls the pod on change (issue #146). It watches Neo4jPlugin, so creating
	// this CR triggers that reconcile — the plugin controller no longer patches the
	// standalone's ConfigMap here (that caused a two-controller tug-of-war and left
	// uninstalled plugins' keys behind). NEO4J_PLUGINS env + init containers below
	// are still managed here.

	// Announce Installing only when this is actually an install.
	//
	// This used to run on EVERY reconcile, including the many that happen once
	// a plugin is settled, so the phase oscillated Ready → Installing → Ready
	// indefinitely: sampling it every 2s returned a different answer each time
	// while APOC was installed and working. Anything keying off status.phase
	// flapped with it — this project's own ArgoCD health checks included.
	settled := plugin.Status.Phase == neo4jv1beta1.PhaseReady &&
		plugin.Status.ObservedGeneration == plugin.Generation
	// Whether this pass may write to the target's StatefulSet, decided where a
	// write is first wanted: the target is Ready with every server available
	// and has rolled out its latest spec (IsReady), or the write changes what
	// every server that is down keeps failing in — the plugin's own init
	// container, or its settings in the neo4j container (selfRepair; repair
	// says which pods, repairSettings whether it is the settings).
	repair, repairSettings, repairChecked := "", false, false
	mayWrite := func() bool {
		if deployment.IsReady {
			return true
		}
		if !repairChecked {
			repairChecked = true
			var err error
			if repair, repairSettings, err = r.selfRepair(ctx, plugin, deployment); err != nil {
				logger.Error(err, "Cannot tell whether the plugin's write repairs its target")
			}
		}
		return repair != ""
	}
	if settled {
		// A settled plugin is reconciled again whenever its target's pod
		// template changes (SetupWithManager), because other controllers write
		// that StatefulSet too. If it still carries the plugin there is nothing
		// to do: no Bolt, no write, no phase change, so a rollout someone else
		// started does not flip the plugin out of Ready. If something removed
		// the plugin — a cluster-controller conflict retry once deleted
		// NEO4J_PLUGINS (operations.md id 134), its template apply dropped a
		// VerifiedDownload init container (id 141) — it is not Ready any more:
		// say so, and restore it below, once the target can take the restart.
		// What follows the target's spec.trustedCASecrets — a VerifiedDownload
		// plugin's CA volume, and its init container's mount of it — is brought
		// in line with the list when the user changes it (id 147): a change
		// someone asked for, not a loss, so no Warning; it waits like an install.
		// A variable the plugin recorded and no longer sets is taken away the
		// same way (id 155).
		drift, err := r.targetDrift(ctx, plugin, deployment)
		missing, outdated := drift.missing, drift.outdated
		switch {
		case err != nil:
			// The install below reads the same StatefulSet and reports it.
			logger.Error(err, "Cannot tell whether the target still carries the plugin")
		case missing == "" && outdated == "" && drift.removing == "":
			// Nothing to put right. A plugin installed by an earlier operator
			// records what it sets, which restarts nothing (id 155).
			if err := r.recordPluginEnv(ctx, plugin, deployment); err != nil {
				logger.Error(err, "Cannot record the variables the plugin sets on its target")
			}
			return ctrl.Result{}, nil
		case missing == "" && outdated == "":
			logger.Info("The target holds variables the plugin no longer sets; removing them", "removing", drift.removing)
			if mayWrite() && repair == "" {
				r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Removing what the plugin no longer sets: "+drift.removing)
			}
		case missing == "":
			// Not writable: the gate below holds the write like an install's.
			logger.Info("The target's trusted CAs changed; updating the plugin's", "change", outdated)
			if mayWrite() && repair == "" {
				r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Updating the plugin's trusted CAs: "+outdated)
			}
		case !mayWrite():
			// Restoring it restarts every server, like an install, and waits
			// for the same reason (below).
			logger.Info("Plugin missing from its target; restoring it once the target is fully available", "missing", missing)
			return r.deferInstall(ctx, plugin, deployment, missing, "")
		default:
			logger.Info("Plugin missing from its target; restoring it", "missing", missing)
			r.Recorder.Eventf(plugin, corev1.EventTypeWarning, EventReasonPluginMissing,
				"%s; restoring it, which restarts the servers", missing)
			if repair == "" {
				r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Restoring the plugin: "+missing)
			}
		}
	}

	// An install restarts every server, and so does any write of the plugin to
	// the target's StatefulSet. A cluster already short of a server (Degraded),
	// or still forming, rolling or upgrading, could lose its majority to it
	// (CLAUDE.md, "Server availability (#444)"), so the write waits until the
	// target is Ready with every server available and has rolled out its
	// latest spec — unless it repairs the plugin's own init container, failing
	// on every server that is down (selfRepair). A pass with nothing to write
	// — one waiting for the servers this install restarted, which turned the
	// cluster Forming — carries on.
	if !deployment.IsReady {
		changes, err := r.installChangesTarget(ctx, plugin, deployment)
		switch {
		case err != nil:
			// The install below reads the same StatefulSet and reports it.
			logger.Error(err, "Cannot tell whether installing the plugin would change its target")
		case !changes:
		case !mayWrite():
			logger.Info("Target not fully available; deferring the plugin installation", "type", deployment.Type, "name", deployment.Name)
			return r.deferInstall(ctx, plugin, deployment, "", r.trustedCAUpdate(ctx, plugin, deployment))
		default:
			logger.Info("Target not fully available, but every server that is down fails in what this write changes; writing the repair",
				"type", deployment.Type, "name", deployment.Name, "servers", repair, "settings", repairSettings)
		}
	}

	// A standalone's pod that the plugin's init container keeps from starting
	// is not replaced by its StatefulSet once a write repairs it; the plugin
	// deletes it (replaceStuckPods). Before the functional check: the
	// standalone answers nothing until the pod is back.
	r.replaceStuckPods(ctx, plugin, deployment)

	// Check if deployment is actually functional, not just status reporting.
	// Not before a repair: the servers it repairs are down, and a standalone
	// whose one server is down answers nothing.
	if repair == "" && !r.deploymentFunctional(ctx, deployment) {
		logger.Info("Target deployment not functional, requeuing", "type", deployment.Type, "name", deployment.Name)
		r.updatePluginStatus(ctx, plugin, "Waiting", fmt.Sprintf("Waiting for %s %s to be functional", deployment.Type, deployment.Name))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Installing is announced only where an install starts (see settled
	// above), so not by a pass that continues one of this generation: one
	// waiting for the servers, or retrying a failure. The Installing write is a
	// change of its own and re-queues the plugin at once: while the pods
	// restarted, "Installing plugin" and "Waiting for pods" took turns, and a
	// lasting failure flipped Failed → Installing → Failed with a Warning each
	// time, as fast as passes could run.
	inProgress := plugin.Status.ObservedGeneration == plugin.Generation &&
		(plugin.Status.Phase == neo4jv1beta1.PhaseInstalling || plugin.Status.Phase == neo4jv1beta1.PhaseFailed)
	switch {
	case repair != "":
		// A write to a target that is not Ready: said once, from the status
		// write that records it (operations.md ids 136, 145).
		message := fmt.Sprintf("Writing the plugin to %s although it is not Ready: its init container keeps %s from starting, "+
			"and this write changes that container; the roll replaces those pods first", targetName(deployment), repair)
		reason := EventReasonPluginInitContainerFailing
		if repairSettings {
			message = fmt.Sprintf("Writing the plugin to %s although it is not Ready: every server that is down keeps failing "+
				"with settings this write changes (%s); the roll replaces those pods first", targetName(deployment), repair)
			reason = EventReasonPluginSettingsFailing
		}
		if r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, message) {
			r.Recorder.Event(plugin, corev1.EventTypeWarning, reason, message)
		}
	case !settled && !inProgress:
		message := "Installing plugin"
		if outdated := r.trustedCAUpdate(ctx, plugin, deployment); outdated != "" {
			// A CA update that waited for the target is still a CA update.
			message = "Updating the plugin's trusted CAs: " + outdated
		}
		r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, message)
	}

	// Install plugin using NEO4J_PLUGINS environment variable (recommended by Neo4j docs)
	written, err := r.writePluginToTarget(ctx, plugin, deployment)
	if err != nil {
		if retryablePluginError(err) {
			logger.Info("Plugin installation interrupted; retrying", "error", err.Error())
			r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling,
				fmt.Sprintf("Retrying the plugin installation: %v", err))
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
		logger.Error(err, "Failed to install plugin")
		return r.installFailed(ctx, plugin, fmt.Sprintf("Plugin installation failed: %v", err),
			fmt.Sprintf("Plugin %s installation failed: %v", plugin.Spec.Name, err))
	}

	// The write restarts every server, and the old pods report Ready until
	// they stop: the plugin is Ready only once its target's StatefulSet has
	// rolled the template out to every pod and they are ready (non-blocking),
	// judged against the generation this pass wrote, if it wrote. A pod its init
	// container keeps from starting is named, so the wait says what it waits for.
	if pending, failing := r.rolloutPending(ctx, plugin, deployment, written); pending != "" || !r.arePodsReady(ctx, deployment) {
		logger.Info("Waiting for pods to be ready after plugin installation", "rollout", pending)
		message := "Waiting for pods to be ready after plugin installation"
		if failing != "" {
			message += "; cannot start: " + failing
		}
		r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, message)
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Configure plugin, once every server runs it. A server that cannot be
	// reached even then (one restarting for another reason; before the wait
	// above, the old pods still reported Ready while they stopped) is part of
	// the install, not a failed one.
	if err := r.configure(ctx, plugin, deployment); err != nil {
		if retryablePluginError(err) {
			logger.Info("Cannot reach the target to configure the plugin yet; retrying", "error", err.Error())
			r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling,
				fmt.Sprintf("Waiting for %s %s to accept connections to configure the plugin", deployment.Type, deployment.Name))
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
		logger.Error(err, "Failed to configure plugin")
		return r.installFailed(ctx, plugin, fmt.Sprintf("Plugin configuration failed: %v", err),
			fmt.Sprintf("Plugin %s configuration failed: %v", plugin.Spec.Name, err))
	}

	// Update status to "Ready"
	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	r.Recorder.Eventf(plugin, corev1.EventTypeNormal, EventReasonPluginInstalled,
		"Plugin %s version %s installed successfully", plugin.Spec.Name, plugin.Spec.Version)

	logger.Info("Plugin reconciliation completed")
	return ctrl.Result{}, nil
}

// installFailed records a failed install or configuration: phase Failed, a
// Warning when the failure is new, and another attempt after RequeueAfter. A
// retry that fails the same way changes nothing and says nothing more. What
// failed can lie outside the plugin (a Secret, the cluster's own spec), so the
// plugin keeps trying rather than waiting for an edit to its CR.
func (r *Neo4jPluginReconciler) installFailed(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, message, event string) (ctrl.Result, error) {
	if plugin.Status.Phase != neo4jv1beta1.PhaseFailed || plugin.Status.Message != message {
		r.Recorder.Event(plugin, corev1.EventTypeWarning, EventReasonPluginInstallFailed, event)
	}
	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseFailed, message)
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}

// deferInstall holds a write of the plugin to a target that is not Ready with
// every server available: phase Waiting, another look after RequeueAfter, and
// one event when the wait starts — PluginMissing for a Ready plugin whose target
// lost what it set (missing says what), PluginInstallDeferred otherwise. A
// write that only follows the target's trusted CAs (caUpdate says what
// changed) says so. The event follows the status write that starts the wait
// (operations.md id 136), so the passes that go on waiting, and an operator
// restart, say nothing more.
func (r *Neo4jPluginReconciler) deferInstall(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo, missing, caUpdate string) (ctrl.Result, error) {
	until := targetAvailable(deployment)
	what := "installing the plugin"
	if caUpdate != "" {
		what = "updating the plugin's trusted CAs"
	}
	message := "Waiting until " + until + ": " + what + " restarts every server"
	if deployment.Type == "standalone" {
		message = "Waiting until " + until + ": " + what + " restarts it"
	}
	if r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseWaiting, message) {
		if missing != "" {
			r.Recorder.Eventf(plugin, corev1.EventTypeWarning, EventReasonPluginMissing,
				"%s; restoring it restarts the servers, so it waits until %s", missing, until)
		} else {
			r.Recorder.Event(plugin, corev1.EventTypeNormal, EventReasonPluginInstallDeferred, message)
		}
	}
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}

// targetAvailable names the state deferInstall waits for.
func targetAvailable(deployment *DeploymentInfo) string {
	if deployment.Type == "standalone" {
		return targetName(deployment) + " is Ready"
	}
	return targetName(deployment) + " is Ready with every server available"
}

// targetName names the plugin's target with its kind.
func targetName(deployment *DeploymentInfo) string {
	if deployment.Type == "standalone" {
		return "Neo4jEnterpriseStandalone " + deployment.Name
	}
	return "Neo4jEnterpriseCluster " + deployment.Name
}

// trustedCAUpdate says what changed when all the plugin's write would put
// right on its target is what follows the target's spec.trustedCASecrets — its
// CA volume, and its init container's mount of it (missingFromTarget's
// "outdated", operations.md id 147); "" otherwise. Every pass words a write the
// same way from it — the settled one that finds the change, the wait for the
// target, the pass that then writes — so a wait is announced once.
func (r *Neo4jPluginReconciler) trustedCAUpdate(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) string {
	missing, outdated, err := r.missingFromTarget(ctx, plugin, deployment)
	if err != nil || missing != "" {
		return ""
	}
	return outdated
}

// installChangesTarget reports whether installing the plugin would change its
// target's StatefulSet, which restarts the servers: the install's own edits
// (applyPluginToTemplate) made on a copy of the live pod template.
func (r *Neo4jPluginReconciler) installChangesTarget(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return false, err
	}
	// The pod template only: the plugin's record is StatefulSet metadata, and
	// writing it alone restarts nothing.
	desired := sts.DeepCopy()
	if _, err := r.applyPlugin(ctx, desired, plugin, deployment); err != nil {
		return false, err
	}
	return !equality.Semantic.DeepEqual(sts.Spec.Template, desired.Spec.Template), nil
}

// retryablePluginError reports whether an install or configuration step failed
// for a reason to wait out rather than a failed install: the StatefulSet kept
// changing under the write (its own controller rewrites its status at every
// pod transition of a restart), or Neo4j could not be reached because the
// servers are restarting to take the plugin.
func retryablePluginError(err error) bool {
	return errors.IsConflict(err) ||
		neo4jclient.IsConnectivityError(err) ||
		stderrors.Is(err, context.DeadlineExceeded)
}

func (r *Neo4jPluginReconciler) handleDeletion(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Uninstall the plugin: one write of its target's StatefulSet, which does
	// not wait for a fully available target (operations.md id 141).
	if err := r.uninstallPlugin(ctx, plugin); err != nil {
		logger.Error(err, "Failed to uninstall plugin")
		return ctrl.Result{}, err
	}

	// Remove finalizer
	if err := removeFinalizer(ctx, r.Client, plugin, PluginFinalizer); err != nil {
		logger.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	logger.Info("Plugin deleted successfully")
	return ctrl.Result{}, nil
}

// DeploymentInfo holds information about the target deployment
type DeploymentInfo struct {
	Object    client.Object
	Type      string // "cluster" or "standalone"
	Name      string
	Namespace string
	IsReady   bool
}

func (r *Neo4jPluginReconciler) getTargetDeployment(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin) (*DeploymentInfo, error) {
	// Try Neo4jEnterpriseCluster first
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      plugin.Spec.ClusterRef,
		Namespace: plugin.Namespace,
	}, cluster); err == nil {
		info := &DeploymentInfo{
			Object:    cluster,
			Type:      "cluster",
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		}
		// Ready only, not Degraded: installing a plugin rolls every server,
		// which on a cluster already short of one could cost it its majority.
		// And not a Ready the cluster has yet to act on: one read before it
		// processed its latest spec, or while its servers still roll
		// (targetRolloutPending).
		info.IsReady = cluster.Status.Phase == "Ready" && serversUnavailableSince(cluster) == nil &&
			r.targetRolloutPending(ctx, info, cluster.Generation, cluster.Status.ObservedGeneration) == ""
		return info, nil
	}

	// Try Neo4jEnterpriseStandalone
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      plugin.Spec.ClusterRef,
		Namespace: plugin.Namespace,
	}, standalone); err == nil {
		info := &DeploymentInfo{
			Object:    standalone,
			Type:      "standalone",
			Name:      standalone.Name,
			Namespace: standalone.Namespace,
		}
		// A standalone reads Ready while its one pod does, also when its
		// StatefulSet is about to replace it.
		info.IsReady = standalone.Status.Phase == "Ready" &&
			r.targetRolloutPending(ctx, info, standalone.Generation, standalone.Status.ObservedGeneration) == ""
		return info, nil
	}

	return nil, fmt.Errorf("target deployment %s not found (tried both Neo4jEnterpriseCluster and Neo4jEnterpriseStandalone)", plugin.Spec.ClusterRef)
}

// arePodsReady checks if all pods are ready without blocking
func (r *Neo4jPluginReconciler) arePodsReady(ctx context.Context, deployment *DeploymentInfo) bool {
	logger := log.FromContext(ctx)

	// Check if all pods are ready
	pods := &corev1.PodList{}
	podLabels := r.getPodLabels(deployment)
	if err := r.List(ctx, pods, client.InNamespace(deployment.Namespace), client.MatchingLabels(podLabels)); err != nil {
		logger.Error(err, "Failed to list pods")
		return false
	}

	expectedReplicas := r.getExpectedReplicas(deployment)
	if len(pods.Items) != expectedReplicas {
		logger.Info("Not all pods are created yet", "current", len(pods.Items), "expected", expectedReplicas)
		return false
	}

	for _, pod := range pods.Items {
		// Check if pod is ready
		podReady := pod.Status.Phase == corev1.PodRunning
		if podReady {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady {
					podReady = condition.Status == corev1.ConditionTrue
					break
				}
			}
		}
		if !podReady {
			logger.Info("Pod not ready yet", "pod", pod.Name, "phase", pod.Status.Phase)
			return false
		}
	}

	logger.Info("All pods are ready")
	return true
}

func (r *Neo4jPluginReconciler) configurePlugin(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	logger := log.FromContext(ctx)

	// Check if plugin requires automatic security configuration even without user config
	needsAutoSecurity := r.requiresAutomaticSecurityConfiguration(plugin.Spec.Name)

	if len(plugin.Spec.Config) == 0 && plugin.Spec.Security == nil && !needsAutoSecurity {
		logger.Info("No plugin configuration provided")
		return nil
	}

	// For plugins that require environment variable configuration only
	// APOC settings are no longer supported in neo4j.conf in Neo4j 5.26+
	if r.isEnvironmentVariableOnlyPlugin(plugin.Spec.Name) {
		logger.Info("Plugin configuration handled via environment variables only", "plugin", plugin.Spec.Name)
		return r.applyAPOCSecurityConfiguration(ctx, plugin, deployment)
	}

	// For other plugins that require Neo4j client configuration
	neo4jClientConfig := r.filterNeo4jClientConfig(plugin.Spec.Config)
	if len(neo4jClientConfig) == 0 && plugin.Spec.Security == nil && !needsAutoSecurity {
		logger.Info("No Neo4j client configuration required")
		return nil
	}

	// Create Neo4j client based on deployment type
	var neo4jClient *neo4jclient.Client
	var err error

	if deployment.Type == "cluster" {
		cluster, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseCluster)
		if !ok {
			return fmt.Errorf("deployment typed as cluster but object is %T", deployment.Object)
		}
		neo4jClient, err = neo4jclient.NewClientForEnterprise(cluster, r.Client, getClusterAdminSecretName(cluster))
	} else {
		standalone, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseStandalone)
		if !ok {
			return fmt.Errorf("deployment typed as standalone but object is %T", deployment.Object)
		}
		neo4jClient, err = neo4jclient.NewClientForEnterpriseStandalone(standalone, r.Client, getStandaloneAdminSecretName(standalone))
	}

	if err != nil {
		return fmt.Errorf("failed to create Neo4j client: %w", err)
	}
	defer neo4jClient.Close()

	// Configure plugin settings that should go through Neo4j configuration system
	for key, value := range neo4jClientConfig {
		if err := neo4jClient.SetConfiguration(ctx, key, value); err != nil {
			return fmt.Errorf("failed to set configuration %s=%s: %w", key, value, err)
		}
	}

	// Apply security configuration if specified
	// Only apply runtime security configuration for settings that are dynamic
	if plugin.Spec.Security != nil && r.hasRuntimeSecurityConfiguration(plugin.Spec.Security) {
		if err := r.applySecurityConfiguration(ctx, neo4jClient, plugin); err != nil {
			return fmt.Errorf("failed to apply security configuration: %w", err)
		}
	}

	logger.Info("Plugin configuration applied successfully")
	return nil
}

func (r *Neo4jPluginReconciler) applySecurityConfiguration(ctx context.Context, neo4jClient *neo4jclient.Client, plugin *neo4jv1beta1.Neo4jPlugin) error {
	logger := log.FromContext(ctx)

	// In Neo4j 5.26+, most security settings (allowlist, denylist, unrestricted) are non-dynamic
	// and must be applied as environment variables at startup, not at runtime
	logger.Info("Skipping runtime security configuration - all security settings are applied as environment variables in Neo4j 5.26+")

	// Future: If there are any dynamic security settings that can be applied at runtime,
	// add them here. Currently, all common security settings are non-dynamic.

	return nil
}

// uninstallPlugin takes the plugin off its target: everything it wrote to the
// target's StatefulSet, in one write (removePluginFromTarget, operations.md id
// 155) — its settings, its tokens in the procedure lists, its NEO4J_PLUGINS
// names in Managed mode, its VerifiedDownload init container and volumes — and
// nothing the target or another plugin needs.
//
// A Managed plugin's JAR goes with its NEO4J_PLUGINS entry: /plugins is a
// per-pod EmptyDir (the "plugins" volume in internal/resources/cluster.go) that
// the Neo4j Docker entrypoint fills from NEO4J_PLUGINS at every container
// start, so the restart this write causes leaves it out. An earlier uninstall
// launched a `rm -f /plugins/<name>*.jar` Job, which could not work (it deleted
// files in an EmptyDir of its own) and wedged deletion. A PreBaked plugin's JAR
// is the user's image's, and NEO4J_PLUGINS is left alone.
func (r *Neo4jPluginReconciler) uninstallPlugin(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin) error {
	logger := log.FromContext(ctx)

	deployment, err := r.getTargetDeployment(ctx, plugin)
	if err != nil {
		// Deployment gone → nothing to uninstall from. Succeed so the finalizer
		// releases instead of wedging on a target that no longer exists.
		logger.Info("Target deployment not found, considering plugin uninstalled")
		return nil //nolint:nilerr // intentional: missing target means already uninstalled
	}
	if err := r.removePluginFromTarget(ctx, plugin, deployment); err != nil {
		return fmt.Errorf("failed to remove the plugin from %s: %w", targetName(deployment), err)
	}
	logger.Info("Plugin uninstalled successfully")
	return nil
}

// updatePluginStatus writes the phase, message and Ready condition, and the
// installed version on reaching Ready. It reports whether the write changed
// the phase, the message or the Ready condition's status or reason — a state
// transition — from the attempt whose write succeeded, so a caller can emit
// the matching event once, on the transition.
func (r *Neo4jPluginReconciler) updatePluginStatus(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, phase, message string) bool {
	// The version this reconcile installed: what spec.version asked for when
	// the install ran (not whatever the spec says by the time the status write
	// lands).
	installedVersion := plugin.Spec.Version

	transitioned := false
	update := func() error {
		transitioned = false
		latest := &neo4jv1beta1.Neo4jPlugin{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(plugin), latest); err != nil {
			return err
		}
		// status.installedVersion / installationTime are recorded when the
		// plugin reaches Ready, and only then: a plugin that is still
		// Installing or has Failed keeps reporting the last install that
		// worked. A different version is a new installation (it is
		// reinstalled), so the time follows the version; the same version
		// across Ready -> Installing -> Ready blips keeps its original time.
		// Also true for a plugin that was already Ready before these fields
		// existed, which is why it takes part in the "nothing changed" check
		// below rather than being treated as settled.
		recordInstall := phase == neo4jv1beta1.PhaseReady &&
			(latest.Status.InstalledVersion != installedVersion || latest.Status.InstallationTime == nil)

		// A write that changes nothing still produces a watch event, which
		// reconciles again, which writes again: the CR was observed being
		// rewritten roughly once a second in steady state. Nothing to say is
		// a valid outcome.
		if latest.Status.Phase == phase &&
			latest.Status.Message == message &&
			latest.Status.ObservedGeneration == latest.Generation &&
			!recordInstall {
			return nil
		}
		changed := latest.Status.Phase != phase || latest.Status.Message != message
		latest.Status.Phase = phase
		latest.Status.Message = message
		latest.Status.ObservedGeneration = latest.Generation
		if recordInstall {
			now := metav1.Now()
			latest.Status.InstalledVersion = installedVersion
			latest.Status.InstallationTime = &now
		}
		condStatus, condReason := PhaseToConditionStatus(phase)
		if SetReadyCondition(&latest.Status.Conditions, latest.Generation, condStatus, condReason, message) {
			changed = true
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		transitioned = changed
		return nil
	}
	err := retry.RetryOnConflict(retry.DefaultBackoff, update)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to update plugin status")
		return false
	}
	return transitioned
}

// installPluginViaEnvironment writes the plugin onto its target's StatefulSet,
// in one update: its env edits (NEO4J_PLUGINS in Managed mode — the approach
// Neo4j recommends for its Docker image — and its settings) and, in
// VerifiedDownload mode, its checksum-verifying init container and the volumes
// that container reads (applyPluginToTemplate). A pass whose edits change
// nothing writes nothing.
func (r *Neo4jPluginReconciler) installPluginViaEnvironment(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	_, err := r.writePluginToTarget(ctx, plugin, deployment)
	return err
}

// writePluginToTarget is installPluginViaEnvironment, and returns the
// StatefulSet generation its write created (0 when it wrote nothing), so the
// pass can wait for that generation's roll: the cached StatefulSet it reads
// next may not have seen the write yet, and reads as rolled out.
func (r *Neo4jPluginReconciler) writePluginToTarget(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (int64, error) {
	logger := log.FromContext(ctx)
	switch {
	case isVerifiedDownloadInstallMode(plugin):
		logger.Info("Configuring plugin in VerifiedDownload mode — injecting checksum-verifying init container, skipping NEO4J_PLUGINS mutation", "plugin", plugin.Spec.Name)
	case isPreBakedInstallMode(plugin):
		logger.Info("Configuring plugin in PreBaked mode — skipping NEO4J_PLUGINS mutation", "plugin", plugin.Spec.Name)
	default:
		logger.Info("Installing plugin via NEO4J_PLUGINS environment variable", "plugin", plugin.Spec.Name)
	}

	stsKey := types.NamespacedName{
		Name:      r.getStatefulSetName(deployment),
		Namespace: deployment.Namespace,
	}

	// Update the StatefulSet with retry on conflict
	var written int64
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Fetch latest version of StatefulSet for each retry
		currentSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, stsKey, currentSts); err != nil {
			return err
		}

		// Apply the plugin's edits to the current StatefulSet, and its record
		// of the variables it sets (operations.md id 155).
		base := currentSts.DeepCopy()
		before := &base.Spec.Template
		if _, err := r.applyPlugin(ctx, currentSts, plugin, deployment); err != nil {
			return err
		}
		if equality.Semantic.DeepEqual(*before, currentSts.Spec.Template) {
			// Everything the plugin sets is there already. Writing would change
			// nothing, and the passes that wait for the servers to restart would
			// only race the writes of the StatefulSet's own controller. A record
			// that changed is written alone: metadata, no restart.
			if stringMapsEqual(base.Annotations, currentSts.Annotations) {
				return nil
			}
			return r.Patch(ctx, currentSts, mergePatchSince(base))
		}
		if deployment.Type == "cluster" {
			// The cluster controller puts its own value back on a variable it
			// renders from the cluster's spec, which restarts the servers, and
			// the plugin's next pass would change it again. Say so instead of
			// starting that tug of war.
			changed := changedEnvNames(neo4jContainerEnv(before.Spec), neo4jContainerEnv(currentSts.Spec.Template.Spec))
			if owned := ownedEnvNames(changed, readOwnedEnvVarNames(currentSts)); len(owned) > 0 {
				return &pluginEnvOwnedError{statefulSet: currentSts.Name, cluster: deployment.Name, names: owned}
			}
		}

		if err := r.Update(ctx, currentSts); err != nil {
			return err
		}
		// The API server's answer: the generation the template write moved
		// the StatefulSet to.
		written = max(currentSts.Generation, 1)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to update StatefulSet with plugin configuration: %w", err)
	}

	if written > 0 {
		logger.Info("Successfully updated StatefulSet with plugin configuration", "plugin", plugin.Spec.Name, "generation", written)
	}

	// Note: ConfigMap updates for standalone deployments are now handled earlier in reconcile flow
	// before connectivity checks to ensure security settings are applied before Neo4j starts

	return written, nil
}

// applyPluginToTemplate makes on template, the target's pod template, every
// edit the plugin owns: its env edits on the neo4j container (applyPluginEnv)
// and, in VerifiedDownload mode, its init container and volumes
// (applyVerifiedDownload). installPluginViaEnvironment writes what it
// produces; missingFromTarget and installChangesTarget compare against it, so
// the three cannot disagree about what the plugin needs.
func (r *Neo4jPluginReconciler) applyPluginToTemplate(template *corev1.PodTemplateSpec, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	container := neo4jContainer(&template.Spec)
	if container == nil {
		return fmt.Errorf("neo4j container not found in StatefulSet")
	}
	if err := r.applyPluginEnv(container, plugin, deployment); err != nil {
		return err
	}
	// VerifiedDownload mode: the init container puts the verified JAR in
	// /plugins before Neo4j starts. This is the only code path that emits
	// init containers; PreBaked relies on the user's custom image carrying
	// the JAR, and Managed delegates to the entrypoint.
	if isVerifiedDownloadInstallMode(plugin) {
		r.applyVerifiedDownload(template, plugin, deployment)
	}
	return nil
}

// neo4jContainer returns the pod's neo4j container, or nil.
func neo4jContainer(pod *corev1.PodSpec) *corev1.Container {
	for i := range pod.Containers {
		if pod.Containers[i].Name == "neo4j" {
			return &pod.Containers[i]
		}
	}
	return nil
}

// neo4jContainerEnv returns the env of the pod's neo4j container.
func neo4jContainerEnv(pod corev1.PodSpec) []corev1.EnvVar {
	if c := neo4jContainer(&pod); c != nil {
		return c.Env
	}
	return nil
}

// applyPluginEnv makes on container, the target's neo4j container, every env
// edit the plugin owns: its names in NEO4J_PLUGINS (Managed mode), its
// spec.config settings, and its security settings (unioned into a cluster's
// env; kept out of a standalone's). installPluginViaEnvironment writes what it
// produces and missingFromTarget compares against it, so the two cannot
// disagree about what the plugin needs.
func (r *Neo4jPluginReconciler) applyPluginEnv(container *corev1.Container, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	pluginName := r.mapPluginName(plugin.Spec.Name)
	pluginsToInstall := []string{pluginName}
	for _, dep := range plugin.Spec.Dependencies {
		depName := r.mapPluginName(dep.Name)
		pluginsToInstall = append(pluginsToInstall, depName)
	}

	if !shouldSkipNeo4jPluginsEnv(plugin) {
		// Find existing NEO4J_PLUGINS environment variable or create new one
		var pluginsEnvVar *corev1.EnvVar
		for i := range container.Env {
			if container.Env[i].Name == "NEO4J_PLUGINS" {
				pluginsEnvVar = &container.Env[i]
				break
			}
		}
		if pluginsEnvVar == nil {
			// Add new NEO4J_PLUGINS environment variable with all plugins
			var quotedPlugins []string
			for _, plugin := range pluginsToInstall {
				quotedPlugins = append(quotedPlugins, fmt.Sprintf("\"%s\"", plugin))
			}
			container.Env = append(container.Env, corev1.EnvVar{
				Name:  "NEO4J_PLUGINS",
				Value: fmt.Sprintf("[%s]", strings.Join(quotedPlugins, ",")),
			})
		} else {
			// Update existing NEO4J_PLUGINS — delegate to the same helper used
			// in installPluginViaEnvironment's outer block so the merge is
			// JSON-aware, idempotent, and bug-free. A previous inline
			// implementation read `currentValue` once but then mutated only
			// `pluginsEnvVar.Value`, leaving `currentValue` stale across loop
			// iterations — only the first plugin in pluginsToInstall would
			// actually land in the env var.
			currentValue := pluginsEnvVar.Value
			for _, plugin := range pluginsToInstall {
				updated, err := r.addPluginToList(currentValue, plugin)
				if err != nil {
					return fmt.Errorf("failed to update plugin list: %w", err)
				}
				currentValue = updated
			}
			pluginsEnvVar.Value = currentValue
		}
	}

	// A standalone carries a plugin's security and startup-only settings in
	// neo4j.conf, merged there with the user's own spec.config by the
	// standalone controller (#146). The image applies NEO4J_* env vars over
	// neo4j.conf, so an env copy of such a key replaced that merged value —
	// dropping the user's own procedure allowlist. Those keys stay out of a
	// standalone's env; the plugin's other settings (APOC's, …) stay in it.
	standalone := deployment.Type == "standalone"

	// Add plugin-specific configuration as environment variables
	for key, value := range plugin.Spec.Config {
		if standalone && pluginConfKey(key) {
			continue
		}
		envVarName := resources.Neo4jSettingEnvVarName(key)
		// Check if environment variable already exists
		exists := false
		for i := range container.Env {
			if container.Env[i].Name == envVarName {
				container.Env[i].Value = value
				exists = true
				break
			}
		}
		if !exists {
			container.Env = append(container.Env, corev1.EnvVar{
				Name:  envVarName,
				Value: value,
			})
		}
	}

	if standalone {
		// …and env copies an earlier operator wrote are removed (one roll).
		container.Env = removeStandalonePluginConfEnv(container.Env, plugin, r.pluginSecurityRemovalSettings(plugin))
	} else {
		// Apply security settings as environment variables, unioning additive
		// allowlists across plugins (see mergePluginSecurityEnv).
		container.Env = mergePluginSecurityEnv(container.Env, r.pluginSecuritySettings(plugin))
	}
	return nil
}

// pluginTargetDrift is what a settled plugin puts right on its target's
// StatefulSet; "" for each when there is nothing.
type pluginTargetDrift struct {
	// missing: what the StatefulSet no longer carries — something removed it.
	missing string
	// outdated: what the plugin wrote that no longer follows the target's
	// spec.trustedCASecrets — the user changed the list (operations.md id 147).
	outdated string
	// removing: variables the plugin recorded and no longer sets, still on the
	// StatefulSet (operations.md id 155). A write records what the plugin sets
	// and removes the rest, so a settled plugin has none unless what it owns
	// changed without an edit of its CR (an operator upgrade).
	removing string
}

// targetDrift reports what a settled plugin puts right on its target's
// StatefulSet: the plugin's edits (applyPlugin) are made on a copy of it, and
// pluginEnvDrift and pluginPodDrift decide which of them count.
func (r *Neo4jPluginReconciler) targetDrift(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (pluginTargetDrift, error) {
	stsName := r.getStatefulSetName(deployment)
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: deployment.Namespace}, sts); err != nil {
		if errors.IsNotFound(err) {
			return pluginTargetDrift{missing: fmt.Sprintf("StatefulSet %s does not exist", stsName)}, nil
		}
		return pluginTargetDrift{}, fmt.Errorf("failed to get StatefulSet %s: %w", stsName, err)
	}
	if neo4jContainer(&sts.Spec.Template.Spec) == nil {
		return pluginTargetDrift{}, fmt.Errorf("neo4j container not found in StatefulSet %s", stsName)
	}
	desired := sts.DeepCopy()
	removed, err := r.applyPlugin(ctx, desired, plugin, deployment)
	if err != nil {
		return pluginTargetDrift{}, err
	}
	var out pluginTargetDrift
	drift := slices.DeleteFunc(
		pluginEnvDrift(neo4jContainerEnv(sts.Spec.Template.Spec), neo4jContainerEnv(desired.Spec.Template.Spec), deployment.Type == "standalone"),
		func(name string) bool { return slices.Contains(removed, name) })
	podMissing, podOutdated := pluginPodDrift(sts.Spec.Template.Spec, desired.Spec.Template.Spec)
	drift = append(drift, podMissing...)
	if len(drift) > 0 {
		out.missing = fmt.Sprintf("StatefulSet %s does not hold what the plugin sets in %s", stsName, strings.Join(drift, ", "))
	}
	if len(podOutdated) > 0 {
		out.outdated = fmt.Sprintf("spec.trustedCASecrets of %s changed (%s of StatefulSet %s)",
			targetName(deployment), strings.Join(podOutdated, ", "), stsName)
	}
	if len(removed) > 0 {
		out.removing = fmt.Sprintf("%s on StatefulSet %s", strings.Join(removed, ", "), stsName)
	}
	return out, nil
}

// missingFromTarget is targetDrift's missing and outdated.
func (r *Neo4jPluginReconciler) missingFromTarget(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (missing, outdated string, err error) {
	drift, err := r.targetDrift(ctx, plugin, deployment)
	return drift.missing, drift.outdated, err
}

// pluginPodDrift compares live, the target's pod spec, with desired, the same
// after the plugin's edits, for what a VerifiedDownload plugin adds:
//   - missing: an init container or volume of the plugin's that live does not
//     have — what something removed: a StatefulSet recreated from the
//     cluster's own template (storage expansion does that), or an earlier
//     operator's cluster controller, which dropped them on every apply.
//   - outdated: what follows the target's spec.trustedCASecrets — the shared
//     plugin-ca volume (projecting other CAs than the list, added because the
//     list is no longer empty, or going because it is), and an init container
//     whose mount of that volume comes or goes with it.
//
// Nothing else counts, as with env vars: the live init container carries the
// API server's defaults, and a field someone else changed (its image, say) is
// left alone. A plugin-ca volume absent while the plugin's init container is
// there was never written (the list was empty); absent with it, it was removed.
func pluginPodDrift(live, desired corev1.PodSpec) (missing, outdated []string) {
	initContainers := make(map[string]corev1.Container, len(live.InitContainers))
	for _, c := range live.InitContainers {
		initContainers[c.Name] = c
	}
	for _, c := range desired.InitContainers {
		have, ok := initContainers[c.Name]
		switch {
		case !ok:
			missing = append(missing, "init container "+c.Name)
		case containerMountsVolume(have, resources.PluginCAVolumeName) != containerMountsVolume(c, resources.PluginCAVolumeName):
			outdated = append(outdated, "init container "+c.Name)
		}
	}
	initContainerMissing := len(missing) > 0
	for _, v := range desired.Volumes {
		i := volumeIndex(&live, v.Name)
		switch {
		case v.Name != resources.PluginCAVolumeName && i < 0:
			missing = append(missing, "volume "+v.Name)
		case v.Name != resources.PluginCAVolumeName:
		case i < 0 && initContainerMissing:
			missing = append(missing, "volume "+v.Name)
		case i < 0 || !sameTrustedCAs(live.Volumes[i], v):
			outdated = append(outdated, "volume "+v.Name)
		}
	}
	if podSpecHasVolume(&live, resources.PluginCAVolumeName) && !podSpecHasVolume(&desired, resources.PluginCAVolumeName) {
		outdated = append(outdated, "volume "+resources.PluginCAVolumeName)
	}
	return missing, outdated
}

// pluginEnvDrift returns, sorted, the variables a settled plugin puts right on
// its target (live is the neo4j container's env, desired the same after the
// plugin's edits):
//   - one it sets that is absent;
//   - NEO4J_PLUGINS when it no longer lists a name the plugin needs (the merge
//     only ever adds names, so a different value is a missing name);
//   - on a standalone, a copy of one of the plugin's neo4j.conf settings that
//     an earlier operator left in the env (operations.md id 114).
//
// A value someone else gave a variable the plugin sets is left alone. Two
// writers that disagree on it — two plugins setting one key, or one whose
// spec.config overwrites another's list — would otherwise restart the servers
// in turn for ever, each pass woken by the other's write.
func pluginEnvDrift(live, desired []corev1.EnvVar, standalone bool) []string {
	have, want := envByName(live), envByName(desired)
	var drift []string
	for _, name := range changedEnvNames(live, desired) {
		_, inLive := have[name]
		_, inDesired := want[name]
		// On a standalone the plugin only ever takes its tokens out of a
		// list (no plugin writes one into a standalone's env), so a change
		// there cannot be fought over.
		if inLive && inDesired && name != "NEO4J_PLUGINS" &&
			(!standalone || !resources.IsAdditiveConfEnvName(name)) {
			continue
		}
		drift = append(drift, name)
	}
	return drift
}

// changedEnvNames returns, sorted, the names whose variable differs between
// before and after: added, removed, or given another value.
func changedEnvNames(before, after []corev1.EnvVar) []string {
	was, is := envByName(before), envByName(after)
	var changed []string
	for name, now := range is {
		if then, ok := was[name]; !ok || !equality.Semantic.DeepEqual(then, now) {
			changed = append(changed, name)
		}
	}
	for name := range was {
		if _, ok := is[name]; !ok {
			changed = append(changed, name)
		}
	}
	sort.Strings(changed)
	return changed
}

// envByName indexes env vars by name; of a repeated name the last one wins,
// as in the container.
func envByName(env []corev1.EnvVar) map[string]corev1.EnvVar {
	out := make(map[string]corev1.EnvVar, len(env))
	for _, e := range env {
		out[e.Name] = e
	}
	return out
}

// ownedEnvNames returns the names in names that owned lists.
func ownedEnvNames(names []string, owned map[string]struct{}) []string {
	var out []string
	for _, name := range names {
		if _, ok := owned[name]; ok {
			out = append(out, name)
		}
	}
	return out
}

// pluginEnvOwnedError: the plugin would change variables that the cluster
// controller renders from the cluster's own spec (its
// neo4j.com/cluster-controller-env-vars annotation lists them). That
// controller puts its own value back whenever the live one differs, so writing
// would only restart the servers twice, and then again at the plugin's next
// pass.
type pluginEnvOwnedError struct {
	statefulSet string
	cluster     string
	names       []string
}

func (e *pluginEnvOwnedError) Error() string {
	return fmt.Sprintf("%s on StatefulSet %s comes from Neo4jEnterpriseCluster %s itself (spec.env, or a setting it renders), "+
		"which puts its own value back: set what the plugin needs there, or remove the variable from the cluster's spec",
		strings.Join(e.names, ", "), e.statefulSet, e.cluster)
}

// mergePluginSecurityEnv applies a plugin's security settings to a container's
// env vars without clobbering across plugins. For additive list keys
// (resources.IsAdditiveConfKey — e.g. dbms.security.procedures.unrestricted /
// allowlist), the value is UNIONED with any existing env value, so GDS's `gds.*`
// and APOC's `apoc.*` both survive instead of the last-reconciled plugin
// overwriting the first. Scalar keys are set in place. Deterministic (keys
// processed sorted) and idempotent (re-applying the same settings is a no-op).
//
// NOTE: this accumulates tokens; tokens from an *uninstalled* plugin are not
// pruned here — that needs the authoritative recompute tracked in issue #146.
// pluginSecuritySettings returns the Neo4j security settings a plugin
// contributes (automatic per-plugin defaults plus any user-provided
// spec.security). Used both when applying settings at install and when removing
// them on uninstall, so the two paths can't diverge.
func (r *Neo4jPluginReconciler) pluginSecuritySettings(plugin *neo4jv1beta1.Neo4jPlugin) map[string]string {
	settings := r.getAutomaticSecuritySettings(plugin.Spec.Name)
	for k, v := range specSecuritySettings(plugin) {
		settings[k] = v
	}
	return settings
}

// pluginSecurityRemovalSettings returns the additive-key token set to subtract on
// uninstall: the UNION of the plugin's automatic per-name defaults and its
// current spec.security override. The merge path (mergePluginSecurityEnv) unions
// every reconcile's pluginSecuritySettings into the env, but pluginSecuritySettings
// *replaces* unrestricted/allowlist with the custom list when allowedProcedures is
// set — so a plugin installed with defaults and later given an override leaves the
// automatic tokens (e.g. gds.*,apoc.load.*) accumulated in env that the current
// spec no longer mentions. Always subtracting the automatic defaults too means
// they can never be orphaned, regardless of spec toggling.
//
// This closes the common orphaned-defaults case. The fully general gap — a plugin
// whose custom allowedProcedures was changed to several *different* values over its
// lifetime — still needs per-plugin token ownership tracking, deferred to #146,
// because only the current custom value is knowable here.
func (r *Neo4jPluginReconciler) pluginSecurityRemovalSettings(plugin *neo4jv1beta1.Neo4jPlugin) map[string]string {
	auto := r.getAutomaticSecuritySettings(plugin.Spec.Name)
	out := make(map[string]string, len(auto))
	for k, v := range auto {
		out[k] = v
	}
	for k, v := range r.pluginSecuritySettings(plugin) {
		if existing, ok := out[k]; ok && resources.IsAdditiveConfKey(k) {
			out[k] = resources.MergeConfListValues(existing, v)
		} else {
			out[k] = v
		}
	}
	return out
}

// removePluginSecurityEnv reverses mergePluginSecurityEnv for a single plugin on
// uninstall: for additive list keys it subtracts that plugin's tokens from the
// env var (dropping the var entirely if nothing remains), so another plugin's
// allowlist is preserved while the uninstalled plugin's entries are pruned.
// Non-additive (scalar) keys are left untouched — they may be shared, and the
// accumulation problem this prunes is specific to the additive lists. Plugin
// token sets are disjoint (gds.*, apoc.*, …), so subtraction is exact.
func removePluginSecurityEnv(env []corev1.EnvVar, settings map[string]string) []corev1.EnvVar {
	remove := make(map[string]string) // env var name -> tokens to subtract
	for key, value := range settings {
		if resources.IsAdditiveConfKey(key) {
			remove[resources.Neo4jSettingEnvVarName(key)] = value
		}
	}
	if len(remove) == 0 {
		return env
	}

	out := env[:0:0]
	for _, e := range env {
		if toks, ok := remove[e.Name]; ok {
			e.Value = subtractCSV(e.Value, toks)
			if e.Value == "" {
				continue // nothing left for this key — drop it
			}
		}
		out = append(out, e)
	}
	return out
}

// removeStandalonePluginConfEnv removes from a standalone's env the copies of
// a plugin's neo4j.conf settings that earlier operators wrote there: this
// plugin's tokens from the additive lists (removePluginSecurityEnv), and a
// scalar setting only where the env still holds exactly the plugin's value —
// a value someone else set is left alone.
func removeStandalonePluginConfEnv(env []corev1.EnvVar, plugin *neo4jv1beta1.Neo4jPlugin, removal map[string]string) []corev1.EnvVar {
	conf := pluginConfSettings(plugin)
	tokens := make(map[string]string, len(removal)+len(conf))
	for k, v := range removal {
		tokens[k] = v
	}
	scalar := make(map[string]string)
	for k, v := range conf {
		if !resources.IsAdditiveConfKey(k) {
			scalar[resources.Neo4jSettingEnvVarName(k)] = v
			continue
		}
		if existing, ok := tokens[k]; ok {
			tokens[k] = resources.MergeConfListValues(existing, v)
		} else {
			tokens[k] = v
		}
	}
	env = removePluginSecurityEnv(env, tokens)
	out := env[:0:0]
	for _, e := range env {
		if v, ok := scalar[e.Name]; ok && e.ValueFrom == nil && e.Value == v {
			continue
		}
		out = append(out, e)
	}
	return out
}

// subtractCSV returns value's comma-separated tokens with remove's tokens taken
// out, preserving order.
func subtractCSV(value, remove string) string {
	rm := make(map[string]bool)
	for _, t := range strings.Split(remove, ",") {
		if t = strings.TrimSpace(t); t != "" {
			rm[t] = true
		}
	}
	var keep []string
	for _, t := range strings.Split(value, ",") {
		if t = strings.TrimSpace(t); t != "" && !rm[t] {
			keep = append(keep, t)
		}
	}
	return strings.Join(keep, ",")
}

func mergePluginSecurityEnv(env []corev1.EnvVar, settings map[string]string) []corev1.EnvVar {
	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		envName := resources.Neo4jSettingEnvVarName(key)
		idx := -1
		for i := range env {
			if env[i].Name == envName {
				idx = i
				break
			}
		}
		switch {
		case idx >= 0 && resources.IsAdditiveConfKey(key):
			env[idx].Value = resources.MergeConfListValues(env[idx].Value, settings[key])
		case idx >= 0:
			env[idx].Value = settings[key]
		default:
			env = append(env, corev1.EnvVar{Name: envName, Value: settings[key]})
		}
	}
	return env
}

// applyVerifiedDownload adds (or updates) the plugin's init container on the
// target's pod template, plus the matching auth/CA volumes. Ownership-tracked
// via the PluginInitContainersAnnotation so the controller can remove its own
// containers on uninstall without disturbing foreign ones; the cluster
// controller reads the same annotation on a StatefulSet that has no record of
// its own init containers yet (readOwnedTemplateItems).
//
// The live init container carries what the API server defaulted
// (terminationMessagePath, …), so it is replaced only when a field the plugin
// sets differs from it: replacing it on every pass made each one a change, and
// a change is a write and a restart of the servers.
func (r *Neo4jPluginReconciler) applyVerifiedDownload(template *corev1.PodTemplateSpec, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) {
	caSecrets := deploymentTrustedCASecrets(deployment)
	desired := resources.BuildPluginVerifiedDownloadInitContainer(plugin, r.PluginInitImage, caSecrets)
	authVol := resources.BuildPluginAuthVolume(plugin)
	caVol := resources.BuildPluginCAVolume(caSecrets)

	podSpec := &template.Spec

	// Replace existing init container of the same name; otherwise append.
	// Same name is the source of truth — Kubernetes itself enforces
	// uniqueness, and our annotation tracks ownership for cleanup.
	found := false
	for i := range podSpec.InitContainers {
		if podSpec.InitContainers[i].Name != desired.Name {
			continue
		}
		found = true
		if !equality.Semantic.DeepDerivative(desired, podSpec.InitContainers[i]) {
			podSpec.InitContainers[i] = desired
		}
		break
	}
	if !found {
		podSpec.InitContainers = append(podSpec.InitContainers, desired)
	}

	// Add the plugin's own auth volume if not already present; the CA
	// volume, which every VerifiedDownload plugin of the target shares,
	// follows the target's spec.trustedCASecrets.
	if authVol != nil && !podSpecHasVolume(podSpec, authVol.Name) {
		podSpec.Volumes = append(podSpec.Volumes, *authVol)
	}
	setPluginCAVolume(podSpec, caVol)

	// Record ownership in the annotation so the uninstall path can
	// remove only our init containers. The annotation lives on the
	// PodTemplate annotation map so it travels with the spec
	// fingerprint and triggers a rolling restart on change.
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	owned := parsePluginInitOwned(template.Annotations[PluginInitContainersAnnotation])
	if _, exists := owned[desired.Name]; !exists {
		owned[desired.Name] = struct{}{}
		template.Annotations[PluginInitContainersAnnotation] = formatPluginInitOwned(owned)
	}
}

// deploymentTrustedCASecrets is the target's spec.trustedCASecrets. The CA
// bundle a VerifiedDownload init container trusts comes from the owning
// cluster/standalone: the JAR fetch typically goes to an internal Artifactory
// using the same corporate CA the rest of the deployment already trusts.
func deploymentTrustedCASecrets(deployment *DeploymentInfo) []neo4jv1beta1.TrustedCASecret {
	switch deployment.Type {
	case "cluster":
		if c, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok {
			return c.Spec.TrustedCASecrets
		}
	case "standalone":
		if s, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseStandalone); ok {
			return s.Spec.TrustedCASecrets
		}
	}
	return nil
}

// setPluginCAVolume brings the plugin-ca volume of podSpec in line with
// desired, the one BuildPluginCAVolume makes from the target's
// spec.trustedCASecrets (nil when it lists none). The plugin controller is
// the volume's only writer — the cluster and standalone controllers keep it as
// a volume they did not render — and every VerifiedDownload plugin of the
// target builds the same one, so whichever plugin passes first updates it and
// the rest find nothing to change.
//
//   - Missing and wanted: added.
//   - Present, projecting other Secrets, keys or paths than desired: replaced.
//     Nothing else counts (sameTrustedCAs), so a volume the API server
//     defaulted, or one an earlier operator wrote without optional
//     projections, is left alone while the list stays the same: rewriting it
//     would restart the servers for nothing.
//   - Present and no longer wanted: removed once no container mounts it (the
//     caller has rebuilt this plugin's init container without the mount by
//     then). Another plugin's init container that still mounts it keeps it
//     until that plugin's pass rebuilds its own.
func setPluginCAVolume(podSpec *corev1.PodSpec, desired *corev1.Volume) {
	i := volumeIndex(podSpec, resources.PluginCAVolumeName)
	switch {
	case desired == nil:
		if i >= 0 && !podSpecMountsVolume(podSpec, resources.PluginCAVolumeName) {
			podSpec.Volumes = append(podSpec.Volumes[:i:i], podSpec.Volumes[i+1:]...)
		}
	case i < 0:
		podSpec.Volumes = append(podSpec.Volumes, *desired)
	case !sameTrustedCAs(podSpec.Volumes[i], *desired):
		podSpec.Volumes[i] = *desired
	}
}

// sameTrustedCAs reports whether two plugin-ca volumes project the same CA
// files: the same Secret, key and path for each, in any order.
func sameTrustedCAs(a, b corev1.Volume) bool {
	return slices.Equal(trustedCAProjections(a), trustedCAProjections(b))
}

// trustedCAProjections lists, sorted, what a plugin-ca volume projects as
// "secret/key/path". A source other than a Secret — which the plugin never
// writes — is listed as "", so a volume holding one is never the plugin's.
func trustedCAProjections(v corev1.Volume) []string {
	if v.Projected == nil {
		return []string{""}
	}
	var out []string
	for _, source := range v.Projected.Sources {
		if source.Secret == nil {
			out = append(out, "")
			continue
		}
		if len(source.Secret.Items) == 0 {
			out = append(out, source.Secret.Name+"/")
		}
		for _, item := range source.Secret.Items {
			out = append(out, source.Secret.Name+"/"+item.Key+"/"+item.Path)
		}
	}
	sort.Strings(out)
	return out
}

// podSpecHasVolume reports whether a volume of the given name is
// already declared on the PodSpec. Used to avoid duplicate Volume
// entries when multiple VerifiedDownload plugins land on the same
// StatefulSet over successive reconciles.
func podSpecHasVolume(podSpec *corev1.PodSpec, name string) bool {
	return volumeIndex(podSpec, name) >= 0
}

// volumeIndex returns the index of the named volume in the pod, or -1.
func volumeIndex(podSpec *corev1.PodSpec, name string) int {
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == name {
			return i
		}
	}
	return -1
}

// podSpecMountsVolume reports whether any container or init container of the
// pod mounts the named volume.
func podSpecMountsVolume(podSpec *corev1.PodSpec, name string) bool {
	for _, list := range [][]corev1.Container{podSpec.InitContainers, podSpec.Containers} {
		for _, c := range list {
			if containerMountsVolume(c, name) {
				return true
			}
		}
	}
	return false
}

// containerMountsVolume reports whether the container mounts the named volume.
func containerMountsVolume(c corev1.Container, name string) bool {
	for _, m := range c.VolumeMounts {
		if m.Name == name {
			return true
		}
	}
	return false
}

// parsePluginInitOwned + formatPluginInitOwned shuttle the comma-separated
// annotation value through a set so add/remove operations are
// order-independent and idempotent. Empty/whitespace tokens are
// skipped.
func parsePluginInitOwned(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func formatPluginInitOwned(set map[string]struct{}) string {
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// mapPluginName maps our plugin names to Neo4j's expected plugin names for NEO4J_PLUGINS
func (r *Neo4jPluginReconciler) mapPluginName(pluginName string) string {
	switch pluginName {
	case "apoc":
		return "apoc"
	case "apoc-extended":
		return "apoc-extended"
	case "graph-data-science", "gds":
		return "graph-data-science"
	case "bloom":
		return "bloom"
	case "genai":
		return "genai"
	case "n10s", "neosemantics":
		return "n10s"
	case "graphql":
		return "graphql"
	case "fleet-management":
		return "fleet-management"
	default:
		return pluginName // Use as-is for custom plugins
	}
}

// isDeploymentFunctional checks if the deployment is actually functional by testing Neo4j connectivity
func (r *Neo4jPluginReconciler) isDeploymentFunctional(ctx context.Context, deployment *DeploymentInfo) bool {
	logger := log.FromContext(ctx)

	// For clusters, try to connect and verify cluster formation
	if deployment.Type == "cluster" {
		cluster, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseCluster)
		if !ok {
			return false
		}
		neo4jClient, err := neo4jclient.NewClientForEnterprise(cluster, r.Client, getClusterAdminSecretName(cluster))
		if err != nil {
			logger.Info("Cannot create Neo4j client", "error", err)
			return false
		}
		defer neo4jClient.Close()

		// Test connectivity by getting server list
		servers, err := neo4jClient.GetServerList(ctx)
		if err != nil {
			logger.Info("Cannot get server list", "error", err)
			return false
		}

		// For clusters, verify we have the expected number of servers
		expectedServers := int(cluster.Spec.Topology.Servers)
		if len(servers) < expectedServers {
			logger.Info("Cluster not fully formed", "expected", expectedServers, "actual", len(servers))
			return false
		}

		logger.Info("Cluster is functional", "servers", len(servers))
		return true
	}

	// For standalone, just check basic connectivity
	if deployment.Type == "standalone" {
		standalone, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseStandalone)
		if !ok {
			return false
		}
		neo4jClient, err := neo4jclient.NewClientForEnterpriseStandalone(standalone, r.Client, getStandaloneAdminSecretName(standalone))
		if err != nil {
			logger.Info("Cannot create Neo4j standalone client", "error", err)
			return false
		}
		defer neo4jClient.Close()

		// Test connectivity by getting server list (should have 1 server)
		servers, err := neo4jClient.GetServerList(ctx)
		if err != nil {
			logger.Info("Cannot get server list for standalone", "error", err)
			return false
		}

		if len(servers) == 0 {
			logger.Info("Standalone instance not ready")
			return false
		}

		logger.Info("Standalone is functional")
		return true
	}

	logger.Info("Unknown deployment type", "type", deployment.Type)
	return false
}

// addPluginToList is a method shim that delegates to the package-level MergeNeo4jPluginList.
func (r *Neo4jPluginReconciler) addPluginToList(existing string, newPlugin string) (string, error) {
	return MergeNeo4jPluginList(existing, newPlugin)
}

// MergeNeo4jPluginList parses the existing NEO4J_PLUGINS JSON array value and adds
// newPlugin if it is not already present. Returns the (possibly unchanged) JSON array.
// Expected format: ["plugin1","plugin2"].
//
// This is intentionally exported so other controllers (e.g. the fleet
// management reconciler) can perform the same idempotent merge without
// duplicating the logic.
//
// Uses encoding/json rather than ad-hoc string trimming so values with
// whitespace, embedded escapes, or unusual but legal JSON formatting
// round-trip correctly. Empty/whitespace input is treated as the empty
// list (the env var is sometimes literally "" before the operator has
// touched it).
func MergeNeo4jPluginList(existing string, newPlugin string) (string, error) {
	var plugins []string
	if strings.TrimSpace(existing) != "" {
		if err := json.Unmarshal([]byte(existing), &plugins); err != nil {
			return "", fmt.Errorf("failed to parse existing NEO4J_PLUGINS as JSON array: %w", err)
		}
	}

	for _, plugin := range plugins {
		if plugin == newPlugin {
			return existing, nil // already present — no change
		}
	}

	plugins = append(plugins, newPlugin)
	merged, err := json.Marshal(plugins)
	if err != nil {
		return "", fmt.Errorf("failed to marshal merged NEO4J_PLUGINS: %w", err)
	}
	return string(merged), nil
}

// RemoveFromNeo4jPluginList is the inverse of MergeNeo4jPluginList: it drops one
// plugin from a NEO4J_PLUGINS JSON array, leaving every other entry — including
// entries other controllers own — untouched.
//
// Returns the input UNCHANGED (byte for byte) when the plugin is not listed, so
// callers can use `result == input` as "nothing to do" and skip a StatefulSet
// Update that would otherwise restart the pods for no reason.
//
// Removing the last entry yields "[]" rather than "", because the Neo4j Docker
// entrypoint treats an explicit empty array as "install nothing" while an empty
// string is ambiguous.
func RemoveFromNeo4jPluginList(existing string, remove string) (string, error) {
	if strings.TrimSpace(existing) == "" {
		return existing, nil
	}

	var plugins []string
	if err := json.Unmarshal([]byte(existing), &plugins); err != nil {
		return "", fmt.Errorf("failed to parse existing NEO4J_PLUGINS as JSON array: %w", err)
	}

	remaining := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		if plugin != remove {
			remaining = append(remaining, plugin)
		}
	}
	if len(remaining) == len(plugins) {
		return existing, nil // not present — signal "no change" to the caller
	}

	pruned, err := json.Marshal(remaining)
	if err != nil {
		return "", fmt.Errorf("failed to marshal pruned NEO4J_PLUGINS: %w", err)
	}
	return string(pruned), nil
}

func (r *Neo4jPluginReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&neo4jv1beta1.Neo4jPlugin{}).
		// What a plugin installs lives on its target's StatefulSet, which the
		// cluster, standalone and fleet controllers write too, and a Ready
		// plugin is not requeued. Without this, a plugin something removed —
		// a conflict retry of the cluster controller once deleted NEO4J_PLUGINS
		// (operations.md id 134) — stayed Ready without the plugin on its
		// servers until the operator restarted.
		Watches(&appsv1.StatefulSet{},
			handler.EnqueueRequestsFromMapFunc(r.readyPluginsForStatefulSet),
			builder.WithPredicates(podTemplateChanged)).
		Complete(r)
}

// readyPluginsForStatefulSet maps a StatefulSet to the Ready Neo4jPlugins in
// its namespace whose target it is: a cluster's {name}-server or a
// standalone's {name}. Only Ready ones: a plugin in any other phase is part way
// through an install and requeues on its own, or waits for an edit to its CR,
// and a pass of those writes the StatefulSet again — enqueuing them on its
// changes would let a controller that reverts the plugin's write restart the
// servers in a loop. (The cluster controller once did that to a
// VerifiedDownload init container, operations.md id 141.)
func (r *Neo4jPluginReconciler) readyPluginsForStatefulSet(ctx context.Context, obj client.Object) []reconcile.Request {
	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := r.List(ctx, plugins, client.InNamespace(obj.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list Neo4jPlugins for a StatefulSet change",
			"statefulSet", obj.GetName(), "namespace", obj.GetNamespace())
		return nil
	}
	var requests []reconcile.Request
	for i := range plugins.Items {
		plugin := &plugins.Items[i]
		if plugin.Status.Phase != neo4jv1beta1.PhaseReady || plugin.Spec.ClusterRef == "" {
			continue
		}
		if obj.GetName() != plugin.Spec.ClusterRef+"-server" && obj.GetName() != plugin.Spec.ClusterRef {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(plugin)})
	}
	return requests
}

// podTemplateChanged passes the StatefulSet events that can take away what a
// plugin wrote: a StatefulSet created (one recreated without it) and a pod
// template changed. Status updates, which come at every pod transition of a
// rollout, do not pass.
var podTemplateChanged = predicate.Funcs{
	CreateFunc: func(event.CreateEvent) bool { return true },
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldSts, okOld := e.ObjectOld.(*appsv1.StatefulSet)
		newSts, okNew := e.ObjectNew.(*appsv1.StatefulSet)
		if !okOld || !okNew {
			return false
		}
		return !equality.Semantic.DeepEqual(oldSts.Spec.Template, newSts.Spec.Template)
	},
	DeleteFunc:  func(event.DeleteEvent) bool { return false },
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// Helper functions

// getStatefulSetName returns the correct StatefulSet name for the deployment type
func (r *Neo4jPluginReconciler) getStatefulSetName(deployment *DeploymentInfo) string {
	if deployment.Type == "cluster" {
		return deployment.Name + "-server"
	}
	// For standalone, the StatefulSet name is the same as the deployment name
	return deployment.Name
}

// getPodLabels returns the appropriate pod labels for the deployment type
func (r *Neo4jPluginReconciler) getPodLabels(deployment *DeploymentInfo) map[string]string {
	if deployment.Type == "cluster" {
		return map[string]string{
			"app.kubernetes.io/name":     "neo4j",
			"app.kubernetes.io/instance": deployment.Name,
		}
	}
	// For standalone
	return map[string]string{
		"app": deployment.Name,
	}
}

// getExpectedReplicas returns the expected number of replicas for the deployment
func (r *Neo4jPluginReconciler) getExpectedReplicas(deployment *DeploymentInfo) int {
	if deployment.Type == "cluster" {
		if cluster, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok {
			return int(cluster.Spec.Topology.Servers)
		}
	}
	// Standalone always has 1 replica
	return 1
}

// PluginType represents different categories of Neo4j plugins
type PluginType int

const (
	PluginTypeEnvironmentOnly PluginType = iota // APOC, APOC Extended - environment variables only
	PluginTypeNeo4jConfig                       // GDS, Bloom, GenAI - neo4j.conf configuration
	PluginTypeHybrid                            // Plugins that use both methods
)

// getPluginType determines the configuration approach for a plugin
func (r *Neo4jPluginReconciler) getPluginType(pluginName string) PluginType {
	switch pluginName {
	case "apoc", "apoc-extended":
		// APOC and APOC Extended in Neo4j 5.26+ use environment variables only
		// APOC-specific settings are no longer supported in neo4j.conf
		return PluginTypeEnvironmentOnly
	case "graph-data-science", "gds":
		// GDS uses neo4j.conf for configuration (gds.*, dbms.security.procedures.*)
		return PluginTypeNeo4jConfig
	case "bloom":
		// Bloom uses neo4j.conf for configuration (dbms.bloom.*, dbms.security.procedures.*)
		return PluginTypeNeo4jConfig
	case "genai":
		// GenAI uses neo4j.conf for configuration and procedure security
		return PluginTypeNeo4jConfig
	case "n10s", "neosemantics":
		// Neo Semantics uses neo4j.conf for configuration
		return PluginTypeNeo4jConfig
	case "graphql":
		// GraphQL plugin uses neo4j.conf for configuration
		return PluginTypeNeo4jConfig
	case "fleet-management":
		// Fleet management plugin requires procedure security configuration.
		// NEO4J_PLUGINS=["fleet-management"] copies the pre-bundled jar from
		// /var/lib/neo4j/products/ — no internet access required.
		return PluginTypeNeo4jConfig
	default:
		// Default to neo4j.conf configuration for unknown plugins
		return PluginTypeNeo4jConfig
	}
}

// isEnvironmentVariableOnlyPlugin checks if the plugin requires environment variable configuration only
func (r *Neo4jPluginReconciler) isEnvironmentVariableOnlyPlugin(pluginName string) bool {
	return r.getPluginType(pluginName) == PluginTypeEnvironmentOnly
}

// requiresAutomaticSecurityConfiguration checks if the plugin needs automatic security settings even without user config
func (r *Neo4jPluginReconciler) requiresAutomaticSecurityConfiguration(pluginName string) bool {
	switch pluginName {
	case "bloom":
		// Bloom requires automatic security configuration:
		// - dbms.security.procedures.unrestricted=bloom.*
		// - dbms.security.http_auth_allowlist=/,/browser.*,/bloom.*
		// - server.unmanaged_extension_classes=com.neo4j.bloom.server=/bloom
		return true
	case "graph-data-science", "gds":
		// GDS requires automatic security configuration:
		// - dbms.security.procedures.unrestricted=gds.* (or allowlist based on sandbox setting)
		return true
	case "fleet-management":
		// Fleet management requires automatic security configuration:
		// - dbms.security.procedures.unrestricted=fleetManagement.*
		// - dbms.security.procedures.allowlist=fleetManagement.*
		return true
	default:
		return false
	}
}

// getAutomaticSecuritySettings returns the security settings a plugin requires.
// Delegates to the shared helper so the env-var path and the standalone
// ConfigMap owner (issue #146) agree on the exact set.
func (r *Neo4jPluginReconciler) getAutomaticSecuritySettings(pluginName string) map[string]string {
	return automaticPluginSecuritySettings(pluginName)
}

// filterNeo4jClientConfig filters out plugin-specific configurations that should be handled via environment variables
func (r *Neo4jPluginReconciler) filterNeo4jClientConfig(config map[string]string) map[string]string {
	filtered := make(map[string]string)

	for key, value := range config {
		// Skip APOC-specific settings - these must be handled via environment variables only in Neo4j 5.26+
		if strings.HasPrefix(key, "apoc.") {
			continue
		}

		// Skip non-dynamic GDS settings - these can only be set at startup, not runtime
		if r.isNonDynamicSetting(key) {
			continue
		}

		// Include settings that can be applied dynamically through Neo4j configuration system:
		// - Dynamic GDS settings (gds.* except license_file)
		// - Bloom settings (dbms.bloom.*, server.unmanaged_extension_classes, dbms.security.http_auth_allowlist)
		// - GenAI settings (provider configurations, procedure security)
		// - General Neo4j settings (dbms.*, server.*)
		// - Plugin procedure security settings (dbms.security.procedures.*)
		filtered[key] = value
	}

	return filtered
}

// isNonDynamicSetting determines if a configuration setting can only be applied at startup
func (r *Neo4jPluginReconciler) isNonDynamicSetting(key string) bool {
	nonDynamicSettings := []string{
		// Plugin license files (must be available at startup)
		"gds.enterprise.license_file",
		"dbms.bloom.license_file",
		// Security settings (must be applied at startup via environment variables)
		"dbms.security.procedures.allowlist",
		"dbms.security.procedures.denylist",
		"dbms.security.procedures.unrestricted",
		// Add other non-dynamic settings as needed
	}

	for _, nonDynamic := range nonDynamicSettings {
		if key == nonDynamic {
			return true
		}
	}

	return false
}

// hasRuntimeSecurityConfiguration checks if security configuration contains settings that can be applied at runtime
// Most security settings (allowlist, denylist, unrestricted) are non-dynamic and must be applied as environment variables
func (r *Neo4jPluginReconciler) hasRuntimeSecurityConfiguration(security *neo4jv1beta1.PluginSecurity) bool {
	// Currently, all common security settings are non-dynamic in Neo4j 5.26+
	// They must be applied as environment variables at StatefulSet creation time
	// Future: if there are any dynamic security settings, check for them here
	return false
}

// applyAPOCSecurityConfiguration applies APOC security configuration without using Neo4j client configuration
// APOC security is handled via environment variables and procedure allowlists in Neo4j configuration
func (r *Neo4jPluginReconciler) applyAPOCSecurityConfiguration(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	logger := log.FromContext(ctx)

	if plugin.Spec.Security == nil {
		return nil
	}

	// For APOC security configuration, we need to set procedure allowlists via Neo4j configuration
	// but not APOC-specific settings
	if len(plugin.Spec.Security.AllowedProcedures) > 0 || len(plugin.Spec.Security.DeniedProcedures) > 0 {
		var neo4jClient *neo4jclient.Client
		var err error

		if deployment.Type == "cluster" {
			cluster, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseCluster)
			if !ok {
				return fmt.Errorf("deployment typed as cluster but object is %T", deployment.Object)
			}
			neo4jClient, err = neo4jclient.NewClientForEnterprise(cluster, r.Client, getClusterAdminSecretName(cluster))
		} else {
			standalone, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseStandalone)
			if !ok {
				return fmt.Errorf("deployment typed as standalone but object is %T", deployment.Object)
			}
			neo4jClient, err = neo4jclient.NewClientForEnterpriseStandalone(standalone, r.Client, getStandaloneAdminSecretName(standalone))
		}

		if err != nil {
			return fmt.Errorf("failed to create Neo4j client for security configuration: %w", err)
		}
		defer neo4jClient.Close()

		// In Neo4j 5.26+, all security settings are applied as environment variables
		// Skip runtime security configuration for consistency
		logger.Info("Skipping APOC runtime security configuration - all security settings are applied as environment variables in Neo4j 5.26+")
	}

	logger.Info("APOC security configuration applied successfully")
	return nil
}
