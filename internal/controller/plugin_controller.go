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
		controllerutil.AddFinalizer(plugin, PluginFinalizer)
		if err := r.Update(ctx, plugin); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		// The Update above triggers a watch event that re-queues this object.
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
		r.updatePluginStatus(ctx, plugin, "Failed", msg)
		r.Recorder.Event(plugin, corev1.EventTypeWarning, EventReasonPluginDuplicate, msg)
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
		missing, err := r.missingFromTarget(ctx, plugin, deployment)
		switch {
		case err != nil:
			// The install below reads the same StatefulSet and reports it.
			logger.Error(err, "Cannot tell whether the target still carries the plugin")
		case missing == "":
			return ctrl.Result{}, nil
		case !deployment.IsReady:
			// Restoring it restarts every server, like an install, and waits
			// for the same reason (below).
			logger.Info("Plugin missing from its target; restoring it once the target is fully available", "missing", missing)
			return r.deferInstall(ctx, plugin, deployment, missing)
		default:
			logger.Info("Plugin missing from its target; restoring it", "missing", missing)
			r.Recorder.Eventf(plugin, corev1.EventTypeWarning, EventReasonPluginMissing,
				"%s; restoring it, which restarts the servers", missing)
			r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Restoring the plugin: "+missing)
		}
	}

	// An install restarts every server, and so does any write of the plugin to
	// the target's StatefulSet. A cluster already short of a server (Degraded),
	// or still forming, rolling or upgrading, could lose its majority to it
	// (CLAUDE.md, "Server availability (#444)"), so the write waits until the
	// target is Ready with every server available. A pass with nothing to write
	// — one waiting for the servers this install restarted, which turned the
	// cluster Forming — carries on.
	if !deployment.IsReady {
		changes, err := r.installChangesTarget(ctx, plugin, deployment)
		switch {
		case err != nil:
			// The install below reads the same StatefulSet and reports it.
			logger.Error(err, "Cannot tell whether installing the plugin would change its target")
		case changes:
			logger.Info("Target not fully available; deferring the plugin installation", "type", deployment.Type, "name", deployment.Name)
			return r.deferInstall(ctx, plugin, deployment, "")
		}
	}

	// Check if deployment is actually functional, not just status reporting
	if !r.deploymentFunctional(ctx, deployment) {
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
	if !settled && !inProgress {
		r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Installing plugin")
	}

	// Install plugin using NEO4J_PLUGINS environment variable (recommended by Neo4j docs)
	if err := r.installPluginViaEnvironment(ctx, plugin, deployment); err != nil {
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

	// Check if deployment is ready after restart (non-blocking)
	if !r.arePodsReady(ctx, deployment) {
		logger.Info("Waiting for pods to be ready after plugin installation")
		r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	// Configure plugin. This can run while the servers restart to take the
	// plugin: the write above is what restarts them, and the old pods still
	// report Ready until they stop. A server that cannot be reached then is
	// part of the install, not a failed one.
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
// lost what it set (missing says what), PluginInstallDeferred otherwise. The
// event follows the status write that starts the wait (operations.md id 136),
// so the passes that go on waiting, and an operator restart, say nothing more.
func (r *Neo4jPluginReconciler) deferInstall(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo, missing string) (ctrl.Result, error) {
	until := targetAvailable(deployment)
	message := "Waiting until " + until + ": installing the plugin restarts every server"
	if deployment.Type == "standalone" {
		message = "Waiting until " + until + ": installing the plugin restarts it"
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
		return fmt.Sprintf("Neo4jEnterpriseStandalone %s is Ready", deployment.Name)
	}
	return fmt.Sprintf("Neo4jEnterpriseCluster %s is Ready with every server available", deployment.Name)
}

// installChangesTarget reports whether installing the plugin would change its
// target's StatefulSet, which restarts the servers: the install's own edits
// (applyPluginToTemplate) made on a copy of the live pod template.
func (r *Neo4jPluginReconciler) installChangesTarget(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return false, err
	}
	desired := sts.Spec.Template.DeepCopy()
	if err := r.applyPluginToTemplate(desired, plugin, deployment); err != nil {
		return false, err
	}
	return !equality.Semantic.DeepEqual(sts.Spec.Template, *desired), nil
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

	// Uninstall plugin
	if err := r.uninstallPlugin(ctx, plugin); err != nil {
		logger.Error(err, "Failed to uninstall plugin")
		return ctrl.Result{}, err
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(plugin, PluginFinalizer)
	if err := r.Update(ctx, plugin); err != nil {
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
		// Ready only, not Degraded: installing a plugin rolls every server,
		// which on a cluster already short of one could cost it its majority.
		isReady := cluster.Status.Phase == "Ready" && serversUnavailableSince(cluster) == nil
		return &DeploymentInfo{
			Object:    cluster,
			Type:      "cluster",
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			IsReady:   isReady,
		}, nil
	}

	// Try Neo4jEnterpriseStandalone
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      plugin.Spec.ClusterRef,
		Namespace: plugin.Namespace,
	}, standalone); err == nil {
		isReady := standalone.Status.Phase == "Ready"
		return &DeploymentInfo{
			Object:    standalone,
			Type:      "standalone",
			Name:      standalone.Name,
			Namespace: standalone.Namespace,
			IsReady:   isReady,
		}, nil
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

func (r *Neo4jPluginReconciler) uninstallPlugin(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin) error {
	logger := log.FromContext(ctx)

	// Get target deployment
	deployment, err := r.getTargetDeployment(ctx, plugin)
	if err != nil {
		// Deployment gone → nothing to uninstall from. Succeed so the finalizer
		// releases instead of wedging on a target that no longer exists.
		logger.Info("Target deployment not found, considering plugin uninstalled")
		return nil //nolint:nilerr // intentional: missing target means already uninstalled
	}

	// Prune this plugin's additive security tokens (e.g. gds.*) from the
	// StatefulSet env vars. The install path unions these in for EVERY mode
	// (mergePluginSecurityEnv runs regardless of PreBaked/VerifiedDownload),
	// so removal must be mode-independent too — done here, before the
	// mode-specific early returns below, rather than only in the Managed
	// JAR-removal path. The cluster controller's env merge is subset/
	// ownership-tracked and will NOT drop tokens it doesn't own, so this
	// explicit prune is the only thing that removes them. Best-effort: a
	// failure here must not block the finalizer.
	if err := r.prunePluginSecurityEnv(ctx, plugin, deployment); err != nil {
		logger.Error(err, "Failed to prune plugin security env vars from StatefulSet; continuing", "plugin", plugin.Spec.Name)
	}

	// VerifiedDownload installs need the init container + auth/CA
	// volumes removed from the StatefulSet on uninstall — otherwise
	// the next reconcile would still try to download and the pod
	// would re-add the JAR to the emptyDir each restart. Done first
	// so the PreBaked-style "skip JAR removal" guard below doesn't
	// short-circuit the cleanup.
	if isVerifiedDownloadInstallMode(plugin) {
		if err := r.removeVerifiedDownloadInitContainer(ctx, plugin, deployment); err != nil {
			// Non-fatal — the finalizer release shouldn't block on a
			// reconcile race against the cluster controller. Next
			// reconcile of the cluster will eventually drop the stale
			// init container.
			logger.Error(err, "Failed to remove VerifiedDownload init container; releasing finalizer anyway", "plugin", plugin.Spec.Name)
		}
		logger.Info("Removed VerifiedDownload init container", "plugin", plugin.Spec.Name)
		return nil
	}

	// PreBaked installs are JARs the operator did not deliver — never run the
	// jar-removal Job against them. The user-supplied custom image owns
	// /plugins/*; touching it could orphan files the operator did not put
	// there. The operator-owned configuration (security env tokens) has
	// already been pruned above; ConfigMap entries are reconciled separately.
	if isPreBakedInstallMode(plugin) {
		logger.Info("Skipping JAR removal for PreBaked plugin", "plugin", plugin.Spec.Name)
		return nil
	}

	// Remove plugin from deployment
	if err := r.removePluginFromDeployment(ctx, plugin, deployment); err != nil {
		return fmt.Errorf("failed to remove plugin from deployment: %w", err)
	}

	// Dependencies are folded into the parent plugin's NEO4J_PLUGINS env var
	// by installPlugin; there are no separate Neo4jPlugin CRs to clean up.

	logger.Info("Plugin uninstalled successfully")
	return nil
}

// removePluginFromDeployment uninstalls a Managed plugin by dropping its name
// from the target StatefulSet's NEO4J_PLUGINS env var.
//
// That env var is the ONLY lever that uninstalls a Managed plugin. `/plugins` is
// a per-pod EmptyDir (see the "plugins" volume in internal/resources/cluster.go)
// that the Neo4j Docker entrypoint repopulates from NEO4J_PLUGINS on every
// container start, so the JAR on disk is derived state — leave the name in the
// env var and the next restart reinstalls the plugin.
//
// This used to launch a `rm -f /plugins/<name>*.jar` Job, which could not work by
// construction: the Job declared its OWN fresh EmptyDir, so it deleted files in a
// directory it had just created and never touched the Neo4j pod. It also wedged
// deletion — the Job outlived the reconcile that made it, and every later
// reconcile re-issued the same Create and failed `already exists`, so the
// finalizer was never released and the CR stayed Terminating forever (which in
// turn blocked namespace deletion). Removing it fixes the wedge and makes
// uninstall symmetric with install, which mutates the same env var.
//
// Security-token env pruning is handled mode-independently in uninstallPlugin
// (before the mode branches), so it is not repeated here.
func (r *Neo4jPluginReconciler) removePluginFromDeployment(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	logger := log.FromContext(ctx)
	logger.Info("Removing plugin from deployment", "plugin", plugin.Spec.Name, "type", deployment.Type)

	stsKey := types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, stsKey, sts); err != nil {
			if errors.IsNotFound(err) {
				// Target gone → nothing to uninstall from; let the finalizer go.
				return nil
			}
			return err
		}
		for i := range sts.Spec.Template.Spec.Containers {
			c := &sts.Spec.Template.Spec.Containers[i]
			if c.Name != "neo4j" {
				continue
			}
			for j := range c.Env {
				if c.Env[j].Name != "NEO4J_PLUGINS" {
					continue
				}
				remaining, err := RemoveFromNeo4jPluginList(c.Env[j].Value, plugin.Spec.Name)
				if err != nil {
					return err
				}
				if remaining == c.Env[j].Value {
					return nil // not listed — nothing to do, and no needless restart
				}
				c.Env[j].Value = remaining
				logger.Info("Dropped plugin from NEO4J_PLUGINS", "plugin", plugin.Spec.Name, "remaining", remaining)
				return r.Update(ctx, sts)
			}
			return nil // no NEO4J_PLUGINS at all (e.g. PreBaked-only deployment)
		}
		return nil
	})
}

// prunePluginSecurityEnv removes the uninstalled plugin's additive security
// tokens from the target StatefulSet's neo4j container env vars. No-op when the
// StatefulSet is gone or the plugin contributed no additive env vars (e.g. a
// standalone, which carries plugin security in neo4j.conf rather than env vars).
func (r *Neo4jPluginReconciler) prunePluginSecurityEnv(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	stsKey := types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, stsKey, sts); err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
		for i := range sts.Spec.Template.Spec.Containers {
			c := &sts.Spec.Template.Spec.Containers[i]
			if c.Name != "neo4j" {
				continue
			}
			pruned := removePluginSecurityEnv(c.Env, r.pluginSecurityRemovalSettings(plugin))
			if len(pruned) == len(c.Env) {
				// Recompute equality cheaply: lengths match AND values unchanged?
				// removePluginSecurityEnv only ever shrinks or rewrites values, so
				// compare to avoid a no-op Update (which would needlessly restart).
				same := true
				for j := range pruned {
					if pruned[j] != c.Env[j] {
						same = false
						break
					}
				}
				if same {
					return nil
				}
			}
			c.Env = pruned
			return r.Update(ctx, sts)
		}
		return nil
	})
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
	written := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Fetch latest version of StatefulSet for each retry
		currentSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, stsKey, currentSts); err != nil {
			return err
		}

		// Apply the plugin's edits to the current StatefulSet
		before := currentSts.Spec.Template.DeepCopy()
		if err := r.applyPluginToTemplate(&currentSts.Spec.Template, plugin, deployment); err != nil {
			return err
		}
		if equality.Semantic.DeepEqual(*before, currentSts.Spec.Template) {
			// Everything the plugin sets is there already. Writing would change
			// nothing, and the passes that wait for the servers to restart would
			// only race the writes of the StatefulSet's own controller.
			return nil
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
		written = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update StatefulSet with plugin configuration: %w", err)
	}

	if written {
		logger.Info("Successfully updated StatefulSet with plugin configuration", "plugin", plugin.Spec.Name)
	}

	// Note: ConfigMap updates for standalone deployments are now handled earlier in reconcile flow
	// before connectivity checks to ensure security settings are applied before Neo4j starts

	return nil
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

// missingFromTarget reports what of the plugin its target's StatefulSet no
// longer carries, or "" when it carries all of it: the plugin's edits are made
// on a copy of the live pod template, and pluginEnvDrift and pluginPodDrift
// decide which of them a settled plugin puts right.
func (r *Neo4jPluginReconciler) missingFromTarget(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (string, error) {
	stsName := r.getStatefulSetName(deployment)
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: deployment.Namespace}, sts); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Sprintf("StatefulSet %s does not exist", stsName), nil
		}
		return "", fmt.Errorf("failed to get StatefulSet %s: %w", stsName, err)
	}
	if neo4jContainer(&sts.Spec.Template.Spec) == nil {
		return "", fmt.Errorf("neo4j container not found in StatefulSet %s", stsName)
	}
	desired := sts.Spec.Template.DeepCopy()
	if err := r.applyPluginToTemplate(desired, plugin, deployment); err != nil {
		return "", err
	}
	drift := pluginEnvDrift(neo4jContainerEnv(sts.Spec.Template.Spec), neo4jContainerEnv(desired.Spec), deployment.Type == "standalone")
	drift = append(drift, pluginPodDrift(sts.Spec.Template.Spec, desired.Spec)...)
	if len(drift) == 0 {
		return "", nil
	}
	return fmt.Sprintf("StatefulSet %s does not hold what the plugin sets in %s", stsName, strings.Join(drift, ", ")), nil
}

// pluginPodDrift returns the init containers and volumes a VerifiedDownload
// plugin adds that live, the target's pod spec, does not have (desired is live
// after the plugin's edits): what something removed — a StatefulSet recreated
// from the cluster's own template (storage expansion does that), or an earlier
// operator's cluster controller, which dropped them on every apply. Only
// absence is put right, as with env vars: the live init container carries the
// API server's defaults, and nothing else writes the plugin's own names.
func pluginPodDrift(live, desired corev1.PodSpec) []string {
	var drift []string
	initContainers := make(map[string]struct{}, len(live.InitContainers))
	for _, c := range live.InitContainers {
		initContainers[c.Name] = struct{}{}
	}
	for _, c := range desired.InitContainers {
		if _, ok := initContainers[c.Name]; !ok {
			drift = append(drift, "init container "+c.Name)
		}
	}
	for _, v := range desired.Volumes {
		if !podSpecHasVolume(&live, v.Name) {
			drift = append(drift, "volume "+v.Name)
		}
	}
	return drift
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

	// Add volumes if not already present (multiple VerifiedDownload
	// plugins on the same cluster share the CA volume; each plugin
	// has its own auth volume).
	if authVol != nil && !podSpecHasVolume(podSpec, authVol.Name) {
		podSpec.Volumes = append(podSpec.Volumes, *authVol)
	}
	if caVol != nil && !podSpecHasVolume(podSpec, caVol.Name) {
		podSpec.Volumes = append(podSpec.Volumes, *caVol)
	}

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

// removeVerifiedDownloadInitContainer is the inverse of
// applyVerifiedDownload: drops the plugin's init container from the
// StatefulSet PodSpec, removes the plugin-specific auth volume if
// present, and updates the ownership annotation. The shared CA volume
// goes only once nothing mounts it — other VerifiedDownload plugins on
// the same StatefulSet may still be using it; deleting it
// unilaterally would break them.
//
// Idempotent: a second call (or a call after a delete-already-happened
// scenario) is a no-op via the "not found" early returns.
func (r *Neo4jPluginReconciler) removeVerifiedDownloadInitContainer(
	ctx context.Context,
	plugin *neo4jv1beta1.Neo4jPlugin,
	deployment *DeploymentInfo,
) error {
	stsKey := types.NamespacedName{
		Name:      r.getStatefulSetName(deployment),
		Namespace: deployment.Namespace,
	}
	containerName := resources.PluginInitContainerName(plugin.Spec.Name)
	authVolName := "plugin-auth-" + plugin.Spec.Name

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, stsKey, sts); err != nil {
			if errors.IsNotFound(err) {
				return nil // StatefulSet already gone — nothing to clean up.
			}
			return err
		}

		podSpec := &sts.Spec.Template.Spec
		changed := false

		// Strip the init container.
		filtered := podSpec.InitContainers[:0]
		for _, ic := range podSpec.InitContainers {
			if ic.Name == containerName {
				changed = true
				continue
			}
			filtered = append(filtered, ic)
		}
		podSpec.InitContainers = filtered

		// Strip the per-plugin auth volume, and the shared CA volume once no
		// container mounts it: the cluster controller keeps volumes it did not
		// render, so an orphaned one would stay, still projecting CA Secrets
		// the pods then cannot start without once they are deleted.
		caUnused := !podSpecMountsVolume(podSpec, resources.PluginCAVolumeName)
		filteredVols := podSpec.Volumes[:0]
		for _, v := range podSpec.Volumes {
			if v.Name == authVolName || (caUnused && v.Name == resources.PluginCAVolumeName) {
				changed = true
				continue
			}
			filteredVols = append(filteredVols, v)
		}
		podSpec.Volumes = filteredVols

		// Drop ourselves from the ownership annotation.
		if sts.Spec.Template.Annotations != nil {
			owned := parsePluginInitOwned(sts.Spec.Template.Annotations[PluginInitContainersAnnotation])
			if _, ok := owned[containerName]; ok {
				delete(owned, containerName)
				if len(owned) == 0 {
					delete(sts.Spec.Template.Annotations, PluginInitContainersAnnotation)
				} else {
					sts.Spec.Template.Annotations[PluginInitContainersAnnotation] = formatPluginInitOwned(owned)
				}
				changed = true
			}
		}

		if !changed {
			return nil // Nothing to update — return without bumping ResourceVersion.
		}
		return r.Update(ctx, sts)
	})
}

// podSpecHasVolume reports whether a volume of the given name is
// already declared on the PodSpec. Used to avoid duplicate Volume
// entries when multiple VerifiedDownload plugins land on the same
// StatefulSet over successive reconciles.
func podSpecHasVolume(podSpec *corev1.PodSpec, name string) bool {
	for _, v := range podSpec.Volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

// podSpecMountsVolume reports whether any container or init container of the
// pod mounts the named volume.
func podSpecMountsVolume(podSpec *corev1.PodSpec, name string) bool {
	for _, list := range [][]corev1.Container{podSpec.InitContainers, podSpec.Containers} {
		for _, c := range list {
			for _, m := range c.VolumeMounts {
				if m.Name == name {
					return true
				}
			}
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

// getRequiredProcedureSecuritySettings returns the required procedure security settings for a plugin
func (r *Neo4jPluginReconciler) getRequiredProcedureSecuritySettings(pluginName string) map[string]string {
	settings := make(map[string]string)

	switch pluginName {
	case "graph-data-science", "gds":
		// GDS requires unrestricted access for performance
		settings["dbms.security.procedures.unrestricted"] = "gds.*"
		settings["dbms.security.procedures.allowlist"] = "gds.*"
	case "bloom":
		// Bloom requires unrestricted access
		settings["dbms.security.procedures.unrestricted"] = "bloom.*"
		// Bloom also needs HTTP auth allowlist for web interface
		settings["dbms.security.http_auth_allowlist"] = "/,/browser.*,/bloom.*"
		// Unmanaged extension for hosting Bloom client
		settings["server.unmanaged_extension_classes"] = "com.neo4j.bloom.server=/bloom"
	case "apoc", "apoc-extended":
		// APOC procedures should be configured via allowlist (principle of least privilege)
		// Note: This is handled separately since APOC config is environment-variable only
		// These settings would still go through neo4j.conf for procedure security
		break
	case "genai":
		// GenAI may need procedure allowlist depending on usage
		break
	case "n10s", "neosemantics":
		// Neo Semantics procedures
		settings["dbms.security.procedures.unrestricted"] = "n10s.*"
		settings["dbms.security.procedures.allowlist"] = "n10s.*"
	case "graphql":
		// GraphQL plugin procedures
		settings["dbms.security.procedures.unrestricted"] = "graphql.*"
		settings["dbms.security.procedures.allowlist"] = "graphql.*"
	case "fleet-management":
		// Fleet management plugin procedures
		settings["dbms.security.procedures.unrestricted"] = "fleetManagement.*"
		settings["dbms.security.procedures.allowlist"] = "fleetManagement.*"
	}

	return settings
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

// mergeableCSVKeys is the closed set of plugin-driven Neo4j config keys whose
// values are comma-separated lists where merging the union (rather than
// overwriting one with the other) is the correct behaviour. Keep this list
// closed — substring matches are dangerous: a user-supplied setting like
// `my.custom.allowlist` should not silently get merge semantics.
var mergeableCSVKeys = map[string]struct{}{
	"dbms.security.procedures.allowlist":    {},
	"dbms.security.procedures.unrestricted": {},
	"dbms.security.procedures.denylist":     {},
	"dbms.security.http_auth_allowlist":     {},
	"server.unmanaged_extension_classes":    {},
}

// isMergeableCSVKey reports whether a Neo4j config key carries a CSV value
// that should be unioned (rather than overwritten) when both an existing
// env var and a plugin-required setting specify a value.
func isMergeableCSVKey(key string) bool {
	_, ok := mergeableCSVKeys[key]
	return ok
}

// mergeCSV returns the deduplicated comma-separated union of a and b,
// preserving first-seen order. Whitespace around each element is trimmed;
// empty elements are dropped.
//
// Used for security/extension settings (see mergeableCSVKeys) where
// multiple sources contribute entries — typically the user via
// plugin.Spec.Config plus the operator's required defaults for the
// plugin. The previous code concatenated `existing + "," + new` blindly,
// which produced duplicate-laden values when `new` was itself
// multi-element (e.g. Bloom's `http_auth_allowlist`).
func mergeCSV(a, b string) string {
	seen := make(map[string]struct{})
	var out []string
	for _, src := range [...]string{a, b} {
		for _, p := range strings.Split(src, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if _, dup := seen[p]; dup {
				continue
			}
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return strings.Join(out, ",")
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
