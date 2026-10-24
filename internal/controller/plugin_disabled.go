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

// A Neo4jPlugin with spec.enabled: false is kept off its target's servers, and
// the CR stays (operations.md id 160). Until #540 the plugin controller did not
// read the field: a disabled plugin was installed all the same, while the
// standalone's neo4j.conf union and the MCP APOC check, which did read it,
// left it out.

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// reconcileDisabled takes a disabled plugin off its target and keeps it off:
//   - what the plugin wrote goes in one write, the uninstall a deletion makes
//     (withoutPlugin, operations.md id 155): its NEO4J_PLUGINS names, its
//     variables, its procedure-list tokens, its VerifiedDownload init container
//     and volumes, its records;
//   - that write restarts the servers, so like every write of the plugin it
//     waits for a target that is Ready with every server available and rolled
//     out (id 141; phase Waiting, one PluginInstallDeferred), except when every
//     server that is down keeps failing in what the removal changes (selfRepair,
//     ids 152 and 155): disabling the plugin that keeps servers from starting
//     goes through at once, with one PluginInitContainerFailing or
//     PluginSettingsFailing Warning;
//   - phase Installing while the removal rolls out, then Disabled with one
//     PluginDisabled event, announced from the status write that makes it
//     Disabled (id 136). A plugin with nothing on its target is Disabled at
//     once.
//
// Setting spec.enabled back to true installs the plugin again: the next pass
// finds it neither settled nor part way through an install of its generation.
func (r *Neo4jPluginReconciler) reconcileDisabled(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	// A removal of this generation already written: its roll is waited for.
	removing := plugin.Status.ObservedGeneration == plugin.Generation && plugin.Status.Phase == neo4jv1beta1.PhaseInstalling

	changes, err := r.installChangesTarget(ctx, plugin, deployment)
	switch {
	case errors.IsNotFound(err):
		r.reportDisabled(ctx, plugin, fmt.Sprintf("Plugin disabled (spec.enabled: false): %s has no StatefulSet", targetName(deployment)))
		return ctrl.Result{}, nil
	case err != nil:
		logger.Error(err, "Cannot tell whether the disabled plugin is still on its target")
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	if changes {
		repair, repairSettings := "", false
		if !deployment.IsReady {
			if repair, repairSettings, err = r.selfRepair(ctx, plugin, deployment); err != nil {
				logger.Error(err, "Cannot tell whether removing the disabled plugin repairs its target")
			}
			if repair == "" {
				logger.Info("Target not fully available; deferring the removal of the disabled plugin", "type", deployment.Type, "name", deployment.Name)
				return r.deferInstall(ctx, plugin, deployment, "", "")
			}
		}
		if repair != "" {
			// A write to a target that is not Ready: said once, from the status
			// write that records it (operations.md ids 136, 145).
			message := fmt.Sprintf("Removing the disabled plugin from %s although it is not Ready: every server that is down "+
				"keeps failing in what the plugin set (%s); the roll replaces those pods first", targetName(deployment), repair)
			reason := EventReasonPluginInitContainerFailing
			if repairSettings {
				reason = EventReasonPluginSettingsFailing
			}
			if r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, message) {
				r.Recorder.Event(plugin, corev1.EventTypeWarning, reason, message)
			}
		} else {
			r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling,
				fmt.Sprintf("Removing the plugin from %s (spec.enabled: false)", targetName(deployment)))
		}
		removing = true
	}

	// One write, also when only the plugin's records are left (a metadata
	// patch, which restarts nothing).
	written, err := r.writeRemoval(ctx, plugin, deployment)
	if err != nil {
		if retryablePluginError(err) {
			logger.Info("Removal of the disabled plugin interrupted; retrying", "error", err.Error())
			r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling,
				fmt.Sprintf("Retrying the removal of the disabled plugin: %v", err))
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
		logger.Error(err, "Failed to remove the disabled plugin")
		return r.installFailed(ctx, plugin, fmt.Sprintf("Removing the disabled plugin failed: %v", err),
			fmt.Sprintf("Plugin %s removal failed: %v", plugin.Spec.Name, err))
	}

	// A standalone's pod stuck on what the plugin set is not replaced by its
	// OrderedReady StatefulSet (ids 152, 155).
	r.replaceStuckPods(ctx, plugin, deployment)

	// Disabled only once the servers run without it, as an install is Ready
	// only once its roll is done (id 141).
	if removing {
		if pending, failing := r.rolloutPending(ctx, plugin, deployment, written); pending != "" || !r.arePodsReady(ctx, deployment) {
			logger.Info("Waiting for the servers to restart without the disabled plugin", "rollout", pending)
			message := "Removing the plugin (spec.enabled: false): waiting for the servers to restart without it"
			if failing != "" {
				message += "; cannot start: " + failing
			}
			r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, message)
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
	}

	r.reportDisabled(ctx, plugin, fmt.Sprintf("Plugin disabled (spec.enabled: false): not on the servers of %s", targetName(deployment)))
	return ctrl.Result{}, nil
}

// reportDisabled writes phase Disabled with message, and announces it once,
// from the write that makes the plugin Disabled (operations.md id 136).
func (r *Neo4jPluginReconciler) reportDisabled(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, message string) {
	if r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseDisabled, message) {
		r.Recorder.Event(plugin, corev1.EventTypeNormal, EventReasonPluginDisabled, message)
	}
}
