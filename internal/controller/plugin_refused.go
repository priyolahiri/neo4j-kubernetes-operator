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

// A Managed Neo4jPlugin whose NEO4J_PLUGINS names the image of its target
// cannot install is refused, and nothing of it stays on the target
// (operations.md id 170; #563). The Neo4j Docker entrypoint exits on a name its
// /startup/neo4j-plugins.json does not list, so writing one crash-loops every
// server it reaches: graphql on every supported release, n10s on CalVer from
// 2025.05, and the name of any custom JAR. So is a name the image lists whose
// JAR it does not run: n10s from 5.26.22, and on CalVer before 2025.05. Earlier operators wrote them, so a
// target may already carry one; the refused plugin takes it off, as a disabled
// plugin does (id 160).

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// targetImageTag is the image tag the target's servers start from: its
// spec.image.tag, which a write of the plugin reaches together with the image
// (a restart, an upgrade's step) — "" when the target is unknown.
func targetImageTag(deployment *DeploymentInfo) string {
	if deployment == nil {
		return ""
	}
	switch target := deployment.Object.(type) {
	case *neo4jv1beta1.Neo4jEnterpriseCluster:
		return target.Spec.Image.Tag
	case *neo4jv1beta1.Neo4jEnterpriseStandalone:
		return target.Spec.Image.Tag
	}
	return ""
}

// pluginRefusal says why the Neo4j image tagged imageTag cannot install the
// plugin (validation.ManagedPluginImageErrors: a Managed plugin's NEO4J_PLUGINS
// name its entrypoint does not list); "" when it can, or the plugin is disabled.
func pluginRefusal(plugin *neo4jv1beta1.Neo4jPlugin, imageTag string) string {
	errs := validation.ManagedPluginImageErrors(plugin, imageTag)
	if len(errs) == 0 {
		return ""
	}
	return errs.ToAggregate().Error()
}

// pluginOnImage reports whether the plugin is to be on a target running the
// image tagged imageTag: enabled, and not refused by that image.
func pluginOnImage(plugin *neo4jv1beta1.Neo4jPlugin, imageTag string) bool {
	return plugin.Spec.Enabled && pluginRefusal(plugin, imageTag) == ""
}

// pluginOnTarget is pluginOnImage for the plugin's target.
func pluginOnTarget(plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) bool {
	return pluginOnImage(plugin, targetImageTag(deployment))
}

// reconcileRefused keeps a refused plugin off its target, phase Failed with
// refusal as its message:
//   - nothing of it on the target: nothing is written, one PluginUnknownToImage
//     Warning;
//   - what an earlier write left there — its NEO4J_PLUGINS names, those its
//     record lists and its own and its dependencies' as the entrypoint knows
//     them, its variables, its list tokens, its records — goes in the one write
//     a disabled plugin's removal makes (withoutPlugin, ids 155 and 160). It
//     restarts the servers, so it waits for a target that is Ready with every
//     server available and rolled out (id 141), except when every server that
//     is down keeps failing on what it removes (selfRepair, ids 152 and 155):
//     the servers that crash-loop on the name are repaired at once, with one
//     PluginSettingsFailing Warning.
//
// The refusal is announced once (reportRefused); the message carries what the
// removal waits for. The plugin is looked at again every RequeueAfter, as a
// failed install is: the refusal depends on the target's image tag, which can
// change without an edit of the plugin.
func (r *Neo4jPluginReconciler) reconcileRefused(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo, refusal string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	requeue := ctrl.Result{RequeueAfter: r.RequeueAfter}
	target := targetName(deployment)
	removing := refusal + "; taking it off " + target
	// A removal an earlier pass of this generation wrote: its roll is waited for.
	inFlight := plugin.Status.ObservedGeneration == plugin.Generation && plugin.Status.Phase == neo4jv1beta1.PhaseFailed &&
		strings.HasPrefix(plugin.Status.Message, removing)

	changes, err := r.installChangesTarget(ctx, plugin, deployment)
	switch {
	case errors.IsNotFound(err):
		// No StatefulSet: nothing of the plugin is on the target.
		r.reportRefused(ctx, plugin, refusal, refusal)
		return requeue, nil
	case err != nil:
		logger.Error(err, "Cannot tell whether the refused plugin is still on its target")
		if !inFlight {
			r.reportRefused(ctx, plugin, refusal, refusal)
		}
		return requeue, nil
	}

	if changes {
		logger.Info("Refused plugin is on its target; taking it off", "refusal", refusal)
		if !deployment.IsReady {
			repair, repairSettings, err := r.selfRepair(ctx, plugin, deployment)
			if err != nil {
				logger.Error(err, "Cannot tell whether removing the refused plugin repairs its target")
			}
			if repair == "" {
				once, restarts := "once it is Ready with every server available", "every server"
				if deployment.Type == "standalone" {
					once, restarts = "once it is Ready", "it"
				}
				r.reportRefused(ctx, plugin, refusal, fmt.Sprintf("%s; it is taken off %s %s, since that restarts %s", refusal, target, once, restarts))
				return requeue, nil
			}
			reason := EventReasonPluginSettingsFailing
			if !repairSettings {
				reason = EventReasonPluginInitContainerFailing
			}
			// A write to a target that is not Ready: said once, from the status
			// write that records it (operations.md ids 136, 145).
			if r.reportRefused(ctx, plugin, refusal, fmt.Sprintf("%s although it is not Ready: every server that is down keeps failing with it (%s); "+
				"the roll replaces those pods first", removing, repair)) {
				r.Recorder.Eventf(plugin, corev1.EventTypeWarning, reason,
					"Taking the refused plugin off %s although it is not Ready: every server that is down keeps failing with it (%s); the roll replaces those pods first",
					target, repair)
			}
		}
	}

	// One write, also when only the plugin's records are left (a metadata
	// patch, which restarts nothing).
	written, err := r.writeRemoval(ctx, plugin, deployment)
	if err != nil {
		if !retryablePluginError(err) {
			logger.Error(err, "Failed to take the refused plugin off its target")
		}
		r.reportRefused(ctx, plugin, refusal, fmt.Sprintf("%s: the write failed and is retried: %v", removing, err))
		return requeue, nil
	}
	inFlight = inFlight || written > 0

	// A standalone's pod stuck on what the plugin set is not replaced by its
	// OrderedReady StatefulSet (ids 152, 155).
	r.replaceStuckPods(ctx, plugin, deployment)

	if inFlight {
		if pending, failing := r.rolloutPending(ctx, plugin, deployment, written); pending != "" || !r.arePodsReady(ctx, deployment) {
			message := removing + ": waiting for the servers to restart without it"
			if failing != "" {
				message += "; cannot start: " + failing
			}
			r.reportRefused(ctx, plugin, refusal, message)
			return requeue, nil
		}
	}
	r.reportRefused(ctx, plugin, refusal, refusal)
	return requeue, nil
}

// reportRefused writes phase Failed with message, which begins with refusal,
// and reports whether the write changed the status. The refusal is announced
// once — a PluginUnknownToImage Warning when the write moved the plugin into it
// from a status that did not carry it already, as the write read it (ids 136,
// 145) — so the steps of taking an earlier write off the target, which change
// the message, do not announce it again, nor does a restarted operator. A
// different refusal (the target's image changed) is announced once more.
func (r *Neo4jPluginReconciler) reportRefused(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, refusal, message string) bool {
	changed, prevPhase, prevMessage := r.writePluginStatus(ctx, plugin, neo4jv1beta1.PhaseFailed, message)
	if changed && (prevPhase != neo4jv1beta1.PhaseFailed || !strings.HasPrefix(prevMessage, refusal)) {
		r.Recorder.Event(plugin, corev1.EventTypeWarning, EventReasonPluginUnknownToImage, refusal)
	}
	return changed
}
