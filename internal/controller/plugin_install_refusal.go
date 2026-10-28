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

// A Neo4jPlugin the operator refuses, for a reason the user has to fix, is a
// status, not a controller error (operations.md id 228): phase Failed (Invalid
// for a spec its validator refuses), one event from the status write that
// records it, and its log line from the same write — at info when the write
// changed the status, at V(1) on the passes in between, never an ERROR
// (logRefusal, as the cluster's, standalone's, database kinds' and restore's
// refusals, ids 136, 197, 215). A refused plugin whose cluster's own spec.env
// NEO4J_PLUGINS lacked its name logged ERROR "Failed to install plugin", with
// a stack trace, on every retry, about every 27 seconds (#608, the walk of
// #613).
//
// The refusals: a spec the validator refuses (Invalid), a newer duplicate of
// a (clusterRef, spec.name) tuple (Failed, PluginDuplicate), a name the
// target's image cannot install (Failed, PluginUnknownToImage, id 170), and the
// two install refusals here — a write that would change a variable the
// cluster renders from its own spec (pluginEnvOwnedError, id 135) and a
// NEO4J_PLUGINS on the target that is not a JSON list of names
// (pluginListUnreadableError). An install refusal is decided from the
// StatefulSet before anything else is asked: the gate that waits for a Ready
// target and the Bolt check that it answers are for a write, and there is
// none to make.
//
// A real failure is still one: a StatefulSet write the API server refuses, a
// read that fails, logs its ERROR as before (installFailed).
//
// A refused install is looked at again every RequeueAfter, as before: its
// cause is the target's spec, which changes without an edit of the plugin,
// and the StatefulSet watch wakes only Ready plugins (id 135). The passes in
// between write nothing and log at V(1).

import (
	"context"
	stderrors "errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// pluginListUnreadableError: the target's NEO4J_PLUGINS is not a JSON list of
// plugin names, so the plugin cannot add its name to it (MergeNeo4jPluginList).
// Usually the target's own spec.env sets it so; the entrypoint cannot read such
// a value either.
type pluginListUnreadableError struct{ err error }

func (e *pluginListUnreadableError) Error() string {
	return fmt.Sprintf("NEO4J_PLUGINS on the target is not a JSON list of plugin names (%v), so the plugin cannot add its name to it: "+
		`make it one, such as ["apoc"], where it is set (usually the target's spec.env)`, e.err)
}

func (e *pluginListUnreadableError) Unwrap() error { return e.err }

// pluginInstallRefusal returns the refusal err carries, or nil: a write that
// would change a variable the cluster renders itself (pluginEnvOwnedError), or
// a NEO4J_PLUGINS that is not a JSON list (pluginListUnreadableError) — the
// error itself, unwrapped, so the message is the same whichever step met it.
func pluginInstallRefusal(err error) error {
	var owned *pluginEnvOwnedError
	if stderrors.As(err, &owned) {
		return owned
	}
	var unreadable *pluginListUnreadableError
	if stderrors.As(err, &unreadable) {
		return unreadable
	}
	return nil
}

// ownedEnvRefusal is the check of a write of the plugin to a cluster: before
// is its StatefulSet as read, after with the plugin's edits made. A variable
// the edits change that the cluster renders from its own spec (its
// neo4j.com/cluster-controller-env-vars record lists it) is refused: the
// cluster controller puts its own value back, so the two would restart the
// servers in turn (operations.md id 135). A standalone's controller never puts
// a live value back, so nothing is refused there.
func ownedEnvRefusal(before, after *appsv1.StatefulSet, deployment *DeploymentInfo) error {
	if deployment.Type != "cluster" {
		return nil
	}
	changed := changedEnvNames(neo4jContainerEnv(before.Spec.Template.Spec), neo4jContainerEnv(after.Spec.Template.Spec))
	if owned := ownedEnvNames(changed, readOwnedEnvVarNames(before)); len(owned) > 0 {
		return &pluginEnvOwnedError{statefulSet: before.Name, cluster: deployment.Name, names: owned}
	}
	return nil
}

// refuseInstall records an install refusal (pluginInstallRefusal): phase
// Failed, its message naming what to change, one PluginInstallFailed Warning
// and one info line from the status write that records it — when the refusal
// starts or its message changes — and V(1) on the passes in between. Another
// look after RequeueAfter: the fix is in the target's spec.
func (r *Neo4jPluginReconciler) refuseInstall(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, refusal error) (ctrl.Result, error) {
	message := "Plugin installation refused: " + refusal.Error()
	announced := r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseFailed, message)
	logRefusal(log.FromContext(ctx), announced, "Plugin installation refused; the plugin is looked at again once its target's spec changes", refusal)
	if announced {
		r.Recorder.Eventf(plugin, corev1.EventTypeWarning, EventReasonPluginInstallFailed,
			"Plugin %s installation refused: %v", plugin.Spec.Name, refusal)
	}
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}
