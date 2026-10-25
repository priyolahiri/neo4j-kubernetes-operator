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

// A target whose StatefulSet does not exist is waited for, quietly
// (operations.md id 177). Its cluster or standalone exists, but the
// StatefulSet does not: the target's controller has not created it yet, or a
// storage expansion deleted it and creates it again, as it was, seconds later
// (id 171). Nothing of a plugin was taken off it and nothing can be put back
// yet, so this is neither drift nor a failure.

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const (
	// stuckPodRecheck is how soon a deleted plugin looks again for the
	// standalone pod its uninstall is to replace: the StatefulSet's controller
	// sees a template write within seconds.
	stuckPodRecheck = 5 * time.Second
	// pluginUninstallPodWait bounds how long a deleted plugin's finalizer waits
	// for that pod, from the deletion's request: a deletion must not wedge.
	pluginUninstallPodWait = 5 * time.Minute
)

// uninstallWaitsForStuckPod reports whether a deleted plugin's finalizer is to
// wait for the pod of its standalone target that keeps failing on what the
// plugin set — its init container, or settings Neo4j refuses at startup. The
// uninstall's write takes them off the template, but an OrderedReady
// StatefulSet does not replace a pod that is not Ready (ids 152, 155), and the
// plugin, once its finalizer is released, has no pass left to delete it: the
// pod stayed in CrashLoopBackOff, needing a delete by hand, while the
// standalone said a restart was on its way (v1.22.0 re-walk, a crash-looping
// plugin deleted). So the finalizer waits until the StatefulSet's controller
// has seen the uninstall and replaceStuckPods has deleted the pod, or
// pluginUninstallPodWait has passed since the deletion was asked for. A
// disabled or refused plugin's removal needs none of this: its CR stays, and
// its next passes delete the pod.
func (r *Neo4jPluginReconciler) uninstallWaitsForStuckPod(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin) bool {
	deployment, err := r.getTargetDeployment(ctx, plugin)
	if err != nil || deployment.Type != "standalone" {
		return false
	}
	if !r.replaceStuckPods(ctx, plugin, deployment) {
		return false
	}
	if plugin.DeletionTimestamp != nil && time.Since(plugin.DeletionTimestamp.Time) > pluginUninstallPodWait {
		log.FromContext(ctx).Info("Releasing the plugin although the standalone's pod that keeps failing on it has not been replaced",
			"standalone", deployment.Name, "waited", pluginUninstallPodWait)
		return false
	}
	return true
}

// statefulSetAbsent reports whether the target's StatefulSet does not exist.
func (r *Neo4jPluginReconciler) statefulSetAbsent(ctx context.Context, deployment *DeploymentInfo) (bool, error) {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts)
	if errors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}

// awaitStatefulSet waits for a target whose StatefulSet does not exist, with
// no event, no ERROR and no status write that a plugin's state does not need:
//   - a plugin Ready (or Disabled) at its generation keeps its status and is
//     not requeued. The StatefulSet's creation wakes a Ready plugin (id 135):
//     a StatefulSet created again as it was holds what the plugin set, and
//     the settled pass finds nothing to do — no PluginMissing, no
//     PluginInstalled again — while one created without it is restored, with
//     the one PluginMissing that says so;
//   - a plugin part way through its generation (Installing, Waiting, Failed)
//     keeps its status and looks again after RequeueAfter;
//   - any other plugin — one just created, or edited — is Waiting ("Waiting
//     for StatefulSet … to be created") and looks again after RequeueAfter.
//
// During a storage expansion each plugin used to read the missing StatefulSet
// as drift: a PluginMissing Warning, a PluginInstallFailed Warning when the
// write found nothing to write to, two ERROR logs, Installing for the whole
// expansion, and PluginInstalled again at its end (v1.22.0 re-walk of the
// combined build).
func (r *Neo4jPluginReconciler) awaitStatefulSet(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (ctrl.Result, error) {
	name := r.getStatefulSetName(deployment)
	log.FromContext(ctx).V(1).Info("The target's StatefulSet does not exist; waiting for it", "statefulSet", name)
	if plugin.Status.ObservedGeneration == plugin.Generation {
		switch plugin.Status.Phase {
		case neo4jv1beta1.PhaseReady, neo4jv1beta1.PhaseDisabled:
			return ctrl.Result{}, nil
		case neo4jv1beta1.PhaseInstalling, neo4jv1beta1.PhaseWaiting, neo4jv1beta1.PhaseFailed:
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
	}
	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseWaiting,
		fmt.Sprintf("Waiting for StatefulSet %s of %s to be created", name, targetName(deployment)))
	return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
}
