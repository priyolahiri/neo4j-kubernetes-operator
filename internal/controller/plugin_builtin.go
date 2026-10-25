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

// A Managed fleet-management Neo4jPlugin on an image with Fleet Manager built
// in (2026.03 and later) installs nothing, and is Ready (operations.md id
// 192). The image's entrypoint cannot install the plugin there — its
// neo4j-plugins.json names it with no versions URL and products/ has no JAR,
// so it wgets "null" and starts without it — and the procedures it would have
// brought (fleetManagement.status, fleetManagement.registerToken, …) are the
// server's own. Refusing it, as a name the image cannot install is refused
// (id 170), would say a working deployment is Failed, and the refusal's repair
// would restart every server to take off a name the entrypoint skips; a name
// an earlier operator wrote stays, as Aura Fleet Management's own does
// (addFleetManagementPlugin).

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// fleetManagementBuiltInFor reports whether plugin is a Managed, enabled
// fleet-management plugin whose target's image has Fleet Manager built in.
func fleetManagementBuiltInFor(plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) bool {
	return plugin.Spec.Enabled && !shouldSkipNeo4jPluginsEnv(plugin) &&
		validation.EntrypointPluginName(plugin.Spec.Name) == resources.FleetManagementPlugin &&
		validation.FleetManagementBuiltIn(targetImageTag(deployment))
}

// reconcileBuiltIn records a fleet-management plugin on an image with Fleet
// Manager built in as Ready, writing nothing to its target. It is not
// requeued: the target's image can only move on (a CalVer is never
// downgraded), and an edit of the plugin reconciles it again.
func (r *Neo4jPluginReconciler) reconcileBuiltIn(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (ctrl.Result, error) {
	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, fmt.Sprintf(
		"Fleet Manager is built into neo4j:%s: the plugin installs nothing on %s (NEO4J_PLUGINS does not list it); "+
			"Aura Fleet Management needs no Neo4jPlugin there", targetImageTag(deployment), targetName(deployment)))
	return ctrl.Result{}, nil
}
