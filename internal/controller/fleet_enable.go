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

// Turning Aura Fleet Management on for a running deployment rolls its servers
// once (operations.md id 226). The enable changes two things that restart
// them: neo4j.conf (Fleet Management's procedure lines, id 192) and
// NEO4J_PLUGINS (the fleet-management name, id 175). On a cluster without
// spec.env NEO4J_PLUGINS they were two writes in the same second — the
// ConfigMap's restart stamp, then the fleet merge — two controllerrevisions,
// and a slower StatefulSet controller could restart a pod twice (#608). The
// disable already rode the stamp (id 222), and so did an enable of a cluster
// whose own spec.env sets the variable (id 223).
//
//   - Cluster: the restart stamp (triggerRollingRestartForConfigChange) adds
//     the name, recorded as Fleet Management's, in its write
//     (addFleetEntryInWrite). The merge later in the pass finds it listed; it
//     waits while a conf roll is due (confRollPending) and writes against a
//     read from the API server (mergeFleetManagementPlugin).
//   - Standalone: the StatefulSet write that carries the new conf hash adds
//     the name already (addFleetEntryInWrite in reconcileStatefulSet), so a
//     pass on a fresh cache was one write. On a ConfigMap read from a cache
//     that had not seen the pass's write it stamped the old conf, and the
//     merge then added the name alone: two rolls. The merge now waits while
//     the conf the StatefulSet stands for is not the one the standalone
//     renders (standaloneConfRollDue), as the disable's removal does
//     (standaloneFleetEntryDue).

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// standaloneConfRollDue reports whether the standalone's next StatefulSet
// write carries a new conf hash — and so adds fleet-management itself
// (addFleetEntryInWrite) — rather than the fleet merge: sts, the StatefulSet as
// the merge reads it, records the semantic hash of a neo4j.conf that is not the
// one the standalone renders now (renderedConfSemantic). Turning Fleet
// Management on changes the conf, so its roll is that write; the merge waits
// for it. A StatefulSet without the record (one an earlier operator wrote), or
// a conf that cannot be rendered, is not waited for: the merge writes the name
// as before.
func (r *Neo4jEnterpriseStandaloneReconciler) standaloneConfRollDue(ctx context.Context,
	standalone *neo4jv1beta1.Neo4jEnterpriseStandalone, sts *appsv1.StatefulSet) bool {
	recorded := sts.Annotations[standaloneConfigSemanticHashAnnotation]
	if recorded == "" {
		return false
	}
	rendered, err := r.renderedConfSemantic(ctx, standalone)
	if err != nil {
		log.FromContext(ctx).V(1).Info("Cannot render the standalone's neo4j.conf; fleet-management is added without waiting for its roll",
			"error", err.Error())
		return false
	}
	return rendered != recorded
}
