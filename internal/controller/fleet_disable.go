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

// Turning Aura Fleet Management off (spec.auraFleetManagement.enabled: false,
// or the block removed) takes fleet-management out of the servers'
// NEO4J_PLUGINS again: the entry Fleet Management's own path put there, and
// only that one (operations.md id 222). Both controllers used to merge the
// name only while the feature was enabled, and the merge only adds, so the
// JAR stayed on the servers for good.
//
// The record. The path that adds the name — the write that creates the
// StatefulSet (id 175) and every pass's merge — records it on the server
// StatefulSet under neo4j.com/aura-fleet-management-entries, StatefulSet
// metadata that restarts nothing. A name another writer put there is not
// recorded: a Neo4jPlugin records its own names (neo4j.com/plugin-entries.<
// plugin>, id 155), and a NEO4J_PLUGINS the deployment's spec.env sets is the
// deployment's own.
//
// A deployment an earlier operator gave the name has no record. While Fleet
// Management is enabled, a pass adopts the entry — records it, with a
// metadata patch — when NEO4J_PLUGINS lists it, the variable is not the
// deployment's own and no Neo4jPlugin claims the name. An entry nobody
// recorded on a deployment whose Fleet Management was already off when the
// operator was upgraded is left alone: it cannot be told from one a user put
// there, and is taken out by hand.
//
// The removal. Once the spec no longer installs fleet-management, an entry the
// record lists leaves NEO4J_PLUGINS and the record goes; the entry stays, and
// only the record goes, when a Neo4jPlugin claims the name or NEO4J_PLUGINS is
// the deployment's own. Disabling also changes neo4j.conf (Fleet Management's
// procedure lines go), and that change rolls the servers: the removal goes in
// the write that rolls them — a cluster's ConfigMap restart stamp
// (triggerRollingRestartForConfigChange), a standalone's StatefulSet write
// carrying its new conf hash — so a disable is one roll. A removal no conf
// change carries is written alone, once the conf is settled. Like the conf
// change and the merge that adds the name, it waits for nothing a plugin waits
// for: it is the deployment's own spec change. From 2026.03 (Fleet Manager
// built in) the same: the entry the merge keeps there while enabled (id 192)
// goes with the disable's roll.

import (
	"context"
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// fleetEntriesAnnotation is Aura Fleet Management's record, on the server
// StatefulSet (a cluster's {name}-server, a standalone's {name}), of the
// NEO4J_PLUGINS name its own path put there: "fleet-management", sorted and
// comma-separated like a plugin's neo4j.com/plugin-entries.<plugin>. It is
// StatefulSet metadata, not pod-template metadata, so writing it restarts
// nothing.
const fleetEntriesAnnotation = "neo4j.com/aura-fleet-management-entries"

// fleetEntryRecorded reports whether sts's record lists fleet-management.
func fleetEntryRecorded(sts *appsv1.StatefulSet) bool {
	_, ok := splitCSVSet(sts.Annotations[fleetEntriesAnnotation])[fleetManagementPluginEntry]
	return ok
}

// setFleetEntryRecord writes the record on sts, or removes it.
func setFleetEntryRecord(sts *appsv1.StatefulSet, recorded bool) {
	if !recorded {
		delete(sts.Annotations, fleetEntriesAnnotation)
		return
	}
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[fleetEntriesAnnotation] = fleetManagementPluginEntry
}

// fleetEntryListed reports whether the template's neo4j container lists
// fleet-management in NEO4J_PLUGINS.
func fleetEntryListed(template *corev1.PodTemplateSpec) bool {
	container := neo4jContainer(&template.Spec)
	return container != nil && slices.Contains(neo4jPluginsList(container.Env), fleetManagementPluginEntry)
}

// neo4jPluginsOwnedByTarget reports whether the deployment renders
// NEO4J_PLUGINS itself (its spec.env sets it): the variable is then the
// user's, recorded as the cluster's or the standalone's own, and Fleet
// Management never records or removes a name in it.
func neo4jPluginsOwnedByTarget(sts *appsv1.StatefulSet) bool {
	_, own := targetOwnedEnvNames(sts)["NEO4J_PLUGINS"]
	return own
}

// fleetEntryClaimed reports whether a Neo4jPlugin puts fleet-management in
// sts's NEO4J_PLUGINS: a plugin's record lists the name (pluginEntriesAnnotation
// — a disabled plugin's too, until its own removal takes it away, id 160), or a
// plugin of the target adds it: named target, on its image (pluginOnImage:
// enabled, not refused), not being deleted, Managed, with fleet-management as
// its spec.name or a dependency's, as the entrypoint knows them. A plugin's
// own removal keeps the name the same way while spec.auraFleetManagement
// installs it (withoutPluginEntries), so neither takes away what the other
// still needs.
func fleetEntryClaimed(sts *appsv1.StatefulSet, plugins []neo4jv1beta1.Neo4jPlugin, target, imageTag string) bool {
	for key, value := range sts.Annotations {
		if !strings.HasPrefix(key, pluginEntriesAnnotationPrefix) {
			continue
		}
		if _, ok := splitCSVSet(value)[fleetManagementPluginEntry]; ok {
			return true
		}
	}
	for i := range plugins {
		p := &plugins[i]
		if p.Spec.ClusterRef != target || p.DeletionTimestamp != nil || !pluginOnImage(p, imageTag) || shouldSkipNeo4jPluginsEnv(p) {
			continue
		}
		if validation.EntrypointPluginName(p.Spec.Name) == fleetManagementPluginEntry {
			return true
		}
		for _, dep := range p.Spec.Dependencies {
			if validation.EntrypointPluginName(dep.Name) == fleetManagementPluginEntry {
				return true
			}
		}
	}
	return false
}

// fleetEntryClaimedOn is fleetEntryClaimed with the plugins of sts's namespace
// listed from c; the list is read only when no plugin's record settles it.
func fleetEntryClaimedOn(ctx context.Context, c client.Reader, sts *appsv1.StatefulSet, target, imageTag string) (bool, error) {
	if fleetEntryClaimed(sts, nil, target, imageTag) {
		return true, nil
	}
	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := c.List(ctx, plugins, client.InNamespace(sts.Namespace)); err != nil {
		return false, fmt.Errorf("listing Neo4jPlugins in %s: %w", sts.Namespace, err)
	}
	return fleetEntryClaimed(sts, plugins.Items, target, imageTag), nil
}

// removeFleetEntry takes fleet-management out of the template's neo4j
// container's NEO4J_PLUGINS and reports whether it was there. Like
// addFleetManagementPlugin it writes a new env list, never into the one the
// template had; the containers array is the caller's to copy. A last entry
// leaves "[]", which the entrypoint reads as nothing to install
// (RemoveFromNeo4jPluginList).
func removeFleetEntry(template *corev1.PodTemplateSpec) (bool, error) {
	container := neo4jContainer(&template.Spec)
	if container == nil {
		return false, nil
	}
	for i, e := range container.Env {
		if e.Name != "NEO4J_PLUGINS" || e.ValueFrom != nil {
			continue
		}
		updated, err := RemoveFromNeo4jPluginList(e.Value, fleetManagementPluginEntry)
		if err != nil {
			return false, err
		}
		if updated == e.Value {
			return false, nil
		}
		env := append([]corev1.EnvVar(nil), container.Env...)
		env[i].Value = updated
		container.Env = env
		return true, nil
	}
	return false, nil
}

// dropFleetEntry is the disable's edit of sts, a StatefulSet the caller owns,
// when its record lists fleet-management: the record goes, and the name
// leaves the neo4j container's NEO4J_PLUGINS unless a Neo4jPlugin claims it
// (claimed) or NEO4J_PLUGINS is the deployment's own. It reports whether it
// changed sts, and whether it changed the pod template — a roll of the
// servers. A NEO4J_PLUGINS that is not a JSON list is left as it is (the
// entrypoint cannot read it either), and the record goes all the same, so no
// later pass tries again.
func dropFleetEntry(ctx context.Context, sts *appsv1.StatefulSet, claimed bool) (changed, rolls bool) {
	if !fleetEntryRecorded(sts) {
		return false, false
	}
	if !claimed && !neo4jPluginsOwnedByTarget(sts) {
		removed, err := removeFleetEntry(&sts.Spec.Template)
		if err != nil {
			log.FromContext(ctx).Info("NEO4J_PLUGINS is not a JSON list; leaving it as it is and forgetting Aura Fleet Management's entry",
				"statefulSet", sts.Name, "reason", err.Error())
		}
		rolls = removed
	}
	setFleetEntryRecord(sts, false)
	return true, rolls
}

// fleetDisableEdit is dropFleetEntry on the StatefulSet of target, a
// deployment whose spec no longer installs fleet-management, with the claim
// read from its plugins (fleetEntryClaimedOn). Without a record nothing is
// read and nothing changes. An error is the plugin list's: sts is unchanged.
func fleetDisableEdit(ctx context.Context, c client.Reader, sts *appsv1.StatefulSet, target, imageTag string) (changed, rolls bool, err error) {
	if !fleetEntryRecorded(sts) {
		return false, false, nil
	}
	claimed, err := fleetEntryClaimedOn(ctx, c, sts, target, imageTag)
	if err != nil {
		return false, false, err
	}
	changed, rolls = dropFleetEntry(ctx, sts, claimed)
	return changed, rolls, nil
}

// fleetRecordUpkeep keeps sts's record in line with what Fleet Management's
// own path keeps in NEO4J_PLUGINS while the deployment's spec installs it: it
// adopts an entry without a record — listed, the variable not the
// deployment's own, the name claimed by no Neo4jPlugin — which an earlier
// operator added, or which the merge keeps on an image with Fleet Manager built
// in (id 192); and it drops a record whose entry is gone. It reports whether
// it changed sts's annotations; the pod template is never touched.
func fleetRecordUpkeep(ctx context.Context, c client.Reader, sts *appsv1.StatefulSet, target, imageTag string) (bool, error) {
	listed, recorded := fleetEntryListed(&sts.Spec.Template), fleetEntryRecorded(sts)
	switch {
	case recorded && !listed:
		setFleetEntryRecord(sts, false)
		return true, nil
	case !recorded && listed && !neo4jPluginsOwnedByTarget(sts):
		claimed, err := fleetEntryClaimedOn(ctx, c, sts, target, imageTag)
		if err != nil || claimed {
			return false, err
		}
		setFleetEntryRecord(sts, true)
		return true, nil
	}
	return false, nil
}

// keepFleetRecord brings the record on the StatefulSet in line with
// fleetRecordUpkeep, for a deployment whose spec installs fleet-management,
// with a patch of its metadata alone, which restarts nothing. cached is the
// StatefulSet as the cache has it, and decides whether anything is due; the
// patch is made against a read from reader (the API server), so a cache that
// has not seen this pass's writes neither records what is not there nor
// meets a Conflict. Bookkeeping: a failure is logged at V(1) and left to the
// next pass, never a failure of the pass.
func keepFleetRecord(ctx context.Context, c client.Client, reader client.Reader, cached *appsv1.StatefulSet, target, imageTag string) {
	logger := log.FromContext(ctx)
	fail := func(err error) {
		logger.V(1).Info("Could not update the record of Aura Fleet Management's NEO4J_PLUGINS entry; trying again on the next pass",
			"statefulSet", cached.Name, "error", err.Error())
	}
	if changed, err := fleetRecordUpkeep(ctx, c, cached.DeepCopy(), target, imageTag); err != nil || !changed {
		if err != nil {
			fail(err)
		}
		return
	}
	live := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(cached), live); err != nil {
		fail(err)
		return
	}
	edited := live.DeepCopy()
	changed, err := fleetRecordUpkeep(ctx, c, edited, target, imageTag)
	if err == nil && changed {
		err = c.Patch(ctx, edited, mergePatchSince(live))
	}
	if err != nil {
		fail(err)
	}
}

// reconcileFleetEntry keeps Aura Fleet Management's NEO4J_PLUGINS entry in
// line with the cluster's spec, after the pass's template apply: while the
// spec installs fleet-management it keeps the record (keepFleetRecord —
// adopting an entry an earlier operator added), once it no longer does it
// takes the recorded entry out (removeFleetManagementPlugin). Run after the
// template apply, a metadata patch never makes that apply conflict with a
// cache that has not seen it. Nothing in the first step of a two-step
// recreate (restorePending, id 171).
func (r *Neo4jEnterpriseClusterReconciler) reconcileFleetEntry(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	key := types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}
	cached := &appsv1.StatefulSet{}
	if err := r.Get(ctx, key, cached); err != nil {
		return
	}
	if _, pending := restorePending(cached); pending {
		return
	}
	if clusterInstallsFleetManagement(cluster) {
		keepFleetRecord(ctx, r.Client, r.apiReader(), cached, cluster.Name, cluster.Spec.Image.Tag)
		return
	}
	r.removeFleetManagementPlugin(ctx, cluster, cached)
}

// removeFleetManagementPlugin is the cluster's write of the disable's edit
// (fleetDisableEdit) that no conf change carries. Turning Fleet Management off
// changes neo4j.conf, and the ConfigMap's restart stamp, written earlier in
// the pass, carries the edit (triggerRollingRestartForConfigChange); so this
// waits while a conf roll is due (confRollPending) and runs in the passes
// after, where it finds the record gone and writes nothing. Not in the first
// step of a two-step recreate (restorePending, id 171): nothing but its second
// step is written then. cached, the server StatefulSet as the cache has it,
// decides whether anything is due; the write is made against a read from the
// API server, since the cache may not have the stamp yet. Non-fatal: a
// failure is logged at info once per cause, at V(1) in between, and the next
// pass tries again.
func (r *Neo4jEnterpriseClusterReconciler) removeFleetManagementPlugin(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, cached *appsv1.StatefulSet) {
	if clusterInstallsFleetManagement(cluster) || !fleetEntryRecorded(cached) {
		return
	}
	if _, pending := restorePending(cached); pending {
		return
	}
	key := client.ObjectKeyFromObject(cached)
	if r.ConfigMapManager != nil && r.ConfigMapManager.confRollPending(ctx, cluster) {
		return
	}
	logger := log.FromContext(ctx)
	var rolled *appsv1.StatefulSet
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rolled = nil
		sts := &appsv1.StatefulSet{}
		if err := r.apiReader().Get(ctx, key, sts); err != nil {
			return client.IgnoreNotFound(err)
		}
		if _, pending := restorePending(sts); pending {
			return nil
		}
		before := sts.DeepCopy()
		changed, rolls, err := fleetDisableEdit(ctx, r.Client, sts, cluster.Name, cluster.Spec.Image.Tag)
		if err != nil || !changed {
			return err
		}
		if !rolls {
			return r.Patch(ctx, sts, mergePatchSince(before))
		}
		if err := r.Update(ctx, sts); err != nil {
			return err
		}
		rolled = sts
		return nil
	})
	clusterKey := client.ObjectKeyFromObject(cluster).String()
	if err != nil {
		logRefusal(logger, r.fleetEntryRemovalFailures.changed(clusterKey, err.Error()),
			"Could not take Aura Fleet Management's entry out of NEO4J_PLUGINS; trying again on the next pass", err)
		return
	}
	r.fleetEntryRemovalFailures.forget(clusterKey)
	if rolled != nil {
		logger.Info("Took fleet-management out of NEO4J_PLUGINS: spec.auraFleetManagement no longer installs it",
			"statefulSet", key.Name)
		// The rollout checks later in the pass read the cache (id 141).
		r.noteServerStatefulSetWrite(cluster, rolled)
	}
}

// standaloneFleetEntryDue reports whether this pass's write of the
// standalone's StatefulSet makes the disable's edit (dropFleetEntry), and
// whether a Neo4jPlugin claims the name: the standalone's spec no longer
// installs fleet-management, existing's record lists it, and the write carries
// the neo4j.conf the standalone renders now — confSemantic, what this pass's
// config-hash stamp stands for (renderedConfStamp), is the semantic hash of
// that conf. Turning Fleet Management off changes the conf, so the edit rides
// the write that rolls the pod for it; with no conf change it goes alone. A
// ConfigMap read from a cache that has not seen this pass's write still
// stamps the conf before, and the edit then waits for the pass that stamps
// the new one, so the pod does not restart twice. A plugin list that cannot be
// read leaves it to a later pass.
func (r *Neo4jEnterpriseStandaloneReconciler) standaloneFleetEntryDue(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone,
	existing *appsv1.StatefulSet, confSemantic string) (due, claimed bool) {
	if standaloneInstallsFleetManagement(standalone) || !fleetEntryRecorded(existing) || confSemantic == "" {
		return false, false
	}
	logger := log.FromContext(ctx)
	desired, err := desiredStandaloneConfigMap(ctx, r.Client, standalone)
	if err != nil {
		logger.V(1).Info("Cannot render the standalone's neo4j.conf; Aura Fleet Management's entry is taken out on a later pass", "error", err.Error())
		return false, false
	}
	live := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: standalone.Name + "-config", Namespace: standalone.Namespace}, live); err != nil {
		return false, false
	}
	if standaloneConfSemanticHash(mergeStandaloneConf(desired.Data["neo4j.conf"], live)) != confSemantic {
		return false, false
	}
	claimed, err = fleetEntryClaimedOn(ctx, r.Client, existing, standalone.Name, standalone.Spec.Image.Tag)
	if err != nil {
		logger.V(1).Info("Cannot tell whether a Neo4jPlugin installs fleet-management; Aura Fleet Management's entry is taken out on a later pass", "error", err.Error())
		return false, false
	}
	return true, claimed
}
