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

// What a Neo4jPlugin owns on its target's StatefulSet, and how it takes that
// away again (operations.md id 155). A plugin writes a variable per spec.config
// key onto the servers (NEO4J_<setting>); the cluster and standalone
// controllers keep every variable they did not render, so nothing else ever
// removes one. Each plugin records the variables it sets, as the cluster
// controller does in neo4j.com/cluster-controller-env-vars, and a write drops
// the ones it recorded and no longer sets; its uninstall takes away everything
// it wrote.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// pluginEnvVarsAnnotationPrefix starts the key of every plugin's record on its
// target's StatefulSet (pluginEnvVarsAnnotation).
const pluginEnvVarsAnnotationPrefix = "neo4j.com/plugin-env-vars."

// pluginEnvVarsAnnotation is the key under which the Neo4jPlugin named plugin
// records, on its target's StatefulSet, the variables it sets on the neo4j
// container: sorted and comma-separated, like neo4j.com/cluster-controller-env-
// vars. It is StatefulSet metadata, not pod-template metadata, so writing it
// restarts nothing. The name part of an annotation key holds 63 characters; a
// longer plugin name is cut and ended with a hash of the whole name.
func pluginEnvVarsAnnotation(plugin string) string {
	room := 63 - len(strings.TrimPrefix(pluginEnvVarsAnnotationPrefix, "neo4j.com/"))
	if len(plugin) <= room {
		return pluginEnvVarsAnnotationPrefix + plugin
	}
	sum := sha256.Sum256([]byte(plugin))
	return pluginEnvVarsAnnotationPrefix + plugin[:room-9] + "-" + hex.EncodeToString(sum[:4])
}

// pluginOwnedEnvNames returns the variables the plugin sets on its target's
// neo4j container as a whole (applyPluginEnv): one per spec.config key it puts
// in the env — on a standalone not the keys that go to neo4j.conf (id 114) —
// and, on a cluster, a security setting that is not a list. The procedure and
// extension lists (resources.IsAdditiveConfKey) are shared: a plugin adds its
// tokens to them and never owns one.
func (r *Neo4jPluginReconciler) pluginOwnedEnvNames(plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) map[string]struct{} {
	standalone := deployment.Type == "standalone"
	owned := map[string]struct{}{}
	for key := range plugin.Spec.Config {
		if resources.IsAdditiveConfKey(key) || (standalone && pluginConfKey(key)) {
			continue
		}
		owned[resources.Neo4jSettingEnvVarName(key)] = struct{}{}
	}
	if !standalone {
		for key := range r.pluginSecuritySettings(plugin) {
			if !resources.IsAdditiveConfKey(key) {
				owned[resources.Neo4jSettingEnvVarName(key)] = struct{}{}
			}
		}
	}
	return owned
}

// otherPlugins lists the Neo4jPlugins of plugin's target other than plugin:
// same namespace and clusterRef, not being deleted, and not a duplicate of it
// (same spec.name — checkForDuplicatePlugin refuses those, so they write
// nothing).
func (r *Neo4jPluginReconciler) otherPlugins(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin) ([]neo4jv1beta1.Neo4jPlugin, error) {
	list := &neo4jv1beta1.Neo4jPluginList{}
	if err := r.List(ctx, list, client.InNamespace(plugin.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list Neo4jPlugins in namespace %q: %w", plugin.Namespace, err)
	}
	var out []neo4jv1beta1.Neo4jPlugin
	for _, other := range list.Items {
		if other.Name == plugin.Name || other.Spec.ClusterRef != plugin.Spec.ClusterRef ||
			other.DeletionTimestamp != nil || other.Spec.Name == plugin.Spec.Name {
			continue
		}
		out = append(out, other)
	}
	return out, nil
}

// targetOwnedEnvNames returns the variables the target's own controller
// renders onto sts (the cluster's or the standalone's record): never the
// plugin's to remove or edit.
func targetOwnedEnvNames(sts *appsv1.StatefulSet) map[string]struct{} {
	owned := readOwnedEnvVarNames(sts)
	for name := range splitCSVSet(sts.Annotations[standaloneOwnedEnvVarsAnnotation]) {
		owned[name] = struct{}{}
	}
	return owned
}

// envOwnedElsewhere returns the variables on sts that plugin must leave when it
// stops setting them: the target's own (targetOwnedEnvNames), those another
// plugin records, and those another plugin of the target sets — also when that
// plugin has not recorded them yet (installed by an earlier operator).
func (r *Neo4jPluginReconciler) envOwnedElsewhere(sts *appsv1.StatefulSet, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo, others []neo4jv1beta1.Neo4jPlugin) map[string]struct{} {
	out := targetOwnedEnvNames(sts)
	mine := pluginEnvVarsAnnotation(plugin.Name)
	for key, value := range sts.Annotations {
		if key == mine || !strings.HasPrefix(key, pluginEnvVarsAnnotationPrefix) {
			continue
		}
		for name := range splitCSVSet(value) {
			out[name] = struct{}{}
		}
	}
	for i := range others {
		for name := range r.pluginOwnedEnvNames(&others[i], deployment) {
			out[name] = struct{}{}
		}
	}
	return out
}

// applyPlugin makes on sts, a copy of the target's StatefulSet, every edit the
// plugin owns: those on the pod template (applyPluginToTemplate), the removal
// of the variables its record lists and it no longer sets, and the record of
// those it sets now. It returns, sorted, the variables it removed.
//
// A variable is removed only when the plugin recorded it: a plugin installed
// by an earlier operator has no record, and its first pass records what it
// sets and removes nothing — a variable a key removed under that operator left
// behind cannot be told from one someone else set. Nor is one removed that is
// owned elsewhere (envOwnedElsewhere), or a shared list.
//
// installPluginViaEnvironment writes what it produces; missingFromTarget,
// installChangesTarget and selfRepair compare against it, so none of them can
// disagree about what the plugin's write changes.
func (r *Neo4jPluginReconciler) applyPlugin(ctx context.Context, sts *appsv1.StatefulSet, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) ([]string, error) {
	if err := r.applyPluginToTemplate(&sts.Spec.Template, plugin, deployment); err != nil {
		return nil, err
	}
	others, err := r.otherPlugins(ctx, plugin)
	if err != nil {
		return nil, err
	}
	key := pluginEnvVarsAnnotation(plugin.Name)
	want := r.pluginOwnedEnvNames(plugin, deployment)
	stale := map[string]struct{}{}
	for name := range splitCSVSet(sts.Annotations[key]) {
		if _, still := want[name]; !still {
			stale[name] = struct{}{}
		}
	}
	removed := withoutEnvNames(&sts.Spec.Template, stale, r.envOwnedElsewhere(sts, plugin, deployment, others))
	setPluginEnvRecord(sts, key, want)
	return removed, nil
}

// withoutEnvNames removes from the template's neo4j container the variables
// names lists, except those keep lists and the shared lists; it returns, sorted,
// the names it removed.
func withoutEnvNames(template *corev1.PodTemplateSpec, names, keep map[string]struct{}) []string {
	container := neo4jContainer(&template.Spec)
	if container == nil || len(names) == 0 {
		return nil
	}
	drop := func(name string) bool {
		_, listed := names[name]
		_, kept := keep[name]
		return listed && !kept && !resources.IsAdditiveConfEnvName(name)
	}
	removed := map[string]struct{}{}
	env := container.Env[:0:0]
	for _, e := range container.Env {
		if drop(e.Name) {
			removed[e.Name] = struct{}{}
			continue
		}
		env = append(env, e)
	}
	if len(removed) == 0 {
		return nil
	}
	container.Env = env
	out := make([]string, 0, len(removed))
	for name := range removed {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// setPluginEnvRecord writes the record key on sts: the names in owned, or no
// record when there are none.
func setPluginEnvRecord(sts *appsv1.StatefulSet, key string, owned map[string]struct{}) {
	if len(owned) == 0 {
		delete(sts.Annotations, key)
		return
	}
	names := make([]string, 0, len(owned))
	for name := range owned {
		names = append(names, name)
	}
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[key] = ownedKeysCSV(names)
}

// recordPluginEnv writes, alone, the plugin's record on its target's
// StatefulSet when it is not what the plugin sets: on a settled plugin's first
// pass after an upgrade from an operator that kept none. It is a patch of the
// StatefulSet's metadata, which restarts nothing and does not wake the plugin
// (podTemplateChanged); the pod template is left as it is, whatever the
// plugin's edits would make of it.
func (r *Neo4jPluginReconciler) recordPluginEnv(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return client.IgnoreNotFound(err)
	}
	desired := sts.DeepCopy()
	if _, err := r.applyPlugin(ctx, desired, plugin, deployment); err != nil {
		return err
	}
	key := pluginEnvVarsAnnotation(plugin.Name)
	want, wanted := desired.Annotations[key]
	have, had := sts.Annotations[key]
	if want == have && wanted == had {
		return nil
	}
	return patchObject(ctx, r.Client, sts, func() {
		if !wanted {
			delete(sts.Annotations, key)
			return
		}
		if sts.Annotations == nil {
			sts.Annotations = map[string]string{}
		}
		sts.Annotations[key] = want
	})
}

// removePluginFromTarget is the uninstall: it takes off the target's
// StatefulSet everything the plugin wrote (withoutPlugin), in one write — a
// patch of the record alone when the pod template keeps nothing of the
// plugin's. It used to be up to three writes, each a restart of the servers,
// and none of them took the plugin's settings away.
func (r *Neo4jPluginReconciler) removePluginFromTarget(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) error {
	stsKey := types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, stsKey, sts); err != nil {
			if errors.IsNotFound(err) {
				return nil // the target is gone: nothing to take away
			}
			return err
		}
		others, err := r.otherPlugins(ctx, plugin)
		if err != nil {
			return err
		}
		before := sts.DeepCopy()
		r.withoutPlugin(ctx, sts, plugin, deployment, others)
		if equality.Semantic.DeepEqual(before.Spec.Template, sts.Spec.Template) {
			if stringMapsEqual(before.Annotations, sts.Annotations) {
				return nil
			}
			return r.Patch(ctx, sts, mergePatchSince(before))
		}
		return r.Update(ctx, sts)
	})
}

// withoutPlugin takes off sts, the target's StatefulSet, everything the plugin
// wrote, and never what the target renders itself (targetOwnedEnvNames) or
// another plugin of the target needs:
//   - the variables it sets: those its record lists and those its spec sets
//     now (a plugin installed by an earlier operator has no record), less
//     those owned elsewhere (envOwnedElsewhere);
//   - its tokens in the procedure and extension lists, less those another
//     plugin of the target puts there; a list left empty goes;
//   - in Managed mode, its names in NEO4J_PLUGINS — the plugin, as the
//     entrypoint knows it (mapPluginName: "gds" is "graph-data-science"), and
//     its dependencies — less those another Managed plugin of the target
//     installs, and fleet-management while the target's
//     spec.auraFleetManagement installs it;
//   - its VerifiedDownload init container, its auth volume, and the shared CA
//     volume once nothing mounts it (withoutPluginInitContainer);
//   - its record.
func (r *Neo4jPluginReconciler) withoutPlugin(ctx context.Context, sts *appsv1.StatefulSet, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo, others []neo4jv1beta1.Neo4jPlugin) {
	key := pluginEnvVarsAnnotation(plugin.Name)
	targetOwned := targetOwnedEnvNames(sts)
	if container := neo4jContainer(&sts.Spec.Template.Spec); container != nil {
		names := splitCSVSet(sts.Annotations[key])
		for name := range r.pluginOwnedEnvNames(plugin, deployment) {
			names[name] = struct{}{}
		}
		withoutEnvNames(&sts.Spec.Template, names, r.envOwnedElsewhere(sts, plugin, deployment, others))
		container.Env = removePluginSecurityEnv(container.Env, r.pluginListTokens(plugin, others, targetOwned))
		if _, own := targetOwned["NEO4J_PLUGINS"]; !own && !shouldSkipNeo4jPluginsEnv(plugin) {
			env, err := r.withoutPluginEntries(container.Env, plugin, others, deployment)
			if err != nil {
				// Left as it is: a value the entrypoint cannot read either is
				// not the plugin's to rewrite, and the CR still goes.
				log.FromContext(ctx).Error(err, "Cannot take the plugin out of NEO4J_PLUGINS; leaving it", "plugin", plugin.Spec.Name)
			} else {
				container.Env = env
			}
		}
	}
	withoutPluginInitContainer(&sts.Spec.Template, plugin)
	delete(sts.Annotations, key)
}

// pluginListTokens returns, by setting, the tokens the plugin put in the
// procedure and extension lists (pluginSecurityRemovalSettings, and the lists
// its spec.config sets), less those another plugin of the target puts there,
// and without the lists the target renders itself.
func (r *Neo4jPluginReconciler) pluginListTokens(plugin *neo4jv1beta1.Neo4jPlugin, others []neo4jv1beta1.Neo4jPlugin, targetOwned map[string]struct{}) map[string]string {
	listTokens := func(p *neo4jv1beta1.Neo4jPlugin, settings map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range settings {
			if resources.IsAdditiveConfKey(k) {
				out[k] = v
			}
		}
		for k, v := range p.Spec.Config {
			if resources.IsAdditiveConfKey(k) {
				out[k] = resources.MergeConfListValues(out[k], v)
			}
		}
		return out
	}
	tokens := listTokens(plugin, r.pluginSecurityRemovalSettings(plugin))
	for i := range others {
		for k, v := range listTokens(&others[i], r.pluginSecuritySettings(&others[i])) {
			if mine, ok := tokens[k]; ok {
				tokens[k] = subtractCSV(mine, v)
			}
		}
	}
	for k, v := range tokens {
		_, own := targetOwned[resources.Neo4jSettingEnvVarName(k)]
		if v == "" || own {
			delete(tokens, k)
		}
	}
	return tokens
}

// withoutPluginEntries returns env with the plugin's names taken out of
// NEO4J_PLUGINS (withoutPlugin says which); env unchanged when none is listed.
func (r *Neo4jPluginReconciler) withoutPluginEntries(env []corev1.EnvVar, plugin *neo4jv1beta1.Neo4jPlugin, others []neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) ([]corev1.EnvVar, error) {
	keep := map[string]struct{}{}
	for i := range others {
		if shouldSkipNeo4jPluginsEnv(&others[i]) {
			continue
		}
		for _, name := range r.pluginEntries(&others[i]) {
			keep[name] = struct{}{}
		}
	}
	if fleetManagementEnabled(deployment) {
		keep["fleet-management"] = struct{}{}
	}
	out := make([]corev1.EnvVar, len(env))
	copy(out, env)
	for i := range out {
		if out[i].Name != "NEO4J_PLUGINS" || out[i].ValueFrom != nil {
			continue
		}
		value := out[i].Value
		for _, name := range r.pluginEntries(plugin) {
			if _, kept := keep[name]; kept {
				continue
			}
			var err error
			if value, err = RemoveFromNeo4jPluginList(value, name); err != nil {
				return env, err
			}
		}
		out[i].Value = value
	}
	return out, nil
}

// pluginEntries returns the names a Managed plugin adds to NEO4J_PLUGINS
// (applyPluginEnv): its own and its dependencies', as the entrypoint knows them.
func (r *Neo4jPluginReconciler) pluginEntries(plugin *neo4jv1beta1.Neo4jPlugin) []string {
	names := []string{r.mapPluginName(plugin.Spec.Name)}
	for _, dep := range plugin.Spec.Dependencies {
		names = append(names, r.mapPluginName(dep.Name))
	}
	return names
}

// fleetManagementEnabled reports whether the target installs fleet-management
// itself (spec.auraFleetManagement.enabled), which puts it in NEO4J_PLUGINS.
func fleetManagementEnabled(deployment *DeploymentInfo) bool {
	switch target := deployment.Object.(type) {
	case *neo4jv1beta1.Neo4jEnterpriseCluster:
		return target.Spec.AuraFleetManagement != nil && target.Spec.AuraFleetManagement.Enabled
	case *neo4jv1beta1.Neo4jEnterpriseStandalone:
		return target.Spec.AuraFleetManagement != nil && target.Spec.AuraFleetManagement.Enabled
	}
	return false
}

// withoutPluginInitContainer is the inverse of applyVerifiedDownload: it drops
// the plugin's init container from the template, its auth volume, and the
// shared CA volume once nothing mounts it — the cluster controller keeps
// volumes it did not render, so an orphaned one would stay, still projecting
// CA Secrets the pods then cannot start without once they are deleted — and
// takes the container out of the plugin controller's ownership annotation.
// Other VerifiedDownload plugins of the target keep theirs.
func withoutPluginInitContainer(template *corev1.PodTemplateSpec, plugin *neo4jv1beta1.Neo4jPlugin) {
	podSpec := &template.Spec
	containerName := resources.PluginInitContainerName(plugin.Spec.Name)
	authVolName := "plugin-auth-" + plugin.Spec.Name

	initContainers := podSpec.InitContainers[:0:0]
	for _, ic := range podSpec.InitContainers {
		if ic.Name != containerName {
			initContainers = append(initContainers, ic)
		}
	}
	if len(initContainers) != len(podSpec.InitContainers) {
		podSpec.InitContainers = initContainers
	}

	caUnused := !podSpecMountsVolume(podSpec, resources.PluginCAVolumeName)
	volumes := podSpec.Volumes[:0:0]
	for _, v := range podSpec.Volumes {
		if v.Name == authVolName || (caUnused && v.Name == resources.PluginCAVolumeName) {
			continue
		}
		volumes = append(volumes, v)
	}
	if len(volumes) != len(podSpec.Volumes) {
		podSpec.Volumes = volumes
	}

	if template.Annotations == nil {
		return
	}
	owned := parsePluginInitOwned(template.Annotations[PluginInitContainersAnnotation])
	if _, ok := owned[containerName]; !ok {
		return
	}
	delete(owned, containerName)
	if len(owned) == 0 {
		delete(template.Annotations, PluginInitContainersAnnotation)
	} else {
		template.Annotations[PluginInitContainersAnnotation] = formatPluginInitOwned(owned)
	}
}
