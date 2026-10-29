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

// A NEO4J_PLUGINS a deployment's own spec.env sets is the deployment's: its
// controller renders it and owns it on the StatefulSet (operations.md ids 135,
// 223). A Managed Neo4jPlugin whose name is in that list is Ready without
// writing the name. When the user takes the variable out of spec.env, the
// template write that stops owning it dropped the whole variable — keeping
// only fleet-management, which Aura Fleet Management puts back in the same
// write (id 227) — and the plugin's name went with it. The plugin then raised
// PluginMissing and wrote its name back: a second roll of the servers for one
// edit (#619 S7, the v1.24.0 journey's GDS cluster).
//
// That write now hands the variable to the plugins, as the write that moves a
// shared list into neo4j.conf does (handOverSharedLists, id 209): it keeps
// the names the target's installed plugins add and records them as each
// plugin's (neo4j.com/plugin-entries.<plugin>), so each plugin finds its names
// and its record in place and writes nothing (operations.md id 233).

import (
	"context"
	"encoding/json"
	"slices"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// handOverNeo4jPlugins returns merged — the neo4j container's env a
// deployment's template write is about to write — with NEO4J_PLUGINS given to
// the target's plugins when this write stops owning it: the deployment owned
// it (previousOwned), the live env (current) has it as a plain value, and the
// deployment's desired env no longer sets it, so mergeEnvVars left it out of
// merged. The variable keeps the names the target's installed plugins add
// (installedPluginEntries): in the order the live list had them, then any it
// lacked. Each plugin's names are recorded under its pluginEntriesAnnotation
// on sts, as its own write records them (applyPluginEntries).
//
// With no installed plugin the variable goes, as before. When the plugins
// cannot be listed it goes too, and each plugin writes its own name back, a
// second roll, as before.
func handOverNeo4jPlugins(ctx context.Context, c client.Reader, sts *appsv1.StatefulSet, target, imageTag string,
	merged, current, desired []corev1.EnvVar, previousOwned map[string]struct{}) []corev1.EnvVar {
	const name = "NEO4J_PLUGINS"
	if _, owned := previousOwned[name]; !owned {
		return merged
	}
	if _, still := envByName(desired)[name]; still {
		return merged
	}
	if _, kept := envByName(merged)[name]; kept {
		return merged
	}
	live, ok := envByName(current)[name]
	if !ok || live.ValueFrom != nil {
		return merged
	}
	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := c.List(ctx, plugins, client.InNamespace(sts.Namespace)); err != nil {
		log.FromContext(ctx).V(1).Info("Cannot list the deployment's plugins; each writes its NEO4J_PLUGINS names back itself",
			"statefulSet", sts.Name, "error", err.Error())
		return merged
	}
	entries := installedPluginEntries(sts, plugins.Items, target, imageTag)
	if len(entries) == 0 {
		return merged
	}

	wanted := map[string]struct{}{}
	owners := make([]string, 0, len(entries))
	for owner, names := range entries {
		owners = append(owners, owner)
		for _, n := range names {
			wanted[n] = struct{}{}
		}
	}
	sort.Strings(owners)
	var names []string
	for _, n := range neo4jPluginsList([]corev1.EnvVar{live}) {
		if _, ok := wanted[n]; ok && !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	for _, owner := range owners {
		for _, n := range entries[owner] {
			if !slices.Contains(names, n) {
				names = append(names, n)
			}
		}
	}
	value, err := json.Marshal(names)
	if err != nil {
		return merged
	}
	for _, owner := range owners {
		record := map[string]struct{}{}
		for _, n := range entries[owner] {
			record[n] = struct{}{}
		}
		setPluginEnvRecord(sts, pluginEntriesAnnotation(owner), record)
	}
	log.FromContext(ctx).Info("NEO4J_PLUGINS is no longer the deployment's own; keeping its plugins' names in the same write",
		"statefulSet", sts.Name, "names", string(value))
	return append(merged, corev1.EnvVar{Name: name, Value: string(value)})
}

// installedPluginEntries returns, by plugin, the NEO4J_PLUGINS names each
// Neo4jPlugin of target installs on sts — its spec.name and its dependencies',
// as the entrypoint knows them (pluginEntries) — for the plugins that are
// installed: named target, on its image (pluginOnImage: enabled, not
// refused), not being deleted, Managed (shouldSkipNeo4jPluginsEnv), and Ready
// or with a record of their own on sts. A plugin that has not installed yet
// (one refused because the deployment's own list lacked its name, id 135)
// installs itself, behind its own gate (id 141).
func installedPluginEntries(sts *appsv1.StatefulSet, plugins []neo4jv1beta1.Neo4jPlugin, target, imageTag string) map[string][]string {
	out := map[string][]string{}
	for i := range plugins {
		p := &plugins[i]
		if p.Spec.ClusterRef != target || p.DeletionTimestamp != nil || !pluginOnImage(p, imageTag) || shouldSkipNeo4jPluginsEnv(p) {
			continue
		}
		_, env := sts.Annotations[pluginEnvVarsAnnotation(p.Name)]
		_, recorded := sts.Annotations[pluginEntriesAnnotation(p.Name)]
		if p.Status.Phase != neo4jv1beta1.PhaseReady && !env && !recorded {
			continue
		}
		names := []string{validation.EntrypointPluginName(p.Spec.Name)}
		for _, dep := range p.Spec.Dependencies {
			names = append(names, validation.EntrypointPluginName(dep.Name))
		}
		out[p.Name] = names
	}
	return out
}
