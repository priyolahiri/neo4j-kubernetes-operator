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

// A standalone's neo4j.conf, as its controller writes it: its own spec, its
// plugins' settings (#146), the procedure and extension lists of its spec.env,
// the image's plugin entries in the unrestricted list, and Neo4j's default
// allowlist against the image's fleet-management default (operations.md ids
// 192, 193). The standalone controller writes it (reconcileConfigMap); the
// plugin controller reads it to tell whether the allowlist hides a plugin's
// procedures, as the server will start with it (id 194).

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// desiredStandaloneConfigMap renders the standalone's ConfigMap as its
// controller writes it, before the keys someone else put in the live one are
// merged back (mergeStandaloneConf):
//   - its own spec (renderStandaloneConfigMap);
//   - the settings of its plugins, their lists joined with its own
//     (collectStandalonePluginConf, #146);
//   - the procedure and extension lists its spec.env sets as variables,
//     joined with those: the entrypoint writes a variable in place of the
//     conf's line, which on a standalone held its own and its plugins' entries
//     (resources.SpecEnvLists, ids 193, 209; a cluster's are in its
//     neo4j.conf too);
//   - Neo4j's default allowlist, *, where the image's entrypoint would set an
//     allowlist of fleetManagement.* alone (id 192);
//   - the image's entry for each plugin NEO4J_PLUGINS installs, in an
//     unrestricted list the conf sets and the standalone does not set itself
//     — in its spec.config or its spec.env (withImageUnrestrictedDefaults,
//     id 193).
//
// A plugin listing error is returned, never taken for "no plugins".
func desiredStandaloneConfigMap(ctx context.Context, c client.Reader, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) (*corev1.ConfigMap, error) {
	// The lists its spec.env sets as variables, joined here and left off its
	// StatefulSet: the entrypoint writes such a variable in place of the
	// conf's line, so one set by hand replaced every other entry of the list
	// (resources.SpecEnvLists; operations.md ids 193, 209).
	envLists := resources.SpecEnvLists(standalone.Spec.Env)
	_, envAllowlist := envLists[resources.AllowlistSetting]
	pluginConf, fleetPlugin, installs, err := collectStandalonePluginConf(ctx, c, standalone, envAllowlist)
	if err != nil {
		return nil, err
	}
	// Aura Fleet Management's allowlist entry is rendered only beside an
	// allowlist something else sets (operations.md id 192).
	_, pluginAllowlist := pluginConf[resources.AllowlistSetting]
	desired := renderStandaloneConfigMap(standalone, pluginAllowlist || envAllowlist)
	conf := desired.Data["neo4j.conf"]
	if len(pluginConf) > 0 {
		conf = resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(conf, pluginConf))
	}
	if len(envLists) > 0 {
		conf = resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(conf, envLists))
	}
	// The image's entrypoint narrows an allowlist set nowhere: for
	// fleet-management in NEO4J_PLUGINS it adds
	// dbms.security.procedures.allowlist=fleetManagement.* to a neo4j.conf
	// without the setting, and nothing else loads. Neo4j's default, *, in the
	// conf keeps it from doing so (operations.md id 192). Only where the
	// entrypoint installs the plugin: from 2026.03 it cannot, and adds nothing.
	if (standaloneInstallsFleetManagement(standalone) || fleetPlugin) && !validation.FleetManagementBuiltIn(standalone.Spec.Image.Tag) {
		if _, set := resources.Neo4jConfSettings(conf)[resources.AllowlistSetting]; !set {
			conf = resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(conf,
				map[string]string{resources.AllowlistSetting: "*"}))
		}
	}
	desired.Data["neo4j.conf"] = withImageUnrestrictedDefaults(standalone, conf, installs)
	return desired, nil
}

// collectStandalonePluginConf returns the union of neo4j.conf settings derived
// from every Neo4jPlugin on this standalone (#146): enabled, and not refused by
// its image (operations.md id 170); whether one of them is a Managed
// fleet-management plugin, which puts fleet-management in NEO4J_PLUGINS
// (operations.md id 192); and the names the standalone's NEO4J_PLUGINS
// installs (standaloneNeo4jPlugins). envAllowlist says whether its spec.env
// sets an allowlist. A list error is returned (not swallowed) so the caller
// can requeue without pruning plugin-owned keys from neo4j.conf.
func collectStandalonePluginConf(ctx context.Context, c client.Reader, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone,
	envAllowlist bool) (map[string]string, bool, []string, error) {
	// Cluster-first target resolution, mirroring the plugin controller's
	// getTargetDeployment: a Neo4jPlugin's clusterRef resolves to a same-named
	// Neo4jEnterpriseCluster when one exists, otherwise to the standalone. So if a
	// cluster of this name coexists in the namespace, plugins referencing the name
	// target the CLUSTER — fold none of them into this standalone's neo4j.conf.
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	switch err := c.Get(ctx, types.NamespacedName{Name: standalone.Name, Namespace: standalone.Namespace}, cluster); {
	case err == nil:
		return nil, false, standaloneNeo4jPlugins(standalone, nil), nil // a same-named cluster owns these plugins
	case !errors.IsNotFound(err):
		return nil, false, nil, fmt.Errorf("checking for a same-named cluster: %w", err)
	}

	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := c.List(ctx, plugins, client.InNamespace(standalone.Namespace)); err != nil {
		return nil, false, nil, fmt.Errorf("listing Neo4jPlugins in %s: %w", standalone.Namespace, err)
	}
	fleetPlugin := false
	for i := range plugins.Items {
		p := &plugins.Items[i]
		if p.Spec.ClusterRef == standalone.Name && p.DeletionTimestamp == nil && pluginOnImage(p, standalone.Spec.Image.Tag) &&
			!shouldSkipNeo4jPluginsEnv(p) && validation.EntrypointPluginName(p.Spec.Name) == resources.FleetManagementPlugin {
			fleetPlugin = true
		}
	}
	installs := standaloneNeo4jPlugins(standalone, plugins.Items)
	// A fleet-management plugin's automatic allowlist entry joins only an
	// allowlist the standalone's own spec.config, its spec.env or another
	// plugin sets: alone it loaded nothing else (operations.md id 192).
	// Computed both ways so an allowlist that has it keeps its order, and the
	// standalone's neo4j.conf does not change where nothing else does.
	without := unionPluginConf(plugins.Items, standalone.Name, standalone.Spec.Image.Tag, false)
	_, own := standalone.Spec.Config[resources.AllowlistSetting]
	if _, others := without[resources.AllowlistSetting]; !own && !envAllowlist && !others {
		return without, fleetPlugin, installs, nil
	}
	return unionPluginConfSettings(plugins.Items, standalone.Name, standalone.Spec.Image.Tag), fleetPlugin, installs, nil
}

// standaloneNeo4jPlugins returns the names the standalone's NEO4J_PLUGINS
// installs, as their writers put them there, each once: its Managed
// Neo4jPlugins' (their own names and their dependencies', in plugin-name
// order; the plugin controller), fleet-management for Aura Fleet Management
// where the image does not have it built in (id 192), and those of its
// spec.env. A NEO4J_PLUGINS entry patched onto the StatefulSet by hand is not
// among them.
func standaloneNeo4jPlugins(standalone *neo4jv1beta1.Neo4jEnterpriseStandalone, plugins []neo4jv1beta1.Neo4jPlugin) []string {
	var names []string
	add := func(name string) {
		if name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	sorted := make([]*neo4jv1beta1.Neo4jPlugin, 0, len(plugins))
	for i := range plugins {
		p := &plugins[i]
		if p.Spec.ClusterRef == standalone.Name && p.DeletionTimestamp == nil && pluginOnImage(p, standalone.Spec.Image.Tag) &&
			!shouldSkipNeo4jPluginsEnv(p) {
			sorted = append(sorted, p)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, p := range sorted {
		add(validation.EntrypointPluginName(p.Spec.Name))
		for _, dep := range p.Spec.Dependencies {
			add(validation.EntrypointPluginName(dep.Name))
		}
	}
	if standaloneInstallsFleetManagement(standalone) && !validation.FleetManagementBuiltIn(standalone.Spec.Image.Tag) {
		add(resources.FleetManagementPlugin)
	}
	for _, e := range standalone.Spec.Env {
		if e.Name != "NEO4J_PLUGINS" || e.ValueFrom != nil {
			continue
		}
		var listed []string
		if json.Unmarshal([]byte(e.Value), &listed) == nil {
			for _, name := range listed {
				add(name)
			}
		}
	}
	return names
}

// withImageUnrestrictedDefaults adds to conf's unrestricted list the image's
// entry for each plugin the standalone's NEO4J_PLUGINS installs (installs;
// validation.ImageUnrestrictedDefaults), unless the standalone sets the list
// itself (resources.OwnsUnrestrictedList: its spec.config, or its spec.env's
// variable, whose value its conf holds; one from a Secret or ConfigMap
// replaces the conf's line on the server). The image's entrypoint adds those
// only to a neo4j.conf without the list (apply_plugin_default_configuration):
// a standalone whose conf sets it — Aura Fleet Management's line, a plugin's
// list — lost them (operations.md id 193). An explicit setting wins, as in
// Neo4j. A conf without the list is left to the entrypoint.
func withImageUnrestrictedDefaults(standalone *neo4jv1beta1.Neo4jEnterpriseStandalone, conf string, installs []string) string {
	if resources.OwnsUnrestrictedList(standalone.Spec.Config, standalone.Spec.Env) {
		return conf
	}
	if _, set := resources.Neo4jConfSettings(conf)[resources.UnrestrictedSetting]; !set {
		return conf
	}
	defaults := validation.ImageUnrestrictedDefaults(standalone.Spec.Image.Tag, installs)
	if len(defaults) == 0 {
		return conf
	}
	return resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(conf,
		map[string]string{resources.UnrestrictedSetting: strings.Join(defaults, ",")}))
}

// mergeStandaloneConf returns the neo4j.conf the standalone controller writes
// over live, the standalone's ConfigMap as it is (nil, or one without data,
// when there is none): desiredConf with live's keys that no one owns merged
// back.
//   - A key the operator owns this reconcile (in desiredConf) is skipped:
//     desiredConf holds the authoritative value (scalar, or the additive UNION
//     computed from spec.config + all plugin CRs). Carrying the existing value
//     back would re-introduce stale additive tokens from an uninstalled
//     plugin, so the key would never prune.
//   - A key the operator owned before but no longer does (the
//     ownedStandaloneConfKeysAnnotation record, less desiredConf's) is skipped:
//     the user or a plugin removed it; it is not resurrected.
//   - Everything else is a genuinely FOREIGN key (added by some other actor
//     the operator doesn't manage) and is preserved: additive keys union (so
//     their tokens survive), scalars add-if-absent (so the operator's
//     re-rendered value wins). Idempotent: no ConfigMap churn.
func mergeStandaloneConf(desiredConf string, live *corev1.ConfigMap) string {
	if live == nil {
		live = &corev1.ConfigMap{}
	}
	desiredKeys := resources.Neo4jConfSettings(desiredConf)
	prevOwned := splitCSVSet(live.Annotations[ownedStandaloneConfKeysAnnotation])
	mergeBack := make(map[string]string)
	for k, v := range resources.Neo4jConfSettings(live.Data["neo4j.conf"]) {
		if _, owned := desiredKeys[k]; owned {
			continue
		}
		if _, wasOwned := prevOwned[k]; wasOwned {
			continue
		}
		mergeBack[k] = v
	}
	return resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(desiredConf, mergeBack))
}
