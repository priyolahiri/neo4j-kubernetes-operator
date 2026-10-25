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

package validation

import (
	"context"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ManagedPluginsImageChangeErrors refuses moving a cluster or standalone from
// the image it runs (running: its status.version) to tag while a Managed
// Neo4jPlugin of it runs on the running image and cannot on tag
// (ManagedPluginImageErrors: a name tag's entrypoint does not list, or whose
// JAR that release does not run — n10s from 5.26.22, or on CalVer). Rolled
// out, every server would restart on tag with the plugin's name in
// NEO4J_PLUGINS and not start; the plugin was refused and taken off only once
// the first of them crash-looped (operations.md ids 170, 187; #573).
//
// Only a change is refused. A deployment whose running image cannot run the
// plugin either already carries it — the plugin is refused and taken off by
// its own reconcile (id 170) — and is not held to it, so a deployment never
// becomes impossible to change because of a plugin. Nothing is refused for a
// disabled plugin, one in PreBaked or VerifiedDownload mode, one being
// deleted, a deployment no status.version has been recorded for, or without a
// client (`kubectl neo4j validate` offline). Plugins that cannot be listed
// are not judged.
//
// standalone says the deployment is a Neo4jEnterpriseStandalone: a plugin
// resolves its clusterRef to the cluster of that name first, so a standalone
// that shares its name with a cluster has no plugins of its own.
func ManagedPluginsImageChangeErrors(ctx context.Context, c client.Client, namespace, name string, standalone bool, running, tag string) field.ErrorList {
	if c == nil || running == "" || tag == running {
		return nil
	}
	if standalone {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, cluster); err == nil || !apierrors.IsNotFound(err) {
			return nil
		}
	}
	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := c.List(ctx, plugins, client.InNamespace(namespace)); err != nil {
		return nil
	}
	sort.Slice(plugins.Items, func(i, j int) bool { return plugins.Items[i].Name < plugins.Items[j].Name })
	var errs field.ErrorList
	for i := range plugins.Items {
		plugin := &plugins.Items[i]
		if plugin.Spec.ClusterRef != name || plugin.DeletionTimestamp != nil || len(ManagedPluginImageErrors(plugin, running)) > 0 {
			continue
		}
		for _, e := range ManagedPluginImageErrors(plugin, tag) {
			errs = append(errs, field.Invalid(field.NewPath("spec", "image", "tag"), tag, fmt.Sprintf(
				"Neo4jPlugin %s cannot follow this image change from %s, which it runs on: its %s %q is %s. "+
					"The servers would not start on the new image. Change the plugin first — delete it, set spec.enabled: false, "+
					"or move it to installMode: VerifiedDownload with a JAR built for %s — and the image change goes ahead; "+
					"a server the change restarts before the plugin's own write is replaced by that write",
				plugin.Name, running, e.Field, fmt.Sprint(e.BadValue), e.Detail, tag)))
		}
	}
	return errs
}
