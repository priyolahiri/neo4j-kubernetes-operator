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

// What the Neo4j Docker entrypoint can install from NEO4J_PLUGINS
// (operations.md id 170; #563). A Managed Neo4jPlugin puts its name, and its
// dependencies' names, in NEO4J_PLUGINS. The entrypoint looks every name up in
// the image's /startup/neo4j-plugins.json and, for one it does not list, prints
// "is not a known Neo4j plugin" and exits 1 (install_neo4j_plugins in
// docker-entrypoint.sh): the server crash-loops. So a Managed plugin is judged
// against the list of the image its target runs, and a JAR the image does not
// list is installed with installMode: VerifiedDownload, which never touches
// NEO4J_PLUGINS.

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// entrypointPluginList is what the entrypoint of one range of releases
// installs: names, from release from until the next entry of the same line.
type entrypointPluginList struct {
	from  string
	names []string
}

// entrypointPluginLists is, release by release, the keys of the image's
// /startup/neo4j-plugins.json — the names NEO4J_PLUGINS may hold — less
// "_testing", a fixture of the image's own tests. One line after the other, each
// in release order; add an entry when a release changes its list.
//
// Read on 2026-10-24 from github.com/neo4j/docker-neo4j at the tag of every
// release (neo4j-<version>): docker-image-src/5/coredb/neo4j-plugins.json for
// 5.26.0 to 5.26.32, docker-image-src/calver/coredb/neo4j-plugins.json for
// 2025.01.0 to 2026.10.1. The images agree (#563: neo4j:5.26-enterprise and
// neo4j:2026.08.1-enterprise, checked on 2026-10-24).
//   - fleet-management joined at 5.26.11 and 2025.08.0 (docker-neo4j
//     dddc2f6, 2025-08-07);
//   - n10s is on every 5.26, and left CalVer at 2025.05.0 (docker-neo4j #556,
//     2025-05-20);
//   - graphql is on no supported release: it left the 5.x list (with
//     graph-algorithms) in docker-neo4j #508 (2024-08-28), before 5.26.0, and the
//     CalVer list never had it.
//
// A release newer than the last one read (2026.10.1) is judged by the newest
// list of its line, an assumption: a later image may install a plugin this
// table does not list, and then the operator refuses a name that image would
// install, until the table learns it.
var entrypointPluginLists = []entrypointPluginList{
	{from: "5.26.0", names: []string{"apoc", "apoc-extended", "bloom", "genai", "graph-data-science", "n10s"}},
	{from: "5.26.11", names: []string{"apoc", "apoc-extended", "bloom", "fleet-management", "genai", "graph-data-science", "n10s"}},
	{from: "2025.01.0", names: []string{"apoc", "apoc-extended", "bloom", "genai", "graph-data-science", "n10s"}},
	{from: "2025.05.0", names: []string{"apoc", "apoc-extended", "bloom", "genai", "graph-data-science"}},
	{from: "2025.08.0", names: []string{"apoc", "apoc-extended", "bloom", "fleet-management", "genai", "graph-data-science"}},
}

// entrypointBrokenPlugin is a name the entrypoint of a range of releases
// lists, and installs, but whose JAR that release does not run: from release
// from to the end of its line (or until the line's list drops the name). why
// says what happens; instead what to do.
type entrypointBrokenPlugin struct {
	name, from string
	why        string
	instead    string
}

// entrypointBrokenPlugins are the names the entrypoint installs that do not
// work, release by release (operations.md id 170). Read on 2026-10-25:
//   - n10s: every 5.26.x maps, in the versions.json the entrypoint downloads it
//     by (neo4j-labs.github.io/neosemantics), to neosemantics 5.26.0, the only
//     neosemantics release for 5.26. Its JAR bundles jackson-annotations 2.18.0
//     unshaded. Neo4j moved to Jackson 2.21 at 5.26.22 (2.22 by 5.26.32; the
//     pom of neo4j/neo4j at each tag), and its server stops at startup with
//     NoSuchMethodError JsonProperty.isRequired() (v1.22.0 re-walk, 5.26.32).
//     From 5.26.0 to 5.26.21 Neo4j ships the same Jackson 2.18.0.
//   - n10s on CalVer 2025.01 to 2025.04, whose images still list the name: the
//     versions.json has no CalVer release, so the entrypoint finds no JAR and
//     starts the server without it ("No compatible "n10s" plugin found"); from
//     2025.05 the list drops it (entrypointPluginLists).
var entrypointBrokenPlugins = []entrypointBrokenPlugin{
	{
		name: "n10s", from: "5.26.22",
		why: "its entrypoint downloads neosemantics 5.26.0 for it, the only neosemantics release for 5.26, which bundles " +
			"jackson-annotations 2.18.0; from 5.26.22 Neo4j runs Jackson 2.21 or later, and the server stops at startup " +
			"with NoSuchMethodError JsonProperty.isRequired()",
		instead: "; no neosemantics release starts on 5.26.22 or later: run a 5.26 release up to 5.26.21, or a CalVer release " +
			"with neosemantics 2025.06.1 installed by installMode: VerifiedDownload",
	},
	{
		name: "n10s", from: "2025.01.0",
		why: "its entrypoint finds no neosemantics release for CalVer in the versions.json it installs n10s by, and starts " +
			"the server without the plugin",
		instead: "; install a neosemantics JAR built for your release with installMode: VerifiedDownload (source.url + source.checksum) instead",
	},
}

// brokenEntrypointPlugin returns the entry of entrypointBrokenPlugins that
// holds for name on the release v; nil when none does.
func brokenEntrypointPlugin(name string, v *VersionInfo) *entrypointBrokenPlugin {
	var found *entrypointBrokenPlugin
	for i := range entrypointBrokenPlugins {
		b := &entrypointBrokenPlugins[i]
		from := parseTagVersion(b.from)
		if b.name != name || (from.Major >= 2025) != (v.Major >= 2025) {
			continue
		}
		key, fromKey := versionKey(v), versionKey(from)
		if slices.Compare(fromKey[:], key[:]) <= 0 {
			found = b
		}
	}
	return found
}

// EntrypointPluginName is the name the entrypoint knows a plugin by in
// NEO4J_PLUGINS, for a Neo4jPlugin's spec.name or a dependency's name: the
// operator's aliases gds (graph-data-science) and neosemantics (n10s), and
// any other name as it is.
func EntrypointPluginName(name string) string {
	switch name {
	case "gds":
		return "graph-data-science"
	case "neosemantics":
		return "n10s"
	}
	return name
}

// parseTagVersion reads an image tag ("5.26.12-enterprise", "2026.08-enterprise")
// as the upgrade rules do; nil when it names no version.
func parseTagVersion(tag string) *VersionInfo {
	return (&UpgradeValidator{}).parseVersion(tag)
}

// versionKey orders versions; a version without a patch (a floating tag) is the
// newest patch of its line.
func versionKey(v *VersionInfo) [3]int {
	patch := v.Patch
	if !v.HasPatch {
		patch = math.MaxInt
	}
	return [3]int{v.Major, v.Minor, patch}
}

// entrypointPluginNames returns the names the entrypoint of the Neo4j image
// tagged tag installs from NEO4J_PLUGINS; ok is false for a tag that names no
// supported version (5.26.x, CalVer), which is not judged here — the image rule
// refuses it. A tag without a patch ("5.26-enterprise") is its line's newest
// patch, the one it pulls.
func entrypointPluginNames(tag string) ([]string, bool) {
	v := parseTagVersion(tag)
	if v == nil {
		return nil, false
	}
	if lts := v.Major == 5 && v.Minor == 26; !lts && v.Major < 2025 {
		return nil, false
	}
	var names []string
	for _, list := range entrypointPluginLists {
		from := parseTagVersion(list.from)
		if (from.Major >= 2025) != (v.Major >= 2025) {
			continue // the other line
		}
		key, fromKey := versionKey(v), versionKey(from)
		if slices.Compare(fromKey[:], key[:]) <= 0 {
			names = list.names
		}
	}
	return names, names != nil
}

// anySupportedPluginNames is every name an entrypoint of a supported release
// installs, sorted.
func anySupportedPluginNames() []string {
	var all []string
	for _, list := range entrypointPluginLists {
		for _, name := range list.names {
			if !slices.Contains(all, name) {
				all = append(all, name)
			}
		}
	}
	slices.Sort(all)
	return all
}

// managedNameUnknownToEveryImage reports whether the plugin is Managed and its
// own name, as the entrypoint knows it, is one no supported image installs —
// a name ManagedPluginNameWarnings and the operator's refusal speak for.
func managedNameUnknownToEveryImage(plugin *neo4jv1beta1.Neo4jPlugin) bool {
	for _, e := range managedPluginEntries(plugin) {
		if !e.dependency {
			return !slices.Contains(anySupportedPluginNames(), e.name)
		}
	}
	return false
}

// managedPluginEntry is a name a Managed plugin puts in NEO4J_PLUGINS: as the
// spec gives it (value, at path) and as the entrypoint knows it (name).
type managedPluginEntry struct {
	path        *field.Path
	value, name string
	dependency  bool
}

// managedPluginEntries returns the names the plugin puts in NEO4J_PLUGINS: its
// own and its dependencies', in Managed mode, while it is enabled. PreBaked and
// VerifiedDownload never touch NEO4J_PLUGINS, and a disabled plugin is kept off
// its target.
func managedPluginEntries(plugin *neo4jv1beta1.Neo4jPlugin) []managedPluginEntry {
	if !plugin.Spec.Enabled || plugin.Spec.InstallMode == "PreBaked" || plugin.Spec.InstallMode == "VerifiedDownload" {
		return nil
	}
	var out []managedPluginEntry
	if plugin.Spec.Name != "" {
		out = append(out, managedPluginEntry{
			path: field.NewPath("spec", "name"), value: plugin.Spec.Name, name: EntrypointPluginName(plugin.Spec.Name),
		})
	}
	for i, dep := range plugin.Spec.Dependencies {
		if dep.Name == "" {
			continue
		}
		out = append(out, managedPluginEntry{
			path:  field.NewPath("spec", "dependencies").Index(i).Child("name"),
			value: dep.Name, name: EntrypointPluginName(dep.Name), dependency: true,
		})
	}
	return out
}

// instead says how to install the JAR of an entry the entrypoint cannot.
func (e managedPluginEntry) instead() string {
	named := ""
	if e.name != e.value {
		named = fmt.Sprintf(" (%q in NEO4J_PLUGINS)", e.name)
	}
	if e.dependency {
		return named + "; install its JAR with a Neo4jPlugin of its own, installMode: VerifiedDownload (source.url + source.checksum), instead"
	}
	return named + "; install its JAR with installMode: VerifiedDownload (source.url + source.checksum) instead"
}

// listNames renders names as "a, b and c".
func listNames(names []string) string {
	if len(names) < 2 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// ManagedPluginImageErrors refuses a Managed plugin whose NEO4J_PLUGINS names
// — its own and its dependencies', as the entrypoint knows them — the
// entrypoint of the Neo4j image tagged imageTag (the target's spec.image.tag)
// does not install: a server given one does not start. Nothing is refused for a
// disabled plugin, one in PreBaked or VerifiedDownload mode, or a tag that names
// no supported version.
func ManagedPluginImageErrors(plugin *neo4jv1beta1.Neo4jPlugin, imageTag string) field.ErrorList {
	entries := managedPluginEntries(plugin)
	if len(entries) == 0 {
		return nil
	}
	known, ok := entrypointPluginNames(imageTag)
	if !ok {
		return nil
	}
	v := parseTagVersion(imageTag)
	var errs field.ErrorList
	for _, e := range entries {
		if slices.Contains(known, e.name) {
			if b := brokenEntrypointPlugin(e.name, v); b != nil {
				errs = append(errs, field.Invalid(e.path, e.value, fmt.Sprintf(
					"not a plugin the neo4j:%s image can run in installMode Managed: %s%s", imageTag, b.why, b.instead)))
			}
			continue
		}
		errs = append(errs, field.Invalid(e.path, e.value, fmt.Sprintf(
			"not a plugin the neo4j:%s image can install: its entrypoint installs only %s from NEO4J_PLUGINS and exits on any other name, so the server would not start%s",
			imageTag, listNames(known), e.instead())))
	}
	return errs
}

// ManagedPluginNameWarnings is ManagedPluginImageErrors without a target, as
// `kubectl neo4j validate` runs offline: the image is unknown, so it warns about
// a name no supported release installs — graphql, a custom JAR's — and about
// n10s, which runs on 5.26.0 to 5.26.21 only, and says nothing about a name
// every release it lists runs.
func ManagedPluginNameWarnings(plugin *neo4jv1beta1.Neo4jPlugin) []string {
	all := anySupportedPluginNames()
	var out []string
	for _, e := range managedPluginEntries(plugin) {
		if e.name == "n10s" {
			// Installed by name on 5.26.0 to 5.26.21 only (entrypointBrokenPlugins).
			out = append(out, fmt.Sprintf(
				"%s %q: in installMode Managed, Neo4j runs n10s only from 5.26.0 to 5.26.21 — later 5.26 releases stop at startup on "+
					"the jackson-annotations its neosemantics JAR bundles, and CalVer releases install none — so the operator refuses it "+
					"on any other image; elsewhere install a neosemantics JAR built for your release with installMode: VerifiedDownload",
				e.path, e.value))
			continue
		}
		if slices.Contains(all, e.name) {
			continue
		}
		out = append(out, fmt.Sprintf(
			"%s %q is not a plugin any supported Neo4j image can install: their entrypoints install only %s from NEO4J_PLUGINS and exit on any other name, so in installMode Managed the server would not start%s",
			e.path, e.value, listNames(all), e.instead()))
	}
	return out
}

// PluginTargetImageTag reads the image tag of the plugin's target as the plugin
// controller resolves it: the Neo4jEnterpriseCluster named spec.clusterRef in
// the plugin's namespace, else the Neo4jEnterpriseStandalone. target names it
// ("Neo4jEnterpriseCluster c"); found is false when neither exists.
func PluginTargetImageTag(ctx context.Context, c client.Client, plugin *neo4jv1beta1.Neo4jPlugin) (tag, target string, found bool, err error) {
	key := types.NamespacedName{Namespace: plugin.Namespace, Name: plugin.Spec.ClusterRef}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	switch err := c.Get(ctx, key, cluster); {
	case err == nil:
		return cluster.Spec.Image.Tag, "Neo4jEnterpriseCluster " + key.Name, true, nil
	case !apierrors.IsNotFound(err):
		return "", "", false, err
	}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	switch err := c.Get(ctx, key, standalone); {
	case err == nil:
		return standalone.Spec.Image.Tag, "Neo4jEnterpriseStandalone " + key.Name, true, nil
	case !apierrors.IsNotFound(err):
		return "", "", false, err
	}
	return "", "", false, nil
}
