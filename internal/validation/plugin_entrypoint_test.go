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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// What each release's entrypoint lists (docker-neo4j at the release tags, read
// 2026-10-24; #563 confirmed 5.26-enterprise and 2026.08.1-enterprise on the
// images). A floating tag is its line's newest patch; a CalVer newer than the
// table is judged by its newest list.
func TestEntrypointPluginNames(t *testing.T) {
	const (
		n10s  = "n10s"
		fleet = "fleet-management"
	)
	for _, tc := range []struct {
		tag       string
		has, lack []string
	}{
		{tag: "5.26.0-enterprise", has: []string{"apoc", "apoc-extended", "bloom", "genai", "graph-data-science", n10s}, lack: []string{fleet, "graphql"}},
		{tag: "5.26.10", has: []string{n10s}, lack: []string{fleet}},
		{tag: "5.26.11-enterprise", has: []string{n10s, fleet}, lack: []string{"graphql"}},
		{tag: "5.26.32-enterprise", has: []string{n10s, fleet}, lack: []string{"graphql"}},
		{tag: "5.26-enterprise", has: []string{n10s, fleet}, lack: []string{"graphql"}},
		{tag: "2025.01.0-enterprise", has: []string{n10s, "genai"}, lack: []string{fleet, "graphql"}},
		{tag: "2025.04-enterprise", has: []string{n10s}, lack: []string{fleet}},
		{tag: "2025.05.0-enterprise", has: []string{"apoc"}, lack: []string{n10s, fleet}},
		{tag: "2025.07.1-enterprise", lack: []string{n10s, fleet}},
		{tag: "2025.08.0-enterprise", has: []string{fleet}, lack: []string{n10s}},
		{tag: "2026.08.1-enterprise", has: []string{"apoc", "apoc-extended", "bloom", "genai", "graph-data-science", fleet}, lack: []string{n10s, "graphql"}},
		{tag: "2026.08-enterprise", has: []string{fleet}, lack: []string{n10s}},
		{tag: "2026.10.1-enterprise", has: []string{fleet}, lack: []string{n10s}},
		{tag: "2027.03.0-enterprise", has: []string{fleet, "genai"}, lack: []string{n10s}}, // newer than the table: its newest list
	} {
		t.Run(tc.tag, func(t *testing.T) {
			names, ok := entrypointPluginNames(tc.tag)
			require.True(t, ok)
			assert.Subset(t, names, tc.has)
			for _, n := range tc.lack {
				assert.NotContains(t, names, n)
			}
		})
	}
	for _, tag := range []string{"", "latest", "enterprise", "4.4.40-enterprise", "5.25.1"} {
		_, ok := entrypointPluginNames(tag)
		assert.False(t, ok, "%q names no supported version and is not judged", tag)
	}
}

func managed(name string, deps ...string) *neo4jv1beta1.Neo4jPlugin {
	p := &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "c", Name: name, Version: "1.0.0", Enabled: true},
	}
	for _, d := range deps {
		p.Spec.Dependencies = append(p.Spec.Dependencies, neo4jv1beta1.PluginDependency{Name: d})
	}
	return p
}

// The refusal (#563): graphql on both lines, n10s on CalVer only, by the
// target's tag — floating ones too — for the plugin and its dependencies, in
// Managed mode only, with a message that says what to do instead.
func TestManagedPluginImageErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin *neo4jv1beta1.Neo4jPlugin
		tag    string
		fields []string
	}{
		{name: "graphql on 5.26", plugin: managed("graphql"), tag: "5.26.32-enterprise", fields: []string{"spec.name"}},
		{name: "graphql on a floating 5.26 tag", plugin: managed("graphql"), tag: "5.26-enterprise", fields: []string{"spec.name"}},
		{name: "graphql on CalVer", plugin: managed("graphql"), tag: "2026.08.1-enterprise", fields: []string{"spec.name"}},
		{name: "n10s on CalVer", plugin: managed("n10s"), tag: "2026.08.1-enterprise", fields: []string{"spec.name"}},
		{name: "n10s on a floating CalVer tag", plugin: managed("n10s"), tag: "2026.08-enterprise", fields: []string{"spec.name"}},
		{name: "neosemantics is n10s", plugin: managed("neosemantics"), tag: "2026.08.1-enterprise", fields: []string{"spec.name"}},
		{name: "gds is graph-data-science", plugin: managed("gds"), tag: "2026.08.1-enterprise"},
		{name: "a dependency", plugin: managed("apoc", "apoc-extended", "n10s"), tag: "2026.08.1-enterprise", fields: []string{"spec.dependencies[1].name"}},
		{name: "a custom JAR's name", plugin: managed("acme-procs"), tag: "2026.08.1-enterprise", fields: []string{"spec.name"}},
		{name: "fleet-management before 5.26.11", plugin: managed("fleet-management"), tag: "5.26.5-enterprise", fields: []string{"spec.name"}},
		{name: "fleet-management on a floating 5.26 tag", plugin: managed("fleet-management"), tag: "5.26-enterprise"},
		{name: "an explicit Managed mode", plugin: func() *neo4jv1beta1.Neo4jPlugin {
			p := managed("graphql")
			p.Spec.InstallMode = "Managed"
			return p
		}(), tag: "2026.08.1-enterprise", fields: []string{"spec.name"}},
		{name: "VerifiedDownload never touches NEO4J_PLUGINS", plugin: func() *neo4jv1beta1.Neo4jPlugin {
			p := managed("graphql")
			p.Spec.InstallMode = "VerifiedDownload"
			return p
		}(), tag: "2026.08.1-enterprise"},
		{name: "nor does PreBaked", plugin: func() *neo4jv1beta1.Neo4jPlugin {
			p := managed("n10s")
			p.Spec.InstallMode = "PreBaked"
			return p
		}(), tag: "2026.08.1-enterprise"},
		{name: "a disabled plugin is kept off", plugin: func() *neo4jv1beta1.Neo4jPlugin {
			p := managed("graphql")
			p.Spec.Enabled = false
			return p
		}(), tag: "2026.08.1-enterprise"},
		{name: "a tag that names no version is not judged", plugin: managed("graphql"), tag: "latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := ManagedPluginImageErrors(tc.plugin, tc.tag)
			var fields []string
			for _, e := range errs {
				fields = append(fields, e.Field)
				assert.Contains(t, e.Detail, "not a plugin the neo4j:"+tc.tag+" image can install")
				assert.Contains(t, e.Detail, "installMode: VerifiedDownload (source.url + source.checksum)")
			}
			assert.Equal(t, tc.fields, fields)
		})
	}

	errs := ManagedPluginImageErrors(managed("graphql"), "2026.08.1-enterprise")
	require.Len(t, errs, 1)
	assert.Equal(t, `spec.name: Invalid value: "graphql": not a plugin the neo4j:2026.08.1-enterprise image can install: `+
		`its entrypoint installs only apoc, apoc-extended, bloom, fleet-management, genai and graph-data-science from NEO4J_PLUGINS `+
		`and exits on any other name, so the server would not start; install its JAR with installMode: VerifiedDownload `+
		`(source.url + source.checksum) instead`, errs[0].Error())
	errs = ManagedPluginImageErrors(managed("neosemantics"), "2026.08.1-enterprise")
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), `("n10s" in NEO4J_PLUGINS)`)
	errs = ManagedPluginImageErrors(managed("apoc", "n10s"), "2026.08.1-enterprise")
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "with a Neo4jPlugin of its own, installMode: VerifiedDownload")
}

// Offline there is no target, so no image: `kubectl neo4j validate` warns about
// a name no supported release installs, and not about one a release does.
// The defect (v1.22.0 re-walk of the combined build): n10s is a name every
// 5.26 entrypoint installs, so the operator let it through on 5.26.32, and the
// server crash-looped: the entrypoint downloads neosemantics 5.26.0, the only
// neosemantics release for 5.26, whose JAR bundles jackson-annotations 2.18.0,
// and Neo4j runs Jackson 2.21 and later from 5.26.22 (NoSuchMethodError
// JsonProperty.isRequired()). On CalVer before 2025.05 the entrypoint finds no
// neosemantics release at all and starts without it. n10s in Managed mode is
// refused wherever it does not run (operations.md id 170).
func TestManagedPluginImageErrors_N10sWhereItsJARDoesNotRun(t *testing.T) {
	for _, tc := range []struct {
		tag, refusal string
	}{
		{"5.26.32-enterprise", "NoSuchMethodError JsonProperty.isRequired()"},
		{"5.26.22-enterprise", "NoSuchMethodError JsonProperty.isRequired()"},
		{"5.26-enterprise", "NoSuchMethodError JsonProperty.isRequired()"},
		{"5.26.21-enterprise", ""},
		{"5.26.0-enterprise", ""},
		{"2025.04.0-enterprise", "finds no neosemantics release for CalVer"},
		{"2025.01.0-enterprise", "finds no neosemantics release for CalVer"},
		{"2026.08.1-enterprise", "image can install"}, // not on the list at all
	} {
		t.Run(tc.tag, func(t *testing.T) {
			for _, name := range []string{"n10s", "neosemantics"} {
				errs := ManagedPluginImageErrors(managed(name), tc.tag)
				if tc.refusal == "" {
					assert.Empty(t, errs, name)
					continue
				}
				require.Len(t, errs, 1, name)
				assert.Equal(t, "spec.name", errs[0].Field)
				assert.Contains(t, errs[0].Detail, tc.refusal)
				assert.Contains(t, errs[0].Detail, "installMode: VerifiedDownload")
			}
		})
	}
	// A dependency is judged the same way.
	errs := ManagedPluginImageErrors(managed("apoc", "n10s"), "5.26.32-enterprise")
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.dependencies[0].name", errs[0].Field)
	// VerifiedDownload never touches NEO4J_PLUGINS: any JAR goes.
	verified := managed("n10s")
	verified.Spec.InstallMode = "VerifiedDownload"
	assert.Empty(t, ManagedPluginImageErrors(verified, "5.26.32-enterprise"))
}

func TestManagedPluginNameWarnings(t *testing.T) {
	assert.Len(t, ManagedPluginNameWarnings(managed("graphql")), 1)
	assert.Len(t, ManagedPluginNameWarnings(managed("acme-procs")), 1)
	assert.Len(t, ManagedPluginNameWarnings(managed("apoc", "graphql")), 1)
	assert.Contains(t, ManagedPluginNameWarnings(managed("graphql"))[0],
		`spec.name "graphql" is not a plugin any supported Neo4j image can install`)
	for _, ok := range []*neo4jv1beta1.Neo4jPlugin{managed("apoc"), managed("gds"), managed("fleet-management")} {
		assert.Empty(t, ManagedPluginNameWarnings(ok), ok.Spec.Name)
	}
	// n10s runs on 5.26.0 to 5.26.21 only: offline, the image unknown, said.
	for _, n10s := range []*neo4jv1beta1.Neo4jPlugin{managed("n10s"), managed("neosemantics")} {
		warnings := ManagedPluginNameWarnings(n10s)
		require.Len(t, warnings, 1, n10s.Spec.Name)
		assert.Contains(t, warnings[0], "only from 5.26.0 to 5.26.21")
	}
	verified := managed("graphql")
	verified.Spec.InstallMode = "VerifiedDownload"
	assert.Empty(t, ManagedPluginNameWarnings(verified))
}

// The target is resolved as the plugin controller resolves it: a cluster of
// the name first, then a standalone.
func TestPluginTargetImageTag(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	cluster.Spec.Image.Tag = "2026.08.1-enterprise"
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "ns"}}
	standalone.Spec.Image.Tag = "5.26-enterprise"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standalone).Build()

	plugin := managed("n10s")
	tag, target, found, err := PluginTargetImageTag(context.Background(), c, plugin)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "2026.08.1-enterprise", tag)
	assert.Equal(t, "Neo4jEnterpriseCluster c", target)

	plugin.Spec.ClusterRef = "sa"
	tag, target, found, err = PluginTargetImageTag(context.Background(), c, plugin)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "5.26-enterprise", tag)
	assert.Equal(t, "Neo4jEnterpriseStandalone sa", target)

	plugin.Spec.ClusterRef = "none"
	_, _, found, err = PluginTargetImageTag(context.Background(), c, plugin)
	require.NoError(t, err)
	assert.False(t, found)
}
