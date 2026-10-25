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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func imageChangeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// n10sOf is a Managed n10s Neo4jPlugin of the deployment named ref.
func n10sOf(ref string) *neo4jv1beta1.Neo4jPlugin {
	p := managed("n10s")
	p.Name, p.Namespace, p.Spec.ClusterRef = "semantics", "default", ref
	return p
}

// The defect (#573, v1.22.0 re-walk): moving a cluster with a Managed n10s
// plugin from 5.26.21 to 5.26.22, or from 5.26 to CalVer, was accepted and
// rolled; each server restarted with n10s in NEO4J_PLUGINS on an image that
// cannot run it, and the plugin was refused and taken off only once the first
// of them crash-looped. The image change is refused before the roll, naming
// the plugin and what to do.
func TestValidateWithWarnings_RefusesAnImageChangeAManagedPluginCannotFollow(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, running, tag string
		plugin             func(*neo4jv1beta1.Neo4jPlugin)
		want               string // "" for no refusal
	}{
		{"5.26.21 to 5.26.22: the neosemantics JAR stops the server", "5.26.21-enterprise", "5.26.22-enterprise", nil, "NoSuchMethodError"},
		{"5.26 to CalVer: the entrypoint lists no n10s", "5.26.21-enterprise", "2026.08.1-enterprise", nil, "can install"},
		{"onto the floating 5.26 tag, the newest patch", "5.26.21-enterprise", "5.26-enterprise", nil, "NoSuchMethodError"},
		{"a patch it still runs on", "5.26.12-enterprise", "5.26.21-enterprise", nil, ""},
		{"no change", "5.26.21-enterprise", "5.26.21-enterprise", nil, ""},
		{"never Ready: nothing runs yet", "", "2026.08.1-enterprise", nil, ""},
		{"already on an image it cannot run: the plugin's own refusal", "5.26.32-enterprise", "2026.08.1-enterprise", nil, ""},
		{"VerifiedDownload follows any image", "5.26.21-enterprise", "2026.08.1-enterprise",
			func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.InstallMode = "VerifiedDownload" }, ""},
		{"a disabled plugin", "5.26.21-enterprise", "2026.08.1-enterprise",
			func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = false }, ""},
		{"a plugin being deleted", "5.26.21-enterprise", "2026.08.1-enterprise",
			func(p *neo4jv1beta1.Neo4jPlugin) {
				p.DeletionTimestamp, p.Finalizers = &metav1.Time{Time: time.Now()}, []string{"neo4j.neo4j.com/plugin-finalizer"}
			}, ""},
		{"another cluster's plugin", "5.26.21-enterprise", "2026.08.1-enterprise",
			func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.ClusterRef = "other" }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := n10sOf("prod")
			if tc.plugin != nil {
				tc.plugin(plugin)
			}
			cluster := validClusterForWarnings()
			cluster.Spec.Image.Tag, cluster.Status.Version = tc.tag, tc.running

			errs := NewClusterValidator(imageChangeClient(t, plugin)).ValidateWithWarnings(ctx, cluster).Errors
			if tc.want == "" {
				assert.Empty(t, errs)
				return
			}
			require.Len(t, errs, 1, "%v", errs)
			assert.Equal(t, "spec.image.tag", errs[0].Field)
			for _, want := range []string{tc.want, "Neo4jPlugin semantics", "from " + tc.running,
				"installMode: VerifiedDownload with a JAR built for " + tc.tag, "Change the plugin first"} {
				assert.Contains(t, errs[0].Detail, want)
			}
		})
	}

	t.Run("offline, without a client, nothing is judged", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.Image.Tag, cluster.Status.Version = "2026.08.1-enterprise", "5.26.21-enterprise"
		assert.Empty(t, NewClusterValidator(nil).ValidateWithWarnings(ctx, cluster).Errors)
	})
}

// The same rule for a standalone; a plugin resolves its clusterRef to a
// cluster of that name first, so a standalone sharing a cluster's name has no
// plugins of its own.
func TestStandaloneValidate_RefusesAnImageChangeAManagedPluginCannotFollow(t *testing.T) {
	ctx := context.Background()
	standalone := validStandalone()
	standalone.Spec.Image.Tag, standalone.Status.Version = "2026.08.1-enterprise", "5.26.21-enterprise"

	errs := NewStandaloneValidator().Validate(ctx, imageChangeClient(t, n10sOf("test-standalone")), standalone)
	require.Len(t, errs, 1, "%v", errs)
	assert.Equal(t, "spec.image.tag", errs[0].Field)
	assert.Contains(t, errs[0].Detail, "Neo4jPlugin semantics cannot follow this image change from 5.26.21-enterprise")

	sameName := validClusterForWarnings()
	sameName.Name = "test-standalone"
	assert.Empty(t, NewStandaloneValidator().Validate(ctx, imageChangeClient(t, n10sOf("test-standalone"), sameName), standalone))
}
