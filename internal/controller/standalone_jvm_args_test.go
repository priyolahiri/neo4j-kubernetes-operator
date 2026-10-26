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

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// imageLinesRendered is a vendored image's own list less -XX:+AlwaysPreTouch,
// which the operator leaves out (operations.md id 212).
func imageLinesRendered(t *testing.T, release string) []string {
	t.Helper()
	var out []string
	for _, v := range entrypointsim.ImageJVMArguments(t, release) {
		if v != "-XX:+AlwaysPreTouch" {
			out = append(out, v)
		}
	}
	return out
}

// standaloneJVMLines returns the values of the server.jvm.additional lines of
// a rendered neo4j.conf, in order.
func standaloneJVMLines(conf string) []string {
	var out []string
	for _, line := range strings.Split(conf, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "server.jvm.additional="); ok {
			out = append(out, v)
		}
	}
	return out
}

// A standalone's neo4j.conf, which the image's entrypoint puts in place of
// the image's own, carries the image's server.jvm.additional lines first
// (operations.md id 212), then the user's spec.config line and the truststore
// flags, which win a conflicting flag; its pod gets no
// NEO4J_server_jvm_additional of the operator's. Until #594 the conf carried
// none of the image's lines (id 208), and the JVM ran without the image's
// --add-opens and, on CalVer, its vector module. The API reference says so.
func TestStandaloneRendersTheImagesJVMArgumentsFirst(t *testing.T) {
	r := &Neo4jEnterpriseStandaloneReconciler{}
	for tag, release := range map[string]string{"5.26.32-enterprise": "5.26.32", "2026.08.1-enterprise": "2026.08.1", "5.26-enterprise": "5.26.32"} {
		t.Run(tag, func(t *testing.T) {
			standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
					AcceptLicenseAgreement: "eval",
					Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
				},
			}
			image := imageLinesRendered(t, release)
			conf := r.createConfigMap(standalone).Data["neo4j.conf"]
			assert.Equal(t, image, standaloneJVMLines(conf), "the image's own lines, less pre-touch")
			assert.NotContains(t, conf, "server.jvm.additional=-XX:+AlwaysPreTouch")
			assert.Contains(t, conf, resources.RestartNeutralBegin)
			for _, e := range r.buildEnvVars(standalone) {
				assert.NotEqual(t, "NEO4J_server_jvm_additional", e.Name)
			}

			standalone.Spec.Config = map[string]string{"server.jvm.additional": "-XX:+AlwaysPreTouch"}
			standalone.Spec.TrustedCASecrets = []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}}
			want := append(append([]string{}, image...),
				"-XX:+AlwaysPreTouch", // the user's opt-in, once
				"-Djavax.net.ssl.trustStore=/truststore/truststore.jks",
				"-Djavax.net.ssl.trustStorePassword=changeit")
			assert.Equal(t, want, standaloneJVMLines(r.createConfigMap(standalone).Data["neo4j.conf"]),
				"the user's line and the truststore flags after the image's")
		})
	}

	data, err := os.ReadFile("../../docs/api_reference/neo4jenterprisestandalone.md")
	require.NoError(t, err)
	doc := strings.Join(strings.Fields(string(data)), " ")
	for _, says := range []string{
		"A standalone runs the Neo4j image's own JVM arguments",
		"they come first",
	} {
		assert.True(t, strings.Contains(doc, says), "the standalone API reference says: %s", says)
	}
	assert.False(t, strings.Contains(doc, "A standalone gets no extra JVM arguments"),
		"the standalone API reference must not say a standalone gets no JVM arguments")
}

// The operator upgrade that adds the image's lines restarts no standalone:
// the block is left out of the restart decision (withoutDeferredConf), so the
// pod keeps the config-hash stamp of the conf it runs, whether the
// StatefulSet records the semantic hash (every operator since #465) or not.
// The new conf is in the ConfigMap for the pod's next start. So does a change
// of the block's lines: a standalone that rendered the block with
// -XX:+AlwaysPreTouch (only the unreleased #598 build did; nothing released
// renders the block) keeps its pod when the line goes.
func TestStandalone_ImageJVMBlockRestartsNothing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recorded bool
		before   func(conf string) string // the conf an earlier operator rendered
	}{
		{"no block, semantic hash recorded", true, withoutDeferredConf},
		{"no block, no semantic hash", false, withoutDeferredConf},
		{"the block with pre-touch", true, func(conf string) string {
			return strings.Replace(conf, "server.jvm.additional=-XX:-OmitStackTraceInFastThrow\n",
				"server.jvm.additional=-XX:-OmitStackTraceInFastThrow\nserver.jvm.additional=-XX:+AlwaysPreTouch\n", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c := standaloneCMTestReconciler(t)
			sa := standaloneForSTS("2026.08.1-enterprise")
			require.NoError(t, r.reconcileConfigMap(ctx, sa))
			require.NoError(t, r.reconcileStatefulSet(ctx, sa))

			// The ConfigMap and StatefulSet as an earlier operator left them: its
			// conf, the pod stamped for that conf.
			cm := &corev1.ConfigMap{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "sa-config", Namespace: "default"}, cm))
			newConf := cm.Data["neo4j.conf"]
			oldConf := tc.before(newConf)
			require.NotEqual(t, oldConf, newConf, "test premise: the earlier conf differs")
			cm.Data["neo4j.conf"] = oldConf
			require.NoError(t, c.Update(ctx, cm))
			oldStamp := standaloneConfHash(oldConf)
			sts := getSTS(t, r)
			sts.Spec.Template.Annotations[standaloneConfigHashAnnotation] = oldStamp
			if tc.recorded {
				sts.Annotations[standaloneConfigSemanticHashAnnotation] = standaloneConfSemanticHash(oldConf)
			} else {
				delete(sts.Annotations, standaloneConfigSemanticHashAnnotation)
			}
			delete(sts.Annotations, standaloneTemplateHashAnnotation)
			require.NoError(t, c.Update(ctx, sts))

			require.NoError(t, r.reconcileConfigMap(ctx, sa))
			require.NoError(t, r.reconcileStatefulSet(ctx, sa))

			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "sa-config", Namespace: "default"}, cm))
			assert.Equal(t, imageLinesRendered(t, "2026.08.1"), standaloneJVMLines(cm.Data["neo4j.conf"]),
				"the ConfigMap carries the image's lines, less pre-touch, for the next start")
			assert.Equal(t, oldStamp, getSTS(t, r).Spec.Template.Annotations[standaloneConfigHashAnnotation],
				"changing the image's JVM arguments must not restart the pod")
		})
	}
}
