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

// A Neo4jPlugin whose procedures its target's allowlist hides says so
// (operations.md id 161; #553).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestAllowlistLoadsAny(t *testing.T) {
	apoc := []string{"apoc."}
	genai := []string{"genai.", "ai."}
	for _, tc := range []struct {
		allowlist  string
		namespaces []string
		want       bool
	}{
		{"*", apoc, true},
		{"apoc.*", apoc, true},
		{"apoc.coll.*,apoc.load.json", apoc, true}, // part of APOC: the user's choice
		{"apoc.version", apoc, true},
		{"a*", apoc, true},
		{"genai.*,ai.*", apoc, false},
		{"apoc", apoc, false}, // a name, not a pattern: no procedure is called apoc
		{"", apoc, false},     // an empty list loads nothing
		{" , ", apoc, false},
		{"ai.*", genai, true},
		{"genai.vector.encode", genai, true},
		{"gds.*", genai, false},
	} {
		assert.Equal(t, tc.want, allowlistLoadsAny(tc.allowlist, tc.namespaces), "%q in %v", tc.allowlist, tc.namespaces)
	}
}

// hiddenFixture is cluster "c" with APOC (no security block) and GenAI
// (allowedProcedures genai.*, ai.*, as the published example has them)
// installed: GenAI's allowlist is the server's.
func hiddenFixture() (*neo4jv1beta1.Neo4jPlugin, *neo4jv1beta1.Neo4jPlugin, *appsv1.StatefulSet) {
	apoc := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	genai := genaiPlugin("c", neo4jv1beta1.PhaseReady)
	genai.Name, genai.Spec.Config = "genai-plugin", nil
	genai.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"genai.*", "ai.*"}}
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc","genai"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
		corev1.EnvVar{Name: allowlistVar, Value: "genai.*,ai.*"},
		corev1.EnvVar{Name: unrestrictedVar, Value: "genai.*,ai.*"},
	)
	sts.Annotations = map[string]string{
		pluginEnvVarsAnnotation("apoc"): "NEO4J_apoc_export_file_enabled",
		pluginEntriesAnnotation("apoc"): "apoc", pluginEntriesAnnotation("genai-plugin"): "genai",
	}
	return apoc, genai, sts
}

// The defect (#553, v1.22.0 re-walk): installing the published GenAI example
// (allowedProcedures genai.*, ai.*) beside an APOC plugin with no security
// block left APOC Ready while Neo4j loaded none of its procedures:
// apoc.version() failed with 42N48 and nothing said why. The APOC plugin, woken
// by GenAI's write to the StatefulSet, now carries the ProceduresHidden
// condition, with one Warning; it stays Ready (its JAR is on the servers) and
// the list is left as the user set it. The condition goes when the list loads
// APOC again.
func TestReconcile_APluginHiddenByAnotherPluginsAllowlistSaysSo(t *testing.T) {
	apoc, genai, sts := hiddenFixture()
	r, rec, writes, _ := gateReconciler(t, noBolt(t), apoc, genai, readyCluster(), sts)

	reconcilePlugin(t, r, "apoc")

	assert.Zero(t, *writes, "the list is not touched")
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	hidden := findCondition(got.Status.Conditions, ConditionTypeProceduresHidden)
	require.NotNil(t, hidden)
	assert.Equal(t, metav1.ConditionTrue, hidden.Status)
	assert.Equal(t, ConditionReasonNotInAllowlist, hidden.Reason)
	assert.Contains(t, hidden.Message, `"genai.*,ai.*"`)
	assert.Contains(t, hidden.Message, "set by Neo4jPlugin genai-plugin")
	assert.Contains(t, hidden.Message, "add apoc.* to spec.security.allowedProcedures")
	warnings := eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "Warning")

	// The same list again: nothing written, nothing said.
	rv := got.ResourceVersion
	reconcilePlugin(t, r, "apoc")
	assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion)
	assert.Empty(t, drainEvents(rec))

	// The GenAI plugin lets APOC in; its write changes the list.
	editPlugin(t, r.Client, "genai-plugin", func(p *neo4jv1beta1.Neo4jPlugin) {
		p.Spec.Security.AllowedProcedures = append(p.Spec.Security.AllowedProcedures, "apoc.*")
	})
	live := liveSTS(t, r.Client, "c-server")
	for i, e := range live.Spec.Template.Spec.Containers[0].Env {
		if e.Name == allowlistVar || e.Name == unrestrictedVar {
			live.Spec.Template.Spec.Containers[0].Env[i].Value = "genai.*,ai.*,apoc.*"
		}
	}
	require.NoError(t, r.Update(context.Background(), live))
	rollOut(t, r.Client, "c-server")
	reconcilePlugin(t, r, "apoc")
	got = getPlugin(t, r, "apoc")
	assert.Nil(t, findCondition(got.Status.Conditions, ConditionTypeProceduresHidden), "gone with the cause")
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
}

// On a standalone the list is in its neo4j.conf, where its controller unions
// its own spec.config with its plugins' settings.
func TestReconcile_AStandalonePluginHiddenByTheConfAllowlistSaysSo(t *testing.T) {
	apoc := selfHealPlugin("apoc", "sa", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := standaloneSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sa-config", Namespace: selfHealNS},
		Data:       map[string]string{"neo4j.conf": "server.memory.heap.max_size=1G\ndbms.security.procedures.allowlist=gds.*\n"},
	}
	r, rec, _, _ := gateReconciler(t, noBolt(t), apoc, readyStandalone(), sts, cm)

	reconcilePlugin(t, r, "apoc")

	hidden := findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden)
	require.NotNil(t, hidden)
	assert.Contains(t, hidden.Message, `"gds.*" (neo4j.conf of ConfigMap sa-config, set by Neo4jEnterpriseStandalone sa's own configuration)`)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden), 1)
}

// Not judged: a plugin whose namespace the operator does not know (a custom
// JAR), a list read from a Secret, and no list at all (Neo4j loads
// everything).
func TestReconcile_NoHiddenProceduresWithoutAKnownListAndNamespace(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin func(*neo4jv1beta1.Neo4jPlugin)
		env    []corev1.EnvVar
	}{
		{"a custom plugin", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Name = "acme-procs" }, []corev1.EnvVar{{Name: allowlistVar, Value: "genai.*"}}},
		{"a list from a Secret", nil, []corev1.EnvVar{{Name: allowlistVar, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}}}},
		{"no list", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apoc := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
			if tc.plugin != nil {
				tc.plugin(apoc)
			}
			r, rec, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, readyCluster(), selfHealSTS(tc.env...)}, readyClusterPods()...)...)
			msg, err := r.hiddenProceduresMessage(context.Background(), apoc,
				&DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: readyCluster()})
			require.NoError(t, err)
			assert.Empty(t, msg)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
		})
	}
}

// Disabling a hidden plugin takes the condition away with it.
func TestReconcile_ADisabledPluginCarriesNoHiddenProcedures(t *testing.T) {
	apoc, genai, sts := hiddenFixture()
	r, rec, _, _ := gateReconciler(t, noBolt(t), apoc, genai, readyCluster(), sts)
	reconcilePlugin(t, r, "apoc")
	require.NotNil(t, findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden))
	drainEvents(rec)

	editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = false })
	r.bolt = nil
	reconcilePlugin(t, r, "apoc")

	assert.Nil(t, findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden))
}
