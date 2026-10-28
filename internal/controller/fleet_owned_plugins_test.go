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

// A cluster whose own spec.env sets NEO4J_PLUGINS while Aura Fleet Management
// is on restarted its servers without end (#608, the #606 walk): the fleet
// merge added fleet-management to the variable the cluster renders, and the
// cluster's template apply put its own value back. The cluster now renders
// the name into its own list, and the merge never writes a variable the
// deployment owns (operations.md id 223).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// withSpecEnvPlugins gives the cluster fixture spec.env NEO4J_PLUGINS.
func withSpecEnvPlugins(e corev1.EnvVar) func(*neo4jv1beta1.Neo4jEnterpriseCluster) {
	return func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
		e.Name = "NEO4J_PLUGINS"
		c.Spec.Env = []corev1.EnvVar{e}
	}
}

// fromSecretPlugins is a NEO4J_PLUGINS read from a Secret.
var fromSecretPlugins = corev1.EnvVar{ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
	LocalObjectReference: corev1.LocalObjectReference{Name: "plugins"}, Key: "list"}}}

// ready makes the server StatefulSet read as every server Ready, the state in
// which the cluster's template apply compares its own variables with the
// live ones (hasSignificantTemplateChanges) and puts its values back.
func (h *fleetClusterHarness) ready() {
	h.t.Helper()
	sts := h.server()
	sts.Status.Replicas = *sts.Spec.Replicas
	sts.Status.ReadyReplicas = *sts.Spec.Replicas
	require.NoError(h.t, h.c.Status().Update(context.Background(), sts))
	h.writes.reset()
}

// settledPasses runs n passes and returns the writes and rolls they made.
func (h *fleetClusterHarness) settledPasses(n int) (all, rolls int) {
	h.t.Helper()
	h.writes.reset()
	for range n {
		h.pass()
	}
	return h.writes.counts()
}

func (h *fleetClusterHarness) events() []string {
	return recordedEvents(h.t, h.r.Recorder)
}

// recordedEvents drains the fake recorder a harness's reconciler holds.
func recordedEvents(t *testing.T, rec record.EventRecorder) []string {
	t.Helper()
	fake, ok := rec.(*record.FakeRecorder)
	require.True(t, ok, "the harness's recorder is a FakeRecorder")
	return drainEvents(fake)
}

// setSpecEnv writes the cluster's spec.env.
func (h *fleetClusterHarness) setSpecEnv(env []corev1.EnvVar) {
	h.t.Helper()
	require.NoError(h.t, h.c.Get(context.Background(), client.ObjectKeyFromObject(h.cluster), h.cluster))
	require.NoError(h.t, patchObject(context.Background(), h.c, h.cluster, func() { h.cluster.Spec.Env = env }))
}

// neo4jPluginsVar returns the neo4j container's NEO4J_PLUGINS variable.
func neo4jPluginsVar(t *testing.T, sts *appsv1.StatefulSet) corev1.EnvVar {
	t.Helper()
	c := neo4jContainer(&sts.Spec.Template.Spec)
	require.NotNil(t, c)
	for _, e := range c.Env {
		if e.Name == "NEO4J_PLUGINS" {
			return e
		}
	}
	t.Fatal("no NEO4J_PLUGINS")
	return corev1.EnvVar{}
}

// warningsAbout keeps the ValidationWarning events that name NEO4J_PLUGINS.
func warningsAbout(events []string) []string {
	var out []string
	for _, e := range eventsWithReason(events, EventReasonValidationWarning) {
		if strings.Contains(e, "(NEO4J_PLUGINS)") {
			out = append(out, e)
		}
	}
	return out
}

// The loop of the #606 walk: spec.env NEO4J_PLUGINS='["apoc"]' and Aura Fleet
// Management on, on 5.26.32. The cluster renders fleet-management into its own
// list, so the template apply and the fleet merge agree: nothing is written
// after the create, while the servers form or once every one is Ready.
func TestClusterReconcile_ItsOwnNeo4jPluginsWithFleetManagementSettles(t *testing.T) {
	h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	h.created()
	sts := h.server()
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, sts), "the cluster's own list, with the plugin")
	assert.Contains(t, readOwnedEnvVarNames(sts), "NEO4J_PLUGINS", "the cluster's variable")
	assert.False(t, fleetEntryRecorded(sts), "the deployment's own list: nothing recorded")

	all, _ := h.settledPasses(repeatPasses)
	assert.Zero(t, all, "forming: nothing written")

	h.ready()
	all, rolls := h.settledPasses(6)
	assert.Zero(t, rolls, "no restart: the template apply and the fleet merge agree")
	assert.Zero(t, all, "nothing written")
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
	assert.Empty(t, eventsWithReason(h.events(), EventReasonAuraFleetPluginPatchFailed))

	// The list with the name is what the cluster renders, so it puts it back
	// as it puts back any of its own variables, and the merge leaves it to it.
	h.editServer(func(sts *appsv1.StatefulSet) {
		c := neo4jContainer(&sts.Spec.Template.Spec)
		for i := range c.Env {
			if c.Env[i].Name == "NEO4J_PLUGINS" {
				c.Env[i].Value = `["apoc"]`
			}
		}
	})
	_, rolls = h.settledPasses(1)
	assert.Equal(t, 1, rolls, "the cluster's own value, put back")
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
	all, _ = h.settledPasses(repeatPasses)
	assert.Zero(t, all)

	// A StatefulSet an earlier operator's loop left, at either of its two
	// values, under the template hash of that operator's rendering: at most
	// one roll, to the list with the name, then nothing.
	for left, wantRolls := range map[string]int{`["apoc"]`: 1, `["apoc","fleet-management"]`: 0} {
		t.Run("left by an earlier operator at "+left, func(t *testing.T) {
			h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
			h.created()
			h.ready()
			h.editServer(func(sts *appsv1.StatefulSet) {
				c := neo4jContainer(&sts.Spec.Template.Spec)
				for i := range c.Env {
					if c.Env[i].Name == "NEO4J_PLUGINS" {
						c.Env[i].Value = left
					}
				}
				sts.Annotations[clusterTemplateHashAnnotation] = "rendered-by-v1.23.0"
			})
			_, rolls := h.settledPasses(1)
			assert.Equal(t, wantRolls, rolls)
			assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
			all, _ := h.settledPasses(repeatPasses)
			assert.Zero(t, all, "then nothing")
		})
	}
}

// Turning Aura Fleet Management off on such a cluster: the cluster's rendering
// drops the name it added, in the write that rolls the servers for the
// disable's conf change — one roll — and keeps the user's names, and a
// fleet-management the user wrote into spec.env themselves. On again: one roll.
func TestClusterReconcile_TogglingFleetManagementWithItsOwnNeo4jPlugins(t *testing.T) {
	for name, off := range map[string]*neo4jv1beta1.AuraFleetManagementSpec{"enabled: false": fleetOff, "block removed": nil} {
		t.Run(name, func(t *testing.T) {
			h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
			h.created()
			h.ready()
			stampBefore := h.server().Spec.Template.Annotations["neo4j.neo4j.com/config-hash"]

			fleetToggle(t, h.c, h.cluster, off)
			_, rolls := h.settledPasses(1)
			assert.Equal(t, 1, rolls, "one roll: the conf change's, with the list in the same write")
			sts := h.server()
			assert.Equal(t, `["apoc"]`, neo4jPlugins(t, sts), "the user's list, as they wrote it")
			assert.NotEqual(t, stampBefore, sts.Spec.Template.Annotations["neo4j.neo4j.com/config-hash"], "the conf change's restart stamp")
			assert.NotContains(t, h.conf(), resources.FleetManagementProcedures)
			all, _ := h.settledPasses(repeatPasses)
			assert.Zero(t, all, "then nothing")

			fleetToggle(t, h.c, h.cluster, fleetOn)
			_, rolls = h.settledPasses(1)
			assert.Equal(t, 1, rolls, "on again: one roll")
			assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
			all, _ = h.settledPasses(repeatPasses)
			assert.Zero(t, all)
		})
	}

	t.Run("a fleet-management the user wrote stays", func(t *testing.T) {
		h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["fleet-management","apoc"]`}))
		h.created()
		h.ready()
		assert.Equal(t, `["fleet-management","apoc"]`, neo4jPlugins(t, h.server()), "byte for byte: nothing to add")

		fleetToggle(t, h.c, h.cluster, fleetOff)
		_, rolls := h.settledPasses(1)
		assert.Equal(t, 1, rolls, "the conf change's roll alone")
		assert.Equal(t, `["fleet-management","apoc"]`, neo4jPlugins(t, h.server()), "the user's")
		all, _ := h.settledPasses(repeatPasses)
		assert.Zero(t, all)
	})
}

// Turning it on on a running cluster: one roll, the conf change's restart
// stamp carrying the cluster's list — no more than a cluster without spec.env
// NEO4J_PLUGINS makes (its stamp, then the merge's write: two template changes,
// #608 item 3, unchanged here).
func TestClusterReconcile_EnablingFleetManagementWithItsOwnNeo4jPlugins(t *testing.T) {
	enable := func(mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)) (int, *fleetClusterHarness) {
		h := newFleetClusterHarnessWith(t, fleetOff, mutate)
		h.created()
		h.ready()
		fleetToggle(t, h.c, h.cluster, fleetOn)
		_, rolls := h.settledPasses(1)
		return rolls, h
	}
	own, h := enable(withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	assert.Equal(t, 1, own, "one roll")
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
	all, _ := h.settledPasses(repeatPasses)
	assert.Zero(t, all)

	without, h := enable(nil)
	require.Equal(t, `["fleet-management"]`, neo4jPlugins(t, h.server()))
	assert.LessOrEqual(t, own, without, "not more rolls than a cluster without spec.env NEO4J_PLUGINS")
	t.Logf("template changes on the enable: %d with spec.env NEO4J_PLUGINS, %d without", own, without)
}

// A NEO4J_PLUGINS the cluster's spec.env takes from a Secret, or sets to a
// value that is not a JSON list, cannot have the name added: the merge writes
// nothing to it (a value beside valueFrom is refused by the API server; the
// list did not parse and the create failed), and the validator says so, once.
func TestClusterReconcile_ItsOwnNeo4jPluginsTheOperatorCannotAddTo(t *testing.T) {
	for name, e := range map[string]corev1.EnvVar{
		"from a Secret":     fromSecretPlugins,
		"not a JSON list":   {Value: "apoc"},
		"a list of numbers": {Value: `[1]`},
	} {
		t.Run(name, func(t *testing.T) {
			h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(e))
			h.created()
			got := neo4jPluginsVar(t, h.server())
			assert.Equal(t, e.Value, got.Value, "left as the cluster renders it")
			assert.Equal(t, e.ValueFrom, got.ValueFrom)

			h.ready()
			all, _ := h.settledPasses(6)
			assert.Zero(t, all, "nothing written")
			events := h.events()
			assert.Empty(t, eventsWithReason(events, EventReasonAuraFleetPluginPatchFailed))
			warnings := warningsAbout(events)
			require.Len(t, warnings, 1, "announced once")
			assert.Contains(t, warnings[0], "Aura Fleet Management needs fleet-management in that list, and the operator cannot add it")
		})
	}
}

// From 2026.03 Fleet Manager is built in: the cluster adds nothing to its own
// list, and a fleet-management the user lists stays theirs.
func TestClusterReconcile_ItsOwnNeo4jPluginsOnABuiltInImage(t *testing.T) {
	for _, list := range []string{`["apoc"]`, `["apoc","fleet-management"]`} {
		t.Run(list, func(t *testing.T) {
			h := newFleetClusterHarnessWith(t, fleetOn, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
				withSpecEnvPlugins(corev1.EnvVar{Value: list})(c)
				c.Spec.Image.Tag = "2026.08.1-enterprise"
			})
			h.created()
			assert.Equal(t, list, neo4jPlugins(t, h.server()))
			h.ready()
			all, _ := h.settledPasses(repeatPasses)
			assert.Zero(t, all)
			assert.Empty(t, warningsAbout(h.events()), "nothing to add: nothing to warn about")

			fleetToggle(t, h.c, h.cluster, fleetOff)
			h.settledPasses(1)
			assert.Equal(t, list, neo4jPlugins(t, h.server()), "the user's, on and off")
		})
	}
}

// The cluster's spec.env stops setting NEO4J_PLUGINS while Fleet Management is
// on: the write that stops owning the variable keeps fleet-management in it,
// recorded as Fleet Management's — one roll, where the merge of the next pass
// put the name back after the apply dropped it. And the other way: the write
// that starts owning it renders the user's list with the name.
func TestClusterReconcile_SpecEnvNeo4jPluginsComesAndGoesWhileFleetIsOn(t *testing.T) {
	t.Run("removed from spec.env", func(t *testing.T) {
		h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
		h.created()
		h.ready()

		h.setSpecEnv(nil)
		_, rolls := h.settledPasses(1)
		assert.Equal(t, 1, rolls)
		sts := h.server()
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
		assert.NotContains(t, readOwnedEnvVarNames(sts), "NEO4J_PLUGINS", "shared again")
		assert.True(t, fleetEntryRecorded(sts), "Fleet Management's, recorded in the write that keeps it")
		all, _ := h.settledPasses(repeatPasses)
		assert.Zero(t, all)

		fleetToggle(t, h.c, h.cluster, fleetOff)
		h.settledPasses(1)
		assert.Equal(t, `[]`, neo4jPlugins(t, h.server()), "and it goes with a disable (id 222)")
	})

	t.Run("added to spec.env", func(t *testing.T) {
		h := newFleetClusterHarness(t, fleetOn)
		h.created()
		h.ready()

		h.setSpecEnv([]corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc"]`}})
		_, rolls := h.settledPasses(1)
		assert.Equal(t, 1, rolls)
		assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
		all, _ := h.settledPasses(repeatPasses)
		assert.Zero(t, all)
	})
}

// The upgrade's Staging writes the template buildDesiredServerStatefulSet
// renders: the cluster's own list with the name for a target whose entrypoint
// installs it, without it for one with Fleet Manager built in, where the image
// change rolls the servers anyway.
func TestBuildDesiredServerStatefulSet_RendersItsOwnNeo4jPlugins(t *testing.T) {
	r := &Neo4jEnterpriseClusterReconciler{}
	for tag, want := range map[string]string{
		"5.26.32-enterprise":   `["apoc","fleet-management"]`,
		"2026.08.1-enterprise": `["apoc"]`,
	} {
		cluster := minimalCluster("c", "ns")
		cluster.Spec.Image.Tag = tag
		cluster.Spec.AuraFleetManagement = fleetOn
		withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`})(cluster)
		sts, err := r.buildDesiredServerStatefulSet(context.Background(), cluster)
		require.NoError(t, err)
		assert.Equal(t, want, neo4jPlugins(t, sts), tag)
	}
}

// A Neo4jPlugin is still refused a variable the cluster renders (rule 135):
// the name the cluster adds for Fleet Management changes nothing about that.
func TestClusterReconcile_APluginIsStillRefusedTheClustersOwnNeo4jPlugins(t *testing.T) {
	h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	h.created()
	sts := h.server()
	sts.Namespace, sts.Name, sts.ResourceVersion = selfHealNS, "c-server", ""

	gds := selfHealPlugin("gds", "c", "", "")
	gds.Spec.Name, gds.Spec.Config = "graph-data-science", nil
	r, _, stsUpdates := selfHealReconciler(t, nil, nil, gds, sts)
	err := r.installPluginViaEnvironment(context.Background(), gds, &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS})
	var owned *pluginEnvOwnedError
	require.ErrorAs(t, err, &owned)
	assert.Zero(t, *stsUpdates)
}

// On a real API server: a NEO4J_PLUGINS from a Secret with Aura Fleet
// Management on. The write that created the StatefulSet put a value beside
// valueFrom, which the API server refuses ("may not be specified when `value`
// is not empty"), so neither deployment was ever created; now both are, with
// the variable as the spec gives it.
func TestReconcile_ANeo4jPluginsFromASecretWithFleetManagement_APIServer(t *testing.T) {
	c, scheme := startStoredSpecEnv(t)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "kubernetes.io/no-provisioner"}))
	env := []corev1.EnvVar{{Name: "NEO4J_PLUGINS", ValueFrom: fromSecretPlugins.ValueFrom}}
	created := func(t *testing.T, r reconcile.Reconciler, key client.ObjectKey, stsName string) {
		t.Helper()
		for range 3 {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
		}
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: stsName}, sts))
		got := neo4jPluginsVar(t, sts)
		assert.Empty(t, got.Value)
		require.NotNil(t, got.ValueFrom)
		assert.Equal(t, "plugins", got.ValueFrom.SecretKeyRef.Name)
	}

	t.Run("cluster", func(t *testing.T) {
		cluster := minimalCluster("fm-secret", "default")
		cluster.Spec.Image.Tag = "5.26.32-enterprise"
		cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
		cluster.Spec.Env = env
		require.NoError(t, c.Create(ctx, cluster))
		r := &Neo4jEnterpriseClusterReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000), RequeueAfter: time.Minute,
			Validator: validation.NewClusterValidator(c), ConfigMapManager: NewConfigMapManager(c),
		}
		created(t, r, client.ObjectKeyFromObject(cluster), "fm-secret-server")
	})

	t.Run("standalone", func(t *testing.T) {
		standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "fm-secret-sa", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.32-enterprise"},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				AuraFleetManagement:    &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true},
				Env:                    env,
			},
		}
		require.NoError(t, c.Create(ctx, standalone))
		r := &Neo4jEnterpriseStandaloneReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000), RequeueAfter: time.Minute,
			Validator: validation.NewStandaloneValidator(),
		}
		created(t, r, client.ObjectKeyFromObject(standalone), "fm-secret-sa")
	})
}

// standaloneWith writes the standalone's spec through mutate.
func (h *fleetStandaloneHarness) standaloneWith(mutate func(*neo4jv1beta1.Neo4jEnterpriseStandalone)) {
	h.t.Helper()
	require.NoError(h.t, h.c.Get(context.Background(), client.ObjectKeyFromObject(h.standalone), h.standalone))
	require.NoError(h.t, patchObject(context.Background(), h.c, h.standalone, func() { mutate(h.standalone) }))
}

func (h *fleetStandaloneHarness) settledPasses(n int) (all, rolls int) {
	h.t.Helper()
	h.writes.reset()
	for range n {
		h.pass()
	}
	return h.writes.counts()
}

func standaloneSpecEnvPlugins(e corev1.EnvVar) func(*neo4jv1beta1.Neo4jEnterpriseStandalone) {
	return func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
		e.Name = "NEO4J_PLUGINS"
		s.Spec.Env = []corev1.EnvVar{e}
	}
}

// The standalone never looped: its controller applies its template when the
// hash of what it renders moves, and never puts a live value back, so the
// name its template write adds stays. Its writes: one roll for each toggle —
// the conf hash moves with it — the name kept through a later spec change,
// the user's list back when it goes off, a fleet-management the user wrote
// kept, nothing added on a built-in image.
func TestStandaloneReconcile_ItsOwnNeo4jPluginsWithFleetManagement(t *testing.T) {
	t.Run("settles, and a later spec change keeps the name", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.standaloneWith(standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
		h.pass()
		sts := h.server()
		assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, sts))
		assert.False(t, fleetEntryRecorded(sts))
		all, _ := h.settledPasses(repeatPasses)
		assert.Zero(t, all)

		h.standaloneWith(func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
			s.Spec.Env = append(s.Spec.Env, corev1.EnvVar{Name: "NEO4J_server_memory_pagecache_size", Value: "256m"})
		})
		_, rolls := h.settledPasses(1)
		assert.Equal(t, 1, rolls)
		assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()), "not lost")
		all, _ = h.settledPasses(repeatPasses)
		assert.Zero(t, all)
	})

	t.Run("off and on: one roll each", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.standaloneWith(standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
		h.pass()

		fleetToggle(t, h.c, h.standalone, fleetOff)
		_, rolls := h.settledPasses(1)
		assert.Equal(t, 1, rolls)
		assert.Equal(t, `["apoc"]`, neo4jPlugins(t, h.server()))
		all, _ := h.settledPasses(repeatPasses)
		assert.Zero(t, all)

		fleetToggle(t, h.c, h.standalone, fleetOn)
		_, rolls = h.settledPasses(1)
		assert.Equal(t, 1, rolls)
		assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
		all, _ = h.settledPasses(repeatPasses)
		assert.Zero(t, all)
	})

	t.Run("a fleet-management the user wrote stays", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.standaloneWith(standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["fleet-management","apoc"]`}))
		h.pass()
		fleetToggle(t, h.c, h.standalone, fleetOff)
		_, rolls := h.settledPasses(1)
		assert.Equal(t, 1, rolls, "the conf change's")
		assert.Equal(t, `["fleet-management","apoc"]`, neo4jPlugins(t, h.server()))
	})

	t.Run("built in from 2026.03", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.standaloneWith(func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) {
			standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`})(s)
			s.Spec.Image.Tag = "2026.08.1-enterprise"
		})
		h.pass()
		assert.Equal(t, `["apoc"]`, neo4jPlugins(t, h.server()))
		all, _ := h.settledPasses(repeatPasses)
		assert.Zero(t, all)
	})
}

// Turned on while the standalone's cache still has the ConfigMap from before:
// its StatefulSet write stamps the old conf and changes nothing, and the merge
// leaves the standalone's own variable alone — it wrote the name there, a roll
// of its own — so the pod restarts once, with the write that carries the new
// conf.
func TestStandaloneReconcile_EnablingFleetManagementWithItsOwnNeo4jPluginsIsOneRoll(t *testing.T) {
	ctx := context.Background()
	h := newFleetStandaloneHarness(t, fleetOff)
	h.standaloneWith(standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	h.pass()
	fleetToggle(t, h.c, h.standalone, fleetOn)
	h.writes.reset()

	require.NoError(t, h.r.reconcileStatefulSet(ctx, h.standalone))
	require.NoError(t, h.r.mergeFleetManagementPlugin(ctx, h.standalone))
	all, _ := h.writes.counts()
	assert.Zero(t, all, "the old conf hash, and the merge leaves the standalone's variable alone")

	require.NoError(t, h.r.reconcileConfigMap(ctx, h.standalone))
	require.NoError(t, h.r.reconcileStatefulSet(ctx, h.standalone))
	all, rolls := h.writes.counts()
	assert.Equal(t, 1, all)
	assert.Equal(t, 1, rolls, "the new conf hash and the name, one write")
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, h.server()))
}

// A standalone's NEO4J_PLUGINS from a Secret, or not a JSON list: its
// StatefulSet is written as it renders it (a value beside valueFrom was an env
// var the API server refuses, so the StatefulSet was never created; a value
// that did not parse failed the write), and the validator says so, once.
func TestStandaloneReconcile_ItsOwnNeo4jPluginsTheOperatorCannotAddTo(t *testing.T) {
	for name, e := range map[string]corev1.EnvVar{"from a Secret": fromSecretPlugins, "not a JSON list": {Value: "apoc"}} {
		t.Run(name, func(t *testing.T) {
			h := newFleetStandaloneHarness(t, fleetOn)
			h.standaloneWith(standaloneSpecEnvPlugins(e))
			h.pass()
			got := neo4jPluginsVar(t, h.server())
			assert.Equal(t, e.Value, got.Value)
			assert.Equal(t, e.ValueFrom, got.ValueFrom)
			all, _ := h.settledPasses(repeatPasses)
			assert.Zero(t, all)

			events := recordedEvents(t, h.r.Recorder)
			assert.Empty(t, eventsWithReason(events, EventReasonAuraFleetPluginPatchFailed))
			assert.Len(t, warningsAbout(events), 1, "announced once")
		})
	}
}
