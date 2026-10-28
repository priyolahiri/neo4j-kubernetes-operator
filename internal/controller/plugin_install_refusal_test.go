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

// A refused Neo4jPlugin logged ERROR "Failed to install plugin", with a stack
// trace, on every retry — about every 27 seconds — on the walk of #613: a
// Managed GDS plugin on a cluster whose own spec.env NEO4J_PLUGINS lacks its
// name (#608). A refusal the user has to fix is a status: Failed (Invalid for
// a refused spec), one event and one info line from the status write that
// records it, V(1) on the passes after, never an ERROR (operations.md id 228).

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// reconcilePluginWith runs one pass of the plugin named name with ctx, which
// carries the test's logger.
func reconcilePluginWith(ctx context.Context, t *testing.T, r *Neo4jPluginReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: selfHealNS, Name: name}})
	require.NoError(t, err, "a refusal is not a reconcile error")
	return res
}

// ownedPluginsSTS is cluster "c"'s server StatefulSet whose NEO4J_PLUGINS the
// cluster renders from its own spec.env (its record of its env lists it).
func ownedPluginsSTS(value string) *appsv1.StatefulSet {
	sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: value})
	sts.Annotations = map[string]string{ownedEnvVarsAnnotation: "NEO4J_AUTH,NEO4J_PLUGINS"}
	return sts
}

func TestReconcile_ARefusedPluginIsLoggedOnceNotAsAnError(t *testing.T) {
	gds := func() *neo4jv1beta1.Neo4jPlugin {
		p := selfHealPlugin("gds", "c", "", "")
		p.Spec.Name, p.Spec.Config = "graph-data-science", nil
		return p
	}
	cases := map[string]struct {
		plugin    *neo4jv1beta1.Neo4jPlugin
		objs      []client.Object
		validator bool
		phase     string
		reason    string // the event, once
		logged    string // the line: once at info, then at V(1)
		message   string // in status.message
	}{
		// The walk's case: the cluster puts its own value back, so the plugin
		// does not write it (id 135).
		"the cluster's own NEO4J_PLUGINS lacks its name": {
			plugin: gds(), objs: []client.Object{readyCluster(), ownedPluginsSTS(`["apoc"]`)},
			phase: neo4jv1beta1.PhaseFailed, reason: EventReasonPluginInstallFailed,
			logged: "Plugin installation refused", message: "comes from Neo4jEnterpriseCluster c itself",
		},
		"a NEO4J_PLUGINS that is not a JSON list": {
			plugin: gds(), objs: []client.Object{readyCluster(), selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: "apoc"})},
			phase: neo4jv1beta1.PhaseFailed, reason: EventReasonPluginInstallFailed,
			logged: "Plugin installation refused", message: "is not a JSON list of plugin names",
		},
		"a spec the validator refuses": {
			plugin: func() *neo4jv1beta1.Neo4jPlugin { p := gds(); p.Spec.Version = ""; return p }(),
			objs:   []client.Object{readyCluster(), selfHealSTS()}, validator: true,
			phase: "Invalid", reason: EventReasonValidationFailed,
			logged: "Neo4jPlugin spec is invalid", message: "plugin version must be specified",
		},
		"a name the target's image cannot install": {
			plugin: func() *neo4jv1beta1.Neo4jPlugin { p := gds(); p.Spec.Name = "graphql"; return p }(),
			objs:   []client.Object{readyCluster(), selfHealSTS()},
			phase:  neo4jv1beta1.PhaseFailed, reason: EventReasonPluginUnknownToImage,
			logged: "Plugin refused: its target's image cannot install it", message: "not a plugin the neo4j:",
		},
		"a newer duplicate": {
			plugin: func() *neo4jv1beta1.Neo4jPlugin {
				p := gds()
				p.CreationTimestamp = metav1.NewTime(time.Now())
				return p
			}(),
			objs: []client.Object{readyCluster(), selfHealSTS(), func() client.Object {
				p := gds()
				p.Name, p.UID = "gds-older", "uid-older"
				p.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				return p
			}()},
			phase: neo4jv1beta1.PhaseFailed, reason: EventReasonPluginDuplicate,
			logged: "Duplicate Neo4jPlugin", message: "duplicate Neo4jPlugin",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			asked := 0
			bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { asked++; return true }}
			r, rec, stsUpdates := selfHealReconciler(t, bolt, nil, append([]client.Object{tc.plugin}, append(tc.objs, readyClusterPods()...)...)...)
			if tc.validator {
				r.Validator = validation.NewPluginValidator()
			}
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))

			for range 3 {
				res := reconcilePluginWith(ctx, t, r, tc.plugin.Name)
				if tc.phase == neo4jv1beta1.PhaseFailed && tc.reason != EventReasonPluginDuplicate {
					assert.Positive(t, res.RequeueAfter, "looked at again: the fix is in the target's spec")
				}
			}

			got := getPlugin(t, r, tc.plugin.Name)
			assert.Equal(t, tc.phase, got.Status.Phase)
			assert.Contains(t, got.Status.Message, tc.message)
			assert.Len(t, eventsWithReason(drainEvents(rec), tc.reason), 1, "announced once")
			assert.Empty(t, sink.errors(), "a refusal is not logged as an ERROR")
			assert.Len(t, sink.mentioning(0, tc.logged), 1, "logged at info when it is announced")
			assert.Len(t, sink.mentioning(1, tc.logged), 2, "and at V(1) on the passes after")
			assert.Zero(t, *stsUpdates, "nothing written to the target")
			if tc.reason == EventReasonPluginInstallFailed {
				assert.Zero(t, asked, "refused before the target is asked anything")
			}
		})
	}
}

// The refusal is decided from the StatefulSet, before the gate that waits for
// a Ready target: a cluster that is forming does not hold it back as Waiting.
func TestReconcile_AnInstallRefusalIsNotAWaitForTheTarget(t *testing.T) {
	plugin := selfHealPlugin("gds", "c", "", "")
	plugin.Spec.Name, plugin.Spec.Config = "graph-data-science", nil
	forming := minimalCluster("c", selfHealNS)
	forming.Status.Phase = "Forming"
	r, rec, stsUpdates := selfHealReconciler(t, nil, nil, plugin, forming, ownedPluginsSTS(`["apoc"]`))

	reconcilePlugin(t, r, "gds")
	got := getPlugin(t, r, "gds")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, "Plugin installation refused: NEO4J_PLUGINS on StatefulSet c-server comes from Neo4jEnterpriseCluster c itself")
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallFailed), 1)
	assert.Zero(t, *stsUpdates)

	// Fixed in the cluster's spec — its list names the plugin now: the next
	// look installs it.
	sts := liveSTS(t, r.Client, "c-server")
	sts.Spec.Template.Spec.Containers[0].Env[0].Value = `["apoc","graph-data-science"]`
	require.NoError(t, r.Update(context.Background(), sts))
	*stsUpdates = 0
	reconcilePlugin(t, r, "gds")
	assert.NotEqual(t, neo4jv1beta1.PhaseFailed, getPlugin(t, r, "gds").Status.Phase, "no longer refused")
}

// A real failure is still an error in the log: the API server refusing the
// write is not something the plugin's target spec says.
func TestReconcile_ARealInstallFailureIsStillLoggedAsAnError(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Installing plugin")
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	refused := func() error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "c-server",
			errors.New("exceeded quota: compute-resources"))
	}
	r, _, _ := selfHealReconciler(t, bolt, refused, append([]client.Object{plugin, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
	sink := newLevelCaptureSink()
	ctx := log.IntoContext(context.Background(), logr.New(sink))

	reconcilePluginWith(ctx, t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, getPlugin(t, r, "apoc").Status.Phase)
	assert.Contains(t, sink.errors(), "Failed to install plugin")
}

func TestPluginInstallRefusal(t *testing.T) {
	owned := &pluginEnvOwnedError{statefulSet: "c-server", cluster: "c", names: []string{"NEO4J_PLUGINS"}}
	unreadable := &pluginListUnreadableError{err: errors.New("invalid character 'a'")}
	assert.Same(t, owned, pluginInstallRefusal(owned))
	assert.Same(t, owned, pluginInstallRefusal(fmt.Errorf("failed to update StatefulSet with plugin configuration: %w", owned)),
		"unwrapped, so the message is the same whichever step met it")
	assert.Same(t, unreadable, pluginInstallRefusal(fmt.Errorf("x: %w", unreadable)))
	assert.Nil(t, pluginInstallRefusal(nil))
	assert.Nil(t, pluginInstallRefusal(apierrors.NewForbidden(schema.GroupResource{Resource: "statefulsets"}, "c-server", errors.New("quota"))))
}
