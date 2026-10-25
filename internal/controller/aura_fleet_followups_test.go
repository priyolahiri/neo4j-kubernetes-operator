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
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// refusedWithNewRequestIDs returns an Aura refusal that, like the live API,
// carries a request ID of its own on every call.
func refusedWithNewRequestIDs(status int) func() error {
	n := 0
	return func() error {
		n++
		return &aura.APIError{StatusCode: status, Message: "the credential may not do this", RequestID: fmt.Sprintf("req-%d", n)}
	}
}

// ---------------------------------------------------------------------------
// The provider config validates its credentials every 10 minutes by default.
// ---------------------------------------------------------------------------

func TestAuraProviderConfig_RevalidatesEveryTenMinutesByDefault(t *testing.T) {
	scheme := auraTestScheme(t)
	pc := &neo4jv1beta1.AuraProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: testNS},
		Spec:       neo4jv1beta1.AuraProviderConfigSpec{CredentialsSecretRef: *credRef(), DefaultProjectID: "proj-1"},
	}
	c := newAuraFakeClient(t, scheme, pc)
	r := &AuraProviderConfigReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
		ClientFactory: factoryFor(&fakeAuraAPI{})}
	res, err := r.Reconcile(context.Background(), reqFor(pc))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, res.RequeueAfter)
}

// ---------------------------------------------------------------------------
// A failure that repeats is one status write and one Warning (rule 184).
//
// Aura gives every response its own request ID, and an APIError's text
// carries it. Status messages built from that text changed on every retry:
// each was a status write — a watch event that starts the next pass at once —
// and a Warning "announced once per failure" announced on every pass, a loop
// at the speed of the Aura rate limit.
// ---------------------------------------------------------------------------

func TestAuraFail_ARefusalThatRepeatsIsOneWriteAndOneWarning(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("AuraInstance observe", func(t *testing.T) {
		inst := newAuraInstance("inst-403")
		inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
		controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
		refuse := refusedWithNewRequestIDs(http.StatusForbidden)
		api := &fakeAuraAPI{getInstanceFn: func(context.Context, string) (*aura.Instance, error) { return nil, refuse() }}
		c := newAuraFakeClient(t, scheme, inst)
		rec := record.NewFakeRecorder(20)
		r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(api)}
		status := func() neo4jv1beta1.AuraInstanceStatus {
			t.Helper()
			got := &neo4jv1beta1.AuraInstance{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(inst), got))
			return got.Status
		}

		passes(t, r, inst, 1)
		first := status()
		passes(t, r, inst, 2)
		// The API server drops an Update that changes nothing, with no watch
		// event; one that changes the message is a new pass.
		assert.Equal(t, first, status(), "the same refusal with a new request ID changed the status")
		assert.NotContains(t, findCondition(first.Conditions, "Ready").Message, "request-id")
		events := eventsWithReason(drainEvents(rec), EventReasonAuraInstanceFailed)
		require.Len(t, events, 1, "announced once: %q", events)
		assert.Contains(t, events[0], "request-id=req-1", "the Warning keeps the request ID for Aura support")
	})

	t.Run("AuraProviderConfig", func(t *testing.T) {
		pc := &neo4jv1beta1.AuraProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-401", Namespace: testNS},
			Spec:       neo4jv1beta1.AuraProviderConfigSpec{CredentialsSecretRef: *credRef(), DefaultProjectID: "proj-1"},
		}
		refuse := refusedWithNewRequestIDs(http.StatusUnauthorized)
		api := &fakeAuraAPI{listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) { return nil, refuse() }}
		c := newAuraFakeClient(t, scheme, pc)
		rec := record.NewFakeRecorder(20)
		r := &AuraProviderConfigReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(api)}

		passes(t, r, pc, 3)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraCredentialsInvalid), 1)
	})

	t.Run("fleet provisioning", func(t *testing.T) {
		cl := fleetCluster("cluster", fleetProvisionSpec())
		api := &fakeFleetAPI{listErr: refusedWithNewRequestIDs(http.StatusForbidden)}
		c := newAuraFakeClient(t, scheme, cl)
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
			FleetClientFactory: func(auraCredentials) auraFleetAPI { return api }}
		provision := func() {
			t.Helper()
			live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cl), live))
			require.NoError(t, r.newFleetProvisioner().reconcileAuraFleetProvision(context.Background(), live))
		}

		provision()
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cl), got))
		first := got.Status.AuraFleetManagement.Message
		assert.Contains(t, first, "cannot ensure fleet deployment")
		assert.NotContains(t, first, "request-id")

		provision()
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cl), got))
		assert.Equal(t, first, got.Status.AuraFleetManagement.Message,
			"a refusal that repeats must read the same: every status write of a cluster starts another cluster pass")
	})
}

// ---------------------------------------------------------------------------
// AuraSnapshot: a failure is phase Error, announced once; a terminal snapshot
// is not observed again (rule 156).
// ---------------------------------------------------------------------------

func snapshotOf(name string) *neo4jv1beta1.AuraSnapshot {
	return &neo4jv1beta1.AuraSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       neo4jv1beta1.AuraSnapshotSpec{InstanceRef: "snap-src"},
	}
}

func snapshotSource() *neo4jv1beta1.AuraInstance {
	inst := newAuraInstance("snap-src")
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
	return inst
}

func TestAuraSnapshot_AFailureIsPhaseErrorAnnouncedOnce(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("the create Aura refuses", func(t *testing.T) {
		snap := snapshotOf("snap-refused")
		refuse := refusedWithNewRequestIDs(http.StatusBadRequest)
		creates := 0
		api := &fakeAuraAPI{createSnapshotFn: func(_ context.Context, id string) (*aura.Snapshot, error) {
			creates++
			if creates > 3 {
				return &aura.Snapshot{InstanceID: id, SnapshotID: "snap-1"}, nil
			}
			return nil, refuse()
		}}
		c := newAuraFakeClient(t, scheme, snapshotSource(), snap)
		rec := record.NewFakeRecorder(20)
		newR := func() *AuraSnapshotReconciler {
			return &AuraSnapshotReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(api)}
		}

		passes(t, newR(), snap, 2)
		got := &neo4jv1beta1.AuraSnapshot{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(snap), got))
		assert.Equal(t, neo4jv1beta1.PhaseError, got.Status.Phase, "a failure shows in the phase")
		ready := findCondition(got.Status.Conditions, "Ready")
		require.NotNil(t, ready)
		assert.Equal(t, "CreateFailed", ready.Reason)
		assert.NotContains(t, ready.Message, "request-id")
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraSnapshotFailed), 1, "announced once")

		passes(t, newR(), snap, 1)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonAuraSnapshotFailed), "a restart announces nothing")

		passes(t, newR(), snap, 1) // the fourth create succeeds
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(snap), got))
		assert.Equal(t, aura.SnapshotStatusPending, got.Status.Phase)
		assert.Equal(t, "snap-1", got.Status.SnapshotID)
	})

	t.Run("the poll Aura refuses", func(t *testing.T) {
		snap := snapshotOf("snap-unreadable")
		snap.Status = neo4jv1beta1.AuraSnapshotStatus{SnapshotID: "snap-1", Phase: aura.SnapshotStatusInProgress}
		refuse := refusedWithNewRequestIDs(http.StatusForbidden)
		api := &fakeAuraAPI{getSnapshotFn: func(context.Context, string, string) (*aura.Snapshot, error) { return nil, refuse() }}
		c := newAuraFakeClient(t, scheme, snapshotSource(), snap)
		rec := record.NewFakeRecorder(20)
		r := &AuraSnapshotReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(api)}

		passes(t, r, snap, 3)
		got := &neo4jv1beta1.AuraSnapshot{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(snap), got))
		assert.Equal(t, neo4jv1beta1.PhaseError, got.Status.Phase)
		assert.Equal(t, "ObserveFailed", findCondition(got.Status.Conditions, "Ready").Reason)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraSnapshotFailed), 1)
	})

	t.Run("a terminal snapshot is not observed again", func(t *testing.T) {
		snap := snapshotOf("snap-done")
		snap.Status = neo4jv1beta1.AuraSnapshotStatus{SnapshotID: "snap-1", Phase: aura.SnapshotStatusCompleted}
		gets := 0
		api := &fakeAuraAPI{getSnapshotFn: func(context.Context, string, string) (*aura.Snapshot, error) {
			gets++
			return nil, &aura.APIError{StatusCode: http.StatusForbidden, Message: "credential revoked"}
		}}
		c := newAuraFakeClient(t, scheme, snapshotSource(), snap)
		r := &AuraSnapshotReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20), ClientFactory: factoryFor(api)}

		passes(t, r, snap, 2)
		assert.Zero(t, gets, "a Completed snapshot was read again")
		got := &neo4jv1beta1.AuraSnapshot{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(snap), got))
		assert.Equal(t, aura.SnapshotStatusCompleted, got.Status.Phase)
	})
}

// ---------------------------------------------------------------------------
// Fleet deployment: only a failed Aura call is retried quietly (rule 88).
// ---------------------------------------------------------------------------

func TestFleetProvision_AFailedDeploymentAnnotationWriteIsReported(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("the annotation write fails", func(t *testing.T) {
		cl := fleetCluster("cluster", fleetProvisionSpec())
		api := &fakeFleetAPI{}
		c := auraFakeClientBuilder(t, scheme, cl).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cw client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok {
					return errors.New(`admission webhook "policy.example.com" denied the request`)
				}
				return cw.Patch(ctx, obj, patch, opts...)
			},
		}).Build()
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
			FleetClientFactory: func(auraCredentials) auraFleetAPI { return api }}

		require.NoError(t, r.newFleetProvisioner().reconcileAuraFleetProvision(context.Background(), cl))
		assert.True(t, api.createCalled)
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cl), got))
		require.NotNil(t, got.Status.AuraFleetManagement, "the failure must reach status")
		assert.Contains(t, got.Status.AuraFleetManagement.Message, "cannot ensure fleet deployment")
		assert.Contains(t, got.Status.AuraFleetManagement.Message, "denied the request")
		assert.False(t, api.mintCalled, "no token is minted for a deployment the CR does not record")
	})

	t.Run("a transient failure of the Aura call is retried quietly", func(t *testing.T) {
		cl := fleetCluster("cluster", fleetProvisionSpec())
		api := &fakeFleetAPI{listErr: func() error {
			return &aura.APIError{StatusCode: http.StatusServiceUnavailable, Message: "upstream unavailable"}
		}}
		_, got := runFleetProvision(t, cl, api)
		assert.True(t, got.Status.AuraFleetManagement == nil || got.Status.AuraFleetManagement.Message == "",
			"a 503 is retried without a status message: %+v", got.Status.AuraFleetManagement)
	})
}

// ---------------------------------------------------------------------------
// AuraFleetManagementPluginPatchFailed is announced once per cause (rule 145).
// ---------------------------------------------------------------------------

func TestFleetPluginPatchFailed_IsAnnouncedOncePerCause(t *testing.T) {
	scheme := auraTestScheme(t)
	ctx := context.Background()
	brokenSTS := func(name string) *appsv1.StatefulSet {
		sts := fleetTestStatefulSet(name)
		sts.Spec.Template.Spec.Containers[0].Name = "not-neo4j"
		return sts
	}
	setContainer := func(t *testing.T, c client.Client, name, container string) {
		t.Helper()
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, sts))
		sts.Spec.Template.Spec.Containers[0].Name = container
		require.NoError(t, c.Update(ctx, sts))
	}

	t.Run("cluster", func(t *testing.T) {
		cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: testNS},
			Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		}
		c := newAuraFakeClient(t, scheme, cl, brokenSTS("cl-server"))
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}
		for range 3 {
			require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed), 1, "announced once while it lasts")

		setContainer(t, c, "cl-server", "neo4j")
		require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
		setContainer(t, c, "cl-server", "renamed-again")
		require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed), 1,
			"a failure that comes back after a patch went through is announced again")
	})

	t.Run("standalone", func(t *testing.T) {
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNS},
			Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		}
		c := newAuraFakeClient(t, scheme, sa, brokenSTS("sa"))
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: rec}
		for range 3 {
			require.NoError(t, r.reconcileAuraFleetManagement(ctx, sa))
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed), 1, "announced once while it lasts")
	})
}

// The patch failure was also logged as an ERROR on every pass — about eight a
// minute on the live walk — while its event fired once. It is logged at info
// when announced, at V(1) on the passes in between, and the reconcile goes on
// without an error (operations.md ids 145, 164).
func TestFleetPluginPatchFailed_IsLoggedOnceNotAsAnError(t *testing.T) {
	scheme := auraTestScheme(t)
	brokenSTS := func(name string) *appsv1.StatefulSet {
		sts := fleetTestStatefulSet(name)
		sts.Spec.Template.Spec.Containers[0].Name = "not-neo4j"
		return sts
	}
	cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: testNS},
		Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
	}
	clusterR := &Neo4jEnterpriseClusterReconciler{Client: newAuraFakeClient(t, scheme, cl, brokenSTS("cl-server")),
		Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNS},
		Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
	}
	standaloneR := &Neo4jEnterpriseStandaloneReconciler{Client: newAuraFakeClient(t, scheme, sa, brokenSTS("sa")),
		Scheme: scheme, Recorder: record.NewFakeRecorder(20)}

	for name, pass := range map[string]func(context.Context) error{
		"cluster":    func(ctx context.Context) error { return clusterR.reconcileAuraFleetManagement(ctx, cl) },
		"standalone": func(ctx context.Context) error { return standaloneR.reconcileAuraFleetManagement(ctx, sa) },
	} {
		t.Run(name, func(t *testing.T) {
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			for range 3 {
				require.NoError(t, pass(ctx), "a lasting patch failure is not a reconcile error")
			}
			assert.Empty(t, sink.errors(), "a lasting patch failure is not logged as an ERROR")
			assert.Len(t, sink.mentioning(0, "for fleet-management"), 1, "logged at info when it is announced")
			assert.Len(t, sink.mentioning(1, "for fleet-management"), 2, "and at V(1) on the passes in between")
		})
	}
}

// ---------------------------------------------------------------------------
// AuraFleetManagementPluginPatchFailed names what failed (rule 192).
//
// It said "Failed to add fleet-management to NEO4J_PLUGINS" whatever failed —
// also on an image with Fleet Manager built in (2026.03+), where the merge
// never adds anything and all a pass does is read the StatefulSet and look at
// its neo4j container. "Failed to add" is now said only of the write.
// ---------------------------------------------------------------------------

func TestFleetPluginPatchFailed_NamesWhatFailed(t *testing.T) {
	scheme := auraTestScheme(t)
	ctx := context.Background()
	refused := errors.New("the API server refused the request")
	readsRefused := interceptor.Funcs{
		Get: func(ctx context.Context, cw client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				return refused
			}
			return cw.Get(ctx, key, obj, opts...)
		},
	}
	writesRefused := interceptor.Funcs{
		Update: func(ctx context.Context, cw client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				return refused
			}
			return cw.Update(ctx, obj, opts...)
		},
	}
	const builtIn = "neo4j:2026.08.1-enterprise"
	const check = "Could not check the server StatefulSet's NEO4J_PLUGINS for fleet-management: "
	cases := []struct {
		name      string
		image     string
		container string
		funcs     interceptor.Funcs
		want      string
	}{
		{"a built-in image, the StatefulSet unreadable", builtIn, "neo4j", readsRefused, check + "get StatefulSet"},
		{"a built-in image, no neo4j container", builtIn, "not-neo4j", interceptor.Funcs{}, check + "neo4j container not found"},
		{"an image that needs the plugin, the write refused", "neo4j:5.26.12-enterprise", "neo4j", writesRefused,
			"Failed to add fleet-management to NEO4J_PLUGINS: the API server refused the request"},
	}
	for _, tc := range cases {
		server := func(name string) *appsv1.StatefulSet {
			sts := fleetTestStatefulSet(name)
			sts.Spec.Template.Spec.Containers[0].Name = tc.container
			sts.Spec.Template.Spec.Containers[0].Image = tc.image
			return sts
		}
		passes := map[string]func(t *testing.T, rec record.EventRecorder){
			"cluster": func(t *testing.T, rec record.EventRecorder) {
				cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
					ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: testNS},
					Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
				}
				r := &Neo4jEnterpriseClusterReconciler{Scheme: scheme, Recorder: rec,
					Client: auraFakeClientBuilder(t, scheme, cl, server("cl-server")).WithInterceptorFuncs(tc.funcs).Build()}
				require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
			},
			"standalone": func(t *testing.T, rec record.EventRecorder) {
				sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
					ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNS},
					Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
				}
				r := &Neo4jEnterpriseStandaloneReconciler{Scheme: scheme, Recorder: rec,
					Client: auraFakeClientBuilder(t, scheme, sa, server("sa")).WithInterceptorFuncs(tc.funcs).Build()}
				require.NoError(t, r.reconcileAuraFleetManagement(ctx, sa))
			},
		}
		for kind, pass := range passes {
			t.Run(kind+": "+tc.name, func(t *testing.T) {
				rec := record.NewFakeRecorder(20)
				pass(t, rec)
				failed := eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed)
				require.Len(t, failed, 1)
				assert.Contains(t, failed[0], tc.want)
				if tc.image == builtIn {
					assert.NotContains(t, failed[0], "Failed to add",
						"nothing is added on an image with Fleet Manager built in: %q", failed[0])
				}
			})
		}
	}
}
