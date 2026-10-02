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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// drainEvents returns and removes every event currently buffered on the recorder.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// fleetFailureEvents keeps only the AuraFleetManagementFailed Warning events.
func fleetFailureEvents(events []string) []string {
	var out []string
	for _, e := range events {
		if strings.HasPrefix(e, corev1.EventTypeWarning+" "+EventReasonAuraFleetFailed+" ") {
			out = append(out, e)
		}
	}
	return out
}

func fleetTestStatefulSet(name string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:5.26-enterprise"}}},
			},
		},
	}
}

func fleetTokenRef(secret string) *neo4jv1beta1.AuraFleetManagementSpec {
	return &neo4jv1beta1.AuraFleetManagementSpec{
		Enabled:        true,
		TokenSecretRef: &neo4jv1beta1.SecretKeyRef{Name: secret, Key: "token"},
	}
}

// Every registration failure path returns setFleetManagementStatus(...), which
// returns nil once the status is written, so the caller's `err != nil` branch —
// the only place AuraFleetManagementFailed was emitted — never fired. The
// failure reached status.auraFleetManagement.message and nothing else.
//
// This drives the failure through the reconciler and pins the contract:
//
//   - a failure raises ONE Warning event, reason AuraFleetManagementFailed;
//   - the same failure on the next reconcile raises none (no event spam);
//   - a CHANGED failure message raises a fresh one;
//   - the reconcile itself stays non-fatal (nil error, status still written).
func TestFleetRegistrationFailure_EmitsWarningOnlyWhenTheMessageChanges_Standalone(t *testing.T) {
	scheme := auraTestScheme(t)
	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNS},
		Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: "Ready"},
	}
	c := newAuraFakeClient(t, scheme, sa, fleetTestStatefulSet("sa"))
	rec := record.NewFakeRecorder(50)
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: rec}
	ctx := context.Background()

	reconcile := func() {
		t.Helper()
		got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sa), got))
		require.NoError(t, r.reconcileAuraFleetManagement(ctx, got),
			"a registration failure must stay non-fatal to the reconcile")
	}

	// 1. Token Secret absent: first failure announces itself.
	reconcile()
	ev := fleetFailureEvents(drainEvents(rec))
	require.Len(t, ev, 1, "the first failure must raise an AuraFleetManagementFailed event")
	assert.Contains(t, ev[0], "cannot read token secret fleet-token")

	// 2. Unchanged failure on the next reconcile: silent.
	reconcile()
	assert.Empty(t, fleetFailureEvents(drainEvents(rec)), "an unchanged failure must not re-emit")

	// 3. Failure message changes (Secret now exists but lacks the key): announces again.
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "fleet-token", Namespace: testNS},
		Data:       map[string][]byte{"other": []byte("x")},
	}))
	reconcile()
	ev = fleetFailureEvents(drainEvents(rec))
	require.Len(t, ev, 1, "a changed failure message must raise a fresh event")
	assert.Contains(t, ev[0], `key "token" not found in secret fleet-token`)

	// 4. And that one is deduplicated too.
	reconcile()
	assert.Empty(t, fleetFailureEvents(drainEvents(rec)))

	// The status message is still written — the event adds to it, not replaces it.
	got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sa), got))
	require.NotNil(t, got.Status.AuraFleetManagement)
	assert.False(t, got.Status.AuraFleetManagement.Registered)
	assert.Contains(t, got.Status.AuraFleetManagement.Message, `key "token" not found`)
}

func TestFleetRegistrationFailure_EmitsWarningOnlyWhenTheMessageChanges_Cluster(t *testing.T) {
	scheme := auraTestScheme(t)
	cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: testNS},
		Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		Status:     neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: "Ready"},
	}
	c := newAuraFakeClient(t, scheme, cl, fleetTestStatefulSet("cl-server"))
	rec := record.NewFakeRecorder(50)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}
	ctx := context.Background()

	reconcile := func() {
		t.Helper()
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cl), got))
		require.NoError(t, r.reconcileAuraFleetManagement(ctx, got),
			"a registration failure must stay non-fatal to the reconcile")
	}

	reconcile()
	ev := fleetFailureEvents(drainEvents(rec))
	require.Len(t, ev, 1, "the first failure must raise an AuraFleetManagementFailed event")
	assert.Contains(t, ev[0], "cannot read token secret fleet-token")

	reconcile()
	assert.Empty(t, fleetFailureEvents(drainEvents(rec)), "an unchanged failure must not re-emit")

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "fleet-token", Namespace: testNS},
		Data:       map[string][]byte{"other": []byte("x")},
	}))
	reconcile()
	ev = fleetFailureEvents(drainEvents(rec))
	require.Len(t, ev, 1, "a changed failure message must raise a fresh event")
	assert.Contains(t, ev[0], `key "token" not found in secret fleet-token`)

	reconcile()
	assert.Empty(t, fleetFailureEvents(drainEvents(rec)))

	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cl), got))
	require.NotNil(t, got.Status.AuraFleetManagement)
	assert.False(t, got.Status.AuraFleetManagement.Registered)
	assert.Contains(t, got.Status.AuraFleetManagement.Message, `key "token" not found`)
}

// Two clusters failing identically must each announce: the dedupe is per
// resource, not global.
func TestFleetFailureTracker_IsPerResource(t *testing.T) {
	var tr fleetFailureTracker
	a := client.ObjectKey{Namespace: "ns", Name: "a"}
	b := client.ObjectKey{Namespace: "ns", Name: "b"}

	assert.True(t, tr.shouldAnnounce(a, "boom"))
	assert.False(t, tr.shouldAnnounce(a, "boom"))
	assert.True(t, tr.shouldAnnounce(b, "boom"), "a different resource has its own last failure")
	assert.True(t, tr.shouldAnnounce(a, "different"))

	// A success clears it, so a recurrence after recovery is announced again.
	tr.clear(a)
	assert.True(t, tr.shouldAnnounce(a, "different"))
}
