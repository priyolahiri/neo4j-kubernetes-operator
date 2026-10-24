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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A namespace teardown deletes the credentials Secret (and the provider
// config) alongside the Aura CRs. The AuraInstance, AuraCustomerManagedKey and
// AuraIPFilter reconcilers resolved the credentials before they looked at the
// deletion, so once the Secret was gone the finalizer was never released — even
// under deletionPolicy Orphan, the default, which calls nothing at all. Now a
// deletion resolves the credentials only to call Aura, and a Delete that cannot
// waits finalizerDeletionGracePeriod, then releases the finalizer and says the
// resource was left in Aura (knowledge rule 157).

type auraDeletionCase struct {
	name      string
	finalizer string
	// orphaned is the reason of the event that reports the resource left in Aura.
	orphaned string
	// failed is the reason of the Warning that records the wait on status.
	failed string
	// obj builds the CR, its credentials Secret missing, with an external ID
	// and the given deletion policy, terminating since deletedAt.
	obj func(policy string, deletedAt time.Time) client.Object
	// newReconciler returns the reconciler and a probe of whether it called
	// Aura's delete.
	newReconciler func(c client.Client, rec record.EventRecorder) (reconcile.Reconciler, func() bool)
}

// terminatingSince stamps a deletion timestamp at the given time; the
// finalizer keeps the object in the fake client.
func terminatingSince(o client.Object, finalizer string, at time.Time) {
	controllerutil.AddFinalizer(o, finalizer)
	ts := metav1.NewTime(at)
	o.SetDeletionTimestamp(&ts)
}

func auraDeletionCases() []auraDeletionCase {
	return []auraDeletionCase{
		{
			name: "AuraInstance", finalizer: AuraInstanceFinalizer,
			orphaned: EventReasonAuraInstanceOrphaned, failed: EventReasonAuraInstanceFailed,
			obj: func(policy string, at time.Time) client.Object {
				inst := newAuraInstance("inst-gone-creds")
				inst.Spec.CredentialsSecretRef = missingCreds("gone")
				inst.Spec.DeletionPolicy = policy
				inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
				terminatingSince(inst, AuraInstanceFinalizer, at)
				return inst
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) (reconcile.Reconciler, func() bool) {
				api := &fakeAuraAPI{}
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: factoryFor(api)},
					func() bool { return api.deleteCalled }
			},
		},
		{
			name: "AuraCustomerManagedKey", finalizer: AuraCMKFinalizer,
			orphaned: EventReasonAuraCMKOrphaned, failed: EventReasonAuraCMKFailed,
			obj: func(policy string, at time.Time) client.Object {
				cmk := newAuraCMK("cmk-gone-creds")
				cmk.Spec.CredentialsSecretRef = missingCreds("gone")
				cmk.Spec.DeletionPolicy = policy
				cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
				terminatingSince(cmk, AuraCMKFinalizer, at)
				return cmk
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) (reconcile.Reconciler, func() bool) {
				api := &fakeCMKAPI{}
				return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: cmkFactoryFor(api)},
					func() bool { return api.deleteCalled }
			},
		},
		{
			name: "AuraIPFilter", finalizer: AuraIPFilterFinalizer,
			orphaned: EventReasonAuraIPFilterOrphaned, failed: EventReasonAuraIPFilterFailed,
			obj: func(policy string, at time.Time) client.Object {
				f := newAuraIPFilter("ipf-gone-creds")
				f.Spec.CredentialsSecretRef = missingCreds("gone")
				f.Spec.DeletionPolicy = policy
				f.Annotations = map[string]string{AuraExternalIPFilterAnnotation: "ipf-1"}
				terminatingSince(f, AuraIPFilterFinalizer, at)
				return f
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) (reconcile.Reconciler, func() bool) {
				api := &fakeIPFilterAPI{}
				return &AuraIPFilterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: ipFilterFactoryFor(api)},
					func() bool { return api.deleteCalled }
			},
		},
	}
}

// finalizerReleased reports whether the object is gone or no longer carries
// the finalizer.
func finalizerReleased(t *testing.T, c client.Client, proto client.Object, finalizer string) bool {
	t.Helper()
	live, ok := proto.DeepCopyObject().(client.Object)
	require.True(t, ok)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(proto), live); err != nil {
		require.True(t, client.IgnoreNotFound(err) == nil, "unexpected read error: %v", err)
		return true
	}
	return !controllerutil.ContainsFinalizer(live, finalizer)
}

// Orphan calls nothing in Aura, so it needs no credentials: the finalizer is
// released on the first pass even with the Secret gone.
func TestAuraDeletion_OrphanNeedsNoCredentials(t *testing.T) {
	scheme := auraTestScheme(t)
	for _, tc := range auraDeletionCases() {
		t.Run(tc.name, func(t *testing.T) {
			obj := tc.obj("Orphan", time.Now())
			c := newAuraFakeClient(t, scheme, obj)
			rec := record.NewFakeRecorder(20)
			r, deleted := tc.newReconciler(c, rec)

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
			require.NoError(t, err)

			assert.True(t, finalizerReleased(t, c, obj, tc.finalizer),
				"an Orphan deletion must not wait for credentials it never uses")
			assert.False(t, deleted(), "Orphan must not delete in Aura")
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, tc.orphaned), 1, "the orphaning is reported: %q", events)
			assert.Empty(t, eventsWithReason(events, tc.failed), "nothing failed: %q", events)
		})
	}
}

// Delete has to call Aura. Without credentials it waits — the wait recorded
// on status and announced once — and after finalizerDeletionGracePeriod it
// releases the finalizer, says the resource was left in Aura, and never calls
// the delete it could not authenticate.
func TestAuraDeletion_DeleteWithoutCredentialsWaitsThenOrphans(t *testing.T) {
	scheme := auraTestScheme(t)
	for _, tc := range auraDeletionCases() {
		t.Run(tc.name+" within the grace period", func(t *testing.T) {
			obj := tc.obj("Delete", time.Now())
			c := newAuraFakeClient(t, scheme, obj)
			rec := record.NewFakeRecorder(20)
			r, deleted := tc.newReconciler(c, rec)
			key := client.ObjectKeyFromObject(obj)

			for range 3 {
				res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
				assert.Positive(t, res.RequeueAfter, "the wait is retried")
			}

			assert.False(t, finalizerReleased(t, c, obj, tc.finalizer), "the finalizer is kept while waiting")
			assert.False(t, deleted())
			events := drainEvents(rec)
			waits := eventsWithReason(events, tc.failed)
			require.Len(t, waits, 1, "the wait is announced once: %q", events)
			assert.Contains(t, waits[0], "deletionPolicy is Delete")
			assert.Empty(t, eventsWithReason(events, tc.orphaned))

			live, ok := obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(context.Background(), key, live))
			conds, phase := auraStatusOf(t, live)
			ready := findCondition(conds, "Ready")
			require.NotNil(t, ready)
			assert.Equal(t, "CredentialsUnavailable", ready.Reason)
			assert.Contains(t, ready.Message, "is left in Aura")
			assert.Equal(t, neo4jv1beta1.PhaseError, phase)
		})

		t.Run(tc.name+" after the grace period", func(t *testing.T) {
			obj := tc.obj("Delete", time.Now().Add(-finalizerDeletionGracePeriod-time.Minute))
			c := newAuraFakeClient(t, scheme, obj)
			rec := record.NewFakeRecorder(20)
			r, deleted := tc.newReconciler(c, rec)

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
			require.NoError(t, err)

			assert.True(t, finalizerReleased(t, c, obj, tc.finalizer), "the deletion must not wedge")
			assert.False(t, deleted(), "no delete without credentials")
			orphaned := eventsWithReason(drainEvents(rec), tc.orphaned)
			require.Len(t, orphaned, 1)
			assert.True(t, strings.HasPrefix(orphaned[0], corev1.EventTypeWarning+" "),
				"leaving a resource the user asked to delete is a Warning: %q", orphaned[0])
			assert.Contains(t, orphaned[0], "although deletionPolicy is Delete")
		})
	}
}

// auraStatusOf returns the conditions and phase of one of the Aura kinds above.
func auraStatusOf(t *testing.T, o client.Object) ([]metav1.Condition, string) {
	t.Helper()
	switch v := o.(type) {
	case *neo4jv1beta1.AuraInstance:
		return v.Status.Conditions, v.Status.Phase
	case *neo4jv1beta1.AuraCustomerManagedKey:
		return v.Status.Conditions, v.Status.Phase
	case *neo4jv1beta1.AuraIPFilter:
		return v.Status.Conditions, v.Status.Phase
	}
	t.Fatalf("no status accessor for %T", o)
	return nil, ""
}
