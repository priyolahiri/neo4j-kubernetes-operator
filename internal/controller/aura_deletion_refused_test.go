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
	"net/http"
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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// A DELETE Aura refused — not a conflict, not transient, not "not found" — was
// returned from Reconcile: error backoff for ever, a "Reconciler error" line
// each time, nothing on status, and a CR that never went away. It is waited on
// like a deletion without credentials (rule 157): recorded on status with one
// Warning, asked again for finalizerDeletionGracePeriod, then the finalizer is
// released with the kind's Orphaned Warning naming what is left in Aura
// (knowledge rule 201).

// auraRefusedDelete is a DELETE Aura refuses for good: a 403 is neither a
// conflict nor transient.
func auraRefusedDelete() error {
	return &aura.APIError{StatusCode: http.StatusForbidden, Reason: "forbidden",
		Message: "the credential may not delete this", RequestID: "req-refused-1"}
}

type auraRefusalCase struct {
	name      string
	finalizer string
	orphaned  string
	failed    string
	deleted   string // the reason of the Normal event of a deletion that went through
	// objs builds the CR (with credentials, an external ID and deletionPolicy
	// Delete, terminating since deletedAt) and what it needs beside it.
	objs func(deletedAt time.Time) (client.Object, []client.Object)
	// newReconciler returns the reconciler, its Aura DELETE answering err, and a
	// probe of whether it was called.
	newReconciler func(c client.Client, rec record.EventRecorder, err error) (reconcile.Reconciler, func() bool)
}

// auraRefusalCases are the kinds of auraDeletionCases with their credentials
// in place, and the AuraDatabase.
func auraRefusalCases() []auraRefusalCase {
	reasons := map[string]string{
		"AuraInstance":           EventReasonAuraInstanceDeleted,
		"AuraCustomerManagedKey": EventReasonAuraCMKDeleted,
		"AuraIPFilter":           EventReasonAuraIPFilterDeleted,
		"AuraInvite":             EventReasonAuraInviteDeleted,
		"AuraOrganizationMember": EventReasonAuraMemberRemoved,
		"AuraProjectMember":      EventReasonAuraMemberRemoved,
	}
	var out []auraRefusalCase
	for _, base := range auraDeletionCases() {
		build := base.obj
		out = append(out, auraRefusalCase{
			name: base.name, finalizer: base.finalizer, orphaned: base.orphaned, failed: base.failed,
			deleted: reasons[base.name],
			objs: func(at time.Time) (client.Object, []client.Object) {
				obj := build("Delete", at)
				withAuraCredentials(obj)
				return obj, nil
			},
			newReconciler: refusingReconciler(base.name),
		})
	}
	return append(out, auraRefusalCase{
		name: "AuraDatabase", finalizer: AuraDatabaseFinalizer,
		orphaned: EventReasonAuraDatabaseOrphaned, failed: EventReasonAuraDatabaseFailed, deleted: EventReasonAuraDatabaseDeleted,
		objs: func(at time.Time) (client.Object, []client.Object) {
			db, inst := terminatingAuraDatabase(at)
			return db, []client.Object{inst}
		},
		newReconciler: refusingReconciler("AuraDatabase"),
	})
}

// withAuraCredentials points one of the auraDeletionCases kinds at the
// credentials Secret newAuraFakeClient seeds.
func withAuraCredentials(o client.Object) {
	switch v := o.(type) {
	case *neo4jv1beta1.AuraInstance:
		v.Spec.CredentialsSecretRef = credRef()
	case *neo4jv1beta1.AuraCustomerManagedKey:
		v.Spec.CredentialsSecretRef = credRef()
	case *neo4jv1beta1.AuraIPFilter:
		v.Spec.CredentialsSecretRef = credRef()
	case *neo4jv1beta1.AuraInvite:
		v.Spec.CredentialsSecretRef = credRef()
	case *neo4jv1beta1.AuraOrganizationMember:
		v.Spec.CredentialsSecretRef = credRef()
	case *neo4jv1beta1.AuraProjectMember:
		v.Spec.CredentialsSecretRef = credRef()
	}
}

// terminatingAuraDatabase is a created AuraDatabase on a multi-database
// AuraInstance, terminating since at under deletionPolicy Delete.
func terminatingAuraDatabase(at time.Time) (*neo4jv1beta1.AuraDatabase, *neo4jv1beta1.AuraInstance) {
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "multi", Namespace: testNS,
			Annotations: map[string]string{AuraExternalIDAnnotation: "inst-1"}},
		Spec: neo4jv1beta1.AuraInstanceSpec{CredentialsSecretRef: credRef(), ProjectID: "proj-1"},
	}
	db := &neo4jv1beta1.AuraDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-db", Namespace: testNS,
			Annotations: map[string]string{AuraExternalDatabaseAnnotation: "db-1"}},
		Spec: neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "multi", Name: "extra", OrganizationID: "org-1", DeletionPolicy: "Delete"},
	}
	terminatingSince(db, AuraDatabaseFinalizer, at)
	return db, inst
}

func refusingReconciler(name string) func(c client.Client, rec record.EventRecorder, err error) (reconcile.Reconciler, func() bool) {
	return func(c client.Client, rec record.EventRecorder, err error) (reconcile.Reconciler, func() bool) {
		switch name {
		case "AuraInstance":
			api := &fakeAuraAPI{deleteErr: err}
			return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: factoryFor(api)},
				func() bool { return api.deleteCalled }
		case "AuraCustomerManagedKey":
			api := &fakeCMKAPI{deleteFn: func(context.Context, string) error { return err }}
			return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: cmkFactoryFor(api)},
				func() bool { return api.deleteCalled }
		case "AuraIPFilter":
			api := &fakeIPFilterAPI{deleteErr: err}
			return &AuraIPFilterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: ipFilterFactoryFor(api)},
				func() bool { return api.deleteCalled }
		case "AuraInvite":
			api := &fakeMemberAPI{deleteErr: err}
			return &AuraInviteReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(api)},
				func() bool { return api.deleteInvite }
		case "AuraOrganizationMember":
			api := &fakeMemberAPI{deleteErr: err}
			return &AuraOrganizationMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(api)},
				func() bool { return api.deleteOrgMember }
		case "AuraProjectMember":
			api := &fakeMemberAPI{deleteErr: err}
			return &AuraProjectMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(api)},
				func() bool { return api.deleteProjMember }
		case "AuraDatabase":
			api := &fakeDatabaseAPI{deleteErr: err}
			return &AuraDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: dbFactory(api)},
				func() bool { return api.deleteCalls > 0 }
		}
		panic("no reconciler for " + name)
	}
}

func TestAuraDeletion_ARefusedDeleteWaitsThenOrphans(t *testing.T) {
	scheme := auraTestScheme(t)
	for _, tc := range auraRefusalCases() {
		t.Run(tc.name+" within the grace period", func(t *testing.T) {
			obj, others := tc.objs(time.Now())
			c := newAuraFakeClient(t, scheme, append(others, obj)...)
			rec := record.NewFakeRecorder(20)
			r, deleted := tc.newReconciler(c, rec, auraRefusedDelete())
			key := client.ObjectKeyFromObject(obj)

			for range 3 {
				res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
				require.NoError(t, err, "a refused DELETE is waited on, not returned for error backoff")
				assert.Positive(t, res.RequeueAfter, "it is asked again")
			}

			assert.True(t, deleted(), "Aura is asked")
			assert.False(t, finalizerReleased(t, c, obj, tc.finalizer), "the finalizer is kept while Aura refuses")
			events := drainEvents(rec)
			waits := eventsWithReason(events, tc.failed)
			require.Len(t, waits, 1, "the wait is announced once: %q", events)
			assert.True(t, strings.HasPrefix(waits[0], corev1.EventTypeWarning+" "), "%q", waits[0])
			assert.Contains(t, waits[0], "Aura refused to delete")
			assert.Empty(t, eventsWithReason(events, tc.orphaned), "%q", events)

			live, ok := obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(context.Background(), key, live))
			conds, phase := auraStatusOf(t, live)
			ready := findCondition(conds, "Ready")
			require.NotNil(t, ready, "the wait is on status")
			assert.Equal(t, "DeletionRefused", ready.Reason)
			assert.Contains(t, ready.Message, "the credential may not delete this")
			assert.Contains(t, ready.Message, "is left in Aura")
			assert.NotContains(t, ready.Message, "req-refused-1", "a status message carries no request ID (rule 184)")
			assert.Equal(t, neo4jv1beta1.PhaseError, phase)
		})

		t.Run(tc.name+" after the grace period", func(t *testing.T) {
			obj, others := tc.objs(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
			c := newAuraFakeClient(t, scheme, append(others, obj)...)
			rec := record.NewFakeRecorder(20)
			r, deleted := tc.newReconciler(c, rec, auraRefusedDelete())

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
			require.NoError(t, err)

			assert.True(t, deleted(), "Aura is asked once more")
			assert.True(t, finalizerReleased(t, c, obj, tc.finalizer), "the deletion must not wedge")
			events := drainEvents(rec)
			orphaned := eventsWithReason(events, tc.orphaned)
			require.Len(t, orphaned, 1, "%q", events)
			assert.True(t, strings.HasPrefix(orphaned[0], corev1.EventTypeWarning+" "),
				"leaving a resource the user asked to delete is a Warning: %q", orphaned[0])
			assert.Contains(t, orphaned[0], "although deletionPolicy is Delete")
			assert.Contains(t, orphaned[0], "Aura console")
			assert.Empty(t, eventsWithReason(events, tc.deleted), "nothing was deleted: %q", events)
		})
	}
}

// A conflict and a transient failure are not refusals: they are asked again on
// every pass, with no deadline, as before — the operation in flight ends, the
// outage passes.
func TestAuraDeletion_AConflictOrOutageIsNotBounded(t *testing.T) {
	scheme := auraTestScheme(t)
	for name, err := range map[string]error{
		"conflict":  &aura.APIError{StatusCode: http.StatusConflict, Message: "an operation is in progress"},
		"transient": &aura.APIError{StatusCode: http.StatusServiceUnavailable, Message: "upstream unavailable"},
	} {
		for _, tc := range auraRefusalCases() {
			t.Run(tc.name+" "+name, func(t *testing.T) {
				obj, others := tc.objs(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
				c := newAuraFakeClient(t, scheme, append(others, obj)...)
				rec := record.NewFakeRecorder(20)
				r, deleted := tc.newReconciler(c, rec, err)

				res, rerr := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
				require.NoError(t, rerr)
				assert.Positive(t, res.RequeueAfter)
				assert.True(t, deleted())
				assert.False(t, finalizerReleased(t, c, obj, tc.finalizer), "retried, never orphaned")
				assert.Empty(t, eventsWithReason(drainEvents(rec), tc.orphaned))
			})
		}
	}
}

// A customer-managed key still bound to an instance waited for ever: a
// namespace whose instance had been orphaned, still using the key, stayed
// Terminating. The wait is bounded like any refusal (rule 201).
func TestAuraCMK_AKeyInUseIsWaitedOnBounded(t *testing.T) {
	scheme := auraTestScheme(t)
	inUse := func(context.Context, string) error {
		return &aura.APIError{StatusCode: http.StatusBadRequest, Reason: aura.ReasonEncryptionKeyActive,
			Message: "The encryption key is active"}
	}
	cmkAt := func(at time.Time) *neo4jv1beta1.AuraCustomerManagedKey {
		cmk := newAuraCMK("cmk-in-use")
		cmk.Spec.DeletionPolicy = "Delete"
		cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
		terminatingSince(cmk, AuraCMKFinalizer, at)
		return cmk
	}

	t.Run("within the grace period", func(t *testing.T) {
		cmk := cmkAt(time.Now())
		c := newAuraFakeClient(t, scheme, cmk)
		rec := record.NewFakeRecorder(20)
		r := &AuraCustomerManagedKeyReconciler{Client: c, Scheme: scheme, Recorder: rec,
			ClientFactory: cmkFactoryFor(&fakeCMKAPI{deleteFn: inUse})}
		for range 3 {
			res, err := r.Reconcile(context.Background(), reqFor(cmk))
			require.NoError(t, err)
			assert.Positive(t, res.RequeueAfter)
		}
		assert.False(t, finalizerReleased(t, c, cmk, AuraCMKFinalizer))
		events := drainEvents(rec)
		assert.Len(t, eventsWithReason(events, EventReasonAuraCMKDeleteBlocked), 1, "%q", events)
		assert.Empty(t, eventsWithReason(events, EventReasonAuraCMKOrphaned), "%q", events)

		got := &neo4jv1beta1.AuraCustomerManagedKey{}
		require.NoError(t, c.Get(context.Background(), reqFor(cmk).NamespacedName, got))
		synced := findCondition(got.Status.Conditions, "Synced")
		require.NotNil(t, synced)
		assert.Equal(t, "KeyInUse", synced.Reason)
		assert.Contains(t, synced.Message, "Asked again until "+auraDeletionDeadline(cmk).UTC().Format(time.RFC3339))
	})

	t.Run("after the grace period", func(t *testing.T) {
		cmk := cmkAt(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
		c := newAuraFakeClient(t, scheme, cmk)
		rec := record.NewFakeRecorder(20)
		r := &AuraCustomerManagedKeyReconciler{Client: c, Scheme: scheme, Recorder: rec,
			ClientFactory: cmkFactoryFor(&fakeCMKAPI{deleteFn: inUse})}
		_, err := r.Reconcile(context.Background(), reqFor(cmk))
		require.NoError(t, err)

		assert.True(t, finalizerReleased(t, c, cmk, AuraCMKFinalizer), "a key in use no longer holds the namespace")
		orphaned := eventsWithReason(drainEvents(rec), EventReasonAuraCMKOrphaned)
		require.Len(t, orphaned, 1)
		assert.True(t, strings.HasPrefix(orphaned[0], corev1.EventTypeWarning+" "), "%q", orphaned[0])
		assert.Contains(t, orphaned[0], "still bound to an instance")
	})
}

// An AuraDatabase whose instance or credentials could not be resolved released
// its finalizer at once with a Normal "Orphaning database" event: a database,
// with its data, left in Aura and announced as routine (knowledge rule 202).
func TestAuraDatabaseDeletion_AnUnresolvableTargetIsAWarning(t *testing.T) {
	scheme := auraTestScheme(t)
	newReconciler := func(c client.Client, rec record.EventRecorder) (*AuraDatabaseReconciler, *fakeDatabaseAPI) {
		api := &fakeDatabaseAPI{}
		return &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: dbFactory(api)}, api
	}

	// The AuraInstance CR is gone: the credentials came through it, so nothing
	// will ever reach Aura for the database again. Released at once, but as a
	// Warning that says the database may still be there.
	t.Run("the AuraInstance is gone", func(t *testing.T) {
		db, _ := terminatingAuraDatabase(time.Now())
		c := newAuraFakeClient(t, scheme, db)
		rec := record.NewFakeRecorder(20)
		r, api := newReconciler(c, rec)

		_, err := r.Reconcile(context.Background(), reqFor(db))
		require.NoError(t, err)

		assert.True(t, finalizerReleased(t, c, db, AuraDatabaseFinalizer), "nothing can come back to delete it")
		assert.Zero(t, api.deleteCalls)
		orphaned := eventsWithReason(drainEvents(rec), EventReasonAuraDatabaseOrphaned)
		require.Len(t, orphaned, 1)
		assert.True(t, strings.HasPrefix(orphaned[0], corev1.EventTypeWarning+" "), "%q", orphaned[0])
		assert.Contains(t, orphaned[0], `its AuraInstance "multi" no longer exists`)
		assert.Contains(t, orphaned[0], "Aura console")
	})

	// The credentials can come back (a Secret restored, a namespace teardown
	// ordering), so the deletion waits for them, bounded (rule 157).
	t.Run("the credentials are missing, within the grace period", func(t *testing.T) {
		db, inst := terminatingAuraDatabase(time.Now())
		inst.Spec.CredentialsSecretRef = missingCreds("gone")
		c := newAuraFakeClient(t, scheme, inst, db)
		rec := record.NewFakeRecorder(20)
		r, api := newReconciler(c, rec)

		for range 3 {
			res, err := r.Reconcile(context.Background(), reqFor(db))
			require.NoError(t, err)
			assert.Positive(t, res.RequeueAfter)
		}
		assert.False(t, finalizerReleased(t, c, db, AuraDatabaseFinalizer))
		assert.Zero(t, api.deleteCalls)
		events := drainEvents(rec)
		require.Len(t, eventsWithReason(events, EventReasonAuraDatabaseFailed), 1, "%q", events)
		assert.Empty(t, eventsWithReason(events, EventReasonAuraDatabaseOrphaned), "%q", events)
		got := &neo4jv1beta1.AuraDatabase{}
		require.NoError(t, c.Get(context.Background(), reqFor(db).NamespacedName, got))
		ready := findCondition(got.Status.Conditions, "Ready")
		require.NotNil(t, ready)
		assert.Equal(t, "CredentialsUnavailable", ready.Reason)
		assert.Contains(t, ready.Message, "is left in Aura")
	})

	t.Run("the credentials are missing, after the grace period", func(t *testing.T) {
		db, inst := terminatingAuraDatabase(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
		inst.Spec.CredentialsSecretRef = missingCreds("gone")
		c := newAuraFakeClient(t, scheme, inst, db)
		rec := record.NewFakeRecorder(20)
		r, api := newReconciler(c, rec)

		_, err := r.Reconcile(context.Background(), reqFor(db))
		require.NoError(t, err)
		assert.True(t, finalizerReleased(t, c, db, AuraDatabaseFinalizer))
		assert.Zero(t, api.deleteCalls)
		orphaned := eventsWithReason(drainEvents(rec), EventReasonAuraDatabaseOrphaned)
		require.Len(t, orphaned, 1)
		assert.True(t, strings.HasPrefix(orphaned[0], corev1.EventTypeWarning+" "), "%q", orphaned[0])
		assert.Contains(t, orphaned[0], "although deletionPolicy is Delete")
		assert.Contains(t, orphaned[0], `it stays on Aura instance "multi" with its data`)
	})

	// Orphan is what the user asked for: Normal, at once, whatever resolves.
	t.Run("deletionPolicy Orphan", func(t *testing.T) {
		db, _ := terminatingAuraDatabase(time.Now())
		db.Spec.DeletionPolicy = "Orphan"
		c := newAuraFakeClient(t, scheme, db)
		rec := record.NewFakeRecorder(20)
		r, api := newReconciler(c, rec)

		_, err := r.Reconcile(context.Background(), reqFor(db))
		require.NoError(t, err)
		assert.True(t, finalizerReleased(t, c, db, AuraDatabaseFinalizer))
		assert.Zero(t, api.deleteCalls)
		orphaned := eventsWithReason(drainEvents(rec), EventReasonAuraDatabaseOrphaned)
		require.Len(t, orphaned, 1)
		assert.True(t, strings.HasPrefix(orphaned[0], corev1.EventTypeNormal+" "), "%q", orphaned[0])
	})

	t.Run("deletionPolicy Delete with everything in place", func(t *testing.T) {
		db, inst := terminatingAuraDatabase(time.Now())
		c := newAuraFakeClient(t, scheme, inst, db)
		rec := record.NewFakeRecorder(20)
		r, api := newReconciler(c, rec)

		_, err := r.Reconcile(context.Background(), reqFor(db))
		require.NoError(t, err)
		assert.Equal(t, 1, api.deleteCalls)
		assert.True(t, finalizerReleased(t, c, db, AuraDatabaseFinalizer))
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraDatabaseDeleted), 1)
	})
}
