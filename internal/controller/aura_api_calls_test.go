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
	"net/http"
	"testing"

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

// ---------------------------------------------------------------------------
// A recorded refusal is not asked again.
//
// A terminal refusal returns without a requeue, but nothing stopped the next
// pass — a resync, an operator restart, the refusal's own status write — from
// running the same Aura calls again, and for a refusal Aura gave (a tier that
// cannot be multi-database, an instance that is not one) sending the same
// refused create again. Now status that records the refusal for the current
// generation stops the pass before Aura is called; a spec edit, or for the two
// refusals whose cause lies outside the spec a change there, looks again
// (knowledge rule 156).
// ---------------------------------------------------------------------------

func passes(t *testing.T, r reconcile.Reconciler, o client.Object, n int) {
	t.Helper()
	for range n {
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(o)})
		require.NoError(t, err)
	}
}

// editSpec applies edit to the stored object and bumps its generation, as the
// API server does for a spec change (the fake client does not).
func editSpec[T client.Object](t *testing.T, c client.Client, o T, edit func(T)) {
	t.Helper()
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(o), o))
	edit(o)
	o.SetGeneration(o.GetGeneration() + 1)
	require.NoError(t, c.Update(context.Background(), o))
}

func TestAuraInstance_ARecordedRefusalIsNotAskedAgain(t *testing.T) {
	scheme := auraTestScheme(t)
	multiDB := func(name string) *neo4jv1beta1.AuraInstance {
		return &neo4jv1beta1.AuraInstance{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Generation: 1},
			Spec: neo4jv1beta1.AuraInstanceSpec{
				CredentialsSecretRef: credRef(), ProjectID: "proj-1",
				CloudProvider: "gcp", Region: "europe-west1", Type: "business-critical",
				Version: "5", Memory: "2GB", MultiDatabase: ptrTo(true),
			},
		}
	}

	t.Run("a spec without an organization, until the spec names one", func(t *testing.T) {
		inst := multiDB("mdb-noorg")
		v1, v2 := &fakeAuraAPI{}, &fakeInstanceV2API{}
		c := newAuraFakeClient(t, scheme, inst)
		r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
			ClientFactory: factoryFor(v1), InstanceV2ClientFactory: instV2Factory(v2)}

		passes(t, r, inst, 3)
		assert.Equal(t, 1, v1.listCalls, "the refusal is recorded once, then not asked again")
		assert.False(t, v2.createCalled)

		editSpec(t, c, inst, func(o *neo4jv1beta1.AuraInstance) { o.Spec.OrganizationID = "org-1" })
		passes(t, r, inst, 1)
		assert.Equal(t, 2, v1.listCalls, "a spec edit looks again")
		assert.True(t, v2.createCalled)
	})

	t.Run("a provider config without an organization, until it has one", func(t *testing.T) {
		pc := &neo4jv1beta1.AuraProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "acct", Namespace: testNS},
			Spec:       neo4jv1beta1.AuraProviderConfigSpec{CredentialsSecretRef: *credRef(), DefaultProjectID: "proj-1"},
		}
		inst := multiDB("mdb-pcorg")
		inst.Spec.CredentialsSecretRef = nil
		inst.Spec.ProviderConfigRef = &corev1.LocalObjectReference{Name: "acct"}
		v1, v2 := &fakeAuraAPI{}, &fakeInstanceV2API{}
		c := newAuraFakeClient(t, scheme, inst, pc)
		r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
			ClientFactory: factoryFor(v1), InstanceV2ClientFactory: instV2Factory(v2)}

		passes(t, r, inst, 3)
		assert.Equal(t, 1, v1.listCalls)

		// The fix the message offers is outside the AuraInstance's spec, so its
		// generation does not move; the refusal is looked at again regardless.
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(pc), pc))
		pc.Spec.DefaultOrganizationID = "org-1"
		require.NoError(t, c.Update(context.Background(), pc))
		passes(t, r, inst, 1)
		assert.Equal(t, 2, v1.listCalls, "an organization from the provider config looks again")
		assert.True(t, v2.createCalled)
	})

	t.Run("Aura's own refusal of the tier", func(t *testing.T) {
		inst := multiDB("mdb-tier")
		inst.Spec.OrganizationID = "org-1"
		creates := 0
		v1 := &fakeAuraAPI{}
		v2 := &fakeInstanceV2API{createFn: func(aura.CreateInstanceV2Request) (*aura.CreateInstanceV2Response, error) {
			creates++
			return nil, &aura.APIError{StatusCode: http.StatusBadRequest, Reason: aura.ReasonMultiDatabaseTierNotSupported,
				Message: "Multi-database instances are not supported by the provided tier"}
		}}
		c := newAuraFakeClient(t, scheme, inst)
		r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
			ClientFactory: factoryFor(v1), InstanceV2ClientFactory: instV2Factory(v2)}

		passes(t, r, inst, 3)
		assert.Equal(t, 1, creates, "the create Aura refused is not sent again")
		assert.Equal(t, 1, v1.listCalls)
	})
}

func TestAuraDatabase_ARecordedRefusalIsNotAskedAgain(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("an instance recorded as not multi-database", func(t *testing.T) {
		db, inst := newDBAgainstInstance(t, ptrTo(false))
		api := &fakeDatabaseAPI{}
		c := newAuraFakeClient(t, scheme, inst, db)
		r := &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20), ClientFactory: dbFactory(api)}

		passes(t, r, db, 3)
		assert.Equal(t, 1, api.listCalls, "the refusal is recorded once, then not asked again")
		assert.False(t, api.createCalled)
	})

	t.Run("Aura's 409, until the instance is replaced", func(t *testing.T) {
		db, inst := newDBAgainstInstance(t, nil) // unknown, so the create is attempted
		api := &fakeDatabaseAPI{createErr: &aura.APIError{
			StatusCode: http.StatusConflict, Reason: aura.ReasonMultiDBOnly,
			Message: "Only multi database Instances can add databases",
		}}
		c := newAuraFakeClient(t, scheme, inst, db)
		r := &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20), ClientFactory: dbFactory(api)}

		passes(t, r, db, 3)
		assert.Equal(t, 1, api.createCalls, "the create Aura refused is not sent again")

		// A new instance under the same name has another ID: a different target.
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(inst), inst))
		inst.Annotations[AuraExternalIDAnnotation] = "inst-2"
		require.NoError(t, c.Update(context.Background(), inst))
		passes(t, r, db, 1)
		assert.Equal(t, 2, api.createCalls, "a replaced instance is asked again")
	})
}

func TestAuraInvite_ARecordedRefusalIsNotAskedAgain(t *testing.T) {
	scheme := auraTestScheme(t)
	inv := &neo4jv1beta1.AuraInvite{
		ObjectMeta: metav1.ObjectMeta{Name: "invite-nsonly", Namespace: testNS, Generation: 1},
		Spec: neo4jv1beta1.AuraInviteSpec{
			CredentialsSecretRef: credRef(), OrganizationID: "org-1", ProjectID: "proj-1",
			Email: "dave@example.com", Role: aura.NamespaceRoleViewer,
		},
	}
	api := &fakeMemberAPI{}
	c := newAuraFakeClient(t, scheme, inv)
	r := &AuraInviteReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20), ClientFactory: memberFactory(api)}

	passes(t, r, inv, 3)
	assert.Equal(t, 1, api.listInviteCalls, "the refusal is recorded once, then not asked again")
	assert.False(t, api.createInvite)
	got := &neo4jv1beta1.AuraInvite{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(inv), got))
	assert.Equal(t, "OrganizationRoleRequired", findCondition(got.Status.Conditions, "Ready").Reason)

	editSpec(t, c, inv, func(o *neo4jv1beta1.AuraInvite) { o.Spec.OrganizationRole = aura.OrgRoleMember })
	passes(t, r, inv, 1)
	assert.Equal(t, 2, api.listInviteCalls, "a spec edit looks again")
	assert.True(t, api.createInvite)
}

// ---------------------------------------------------------------------------
// A transient failure of a create or a backup is retried, not recorded.
//
// Every other Aura call retries a 429, a 5xx or a transport failure quietly;
// the creates and the on-demand backup recorded it as CreateFailed or
// BackupFailed instead — phase Error, a Warning, and, since an APIError's text
// carries Aura's request ID, a new Warning on every retry. They now retry it as
// it is. The mark is set where the Aura call failed, never derived from
// aura.IsTransient on the error the create path returns: that would also
// swallow the operator's own errors, which aura.IsTransient calls transient
// (knowledge rule 88).
// ---------------------------------------------------------------------------

type auraTransientCase struct {
	name   string
	failed string // the Warning reason a recorded failure is announced with
	// objs are seeded; the first is reconciled.
	objs          func() []client.Object
	newReconciler func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
}

var (
	errAura503 = &aura.APIError{StatusCode: http.StatusServiceUnavailable, Message: "upstream unavailable", RequestID: "req-1"}
	errAura429 = &aura.APIError{StatusCode: http.StatusTooManyRequests, Message: "rate limited"}
	errAura409 = &aura.APIError{StatusCode: http.StatusConflict, Reason: aura.ReasonOngoingDatabaseOperation, Message: "busy"}
	errNetwork = errors.New("dial tcp: connection refused")
)

func auraTransientCases(t *testing.T) []auraTransientCase {
	backupObjs := func() []client.Object {
		db := &neo4jv1beta1.AuraDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: "db-of-backup", Namespace: testNS,
				Annotations: map[string]string{AuraExternalDatabaseAnnotation: "db-1"}},
			Spec: neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "single", Name: "of-backup", OrganizationID: "org-1"},
		}
		_, inst := newDBAgainstInstance(t, nil)
		return []client.Object{&neo4jv1beta1.AuraDatabaseBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "bk-transient", Namespace: testNS},
			Spec:       neo4jv1beta1.AuraDatabaseBackupSpec{DatabaseRef: "db-of-backup"},
		}, db, inst}
	}
	return []auraTransientCase{
		{
			name: "AuraInstance v1 create 503", failed: EventReasonAuraInstanceFailed,
			objs: func() []client.Object { return []client.Object{newAuraInstance("inst-503")} },
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeAuraAPI{createInstanceFn: func(context.Context, aura.CreateInstanceRequest) (*aura.CreateInstanceResponse, error) {
					return nil, errAura503
				}}
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: factoryFor(api)}
			},
		},
		{
			name: "AuraInstance list 429", failed: EventReasonAuraInstanceFailed,
			objs: func() []client.Object { return []client.Object{newAuraInstance("inst-429")} },
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeAuraAPI{listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) {
					return nil, errAura429
				}}
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: factoryFor(api)}
			},
		},
		{
			name: "AuraInstance v2beta1 create unreachable", failed: EventReasonAuraInstanceFailed,
			objs: func() []client.Object {
				inst := newAuraInstance("inst-v2-net")
				inst.Spec.Type, inst.Spec.OrganizationID, inst.Spec.MultiDatabase = "business-critical", "org-1", ptrTo(true)
				return []client.Object{inst}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				v2 := &fakeInstanceV2API{createFn: func(aura.CreateInstanceV2Request) (*aura.CreateInstanceV2Response, error) {
					return nil, errNetwork
				}}
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: factoryFor(&fakeAuraAPI{}), InstanceV2ClientFactory: instV2Factory(v2)}
			},
		},
		{
			name: "AuraCustomerManagedKey create 503", failed: EventReasonAuraCMKFailed,
			objs: func() []client.Object { return []client.Object{newAuraCMK("cmk-503")} },
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeCMKAPI{createFn: func(context.Context, aura.CreateCMKRequest) (*aura.CustomerManagedKey, error) {
					return nil, errAura503
				}}
				return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: cmkFactoryFor(api)}
			},
		},
		{
			name: "AuraIPFilter create unreachable", failed: EventReasonAuraIPFilterFailed,
			objs: func() []client.Object { return []client.Object{newAuraIPFilter("ipf-net")} },
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeIPFilterAPI{createFn: func(context.Context, string, aura.CreateIPFilterRequest) (*aura.IPFilter, error) {
					return nil, errNetwork
				}}
				return &AuraIPFilterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: ipFilterFactoryFor(api)}
			},
		},
		{
			name: "AuraDatabase create 503", failed: EventReasonAuraDatabaseFailed,
			objs: func() []client.Object {
				db, inst := newDBAgainstInstance(t, nil)
				return []client.Object{db, inst}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{createErr: errAura503})}
			},
		},
		{
			name: "AuraInvite create 503", failed: EventReasonAuraInviteFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraInvite{
					ObjectMeta: metav1.ObjectMeta{Name: "invite-503", Namespace: testNS},
					Spec: neo4jv1beta1.AuraInviteSpec{CredentialsSecretRef: credRef(), OrganizationID: "org-1",
						Email: "erin@example.com", Role: aura.OrgRoleMember},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraInviteReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: memberFactory(&fakeMemberAPI{createInviteErr: errAura503})}
			},
		},
		{
			name: "AuraDatabaseBackup create 503", failed: EventReasonAuraDatabaseBackupFailed,
			objs: backupObjs,
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseBackupReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{createBackupErr: errAura503})}
			},
		},
		{
			name: "AuraDatabaseBackup create 409 while another operation runs", failed: EventReasonAuraDatabaseBackupFailed,
			objs: backupObjs,
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseBackupReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{createBackupErr: errAura409})}
			},
		},
	}
}

func TestAuraCreate_ATransientFailureIsRetriedNotRecorded(t *testing.T) {
	scheme := auraTestScheme(t)
	for _, tc := range auraTransientCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			objs := tc.objs()
			c := newAuraFakeClient(t, scheme, objs...)
			rec := record.NewFakeRecorder(20)
			r := tc.newReconciler(c, rec)
			key := client.ObjectKeyFromObject(objs[0])

			for range 2 {
				res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
				assert.Positive(t, res.RequeueAfter, "a transient failure is retried")
			}

			events := drainEvents(rec)
			assert.Empty(t, eventsWithReason(events, tc.failed), "a transient failure is not announced: %q", events)
			live, ok := objs[0].DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(context.Background(), key, live))
			conds, phase := auraConditionsAndPhase(t, live)
			assert.NotEqual(t, neo4jv1beta1.PhaseError, phase, "a transient failure is not an Error")
			if ready := findCondition(conds, "Ready"); ready != nil {
				assert.NotContains(t, []string{"CreateFailed", "BackupFailed"}, ready.Reason)
			}
		})
	}
}

// The mark is set at the Aura call. An error the operator raises on the create
// path — here a clone source not yet provisioned — is not an aura.APIError, so
// aura.IsTransient would call it transient; it must still reach status and its
// Warning (knowledge rule 88), as must a 4xx Aura gave.
func TestAuraCreate_AnErrorThatIsNotTransientStillReachesStatus(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("the operator's own error", func(t *testing.T) {
		src := newAuraInstance("clone-source") // never provisioned: no instance ID
		inst := newAuraInstance("clone")
		inst.Spec.Source = &neo4jv1beta1.AuraInstanceSource{InstanceRef: "clone-source"}
		c := newAuraFakeClient(t, scheme, inst, src)
		rec := record.NewFakeRecorder(20)
		r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(&fakeAuraAPI{})}

		passes(t, r, inst, 1)
		got := &neo4jv1beta1.AuraInstance{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(inst), got))
		ready := findCondition(got.Status.Conditions, "Ready")
		require.NotNil(t, ready)
		assert.Equal(t, "CreateFailed", ready.Reason)
		assert.Contains(t, ready.Message, "has no instanceId yet")
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraInstanceFailed), 1)
	})

	t.Run("a 4xx from Aura", func(t *testing.T) {
		cmk := newAuraCMK("cmk-400")
		api := &fakeCMKAPI{createFn: func(context.Context, aura.CreateCMKRequest) (*aura.CustomerManagedKey, error) {
			return nil, &aura.APIError{StatusCode: http.StatusBadRequest, Message: "key_id is not a valid ARN"}
		}}
		c := newAuraFakeClient(t, scheme, cmk)
		rec := record.NewFakeRecorder(20)
		r := &AuraCustomerManagedKeyReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: cmkFactoryFor(api)}

		passes(t, r, cmk, 1)
		got := &neo4jv1beta1.AuraCustomerManagedKey{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cmk), got))
		assert.Equal(t, "CreateFailed", findCondition(got.Status.Conditions, "Ready").Reason)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraCMKFailed), 1)
	})
}

func TestRetryableAuraCall(t *testing.T) {
	assert.NoError(t, retryableAuraCall(nil))
	for _, err := range []error{errAura503, errAura429, errNetwork, errors.Join(errors.New("listing"), errAura503)} {
		marked := retryableAuraCall(err)
		assert.True(t, isRetryableAuraCall(marked), "%v", err)
		assert.ErrorIs(t, marked, err, "the mark keeps the cause")
		assert.Equal(t, err.Error(), marked.Error(), "the mark adds no text")
	}
	refused := &aura.APIError{StatusCode: http.StatusBadRequest, Message: "bad"}
	assert.False(t, isRetryableAuraCall(retryableAuraCall(refused)), "a 4xx is not retried as it is")
	assert.False(t, isRetryableAuraCall(errNetwork), "only the call site marks: an unmarked error is not retryable")
	assert.False(t, isRetryableAuraCall(refusef("no")))
}

// auraConditionsAndPhase returns the conditions and phase of the kinds above.
func auraConditionsAndPhase(t *testing.T, o client.Object) ([]metav1.Condition, string) {
	t.Helper()
	switch v := o.(type) {
	case *neo4jv1beta1.AuraDatabase:
		return v.Status.Conditions, v.Status.Phase
	case *neo4jv1beta1.AuraDatabaseBackup:
		return v.Status.Conditions, v.Status.Phase
	case *neo4jv1beta1.AuraInvite:
		return v.Status.Conditions, v.Status.Phase
	}
	return auraStatusOf(t, o)
}
