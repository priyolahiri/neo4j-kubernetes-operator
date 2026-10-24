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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// Every Aura controller watches its own kind, so a status write is a watch
// event that starts another pass at once. The status writers stamped a fresh
// lastSyncedTime on every pass, so every pass started the next: an invite or
// member that was failing or waiting about twice every 30s, a Ready resource
// twice or more every 5 minutes, each pass spending the Aura API's rate limit.
// And each of those passes announced its Ready event again — AuraMemberReady,
// AuraInviteReady, AuraDatabaseReady, AuraIPFilterReady,
// AuraCustomerManagedKeyReady every 5 minutes, AuraCredentialsValidated on
// every 30s revalidation. A status is now written when it changes, or when its
// lastSyncedTime is auraSyncedTimeRefresh old, and the Ready event is
// announced when the write that records it changed it (knowledge rules 136
// and 156).

// countingStatusWrites builds the client with an interceptor that counts
// status updates.
func countingStatusWrites(b *fake.ClientBuilder) (client.Client, *int) {
	writes := new(int)
	c := b.WithInterceptorFuncs(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			*writes++
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}).Build()
	return c, writes
}

// lastSyncedOf returns a pointer to the status.lastSyncedTime of an Aura kind.
func lastSyncedOf(t *testing.T, o client.Object) **metav1.Time {
	t.Helper()
	switch v := o.(type) {
	case *neo4jv1beta1.AuraInstance:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraCustomerManagedKey:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraIPFilter:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraDatabase:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraDatabaseBackup:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraInvite:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraOrganizationMember:
		return &v.Status.LastSyncedTime
	case *neo4jv1beta1.AuraProjectMember:
		return &v.Status.LastSyncedTime
	}
	t.Fatalf("no lastSyncedTime on %T", o)
	return nil
}

type auraSteadyCase struct {
	name string
	// ready is the reason of the event announcing the state, "" when the kind
	// emits none.
	ready string
	// objs are seeded into the fake client; the first is the CR reconciled.
	objs          func() []client.Object
	newReconciler func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
}

func auraSteadyCases() []auraSteadyCase {
	running := func(name string) func(context.Context, string) (*aura.Instance, error) {
		// Matches newAuraInstance's spec, so nothing drifts.
		return func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{ID: id, Name: name, Status: aura.InstanceStatusRunning, Memory: "4GB",
				Type: "professional-db", Region: "europe-west1", CloudProvider: "gcp"}, nil
		}
	}
	dbTarget := func() *neo4jv1beta1.AuraInstance {
		return &neo4jv1beta1.AuraInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: testNS,
				Annotations: map[string]string{AuraExternalIDAnnotation: "inst-1"}},
			Spec: neo4jv1beta1.AuraInstanceSpec{CredentialsSecretRef: credRef(), ProjectID: "proj-1"},
		}
	}
	return []auraSteadyCase{
		{
			name: "AuraInstance running",
			objs: func() []client.Object {
				inst := newAuraInstance("inst-steady")
				inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
				return []client.Object{inst}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: factoryFor(&fakeAuraAPI{getInstanceFn: running("inst-steady")})}
			},
		},
		{
			name: "AuraCustomerManagedKey ready", ready: EventReasonAuraCMKReady,
			objs: func() []client.Object {
				cmk := newAuraCMK("cmk-steady")
				cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
				return []client.Object{cmk}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: cmkFactoryFor(&fakeCMKAPI{})}
			},
		},
		{
			name: "AuraIPFilter ready", ready: EventReasonAuraIPFilterReady,
			objs: func() []client.Object {
				f := newAuraIPFilter("ipf-steady")
				f.Annotations = map[string]string{AuraExternalIPFilterAnnotation: "ipf-1"}
				return []client.Object{f}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeIPFilterAPI{getFn: func(_ context.Context, _, id string) (*aura.IPFilter, error) {
					// Echo the spec, so nothing drifts.
					return &aura.IPFilter{ID: id, Name: "ipf-steady",
						AllowList: []aura.IPFilterAllowEntry{{Address: "203.0.113.0", PrefixLen: 24}}}, nil
				}}
				return &AuraIPFilterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: ipFilterFactoryFor(api)}
			},
		},
		{
			name: "AuraDatabase ready", ready: EventReasonAuraDatabaseReady,
			objs: func() []client.Object {
				db := &neo4jv1beta1.AuraDatabase{
					ObjectMeta: metav1.ObjectMeta{Name: "db-steady", Namespace: testNS,
						Annotations: map[string]string{AuraExternalDatabaseAnnotation: "db-1"}},
					Spec: neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "single", Name: "steady", OrganizationID: "org-1"},
				}
				return []client.Object{db, dbTarget()}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: dbFactory(&fakeDatabaseAPI{})}
			},
		},
		{
			name: "AuraDatabaseBackup in progress",
			objs: func() []client.Object {
				db := &neo4jv1beta1.AuraDatabase{
					ObjectMeta: metav1.ObjectMeta{Name: "db-of-backup", Namespace: testNS,
						Annotations: map[string]string{AuraExternalDatabaseAnnotation: "db-1"}},
					Spec: neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "single", Name: "of-backup", OrganizationID: "org-1"},
				}
				b := &neo4jv1beta1.AuraDatabaseBackup{
					ObjectMeta: metav1.ObjectMeta{Name: "bk-steady", Namespace: testNS,
						Annotations: map[string]string{AuraExternalBackupAnnotation: "bk-1"}},
					Spec: neo4jv1beta1.AuraDatabaseBackupSpec{DatabaseRef: "db-of-backup"},
				}
				return []client.Object{b, db, dbTarget()}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseBackupReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{backupStatus: aura.BackupStatusInProgress})}
			},
		},
		{
			name: "AuraInvite sent", ready: EventReasonAuraInviteReady,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraInvite{
					ObjectMeta: metav1.ObjectMeta{Name: "invite-steady", Namespace: testNS,
						Annotations: map[string]string{AuraExternalInviteAnnotation: "inv-1"}},
					Spec: neo4jv1beta1.AuraInviteSpec{CredentialsSecretRef: credRef(), OrganizationID: "org-1",
						Email: "carol@example.com", Role: aura.OrgRoleMember},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeMemberAPI{invites: []aura.Invite{{ID: "inv-1", Email: "carol@example.com", Status: aura.InviteStatusActive}}}
				return &AuraInviteReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(api)}
			},
		},
		{
			name: "AuraInvite failing",
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraInvite{
					ObjectMeta: metav1.ObjectMeta{Name: "invite-noorg", Namespace: testNS},
					Spec: neo4jv1beta1.AuraInviteSpec{CredentialsSecretRef: credRef(),
						Email: "carol@example.com", Role: aura.OrgRoleMember},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraInviteReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(&fakeMemberAPI{})}
			},
		},
		{
			name: "AuraOrganizationMember ready", ready: EventReasonAuraMemberReady,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraOrganizationMember{
					ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: testNS},
					Spec: neo4jv1beta1.AuraOrganizationMemberSpec{CredentialsSecretRef: credRef(), OrganizationID: "org-1",
						Email: "alice@example.com", Role: aura.OrgRoleMember},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeMemberAPI{orgMembers: []aura.Member{
					{UserID: "u-1", Email: "alice@example.com", OrganizationRoles: []string{aura.OrgRoleMember}},
				}}
				return &AuraOrganizationMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(api)}
			},
		},
		{
			name: "AuraOrganizationMember waiting for an invite",
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraOrganizationMember{
					ObjectMeta: metav1.ObjectMeta{Name: "ghost", Namespace: testNS},
					Spec: neo4jv1beta1.AuraOrganizationMemberSpec{CredentialsSecretRef: credRef(), OrganizationID: "org-1",
						Email: "ghost@example.com", Role: aura.OrgRoleMember},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraOrganizationMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(&fakeMemberAPI{})}
			},
		},
		{
			name: "AuraProjectMember ready", ready: EventReasonAuraMemberReady,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraProjectMember{
					ObjectMeta: metav1.ObjectMeta{Name: "alice-proj", Namespace: testNS},
					Spec: neo4jv1beta1.AuraProjectMemberSpec{CredentialsSecretRef: credRef(), OrganizationID: "org-1",
						ProjectID: "proj-1", Email: "alice@example.com", Role: aura.ProjectRoleViewer},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				api := &fakeMemberAPI{orgMembers: []aura.Member{
					{UserID: "u-1", Email: "alice@example.com", ProjectRoles: []string{aura.ProjectRoleViewer}},
				}}
				return &AuraProjectMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, ClientFactory: memberFactory(api)}
			},
		},
	}
}

func TestAuraReconcile_ASteadyStateWritesAndAnnouncesNothing(t *testing.T) {
	scheme := auraTestScheme(t)
	for _, tc := range auraSteadyCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			objs := tc.objs()
			c, writes := countingStatusWrites(auraFakeClientBuilder(t, scheme, objs...))
			rec := record.NewFakeRecorder(100)
			key := client.ObjectKeyFromObject(objs[0])
			pass := func(r reconcile.Reconciler) {
				t.Helper()
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
			}
			r := tc.newReconciler(c, rec)

			for range 3 {
				pass(r)
			}
			if tc.ready != "" {
				events := drainEvents(rec)
				assert.Len(t, eventsWithReason(events, tc.ready), 1,
					"announced when the state is reached, not on every pass: %q", events)
			}

			// A pass that observes the same state writes nothing: the write would be
			// a watch event that starts yet another pass.
			*writes = 0
			pass(r)
			assert.Zero(t, *writes, "a pass that changed nothing wrote status")

			// An operator restart reads the state back from status.
			pass(tc.newReconciler(c, rec))
			assert.Zero(t, *writes, "a restarted reconciler wrote an unchanged status")
			if tc.ready != "" {
				assert.Empty(t, eventsWithReason(drainEvents(rec), tc.ready), "a restart announced a steady state")
			}

			// lastSyncedTime is refreshed once it is auraSyncedTimeRefresh old,
			// and the refresh announces nothing.
			live, ok := objs[0].DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(ctx, key, live))
			stale := metav1.NewTime(time.Now().Add(-auraSyncedTimeRefresh - time.Minute).Truncate(time.Second))
			*lastSyncedOf(t, live) = &stale
			require.NoError(t, c.Status().Update(ctx, live))
			*writes = 0
			pass(r)
			assert.Equal(t, 1, *writes, "a stale lastSyncedTime is refreshed")
			require.NoError(t, c.Get(ctx, key, live))
			refreshed := *lastSyncedOf(t, live)
			require.NotNil(t, refreshed)
			assert.True(t, refreshed.After(stale.Time), "lastSyncedTime was not refreshed")
			if tc.ready != "" {
				assert.Empty(t, eventsWithReason(drainEvents(rec), tc.ready), "a refresh announced a steady state")
			}
		})
	}
}

// The provider config revalidates its credentials every 30s in the shipped
// configuration; it announced AuraCredentialsValidated on every one.
func TestAuraProviderConfig_ValidatedIsAnnouncedOnTheTransition(t *testing.T) {
	scheme := auraTestScheme(t)
	pc := &neo4jv1beta1.AuraProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: testNS},
		Spec:       neo4jv1beta1.AuraProviderConfigSpec{CredentialsSecretRef: *credRef(), DefaultProjectID: "proj-1"},
	}
	c := newAuraFakeClient(t, scheme, pc)
	rec := record.NewFakeRecorder(50)
	var listErr error
	api := &fakeAuraAPI{listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) { return nil, listErr }}
	newR := func() reconcile.Reconciler {
		return &AuraProviderConfigReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(api)}
	}
	r := newR()
	pass := func(r reconcile.Reconciler) {
		t.Helper()
		_, err := r.Reconcile(context.Background(), reqFor(pc))
		require.NoError(t, err)
	}

	for range 3 {
		pass(r)
	}
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraCredentialsValidated), 1,
		"announced when the credentials are first validated, not on every revalidation")

	pass(newR())
	assert.Empty(t, drainEvents(rec), "a restarted reconciler announced credentials already validated")

	// Invalid, then valid again: each transition is news.
	listErr = errors.New("401 unauthorized")
	pass(r)
	pass(r)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraCredentialsInvalid), 1)
	listErr = nil
	pass(r)
	pass(r)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraCredentialsValidated), 1,
		"valid again after a failure is announced again, once")
}

// fail() recorded the Ready condition but no phase on an AuraInstance or an
// AuraCustomerManagedKey, so their phase stayed empty (or the last status Aura
// reported) and `kubectl neo4j status` and the ArgoCD health check, which read
// the phase, showed a failing resource as healthy. Every other Aura fail()
// writes Error.
func TestAuraFail_RecordsPhaseError(t *testing.T) {
	scheme := auraTestScheme(t)
	cases := []struct {
		name          string
		obj           client.Object
		newReconciler func(c client.Client) reconcile.Reconciler
	}{
		{
			name: "AuraInstance",
			obj: func() client.Object {
				inst := newAuraInstance("inst-fail")
				inst.Spec.CredentialsSecretRef = missingCreds("gone")
				inst.Status.Phase = aura.InstanceStatusRunning
				return inst
			}(),
			newReconciler: func(c client.Client) reconcile.Reconciler {
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(10),
					ClientFactory: factoryFor(&fakeAuraAPI{})}
			},
		},
		{
			name: "AuraCustomerManagedKey",
			obj: func() client.Object {
				cmk := newAuraCMK("cmk-fail")
				cmk.Spec.CredentialsSecretRef = missingCreds("gone")
				cmk.Status.Phase = aura.CMKStatusReady
				return cmk
			}(),
			newReconciler: func(c client.Client) reconcile.Reconciler {
				return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(10),
					ClientFactory: cmkFactoryFor(&fakeCMKAPI{})}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newAuraFakeClient(t, scheme, tc.obj)
			_, err := tc.newReconciler(c).Reconcile(context.Background(), reqFor(tc.obj))
			require.NoError(t, err)
			live, ok := tc.obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(tc.obj), live))
			_, phase := auraStatusOf(t, live)
			assert.Equal(t, neo4jv1beta1.PhaseError, phase, "a failure must show in the phase")
		})
	}
}
