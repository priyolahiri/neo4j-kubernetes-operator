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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// An Aura reconciler retries a failure every 30s (and a member or invite whose
// status write bumps LastSyncedTime reconciles again at once). Its Warning used
// to be emitted on every one of those passes while the cause stayed: a missing
// credentials Secret, a person who is not yet an organization member, a key
// still in use, deletionProtection holding a delete. It is now emitted when the
// status write that records the failure changes the condition (or phase): once
// when it starts or its message changes, and not at all after an operator
// restart, which reads it back from status (knowledge rule 145).

type auraWarningCase struct {
	name   string
	reason string
	// objs are seeded into the fake client; the first is the CR reconciled.
	objs          func() []client.Object
	newReconciler func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
	// edit, when set, keeps the CR failing with a different message.
	edit func(client.Object)
}

// missingCreds points a CR at a credentials Secret that does not exist.
func missingCreds(name string) *neo4jv1beta1.AuraCredentialsSecretRef {
	return &neo4jv1beta1.AuraCredentialsSecretRef{Name: name}
}

// terminating stamps a deletion timestamp; finalizer keeps the object in the
// fake client.
func terminating(o client.Object, finalizer string) {
	controllerutil.AddFinalizer(o, finalizer)
	now := metav1.Now()
	o.SetDeletionTimestamp(&now)
}

func auraWarningCases() []auraWarningCase {
	return []auraWarningCase{
		{
			name:   "AuraProviderConfig credentials invalid",
			reason: EventReasonAuraCredentialsInvalid,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraProviderConfig{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: testNS},
					Spec: neo4jv1beta1.AuraProviderConfigSpec{
						CredentialsSecretRef: *missingCreds("gone-a"), DefaultProjectID: "proj-1",
					},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraProviderConfig) { o.Spec.CredentialsSecretRef.Name = "gone-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraProviderConfigReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: factoryFor(&fakeAuraAPI{})}
			},
		},
		{
			name:   "AuraProviderConfig API refuses the credentials",
			reason: EventReasonAuraCredentialsInvalid,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraProviderConfig{
					ObjectMeta: metav1.ObjectMeta{Name: "cfg-401", Namespace: testNS},
					Spec:       neo4jv1beta1.AuraProviderConfigSpec{CredentialsSecretRef: *credRef(), DefaultProjectID: "proj-1"},
				}}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraProviderConfigReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: factoryFor(&fakeAuraAPI{listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) {
						return nil, errors.New("401 unauthorized")
					}})}
			},
		},
		{
			name:   "AuraInstance credentials unavailable",
			reason: EventReasonAuraInstanceFailed,
			objs: func() []client.Object {
				inst := newAuraInstance("inst-nocreds")
				inst.Spec.CredentialsSecretRef = missingCreds("gone-a")
				return []client.Object{inst}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraInstance) { o.Spec.CredentialsSecretRef.Name = "gone-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: factoryFor(&fakeAuraAPI{})}
			},
		},
		{
			name:   "AuraInstance deletionProtection holds the delete",
			reason: EventReasonAuraInstanceFailed,
			objs: func() []client.Object {
				inst := newAuraInstance("inst-protected")
				inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
				inst.Spec.DeletionPolicy = "Delete"
				inst.Spec.DeletionProtection = true
				terminating(inst, AuraInstanceFinalizer)
				return []client.Object{inst}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraInstanceReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: factoryFor(&fakeAuraAPI{})}
			},
		},
		{
			name:   "AuraCustomerManagedKey credentials unavailable",
			reason: EventReasonAuraCMKFailed,
			objs: func() []client.Object {
				cmk := newAuraCMK("cmk-nocreds")
				cmk.Spec.CredentialsSecretRef = missingCreds("gone-a")
				return []client.Object{cmk}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraCustomerManagedKey) { o.Spec.CredentialsSecretRef.Name = "gone-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: cmkFactoryFor(&fakeCMKAPI{})}
			},
		},
		{
			name:   "AuraCustomerManagedKey still in use blocks the delete",
			reason: EventReasonAuraCMKDeleteBlocked,
			objs: func() []client.Object {
				cmk := newAuraCMK("cmk-active")
				cmk.Spec.DeletionPolicy = "Delete"
				cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
				terminating(cmk, AuraCMKFinalizer)
				return []client.Object{cmk}
			},
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraCustomerManagedKeyReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: cmkFactoryFor(&fakeCMKAPI{deleteFn: func(context.Context, string) error {
						return &aura.APIError{StatusCode: 400, Reason: aura.ReasonEncryptionKeyActive, Message: "The encryption key is active"}
					}})}
			},
		},
		{
			name:   "AuraIPFilter credentials unavailable",
			reason: EventReasonAuraIPFilterFailed,
			objs: func() []client.Object {
				f := newAuraIPFilter("ipf-nocreds")
				f.Spec.CredentialsSecretRef = missingCreds("gone-a")
				return []client.Object{f}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraIPFilter) { o.Spec.CredentialsSecretRef.Name = "gone-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraIPFilterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: ipFilterFactoryFor(&fakeIPFilterAPI{})}
			},
		},
		{
			name:   "AuraDatabase target unresolved",
			reason: EventReasonAuraDatabaseFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraDatabase{
					ObjectMeta: metav1.ObjectMeta{Name: "db-orphan", Namespace: testNS},
					Spec:       neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "no-such-a", Name: "analytics", OrganizationID: "org-1"},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraDatabase) { o.Spec.InstanceRef = "no-such-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{})}
			},
		},
		{
			name:   "AuraDatabaseBackup database ref unresolved",
			reason: EventReasonAuraDatabaseBackupFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraDatabaseBackup{
					ObjectMeta: metav1.ObjectMeta{Name: "bk-orphan", Namespace: testNS},
					Spec:       neo4jv1beta1.AuraDatabaseBackupSpec{DatabaseRef: "no-such-a"},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraDatabaseBackup) { o.Spec.DatabaseRef = "no-such-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseBackupReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{})}
			},
		},
		{
			name:   "AuraDatabaseRestore database ref unresolved",
			reason: EventReasonAuraDatabaseRestoreFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraDatabaseRestore{
					ObjectMeta: metav1.ObjectMeta{Name: "rs-orphan", Namespace: testNS},
					Spec:       neo4jv1beta1.AuraDatabaseRestoreSpec{DatabaseRef: "no-such-a", BackupID: "bk-1"},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraDatabaseRestore) { o.Spec.DatabaseRef = "no-such-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraDatabaseRestoreReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: dbFactory(&fakeDatabaseAPI{})}
			},
		},
		{
			name:   "AuraInvite organization missing",
			reason: EventReasonAuraInviteFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraInvite{
					ObjectMeta: metav1.ObjectMeta{Name: "invite-noorg", Namespace: testNS},
					Spec: neo4jv1beta1.AuraInviteSpec{
						CredentialsSecretRef: credRef(), Email: "carol@example.com", Role: aura.OrgRoleMember,
					},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraInvite) { o.Spec.CredentialsSecretRef = missingCreds("gone-b") }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraInviteReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: memberFactory(&fakeMemberAPI{})}
			},
		},
		{
			name:   "AuraOrganizationMember not a member",
			reason: EventReasonAuraMemberNotFound,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraOrganizationMember{
					ObjectMeta: metav1.ObjectMeta{Name: "ghost", Namespace: testNS},
					Spec: neo4jv1beta1.AuraOrganizationMemberSpec{
						CredentialsSecretRef: credRef(), OrganizationID: "org-1",
						Email: "ghost@example.com", Role: aura.OrgRoleMember,
					},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraOrganizationMember) { o.Spec.Email = "phantom@example.com" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraOrganizationMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: memberFactory(&fakeMemberAPI{})}
			},
		},
		{
			name:   "AuraOrganizationMember organization missing",
			reason: EventReasonAuraMemberFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraOrganizationMember{
					ObjectMeta: metav1.ObjectMeta{Name: "noorg", Namespace: testNS},
					Spec: neo4jv1beta1.AuraOrganizationMemberSpec{
						CredentialsSecretRef: credRef(), Email: "ghost@example.com", Role: aura.OrgRoleMember,
					},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraOrganizationMember) { o.Spec.CredentialsSecretRef = missingCreds("gone-b") }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraOrganizationMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: memberFactory(&fakeMemberAPI{})}
			},
		},
		{
			name:   "AuraProjectMember not an organization member",
			reason: EventReasonAuraMemberNotFound,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraProjectMember{
					ObjectMeta: metav1.ObjectMeta{Name: "ghost-proj", Namespace: testNS},
					Spec: neo4jv1beta1.AuraProjectMemberSpec{
						CredentialsSecretRef: credRef(), OrganizationID: "org-1", ProjectID: "proj-1",
						Email: "ghost@example.com", Role: aura.ProjectRoleViewer,
					},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraProjectMember) { o.Spec.Email = "phantom@example.com" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraProjectMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: memberFactory(&fakeMemberAPI{projectMembersFn: func() ([]aura.Member, error) { return nil, nil }})}
			},
		},
		{
			name:   "AuraProjectMember credentials unavailable",
			reason: EventReasonAuraMemberFailed,
			objs: func() []client.Object {
				return []client.Object{&neo4jv1beta1.AuraProjectMember{
					ObjectMeta: metav1.ObjectMeta{Name: "nocreds-proj", Namespace: testNS},
					Spec: neo4jv1beta1.AuraProjectMemberSpec{
						CredentialsSecretRef: missingCreds("gone-a"), OrganizationID: "org-1", ProjectID: "proj-1",
						Email: "ghost@example.com", Role: aura.ProjectRoleViewer,
					},
				}}
			},
			edit: editAs(func(o *neo4jv1beta1.AuraProjectMember) { o.Spec.CredentialsSecretRef.Name = "gone-b" }),
			newReconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &AuraProjectMemberReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					ClientFactory: memberFactory(&fakeMemberAPI{})}
			},
		},
	}
}

func TestAuraReconcile_FailureWarningIsAnnouncedOnce(t *testing.T) {
	scheme := auraTestScheme(t)
	for _, tc := range auraWarningCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			objs := tc.objs()
			c := newAuraFakeClient(t, scheme, objs...)
			rec := record.NewFakeRecorder(100)
			key := client.ObjectKeyFromObject(objs[0])
			reconcileWith := func(r reconcile.Reconciler) {
				t.Helper()
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
			}
			r := tc.newReconciler(c, rec)

			for range 3 {
				reconcileWith(r)
			}
			first := eventsWithReason(drainEvents(rec), tc.reason)
			require.Len(t, first, 1, "announced when the failure starts, not on every pass: %q", first)

			if tc.edit != nil {
				// Still failing, differently: the new message is news.
				live, ok := objs[0].DeepCopyObject().(client.Object)
				require.True(t, ok)
				require.NoError(t, c.Get(ctx, key, live))
				tc.edit(live)
				require.NoError(t, c.Update(ctx, live))
				reconcileWith(r)
				reconcileWith(r)
				again := eventsWithReason(drainEvents(rec), tc.reason)
				require.Len(t, again, 1, "a changed failure is announced once: %q", again)
				assert.NotEqual(t, first[0], again[0])
			}

			// An operator restart reads the failure back from status and stays quiet.
			reconcileWith(tc.newReconciler(c, rec))
			assert.Empty(t, eventsWithReason(drainEvents(rec), tc.reason))
		})
	}
}
