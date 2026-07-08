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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// ---------------------------------------------------------------------------
// Fakes for the v2beta1 database + member APIs.
// ---------------------------------------------------------------------------

type fakeDatabaseAPI struct {
	createCalled bool
	listFn       func() ([]aura.Database, error)
	getFn        func(id string) (*aura.Database, error)
}

func (f *fakeDatabaseAPI) CreateDatabase(_ context.Context, _, _, _ string, req aura.CreateDatabaseRequest) (*aura.Database, error) {
	f.createCalled = true
	return &aura.Database{ID: "db-new", Name: req.Name}, nil
}
func (f *fakeDatabaseAPI) GetDatabase(_ context.Context, _, _, _, id string) (*aura.Database, error) {
	if f.getFn != nil {
		return f.getFn(id)
	}
	return &aura.Database{ID: id, Name: "analytics"}, nil
}
func (f *fakeDatabaseAPI) ListDatabases(_ context.Context, _, _, _ string) ([]aura.Database, error) {
	if f.listFn != nil {
		return f.listFn()
	}
	return nil, nil
}
func (f *fakeDatabaseAPI) DeleteDatabase(_ context.Context, _, _, _, _ string) error { return nil }
func (f *fakeDatabaseAPI) CreateDatabaseBackup(_ context.Context, _, _, _, dbID string) (*aura.DatabaseBackup, error) {
	return &aura.DatabaseBackup{ID: "bk-new", DatabaseID: dbID, Status: "COMPLETED"}, nil
}
func (f *fakeDatabaseAPI) GetDatabaseBackup(_ context.Context, _, _, _, _, id string) (*aura.DatabaseBackup, error) {
	return &aura.DatabaseBackup{ID: id, Status: "COMPLETED"}, nil
}
func (f *fakeDatabaseAPI) RestoreDatabase(_ context.Context, _, _, _, _ string, _ aura.RestoreDatabaseRequest) error {
	return nil
}

func dbFactory(f *fakeDatabaseAPI) auraDatabaseClientFactory {
	return func(auraCredentials) auraDatabaseAPI { return f }
}

type fakeMemberAPI struct {
	orgMembers    []aura.Member
	updateCalled  bool
	createInvite  bool
	deleteInvite  bool
	invites       []aura.Invite
	lastInviteReq aura.CreateInviteRequest
}

func (f *fakeMemberAPI) ListOrgMembers(_ context.Context, _ string) ([]aura.Member, error) {
	return f.orgMembers, nil
}
func (f *fakeMemberAPI) UpdateOrgMemberRole(_ context.Context, _, userID, role string) (*aura.Member, error) {
	f.updateCalled = true
	return &aura.Member{ID: userID, Role: role}, nil
}
func (f *fakeMemberAPI) DeleteOrgMember(_ context.Context, _, _ string) error { return nil }
func (f *fakeMemberAPI) ListProjectMembers(_ context.Context, _, _ string) ([]aura.Member, error) {
	return f.orgMembers, nil
}
func (f *fakeMemberAPI) UpdateProjectMemberRole(_ context.Context, _, _, userID, role string) (*aura.Member, error) {
	f.updateCalled = true
	return &aura.Member{ID: userID, Role: role}, nil
}
func (f *fakeMemberAPI) DeleteProjectMember(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeMemberAPI) CreateInvite(_ context.Context, _ string, req aura.CreateInviteRequest) (*aura.Invite, error) {
	f.createInvite = true
	f.lastInviteReq = req
	return &aura.Invite{ID: "inv-new", Email: req.Email, Role: req.Role, ProjectID: req.ProjectID}, nil
}
func (f *fakeMemberAPI) GetInvite(_ context.Context, _, id string) (*aura.Invite, error) {
	return &aura.Invite{ID: id, Email: "carol@example.com", Role: aura.OrgRoleMember}, nil
}
func (f *fakeMemberAPI) ListInvites(_ context.Context, _ string) ([]aura.Invite, error) {
	return f.invites, nil
}
func (f *fakeMemberAPI) DeleteInvite(_ context.Context, _, _ string) error {
	f.deleteInvite = true
	return nil
}

func memberFactory(f *fakeMemberAPI) auraMemberClientFactory {
	return func(auraCredentials) auraMemberAPI { return f }
}

// ---------------------------------------------------------------------------
// AuraDatabase
// ---------------------------------------------------------------------------

func TestAuraDatabase_CreateThenReady(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "analytics", Namespace: testNS,
			Annotations: map[string]string{AuraExternalIDAnnotation: "inst-1"},
		},
		Spec: neo4jv1beta1.AuraInstanceSpec{CredentialsSecretRef: credRef(), ProjectID: "proj-1"},
	}
	db := &neo4jv1beta1.AuraDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-db", Namespace: testNS},
		Spec: neo4jv1beta1.AuraDatabaseSpec{
			InstanceRef: "analytics", Name: "analytics", OrganizationID: "org-1",
		},
	}
	api := &fakeDatabaseAPI{
		listFn: func() ([]aura.Database, error) { return nil, nil },
		getFn:  func(id string) (*aura.Database, error) { return &aura.Database{ID: id, Name: "analytics"}, nil },
	}
	c := newAuraFakeClient(t, scheme, inst, db)
	r := &AuraDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: dbFactory(api)}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(db)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if !api.createCalled {
		t.Fatal("expected CreateDatabase to be called")
	}
	got := &neo4jv1beta1.AuraDatabase{}
	if err := c.Get(ctx, reqFor(db).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalDatabaseAnnotation]; id != "db-new" {
		t.Errorf("external-database-id = %q, want db-new", id)
	}
	if s := conditionStatus(got.Status.Conditions, "Ready"); s != metav1.ConditionTrue {
		t.Errorf("Ready = %q, want True", s)
	}
}

// ---------------------------------------------------------------------------
// AuraOrganizationMember
// ---------------------------------------------------------------------------

func TestAuraOrganizationMember_UpdatesRole(t *testing.T) {
	scheme := auraTestScheme(t)
	m := &neo4jv1beta1.AuraOrganizationMember{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: testNS},
		Spec: neo4jv1beta1.AuraOrganizationMemberSpec{
			CredentialsSecretRef: credRef(), OrganizationID: "org-1",
			Email: "alice@example.com", Role: aura.OrgRoleAdmin,
		},
	}
	api := &fakeMemberAPI{orgMembers: []aura.Member{{ID: "u-1", Email: "alice@example.com", Role: aura.OrgRoleMember}}}
	c := newAuraFakeClient(t, scheme, m)
	r := &AuraOrganizationMemberReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: memberFactory(api)}
	if _, err := r.Reconcile(context.Background(), reqFor(m)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !api.updateCalled {
		t.Error("expected UpdateOrgMemberRole when the observed role differs from spec")
	}
	got := &neo4jv1beta1.AuraOrganizationMember{}
	_ = c.Get(context.Background(), reqFor(m).NamespacedName, got)
	if got.Status.UserID != "u-1" {
		t.Errorf("status.userId = %q, want u-1", got.Status.UserID)
	}
}

func TestAuraOrganizationMember_NotAMember(t *testing.T) {
	scheme := auraTestScheme(t)
	m := &neo4jv1beta1.AuraOrganizationMember{
		ObjectMeta: metav1.ObjectMeta{Name: "ghost", Namespace: testNS},
		Spec: neo4jv1beta1.AuraOrganizationMemberSpec{
			CredentialsSecretRef: credRef(), OrganizationID: "org-1",
			Email: "ghost@example.com", Role: aura.OrgRoleMember,
		},
	}
	api := &fakeMemberAPI{orgMembers: nil}
	c := newAuraFakeClient(t, scheme, m)
	r := &AuraOrganizationMemberReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: memberFactory(api)}
	if _, err := r.Reconcile(context.Background(), reqFor(m)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if api.updateCalled {
		t.Error("must NOT update role when the email is not a member")
	}
	got := &neo4jv1beta1.AuraOrganizationMember{}
	_ = c.Get(context.Background(), reqFor(m).NamespacedName, got)
	if got.Status.Phase != "NotAMember" {
		t.Errorf("phase = %q, want NotAMember", got.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// AuraInvite
// ---------------------------------------------------------------------------

func TestAuraInvite_CreateThenReady(t *testing.T) {
	scheme := auraTestScheme(t)
	inv := &neo4jv1beta1.AuraInvite{
		ObjectMeta: metav1.ObjectMeta{Name: "invite-carol", Namespace: testNS},
		Spec: neo4jv1beta1.AuraInviteSpec{
			CredentialsSecretRef: credRef(), OrganizationID: "org-1",
			Email: "carol@example.com", Role: aura.OrgRoleMember,
		},
	}
	api := &fakeMemberAPI{}
	c := newAuraFakeClient(t, scheme, inv)
	r := &AuraInviteReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: memberFactory(api)}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(inv)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if !api.createInvite {
		t.Fatal("expected CreateInvite to be called")
	}
	if api.lastInviteReq.Email != "carol@example.com" || api.lastInviteReq.Role != aura.OrgRoleMember {
		t.Errorf("invite req = %+v", api.lastInviteReq)
	}
	got := &neo4jv1beta1.AuraInvite{}
	if err := c.Get(ctx, reqFor(inv).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalInviteAnnotation]; id != "inv-new" {
		t.Errorf("external-invite-id = %q, want inv-new", id)
	}
}
