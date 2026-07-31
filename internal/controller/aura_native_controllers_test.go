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
	createCalled   bool
	createErr      error
	listFn         func() ([]aura.Database, error)
	getFn          func(id string) (*aura.Database, error)
	listBackupsFn  func() ([]aura.DatabaseBackup, error)
	backupStatus   string
	restoreCalls   int
	lastRestoreReq aura.RestoreDatabaseRequest
}

// NOTE: these fakes mirror the PUBLISHED v2beta1 shapes. A Database carries only
// an ID (DatabaseSummary has no name/status), a freshly-created backup carries
// only an ID, and backup statuses use the title-case enum. Do not "helpfully"
// populate fields the real API never returns — that is what previously let a
// broken contract pass CI.
func (f *fakeDatabaseAPI) CreateDatabase(_ context.Context, _, _, _ string, _ aura.CreateDatabaseRequest) (*aura.Database, error) {
	f.createCalled = true
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &aura.Database{ID: "db-new"}, nil
}
func (f *fakeDatabaseAPI) GetDatabase(_ context.Context, _, _, _, id string) (*aura.Database, error) {
	if f.getFn != nil {
		return f.getFn(id)
	}
	return &aura.Database{ID: id}, nil
}
func (f *fakeDatabaseAPI) ListDatabases(_ context.Context, _, _, _ string) ([]aura.Database, error) {
	if f.listFn != nil {
		return f.listFn()
	}
	return nil, nil
}
func (f *fakeDatabaseAPI) DeleteDatabase(_ context.Context, _, _, _, _ string) error { return nil }

// The create response is `{id}` only — no status.
func (f *fakeDatabaseAPI) CreateDatabaseBackup(_ context.Context, _, _, _, _ string) (*aura.DatabaseBackup, error) {
	return &aura.DatabaseBackup{ID: "bk-new"}, nil
}
func (f *fakeDatabaseAPI) ListDatabaseBackups(_ context.Context, _, _, _, _ string) ([]aura.DatabaseBackup, error) {
	if f.listBackupsFn != nil {
		return f.listBackupsFn()
	}
	return nil, nil
}
func (f *fakeDatabaseAPI) GetDatabaseBackup(_ context.Context, _, _, _, _, id string) (*aura.DatabaseBackup, error) {
	if f.backupStatus != "" {
		return &aura.DatabaseBackup{ID: id, Status: f.backupStatus, Timestamp: "2026-07-01T00:00:00Z"}, nil
	}
	return &aura.DatabaseBackup{ID: id, Status: aura.BackupStatusCompleted, Timestamp: "2026-07-01T00:00:00Z", Exportable: true}, nil
}
func (f *fakeDatabaseAPI) RestoreDatabase(_ context.Context, _, _, _, _ string, req aura.RestoreDatabaseRequest) error {
	f.restoreCalls++
	f.lastRestoreReq = req
	return nil
}

func dbFactory(f *fakeDatabaseAPI) auraDatabaseClientFactory {
	return func(auraCredentials) auraDatabaseAPI { return f }
}

type fakeMemberAPI struct {
	orgMembers       []aura.Member
	projectMembersFn func() ([]aura.Member, error)
	updateCalled     bool
	addCalled        bool
	addedUserID      string
	addedRole        string
	createInvite     bool
	deleteInvite     bool
	invites          []aura.Invite
	lastInviteReq    aura.CreateInviteRequest
}

func (f *fakeMemberAPI) ListOrgMembers(_ context.Context, _ string) ([]aura.Member, error) {
	return f.orgMembers, nil
}
func (f *fakeMemberAPI) UpdateOrgMemberRole(_ context.Context, _, userID, role string) (*aura.Member, error) {
	f.updateCalled = true
	return &aura.Member{UserID: userID, OrganizationRoles: []string{role}}, nil
}
func (f *fakeMemberAPI) DeleteOrgMember(_ context.Context, _, _ string) error { return nil }
func (f *fakeMemberAPI) ListProjectMembers(_ context.Context, _, _ string) ([]aura.Member, error) {
	if f.projectMembersFn != nil {
		return f.projectMembersFn()
	}
	return f.orgMembers, nil
}
func (f *fakeMemberAPI) AddProjectMember(_ context.Context, _, _, userID, role string) error {
	f.addCalled = true
	f.addedUserID = userID
	f.addedRole = role
	return nil
}
func (f *fakeMemberAPI) UpdateProjectMemberRole(_ context.Context, _, _, userID, role string) (*aura.Member, error) {
	f.updateCalled = true
	return &aura.Member{UserID: userID, ProjectRoles: []string{role}}, nil
}
func (f *fakeMemberAPI) DeleteProjectMember(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeMemberAPI) CreateInvite(_ context.Context, _ string, req aura.CreateInviteRequest) (*aura.Invite, error) {
	f.createInvite = true
	f.lastInviteReq = req
	return &aura.Invite{
		ID:                "inv-new",
		Email:             req.Email,
		OrganizationRoles: req.Roles,
		ProjectInvites:    req.ProjectInvites,
		Status:            aura.InviteStatusActive,
	}, nil
}

// FindInvite mirrors the real client: reads from the LIST result and returns
// (nil, nil) when absent. There is no GetInvite — v2beta1 has no
// GET /invites/{id}, only DELETE.
func (f *fakeMemberAPI) FindInvite(_ context.Context, _, id string) (*aura.Invite, error) {
	for i := range f.invites {
		if f.invites[i].ID == id {
			return &f.invites[i], nil
		}
	}
	return nil, nil
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
		getFn:  func(id string) (*aura.Database, error) { return &aura.Database{ID: id}, nil },
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
	api := &fakeMemberAPI{orgMembers: []aura.Member{
		{UserID: "u-1", Email: "alice@example.com", OrganizationRoles: []string{aura.OrgRoleMember}},
	}}
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
// AuraProjectMember
// ---------------------------------------------------------------------------

// An existing ORGANIZATION member who is not yet in the project is added
// directly via POST project users (which takes their user UUID, not an email).
// Previously this dead-ended at NotAMember and told the user to file an invite,
// because the add-to-project operation was not implemented at all.
func TestAuraProjectMember_AddsExistingOrgMemberToProject(t *testing.T) {
	scheme := auraTestScheme(t)
	m := &neo4jv1beta1.AuraProjectMember{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-proj", Namespace: testNS},
		Spec: neo4jv1beta1.AuraProjectMemberSpec{
			CredentialsSecretRef: credRef(), OrganizationID: "org-1", ProjectID: "proj-1",
			Email: "alice@example.com", Role: aura.ProjectRoleAdmin,
		},
	}
	api := &fakeMemberAPI{
		// Known at org level...
		orgMembers: []aura.Member{
			{UserID: "u-1", Email: "alice@example.com", OrganizationRoles: []string{aura.OrgRoleMember}},
		},
		// ...but not yet a member of the project.
		projectMembersFn: func() ([]aura.Member, error) { return nil, nil },
	}
	c := newAuraFakeClient(t, scheme, m)
	r := &AuraProjectMemberReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: memberFactory(api)}
	if _, err := r.Reconcile(context.Background(), reqFor(m)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !api.addCalled {
		t.Fatal("expected AddProjectMember to be called for an existing org member")
	}
	if api.addedUserID != "u-1" {
		t.Errorf("added user id = %q, want the Aura user UUID u-1 (never the email)", api.addedUserID)
	}
	if api.addedRole != aura.ProjectRoleAdmin {
		t.Errorf("added role = %q, want %q", api.addedRole, aura.ProjectRoleAdmin)
	}
	got := &neo4jv1beta1.AuraProjectMember{}
	_ = c.Get(context.Background(), reqFor(m).NamespacedName, got)
	if got.Status.Phase != "Ready" {
		t.Errorf("phase = %q, want Ready", got.Status.Phase)
	}
	if got.Status.UserID != "u-1" {
		t.Errorf("status.userId = %q, want u-1", got.Status.UserID)
	}
}

// Someone unknown at org level cannot be added directly — they need an invite.
func TestAuraProjectMember_UnknownEmailStillNeedsInvite(t *testing.T) {
	scheme := auraTestScheme(t)
	m := &neo4jv1beta1.AuraProjectMember{
		ObjectMeta: metav1.ObjectMeta{Name: "ghost-proj", Namespace: testNS},
		Spec: neo4jv1beta1.AuraProjectMemberSpec{
			CredentialsSecretRef: credRef(), OrganizationID: "org-1", ProjectID: "proj-1",
			Email: "ghost@example.com", Role: aura.ProjectRoleViewer,
		},
	}
	api := &fakeMemberAPI{orgMembers: nil, projectMembersFn: func() ([]aura.Member, error) { return nil, nil }}
	c := newAuraFakeClient(t, scheme, m)
	r := &AuraProjectMemberReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: memberFactory(api)}
	if _, err := r.Reconcile(context.Background(), reqFor(m)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if api.addCalled {
		t.Error("must NOT add a user who is not an organization member")
	}
	got := &neo4jv1beta1.AuraProjectMember{}
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
	// Roles is an array named `roles`; there is no scalar `role` in the API body.
	if api.lastInviteReq.Email != "carol@example.com" ||
		len(api.lastInviteReq.Roles) != 1 || api.lastInviteReq.Roles[0] != aura.OrgRoleMember {
		t.Errorf("invite req = %+v", api.lastInviteReq)
	}
	if len(api.lastInviteReq.ProjectInvites) != 0 {
		t.Errorf("an organization-* invite must not carry project_invites, got %+v", api.lastInviteReq.ProjectInvites)
	}
	got := &neo4jv1beta1.AuraInvite{}
	if err := c.Get(ctx, reqFor(inv).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalInviteAnnotation]; id != "inv-new" {
		t.Errorf("external-invite-id = %q, want inv-new", id)
	}
}

// ---------------------------------------------------------------------------
// AuraDatabaseRestore
// ---------------------------------------------------------------------------

// A restore overwrites a database. Submitting the same one twice destroys data
// that was already restored, so the one-shot guard is a correctness property,
// not a tidiness one — and it must key off the phase the controller ACTUALLY
// writes. It listed only "Completed", which nothing ever writes (v2beta1 gives
// no way to observe a restore finishing, so the terminal success phase is
// "Submitted"), meaning the guard never fired and every subsequent reconcile —
// an operator restart, a cache resync, any watch event — restored again.
func TestAuraDatabaseRestore_IsOneShot(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "analytics", Namespace: testNS,
			Annotations: map[string]string{AuraExternalIDAnnotation: "inst-1"},
		},
		Spec: neo4jv1beta1.AuraInstanceSpec{CredentialsSecretRef: credRef(), ProjectID: "proj-1"},
	}
	db := &neo4jv1beta1.AuraDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Name: "analytics-db", Namespace: testNS,
			Annotations: map[string]string{AuraExternalDatabaseAnnotation: "db-1"},
		},
		Spec:   neo4jv1beta1.AuraDatabaseSpec{InstanceRef: "analytics", OrganizationID: "org-1"},
		Status: neo4jv1beta1.AuraDatabaseStatus{DatabaseID: "db-1"},
	}
	bk := &neo4jv1beta1.AuraDatabaseBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-bk", Namespace: testNS},
		Spec:       neo4jv1beta1.AuraDatabaseBackupSpec{DatabaseRef: "analytics-db"},
		Status:     neo4jv1beta1.AuraDatabaseBackupStatus{BackupID: "bk-1"},
	}
	rs := &neo4jv1beta1.AuraDatabaseRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-restore", Namespace: testNS},
		Spec:       neo4jv1beta1.AuraDatabaseRestoreSpec{DatabaseRef: "analytics-db", BackupRef: "analytics-bk"},
	}
	api := &fakeDatabaseAPI{}
	c := newAuraFakeClient(t, scheme, inst, db, bk, rs)
	r := &AuraDatabaseRestoreReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: dbFactory(api),
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, reqFor(rs)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if api.restoreCalls != 1 {
		t.Fatalf("RestoreDatabase called %d times, want exactly 1 — a repeated restore silently "+
			"overwrites an already-restored database", api.restoreCalls)
	}
	if api.lastRestoreReq.BackupID != "bk-1" {
		t.Errorf("restore used backup %q, want bk-1 (resolved from backupRef)", api.lastRestoreReq.BackupID)
	}

	got := &neo4jv1beta1.AuraDatabaseRestore{}
	if err := c.Get(ctx, reqFor(rs).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// "Submitted", never "Completed": Aura exposes no way to observe completion,
	// so claiming it would assert something the operator cannot know.
	if got.Status.Phase != "Submitted" {
		t.Errorf("phase = %q, want Submitted", got.Status.Phase)
	}
}
