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

package aura

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// Pins the v2beta1 console-RBAC contract. The read/paths half came from the
// published spec; the WRITE half — body field names, the three role vocabularies,
// and the invite slots — was verified against a live organization on 2026-08-01
// and is quoted from the API's own validation errors. Fixtures that show a
// response are verbatim live payloads.
//
// The fixtures below deliberately serve the spec's field names — `user_id`,
// `organization_roles`, `project_roles`, invite `status` — and the assertions
// check the REQUEST bodies, because an earlier version of this suite echoed the
// client's invented `{"id","email","role"}` shape back at it and therefore
// passed against a contract the API does not accept. If you change a fixture here,
// change it to match the spec, never to match the client.

// specOrgUserJSON is a v2beta1 OrganizationUser. All seven properties are
// `required` upstream; the operator only reads the first three.
const specOrgUserJSON = `{
  "user_id": "11111111-1111-1111-1111-111111111111",
  "email": "alice@example.com",
  "organization_roles": ["organization-member"],
  "exempt_from_automatic_removal": false,
  "mfa_enrollment_status": "enrolled",
  "mfa_enrolled_methods": [{"id": "totp"}],
  "last_activity_at": "2026-07-01T00:00:00Z"
}`

// specProjectUserJSON is a v2beta1 ProjectUser (user_id, email, project_roles).
const specProjectUserJSON = `{
  "user_id": "22222222-2222-2222-2222-222222222222",
  "email": "bob@example.com",
  "project_roles": ["project-viewer"]
}`

func TestMembersAndInvites(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	const aliceID = "11111111-1111-1111-1111-111111111111"
	orgUsers := "/v2beta1/organizations/" + org + "/users"
	projUsers := "/v2beta1/organizations/" + org + "/projects/" + proj + "/users"
	invites := "/v2beta1/organizations/" + org + "/invites"

	var orgPatchBody, projPatchBody, addMemberBody, inviteBody map[string]any
	var singleInviteGetCalled bool

	readJSON := func(r *http.Request, into *map[string]any) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, into)
	}

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")

		case r.Method == http.MethodGet && r.URL.Path == orgUsers:
			_, _ = w.Write([]byte(`{"data":[` + specOrgUserJSON + `]}`))

		case r.Method == http.MethodPatch && r.URL.Path == orgUsers+"/"+aliceID:
			readJSON(r, &orgPatchBody)
			_, _ = w.Write([]byte(`{"data":{"user_id":"` + aliceID + `","email":"alice@example.com",` +
				`"organization_roles":["organization-admin"]}}`))

		case r.Method == http.MethodGet && r.URL.Path == projUsers:
			_, _ = w.Write([]byte(`{"data":[` + specProjectUserJSON + `]}`))

		// POST project users returns 201 with NO body at all, per the spec.
		case r.Method == http.MethodPost && r.URL.Path == projUsers:
			readJSON(r, &addMemberBody)
			w.WriteHeader(http.StatusCreated)

		case r.Method == http.MethodPost && r.URL.Path == invites:
			readJSON(r, &inviteBody)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"data":{"id":"inv-1","email":"carol@example.com","organization_id":"` + org + `",` +
				`"organization_roles":["organization-member"],"status":"active",` +
				`"expires_at":"2026-08-01T00:00:00Z","invited_by":"` + aliceID + `"}}`))

		case r.Method == http.MethodGet && r.URL.Path == invites:
			_, _ = w.Write([]byte(`{"data":[{"id":"inv-1","email":"carol@example.com","status":"active",` +
				`"organization_roles":["organization-member"],` +
				`"project_invites":[{"project_id":"` + proj + `","project_roles":["namespace-member"]}]}]}`))

		// The spec defines ONLY delete on /invites/{id}. A GET here is a contract
		// violation by the client, so fail loudly rather than answering it.
		case r.Method == http.MethodGet && r.URL.Path == invites+"/inv-1":
			singleInviteGetCalled = true
			w.WriteHeader(http.StatusMethodNotAllowed)

		case r.Method == http.MethodDelete && r.URL.Path == invites+"/inv-1":
			w.WriteHeader(http.StatusNoContent)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	// ---- org members -------------------------------------------------------
	members, err := c.ListOrgMembers(ctx, org)
	if err != nil || len(members) != 1 {
		t.Fatalf("ListOrgMembers = %+v, err=%v", members, err)
	}
	if members[0].UserID != aliceID {
		t.Errorf("UserID must decode from `user_id`, got %q", members[0].UserID)
	}
	if got := members[0].Role(); got != OrgRoleMember {
		t.Errorf("Role() must read organization_roles[0], got %q want %q", got, OrgRoleMember)
	}

	upd, err := c.UpdateOrgMemberRole(ctx, org, aliceID, OrgRoleAdmin)
	if err != nil || upd.Role() != OrgRoleAdmin {
		t.Errorf("UpdateOrgMemberRole = %+v (role %q), err=%v", upd, upd.Role(), err)
	}
	// The PATCH body must be exactly {"organization_roles":[<one role>]}. The
	// schema sets additionalProperties:false with the array required, so a scalar
	// "role" key is a hard 400.
	if want := map[string]any{"organization_roles": []any{OrgRoleAdmin}}; !reflect.DeepEqual(orgPatchBody, want) {
		t.Errorf("org PATCH body = %v, want exactly %v", orgPatchBody, want)
	}

	// ---- project members ---------------------------------------------------
	pm, err := c.ListProjectMembers(ctx, org, proj)
	if err != nil || len(pm) != 1 {
		t.Fatalf("ListProjectMembers = %+v, err=%v", pm, err)
	}
	if got := pm[0].Role(); got != ProjectRoleViewer {
		t.Errorf("Role() must read project_roles[0], got %q want %q", got, ProjectRoleViewer)
	}

	if err := c.AddProjectMember(ctx, org, proj, aliceID, ProjectRoleAdmin); err != nil {
		t.Fatalf("AddProjectMember: %v", err)
	}
	// Takes a user UUID + project_roles — never an email.
	if want := map[string]any{"user_id": aliceID, "project_roles": []any{ProjectRoleAdmin}}; !reflect.DeepEqual(addMemberBody, want) {
		t.Errorf("add-project-member body = %v, want exactly %v", addMemberBody, want)
	}

	if _, err := c.UpdateProjectMemberRole(ctx, org, proj, aliceID, ProjectRoleMember); err == nil {
		// The fixture has no PATCH handler for the project user path, so this is
		// expected to fail; we only care that it is not a panic.
		_ = projPatchBody
	}

	// ---- invites -----------------------------------------------------------
	inv, err := c.CreateInvite(ctx, org, CreateInviteRequest{
		Email: "carol@example.com",
		Roles: []string{OrgRoleMember},
	})
	if err != nil || inv.ID != "inv-1" {
		t.Fatalf("CreateInvite = %+v, err=%v", inv, err)
	}
	if inv.Status != InviteStatusActive {
		t.Errorf("Status must decode from `status` (not legacy_status), got %q", inv.Status)
	}
	// Roles is an ARRAY named `roles`; there is no scalar `role` and no top-level
	// `project_id`. project_invites is ALWAYS emitted — as [] for an
	// organization-only invite — because omitting it is "Field required" and null
	// is rejected. This assertion previously demanded the opposite (exactly
	// {email, roles}), which is precisely the body the live API 400s.
	if want := map[string]any{
		"email":           "carol@example.com",
		"roles":           []any{OrgRoleMember},
		"project_invites": []any{},
	}; !reflect.DeepEqual(inviteBody, want) {
		t.Errorf("invite body = %v, want exactly %v", inviteBody, want)
	}

	// A project-scoped invite goes in project_invites, using namespace-* roles —
	// but Aura requires an organization role on EVERY invite, so it must be
	// accompanied by one. (This block previously sent no organization role and
	// asserted success; live, that is a 400.)
	if _, err := c.CreateInvite(ctx, org, CreateInviteRequest{
		Email:          "dave@example.com",
		Roles:          []string{OrgRoleMember},
		ProjectInvites: []ProjectInvite{{ProjectID: proj, ProjectRoles: []string{NamespaceRoleMember}}},
	}); err != nil {
		t.Fatalf("CreateInvite (project-scoped): %v", err)
	}
	pi, ok := inviteBody["project_invites"].([]any)
	if !ok || len(pi) != 1 {
		t.Fatalf("project-scoped invite body = %v, want a project_invites array", inviteBody)
	}
	first, _ := pi[0].(map[string]any)
	if want := map[string]any{"project_id": proj, "project_roles": []any{NamespaceRoleMember}}; !reflect.DeepEqual(first, want) {
		t.Errorf("project_invites[0] = %v, want %v", first, want)
	}
	if _, exists := inviteBody["role"]; exists {
		t.Error("invite body must not carry a scalar `role` key")
	}
	if _, exists := inviteBody["project_id"]; exists {
		t.Error("invite body must not carry a top-level `project_id` key")
	}

	// FindInvite reads via LIST, because GET /invites/{id} does not exist.
	found, err := c.FindInvite(ctx, org, "inv-1")
	if err != nil || found == nil || found.ID != "inv-1" {
		t.Fatalf("FindInvite = %+v, err=%v", found, err)
	}
	if len(found.ProjectInvites) != 1 || found.ProjectInvites[0].ProjectID != proj {
		t.Errorf("FindInvite must decode project_invites, got %+v", found.ProjectInvites)
	}
	if singleInviteGetCalled {
		t.Error("client called GET /invites/{id}, which does not exist in v2beta1 (only DELETE)")
	}

	// Absent invite -> (nil, nil), never an error.
	missing, err := c.FindInvite(ctx, org, "does-not-exist")
	if err != nil || missing != nil {
		t.Errorf("FindInvite(absent) = %+v, err=%v; want nil, nil", missing, err)
	}

	if err := c.DeleteInvite(ctx, org, "inv-1"); err != nil {
		t.Errorf("DeleteInvite: %v", err)
	}
}

// TestRoleVocabulariesMatchSpec pins the three DISTINCT role vocabularies. The
// API uses lowercase-hyphenated names, and spells project roles `project-*` on
// the project-users endpoints but `namespace-*` inside an invite body. An
// earlier cut used SCREAMING_SNAKE values that exist nowhere in the API.
// All three enums are quoted from the API's OWN validation errors, obtained live
// on 2026-08-01 by sending a bogus role to each endpoint. The third is the
// surprising one: an invite's per-project roles are `namespace-*`, NOT the
// `project-*` values the project-members endpoints take.
func TestRoleVocabulariesMatchSpec(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{OrgRoleOwner, "organization-owner"},
		{OrgRoleAdmin, "organization-admin"},
		{OrgRoleMember, "organization-member"},
		{ProjectRoleAdmin, "project-admin"},
		{ProjectRoleMember, "project-member"},
		{ProjectRoleViewer, "project-viewer"},
		{ProjectRoleMetricsIntegrationRead, "project-metrics-integration-reader"},
		{NamespaceRoleAdmin, "namespace-admin"},
		{NamespaceRoleMember, "namespace-member"},
		{NamespaceRoleViewer, "namespace-viewer"},
		{NamespaceRoleMetricsIntegrationRead, "namespace-metrics-integration-reader"},
		{InviteStatusActive, "active"},
		{InviteStatusAccepted, "accepted"},
		{InviteStatusRevoked, "revoked"},
		{InviteStatusExpired, "expired"},
		{InviteStatusDeclined, "declined"},
	} {
		if tc.got != tc.want {
			t.Errorf("role/status constant = %q, spec says %q", tc.got, tc.want)
		}
	}
}

// The invite body's tags are load-bearing, and the omitempty that used to be on
// both slices made EVERY invite the operator could build a 400. Verified live
// 2026-08-01:
//   - `roles` must be present with AT LEAST ONE organization role ([] → "List
//     should have at least 1 item after validation"; null → "got null, want
//     array");
//   - `project_invites` must be present but MAY be [] — that is the normal shape
//     for an organization-only invite. Omitted → "Field required".
func TestCreateInviteAlwaysEmitsBothSlots(t *testing.T) {
	var body map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		// Verbatim live response: enveloped, RFC1123 expiry, and `roles` on the
		// way in comes back as `organization_roles`.
		_, _ = w.Write([]byte(`{"data":{"invited_by":"u-1","organization_id":"org-1",` +
			`"organization_roles":["organization-member"],"project_invites":[],"status":"active",` +
			`"email":"a@example.com","expires_at":"Mon, 31 Aug 2026 10:42:36 GMT",` +
			`"id":"d66796dc-1390-4f11-9432-0f8865a13b09"}}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	got, err := c.CreateInvite(context.Background(), "org-1", CreateInviteRequest{
		Email: "a@example.com",
		Roles: []string{OrgRoleMember},
		// ProjectInvites deliberately nil — the organization-only case.
	})
	if err != nil {
		t.Fatalf("CreateInvite: %v", err)
	}
	if got.ID == "" || got.Status != "active" {
		t.Errorf("response mis-decoded: %+v", got)
	}
	if len(got.OrganizationRoles) != 1 {
		t.Errorf("`roles` on write comes back as `organization_roles`; got %+v", got.OrganizationRoles)
	}

	pi, present := body["project_invites"]
	if !present {
		t.Fatalf("project_invites must ALWAYS be emitted (omitted is \"Field required\"), got %v", body)
	}
	if pi == nil {
		t.Errorf("project_invites must be [] and never null (null is rejected), got %v", body)
	}
	if arr, ok := pi.([]any); !ok || len(arr) != 0 {
		t.Errorf("project_invites should be an empty array here, got %#v", pi)
	}
	if roles, _ := body["roles"].([]any); len(roles) != 1 {
		t.Errorf("roles must carry at least one organization role, got %v", body["roles"])
	}
}

// A project-only invite is not expressible on this API, so the client refuses it
// with an explanation rather than letting the API answer "List should have at
// least 1 item after validation, not 0" — which never mentions that an
// organization role is the missing thing.
func TestCreateInviteRefusesAProjectOnlyInvite(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		t.Error("must refuse locally; no request should reach the API")
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	_, err := c.CreateInvite(context.Background(), "org-1", CreateInviteRequest{
		Email:          "a@example.com",
		ProjectInvites: []ProjectInvite{{ProjectID: "p-1", ProjectRoles: []string{NamespaceRoleMember}}},
	})
	if err == nil {
		t.Fatal("expected a refusal for an invite with no organization role")
	}
	if !strings.Contains(err.Error(), "organization role") {
		t.Errorf("message must name what is missing, got %q", err.Error())
	}
}
