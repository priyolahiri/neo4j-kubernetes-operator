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
	"testing"
)

// Pins the v2beta1 console-RBAC contract: org/project user paths, the invites
// path, and the {"data": …} envelope.

func TestMembersAndInvites(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	orgUsers := "/v2beta1/organizations/" + org + "/users"
	projUsers := "/v2beta1/organizations/" + org + "/projects/" + proj + "/users"
	invites := "/v2beta1/organizations/" + org + "/invites"
	var inviteBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodGet && r.URL.Path == orgUsers:
			_, _ = w.Write([]byte(`{"data":[{"id":"u-1","email":"alice@example.com","role":"ORG_MEMBER"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == orgUsers+"/u-1":
			_, _ = w.Write([]byte(`{"data":{"id":"u-1","email":"alice@example.com","role":"ORG_ADMIN"}}`))
		case r.Method == http.MethodGet && r.URL.Path == projUsers:
			_, _ = w.Write([]byte(`{"data":[{"id":"u-2","email":"bob@example.com","role":"PROJECT_VIEWER"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == invites:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &inviteBody)
			_, _ = w.Write([]byte(`{"data":{"id":"inv-1","email":"carol@example.com","role":"ORG_MEMBER"}}`))
		case r.Method == http.MethodGet && r.URL.Path == invites+"/inv-1":
			_, _ = w.Write([]byte(`{"data":{"id":"inv-1","email":"carol@example.com","role":"ORG_MEMBER"}}`))
		case r.Method == http.MethodDelete && r.URL.Path == invites+"/inv-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	members, err := c.ListOrgMembers(ctx, org)
	if err != nil || len(members) != 1 || members[0].Email != "alice@example.com" {
		t.Fatalf("ListOrgMembers = %+v, err=%v", members, err)
	}
	upd, err := c.UpdateOrgMemberRole(ctx, org, "u-1", OrgRoleAdmin)
	if err != nil || upd.Role != "ORG_ADMIN" {
		t.Errorf("UpdateOrgMemberRole = %+v, err=%v", upd, err)
	}

	pm, err := c.ListProjectMembers(ctx, org, proj)
	if err != nil || len(pm) != 1 || pm[0].Role != "PROJECT_VIEWER" {
		t.Errorf("ListProjectMembers = %+v, err=%v", pm, err)
	}

	inv, err := c.CreateInvite(ctx, org, CreateInviteRequest{Email: "carol@example.com", Role: OrgRoleMember})
	if err != nil || inv.ID != "inv-1" {
		t.Fatalf("CreateInvite = %+v, err=%v", inv, err)
	}
	if inviteBody["email"] != "carol@example.com" || inviteBody["role"] != "ORG_MEMBER" {
		t.Errorf("invite body = %v", inviteBody)
	}
	if _, err := c.GetInvite(ctx, org, "inv-1"); err != nil {
		t.Errorf("GetInvite: %v", err)
	}
	if err := c.DeleteInvite(ctx, org, "inv-1"); err != nil {
		t.Errorf("DeleteInvite: %v", err)
	}
}
