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
	"strings"
	"testing"
)

// NOTE: these tests pin the RECONSTRUCTED v2beta1 IP-filter contract (paths,
// envelope, fields). They assert the client behaves consistently with what this
// package assumes — NOT that the assumption matches the live Aura API, which is
// unverified. When the real contract is confirmed, update ipfilter_v2beta1.go
// and these expectations together.

func TestIPFilterV2Base(t *testing.T) {
	c := NewClient(Config{BaseURL: "https://api.neo4j.io/v1"})
	if got := c.v2beta1Base(); got != "https://api.neo4j.io/v2beta1" {
		t.Errorf("v2beta1Base = %q, want https://api.neo4j.io/v2beta1", got)
	}
}

func TestIPFilterLifecycle(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	base := "/v2beta1/organizations/" + org + "/projects/" + proj + "/network/ip-filters"
	var createBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == base:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"id": "ipf-1", "name": "app", "status": "pending",
					"instance_id": "inst-1", "cidrs": []string{"203.0.113.0/24"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == base+"/ipf-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"id": "ipf-1", "name": "app", "status": "ready",
					"instance_id": "inst-1", "cidrs": []string{"203.0.113.0/24"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == base:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": "ipf-1", "name": "app", "status": "ready", "instance_id": "inst-1", "cidrs": []string{"203.0.113.0/24"}},
				},
			})
		case r.Method == http.MethodPatch && r.URL.Path == base+"/ipf-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"id": "ipf-1", "name": "app", "status": "updating",
					"instance_id": "inst-1", "cidrs": []string{"203.0.113.0/24", "198.51.100.7/32"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	created, err := c.CreateIPFilter(ctx, org, proj, CreateIPFilterRequest{
		Name: "app", InstanceID: "inst-1", CIDRs: []string{"203.0.113.0/24"},
	})
	if err != nil {
		t.Fatalf("CreateIPFilter: %v", err)
	}
	if created.ID != "ipf-1" || IsIPFilterReady(created.Status) {
		t.Errorf("created = %+v, want id=ipf-1 status=pending", created)
	}
	if createBody["instance_id"] != "inst-1" {
		t.Errorf("create body instance_id = %v, want inst-1", createBody["instance_id"])
	}

	got, err := c.GetIPFilter(ctx, org, proj, "ipf-1")
	if err != nil {
		t.Fatalf("GetIPFilter: %v", err)
	}
	if !IsIPFilterReady(got.Status) {
		t.Errorf("get status = %q, want ready", got.Status)
	}

	list, err := c.ListIPFilters(ctx, org, proj)
	if err != nil {
		t.Fatalf("ListIPFilters: %v", err)
	}
	if len(list) != 1 || list[0].ID != "ipf-1" {
		t.Errorf("list = %+v, want single ipf-1", list)
	}

	name := "app"
	cidrs := []string{"203.0.113.0/24", "198.51.100.7/32"}
	upd, err := c.UpdateIPFilter(ctx, org, proj, "ipf-1", UpdateIPFilterRequest{Name: &name, CIDRs: &cidrs})
	if err != nil {
		t.Fatalf("UpdateIPFilter: %v", err)
	}
	if len(upd.CIDRs) != 2 {
		t.Errorf("updated cidrs = %v, want 2 entries", upd.CIDRs)
	}
}

func TestIPFilterDeleteTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		if r.Method != http.MethodDelete {
			http.NotFound(w, r)
			return
		}
		// Assert the request targets the v2beta1 base, not v1.
		if !strings.HasPrefix(r.URL.Path, "/v2beta1/organizations/") {
			t.Errorf("delete path = %q, want a /v2beta1/organizations/... path", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{{"message": "gone", "reason": "not-found"}},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteIPFilter(context.Background(), "org-1", "proj-1", "gone"); err != nil {
		t.Errorf("DeleteIPFilter on 404 = %v, want nil (idempotent)", err)
	}
}
