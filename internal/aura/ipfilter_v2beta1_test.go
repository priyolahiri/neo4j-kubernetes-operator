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

// These pin the v2beta1 IP-filter contract from the official spec: an
// ORGANIZATION-scoped path (no project/instance), a BARE response body (no
// {"data":…} envelope), and the allow_list/filtered_entities shape.

func TestIPFilterV2Base(t *testing.T) {
	c := NewClient(Config{BaseURL: "https://api.neo4j.io/v1"})
	if got := c.v2beta1Base(); got != "https://api.neo4j.io/v2beta1" {
		t.Errorf("v2beta1Base = %q, want https://api.neo4j.io/v2beta1", got)
	}
}

func TestIPFilterIDAcceptsStringOrNumber(t *testing.T) {
	// The spec declares id as a string but its examples show a bare integer;
	// IPFilter.UnmarshalJSON must coerce both to the same string ID.
	var num IPFilter
	if err := json.Unmarshal([]byte(`{"id":12345678}`), &num); err != nil || num.ID != "12345678" {
		t.Errorf("numeric id → %q, err=%v; want 12345678", num.ID, err)
	}
	var str IPFilter
	if err := json.Unmarshal([]byte(`{"id":"abc-123"}`), &str); err != nil || str.ID != "abc-123" {
		t.Errorf("string id → %q, err=%v; want abc-123", str.ID, err)
	}
	var null IPFilter
	if err := json.Unmarshal([]byte(`{"name":"x"}`), &null); err != nil || null.ID != "" {
		t.Errorf("absent id → %q, err=%v; want empty", null.ID, err)
	}
}

func TestIPFilterLifecycle(t *testing.T) {
	const org = "org-1"
	base := "/v2beta1/organizations/" + org + "/ip-filters"
	var createBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == base:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			// Bare object, id as an integer (as the spec examples show).
			_, _ = w.Write([]byte(`{"id":12345678,"name":"office","organization_id":"org-1",
				"allow_list":[{"address":"203.0.113.0","prefix_len":24,"description":"office"}],
				"filtered_entities":{"instances":["inst-1"]},"filtering_disabled":false}`))
		case r.Method == http.MethodGet && r.URL.Path == base+"/12345678":
			_, _ = w.Write([]byte(`{"id":12345678,"name":"office","allow_list":[{"address":"203.0.113.0","prefix_len":24}],"filtered_entities":{"instances":["inst-1"]}}`))
		case r.Method == http.MethodGet && r.URL.Path == base:
			// Bare array.
			_, _ = w.Write([]byte(`[{"id":12345678,"name":"office","allow_list":[{"address":"203.0.113.0","prefix_len":24}],"filtered_entities":{"instances":["inst-1"]}}]`))
		case r.Method == http.MethodPatch && r.URL.Path == base+"/12345678":
			_, _ = w.Write([]byte(`{"id":12345678,"name":"office","allow_list":[{"address":"203.0.113.0","prefix_len":24},{"address":"198.51.100.7","prefix_len":32}],"filtered_entities":{"instances":["inst-1"]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	created, err := c.CreateIPFilter(ctx, org, CreateIPFilterRequest{
		Name:             "office",
		AllowList:        []IPFilterAllowEntry{{Address: "203.0.113.0", PrefixLen: 24, Description: "office"}},
		FilteredEntities: IPFilterEntities{Instances: []string{"inst-1"}},
	})
	if err != nil {
		t.Fatalf("CreateIPFilter: %v", err)
	}
	if created.ID != "12345678" {
		t.Errorf("created id = %q, want 12345678 (numeric id must decode)", created.ID)
	}
	// Request body carries allow_list + filtered_entities, not cidrs.
	if _, ok := createBody["allow_list"]; !ok {
		t.Errorf("create body missing allow_list: %v", createBody)
	}
	if _, ok := createBody["cidrs"]; ok {
		t.Errorf("create body must NOT contain cidrs: %v", createBody)
	}

	got, err := c.GetIPFilter(ctx, org, "12345678")
	if err != nil {
		t.Fatalf("GetIPFilter: %v", err)
	}
	if len(got.AllowList) != 1 || got.AllowList[0].PrefixLen != 24 {
		t.Errorf("get allow_list = %+v", got.AllowList)
	}

	list, err := c.ListIPFilters(ctx, org)
	if err != nil {
		t.Fatalf("ListIPFilters: %v", err)
	}
	if len(list) != 1 || list[0].ID != "12345678" {
		t.Errorf("list = %+v, want single 12345678", list)
	}

	upd, err := c.UpdateIPFilter(ctx, org, "12345678", UpdateIPFilterRequest{
		AllowList: &[]IPFilterAllowEntry{{Address: "203.0.113.0", PrefixLen: 24}, {Address: "198.51.100.7", PrefixLen: 32}},
	})
	if err != nil {
		t.Fatalf("UpdateIPFilter: %v", err)
	}
	if len(upd.AllowList) != 2 {
		t.Errorf("updated allow_list = %+v, want 2 entries", upd.AllowList)
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
		if r.URL.Path != "/v2beta1/organizations/org-1/ip-filters/gone" {
			t.Errorf("delete path = %q, want org-scoped /v2beta1/organizations/org-1/ip-filters/gone", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{{"message": "gone", "reason": "not-found"}},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteIPFilter(context.Background(), "org-1", "gone"); err != nil {
		t.Errorf("DeleteIPFilter on 404 = %v, want nil (idempotent)", err)
	}
}
