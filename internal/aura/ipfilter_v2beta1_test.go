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

// Pins the ip-filter contract against the LIVE API, recorded on 2026-08-01 in a
// disposable organization. The previous fixtures were built from the published
// spec and encoded a WRITE shape the API rejects outright, so every create and
// update 400'd while the tests passed. Fixtures below are verbatim live
// payloads; the request assertions are the important half.
func TestIPFilterLifecycle(t *testing.T) {
	const org = "org-1"
	base := "/v2beta1/organizations/" + org + "/ip-filters"
	const id = "9964e2ad-e966-44c0-b2b4-d04a2122ce54"
	// Verbatim live response: BARE (no data envelope), allow_list read back as
	// address+prefix_len, filtered_entities (not entities), the undocumented
	// brain_ip_addresses_enabled, and an RFC1123 updated_at.
	const live = `{"filtered_entities":{"projects":["proj-1"]},"filtering_disabled":false,` +
		`"id":"` + id + `","organization_id":"` + org + `",` +
		`"updated_at":"Sat, 01 Aug 2026 10:22:01 GMT","description":null,"name":"office",` +
		`"allow_list":[{"prefix_len":24,"address":"203.0.113.0","description":"office"}],` +
		`"brain_ip_addresses_enabled":false}`
	var createBody, patchBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == base:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			_, _ = w.Write([]byte(live)) // create answers 200, not 201
		case r.Method == http.MethodGet && r.URL.Path == base+"/"+id:
			_, _ = w.Write([]byte(live))
		case r.Method == http.MethodGet && r.URL.Path == base:
			_, _ = w.Write([]byte("[" + live + "]")) // bare array
		case r.Method == http.MethodPatch && r.URL.Path == base+"/"+id:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &patchBody)
			_, _ = w.Write([]byte(live))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	created, err := c.CreateIPFilter(ctx, org, CreateIPFilterRequest{
		Name:      "office",
		AllowList: []IPFilterAllowEntry{{Address: "203.0.113.0", PrefixLen: 24, Description: "office"}},
		Entities:  IPFilterEntities{Projects: []string{"proj-1"}},
	})
	if err != nil {
		t.Fatalf("CreateIPFilter: %v", err)
	}
	if created.ID != id {
		t.Errorf("created id = %q, want %q", created.ID, id)
	}
	if !created.FilteringDisabled && created.BrainIPAddressesEnabled {
		t.Errorf("unexpected flags: %+v", created)
	}

	// --- the create body is where the shipped bug lived ---
	al, _ := createBody["allow_list"].([]any)
	if len(al) != 1 {
		t.Fatalf("create allow_list = %v", createBody["allow_list"])
	}
	entry, _ := al[0].(map[string]any)
	if entry["ip_range"] != "203.0.113.0/24" {
		t.Errorf("allow_list entry must send ip_range as CIDR, got %v", entry)
	}
	if _, bad := entry["address"]; bad {
		t.Errorf("allow_list entry must NOT send address/prefix_len on write, got %v", entry)
	}
	// description may not be null; an empty string is fine, so the key is always
	// present — omitempty here is a hard 400.
	if _, ok := entry["description"]; !ok {
		t.Errorf("allow_list entry must always carry description (null is rejected), got %v", entry)
	}
	if createBody["organization_id"] != org {
		t.Errorf("create body must carry organization_id even though the path is org-scoped, got %v", createBody)
	}
	if _, ok := createBody["entities"]; !ok {
		t.Errorf("create body must use `entities`, got %v", createBody)
	}
	if _, bad := createBody["filtered_entities"]; bad {
		t.Errorf("`filtered_entities` is the READ name; sending it returns 200 and silently attaches "+
			"NOTHING, got %v", createBody)
	}

	got, err := c.GetIPFilter(ctx, org, id)
	if err != nil {
		t.Fatalf("GetIPFilter: %v", err)
	}
	if len(got.AllowList) != 1 || got.AllowList[0].PrefixLen != 24 || got.AllowList[0].Address != "203.0.113.0" {
		t.Errorf("read allow_list must decode address+prefix_len, got %+v", got.AllowList)
	}
	if got.AllowList[0].CIDR() != "203.0.113.0/24" {
		t.Errorf("CIDR() = %q", got.AllowList[0].CIDR())
	}
	if got.UpdatedAt != "Sat, 01 Aug 2026 10:22:01 GMT" {
		t.Errorf("updated_at is RFC1123 on this endpoint, got %q", got.UpdatedAt)
	}
	if len(got.FilteredEntities.Projects) != 1 {
		t.Errorf("filtered_entities must decode on READ, got %+v", got.FilteredEntities)
	}

	list, err := c.ListIPFilters(ctx, org)
	if err != nil || len(list) != 1 || list[0].ID != id {
		t.Fatalf("ListIPFilters = %+v, err=%v (must decode a BARE array)", list, err)
	}

	if _, err := c.UpdateIPFilter(ctx, org, id, UpdateIPFilterRequest{
		AllowList: &[]IPFilterAllowEntry{{Address: "198.51.100.7", PrefixLen: 32, Description: "vpn"}},
		Entities:  &IPFilterEntities{Projects: []string{"proj-1"}},
	}); err != nil {
		t.Fatalf("UpdateIPFilter: %v", err)
	}
	pal, _ := patchBody["allow_list"].([]any)
	pentry, _ := pal[0].(map[string]any)
	if pentry["ip_range"] != "198.51.100.7/32" {
		t.Errorf("PATCH allow_list must also send ip_range, got %v", pentry)
	}
	if _, ok := patchBody["entities"]; !ok {
		t.Errorf("PATCH must use `entities`, got %v", patchBody)
	}
	if _, bad := patchBody["filtered_entities"]; bad {
		t.Errorf("PATCH must not send `filtered_entities`, got %v", patchBody)
	}
}

// A SUCCESSFUL delete arrives as HTTP 500 — the gateway rejects its own
// backend's 204. Because IsTransient treats 5xx as retryable, the previous
// implementation left the AuraIPFilter finalizer retrying forever on a filter
// that had already been deleted. The client confirms by GET instead of trusting
// the status code.
func TestIPFilterDeleteSucceedsDespiteGateway500(t *testing.T) {
	const org, id = "org-1", "f-1"
	base := "/v2beta1/organizations/" + org + "/ip-filters"
	var getCalls int
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodDelete && r.URL.Path == base+"/"+id:
			// Verbatim live: a 204 from the backend, surfaced as a 500 with an
			// unrendered Go template and an internal address.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`invalid status code 204 [DELETE /ip-filters/{{.Ip_filter_id}}]: ` +
				`https://console-api-private.default.svc.cluster.local.:443/ip-filters/` + id))
		case r.Method == http.MethodGet && r.URL.Path == base+"/"+id:
			getCalls++
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Ip Filter not found: ` + id + `","reason":"ip-filter-not-found"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteIPFilter(context.Background(), org, id); err != nil {
		t.Fatalf("DeleteIPFilter must succeed when the filter is gone, got %v", err)
	}
	if getCalls != 1 {
		t.Errorf("expected one confirming GET, got %d", getCalls)
	}
}

// A delete that genuinely failed must still be reported as a failure.
func TestIPFilterDeleteReportsARealFailure(t *testing.T) {
	const org, id = "org-1", "f-1"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"boom","reason":"internal"}`))
		case r.Method == http.MethodGet:
			// Still there — so the delete really did fail.
			_, _ = w.Write([]byte(`{"id":"` + id + `","name":"x","allow_list":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteIPFilter(context.Background(), org, id); err == nil {
		t.Error("a delete that left the filter in place must return an error, not be swallowed")
	}
}
