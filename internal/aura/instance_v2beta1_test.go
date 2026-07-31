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

// Pins the v2beta1 instance contract against the LIVE API, recorded on
// 2026-07-31 by creating a multi-database Business Critical instance and reading
// it back. There is no published schema for these paths — the spec documents
// neither a requestBody nor a response — so the fixtures below ARE the contract,
// and they are verbatim live payloads. Do not "tidy" them towards the v1 shapes:
// the differences (legacy_status, the string graph_analytics, the tier
// vocabulary, the missing multi_database on the list) are the whole point.

func TestInstanceTypeV2MapsTheVocabularies(t *testing.T) {
	// The v2beta1 tier names are quoted verbatim from the API's own 400 body:
	// "Input should be 'virtual-dedicated-cloud', 'business-critical',
	// 'professional' or 'free'".
	for v1, want := range map[string]string{
		"free-db":           "free",
		"professional-db":   "professional",
		"business-critical": "business-critical",
		"enterprise-db":     "virtual-dedicated-cloud",
	} {
		got, ok := InstanceTypeV2(v1)
		if !ok || got != want {
			t.Errorf("InstanceTypeV2(%q) = %q,%v; want %q,true", v1, got, ok, want)
		}
	}
	// The AuraDS tiers have no v2beta1 equivalent. Reporting one would send a
	// silently-wrong tier to a create that cannot be undone.
	for _, unsupported := range []string{"professional-ds", "enterprise-ds", "", "business-critical-db"} {
		if got, ok := InstanceTypeV2(unsupported); ok {
			t.Errorf("InstanceTypeV2(%q) = %q,true; want unsupported", unsupported, got)
		}
	}
}

func TestSupportsMultiDatabaseMatchesWhatAuraAccepts(t *testing.T) {
	// Verified live 2026-07-31: business-critical created successfully;
	// virtual-dedicated-cloud passed the tier check (failing later on the test
	// project not offering the enterprise tier); free and professional were both
	// refused with 400 / multi-database-tier-not-supported.
	for _, ok := range []string{InstanceTypeV2BusinessCritical, InstanceTypeV2VirtualDedicatedCloud} {
		if !SupportsMultiDatabase(ok) {
			t.Errorf("SupportsMultiDatabase(%q) = false, want true", ok)
		}
	}
	for _, no := range []string{InstanceTypeV2Free, InstanceTypeV2Professional, "", "business-critical-db"} {
		if SupportsMultiDatabase(no) {
			t.Errorf("SupportsMultiDatabase(%q) = true, but Aura refuses it", no)
		}
	}
}

func TestCreateInstanceV2LiveContract(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	base := "/v2beta1/organizations/" + org + "/projects/" + proj + "/instances"
	var createBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == base:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			w.WriteHeader(http.StatusAccepted)
			// Verbatim live 202 body (password redacted). Note default_database_id,
			// project_id and legacy_status — none of which the GET returns.
			_, _ = w.Write([]byte(`{"data":{"cloud_provider":"gcp",` +
				`"connection_url":"neo4j+s://4eca3938.databases.neo4j.io",` +
				`"created_at":"2026-07-31T09:27:04.940937Z","default_database_id":"4eca3938",` +
				`"id":"9f121265","legacy_status":"creating","memory":"2GB","multi_database":true,` +
				`"name":"op-verify-mdb","password":"REDACTED","project_id":"` + proj + `",` +
				`"region":"europe-west1","storage":"4GB","type":"business-critical",` +
				`"username":"neo4j","vector_optimized":false}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	multiDB := true
	resp, err := c.CreateInstanceV2(context.Background(), org, proj, CreateInstanceV2Request{
		Name:          "op-verify-mdb",
		Type:          InstanceTypeV2BusinessCritical,
		CloudProvider: "gcp",
		Region:        "europe-west1",
		Memory:        "2GB",
		MultiDatabase: &multiDB,
	})
	if err != nil {
		t.Fatalf("CreateInstanceV2: %v", err)
	}

	// The body must NOT carry `version` or `tenant_id`: v2beta1 has no version
	// field (Aura picks) and the project is in the path. It also must not carry a
	// v1 tier name.
	for _, absent := range []string{"version", "tenant_id", "project_id", "storage"} {
		if _, present := createBody[absent]; present {
			t.Errorf("create body must omit %q, got %v", absent, createBody)
		}
	}
	if createBody["type"] != "business-critical" || createBody["multi_database"] != true {
		t.Errorf("create body = %v, want type=business-critical multi_database=true", createBody)
	}

	if resp.ID != "9f121265" {
		t.Errorf("ID = %q, want 9f121265 (the {\"data\": …} envelope must unwrap)", resp.ID)
	}
	if resp.MultiDatabase == nil || !*resp.MultiDatabase {
		t.Errorf("MultiDatabase = %v, want true — the create response is the authoritative answer", resp.MultiDatabase)
	}
	if resp.DefaultDatabaseID != "4eca3938" {
		t.Errorf("DefaultDatabaseID = %q, want 4eca3938", resp.DefaultDatabaseID)
	}
	// The one-time credentials must survive: they are returned only here.
	if resp.Username == "" || resp.Password == "" || resp.ConnectionURL == "" {
		t.Errorf("one-time credentials lost: username=%q password set=%t url=%q",
			resp.Username, resp.Password != "", resp.ConnectionURL)
	}
	if resp.LegacyStatus != "creating" {
		t.Errorf("LegacyStatus = %q, want creating — v2beta1 has no `status` field", resp.LegacyStatus)
	}
}

func TestGetInstanceV2ReadsMultiDatabase(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	base := "/v2beta1/organizations/" + org + "/projects/" + proj + "/instances"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		// Verbatim live GET detail. `legacy_status` not `status`; `graph_analytics`
		// is a STRING; no connection_url / project_id / created_at.
		case r.Method == http.MethodGet && r.URL.Path == base+"/9f121265":
			_, _ = w.Write([]byte(`{"data":{"storage":"4GB","type":"business-critical",` +
				`"vector_optimized":false,"legacy_status":"running","memory":"2GB",` +
				`"name":"op-verify-mdb","cloud_provider":"gcp","graph_analytics":"serverless",` +
				`"id":"9f121265","multi_database":true,"region":"europe-west1"}}`))
		// Landmine 6: a v1-created instance is not in the v2beta1 store and the GET
		// answers 500 — NOT 404 — with an internal URL and an unrendered template.
		case r.Method == http.MethodGet && r.URL.Path == base+"/6f2a753a":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`invalid status code 404 [GET /aura-instances/{{.Instance_id}}]: ` +
				`https://console-api-private.default.svc.cluster.local.:443/aura-instances/6f2a753a`))
		// Verbatim live LIST: no multi_database, so it cannot answer the question.
		case r.Method == http.MethodGet && r.URL.Path == base:
			_, _ = w.Write([]byte(`{"data":[{"cloud_provider":"gcp",` +
				`"created_at":"2026-07-31T09:27:04.940937Z","id":"9f121265","name":"op-verify-mdb"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	got, err := c.GetInstanceV2(ctx, org, proj, "9f121265")
	if err != nil {
		t.Fatalf("GetInstanceV2: %v", err)
	}
	if got.MultiDatabase == nil || !*got.MultiDatabase {
		t.Fatalf("MultiDatabase = %v, want true — this endpoint is the ONLY source of the flag", got.MultiDatabase)
	}
	if got.LegacyStatus != "running" {
		t.Errorf("LegacyStatus = %q, want running — decoding from `status` yields \"\" forever", got.LegacyStatus)
	}
	if got.GraphAnalytics != "serverless" {
		t.Errorf("GraphAnalytics = %q, want serverless — it is a string here, not the v1 bool", got.GraphAnalytics)
	}

	// The 500 must surface as an error with its status preserved, so callers can
	// tell it apart and record "unknown" rather than "not multi-database".
	if _, err := c.GetInstanceV2(ctx, org, proj, "6f2a753a"); err == nil {
		t.Fatal("GetInstanceV2 on a v1-created instance must error, not return a zero InstanceV2")
	} else if !IsTransient(err) {
		t.Errorf("the 500 must classify as transient (status preserved through the plain-text body), got %v", err)
	}

	list, err := c.ListInstancesV2(ctx, org, proj)
	if err != nil || len(list) != 1 || list[0].ID != "9f121265" {
		t.Fatalf("ListInstancesV2 = %+v, err=%v", list, err)
	}
	if list[0].Name != "op-verify-mdb" {
		t.Errorf("Name = %q, want op-verify-mdb", list[0].Name)
	}
}

// doV2Data treats a missing or null `data` field as a no-op, so an unexpected 2xx
// envelope decodes to a zero struct with NO error. For a create that is the worst
// possible outcome: the caller stores an empty external-ID annotation and creates
// another PAID instance on every reconcile. That exact shape (bare vs enveloped)
// is what this file's landmines document, so the ID must be validated.
func TestCreateInstanceV2RejectsASuccessWithNoID(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	for _, body := range []string{
		`{"data":null}`,
		`{}`,
		`{"data":{"name":"x"}}`,
		// A BARE response — the divergence that caused the fleet bugs.
		`{"id":"but-not-under-data"}`,
	} {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/oauth/token" {
				writeToken(w, "tok")
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(body))
		}))
		resp, err := newTestClient(t, srv, 1000).CreateInstanceV2(context.Background(), org, proj,
			CreateInstanceV2Request{Name: "x", Type: InstanceTypeV2BusinessCritical})
		srv.Close()
		if err == nil {
			t.Errorf("body %s: got success with resp=%+v; an empty id must be an error, not an "+
				"annotation that lets the next reconcile create another paid instance", body, resp)
		}
		if resp != nil {
			t.Errorf("body %s: must not return a response alongside the error", body)
		}
	}
}
