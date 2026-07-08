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

// Pins the v2beta1 database contract: the instance-nested path and the standard
// {"data": …} envelope (unlike ip-filters, database endpoints ARE data-wrapped).

func TestDatabaseLifecycle(t *testing.T) {
	const org, proj, inst = "org-1", "proj-1", "inst-1"
	base := "/v2beta1/organizations/" + org + "/projects/" + proj + "/instances/" + inst + "/databases"
	var createBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == base:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			_, _ = w.Write([]byte(`{"data":{"id":"db-1","name":"analytics","legacy_status":"running"}}`))
		case r.Method == http.MethodGet && r.URL.Path == base+"/db-1":
			_, _ = w.Write([]byte(`{"data":{"id":"db-1","name":"analytics","legacy_status":"running"}}`))
		case r.Method == http.MethodGet && r.URL.Path == base:
			_, _ = w.Write([]byte(`{"data":[{"id":"db-1","name":"analytics"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == base+"/db-1/backups":
			_, _ = w.Write([]byte(`{"data":{"id":"bk-1","database_id":"db-1","legacy_status":"completed"}}`))
		case r.Method == http.MethodPost && r.URL.Path == base+"/db-1/restore":
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodDelete && r.URL.Path == base+"/db-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	created, err := c.CreateDatabase(ctx, org, proj, inst, CreateDatabaseRequest{Name: "analytics"})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if created.ID != "db-1" || created.Name != "analytics" {
		t.Errorf("created = %+v, want id db-1 name analytics (data envelope must unwrap)", created)
	}
	if createBody["name"] != "analytics" {
		t.Errorf("create body = %v, want name=analytics", createBody)
	}

	got, err := c.GetDatabase(ctx, org, proj, inst, "db-1")
	if err != nil || got.Status != "running" {
		t.Errorf("GetDatabase = %+v, err=%v (legacy_status must decode)", got, err)
	}

	list, err := c.ListDatabases(ctx, org, proj, inst)
	if err != nil || len(list) != 1 || list[0].ID != "db-1" {
		t.Errorf("ListDatabases = %+v, err=%v", list, err)
	}

	bk, err := c.CreateDatabaseBackup(ctx, org, proj, inst, "db-1")
	if err != nil || bk.ID != "bk-1" {
		t.Errorf("CreateDatabaseBackup = %+v, err=%v", bk, err)
	}

	if err := c.RestoreDatabase(ctx, org, proj, inst, "db-1", RestoreDatabaseRequest{BackupID: "bk-1"}); err != nil {
		t.Errorf("RestoreDatabase: %v", err)
	}

	if err := c.DeleteDatabase(ctx, org, proj, inst, "db-1"); err != nil {
		t.Errorf("DeleteDatabase: %v", err)
	}
}

func TestDatabaseDeleteTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]any{{"message": "gone", "reason": "not-found"}}})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteDatabase(context.Background(), "o", "p", "i", "gone"); err != nil {
		t.Errorf("DeleteDatabase on 404 = %v, want nil (idempotent)", err)
	}
}
