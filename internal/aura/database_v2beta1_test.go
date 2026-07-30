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

// Pins the v2beta1 database contract against the PUBLISHED SPEC
// (https://api.neo4j.io/v2beta1/spec.json): the instance-nested path, the
// standard {"data": …} envelope (unlike ip-filters, these ARE data-wrapped), the
// thin DatabaseSummary shape, and the restore body's field name.
//
// The fixtures serve the SPEC's shapes. An earlier version served the client's
// invented `name` / `legacy_status` / `database_id` fields, so the suite passed
// against a contract the API never served and hid a restore body that could
// never work. If you change a fixture, change it to match the spec.

func TestDatabaseLifecycle(t *testing.T) {
	const org, proj, inst = "org-1", "proj-1", "inst-1"
	base := "/v2beta1/organizations/" + org + "/projects/" + proj + "/instances/" + inst + "/databases"
	var createBody, restoreBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		// DatabaseSummary carries ONLY `id` — no name, no status.
		case r.Method == http.MethodPost && r.URL.Path == base:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"data":{"id":"db-1"}}`))
		case r.Method == http.MethodGet && r.URL.Path == base+"/db-1":
			_, _ = w.Write([]byte(`{"data":{"id":"db-1"}}`))
		case r.Method == http.MethodGet && r.URL.Path == base:
			_, _ = w.Write([]byte(`{"data":[{"id":"db-1"}]}`))
		// CreateDatabaseBackupResponse is `{id}` only — no status yet.
		case r.Method == http.MethodPost && r.URL.Path == base+"/db-1/backups":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"data":{"id":"bk-1"}}`))
		// DatabaseBackup: id, timestamp, status (enum), exportable — all required.
		case r.Method == http.MethodGet && r.URL.Path == base+"/db-1/backups/bk-1":
			_, _ = w.Write([]byte(`{"data":{"id":"bk-1","timestamp":"2026-07-01T00:00:00Z",` +
				`"status":"Completed","exportable":true}}`))
		case r.Method == http.MethodGet && r.URL.Path == base+"/db-1/backups":
			_, _ = w.Write([]byte(`{"data":[{"id":"bk-1","timestamp":"2026-07-01T00:00:00Z",` +
				`"status":"InProgress","exportable":false}]}`))
		case r.Method == http.MethodPost && r.URL.Path == base+"/db-1/restore":
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &restoreBody)
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodDelete && r.URL.Path == base+"/db-1":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"data":{"id":"db-1"}}`))
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
	if created.ID != "db-1" {
		t.Errorf("created = %+v, want id db-1 (data envelope must unwrap)", created)
	}
	if createBody["name"] != "analytics" {
		t.Errorf("create body = %v, want name=analytics", createBody)
	}

	got, err := c.GetDatabase(ctx, org, proj, inst, "db-1")
	if err != nil || got.ID != "db-1" {
		t.Errorf("GetDatabase = %+v, err=%v", got, err)
	}

	list, err := c.ListDatabases(ctx, org, proj, inst)
	if err != nil || len(list) != 1 || list[0].ID != "db-1" {
		t.Errorf("ListDatabases = %+v, err=%v", list, err)
	}

	// The create response is `{id}` only, so status is empty here — it must NOT
	// be interpreted as "completed" by callers.
	bk, err := c.CreateDatabaseBackup(ctx, org, proj, inst, "db-1")
	if err != nil || bk.ID != "bk-1" {
		t.Fatalf("CreateDatabaseBackup = %+v, err=%v", bk, err)
	}
	if bk.Status != "" {
		t.Errorf("create-backup Status = %q, want empty (create response carries only id)", bk.Status)
	}

	// A read-back populates the required status/timestamp/exportable fields.
	full, err := c.GetDatabaseBackup(ctx, org, proj, inst, "db-1", "bk-1")
	if err != nil {
		t.Fatalf("GetDatabaseBackup: %v", err)
	}
	if full.Status != BackupStatusCompleted {
		t.Errorf("Status = %q, want %q — must decode from `status`, not `legacy_status`", full.Status, BackupStatusCompleted)
	}
	if !full.Exportable {
		t.Error("Exportable must decode from `exportable` (a required spec field)")
	}
	if full.Timestamp == "" {
		t.Error("Timestamp must decode from `timestamp`")
	}

	backups, err := c.ListDatabaseBackups(ctx, org, proj, inst, "db-1")
	if err != nil || len(backups) != 1 || backups[0].Status != BackupStatusInProgress {
		t.Errorf("ListDatabaseBackups = %+v, err=%v", backups, err)
	}

	if err := c.RestoreDatabase(ctx, org, proj, inst, "db-1", RestoreDatabaseRequest{BackupID: "bk-1"}); err != nil {
		t.Errorf("RestoreDatabase: %v", err)
	}
	// The restore body's required field is `id`, NOT `backup_id`. This assertion
	// is the one that was missing before, which is why the wrong name shipped.
	if restoreBody["id"] != "bk-1" {
		t.Errorf("restore body = %v, want {\"id\":\"bk-1\"} — the spec's required field is `id`", restoreBody)
	}
	if _, exists := restoreBody["backup_id"]; exists {
		t.Error("restore body must not carry `backup_id`; the spec's field is `id`")
	}

	if err := c.DeleteDatabase(ctx, org, proj, inst, "db-1"); err != nil {
		t.Errorf("DeleteDatabase: %v", err)
	}
}

// TestBackupStatusConstantsMatchSpec pins the DatabaseBackup.status enum. The
// canonical casing is title-case; an earlier cut also accepted "COMPLETED",
// which appears nowhere in the API.
func TestBackupStatusConstantsMatchSpec(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{BackupStatusPending, "Pending"},
		{BackupStatusInProgress, "InProgress"},
		{BackupStatusCompleted, "Completed"},
		{BackupStatusFailed, "Failed"},
	} {
		if tc.got != tc.want {
			t.Errorf("backup status constant = %q, spec says %q", tc.got, tc.want)
		}
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
