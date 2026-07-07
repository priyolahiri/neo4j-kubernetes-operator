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

func TestCMKLifecycle(t *testing.T) {
	var createBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == "/v1/customer-managed-keys":
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &createBody)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"id": "cmk-1", "name": "MyKey", "status": "pending",
					"tenant_id": "t1", "instance_type": "enterprise-db",
					"cloud_provider": "gcp", "region": "europe-west1",
					"key_id": "projects/p/locations/eu/keyRings/r/cryptoKeys/k",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customer-managed-keys/cmk-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"id": "cmk-1", "name": "MyKey", "status": "ready",
					"tenant_id": "t1", "instance_type": "enterprise-db",
					"cloud_provider": "gcp", "region": "europe-west1",
					"key_id": "projects/p/locations/eu/keyRings/r/cryptoKeys/k",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customer-managed-keys":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": "cmk-1", "name": "MyKey", "status": "ready", "tenant_id": "t1",
						"instance_type": "enterprise-db", "cloud_provider": "gcp",
						"region": "europe-west1", "key_id": "projects/p/locations/eu/keyRings/r/cryptoKeys/k"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	created, err := c.CreateCustomerManagedKey(ctx, CreateCMKRequest{
		Name:          "MyKey",
		TenantID:      "t1",
		InstanceType:  "enterprise-db",
		CloudProvider: "gcp",
		Region:        "europe-west1",
		KeyID:         "projects/p/locations/eu/keyRings/r/cryptoKeys/k",
	})
	if err != nil {
		t.Fatalf("CreateCustomerManagedKey: %v", err)
	}
	if created.ID != "cmk-1" || !IsCMKPending(created.Status) {
		t.Errorf("created = %+v, want id=cmk-1 status=pending", created)
	}
	// Request body must carry the cloud KMS key id and placement.
	if createBody["key_id"] != "projects/p/locations/eu/keyRings/r/cryptoKeys/k" {
		t.Errorf("create body key_id = %v", createBody["key_id"])
	}
	if createBody["instance_type"] != "enterprise-db" {
		t.Errorf("create body instance_type = %v, want enterprise-db", createBody["instance_type"])
	}

	got, err := c.GetCustomerManagedKey(ctx, "cmk-1")
	if err != nil {
		t.Fatalf("GetCustomerManagedKey: %v", err)
	}
	if !IsCMKReady(got.Status) {
		t.Errorf("got status = %q, want ready", got.Status)
	}

	list, err := c.ListCustomerManagedKeys(ctx, "t1")
	if err != nil {
		t.Fatalf("ListCustomerManagedKeys: %v", err)
	}
	if len(list) != 1 || list[0].ID != "cmk-1" {
		t.Errorf("list = %+v, want single cmk-1", list)
	}
}

func TestCMKListTenantFilter(t *testing.T) {
	var gotQuery string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		gotQuery = r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if _, err := c.ListCustomerManagedKeys(context.Background(), "tenant-xyz"); err != nil {
		t.Fatalf("ListCustomerManagedKeys: %v", err)
	}
	if !strings.Contains(gotQuery, "tenantId=tenant-xyz") {
		t.Errorf("query = %q, want tenantId=tenant-xyz", gotQuery)
	}
}

func TestCMKDeleteTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{{"message": "not found", "reason": "cmk-not-found"}},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteCustomerManagedKey(context.Background(), "gone"); err != nil {
		t.Errorf("DeleteCustomerManagedKey on 404 = %v, want nil (idempotent)", err)
	}
}

func TestCMKDeleteActiveKeyIsCMKActive(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{
				{"message": "The encryption key is active", "reason": "encryption-key-is-active"},
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	err := c.DeleteCustomerManagedKey(context.Background(), "in-use")
	if err == nil {
		t.Fatal("DeleteCustomerManagedKey: expected error for active key, got nil")
	}
	if !IsCMKActive(err) {
		t.Errorf("IsCMKActive(%v) = false, want true", err)
	}
	// Must NOT be misclassified as a transient/retryable failure.
	if IsTransient(err) || IsNotFound(err) {
		t.Errorf("active-key 400 misclassified: %v", err)
	}
}

func TestCMKStatusHelpers(t *testing.T) {
	if !IsCMKReady("Ready") || !IsCMKReady(CMKStatusReady) {
		t.Error("ready not detected (case-tolerant)")
	}
	if !IsCMKPending("PENDING") {
		t.Error("pending not detected (case-tolerant)")
	}
	if IsCMKReady(CMKStatusPending) || IsCMKPending(CMKStatusReady) {
		t.Error("ready/pending helpers cross-matched")
	}
}

func TestUpgradeInstancePostsToUpgradePath(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.UpgradeInstance(context.Background(), "inst-1"); err != nil {
		t.Fatalf("UpgradeInstance: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/instances/inst-1/upgrade" {
		t.Errorf("upgrade call = %s %s, want POST /v1/instances/inst-1/upgrade", gotMethod, gotPath)
	}
}

func TestUpgradeInstanceConflictClassified(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{
				{"message": "operation in progress", "reason": "ongoing-database-operation"},
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	err := c.UpgradeInstance(context.Background(), "busy")
	if err == nil || !IsConflict(err) {
		t.Errorf("UpgradeInstance conflict = %v, want IsConflict", err)
	}
}
