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
	"sync/atomic"
	"testing"
	"time"
)

// newTestClient builds a Client pointed at srv with a generous rate limit so
// tests aren't throttled unless they explicitly set PerMinute low.
func newTestClient(t *testing.T, srv *httptest.Server, perMinute int) *Client {
	t.Helper()
	return NewClient(Config{
		BaseURL:      srv.URL + "/v1",
		TokenURL:     srv.URL + "/oauth/token",
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		PerMinute:    perMinute,
		HTTPClient:   srv.Client(),
	})
}

// writeToken writes a standard OAuth2 token response.
func writeToken(w http.ResponseWriter, token string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tokenResponse{
		AccessToken: token,
		TokenType:   "bearer",
		ExpiresIn:   3600,
	})
}

func TestTokenExchange(t *testing.T) {
	var (
		gotBasicUser, gotBasicPass string
		gotGrantType               string
		gotContentType             string
		gotAuthHeader              string
	)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			gotBasicUser, gotBasicPass, _ = r.BasicAuth()
			gotContentType = r.Header.Get("Content-Type")
			body, _ := io.ReadAll(r.Body)
			// application/x-www-form-urlencoded body
			gotGrantType = extractForm(string(body), "grant_type")
			writeToken(w, "access-123")
		case "/v1/instances/abc":
			gotAuthHeader = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"id": "abc", "name": "n", "status": "running"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if _, err := c.GetInstance(context.Background(), "abc"); err != nil {
		t.Fatalf("GetInstance: %v", err)
	}

	if gotBasicUser != "test-client" || gotBasicPass != "test-secret" {
		t.Errorf("basic auth = %q:%q, want test-client:test-secret", gotBasicUser, gotBasicPass)
	}
	if gotGrantType != "client_credentials" {
		t.Errorf("grant_type = %q, want client_credentials", gotGrantType)
	}
	if gotContentType != "application/x-www-form-urlencoded" {
		t.Errorf("token Content-Type = %q, want application/x-www-form-urlencoded", gotContentType)
	}
	if gotAuthHeader != "Bearer access-123" {
		t.Errorf("API Authorization = %q, want Bearer access-123", gotAuthHeader)
	}
}

func TestTokenCachedAcrossCalls(t *testing.T) {
	var tokenCalls int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			atomic.AddInt32(&tokenCalls, 1)
			writeToken(w, "cached-tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "x"}})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	for i := 0; i < 3; i++ {
		if _, err := c.GetInstance(context.Background(), "x"); err != nil {
			t.Fatalf("GetInstance #%d: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&tokenCalls); got != 1 {
		t.Errorf("token endpoint called %d times, want 1 (should cache)", got)
	}
}

func TestCreateInstanceParsesOneTimePassword(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/instances" {
			http.NotFound(w, r)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("create Content-Type = %q, want application/json", ct)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"id":             "db1d1234",
				"connection_url": "neo4j+s://db1d1234.databases.neo4j.io",
				"username":       "neo4j",
				"password":       "letMeIn123!",
				"tenant_id":      "tenant-1",
				"cloud_provider": "gcp",
				"region":         "europe-west1",
				"type":           "enterprise-db",
				"name":           "Instance01",
				"created_at":     "2023-01-20T13:44:42Z",
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	resp, err := c.CreateInstance(context.Background(), CreateInstanceRequest{
		Version:       "5",
		Region:        "europe-west1",
		Memory:        "2GB",
		Name:          "Instance01",
		Type:          "enterprise-db",
		TenantID:      "tenant-1",
		CloudProvider: "gcp",
	})
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	if resp.Password != "letMeIn123!" {
		t.Errorf("password = %q, want letMeIn123!", resp.Password)
	}
	if resp.Username != "neo4j" {
		t.Errorf("username = %q, want neo4j", resp.Username)
	}
	if resp.ID != "db1d1234" {
		t.Errorf("id = %q, want db1d1234", resp.ID)
	}
}

func TestGetInstanceUnwrapsData(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"id":             "2f49c2b3",
				"name":           "Production",
				"status":         "running",
				"connection_url": "neo4j+s://2f49c2b3.databases.neo4j.io",
				"tenant_id":      "tenant-1",
				"cloud_provider": "gcp",
				"region":         "europe-west1",
				"type":           "enterprise-db",
				"memory":         "8GB",
				"storage":        "16GB",
				"created_at":     "2023-01-20T13:44:42Z",
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	inst, err := c.GetInstance(context.Background(), "2f49c2b3")
	if err != nil {
		t.Fatalf("GetInstance: %v", err)
	}
	if inst.Name != "Production" || inst.Status != "running" || inst.Memory != "8GB" {
		t.Errorf("unwrapped instance = %+v, want Production/running/8GB", inst)
	}
	if !IsInstanceRunning(inst.Status) {
		t.Errorf("IsInstanceRunning(%q) = false, want true", inst.Status)
	}
}

func TestDeleteInstanceTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{
				{"message": "DB not found: gone", "reason": "db-not-found"},
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	if err := c.DeleteInstance(context.Background(), "gone"); err != nil {
		t.Errorf("DeleteInstance on 404 = %v, want nil (idempotent)", err)
	}
}

func TestConflictMapsToIsConflict(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "req-conflict-1")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{
				{"message": "The database is currently undergoing an operation: resuming",
					"reason": "ongoing-database-operation"},
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	err := c.PauseInstance(context.Background(), "busy")
	if err == nil {
		t.Fatal("PauseInstance: expected error, got nil")
	}
	if !IsConflict(err) {
		t.Errorf("IsConflict(%v) = false, want true", err)
	}
	if IsNotFound(err) || IsTransient(err) {
		t.Errorf("409 misclassified as notfound/transient: %v", err)
	}
	// X-Request-Id should be captured.
	if !strings.Contains(err.Error(), "req-conflict-1") {
		t.Errorf("error %q does not carry request id", err.Error())
	}
}

func TestForbiddenTriggersTokenRefreshAndRetry(t *testing.T) {
	var (
		tokenCalls int32
		apiCalls   int32
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			n := atomic.AddInt32(&tokenCalls, 1)
			writeToken(w, "token-v"+itoa(int(n)))
			return
		}
		n := atomic.AddInt32(&apiCalls, 1)
		if n == 1 {
			// First call: pretend the (first) token is expired -> 403.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"errors": []map[string]any{
					{"message": "token expired", "reason": "token-expired"},
				},
			})
			return
		}
		// Retry with the refreshed token should carry the new bearer.
		if auth := r.Header.Get("Authorization"); auth != "Bearer token-v2" {
			t.Errorf("retry Authorization = %q, want Bearer token-v2", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"id": "ok", "name": "n", "status": "running"},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	inst, err := c.GetInstance(context.Background(), "ok")
	if err != nil {
		t.Fatalf("GetInstance after refresh: %v", err)
	}
	if inst.ID != "ok" {
		t.Errorf("instance id = %q, want ok", inst.ID)
	}
	if got := atomic.LoadInt32(&tokenCalls); got != 2 {
		t.Errorf("token endpoint called %d times, want 2 (initial + refresh on 403)", got)
	}
	if got := atomic.LoadInt32(&apiCalls); got != 2 {
		t.Errorf("API called %d times, want 2 (403 then retry)", got)
	}
}

func TestBothErrorBodyShapesParse(t *testing.T) {
	t.Run("errors-array shape", func(t *testing.T) {
		body := []byte(`{"errors":[{"message":"bad thing","reason":"some-reason","field":"memory"}]}`)
		apiErr := newAPIError(http.StatusBadRequest, "rid-1", body)
		if apiErr.Reason != "some-reason" {
			t.Errorf("reason = %q, want some-reason", apiErr.Reason)
		}
		if !strings.Contains(apiErr.Message, "bad thing") {
			t.Errorf("message = %q, want to contain 'bad thing'", apiErr.Message)
		}
		if !strings.Contains(apiErr.Message, "memory") {
			t.Errorf("message = %q, want to contain field 'memory'", apiErr.Message)
		}
		if apiErr.RequestID != "rid-1" {
			t.Errorf("request id = %q, want rid-1", apiErr.RequestID)
		}
	})

	t.Run("gateway error shape", func(t *testing.T) {
		body := []byte(`{"error":"gateway exploded"}`)
		apiErr := newAPIError(http.StatusBadGateway, "rid-2", body)
		if apiErr.Reason != "" {
			t.Errorf("reason = %q, want empty for gateway shape", apiErr.Reason)
		}
		if apiErr.Message != "gateway exploded" {
			t.Errorf("message = %q, want 'gateway exploded'", apiErr.Message)
		}
		if !IsTransient(apiErr) {
			t.Errorf("502 IsTransient = false, want true")
		}
	})

	t.Run("empty body", func(t *testing.T) {
		apiErr := newAPIError(http.StatusInternalServerError, "", nil)
		if apiErr.Message == "" {
			t.Error("empty-body error should still carry a message")
		}
	})
}

func TestRateLimiterWaitInvoked(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "x"}})
	}))
	defer srv.Close()

	// PerMinute=60 => 1 req/sec, burst 1. The first request consumes the burst
	// immediately; the second must wait ~1s for the limiter to refill.
	c := newTestClient(t, srv, 60)

	ctx := context.Background()
	if _, err := c.GetInstance(ctx, "x"); err != nil {
		t.Fatalf("first GetInstance: %v", err)
	}
	start := time.Now()
	if _, err := c.GetInstance(ctx, "x"); err != nil {
		t.Fatalf("second GetInstance: %v", err)
	}
	elapsed := time.Since(start)
	// Allow slack for timer granularity; the limiter must impose a meaningful delay.
	if elapsed < 500*time.Millisecond {
		t.Errorf("second request returned after %v, expected rate-limiter delay ~1s", elapsed)
	}
}

func TestRateLimiterRespectsContextCancellation(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "x"}})
	}))
	defer srv.Close()

	// Very low rate so the second call would block for a long time.
	c := newTestClient(t, srv, 1)
	ctx := context.Background()
	if _, err := c.GetInstance(ctx, "x"); err != nil {
		t.Fatalf("first GetInstance: %v", err)
	}

	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := c.GetInstance(cctx, "x"); err == nil {
		t.Error("expected context deadline error from rate limiter, got nil")
	}
}

func TestListInstancesTenantFilter(t *testing.T) {
	var gotQuery string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"id": "a", "name": "A", "tenant_id": "t1", "cloud_provider": "gcp", "created_at": "x"},
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	list, err := c.ListInstances(context.Background(), "t1")
	if err != nil {
		t.Fatalf("ListInstances: %v", err)
	}
	if len(list) != 1 || list[0].ID != "a" {
		t.Errorf("list = %+v, want single instance id=a", list)
	}
	if !strings.Contains(gotQuery, "tenantId=t1") {
		t.Errorf("query = %q, want tenantId=t1", gotQuery)
	}
}

func TestGetTenantReturnsInstanceConfigurations(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"id":   "tenant-1",
				"name": "Production",
				"instance_configurations": []map[string]any{
					{"region": "europe-west1", "region_name": "Belgium", "type": "enterprise-db",
						"memory": "8GB", "storage": "16GB", "version": "5", "cloud_provider": "gcp"},
				},
			},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	tenant, err := c.GetTenant(context.Background(), "tenant-1")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if len(tenant.InstanceConfigurations) != 1 {
		t.Fatalf("instance_configurations len = %d, want 1", len(tenant.InstanceConfigurations))
	}
	cfg := tenant.InstanceConfigurations[0]
	if cfg.Memory != "8GB" || cfg.Type != "enterprise-db" || cfg.Version != "5" {
		t.Errorf("config = %+v, want 8GB/enterprise-db/5", cfg)
	}
}

func TestSnapshotLifecycle(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		case r.Method == http.MethodPost && r.URL.Path == "/v1/instances/i1/snapshots":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"snapshot_id": "snap-1"},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/instances/i1/snapshots/snap-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"instance_id": "i1", "snapshot_id": "snap-1",
					"profile": "AdHoc", "status": "Completed",
					"timestamp": "2023-01-20T13:44:42Z", "exportable": true,
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/instances/i1/snapshots":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"instance_id": "i1", "snapshot_id": "snap-1", "profile": "Scheduled",
						"status": "Completed", "timestamp": "2023-01-20T13:44:42Z"},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/instances/i1/snapshots/snap-1/restore":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"id": "i1", "status": "restoring"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	snap, err := c.CreateSnapshot(ctx, "i1")
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.SnapshotID != "snap-1" || snap.InstanceID != "i1" {
		t.Errorf("created snapshot = %+v, want snap-1/i1", snap)
	}

	got, err := c.GetSnapshot(ctx, "i1", "snap-1")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Status != SnapshotStatusCompleted || !got.Exportable {
		t.Errorf("got snapshot = %+v, want Completed/exportable", got)
	}

	list, err := c.ListSnapshots(ctx, "i1", "")
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(list) != 1 || list[0].SnapshotID != "snap-1" {
		t.Errorf("list = %+v, want single snap-1", list)
	}

	if err := c.RestoreSnapshot(ctx, "i1", "snap-1"); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
}

func TestPatchInstanceOmitsUnsetFields(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"id": "i1", "name": "NewName", "status": "updating"},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	name := "NewName"
	if _, err := c.PatchInstance(context.Background(), "i1", PatchInstanceRequest{Name: &name}); err != nil {
		t.Fatalf("PatchInstance: %v", err)
	}
	if _, ok := gotBody["memory"]; ok {
		t.Errorf("patch body included unset memory field: %+v", gotBody)
	}
	if gotBody["name"] != "NewName" {
		t.Errorf("patch body name = %v, want NewName", gotBody["name"])
	}
}

func TestNonHTTPSRejected(t *testing.T) {
	c := NewClient(Config{
		BaseURL:      "http://insecure.example.com/v1",
		TokenURL:     "http://insecure.example.com/oauth/token",
		ClientID:     "id",
		ClientSecret: "secret",
		PerMinute:    1000,
	})
	// Populate a token directly to skip the (also-http) token fetch and reach
	// the HTTPS guard in doJSONOnce.
	c.accessToken = "fake"
	c.tokenExpiry = time.Now().Add(time.Hour)

	_, err := c.GetInstance(context.Background(), "x")
	if err == nil || !strings.Contains(err.Error(), "non-HTTPS") {
		t.Errorf("expected non-HTTPS rejection, got %v", err)
	}
}

func TestDefaultsApplied(t *testing.T) {
	c := NewClient(Config{})
	if c.baseURL != DefaultBaseURL {
		t.Errorf("baseURL = %q, want %q", c.baseURL, DefaultBaseURL)
	}
	if c.tokenURL != DefaultTokenURL {
		t.Errorf("tokenURL = %q, want %q", c.tokenURL, DefaultTokenURL)
	}
	if c.httpClient == nil {
		t.Error("httpClient default not applied")
	}
	if c.limiter == nil {
		t.Error("limiter not constructed")
	}
	// DefaultPerMinute => limit of 25/60 per second.
	if got := float64(c.limiter.Limit()); got < 0.4 || got > 0.42 {
		t.Errorf("limiter limit = %v, want ~%v", got, float64(DefaultPerMinute)/60.0)
	}
}

func TestIsInstanceStatusHelpers(t *testing.T) {
	if !IsInstanceRunning(InstanceStatusRunning) {
		t.Error("running not detected")
	}
	if !IsInstancePaused(InstanceStatusPaused) {
		t.Error("paused not detected")
	}
	transient := []string{
		InstanceStatusCreating, InstanceStatusPausing, InstanceStatusResuming,
		InstanceStatusUpdating, InstanceStatusRestoring, InstanceStatusDestroying,
		InstanceStatusOverwriting, InstanceStatusLoading,
	}
	for _, s := range transient {
		if !IsInstanceTransient(s) {
			t.Errorf("IsInstanceTransient(%q) = false, want true", s)
		}
	}
	for _, s := range []string{InstanceStatusRunning, InstanceStatusPaused} {
		if IsInstanceTransient(s) {
			t.Errorf("IsInstanceTransient(%q) = true, want false", s)
		}
	}
}

// --- small helpers (avoid importing strconv/strings needlessly in table code) ---

func extractForm(body, key string) string {
	for _, pair := range strings.Split(body, "&") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 && kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
