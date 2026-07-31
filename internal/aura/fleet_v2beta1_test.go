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
	"net/http"
	"net/http/httptest"
	"testing"
)

// Pins the v2beta1 Fleet Manager contract against the published spec, in
// particular its MIXED envelope handling — which is the easiest thing here to
// get wrong, since most of v2beta1 is uniformly data-wrapped:
//
//	DATA-WRAPPED: EVERYTHING — every GET, both POSTs, and the PATCH
//	NO BODY:      DELETE deployment, DELETE token
//
// The published spec declares the single-deployment GET, POST deployments and
// POST/PATCH token as BARE and is wrong about all four. Fixtures below follow
// the LIVE API (full lifecycle exercised against a real project 2026-07-31),
// including the two field names where live disagrees with the spec
// (token.auto_rotated, Server.mode_constraint) and the fact that the shard /
// txn / lag / role / writer fields live ONLY on the per-server databases
// endpoint, not the deployment-level one.
func TestFleetDeploymentsAndTokens(t *testing.T) {
	const org, proj, dep = "org-1", "proj-1", "dep-1"
	base := "/v2beta1/organizations/" + org + "/projects/" + proj + "/fleet-manager/deployments"

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")

		// LIST is data-wrapped.
		case r.Method == http.MethodGet && r.URL.Path == base:
			_, _ = w.Write([]byte(`{"data":[{"id":"dep-1","name":"prod-cluster","status":"HEALTHY",` +
				`"connection_url":"neo4j+s://x","created_by":"u-1"}]}`))

		// POST deployments is DATA-WRAPPED (live), HTTP 200 not 201.
		case r.Method == http.MethodPost && r.URL.Path == base:
			_, _ = w.Write([]byte(`{"data":{"id":"dep-new"}}`))

		// Single GET is DATA-WRAPPED (live), and the token field is
		// `auto_rotated`, not the spec's `auto_rotate`.
		case r.Method == http.MethodGet && r.URL.Path == base+"/"+dep:
			_, _ = w.Write([]byte(`{"data":{"id":"dep-1","name":"prod-cluster",` +
				`"dbms":{"edition":"enterprise","metric_collection_enabled":true,"packaging":"tar"},` +
				`"token":{"id":"tok-1","auto_rotated":true,"claimed_by":"ABC",` +
				`"creation_time":"2026-07-01T00:00:00Z","claimed_time":"2026-07-01T00:05:00Z",` +
				`"expiry_time":"2027-07-01T00:00:00Z"}}}`))

		// Token POST/PATCH are DATA-WRAPPED (live) and carry the secret once.
		case r.Method == http.MethodPost && r.URL.Path == base+"/"+dep+"/token":
			_, _ = w.Write([]byte(`{"data":{"token":"minted-token-abc"}}`))
		case r.Method == http.MethodPatch && r.URL.Path == base+"/"+dep+"/token":
			_, _ = w.Write([]byte(`{"data":{"token":"rotated-token-def"}}`))
		case r.Method == http.MethodDelete && r.URL.Path == base+"/"+dep+"/token":
			w.WriteHeader(http.StatusNoContent)

		// Telemetry reads are data-wrapped.
		case r.Method == http.MethodGet && r.URL.Path == base+"/"+dep+"/servers":
			_, _ = w.Write([]byte(`{"data":[{"id":"srv-1","name":"server-0","address":"1.2.3.4:7687",` +
				`"mode_constraint":"PRIMARY","health":"Available","status":"online",` +
				`"jvm_version":"21.0.2","os_name":"Linux","os_arch":"amd64",` +
				`"plugin_version":"1.2.3","plugins":[{"name":"apoc","version":"5.26.0"}],` +
				`"license":{"state":"VALID","type":"COMMERCIAL"}}]}`))
		// Deployment-level databases: the `Database` shape — sizing and topology
		// counts, NO shard/txn/lag/role/writer.
		case r.Method == http.MethodGet && r.URL.Path == base+"/"+dep+"/databases":
			_, _ = w.Write([]byte(`{"data":[{"id":"db-1","name":"neo4j","store":"block-block-1.1",` +
				`"access":"read-write","default":true,"requested_status":"online",` +
				`"node_count":7,"relationship_count":3,` +
				`"current_primaries_count":1,"requested_primaries_count":1}]}`))
		// Per-server databases: the `ServerDatabase` shape — this is where the
		// shard / txn / lag / role / writer data actually lives.
		case r.Method == http.MethodGet && r.URL.Path == base+"/"+dep+"/servers/srv-1/databases":
			_, _ = w.Write([]byte(`{"data":[{"id":"sdb-1","server_id":"srv-1","name":"neo4j",` +
				`"role":"primary","writer":true,"current_status":"online","type":"standard",` +
				`"last_committed_txn":42,"replication_lag":0,` +
				`"graph_shards":["g1"],"property_shards":["p1","p2"]}]}`))

		case r.Method == http.MethodDelete && r.URL.Path == base+"/"+dep:
			w.WriteHeader(http.StatusNoContent)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	deps, err := c.ListDeployments(ctx, org, proj)
	if err != nil || len(deps) != 1 || deps[0].ID != "dep-1" {
		t.Fatalf("ListDeployments = %+v, err=%v (LIST must unwrap `data`)", deps, err)
	}
	if deps[0].Name != "prod-cluster" || deps[0].Status != "HEALTHY" {
		t.Errorf("deployment = %+v", deps[0])
	}

	id, err := c.CreateDeployment(ctx, org, proj, "new-cluster")
	if err != nil || id != "dep-new" {
		t.Errorf("CreateDeployment = %q, err=%v (POST IS data-wrapped — an empty id here means doV2JSON crept back)", id, err)
	}

	detail, err := c.GetDeployment(ctx, org, proj, dep)
	if err != nil || detail.ID != "dep-1" {
		t.Fatalf("GetDeployment = %+v, err=%v (single GET IS data-wrapped, despite the spec)", detail, err)
	}
	if detail.DBMS == nil || detail.DBMS.Edition != "enterprise" || !detail.DBMS.MetricCollectionEnabled {
		t.Errorf("DBMS = %+v", detail.DBMS)
	}
	// AutoRotate must decode from the LIVE name `auto_rotated`.
	if detail.Token == nil || !detail.Token.AutoRotate || detail.Token.ClaimedTime == "" {
		t.Errorf("Token metadata = %+v (AutoRotate must read `auto_rotated`)", detail.Token)
	}
	if detail.Token.ExpiryTime == "" || detail.Token.ID == "" {
		t.Errorf("token id/expiry not decoded: %+v", detail.Token)
	}

	// The minted token is the string that must reach
	// CALL fleetManagement.registerToken($token).
	tok, err := c.CreateDeploymentToken(ctx, org, proj, dep)
	if err != nil || tok != "minted-token-abc" {
		t.Errorf("CreateDeploymentToken = %q, err=%v", tok, err)
	}
	rot, err := c.RotateDeploymentToken(ctx, org, proj, dep)
	if err != nil || rot != "rotated-token-def" {
		t.Errorf("RotateDeploymentToken = %q, err=%v", rot, err)
	}
	if err := c.DeleteDeploymentToken(ctx, org, proj, dep); err != nil {
		t.Errorf("DeleteDeploymentToken: %v", err)
	}

	servers, err := c.ListDeploymentServers(ctx, org, proj, dep)
	if err != nil || len(servers) != 1 {
		t.Fatalf("ListDeploymentServers = %+v, err=%v", servers, err)
	}
	s := servers[0]
	// ModeConstraint must decode from the LIVE singular `mode_constraint`.
	if s.ModeConstraint != "PRIMARY" || s.JVMVersion != "21.0.2" {
		t.Errorf("server = %+v (ModeConstraint must read `mode_constraint`)", s)
	}
	if s.ID != "srv-1" || s.Health != "Available" || s.OSArch != "amd64" {
		t.Errorf("live-only server fields not decoded: %+v", s)
	}
	if s.License == nil || s.License.State != "VALID" || s.License.Type != "COMMERCIAL" {
		t.Errorf("license = %+v", s.License)
	}
	if len(s.Plugins) != 1 || s.Plugins[0].Name != "apoc" {
		t.Errorf("plugins = %+v", s.Plugins)
	}

	// Deployment-level list: sizing/topology only.
	dbs, err := c.ListDeploymentDatabases(ctx, org, proj, dep)
	if err != nil || len(dbs) != 1 {
		t.Fatalf("ListDeploymentDatabases = %+v, err=%v", dbs, err)
	}
	if dbs[0].Store != "block-block-1.1" || dbs[0].NodeCount != 7 || !dbs[0].Default {
		t.Errorf("deployment database = %+v", dbs[0])
	}

	// Per-server list: the operationally interesting fields.
	sdbs, err := c.ListServerDatabases(ctx, org, proj, dep, "srv-1")
	if err != nil || len(sdbs) != 1 {
		t.Fatalf("ListServerDatabases = %+v, err=%v", sdbs, err)
	}
	if !sdbs[0].Writer || sdbs[0].LastCommittedTxn != 42 || sdbs[0].Role != "primary" {
		t.Errorf("server database = %+v", sdbs[0])
	}
	if len(sdbs[0].PropertyShards) != 2 || len(sdbs[0].GraphShards) != 1 {
		t.Errorf("shards = graph %v property %v", sdbs[0].GraphShards, sdbs[0].PropertyShards)
	}

	if err := c.DeleteDeployment(ctx, org, proj, dep); err != nil {
		t.Errorf("DeleteDeployment: %v", err)
	}
}

// Both deletes are idempotent — a 404 means "already gone", which is success.
func TestFleetDeleteTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeToken(w, "tok")
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors":[{"message":"gone","reason":"not-found"}]}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, 1000)
	ctx := context.Background()
	if err := c.DeleteDeployment(ctx, "o", "p", "gone"); err != nil {
		t.Errorf("DeleteDeployment on 404 = %v, want nil", err)
	}
	if err := c.DeleteDeploymentToken(ctx, "o", "p", "gone"); err != nil {
		t.Errorf("DeleteDeploymentToken on 404 = %v, want nil", err)
	}
}

// Same class as TestCreateInstanceV2RejectsASuccessWithNoID: an empty deployment
// ID or token returned as success is the failure this file's landmine 1 describes
// — an empty external-ID annotation, and another deployment registered on every
// reconcile.
func TestFleetCreatesRejectEmptyIdentifiers(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth/token":
			writeToken(w, "tok")
		default:
			// Enveloped, 2xx, but carrying nothing useful.
			_, _ = w.Write([]byte(`{"data":{}}`))
		}
	}))
	defer srv.Close()
	c := newTestClient(t, srv, 1000)
	ctx := context.Background()

	if id, err := c.CreateDeployment(ctx, org, proj, "dep"); err == nil {
		t.Errorf("CreateDeployment returned id=%q with no error; an empty id must fail", id)
	}
	if tok, err := c.CreateDeploymentToken(ctx, org, proj, "dep-1"); err == nil {
		t.Errorf("CreateDeploymentToken returned %q with no error; an empty token must fail, or "+
			"ensureToken skips its PATCH fallback and stores nothing", tok)
	}
	if tok, err := c.RotateDeploymentToken(ctx, org, proj, "dep-1"); err == nil {
		t.Errorf("RotateDeploymentToken returned %q with no error; here it is worst of all — the old "+
			"token is already invalid and the replacement is unrecoverable", tok)
	}
}
