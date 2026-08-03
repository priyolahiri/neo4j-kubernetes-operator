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
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// ==========================================================================
// BETA — Aura API v2beta1 Fleet Manager (deployments + deployment tokens).
//
//   /organizations/{org}/projects/{project}/fleet-manager/deployments
//   .../deployments/{id}            (GET, DELETE)
//   .../deployments/{id}/token      (POST, PATCH, DELETE)
//   .../deployments/{id}/databases  (GET)
//   .../deployments/{id}/servers    (GET)
//
// WHY THIS EXISTS: fleet management has two halves. Aura mints a deployment
// token; the self-managed DBMS then claims it over Bolt with
// `CALL fleetManagement.registerToken($token)` — which is exactly what
// internal/neo4j/client.go already does. The spec's own token description says
// the returned token "should be applied to the deployment through
// 'call.fleetManagement.registerToken(\"token\")'". Without this client the
// operator can only consume a token a human pasted into a Secret; with it, the
// operator can mint one itself.
//
// Landmines:
//
//  1. EVERY endpoint is `{"data": …}`-wrapped — GETs, both POSTs and the PATCH.
//     Use doV2Data throughout; never doV2JSON. The published spec declares the
//     single-deployment GET, POST deployments and POST/PATCH token as BARE, and
//     it is WRONG about all four. Verified by exercising the full lifecycle
//     against a live Aura project on 2026-07-31; the observed shape is what the
//     client follows. Getting this wrong
//     is silent: the envelope decodes into the struct and every field lands
//     zero, with no error, so CreateDeployment returns an empty ID and
//     CreateDeploymentToken an empty token.
//
//  2. The POST deployments and POST/PATCH token REQUEST BODIES are not published
//     upstream. Now confirmed live: `{"name": "<=30 chars"}` for a deployment
//     and `{}` for both token calls are accepted.
//
//  3. POST and PATCH token are STRICTLY COMPLEMENTARY, and you cannot tell which
//     one applies:
//       POST  works only when the deployment has NO token; otherwise HTTP 500
//       PATCH works only when the deployment HAS a token; otherwise HTTP 500
//     Both failure modes are 500 with an internal-sounding message
//     ("failed to create api key: … no rows in result set"), NOT a 4xx. And
//     GET deployment does NOT reveal which state you are in — see landmine 4.
//     So the only reliable sequence is: try POST, and on failure fall back to
//     PATCH. Beware that `IsTransient` treats 5xx as retryable, so a caller that
//     does not implement the fallback will retry forever without progressing.
//
//  4. `DetailedDeployment.token` is ABSENT until the token has been CLAIMED by a
//     running DBMS. A deployment that has a freshly-minted, never-used token
//     reports `token: null` and `dbms: null`, indistinguishable from one with no
//     token at all. Never use token presence to decide whether to mint.
//
//  5. DELETE token on a deployment with no token returns HTTP 500 (not 404), so
//     it is NOT idempotent in the usual sense — IsNotFound will not catch it.
//
//  6. FIELD NAMES WHERE THE LIVE API AND THE SPEC DIFFER (observed values used, all
//     verified 2026-07-30):
//       token.auto_rotate   (spec)  ->  token.auto_rotated   (live)
//       Server.mode_constraints (spec) -> Server.mode_constraint (live, singular)
//     Using the spec spelling silently yields a zero value forever.
//
//  7. TWO DIFFERENT DATABASE SHAPES on two different endpoints — do not mix
//     them up (this cost a round of rework):
//       .../deployments/{id}/databases            -> schema `Database`
//           store / access / node+relationship counts / primaries+secondaries
//           counts / requested_status. NO shard, txn, lag, role or writer data.
//       .../deployments/{id}/servers/{sid}/databases -> schema `ServerDatabase`
//           role / writer / last_committed_txn / replication_lag /
//           graph_shards / property_shards.
//     The operator's telemetry wants the SECOND one, so it must go per-server.
// ==========================================================================

// DeploymentToken is the token half of a Fleet Manager deployment. All fields are
// read-only metadata except the token string itself, which is returned ONLY by
// the create/rotate calls and never read back afterwards.
//
// NOTE the json tag on AutoRotate: the spec calls it `auto_rotate`, the live API
// returns `auto_rotated`. ID and ClaimedBy are live-only (undocumented).
type DeploymentToken struct {
	ID           string `json:"id,omitempty"`
	AutoRotate   bool   `json:"auto_rotated,omitempty"`
	ClaimedBy    string `json:"claimed_by,omitempty"`
	ClaimedTime  string `json:"claimed_time,omitempty"`
	CreatedBy    string `json:"created_by,omitempty"`
	CreationTime string `json:"creation_time,omitempty"`
	ExpiryTime   string `json:"expiry_time,omitempty"`
	LastUsedTime string `json:"last_used_time,omitempty"`
	ReleaseTime  string `json:"release_time,omitempty"`
}

// Deployment is a Fleet Manager deployment summary (LIST shape).
type Deployment struct {
	ID            string `json:"id,omitempty"`
	Name          string `json:"name,omitempty"`
	Status        string `json:"status,omitempty"`
	ConnectionURL string `json:"connection_url,omitempty"`
	CreatedBy     string `json:"created_by,omitempty"`
}

// DeploymentDBMS describes the DBMS behind a deployment.
type DeploymentDBMS struct {
	Edition                 string `json:"edition,omitempty"`
	MetricCollectionEnabled bool   `json:"metric_collection_enabled,omitempty"`
	Packaging               string `json:"packaging,omitempty"`
}

// DetailedDeployment is the single-GET shape: a Deployment plus its DBMS and
// token metadata.
type DetailedDeployment struct {
	ID            string           `json:"id,omitempty"`
	Name          string           `json:"name,omitempty"`
	ConnectionURL string           `json:"connection_url,omitempty"`
	CreatedBy     string           `json:"created_by,omitempty"`
	DBMS          *DeploymentDBMS  `json:"dbms,omitempty"`
	Token         *DeploymentToken `json:"token,omitempty"`
}

// License is a server's Neo4j license state.
type License struct {
	State           string `json:"state,omitempty"`
	Type            string `json:"type,omitempty"`
	DaysLeftOnTrial int    `json:"days_left_on_trial,omitempty"`
	TotalTrialDays  int    `json:"total_trial_days,omitempty"`
}

// ServerPlugin is one plugin installed on a fleet-managed server.
type ServerPlugin struct {
	Name     string `json:"name,omitempty"`
	Filename string `json:"filename,omitempty"`
	Version  string `json:"version,omitempty"`
}

// FleetServer is one DBMS server reported to Fleet Manager. ModeConstraint
// corresponds to the operator's own serverModeConstraint concept.
//
// NOTE the json tag on ModeConstraint: the spec calls it `mode_constraints`
// (plural), the live API returns `mode_constraint`. ID, DeploymentID, Health and
// OSArch are live-only (undocumented); Health ("Available") is distinct from
// Status ("offline").
type FleetServer struct {
	ID             string         `json:"id,omitempty"`
	DeploymentID   string         `json:"deployment_id,omitempty"`
	Name           string         `json:"name,omitempty"`
	Address        string         `json:"address,omitempty"`
	State          string         `json:"state,omitempty"`
	Status         string         `json:"status,omitempty"`
	Health         string         `json:"health,omitempty"`
	Version        string         `json:"version,omitempty"`
	ModeConstraint string         `json:"mode_constraint,omitempty"`
	LastPing       string         `json:"last_ping,omitempty"`
	JVMVendor      string         `json:"jvm_vendor,omitempty"`
	JVMVersion     string         `json:"jvm_version,omitempty"`
	OSName         string         `json:"os_name,omitempty"`
	OSVersion      string         `json:"os_version,omitempty"`
	OSArch         string         `json:"os_arch,omitempty"`
	PluginVersion  string         `json:"plugin_version,omitempty"`
	Plugins        []ServerPlugin `json:"plugins,omitempty"`
	License        *License       `json:"license,omitempty"`
}

// FleetDeploymentDatabase is one database as reported at the DEPLOYMENT level
// (schema `Database`, from .../deployments/{id}/databases).
//
// This shape is about sizing and requested-vs-current topology. It carries NO
// shard, transaction, lag, role or writer data — for those use
// FleetServerDatabase via ListServerDatabases. ID, Name and DeploymentID are
// live-only additions to the documented schema.
type FleetDeploymentDatabase struct {
	ID                        string   `json:"id,omitempty"`
	Name                      string   `json:"name,omitempty"`
	DeploymentID              string   `json:"deployment_id,omitempty"`
	Access                    string   `json:"access,omitempty"`
	Aliases                   []string `json:"aliases,omitempty"`
	Default                   bool     `json:"default,omitempty"`
	Store                     string   `json:"store,omitempty"`
	RequestedStatus           string   `json:"requested_status,omitempty"`
	CreationTime              string   `json:"creation_time,omitempty"`
	LastStartTime             string   `json:"last_start_time,omitempty"`
	NodeCount                 int64    `json:"node_count,omitempty"`
	RelationshipCount         int64    `json:"relationship_count,omitempty"`
	CurrentPrimariesCount     int      `json:"current_primaries_count,omitempty"`
	CurrentSecondariesCount   int      `json:"current_secondaries_count,omitempty"`
	RequestedPrimariesCount   int      `json:"requested_primaries_count,omitempty"`
	RequestedSecondariesCount int      `json:"requested_secondaries_count,omitempty"`
}

// FleetServerDatabase is one database as reported by a SPECIFIC server (schema
// `ServerDatabase`, from .../servers/{server_id}/databases).
//
// This is the shape carrying the operationally interesting fields: role, writer,
// last committed transaction, replication lag, and the graph/property shards that
// line up with the operator's property-sharding model. GraphShards and
// PropertyShards are null for a non-sharded database. ID, ServerID, DeploymentID
// and LastSeen are live-only additions to the documented schema.
type FleetServerDatabase struct {
	ID               string   `json:"id,omitempty"`
	ServerID         string   `json:"server_id,omitempty"`
	DeploymentID     string   `json:"deployment_id,omitempty"`
	Name             string   `json:"name,omitempty"`
	Type             string   `json:"type,omitempty"`
	Role             string   `json:"role,omitempty"`
	CurrentStatus    string   `json:"current_status,omitempty"`
	StatusMessage    string   `json:"status_message,omitempty"`
	Writer           bool     `json:"writer,omitempty"`
	LastCommittedTxn int64    `json:"last_committed_txn,omitempty"`
	ReplicationLag   int64    `json:"replication_lag,omitempty"`
	LastSeen         string   `json:"last_seen,omitempty"`
	GraphShards      []string `json:"graph_shards,omitempty"`
	PropertyShards   []string `json:"property_shards,omitempty"`
}

// CreateDeploymentRequest is the POST deployments body. UNPUBLISHED upstream —
// best-effort. The endpoint's 400 caps the name at 30 characters.
type CreateDeploymentRequest struct {
	Name string `json:"name"`
}

// deploymentTokenResponse is the shape both POST and PATCH token return: a bare object
// carrying the token string (no `data` envelope).
type deploymentTokenResponse struct {
	Token string `json:"token"`
}

func fleetDeploymentsPath(orgID, projectID string) string {
	return "/organizations/" + url.PathEscape(orgID) +
		"/projects/" + url.PathEscape(projectID) + "/fleet-manager/deployments"
}

func fleetDeploymentPath(orgID, projectID, deploymentID string) string {
	return fleetDeploymentsPath(orgID, projectID) + "/" + url.PathEscape(deploymentID)
}

// ---- Deployments ----------------------------------------------------------

// ListDeployments lists a project's Fleet Manager deployments (v2beta1, beta).
// Data-wrapped.
func (c *Client) ListDeployments(ctx context.Context, orgID, projectID string) ([]Deployment, error) {
	var out []Deployment
	if err := c.doV2Data(ctx, http.MethodGet, fleetDeploymentsPath(orgID, projectID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing fleet deployments: %w", err)
	}
	return out, nil
}

// GetDeployment returns one deployment with its DBMS + token metadata (v2beta1,
// beta).
//
// Data-wrapped, despite the spec declaring this response BARE. Verified against
// the live API 2026-07-30: it returns {"data": {...}}. Using doV2JSON here (as
// the spec implies) decodes the envelope into the struct and yields all-zero
// fields with no error at all.
func (c *Client) GetDeployment(ctx context.Context, orgID, projectID, deploymentID string) (*DetailedDeployment, error) {
	var out DetailedDeployment
	if err := c.doV2Data(ctx, http.MethodGet, fleetDeploymentPath(orgID, projectID, deploymentID), nil, &out); err != nil {
		return nil, fmt.Errorf("getting fleet deployment %q: %w", deploymentID, err)
	}
	return &out, nil
}

// CreateDeployment registers a new deployment and returns its ID (v2beta1, beta).
//
// Data-wrapped (`{"data":{"id":…}}`, HTTP 200 — not the bare 201 the spec
// declares). Verified live 2026-07-31. With doV2JSON the envelope decodes into
// the struct and the ID comes back EMPTY with no error, which then silently
// orphans the deployment it just created.
func (c *Client) CreateDeployment(ctx context.Context, orgID, projectID, name string) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	body := CreateDeploymentRequest{Name: name}
	if err := c.doV2Data(ctx, http.MethodPost, fleetDeploymentsPath(orgID, projectID), body, &out); err != nil {
		return "", fmt.Errorf("creating fleet deployment %q: %w", name, err)
	}
	// An empty ID must be an error, never a success. doV2Data treats a missing or
	// null `data` field as a no-op, so a 2xx with an unexpected envelope decodes to
	// a zero struct with no error — and the caller would then store an EMPTY
	// external-ID annotation and register another deployment on every reconcile.
	// That is the exact failure this file's landmine 1 documents; the envelope fix
	// removed the known cause, and this closes the class.
	if strings.TrimSpace(out.ID) == "" {
		return "", fmt.Errorf("creating fleet deployment %q: Aura returned success with no deployment id", name)
	}
	return out.ID, nil
}

// DeleteDeployment unregisters a deployment (v2beta1, beta). Idempotent: a 404 is
// treated as success.
func (c *Client) DeleteDeployment(ctx context.Context, orgID, projectID, deploymentID string) error {
	if err := c.doV2JSON(ctx, http.MethodDelete, fleetDeploymentPath(orgID, projectID, deploymentID), nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting fleet deployment %q: %w", deploymentID, err)
	}
	return nil
}

// ---- Deployment tokens ----------------------------------------------------

// CreateDeploymentToken mints the deployment's first token and returns it
// (v2beta1, beta).
//
// The returned string is what must be handed to
// `CALL fleetManagement.registerToken($token)` on the DBMS. It is returned ONLY
// here — persist it immediately. Aura 400s if the deployment already has a
// token; use RotateDeploymentToken in that case.
func (c *Client) CreateDeploymentToken(ctx context.Context, orgID, projectID, deploymentID string) (string, error) {
	var out deploymentTokenResponse
	path := fleetDeploymentPath(orgID, projectID, deploymentID) + "/token"
	if err := c.doV2Data(ctx, http.MethodPost, path, struct{}{}, &out); err != nil {
		return "", fmt.Errorf("creating token for fleet deployment %q: %w", deploymentID, err)
	}
	// Same reasoning as CreateDeployment: an empty token is not a success. Here it
	// also matters that the probe-then-fallback logic in ensureToken keys off this
	// call succeeding — a silent "" would make it skip the PATCH fallback and store
	// nothing.
	if strings.TrimSpace(out.Token) == "" {
		return "", fmt.Errorf("creating token for fleet deployment %q: Aura returned success with no token", deploymentID)
	}
	return out.Token, nil
}

// RotateDeploymentToken replaces the deployment's token and returns the new one
// (v2beta1, beta). Same one-shot caveat as CreateDeploymentToken.
//
// Returns HTTP 500 if the deployment has NO token yet, so this is the fallback
// half of the POST-then-PATCH pair, never a drop-in replacement for it.
func (c *Client) RotateDeploymentToken(ctx context.Context, orgID, projectID, deploymentID string) (string, error) {
	var out deploymentTokenResponse
	path := fleetDeploymentPath(orgID, projectID, deploymentID) + "/token"
	if err := c.doV2Data(ctx, http.MethodPatch, path, struct{}{}, &out); err != nil {
		return "", fmt.Errorf("rotating token for fleet deployment %q: %w", deploymentID, err)
	}
	// A rotation that reports success with no token is worse than a failure: the
	// old token is already invalid and the replacement is unrecoverable.
	if strings.TrimSpace(out.Token) == "" {
		return "", fmt.Errorf("rotating token for fleet deployment %q: Aura returned success with no token; "+
			"the previous token may already be invalid", deploymentID)
	}
	return out.Token, nil
}

// DeleteDeploymentToken revokes the deployment's token (v2beta1, beta).
//
// A 404 is treated as success, but note this is NOT reliably idempotent: Aura
// returns HTTP 500 (not 404) when the deployment has no token, and IsNotFound
// will not catch that. Callers should treat any failure here as non-fatal.
func (c *Client) DeleteDeploymentToken(ctx context.Context, orgID, projectID, deploymentID string) error {
	path := fleetDeploymentPath(orgID, projectID, deploymentID) + "/token"
	if err := c.doV2JSON(ctx, http.MethodDelete, path, nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting token for fleet deployment %q: %w", deploymentID, err)
	}
	return nil
}

// ---- Deployment telemetry -------------------------------------------------

// ListDeploymentServers lists the servers reporting to a deployment (v2beta1,
// beta). Data-wrapped.
func (c *Client) ListDeploymentServers(ctx context.Context, orgID, projectID, deploymentID string) ([]FleetServer, error) {
	var out []FleetServer
	path := fleetDeploymentPath(orgID, projectID, deploymentID) + "/servers"
	if err := c.doV2Data(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("listing servers for fleet deployment %q: %w", deploymentID, err)
	}
	return out, nil
}

// ListDeploymentDatabases lists a deployment's databases at the DEPLOYMENT level
// (v2beta1, beta). Data-wrapped.
//
// Returns the `Database` shape: store, access, node/relationship counts, and
// requested-vs-current topology counts. For role / writer / transaction /
// replication-lag / shard data, use ListServerDatabases instead — that data
// exists only on the per-server endpoint.
func (c *Client) ListDeploymentDatabases(ctx context.Context, orgID, projectID, deploymentID string) ([]FleetDeploymentDatabase, error) {
	var out []FleetDeploymentDatabase
	path := fleetDeploymentPath(orgID, projectID, deploymentID) + "/databases"
	if err := c.doV2Data(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("listing databases for fleet deployment %q: %w", deploymentID, err)
	}
	return out, nil
}

// ListServerDatabases lists the databases as reported by ONE server of a
// deployment (v2beta1, beta). Data-wrapped.
//
// This is the endpoint that carries role, writer, last_committed_txn,
// replication_lag and the graph/property shards.
func (c *Client) ListServerDatabases(ctx context.Context, orgID, projectID, deploymentID, serverID string) ([]FleetServerDatabase, error) {
	var out []FleetServerDatabase
	path := fleetDeploymentPath(orgID, projectID, deploymentID) +
		"/servers/" + url.PathEscape(serverID) + "/databases"
	if err := c.doV2Data(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("listing databases for fleet server %q: %w", serverID, err)
	}
	return out, nil
}
