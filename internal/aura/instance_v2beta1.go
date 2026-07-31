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
// BETA — Aura API v2beta1 instances.
//
// This exists for ONE capability v1 cannot express: `multi_database`. A
// multi-database Aura instance can only be created through the v2beta1 create
// body, and the flag is fixed at creation — there is no API to convert an
// existing instance. Every other instance operation stays on v1 (see
// instances.go), which the operator continues to use for observe/resize/
// pause/resume/upgrade/delete.
//
// Landmines, all verified against the LIVE API on 2026-07-31 (the v2beta1 spec
// publishes NO schema and NO requestBody for these paths — the shapes below come
// from live responses and from the API's own validation errors):
//
//  1. The TIER VOCABULARY IS DIFFERENT from v1. v2beta1 accepts exactly
//     'free' | 'professional' | 'business-critical' | 'virtual-dedicated-cloud'
//     (quoted verbatim from the API's 400 body). v1 uses free-db /
//     professional-db / business-critical / enterprise-db. Use
//     InstanceTypeV2 to translate; never pass a v1 tier name through.
//
//  2. Create takes NO `version` and NO `tenant_id`/`project_id` — the project is
//     in the PATH, and Aura picks the Neo4j version itself. Required fields are
//     only `name` and `type`; `memory`, `region` and `cloud_provider` are
//     accepted but not required.
//
//  3. UNKNOWN REQUEST FIELDS ARE SILENTLY IGNORED — verified by POSTing
//     `totally_unknown_field` and getting back only the (deliberate) `type`
//     error. So sending v1-only fields (storage, secondaries_count,
//     cdc_enrichment_mode, customer_managed_key_id, vector_optimized,
//     graph_analytics_plugin, source_*) does NOT fail: they are DROPPED. Callers
//     must refuse such combinations themselves rather than let the user believe
//     they took effect.
//
//  4. The GET DETAIL shape is not the v1 Instance shape: the status field is
//     `legacy_status` (NOT `status`), graph analytics is the STRING
//     `graph_analytics` (NOT the v1 `graph_analytics_plugin` bool), and there is
//     no `connection_url`, `tenant_id`/`project_id`, `created_at` or
//     `metrics_integration_url`. `multi_database` appears here and ONLY here.
//
//  5. The LIST shape carries only id/name/cloud_provider/created_at — NO
//     `multi_database`. Learning whether an instance is multi-database therefore
//     costs one GET per instance.
//
//  6. GET DETAIL 500s FOR v1-CREATED INSTANCES. An instance created through
//     POST /v1/instances is not registered in the v2beta1 instance store, and
//     the GET fails with HTTP 500 and a body that leaks an internal URL and an
//     unrendered Go template (`invalid status code 404 [GET
//     /aura-instances/{{.Instance_id}}]: https://console-api-private…`) — not a
//     404. Callers MUST treat any error here as "multi_database unknown" and
//     never as "not multi-database", and never let it fail a reconcile. The
//     instance's OTHER v2beta1 sub-resources (…/databases, …/ip-filters) do
//     resolve fine for the same instance — it is the instance GET alone that
//     breaks.
// ==========================================================================

// v2beta1 instance tier names. Distinct vocabulary from the v1 `type` values —
// see landmine 1.
const (
	InstanceTypeV2Free                  = "free"
	InstanceTypeV2Professional          = "professional"
	InstanceTypeV2BusinessCritical      = "business-critical"
	InstanceTypeV2VirtualDedicatedCloud = "virtual-dedicated-cloud"
)

// v1ToV2InstanceType maps the CR's v1 tier vocabulary onto v2beta1's. The AuraDS
// tiers (professional-ds / enterprise-ds) have no v2beta1 equivalent, so they
// are deliberately absent — InstanceTypeV2 reports them as unsupported rather
// than guessing.
var v1ToV2InstanceType = map[string]string{
	"free-db":           InstanceTypeV2Free,
	"professional-db":   InstanceTypeV2Professional,
	"business-critical": InstanceTypeV2BusinessCritical,
	"enterprise-db":     InstanceTypeV2VirtualDedicatedCloud,
}

// InstanceTypeV2 translates a v1/CR instance type into the v2beta1 tier name.
// The second return is false when the tier has no v2beta1 equivalent, in which
// case the caller must not use the v2beta1 create path.
func InstanceTypeV2(v1Type string) (string, bool) {
	v2, ok := v1ToV2InstanceType[v1Type]
	return v2, ok
}

// multiDatabaseCapableV2Types are the ONLY tiers Aura will create a
// multi-database instance on. Verified live 2026-07-31: `free` and `professional`
// are both refused with HTTP 400 / `multi-database-tier-not-supported`, while
// `business-critical` succeeded and `virtual-dedicated-cloud` passed the tier
// check (failing later, on the test project not offering the enterprise tier).
var multiDatabaseCapableV2Types = map[string]bool{
	InstanceTypeV2BusinessCritical:      true,
	InstanceTypeV2VirtualDedicatedCloud: true,
}

// SupportsMultiDatabase reports whether a v2beta1 tier name can host a
// multi-database instance. Callers should refuse up front rather than let Aura
// reject the create — the tier is immutable afterwards, so the failure is
// terminal either way, and only the local check can explain it usefully.
func SupportsMultiDatabase(v2Type string) bool {
	return multiDatabaseCapableV2Types[v2Type]
}

// InstanceV2 is the v2beta1 instance DETAIL shape (GET
// /organizations/{org}/projects/{project}/instances/{id}). See landmine 4 for
// how it differs from the v1 Instance.
type InstanceV2 struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	CloudProvider string `json:"cloud_provider"`
	Region        string `json:"region"`
	Memory        string `json:"memory,omitempty"`
	Storage       string `json:"storage,omitempty"`
	// LegacyStatus is this API's status field. v2beta1 has no `status`.
	LegacyStatus string `json:"legacy_status,omitempty"`
	// MultiDatabase is the whole reason this client exists: it is exposed here
	// and nowhere else (not on v1 GET, not on the v2beta1 LIST).
	MultiDatabase *bool `json:"multi_database,omitempty"`
	// GraphAnalytics is a STRING here (observed: "serverless"), unlike the v1
	// `graph_analytics_plugin` boolean.
	GraphAnalytics  string `json:"graph_analytics,omitempty"`
	VectorOptimized *bool  `json:"vector_optimized,omitempty"`
}

// InstanceV2Summary is the v2beta1 instance LIST shape. Note the absence of
// `multi_database` — see landmine 5.
type InstanceV2Summary struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	CloudProvider string `json:"cloud_provider,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
}

// CreateInstanceV2Request is the v2beta1 create body. Only `name` and `type` are
// required; `type` must be a v2beta1 tier name (see InstanceTypeV2). Any field
// not listed here is silently dropped by the API — landmine 3.
type CreateInstanceV2Request struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	CloudProvider string `json:"cloud_provider,omitempty"`
	Region        string `json:"region,omitempty"`
	Memory        string `json:"memory,omitempty"`
	// MultiDatabase is a pointer so it can be omitted entirely (letting Aura
	// apply its own default) as distinct from explicitly requesting false.
	MultiDatabase *bool `json:"multi_database,omitempty"`
}

// CreateInstanceV2Response is the 202 body of the v2beta1 create. Like v1's, it
// carries the ONE-TIME initial credentials, which the caller must persist
// immediately. Unlike the GET detail, it does include project_id and
// connection_url — plus `default_database_id`, the id of the database Aura
// creates with the instance (the one an AuraDatabase CR must not try to adopt).
type CreateInstanceV2Response struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Type              string `json:"type"`
	CloudProvider     string `json:"cloud_provider"`
	Region            string `json:"region"`
	Memory            string `json:"memory,omitempty"`
	Storage           string `json:"storage,omitempty"`
	ProjectID         string `json:"project_id,omitempty"`
	ConnectionURL     string `json:"connection_url,omitempty"`
	Username          string `json:"username,omitempty"`
	Password          string `json:"password,omitempty"`
	CreatedAt         string `json:"created_at,omitempty"`
	LegacyStatus      string `json:"legacy_status,omitempty"`
	MultiDatabase     *bool  `json:"multi_database,omitempty"`
	DefaultDatabaseID string `json:"default_database_id,omitempty"`
	VectorOptimized   *bool  `json:"vector_optimized,omitempty"`
}

// instanceV2CollectionPath builds the project-scoped instance collection path.
func instanceV2CollectionPath(orgID, projectID string) string {
	return "/organizations/" + url.PathEscape(orgID) +
		"/projects/" + url.PathEscape(projectID) + "/instances"
}

// CreateInstanceV2 creates an instance through v2beta1 (beta). The operator uses
// this ONLY when the CR asks for a multi-database instance; every other create
// goes through v1's CreateInstance.
func (c *Client) CreateInstanceV2(ctx context.Context, orgID, projectID string, req CreateInstanceV2Request) (*CreateInstanceV2Response, error) {
	var out CreateInstanceV2Response
	if err := c.doV2Data(ctx, http.MethodPost, instanceV2CollectionPath(orgID, projectID), req, &out); err != nil {
		return nil, err
	}
	// An empty ID must not read as success. doV2Data treats a missing or null
	// `data` field as a no-op, so an unexpected 2xx envelope decodes to a zero
	// struct with no error — and the caller would store an EMPTY external-ID
	// annotation, then create another PAID instance on every reconcile.
	if strings.TrimSpace(out.ID) == "" {
		return nil, fmt.Errorf("creating instance %q: Aura returned success with no instance id", req.Name)
	}
	return &out, nil
}

// GetInstanceV2 reads the v2beta1 instance detail — the only source of
// `multi_database`. It 500s for v1-created instances (landmine 6), so callers
// must treat every error as "unknown", not as "false".
func (c *Client) GetInstanceV2(ctx context.Context, orgID, projectID, id string) (*InstanceV2, error) {
	var out InstanceV2
	path := instanceV2CollectionPath(orgID, projectID) + "/" + url.PathEscape(id)
	if err := c.doV2Data(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListInstancesV2 lists a project's instances via v2beta1 (beta). The summaries
// do NOT carry multi_database — landmine 5.
func (c *Client) ListInstancesV2(ctx context.Context, orgID, projectID string) ([]InstanceV2Summary, error) {
	var out []InstanceV2Summary
	if err := c.doV2Data(ctx, http.MethodGet, instanceV2CollectionPath(orgID, projectID), nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}
