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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ==========================================================================
// BETA — Aura API v2beta1 IP filtering.
//
// Shapes below are taken from the official v2beta1 OpenAPI spec (the `IpFilter`
// schema + the ip-filters paths). Two things the spec pins that are unusual and
// were wrong in the first (reconstructed) cut, kept here as landmines to respect:
//
//   - IP filters are ORGANIZATION-scoped: /organizations/{org}/ip-filters. They
//     are NOT under a project or instance. (A read-only per-instance *status*
//     view exists at /organizations/{org}/projects/{p}/instances/{i}/ip-filters.)
//   - The ip-filters endpoints return the object/array DIRECTLY — they are the
//     one v2beta1 resource NOT wrapped in a {"data": …} envelope.
//
// The create/update REQUEST body is not itself schema'd in the spec (the POST
// has no requestBody); we mirror the documented `IpFilter` response shape, which
// is the standard convention. v2beta1 is still beta (breaking changes allowed
// without a version bump), so this remains best-effort.
// ==========================================================================

// IP-filter status values as reported by the per-instance status view
// (/…/instances/{id}/ip-filters). The base org-scoped filter object carries no
// status. Retained for callers that read the per-instance view.
const (
	IPFilterStatusUnknown   = "UNKNOWN"
	IPFilterStatusSubmitted = "SUBMITTED"
	IPFilterStatusActive    = "ACTIVE"
	IPFilterStatusDeleted   = "DELETED"
	IPFilterStatusError     = "ERROR"
)

// IPFilterAllowEntry is a single CIDR entry in an IP filter's allow list. The
// v2beta1 API splits CIDR notation into an address + a prefix length (so
// "203.0.113.0/24" is {address:"203.0.113.0", prefix_len:24}).
type IPFilterAllowEntry struct {
	Address     string `json:"address"`
	PrefixLen   int    `json:"prefix_len"`
	Description string `json:"description,omitempty"`
}

// IPFilterEntities is the set of entities an IP filter is applied to. All three
// are lists of Aura IDs.
type IPFilterEntities struct {
	Instances     []string `json:"instances,omitempty"`
	Projects      []string `json:"projects,omitempty"`
	Organizations []string `json:"organizations,omitempty"`
}

// IPFilter is an organization-scoped Aura network IP filter (allowlist), per the
// v2beta1 `IpFilter` schema.
type IPFilter struct {
	ID                string               `json:"id,omitempty"`
	Name              string               `json:"name,omitempty"`
	Description       string               `json:"description,omitempty"`
	OrganizationID    string               `json:"organization_id,omitempty"`
	AllowList         []IPFilterAllowEntry `json:"allow_list"`
	FilteredEntities  IPFilterEntities     `json:"filtered_entities"`
	FilteringDisabled bool                 `json:"filtering_disabled,omitempty"`
	UpdatedAt         string               `json:"updated_at,omitempty"`
}

// UnmarshalJSON decodes an IPFilter, coercing the `id` field which the spec
// declares as a string but whose examples show as a bare integer — so a numeric
// id can't break decoding.
func (f *IPFilter) UnmarshalJSON(b []byte) error {
	type alias IPFilter
	aux := &struct {
		ID json.RawMessage `json:"id"`
		*alias
	}{alias: (*alias)(f)}
	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}
	raw := bytes.TrimSpace(aux.ID)
	switch {
	case len(raw) == 0 || string(raw) == "null":
		f.ID = ""
	case raw[0] == '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		f.ID = s
	default:
		f.ID = string(raw) // bare number
	}
	return nil
}

// CreateIPFilterRequest mirrors the IpFilter response shape (the POST body is
// not separately schema'd upstream).
type CreateIPFilterRequest struct {
	Name              string               `json:"name,omitempty"`
	Description       string               `json:"description,omitempty"`
	AllowList         []IPFilterAllowEntry `json:"allow_list"`
	FilteredEntities  IPFilterEntities     `json:"filtered_entities"`
	FilteringDisabled *bool                `json:"filtering_disabled,omitempty"`
}

// UpdateIPFilterRequest edits a filter's mutable fields.
type UpdateIPFilterRequest struct {
	Name              *string               `json:"name,omitempty"`
	Description       *string               `json:"description,omitempty"`
	AllowList         *[]IPFilterAllowEntry `json:"allow_list,omitempty"`
	FilteredEntities  *IPFilterEntities     `json:"filtered_entities,omitempty"`
	FilteringDisabled *bool                 `json:"filtering_disabled,omitempty"`
}

// v2beta1Base derives the v2beta1 API root from the configured v1 base URL
// (https://api.neo4j.io/v1 → https://api.neo4j.io/v2beta1), so a test server or
// a BaseURL override carries over.
func (c *Client) v2beta1Base() string {
	root := strings.TrimSuffix(c.baseURL, "/v1")
	return root + "/v2beta1"
}

// orgIPFilterPath builds the organization-scoped IP-filter collection path.
func orgIPFilterPath(orgID string) string {
	return "/organizations/" + url.PathEscape(orgID) + "/ip-filters"
}

// CreateIPFilter registers an organization IP filter (v2beta1, beta). The
// response is returned unwrapped.
func (c *Client) CreateIPFilter(ctx context.Context, orgID string, req CreateIPFilterRequest) (*IPFilter, error) {
	var out IPFilter
	if err := c.doV2JSON(ctx, http.MethodPost, orgIPFilterPath(orgID), req, &out); err != nil {
		return nil, fmt.Errorf("creating ip filter: %w", err)
	}
	return &out, nil
}

// GetIPFilter returns a single IP filter by ID (v2beta1, beta).
func (c *Client) GetIPFilter(ctx context.Context, orgID, id string) (*IPFilter, error) {
	var out IPFilter
	if err := c.doV2JSON(ctx, http.MethodGet, orgIPFilterPath(orgID)+"/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, fmt.Errorf("getting ip filter %q: %w", id, err)
	}
	return &out, nil
}

// ListIPFilters lists the organization's IP filters (v2beta1, beta). The
// response is a bare array.
func (c *Client) ListIPFilters(ctx context.Context, orgID string) ([]IPFilter, error) {
	var out []IPFilter
	if err := c.doV2JSON(ctx, http.MethodGet, orgIPFilterPath(orgID), nil, &out); err != nil {
		return nil, fmt.Errorf("listing ip filters: %w", err)
	}
	return out, nil
}

// UpdateIPFilter edits an IP filter's mutable fields (v2beta1, beta).
func (c *Client) UpdateIPFilter(ctx context.Context, orgID, id string, req UpdateIPFilterRequest) (*IPFilter, error) {
	var out IPFilter
	if err := c.doV2JSON(ctx, http.MethodPatch, orgIPFilterPath(orgID)+"/"+url.PathEscape(id), req, &out); err != nil {
		return nil, fmt.Errorf("updating ip filter %q: %w", id, err)
	}
	return &out, nil
}

// DeleteIPFilter deletes an IP filter (v2beta1, beta). Idempotent: a 404 is
// treated as success.
func (c *Client) DeleteIPFilter(ctx context.Context, orgID, id string) error {
	if err := c.doV2JSON(ctx, http.MethodDelete, orgIPFilterPath(orgID)+"/"+url.PathEscape(id), nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting ip filter %q: %w", id, err)
	}
	return nil
}

// doV2JSON is the v2beta1 request path. It reuses the shared OAuth token cache
// and rate limiter but targets the v2beta1 base URL, and is deliberately kept
// separate from the v1 doJSON so the stable v1 client is untouched by beta
// churn. Errors are reported to the same metrics Observer.
func (c *Client) doV2JSON(ctx context.Context, method, path string, body, out any) error {
	if err := c.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}

	fullURL := c.v2beta1Base() + path
	if !strings.HasPrefix(fullURL, "https://") {
		return fmt.Errorf("refusing non-HTTPS request URL: %s", fullURL)
	}

	var bodyReader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
		bodyReader = bytes.NewReader(encoded)
	}

	token, err := c.getToken(ctx, false)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("performing %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newAPIError(resp.StatusCode, resp.Header.Get("X-Request-Id"), respBody)
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decoding response body: %w", err)
		}
	}
	return nil
}
