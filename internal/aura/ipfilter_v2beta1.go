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

// IPFilterAllowEntry is a single entry in an IP filter's allow list, in the
// shape the API READS BACK: address + prefix length.
//
// The WRITE shape is different — see ipFilterAllowEntryWrite. Do not reuse this
// struct for a request body; that is the bug this file shipped with, and every
// create and update failed with HTTP 400 because of it.
type IPFilterAllowEntry struct {
	Address     string `json:"address"`
	PrefixLen   int    `json:"prefix_len"`
	Description string `json:"description,omitempty"`
}

// CIDR renders the entry in the notation the API requires on write.
func (e IPFilterAllowEntry) CIDR() string {
	return fmt.Sprintf("%s/%d", e.Address, e.PrefixLen)
}

// ipFilterAllowEntryWrite is the REQUEST shape for one allow-list entry.
//
// Two things the response shape does not tell you, both verified live on
// 2026-08-01 and both hard 400s:
//   - the field is `ip_range`, a CIDR STRING ("203.0.113.0/24"), not
//     address + prefix_len;
//   - `description` may not be NULL. An empty string is accepted, so the key is
//     always emitted — deliberately NO omitempty, or an entry without a
//     description is rejected.
type ipFilterAllowEntryWrite struct {
	IPRange     string `json:"ip_range"`
	Description string `json:"description"`
}

// writeAllowList converts the read/CRD shape into the request shape.
func writeAllowList(entries []IPFilterAllowEntry) []ipFilterAllowEntryWrite {
	out := make([]ipFilterAllowEntryWrite, 0, len(entries))
	for _, e := range entries {
		out = append(out, ipFilterAllowEntryWrite{IPRange: e.CIDR(), Description: e.Description})
	}
	return out
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
	// BrainIPAddressesEnabled is undocumented in the spec but returned by every
	// response AND writable (the API's own PATCH error enumerates it). Modelled
	// so a PATCH round-trip cannot silently clear it.
	BrainIPAddressesEnabled bool   `json:"brain_ip_addresses_enabled,omitempty"`
	UpdatedAt               string `json:"updated_at,omitempty"`
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

// CreateIPFilterRequest is the caller-facing create input. It is deliberately
// NOT the response shape — see ipFilterCreateBody for what actually goes on the
// wire, and why.
type CreateIPFilterRequest struct {
	Name        string               `json:"-"`
	Description string               `json:"-"`
	AllowList   []IPFilterAllowEntry `json:"-"`
	// Entities are the instances/projects/orgs the filter applies to. The API
	// accepts these at create time (verified live), so no second call is needed.
	Entities                IPFilterEntities `json:"-"`
	FilteringDisabled       *bool            `json:"-"`
	BrainIPAddressesEnabled *bool            `json:"-"`
}

// ipFilterCreateBody is the wire shape of a create.
//
// Two divergences from the response, both verified live on 2026-08-01:
//   - `organization_id` is REQUIRED IN THE BODY even though the path is already
//     organization-scoped. Omitting it is a 400.
//   - the attachment field is `entities` on WRITE and `filtered_entities` on
//     READ. Sending `filtered_entities` returns HTTP 200 and SILENTLY DROPS the
//     attachment — the filter is created applying to nothing. That silence is
//     why this went unnoticed: the CR reported success while filtering nothing.
type ipFilterCreateBody struct {
	Name                    string                    `json:"name,omitempty"`
	Description             string                    `json:"description,omitempty"`
	OrganizationID          string                    `json:"organization_id"`
	AllowList               []ipFilterAllowEntryWrite `json:"allow_list"`
	Entities                IPFilterEntities          `json:"entities"`
	FilteringDisabled       *bool                     `json:"filtering_disabled,omitempty"`
	BrainIPAddressesEnabled *bool                     `json:"brain_ip_addresses_enabled,omitempty"`
}

// UpdateIPFilterRequest edits a filter's mutable fields. The API names the
// settable set in its own error when a PATCH is empty: Name, Description,
// AllowList, FilteringDisabled, BrainIPAddressesEnabled, Entities.
type UpdateIPFilterRequest struct {
	Name                    *string               `json:"-"`
	Description             *string               `json:"-"`
	AllowList               *[]IPFilterAllowEntry `json:"-"`
	Entities                *IPFilterEntities     `json:"-"`
	FilteringDisabled       *bool                 `json:"-"`
	BrainIPAddressesEnabled *bool                 `json:"-"`
}

// ipFilterUpdateBody is the wire shape of a PATCH — `entities`, and the CIDR
// allow-list form, exactly as for create.
type ipFilterUpdateBody struct {
	Name                    *string                    `json:"name,omitempty"`
	Description             *string                    `json:"description,omitempty"`
	AllowList               *[]ipFilterAllowEntryWrite `json:"allow_list,omitempty"`
	Entities                *IPFilterEntities          `json:"entities,omitempty"`
	FilteringDisabled       *bool                      `json:"filtering_disabled,omitempty"`
	BrainIPAddressesEnabled *bool                      `json:"brain_ip_addresses_enabled,omitempty"`
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
	body := ipFilterCreateBody{
		Name:                    req.Name,
		Description:             req.Description,
		OrganizationID:          orgID, // required in the body as well as the path
		AllowList:               writeAllowList(req.AllowList),
		Entities:                req.Entities,
		FilteringDisabled:       req.FilteringDisabled,
		BrainIPAddressesEnabled: req.BrainIPAddressesEnabled,
	}
	if err := c.doV2JSON(ctx, http.MethodPost, orgIPFilterPath(orgID), body, &out); err != nil {
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
	body := ipFilterUpdateBody{
		Name:                    req.Name,
		Description:             req.Description,
		Entities:                req.Entities,
		FilteringDisabled:       req.FilteringDisabled,
		BrainIPAddressesEnabled: req.BrainIPAddressesEnabled,
	}
	if req.AllowList != nil {
		w := writeAllowList(*req.AllowList)
		body.AllowList = &w
	}
	if err := c.doV2JSON(ctx, http.MethodPatch, orgIPFilterPath(orgID)+"/"+url.PathEscape(id), body, &out); err != nil {
		return nil, fmt.Errorf("updating ip filter %q: %w", id, err)
	}
	return &out, nil
}

// DeleteIPFilter deletes an IP filter (v2beta1, beta). Idempotent: a 404 is
// treated as success.
// DeleteIPFilter removes an IP filter (v2beta1, beta). Idempotent.
//
// LANDMINE: a SUCCESSFUL delete arrives as HTTP 500. The gateway rejects its own
// backend's 204 and reports `invalid status code 204 [DELETE
// /ip-filters/{{.Ip_filter_id}}]: https://<internal-address-redacted>` — the same
// leaked-internal-URL, unrendered-Go-template shape as the v2beta1 instance GET.
// Deleting an already-gone filter is a 500 too (wrapping a 404).
//
// So neither IsNotFound nor a 2xx can be used to decide the outcome, and because
// IsTransient treats 5xx as retryable, the previous implementation made the
// AuraIPFilter finalizer retry forever: the filter WAS deleted on the first
// attempt, but the CR could never leave Terminating.
//
// Rather than pattern-match the gateway's prose (which will change the moment
// Aura fixes it), confirm the outcome: if the filter is gone, the delete
// succeeded — whatever status code said so. Verified live 2026-08-01.
func (c *Client) DeleteIPFilter(ctx context.Context, orgID, id string) error {
	delErr := c.doV2JSON(ctx, http.MethodDelete, orgIPFilterPath(orgID)+"/"+url.PathEscape(id), nil, nil)
	if delErr == nil || IsNotFound(delErr) {
		return nil
	}
	// Ask whether it is actually gone. The GET path is well-behaved: it answers a
	// real 404 with reason ip-filter-not-found.
	if _, getErr := c.GetIPFilter(ctx, orgID, id); IsNotFound(getErr) {
		return nil
	}
	return fmt.Errorf("deleting ip filter %q: %w", id, delErr)
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
