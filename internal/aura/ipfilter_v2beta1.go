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
// BETA / UNVERIFIED — Aura API v2beta1 IP filtering.
//
// IP filtering is only exposed on the Aura API v2beta1 surface, which is a
// hierarchical org/project API (base path /v2beta1/organizations/{org}/
// projects/{project}/…). v2beta1 is an UNSTABLE BETA: breaking changes are
// allowed within it without a version bump, a `legacy_status` → `status` rename
// is pending, and some request bodies are undocumented.
//
// The exact endpoint paths and request/response field names below could NOT be
// verified against a rendered spec (the redoc is JS-only; the raw spec is
// gated; the first-party Labs Terraform provider is v1-only). They are
// RECONSTRUCTED from the documented semantics (IP filters are project-scoped,
// CIDR-based, at most one per instance) and MUST be validated against a live
// v2beta1 account before this is relied on. Everything unverified is isolated in
// this one file behind the single `ipFilterCollectionPath` route builder and
// the v2beta1Envelope, so correcting the contract is a localized change.
// ==========================================================================

// v2beta1 IP-filter status values (RECONSTRUCTED — verify against the live API).
const (
	IPFilterStatusReady    = "ready"
	IPFilterStatusPending  = "pending"
	IPFilterStatusUpdating = "updating"
	IPFilterStatusError    = "error"
)

// IPFilter is an Aura network IP filter (allowlist) — RECONSTRUCTED shape.
type IPFilter struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	Status     string   `json:"status"`
	InstanceID string   `json:"instance_id,omitempty"`
	Region     string   `json:"region,omitempty"`
	CIDRs      []string `json:"cidrs"`
}

// CreateIPFilterRequest is the create body — RECONSTRUCTED shape.
type CreateIPFilterRequest struct {
	Name       string   `json:"name"`
	InstanceID string   `json:"instance_id,omitempty"`
	Region     string   `json:"region,omitempty"`
	CIDRs      []string `json:"cidrs"`
}

// UpdateIPFilterRequest is the patch body — RECONSTRUCTED shape. Only the CIDR
// set and name are mutable; placement/instance association is fixed.
type UpdateIPFilterRequest struct {
	Name  *string   `json:"name,omitempty"`
	CIDRs *[]string `json:"cidrs,omitempty"`
}

// IsIPFilterReady reports whether the filter has reached its terminal ready
// state (tolerant of the pending legacy_status → status rename: any of the
// known ready spellings match).
func IsIPFilterReady(status string) bool {
	return normalizeStatus(status) == IPFilterStatusReady
}

// v2beta1Base derives the v2beta1 API root from the configured v1 base URL
// (https://api.neo4j.io/v1 → https://api.neo4j.io/v2beta1), so a test server or
// a BaseURL override carries over.
func (c *Client) v2beta1Base() string {
	root := strings.TrimSuffix(c.baseURL, "/v1")
	return root + "/v2beta1"
}

// ipFilterCollectionPath builds the project-scoped IP-filter collection path.
// THIS ROUTE IS UNVERIFIED — the single place to correct once the live v2beta1
// contract is confirmed.
func ipFilterCollectionPath(orgID, projectID string) string {
	return fmt.Sprintf("/organizations/%s/projects/%s/network/ip-filters",
		url.PathEscape(orgID), url.PathEscape(projectID))
}

// v2beta1Envelope is the assumed v2beta1 response wrapper (matches v1's `data`
// nesting; UNVERIFIED — v2beta1 may return payloads unwrapped).
type v2beta1Envelope[T any] struct {
	Data T `json:"data"`
}

// CreateIPFilter registers an IP filter for a project/instance (v2beta1, beta).
func (c *Client) CreateIPFilter(ctx context.Context, orgID, projectID string, req CreateIPFilterRequest) (*IPFilter, error) {
	var env v2beta1Envelope[IPFilter]
	if err := c.doV2JSON(ctx, http.MethodPost, ipFilterCollectionPath(orgID, projectID), req, &env); err != nil {
		return nil, fmt.Errorf("creating ip filter: %w", err)
	}
	return &env.Data, nil
}

// GetIPFilter returns a single IP filter by ID (v2beta1, beta).
func (c *Client) GetIPFilter(ctx context.Context, orgID, projectID, id string) (*IPFilter, error) {
	path := ipFilterCollectionPath(orgID, projectID) + "/" + url.PathEscape(id)
	var env v2beta1Envelope[IPFilter]
	if err := c.doV2JSON(ctx, http.MethodGet, path, nil, &env); err != nil {
		return nil, fmt.Errorf("getting ip filter %q: %w", id, err)
	}
	return &env.Data, nil
}

// ListIPFilters lists the IP filters in a project (v2beta1, beta).
func (c *Client) ListIPFilters(ctx context.Context, orgID, projectID string) ([]IPFilter, error) {
	var env v2beta1Envelope[[]IPFilter]
	if err := c.doV2JSON(ctx, http.MethodGet, ipFilterCollectionPath(orgID, projectID), nil, &env); err != nil {
		return nil, fmt.Errorf("listing ip filters: %w", err)
	}
	return env.Data, nil
}

// UpdateIPFilter edits an IP filter's mutable fields (v2beta1, beta).
func (c *Client) UpdateIPFilter(ctx context.Context, orgID, projectID, id string, req UpdateIPFilterRequest) (*IPFilter, error) {
	path := ipFilterCollectionPath(orgID, projectID) + "/" + url.PathEscape(id)
	var env v2beta1Envelope[IPFilter]
	if err := c.doV2JSON(ctx, http.MethodPatch, path, req, &env); err != nil {
		return nil, fmt.Errorf("updating ip filter %q: %w", id, err)
	}
	return &env.Data, nil
}

// DeleteIPFilter deletes an IP filter (v2beta1, beta). Idempotent: a 404 is
// treated as success.
func (c *Client) DeleteIPFilter(ctx context.Context, orgID, projectID, id string) error {
	path := ipFilterCollectionPath(orgID, projectID) + "/" + url.PathEscape(id)
	if err := c.doV2JSON(ctx, http.MethodDelete, path, nil, nil); err != nil {
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
// churn. Errors are reported to the same metrics Observer with a "(v2beta1)"
// operation prefix so beta traffic is visible but distinguishable.
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
