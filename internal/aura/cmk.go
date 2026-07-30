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
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Customer-managed-key status values, as returned in the `status` field of a
// CustomerManagedKey.
//
// NOTE: v1 (and v1beta5) type `status` as a bare `string` with NO enum, so these
// are not schema-verifiable. Provenance: "pending" and "ready" appear in the v1
// response examples; "deleting" appears only in v1beta5 examples; "error" is not
// attested anywhere in either spec and is a defensive guess. Treat unrecognized
// values as non-terminal rather than assuming this list is exhaustive.
const (
	CMKStatusReady    = "ready"
	CMKStatusPending  = "pending"
	CMKStatusDeleting = "deleting"
	CMKStatusError    = "error"
)

// ReasonEncryptionKeyActive is returned (with HTTP 400) when a delete is
// attempted against a customer-managed key that is still in use by one or more
// instances. Callers must detach/destroy those instances before the key can be
// deleted.
const ReasonEncryptionKeyActive = "encryption-key-is-active"

// CustomerManagedKey is the full detail of an Aura customer-managed encryption
// key, as returned by GET /customer-managed-keys/{keyId} (unwrapped from the
// `data` envelope). It references a key held in the customer's own cloud KMS;
// Aura stores only the reference (KeyID) and never the key material.
type CustomerManagedKey struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	Created       string `json:"created,omitempty"`
	TenantID      string `json:"tenant_id"`
	InstanceType  string `json:"instance_type"`
	CloudProvider string `json:"cloud_provider"`
	KeyID         string `json:"key_id"`
	Region        string `json:"region"`
}

// CreateCMKRequest is the request body for POST /customer-managed-keys. Every
// field is required by the API: the key is scoped to one tenant, instance type,
// cloud provider, and region, and points at a KMS key the customer already
// created and granted Aura access to (KeyID is the cloud-native key resource
// identifier — an AWS ARN, GCP resource name, or Azure key URL).
type CreateCMKRequest struct {
	Name          string `json:"name"`
	TenantID      string `json:"tenant_id"`
	InstanceType  string `json:"instance_type"`
	CloudProvider string `json:"cloud_provider"`
	KeyID         string `json:"key_id"`
	Region        string `json:"region"`
}

// IsCMKReady reports whether status is the terminal "ready" state — the key is
// registered and usable when creating instances.
func IsCMKReady(status string) bool {
	return normalizeStatus(status) == CMKStatusReady
}

// IsCMKPending reports whether status is the transient "pending" state — Aura is
// still validating access to the key; the caller should keep polling.
func IsCMKPending(status string) bool {
	return normalizeStatus(status) == CMKStatusPending
}

// CreateCustomerManagedKey registers a customer-managed encryption key. The key
// is validated asynchronously: the returned key typically has status "pending" —
// poll GetCustomerManagedKey until it becomes "ready" before using it to create
// an instance.
func (c *Client) CreateCustomerManagedKey(ctx context.Context, req CreateCMKRequest) (*CustomerManagedKey, error) {
	var env dataEnvelope[CustomerManagedKey]
	if err := c.doJSON(ctx, "POST", "/customer-managed-keys", req, &env); err != nil {
		return nil, fmt.Errorf("creating customer-managed key: %w", err)
	}
	return &env.Data, nil
}

// GetCustomerManagedKey returns detail for a single customer-managed key.
func (c *Client) GetCustomerManagedKey(ctx context.Context, id string) (*CustomerManagedKey, error) {
	path := "/customer-managed-keys/" + url.PathEscape(id)
	var env dataEnvelope[CustomerManagedKey]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("getting customer-managed key %q: %w", id, err)
	}
	return &env.Data, nil
}

// CustomerManagedKeySummary is what the LIST endpoint returns — deliberately
// NOT the full CustomerManagedKey.
//
// v1 GET /customer-managed-keys returns a summary carrying only `id`, `name` and
// `tenant_id` (all three required), and its own description says to use
// GET /{id} for the detail. Decoding the list into the full struct silently
// yields empty KeyID/Region/CloudProvider/InstanceType, so matching a candidate
// on any of those fields can never succeed against the real API.
type CustomerManagedKeySummary struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	TenantID string `json:"tenant_id"`
}

// ListCustomerManagedKeys returns the customer-managed keys the credential can
// see, optionally filtered to a single tenant/project. Pass an empty tenantID to
// list across all tenants.
//
// Returns SUMMARIES (id/name/tenant_id only). To compare any other field, fetch
// the detail per candidate with GetCustomerManagedKey.
func (c *Client) ListCustomerManagedKeys(ctx context.Context, tenantID string) ([]CustomerManagedKeySummary, error) {
	path := "/customer-managed-keys"
	if tenantID != "" {
		path += "?tenantId=" + url.QueryEscape(tenantID)
	}
	var env dataEnvelope[[]CustomerManagedKeySummary]
	if err := c.doJSON(ctx, "GET", path, nil, &env); err != nil {
		return nil, fmt.Errorf("listing customer-managed keys: %w", err)
	}
	return env.Data, nil
}

// DeleteCustomerManagedKey deletes a customer-managed key. Deletion is
// idempotent: a 404 (the key is already gone) is treated as success. A key still
// bound to a running instance cannot be deleted — the API returns 400 with the
// encryption-key-is-active reason, which callers detect via IsCMKActive.
func (c *Client) DeleteCustomerManagedKey(ctx context.Context, id string) error {
	path := "/customer-managed-keys/" + url.PathEscape(id)
	if err := c.doJSON(ctx, "DELETE", path, nil, nil); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting customer-managed key %q: %w", id, err)
	}
	return nil
}

// IsCMKActive reports whether err indicates a customer-managed key could not be
// deleted because it is still bound to one or more instances (HTTP 400 with the
// encryption-key-is-active reason, matched case- and punctuation-tolerantly).
// Callers surface this as a blocking condition rather than requeueing forever.
func IsCMKActive(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.Reason == ReasonEncryptionKeyActive {
		return true
	}
	// Some responses carry the signal only in the human-readable message.
	return strings.Contains(strings.ToLower(apiErr.Message), "encryption key is active") ||
		strings.Contains(strings.ToLower(apiErr.Message), "encryption-key-is-active")
}
