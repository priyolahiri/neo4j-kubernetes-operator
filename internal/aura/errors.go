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

// Package aura provides a self-contained HTTP client for the Neo4j Aura
// Platform API v1 (https://neo4j.com/docs/aura/api/). It is consumed by the
// operator's controllers to provision and manage Aura instances and snapshots.
package aura

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// reason values the Aura API returns in the `reason` field that callers key
// decisions off. The set is intentionally small — only the reasons this client
// classifies are named; everything else is surfaced verbatim on APIError.Reason.
const (
	// ReasonOngoingDatabaseOperation is returned (with HTTP 409) when another
	// asynchronous operation is already in flight on the instance, so the
	// requested lifecycle action cannot start yet.
	ReasonOngoingDatabaseOperation = "ongoing-database-operation"
	// ReasonDBNotFound is returned (with HTTP 404) when the target instance
	// does not exist.
	ReasonDBNotFound = "db-not-found"
	// ReasonSnapshotNotFound is returned (with HTTP 404) when the target
	// snapshot does not exist.
	ReasonSnapshotNotFound = "snapshot-not-found"
	// ReasonTenantNotFound is returned (with HTTP 404) when the target
	// tenant/project does not exist.
	ReasonTenantNotFound = "tenant-not-found"
	// ReasonMultiDBOnly is returned by the v2beta1 database-create endpoint
	// (with HTTP 409, message "Only multi database Instances can add
	// databases") when the target instance is not a multi-database instance.
	// It is TERMINAL: `multi_database` is fixed at instance-creation time and
	// there is no API to convert an existing instance, so retrying can never
	// succeed. Verified live 2026-07-31.
	ReasonMultiDBOnly = "multi-db-only"
	// ReasonMultiDatabaseTierNotSupported is returned by the v2beta1 instance
	// create (with HTTP 400, message "Multi-database instances are not supported
	// by the provided tier") when multi_database is requested on a tier that
	// cannot host it. Verified live 2026-07-31: `free` and `professional` are
	// both refused this way; `business-critical` and `virtual-dedicated-cloud`
	// are accepted. TERMINAL — the tier is immutable on an existing instance.
	ReasonMultiDatabaseTierNotSupported = "multi-database-tier-not-supported"
	// ReasonAuraDatabaseNotFound is returned (with HTTP 404) by the v2beta1
	// per-database endpoints when the database does not exist — including the
	// second DELETE of an already-deleted database, which is how those deletes
	// stay idempotent. Verified live 2026-07-31.
	ReasonAuraDatabaseNotFound = "aura-database-not-found"
)

// APIError is the structured error returned for any non-2xx Aura API response.
// It carries the HTTP status code, the API's machine-readable reason and
// human-readable message (parsed from whichever error body shape was returned),
// and the X-Request-Id header for support/correlation.
type APIError struct {
	// StatusCode is the HTTP status code of the response.
	StatusCode int
	// Reason is the API's machine-readable error category (from the
	// `{"errors":[{"reason":...}]}` body shape). Empty for the gateway
	// `{"error":...}` shape, which carries no reason.
	Reason string
	// Message is the human-readable error explanation.
	Message string
	// RequestID is the value of the response's X-Request-Id header, useful for
	// quoting to Aura Support.
	RequestID string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch {
	case e.Reason != "" && e.RequestID != "":
		return fmt.Sprintf("aura API error: status=%d reason=%q message=%q request-id=%s",
			e.StatusCode, e.Reason, e.Message, e.RequestID)
	case e.Reason != "":
		return fmt.Sprintf("aura API error: status=%d reason=%q message=%q",
			e.StatusCode, e.Reason, e.Message)
	case e.RequestID != "":
		return fmt.Sprintf("aura API error: status=%d message=%q request-id=%s",
			e.StatusCode, e.Message, e.RequestID)
	default:
		return fmt.Sprintf("aura API error: status=%d message=%q", e.StatusCode, e.Message)
	}
}

// errorEnvelope models the primary Aura error body shape:
//
//	{"errors":[{"message":"...","reason":"...","field":"..."}]}
type errorEnvelope struct {
	Errors []struct {
		Message string `json:"message"`
		Reason  string `json:"reason"`
		Field   string `json:"field"`
	} `json:"errors"`
}

// middlewareEnvelope models the gateway/middleware error body shape:
//
//	{"error":"..."}
type middlewareEnvelope struct {
	Error string `json:"error"`
}

// v2beta1Envelope models the BARE error body the v2beta1 API returns:
//
//	{"message":"Only multi database Instances can add databases","reason":"multi-db-only"}
//
// Note the difference from v1, which wraps the same pair in an `errors` array.
// Verified live 2026-07-31. Without this shape the whole JSON blob ends up in
// APIError.Message — which is what a user then sees in their CR status — and
// Reason stays empty, so no reason-based classification can work on v2beta1.
type v2beta1Envelope struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

// fieldErrorEnvelope models the v2beta1 VALIDATION error body, where `errors` is
// an OBJECT KEYED BY FIELD — not the array v1 uses:
//
//	{"errors":{"allow_list":{"0":{"ip_range":["Field may not be null."]}},
//	           "organization_id":["Missing data for required field."]},
//	 "message":"The request body contains validation errors","reason":"validation-error"}
//
// Without this the per-field detail is lost and the user sees only "The request
// body contains validation errors", which names neither the field nor the
// problem — exactly the message that made the ip-filter contract bugs so hard to
// place. Verified live 2026-08-01.
type fieldErrorEnvelope struct {
	Errors  map[string]json.RawMessage `json:"errors"`
	Message string                     `json:"message"`
	Reason  string                     `json:"reason"`
}

// flattenFieldErrors renders the nested field map as
// "allow_list[0].ip_range: Field may not be null.; organization_id: …",
// sorted so the output is stable. The PATH matters as much as the message: the
// live body nests by list index, and a summary that says only "allow_list" does
// not tell you WHICH entry or WHICH field was wrong.
func flattenFieldErrors(m map[string]json.RawMessage) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, walkFieldErrors(k, m[k])...)
	}
	return strings.Join(parts, "; ")
}

// walkFieldErrors renders one subtree as "<path>: <msg>" entries. A numeric key
// is rendered as an index so `allow_list` + `0` reads as `allow_list[0]`.
func walkFieldErrors(path string, raw json.RawMessage) []string {
	var msgs []string
	if err := json.Unmarshal(raw, &msgs); err == nil {
		return []string{path + ": " + strings.Join(msgs, ", ")}
	}
	var child map[string]json.RawMessage
	if err := json.Unmarshal(raw, &child); err == nil {
		keys := make([]string, 0, len(child))
		for k := range child {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out []string
		for _, k := range keys {
			next := path + "." + k
			if _, convErr := strconv.Atoi(k); convErr == nil {
				next = path + "[" + k + "]"
			}
			out = append(out, walkFieldErrors(next, child[k])...)
		}
		return out
	}
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		return []string{path + ": " + one}
	}
	return []string{path + ": " + string(raw)}
}

// newAPIError builds an APIError from a non-2xx response. It parses the THREE
// known Aura error body shapes — v1's `{"errors":[...]}`, v2beta1's bare
// `{"message","reason"}`, and the gateway's `{"error":"..."}` — falling back to
// the raw body when none match. That fallback matters: v2beta1 schema-validation
// failures return PLAIN TEXT, not JSON (e.g. `- at ”: missing property 'id'`),
// and the status code must survive so callers can still classify them.
func newAPIError(statusCode int, requestID string, body []byte) *APIError {
	e := &APIError{
		StatusCode: statusCode,
		RequestID:  requestID,
	}

	// v2beta1 validation: `errors` is an object keyed by field. Try this before
	// the array shape — an object cannot decode into []struct, so the array
	// attempt would fall through and lose the detail.
	var fe fieldErrorEnvelope
	if err := json.Unmarshal(body, &fe); err == nil && len(fe.Errors) > 0 {
		e.Reason = fe.Reason
		e.Message = fe.Message
		if detail := flattenFieldErrors(fe.Errors); detail != "" {
			if e.Message == "" {
				e.Message = detail
			} else {
				e.Message = e.Message + " (" + detail + ")"
			}
		}
		return e
	}

	// Try the primary {"errors":[{message,reason,field}]} shape first.
	var env errorEnvelope
	if err := json.Unmarshal(body, &env); err == nil && len(env.Errors) > 0 {
		first := env.Errors[0]
		e.Reason = first.Reason
		e.Message = first.Message
		if first.Field != "" {
			e.Message = fmt.Sprintf("%s (field: %s)", first.Message, first.Field)
		}
		return e
	}

	// v2beta1 returns the same pair unwrapped.
	var v2 v2beta1Envelope
	if err := json.Unmarshal(body, &v2); err == nil && (v2.Message != "" || v2.Reason != "") {
		e.Message = v2.Message
		e.Reason = v2.Reason
		if e.Message == "" {
			e.Message = v2.Reason
		}
		return e
	}

	// Fall back to the gateway {"error":"..."} shape.
	var mw middlewareEnvelope
	if err := json.Unmarshal(body, &mw); err == nil && mw.Error != "" {
		e.Message = mw.Error
		return e
	}

	// Neither shape parsed — surface whatever we have.
	if len(body) > 0 {
		e.Message = string(body)
	} else {
		e.Message = http.StatusText(statusCode)
	}
	return e
}

// IsNotFound reports whether err is an APIError with HTTP 404 — the target
// instance, snapshot, or tenant does not exist. Callers treat this as a
// terminal "already gone" for idempotent deletes.
func IsNotFound(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusNotFound
	}
	return false
}

// IsConflict reports whether err indicates the action cannot be performed
// because another operation is in flight — HTTP 409, or the
// ongoing-database-operation reason. Callers requeue and retry.
//
// The multi-db-only refusal is deliberately EXCLUDED even though it too arrives
// as a 409: it is a permanent property of the instance, so reporting it as a
// retryable conflict makes a controller requeue forever instead of telling the
// user what is wrong. Use IsMultiDatabaseOnly for that case.
func IsConflict(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.Reason == ReasonMultiDBOnly {
			return false
		}
		return apiErr.StatusCode == http.StatusConflict ||
			apiErr.Reason == ReasonOngoingDatabaseOperation
	}
	return false
}

// IsMultiDatabaseOnly reports whether err is the v2beta1 refusal to add a
// database to an instance that was not created as multi-database. Terminal —
// see ReasonMultiDBOnly. Callers must check this BEFORE IsConflict and
// IsTransient, both of which would otherwise send them into a retry loop.
func IsMultiDatabaseOnly(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Reason == ReasonMultiDBOnly
	}
	return false
}

// IsMultiDatabaseTierUnsupported reports whether err is the v2beta1 refusal to
// CREATE a multi-database instance on a tier that cannot host one. Terminal:
// type is immutable on an existing AuraInstance (bar the one professional →
// business-critical upgrade), so retrying the same spec can never succeed.
func IsMultiDatabaseTierUnsupported(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Reason == ReasonMultiDatabaseTierNotSupported
	}
	return false
}

// IsForbidden reports whether err is an APIError with HTTP 403. Aura returns 403
// (not 401) for an expired/revoked access token, so the client uses this to
// decide whether to refresh the token and retry once.
func IsForbidden(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusForbidden
	}
	return false
}

// IsTransient reports whether err is worth retrying: a rate-limit (429), any 5xx
// server error, or a non-APIError transport failure (connection refused, TLS
// error, timeout, etc.). A structured APIError with a 4xx status other than 429
// is treated as terminal.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusTooManyRequests || apiErr.StatusCode >= 500
	}
	// Not an APIError => a transport-level failure that never reached the API.
	return true
}
