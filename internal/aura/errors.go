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

// newAPIError builds an APIError from a non-2xx response. It parses BOTH known
// Aura error body shapes — the primary `{"errors":[...]}` shape and the gateway
// `{"error":"..."}` shape — falling back to a generic message when the body is
// empty or unparseable.
func newAPIError(statusCode int, requestID string, body []byte) *APIError {
	e := &APIError{
		StatusCode: statusCode,
		RequestID:  requestID,
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
func IsConflict(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusConflict ||
			apiErr.Reason == ReasonOngoingDatabaseOperation
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
