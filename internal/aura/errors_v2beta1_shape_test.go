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
	"net/http"
	"strings"
	"testing"
)

// Aura returns THREE different error body shapes, all observed live 2026-07-31.
// newAPIError must handle each and always preserve the status code, because
// classification (IsTransient / IsNotFound / IsConflict) keys off it.
func TestNewAPIError_AllObservedShapes(t *testing.T) {
	t.Run("v1 wraps the pair in an errors array", func(t *testing.T) {
		e := newAPIError(http.StatusForbidden, "rid",
			[]byte(`{"errors":[{"message":"Insufficient permissions","reason":"unauthorized"}]}`))
		if e.StatusCode != http.StatusForbidden || e.Reason != "unauthorized" ||
			e.Message != "Insufficient permissions" {
			t.Errorf("v1 shape mis-parsed: %+v", e)
		}
	})

	t.Run("v2beta1 returns the pair BARE", func(t *testing.T) {
		e := newAPIError(http.StatusConflict, "rid",
			[]byte(`{"message":"Only multi database Instances can add databases","reason":"multi-db-only"}`))
		if e.Reason != "multi-db-only" {
			t.Errorf("Reason must populate from the bare v2beta1 shape, got %q", e.Reason)
		}
		// The user sees Message in their CR status — it must be the sentence, not
		// the raw JSON blob.
		if e.Message != "Only multi database Instances can add databases" {
			t.Errorf("Message must be the sentence, not a JSON blob, got %q", e.Message)
		}
		if strings.Contains(e.Message, "{") {
			t.Errorf("Message leaked raw JSON: %q", e.Message)
		}
	})

	t.Run("v2beta1 schema failures are PLAIN TEXT, not JSON", func(t *testing.T) {
		// Exactly what the restore endpoint returned for a body missing `id`.
		e := newAPIError(http.StatusBadRequest, "rid", []byte(`- at '': missing property 'id'`))
		if e.StatusCode != http.StatusBadRequest {
			t.Errorf("status code must survive an unparseable body, got %d", e.StatusCode)
		}
		if !strings.Contains(e.Message, "missing property 'id'") {
			t.Errorf("plain-text body must be surfaced verbatim, got %q", e.Message)
		}
		// 400 is permanent: it must NOT be classified as retryable.
		if IsTransient(e) {
			t.Error("a 400 must not be transient, or the operator retries a permanent failure forever")
		}
	})

	t.Run("gateway shape", func(t *testing.T) {
		e := newAPIError(http.StatusBadGateway, "rid", []byte(`{"error":"upstream unavailable"}`))
		if e.Message != "upstream unavailable" {
			t.Errorf("gateway shape mis-parsed: %+v", e)
		}
		if !IsTransient(e) {
			t.Error("502 must be transient")
		}
	})

	t.Run("empty body still yields a usable error", func(t *testing.T) {
		e := newAPIError(http.StatusInternalServerError, "rid", nil)
		if e.Message == "" || e.StatusCode != http.StatusInternalServerError {
			t.Errorf("empty body must fall back to status text: %+v", e)
		}
	})
}

// The multi-db-only refusal arrives as a 409, the same status the API uses for
// "another operation is in flight". Only one of those is worth retrying, and
// conflating them is what made the AuraDatabase controller requeue a permanent
// failure every 30 seconds while never explaining it.
func TestMultiDatabaseOnlyIsTerminalNotAConflict(t *testing.T) {
	refusal := newAPIError(http.StatusConflict, "rid",
		[]byte(`{"message":"Only multi database Instances can add databases","reason":"multi-db-only"}`))

	if !IsMultiDatabaseOnly(refusal) {
		t.Fatalf("IsMultiDatabaseOnly must recognise reason %q", refusal.Reason)
	}
	if IsConflict(refusal) {
		t.Error("multi-db-only must NOT be a retryable conflict: multi_database is fixed at instance creation, " +
			"so no amount of retrying can make the create succeed")
	}
	if IsTransient(refusal) {
		t.Error("a 409 must not be transient")
	}

	// A real in-flight conflict must keep behaving as one.
	inFlight := newAPIError(http.StatusConflict, "rid",
		[]byte(`{"errors":[{"message":"The instance x is currently undergoing an operation: creating",`+
			`"reason":"ongoing-database-operation"}]}`))
	if !IsConflict(inFlight) {
		t.Error("ongoing-database-operation must still classify as a retryable conflict")
	}
	if IsMultiDatabaseOnly(inFlight) {
		t.Error("ongoing-database-operation is not a multi-database refusal")
	}
}
