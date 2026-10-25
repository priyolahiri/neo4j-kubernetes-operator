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
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Every Aura response carries a request ID of its own, so the text of a
// refusal that repeats differs on every retry; a status message built from it
// changed on every pass. MessageWithoutRequestID is the text without it.
func TestMessageWithoutRequestID(t *testing.T) {
	first := &APIError{StatusCode: 403, Reason: "forbidden", Message: "no", RequestID: "req-1"}
	second := &APIError{StatusCode: 403, Reason: "forbidden", Message: "no", RequestID: "req-2"}
	noReason := &APIError{StatusCode: 403, Message: "no", RequestID: "req-3"}

	cases := []struct {
		name string
		a, b error
	}{
		{"bare", first, second},
		{"wrapped", fmt.Errorf("listing: %w", first), fmt.Errorf("listing: %w", second)},
		{"joined", fmt.Errorf("post: %w; rotate: %w", first, noReason), fmt.Errorf("post: %w; rotate: %w", second, noReason)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MessageWithoutRequestID(tc.a)
			if got != MessageWithoutRequestID(tc.b) {
				t.Errorf("the same refusal reads differently: %q vs %q", got, MessageWithoutRequestID(tc.b))
			}
			for _, id := range []string{"req-1", "req-2", "req-3", "request-id"} {
				if strings.Contains(got, id) {
					t.Errorf("%q still carries %q", got, id)
				}
			}
			if !strings.Contains(got, `message="no"`) {
				t.Errorf("%q lost the rest of the error", got)
			}
		})
	}

	if got := MessageWithoutRequestID(errors.New("dial tcp: connection refused")); got != "dial tcp: connection refused" {
		t.Errorf("an error without an APIError changed: %q", got)
	}
	if got := MessageWithoutRequestID(nil); got != "" {
		t.Errorf("nil = %q", got)
	}
}
