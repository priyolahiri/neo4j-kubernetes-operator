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

package controller

import (
	stderrors "errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

// invalidQuantityError is a user-supplied resource quantity that is not a
// usable Kubernetes quantity. It is a statement about the SPEC, not about the
// cluster: retrying cannot fix it, only an edit can, so a reconciler that sees
// one reports phase Invalid (or Failed, where the kind has no Invalid) and
// waits for the edit rather than returning the error for a backoff retry.
type invalidQuantityError struct {
	path   string // spec field path, e.g. spec.storage.pvc.size
	value  string
	reason string
}

func (e *invalidQuantityError) Error() string {
	return fmt.Sprintf("%s %q %s", e.path, e.value, e.reason)
}

// isInvalidQuantity reports whether err is, or wraps, an invalidQuantityError.
func isInvalidQuantity(err error) bool {
	var iq *invalidQuantityError
	return stderrors.As(err, &iq)
}

// parseUserQuantity parses a resource quantity that came from a spec field and
// requires it to be greater than zero (a PVC request of 0 is rejected by the
// apiserver anyway, with a worse message). path names the field for the error.
//
// Use this instead of resource.MustParse for ANY value a user typed:
// MustParse panics on a malformed quantity, and a panic in a reconciler takes
// the whole manager down, restart-looping every other CR. A field being guarded
// elsewhere — an inline validator, a CRD pattern — is not a reason to rely on it:
// a CR stored before a pattern was tightened, or any code path that skips the
// validator, still arrives here. MustParse remains correct for constant literals
// ("100m", "2Gi"), which cannot be malformed.
func parseUserQuantity(path, value string) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, &invalidQuantityError{
			path: path, value: value,
			reason: "is not a valid Kubernetes quantity (e.g. \"50Gi\")",
		}
	}
	if q.Sign() <= 0 {
		return resource.Quantity{}, &invalidQuantityError{
			path: path, value: value, reason: "must be greater than zero",
		}
	}
	return q, nil
}
