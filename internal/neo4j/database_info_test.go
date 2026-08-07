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

package neo4j

import "testing"

// TestColumnString covers the NULL handling for SHOW DATABASES columns that
// are surfaced into CR status. A naive fmt.Sprintf renders a nil column as the
// literal "<nil>", which would leak into `kubectl describe` output and break
// equality checks against "".
func TestColumnString(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil becomes empty", nil, ""},
		{"string passes through", "replica", "replica"},
		{"empty string stays empty", "", ""},
		{"bool renders", true, "true"},
		{"int renders", int64(42), "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := columnString(tt.in); got != tt.want {
				t.Errorf("columnString(%#v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
