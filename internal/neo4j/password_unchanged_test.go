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

import (
	"errors"
	"fmt"
	"testing"
)

// The verbatim error Neo4j 2026.06 returned in the CI failure this fix came
// from (PR #337, 2026.06 lane). Kept exact: the classifier matches on the
// message, so a paraphrase would not prove anything.
const realNeo4jPasswordUnchanged = "Neo4jError: Neo.ClientError.Statement.ArgumentError " +
	"(Failed to alter the specified user 'appuser': Old password and new password cannot be the same.)"

func TestIsPasswordUnchangedError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"the real CI error", errors.New(realNeo4jPasswordUnchanged), true},
		{
			"wrapped, as the controller sees it",
			fmt.Errorf("failed to alter user appuser: %w", errors.New(realNeo4jPasswordUnchanged)),
			true,
		},
		{
			"a DIFFERENT ArgumentError must not be swallowed",
			errors.New("Neo4jError: Neo.ClientError.Statement.ArgumentError (Invalid input 'x')"),
			false,
		},
		{
			"the right message under the wrong code is not this condition",
			errors.New("Neo.ClientError.Security.Forbidden (Old password and new password cannot be the same.)"),
			false,
		},
		{"unrelated error", errors.New("connection refused"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPasswordUnchangedError(tc.err); got != tc.want {
				t.Errorf("IsPasswordUnchangedError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
