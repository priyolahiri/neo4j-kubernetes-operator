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
	"testing"

	"github.com/stretchr/testify/assert"

	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// #594: a role whose only unattributed state was a statement granted through
// an alias — `current` pointing at `other`, which another statement of the
// role already granted — read "cannot attribute 0 privilege row(s)", naming
// neither the statement nor the alias. The message names the cause and what
// clears it.
func TestUnattributedMessage_NamesTheAmbiguousStatementAndItsAlias(t *testing.T) {
	aliases := []neo4jclient.AliasInfo{{Name: "current", Database: "other", Location: "local"}}
	stmt := "GRANT ACCESS ON DATABASE current TO stored_form"

	msg := unattributedMessage(nil, []string{stmt}, aliases)
	assert.NotContains(t, msg, "0 privilege row(s)", "no rows are kept; the statement is the cause")
	assert.Contains(t, msg, stmt)
	assert.Contains(t, msg, "`current` is an alias for database `other`")
	assert.Contains(t, msg, "grant on the target database instead of the alias")
	assert.Contains(t, msg, "--privilege-normalisation=probe")

	// Without an alias, the advice is the duplicate statement alone.
	msg = unattributedMessage(nil, []string{"GRANT ACCESS ON DATABASE other TO stored_form"}, aliases)
	assert.NotContains(t, msg, "alias")
	assert.Contains(t, msg, "remove the statement another one already covers")

	// Rows learn mode keeps are still counted and listed.
	msg = unattributedMessage([]string{"GRANT TRAVERSE ON GRAPH * TO stored_form"}, nil, nil)
	assert.Contains(t, msg, "cannot attribute 1 privilege row(s)")
	assert.Contains(t, msg, "GRANT TRAVERSE ON GRAPH * TO stored_form")
}
