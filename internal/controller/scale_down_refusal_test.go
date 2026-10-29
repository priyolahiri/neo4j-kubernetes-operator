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

// The advice of a refused deallocation follows what Neo4j names
// (operations.md id 123). The v1.24.0 journey (#619): a 5 → 3 scale-down of a
// cluster with a 4-primary database was refused by the dry run, and the
// ScaleDownBlocked message advised giving single-primary databases an
// additional primary; the same refusal was announced twice a second apart,
// the two servers named in either order.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// neo4jRefusal is a refused DRYRUN DEALLOCATE as the client returns it: the
// statement, then the driver's error around Neo4j's message.
func neo4jRefusal(ids []string, inner string) error {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + id + "'"
	}
	return fmt.Errorf("DRYRUN DEALLOCATE DATABASES FROM SERVER %s: Neo4jError: Neo.ClientError.Statement.ArgumentError (Could not deallocate server(s) '%s'. %s)",
		strings.Join(quoted, ", "), strings.Join(ids, ","), inner)
}

// The refusals Neo4j gives, verbatim: the topology refusal as the v1.24.0
// journey's operator log recorded it (2026.08.1), its 5.26 form, and the
// single-primary refusal as neo4j-dbms-enterprise words it (5.26.32 and
// 2026.09.0: "Can't move databases with only one primary: %s.").
const (
	wideRefusal2026 = "Unable to reallocate 'wide(PRIMARY)'.\nRequired topology for 'wide' is 4 primaries and 0 secondaries.\n" +
		"Consider running SHOW SERVERS to determine what action is suitable to resolve this issue.\n"
	wideRefusal526 = "Unable to reallocate 'wide'.\nRequired topology for 'wide' is 4 primaries and 0 secondaries.\n" +
		"Consider running SHOW SERVERS to determine what action is suitable to resolve this issue.\n"
)

func TestDeallocateRefusal_AdviceFollowsTheCause(t *testing.T) {
	ids := []string{"14237a2a", "e4f1c8b7"}
	cases := []struct {
		name     string
		inner    string
		cordoned bool
		want     []string
		wantNot  []string
	}{
		{
			name:  "a 4-primary database, 2026.08",
			inner: wideRefusal2026,
			want: []string{
				"Database 'wide' needs 4 primaries and 0 secondaries, each on a server of its own",
				"ALTER DATABASE wide SET TOPOLOGY",
				"(at least 4)",
				"No server has been cordoned; replicas are held until this is resolvable.",
			},
			wantNot: []string{"single-primary", "only primary", "second primary"},
		},
		{
			name:     "a 4-primary database, 5.26, after the cordon",
			inner:    wideRefusal526,
			cordoned: true,
			want: []string{
				"Database 'wide' needs 4 primaries and 0 secondaries",
				"Replicas are held until this is resolvable.",
			},
			wantNot: []string{"second primary", "No server has been cordoned"},
		},
		{
			name:    "a single-primary database",
			inner:   "Can't move databases with only one primary: neo4j.",
			want:    []string{"Neo4j cannot move a database's only primary, and neo4j has one", "ALTER DATABASE neo4j SET TOPOLOGY 2 PRIMARIES"},
			wantNot: []string{"needs 4 primaries", "Either a database"},
		},
		{
			name:  "single-primary databases, listed",
			inner: "Can't move databases with only one primary: [neo4j, orders].",
			want:  []string{"and neo4j, orders has one", "ALTER DATABASE neo4j SET TOPOLOGY 2 PRIMARIES"},
		},
		{
			name:  "a cause it does not name",
			inner: "Cannot change allocations for database 'orders' because it is offline.",
			want: []string{
				"Either a database needs more servers than the scale-down leaves",
				"or a database has its only primary on a server the scale-down removes",
				"offline",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := deallocateRefusal(3, neo4jRefusal(ids, tc.inner), tc.cordoned)
			assert.True(t, strings.HasPrefix(msg, "Scale-down to 3 server(s) is blocked: DEALLOCATE dry-run failed: DRYRUN DEALLOCATE"), msg)
			assert.Contains(t, msg, "The operator never changes a database's topology itself.")
			for _, w := range tc.want {
				assert.Contains(t, msg, w)
			}
			for _, w := range tc.wantNot {
				assert.NotContains(t, msg, w)
			}
		})
	}
}

// Through the drain: the 4-primary refusal of the journey, on every pass the
// same message, so one ScaleDownBlocked Warning, and nothing cordoned.
func TestScaleDownDrain_ARefusedDryRunSaysWhyOnce(t *testing.T) {
	neo := newDrainNeo4j("wide", 5)
	neo.refuseDryRun = func(ids []string) error { return neo4jRefusal(ids, wideRefusal2026) }
	r, fc, c := scalingDown(t, "wide", neo, nil)

	for range 20 {
		drainPass(t, r, fc, c)
	}

	assert.Empty(t, opsOf(neo, "cordon"), "nothing is cordoned")
	pending := pendingDrain(t, fc, c)
	require.NotNil(t, pending)
	assert.Equal(t, metav1.ConditionTrue, pending.Status)
	assert.Equal(t, ConditionReasonScaleDownBlocked, pending.Reason)
	assert.Contains(t, pending.Message, "DRYRUN DEALLOCATE DATABASES FROM SERVER 'uuid-3', 'uuid-4'")
	assert.Contains(t, pending.Message, "Database 'wide' needs 4 primaries")
	assert.NotContains(t, pending.Message, "single-primary")
	blocked := eventsWithReason(events(r.Recorder), EventReasonScaleDownBlocked)
	assert.Len(t, blocked, 1, "one refusal, one Warning: the servers are named in one order")
}

func TestDeallocateRefusal_KeepsNeo4jsError(t *testing.T) {
	err := errors.New("boom")
	msg := deallocateRefusal(4, err, false)
	assert.Contains(t, msg, "DEALLOCATE dry-run failed: boom. Either a database")
}
