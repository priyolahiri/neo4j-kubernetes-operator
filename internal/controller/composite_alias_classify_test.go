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

// A constituent and an ordinary alias whose name merely contains a dot are
// indistinguishable by name and are NOT the same thing. Telling them apart by
// prefix made the operator try to drop the lookalike as a constituent — a DROP
// that matched nothing, with IF EXISTS swallowing the miss — and then announce
// a removal that had not happened, on every reconcile, forever.
//
// Both shapes were observed live on 5.26.30 and 2026.08.1: SHOW ALIASES
// returns composite="cineasts" for a real constituent and NULL for a plain
// alias of the same shape.
func TestClassifyAliases(t *testing.T) {
	alias := func(name, composite string) neo4jclient.AliasInfo {
		return neo4jclient.AliasInfo{Name: name, Composite: composite, Database: "target"}
	}

	tests := []struct {
		name       string
		live       []neo4jclient.AliasInfo
		composite  string
		wantKeys   []string
		wantLookal []string
	}{
		{
			name:      "a constituent is keyed by its short name",
			live:      []neo4jclient.AliasInfo{alias("cineasts.latest", "cineasts")},
			composite: "cineasts",
			wantKeys:  []string{"latest"},
		},
		{
			// The bug: composite is NULL, so this is an ordinary alias.
			name:       "a plain alias that looks like a constituent is not one",
			live:       []neo4jclient.AliasInfo{alias("cineasts.fake", "")},
			composite:  "cineasts",
			wantKeys:   nil,
			wantLookal: []string{"cineasts.fake"},
		},
		{
			name: "both at once, told apart by the column and not the name",
			live: []neo4jclient.AliasInfo{
				alias("cineasts.latest", "cineasts"),
				alias("cineasts.fake", ""),
			},
			composite:  "cineasts",
			wantKeys:   []string{"latest"},
			wantLookal: []string{"cineasts.fake"},
		},
		{
			// A constituent of a DIFFERENT composite that happens to share our
			// prefix is still not ours — and dropping it would break that one.
			name:       "another composite's constituent is never ours",
			live:       []neo4jclient.AliasInfo{alias("cineasts.latest", "other")},
			composite:  "cineasts",
			wantKeys:   nil,
			wantLookal: []string{"cineasts.latest"},
		},
		{
			name:      "an unrelated alias is ignored entirely",
			live:      []neo4jclient.AliasInfo{alias("reporting", ""), alias("other.thing", "other")},
			composite: "cineasts",
		},
		{
			// "cineastsX" shares no dot boundary with "cineasts." and must not
			// be swept in by a sloppier prefix test.
			name:      "a name that merely starts with the composite is not in its namespace",
			live:      []neo4jclient.AliasInfo{alias("cineastsX", ""), alias("cineastsX.y", "cineastsX")},
			composite: "cineasts",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			existing, lookalikes := classifyAliases(tc.live, tc.composite)
			keys := make([]string, 0, len(existing))
			for k := range existing {
				keys = append(keys, k)
			}
			assert.ElementsMatch(t, tc.wantKeys, keys, "constituents")
			assert.Equal(t, tc.wantLookal, lookalikes, "lookalikes")
		})
	}
}

// Lookalikes are reported in a stable order: the status message embeds them,
// and an unstable order would rewrite status on every reconcile.
func TestClassifyAliases_LookalikesAreSorted(t *testing.T) {
	live := []neo4jclient.AliasInfo{
		{Name: "c.zebra"}, {Name: "c.alpha"}, {Name: "c.mid"},
	}
	_, lookalikes := classifyAliases(live, "c")
	assert.Equal(t, []string{"c.alpha", "c.mid", "c.zebra"}, lookalikes)
}
