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
	"context"
	"strings"
	"testing"
)

// TestBuildCreateReplicaFromNetworkCypher_ParameterisesAddresses pins the
// Cypher-injection defense: the upstream name and addresses must become
// driver parameters, never appear in the statement text.
func TestBuildCreateReplicaFromNetworkCypher_ParameterisesAddresses(t *testing.T) {
	src := ReplicaNetworkSource{
		UpstreamDatabase: "foo",
		Addresses:        []string{"upstream-0.example.com:6000", "upstream-1.example.com:6000"},
	}
	query, params := buildCreateReplicaFromNetworkCypher("foo-replica", src)

	if strings.Contains(query, "upstream-0.example.com") {
		t.Fatalf("address leaked into query text: %q", query)
	}
	if !strings.Contains(query, "replicaConfig: {remote: $remote, addresses: $addresses}") {
		t.Fatalf("expected parameterised replicaConfig, got: %q", query)
	}
	if !strings.Contains(query, "CREATE REPLICA DATABASE `foo-replica`") {
		t.Fatalf("expected the replica name to be interpolated, got: %q", query)
	}
	if !strings.Contains(query, "WAIT") {
		t.Fatalf("expected WAIT, got: %q", query)
	}
	if params["remote"] != "foo" {
		t.Fatalf("expected remote param %q, got: %v", "foo", params["remote"])
	}
	addrs, ok := params["addresses"].([]string)
	if !ok || len(addrs) != 2 {
		t.Fatalf("expected addresses param to carry both addresses, got: %v", params["addresses"])
	}
}

// TestBuildCreateReplicaFromNetworkCypher_Topology pins the TOPOLOGY clause
// shape, singular/plural keywords included, matching topologyClause's own
// rules (shared with the backup-mode builder).
func TestBuildCreateReplicaFromNetworkCypher_Topology(t *testing.T) {
	tests := []struct {
		name        string
		primaries   int32
		secondaries int32
		wantClause  string
	}{
		{"no topology requested", 0, 0, ""},
		{"single primary only", 1, 0, "TOPOLOGY 1 PRIMARY"},
		{"multiple primaries and secondaries", 3, 2, "TOPOLOGY 3 PRIMARIES 2 SECONDARIES"},
		{"multiple primaries single secondary", 3, 1, "TOPOLOGY 3 PRIMARIES 1 SECONDARY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := ReplicaNetworkSource{
				UpstreamDatabase: "foo",
				Addresses:        []string{"upstream-0.example.com:6000"},
				Primaries:        tt.primaries,
				Secondaries:      tt.secondaries,
			}
			query, _ := buildCreateReplicaFromNetworkCypher("foo-replica", src)
			if tt.wantClause == "" {
				if strings.Contains(query, "TOPOLOGY") {
					t.Fatalf("expected no TOPOLOGY clause, got: %q", query)
				}
				return
			}
			if !strings.Contains(query, tt.wantClause) {
				t.Fatalf("expected clause %q, got: %q", tt.wantClause, query)
			}
		})
	}
}

// TestBuildCreateReplicaFromNetworkCypher_EscapesBacktick guards the same
// Cypher-injection vector as the backup-mode path: the database name is
// interpolated (admin DDL accepts no parameter there), so a stray backtick
// must be escaped rather than allowed to close the identifier early.
func TestBuildCreateReplicaFromNetworkCypher_EscapesBacktick(t *testing.T) {
	src := ReplicaNetworkSource{
		UpstreamDatabase: "foo",
		Addresses:        []string{"upstream-0.example.com:6000"},
	}
	query, _ := buildCreateReplicaFromNetworkCypher("foo`; DROP DATABASE neo4j; //", src)
	if strings.Contains(query, "foo`; DROP DATABASE") {
		t.Fatalf("backtick was not escaped: %q", query)
	}
}

func TestCreateReplicaDatabaseFromNetwork_RequiresUpstreamDatabase(t *testing.T) {
	c := &Client{}
	err := c.CreateReplicaDatabaseFromNetwork(context.TODO(), "foo-replica", ReplicaNetworkSource{
		Addresses: []string{"upstream-0.example.com:6000"},
	})
	if err == nil {
		t.Fatal("expected an error when upstream database is missing")
	}
}

func TestCreateReplicaDatabaseFromNetwork_RequiresAddresses(t *testing.T) {
	c := &Client{}
	err := c.CreateReplicaDatabaseFromNetwork(context.TODO(), "foo-replica", ReplicaNetworkSource{
		UpstreamDatabase: "foo",
	})
	if err == nil {
		t.Fatal("expected an error when no addresses are supplied")
	}
}
