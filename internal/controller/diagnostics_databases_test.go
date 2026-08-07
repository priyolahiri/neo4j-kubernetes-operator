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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

func TestToDatabaseDiagnostics_Empty(t *testing.T) {
	if got := toDatabaseDiagnostics(nil); got != nil {
		t.Errorf("expected nil for empty input, got %#v", got)
	}
	if got := toDatabaseDiagnostics([]neo4jclient.DatabaseInfo{}); got != nil {
		t.Errorf("expected nil for empty slice, got %#v", got)
	}
}

// TestToDatabaseDiagnostics_CarriesEveryField is the guard that a field added
// to DatabaseInfo actually reaches CR status. The Type/Access/Writer columns
// exist so the operator can tell a cross-cluster replica from an ordinary
// database; dropping them in the mapping would silently defeat that.
func TestToDatabaseDiagnostics_CarriesEveryField(t *testing.T) {
	in := []neo4jclient.DatabaseInfo{{
		Name:            "mydb",
		Status:          "online",
		RequestedStatus: "online",
		Role:            "primary",
		Default:         true,
		Type:            "standard",
		Access:          "read-write",
		Writer:          true,
	}}

	got := toDatabaseDiagnostics(in)
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	d := got[0]

	if d.Name != "mydb" || d.Status != "online" || d.RequestedStatus != "online" {
		t.Errorf("name/status not carried: %#v", d)
	}
	if d.Role != "primary" || !d.Default {
		t.Errorf("role/default not carried: %#v", d)
	}
	if d.Type != "standard" {
		t.Errorf("Type not carried: got %q, want %q", d.Type, "standard")
	}
	if d.Access != "read-write" {
		t.Errorf("Access not carried: got %q, want %q", d.Access, "read-write")
	}
	if !d.Writer {
		t.Error("Writer not carried: got false, want true")
	}
}

// TestToDatabaseDiagnostics_Replica pins the shape a cross-cluster replica
// presents (Neo4j 2026.08+): type "replica", read-only access, and writer
// false on every copy — including the primaries, which is the part that
// surprises people.
func TestToDatabaseDiagnostics_Replica(t *testing.T) {
	in := []neo4jclient.DatabaseInfo{{
		Name:            "foo-replica",
		Status:          "online",
		RequestedStatus: "online",
		Role:            "primary",
		Type:            "replica",
		Access:          "read-only",
		Writer:          false,
	}}

	d := toDatabaseDiagnostics(in)[0]

	if d.Type != "replica" {
		t.Errorf("expected type replica, got %q", d.Type)
	}
	if d.Access != "read-only" {
		t.Errorf("expected read-only access, got %q", d.Access)
	}
	if d.Writer {
		t.Error("a replica primary must not report writer=true")
	}
}

// TestToDatabaseDiagnostics_ShardTypesPassThrough covers the other values the
// `type` column can carry — the sharding types added in 2025.12 and composite
// — so the mapping stays a pass-through rather than growing an allowlist that
// would silently drop a value introduced by a future Neo4j release.
func TestToDatabaseDiagnostics_ShardTypesPassThrough(t *testing.T) {
	in := []neo4jclient.DatabaseInfo{
		{Name: "sharded", Type: "graph shard", Access: "read-write"},
		{Name: "props", Type: "property shard", Access: "read-write"},
		{Name: "composite-db", Type: "composite", Access: "read-only"},
	}

	got := toDatabaseDiagnostics(in)
	for i, want := range []string{"graph shard", "property shard", "composite"} {
		if got[i].Type != want {
			t.Errorf("row %d: expected type %q, got %q", i, want, got[i].Type)
		}
	}
}

// TestUpdateDatabasesCondition_ReplicaIsHealthy guards the interaction
// between the new columns and the existing health check: a replica is
// permanently read-only, but it is still online, so DatabasesHealthy must
// stay True. The check compares Status against RequestedStatus and must not
// start treating read-only access or writer=false as degraded.
func TestUpdateDatabasesCondition_ReplicaIsHealthy(t *testing.T) {
	qm := &QueryMonitor{}
	cluster := diagTestCluster()
	databases := []neo4jclient.DatabaseInfo{
		{Name: "system", Status: "online", RequestedStatus: "online", Type: "system"},
		{
			Name: "foo-replica", Status: "online", RequestedStatus: "online",
			Type: "replica", Access: "read-only", Writer: false,
		},
	}

	qm.updateDatabasesCondition(cluster, databases, nil)

	cond := findCondByType(cluster, ConditionTypeDatabasesHealthy)
	if cond == nil {
		t.Fatal("expected DatabasesHealthy condition")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("an online read-only replica must not be reported unhealthy; got %s (%s)",
			cond.Status, cond.Message)
	}
}
