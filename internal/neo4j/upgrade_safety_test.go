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

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthyClusterDriver answers the pre-upgrade check's queries as a healthy
// three-server cluster on 5.26.31 would.
type healthyClusterDriver struct {
	neo4j.DriverWithContext
}

func (d *healthyClusterDriver) NewSession(context.Context, neo4j.SessionConfig) neo4j.SessionWithContext {
	return &healthyClusterSession{}
}

type healthyClusterSession struct {
	neo4j.SessionWithContext
}

func (s *healthyClusterSession) Run(_ context.Context, cypher string, _ map[string]any, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	switch {
	case strings.Contains(cypher, "SHOW SERVERS"):
		var rows []*neo4j.Record
		for _, address := range []string{"s-0:7687", "s-1:7687", "s-2:7687"} {
			rows = append(rows, &neo4j.Record{
				Keys:   []string{"name", "address", "state", "health", "hosting", "version"},
				Values: []any{address, address, "Enabled", "Available", []any{"system", "neo4j"}, "5.26.31"},
			})
		}
		return &listingResult{records: rows}, nil
	case strings.Contains(cypher, "SHOW DATABASES"):
		return &listingResult{records: []*neo4j.Record{{Keys: []string{"address"}, Values: []any{"s-0:7687"}}}}, nil
	case strings.Contains(cypher, "dbms.components"):
		return &listingResult{records: []*neo4j.Record{{Keys: []string{"version"}, Values: []any{"5.26.31"}}}}, nil
	}
	return &listingResult{}, nil
}

func (s *healthyClusterSession) Close(context.Context) error { return nil }

// The pre-upgrade check asked dbms.components() for the version and refused a
// target equal to it as "the same as current version". For an "-enterprise"
// tag the two never matched, so it refused nothing; for a bare tag
// ("5.26.31"), moving the image to another registry under the same tag was
// refused and the upgrade Failed (#538). The version is the version rule's to
// judge; this check judges health.
func TestValidateUpgradeSafety_JudgesHealthNotTheTag(t *testing.T) {
	c := &Client{driver: &healthyClusterDriver{}, circuitBreaker: newCircuitBreaker(), poolMetrics: newConnectionPoolMetrics()}
	require.NoError(t, c.ValidateUpgradeSafety(context.Background()),
		"a healthy cluster is safe to upgrade, whatever image it moves to")
	servers, err := c.GetServerList(context.Background())
	require.NoError(t, err)
	require.Len(t, servers, 3)
	assert.Equal(t, "5.26.31", servers[0].Version, "what the state machine holds the target to")
}
