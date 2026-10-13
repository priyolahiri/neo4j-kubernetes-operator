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

package resources

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func connectTimeoutLines(conf string) []string {
	var out []string
	for _, line := range strings.Split(conf, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "dbms.cluster.network.connect_timeout=") {
			out = append(out, strings.TrimSpace(line))
		}
	}
	return out
}

// A replaced pod's old IP can still be resolved by the other servers for up
// to 30s, and every connection attempt to it waits out the connect timeout:
// the operator lowers Neo4j's 30s to 5s on both lines.
func TestClusterConfLowersTheClusterConnectTimeout(t *testing.T) {
	for _, tag := range []string{"5.26.31-enterprise", "2026.08.1-enterprise"} {
		c := languageCluster(tag)
		assert.Equal(t, []string{"dbms.cluster.network.connect_timeout=5s"}, connectTimeoutLines(buildNeo4jConfigForEnterprise(c)), tag)
	}
}

// spec.config overrides it, leaving one line: a key declared twice stops a
// CalVer server from starting.
func TestClusterConnectTimeoutIsOverridable(t *testing.T) {
	c := languageCluster("2026.08.1-enterprise")
	c.Spec.Config = map[string]string{"dbms.cluster.network.connect_timeout": "30s"}
	assert.Equal(t, []string{"dbms.cluster.network.connect_timeout=30s"}, connectTimeoutLines(buildNeo4jConfigForEnterprise(c)))
}
