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
	"strings"
	"testing"
)

const testAuraURI = "neo4j+s://d3adb33f.databases.neo4j.io"

func TestBuildAuraConnectionData_DefaultDriverFormat(t *testing.T) {
	data := buildAuraConnectionData("neo4j-driver", testAuraURI, "neo4j", "s3cr3t", "d3adb33f", "prod")

	// Uppercase driver env keys, envFrom-safe.
	want := map[string]string{
		"NEO4J_URI":         testAuraURI,
		"NEO4J_USERNAME":    "neo4j",
		"NEO4J_PASSWORD":    "s3cr3t",
		"NEO4J_DATABASE":    "neo4j",
		"AURA_INSTANCEID":   "d3adb33f",
		"AURA_INSTANCENAME": "prod",
		// Minimal Service Binding compliance on every format.
		"type":     "neo4j",
		"provider": "aura",
	}
	for k, v := range want {
		if got := string(data[k]); got != v {
			t.Errorf("key %q = %q, want %q", k, got, v)
		}
	}
	// The default format must NOT carry the dotenv blob or a JDBC URL.
	if _, ok := data["credentials.env"]; ok {
		t.Error("neo4j-driver format must not include credentials.env blob")
	}
	if _, ok := data["NEO4J_JDBC_URL"]; ok {
		t.Error("neo4j-driver format must not include NEO4J_JDBC_URL")
	}
}

func TestBuildAuraConnectionData_JDBC(t *testing.T) {
	data := buildAuraConnectionData("jdbc", testAuraURI, "neo4j", "pw", "id", "name")
	if got := string(data["NEO4J_JDBC_URL"]); got != "jdbc:neo4j:"+testAuraURI {
		t.Errorf("NEO4J_JDBC_URL = %q, want jdbc:neo4j:%s", got, testAuraURI)
	}
}

func TestBuildAuraConnectionData_AuraDotenv(t *testing.T) {
	data := buildAuraConnectionData("aura-dotenv", testAuraURI, "neo4j", "pw", "id", "name")
	blob := string(data["credentials.env"])
	if blob == "" {
		t.Fatal("aura-dotenv must include a credentials.env blob")
	}
	for _, line := range []string{"NEO4J_URI=" + testAuraURI, "NEO4J_PASSWORD=pw", "AURA_INSTANCEID=id"} {
		if !strings.Contains(blob, line) {
			t.Errorf("credentials.env missing %q; blob:\n%s", line, blob)
		}
	}
}

func TestBuildAuraConnectionData_ServiceBinding(t *testing.T) {
	data := buildAuraConnectionData("servicebinding", testAuraURI, "neo4j", "pw", "id", "name")
	// Lowercase SB-spec keys, no uppercase env noise.
	for _, k := range []string{"type", "provider", "uri", "username", "password", "database"} {
		if _, ok := data[k]; !ok {
			t.Errorf("servicebinding format missing key %q", k)
		}
	}
	if _, ok := data["NEO4J_URI"]; ok {
		t.Error("servicebinding format must not include uppercase NEO4J_URI")
	}
	if got := string(data["type"]); got != "neo4j" {
		t.Errorf("type = %q, want neo4j", got)
	}
}

func TestBuildAuraConnectionData_UsernameDefaultsAndEmptyPassword(t *testing.T) {
	// Empty username defaults to neo4j; empty password produces no key (rather
	// than an empty one), so a later observe can't clobber a real password.
	data := buildAuraConnectionData("neo4j-driver", testAuraURI, "", "", "id", "name")
	if got := string(data["NEO4J_USERNAME"]); got != "neo4j" {
		t.Errorf("username default = %q, want neo4j", got)
	}
	if _, ok := data["NEO4J_PASSWORD"]; ok {
		t.Error("empty password must not be written as a key")
	}
}

func TestExistingPassword(t *testing.T) {
	driver := map[string][]byte{"NEO4J_PASSWORD": []byte("dpw")}
	if got := existingPassword("neo4j-driver", driver); got != "dpw" {
		t.Errorf("driver existingPassword = %q, want dpw", got)
	}
	sb := map[string][]byte{"password": []byte("spw")}
	if got := existingPassword("servicebinding", sb); got != "spw" {
		t.Errorf("servicebinding existingPassword = %q, want spw", got)
	}
}

func TestParseNeo4jURI(t *testing.T) {
	cases := []struct {
		uri, host, scheme, port string
	}{
		{"neo4j+s://d3adb33f.databases.neo4j.io", "d3adb33f.databases.neo4j.io", "neo4j+s", "7687"},
		{"neo4j+s://host.example.com:9999", "host.example.com", "neo4j+s", "9999"},
		{"", "", "", ""},
	}
	for _, c := range cases {
		host, scheme, port := parseNeo4jURI(c.uri)
		if host != c.host || scheme != c.scheme || port != c.port {
			t.Errorf("parseNeo4jURI(%q) = (%q,%q,%q), want (%q,%q,%q)",
				c.uri, host, scheme, port, c.host, c.scheme, c.port)
		}
	}
}
