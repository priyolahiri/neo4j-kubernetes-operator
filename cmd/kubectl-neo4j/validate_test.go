package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeManifest writes content to a temp file and returns its path.
func writeManifest(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

const validCluster = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata:
  name: ok-cluster
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "5.26-enterprise"}
  topology: {servers: 3}
  storage: {size: 10Gi}
`

// TestOfflineValidatorsAreNilClientSafe is the load-bearing test for this
// command. Every kind in the `validators` map is a claim that its validator
// tolerates a nil Kubernetes client — three of them because their constructor
// takes no client, two because they accept one and never dereference it, and
// one because its single client call returns early on nil.
//
// Those are properties of code in internal/validation that this package does
// not own. If someone adds a client call to any of them, the CLI would panic in
// a user's terminal with no warning. This test converts that into a build-time
// failure here.
func TestOfflineValidatorsAreNilClientSafe(t *testing.T) {
	// A minimal document per kind — enough to decode and reach the validator.
	// Correctness of the RESULT is not asserted here; absence of a panic is.
	docs := map[string]string{
		"Neo4jEnterpriseCluster":    "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jEnterpriseCluster\nmetadata: {name: c}\nspec: {}\n",
		"Neo4jEnterpriseStandalone": "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jEnterpriseStandalone\nmetadata: {name: s}\nspec: {}\n",
		"Neo4jBackup":               "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jBackup\nmetadata: {name: b}\nspec: {}\n",
		"Neo4jPlugin":               "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jPlugin\nmetadata: {name: p}\nspec: {}\n",
		"Neo4jDatabaseAlias":        "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jDatabaseAlias\nmetadata: {name: a}\nspec: {}\n",
		"Neo4jReplicaDatabase":      "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jReplicaDatabase\nmetadata: {name: r}\nspec: {}\n",
	}

	require.Len(t, docs, len(validators),
		"every kind in the validators map needs a nil-client probe here — a new kind was added without one")

	for kind, fn := range validators {
		doc, ok := docs[kind]
		require.True(t, ok, "no nil-client probe for kind %q", kind)
		t.Run(kind, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, err := fn([]byte(doc))
				assert.NoError(t, err)
			}, "%s validator dereferenced a nil Kubernetes client", kind)
		})
	}
}

func TestValidate_CleanManifestExitsZero(t *testing.T) {
	path := writeManifest(t, validCluster)
	results, err := validateSource(path)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].findings, "expected no findings, got %+v", results[0].findings)
	assert.Equal(t, 0, results[0].errorCount())
}

func TestValidate_ReportsErrorsWithFieldPaths(t *testing.T) {
	path := writeManifest(t, `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata:
  name: bad-cluster
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "5.26-enterprise"}
  storage: {size: 10Gi}
  topology:
    servers: 3
    serverRoles:
      - {serverIndex: 7, modeConstraint: "PRIMARY"}
`)
	results, err := validateSource(path)
	require.NoError(t, err)
	require.Len(t, results, 1)

	require.NotEmpty(t, results[0].findings)
	var paths []string
	for _, f := range results[0].findings {
		paths = append(paths, f.path)
	}
	assert.Contains(t, strings.Join(paths, " "), "spec.topology.serverRoles[0].serverIndex",
		"field paths must survive into the rendered finding — they are the point of the command")
}

// The operator's own spec.config rules are the clearest example of a rule that
// `kubectl apply --dry-run=server` cannot catch, so it is pinned explicitly.
func TestValidate_CatchesRulesDryRunCannot(t *testing.T) {
	path := writeManifest(t, `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jEnterpriseCluster
metadata:
  name: cfg
spec:
  acceptLicenseAgreement: "yes"
  image: {repo: neo4j, tag: "5.26-enterprise"}
  storage: {size: 10Gi}
  topology: {servers: 3}
  config:
    dbms.default_database: "mydb"
    server.cluster.advertised_address: "foo:6000"
`)
	results, err := validateSource(path)
	require.NoError(t, err)
	require.Len(t, results, 1)

	joined := ""
	for _, f := range results[0].findings {
		joined += f.path + " " + f.detail + "\n"
	}
	assert.Contains(t, joined, "dbms.default_database")
	assert.Contains(t, joined, "server.cluster.advertised_address")
}

func TestValidate_MultiDocumentAndSkips(t *testing.T) {
	path := writeManifest(t, validCluster+`---
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jUser
metadata: {name: u}
spec: {clusterRef: {name: c}, username: u}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: unrelated}
`)
	results, err := validateSource(path)
	require.NoError(t, err)
	require.Len(t, results, 3)

	assert.Empty(t, results[0].skipped, "the cluster should be validated")
	assert.Contains(t, results[1].skipped, "cluster connection",
		"a cross-referencing kind must be skipped, not reported as failing")
	assert.Contains(t, results[2].skipped, "not a Neo4j operator resource",
		"non-Neo4j objects must pass through untouched")
}

func TestValidate_EmptyAndCommentOnlyDocumentsAreIgnored(t *testing.T) {
	path := writeManifest(t, "---\n# just a comment\n---\n"+validCluster)
	results, err := validateSource(path)
	require.NoError(t, err)
	require.Len(t, results, 1, "blank and comment-only documents must not produce results")
}

func TestExpandInputs_DirectoryWalk(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(validCluster), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.yml"), []byte(validCluster), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignored"), 0o600))

	got, err := expandInputs([]string{dir})
	require.NoError(t, err)
	assert.Len(t, got, 2, "only YAML/JSON extensions should be picked up")
}

func TestExpandInputs_MissingPathIsAUsageError(t *testing.T) {
	_, err := expandInputs([]string{filepath.Join(t.TempDir(), "nope.yaml")})
	assert.Error(t, err)
}
