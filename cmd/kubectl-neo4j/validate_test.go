package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
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
	// A minimal document per offline kind — enough to decode and reach the
	// validator. Correctness of the RESULT is not asserted here; absence of a
	// panic is.
	docs := map[string]string{
		"Neo4jEnterpriseCluster":    "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jEnterpriseCluster\nmetadata: {name: c}\nspec: {}\n",
		"Neo4jEnterpriseStandalone": "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jEnterpriseStandalone\nmetadata: {name: s}\nspec: {}\n",
		"Neo4jBackup":               "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jBackup\nmetadata: {name: b}\nspec: {}\n",
		"Neo4jPlugin":               "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jPlugin\nmetadata: {name: p}\nspec: {}\n",
		"Neo4jDatabaseAlias":        "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jDatabaseAlias\nmetadata: {name: a}\nspec: {}\n",
		"Neo4jReplicaDatabase":      "apiVersion: neo4j.neo4j.com/v1beta1\nkind: Neo4jReplicaDatabase\nmetadata: {name: r}\nspec: {}\n",
	}

	offline := map[string]kindValidator{}
	for kind, kv := range validators {
		if !kv.needsClient {
			offline[kind] = kv
		}
	}
	require.Len(t, docs, len(offline),
		"every offline kind needs a nil-client probe here — one was added without one")

	for kind, kv := range offline {
		doc, ok := docs[kind]
		require.True(t, ok, "no nil-client probe for offline kind %q", kind)
		t.Run(kind, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, _, err := kv.fn([]byte(doc), nil)
				assert.NoError(t, err)
			}, "%s validator dereferenced a nil Kubernetes client", kind)
		})
	}
}

// The set of kinds with operator-side validation is a fact about
// internal/validation, not about this command. If a validator is added there
// and not wired here, users silently get "no operator-side validator" for a
// kind that has one — a wrong answer that looks like a correct one.
func TestValidatorsCoverEveryKindWithAValidator(t *testing.T) {
	want := []string{
		// offline
		"Neo4jEnterpriseCluster", "Neo4jEnterpriseStandalone", "Neo4jBackup",
		"Neo4jPlugin", "Neo4jDatabaseAlias", "Neo4jReplicaDatabase",
		// need a cluster
		"Neo4jDatabase", "Neo4jUser", "Neo4jRole", "Neo4jRoleBinding",
		"Neo4jAuthRule", "Neo4jShardedDatabase",
	}
	assert.Len(t, validators, len(want),
		"validators map size changed — add or remove the kind in this list too, and check the count in the map's doc comment")
	for _, kind := range want {
		assert.Contains(t, validators, kind, "%s has a validator in internal/validation but is not wired here", kind)
	}
}

// Kinds needing a cluster must be SKIPPED offline, never reported as failing:
// "not found" for something merely unreachable is a false error.
func TestValidate_CrossReferenceKindsSkippedOffline(t *testing.T) {
	path := writeManifest(t, `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jUser
metadata: {name: analytics}
spec: {clusterRef: {name: prod}, username: analytics}
`)
	results, err := validateSource(path, nil, "")
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Empty(t, results[0].findings, "an unreachable cross-reference must not become an error")
	assert.Contains(t, results[0].skipped, "--connect",
		"the skip message must tell the user how to actually check this kind")
}

// A kind with NO operator-side validator must say so, and must NOT imply that
// connecting would help — an earlier version told users to connect for kinds
// that have no validator at all, sending them after something nonexistent.
func TestValidate_KindsWithoutValidatorsSayThatPlainly(t *testing.T) {
	path := writeManifest(t, `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jRestore
metadata: {name: restore-1}
spec: {}
`)
	results, err := validateSource(path, nil, "")
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Contains(t, results[0].skipped, "no operator-side validator")
	assert.Contains(t, results[0].skipped, "--dry-run=server",
		"users need pointing at the thing that DOES check these")
	assert.NotContains(t, results[0].skipped, "--connect",
		"connecting cannot help a kind with no validator; saying so would mislead")
}

func TestWithDefaultNamespace(t *testing.T) {
	doc := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n")
	patched := withDefaultNamespace(doc, "", "team-a")
	assert.Contains(t, string(patched), "team-a")

	// An explicit namespace in the manifest always wins.
	unchanged := withDefaultNamespace(doc, "already-set", "team-a")
	assert.Equal(t, string(doc), string(unchanged))
}

func TestValidate_CleanManifestExitsZero(t *testing.T) {
	path := writeManifest(t, validCluster)
	results, err := validateSource(path, nil, "")
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
	results, err := validateSource(path, nil, "")
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
	results, err := validateSource(path, nil, "")
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
	results, err := validateSource(path, nil, "")
	require.NoError(t, err)
	require.Len(t, results, 3)

	assert.Empty(t, results[0].skipped, "the cluster should be validated")
	assert.Contains(t, results[1].skipped, "--connect",
		"a cross-referencing kind must be skipped with a pointer to --connect, not reported as failing")
	assert.Contains(t, results[2].skipped, "not a Neo4j operator resource",
		"non-Neo4j objects must pass through untouched")
}

func TestValidate_EmptyAndCommentOnlyDocumentsAreIgnored(t *testing.T) {
	path := writeManifest(t, "---\n# just a comment\n---\n"+validCluster)
	results, err := validateSource(path, nil, "")
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

// testClient builds a fake cluster containing the given objects, with the same
// scheme newClusterClient uses. Cross-reference behaviour is testable without
// Kind or envtest, which keeps this command out of the integration lane and off
// the one-Enterprise-deployment-at-a-time budget entirely.
func testClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

const userManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jUser
metadata: {name: analytics, namespace: neo4j}
spec:
  clusterRef: prod
  username: analytics
  passwordSecretRef: {name: analytics-password, key: password}
`

// The whole point of --connect: a clusterRef that resolves must not be flagged.
func TestValidate_Connected_ResolvesClusterRef(t *testing.T) {
	c := testClient(t,
		&neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "analytics-password", Namespace: "neo4j"},
			Data:       map[string][]byte{"password": []byte("s3cret")},
		},
	)

	results, err := validateSource(writeManifest(t, userManifest), c, "")
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Empty(t, results[0].skipped, "with a client the kind must be validated, not skipped")
	assert.Equal(t, 0, results[0].errorCount(),
		"a resolvable clusterRef and an existing Secret must produce no errors, got %+v", results[0].findings)
}

// A missing cross-reference is only reportable WITH a connection — which is
// exactly why reporting it offline would have been a false error.
func TestValidate_Connected_FlagsMissingClusterRef(t *testing.T) {
	c := testClient(t) // empty cluster: no Neo4jEnterpriseCluster named "prod"

	results, err := validateSource(writeManifest(t, userManifest), c, "")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].skipped)
	assert.NotEmpty(t, results[0].findings,
		"a clusterRef pointing at a cluster that does not exist must be reported once we can actually tell")
}

// Pending is a THIRD outcome the operator models deliberately: a Secret that
// has not been applied yet is "not yet", not "wrong". It must not be rendered
// as an error, and must not affect the exit code.
func TestValidate_Connected_PendingIsNeitherErrorNorWarning(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
	}) // cluster exists, password Secret deliberately does not

	results, err := validateSource(writeManifest(t, userManifest), c, "")
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, 1, results[0].pendingCount(),
		"a not-yet-created password Secret must be reported as pending, got %+v", results[0].findings)
	assert.Equal(t, 0, results[0].errorCount(),
		"pending must not be counted as an error")

	// Even under --strict, pending must not fail the run.
	if code := report(results, os.Stdout, true /*strict*/, true /*quiet*/, true); code != exitOK {
		t.Errorf("pending findings must not fail the run even with --strict, got exit %d", code)
	}
}

// --namespace supplies the namespace for manifests that omit one, so
// cross-reference lookups resolve where the user would actually apply.
func TestValidate_Connected_DefaultNamespaceIsUsedForLookups(t *testing.T) {
	c := testClient(t, &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "team-a"},
	})

	noNamespace := `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jUser
metadata: {name: analytics}
spec:
  clusterRef: prod
  username: analytics
  passwordSecretRef: {name: pw, key: password}
`
	withNs, err := validateSource(writeManifest(t, noNamespace), c, "team-a")
	require.NoError(t, err)
	require.Len(t, withNs, 1)

	joined := ""
	for _, f := range withNs[0].findings {
		joined += f.detail + "\n"
	}
	assert.NotContains(t, joined, "clusterRef",
		"with --namespace team-a the clusterRef must resolve; findings were: %s", joined)
}
