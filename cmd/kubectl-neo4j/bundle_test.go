package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func unstructuredFrom(t *testing.T, obj map[string]interface{}) *unstructured.Unstructured {
	t.Helper()
	return &unstructured.Unstructured{Object: obj}
}

// A support bundle is mailed to strangers. Shipping a Secret value in one is
// the worst thing this command could do, so it is pinned first.
func TestRedact_SecretValuesNeverShip(t *testing.T) {
	obj := unstructuredFrom(t, map[string]interface{}{
		"kind":     "Secret",
		"metadata": map[string]interface{}{"name": "neo4j-admin"},
		"data": map[string]interface{}{
			"password": "c3VwZXItc2VjcmV0",
			"username": "bmVvNGo=",
		},
	})

	out, notes := redactUnstructured(obj)
	rendered := renderObject(t, out)

	assert.NotContains(t, rendered, "c3VwZXItc2VjcmV0", "the encoded password must not survive redaction")
	assert.Contains(t, rendered, redactedPlaceholder)
	assert.Contains(t, strings.Join(notes, "\n"), "data withheld")
}

// last-applied-configuration is a verbatim copy of a previous manifest, so it
// re-introduces anything redacted elsewhere in the same object.
func TestRedact_LastAppliedConfigurationIsWithheld(t *testing.T) {
	obj := unstructuredFrom(t, map[string]interface{}{
		"kind": "Neo4jEnterpriseCluster",
		"metadata": map[string]interface{}{
			"name": "prod",
			"annotations": map[string]interface{}{
				"kubectl.kubernetes.io/last-applied-configuration": `{"spec":{"env":[{"name":"ADMIN_PASSWORD","value":"hunter2"}]}}`,
			},
		},
	})

	out, notes := redactUnstructured(obj)
	rendered := renderObject(t, out)

	assert.NotContains(t, rendered, "hunter2")
	assert.Contains(t, strings.Join(notes, "\n"), "last-applied-configuration")
}

func TestRedact_LiteralSensitiveEnvIsWithheld(t *testing.T) {
	obj := unstructuredFrom(t, map[string]interface{}{
		"kind":     "StatefulSet",
		"metadata": map[string]interface{}{"name": "prod-server"},
		"spec": map[string]interface{}{"template": map[string]interface{}{"spec": map[string]interface{}{
			"containers": []interface{}{map[string]interface{}{
				"name": "neo4j",
				"env": []interface{}{
					map[string]interface{}{"name": "NEO4J_PASSWORD", "value": "hunter2"},
					map[string]interface{}{"name": "SOME_API_TOKEN", "value": "abc123"},
				},
			}},
		}}},
	})

	out, _ := redactUnstructured(obj)
	rendered := renderObject(t, out)

	assert.NotContains(t, rendered, "hunter2")
	assert.NotContains(t, rendered, "abc123")
}

// Over-redaction makes a bundle useless. A non-sensitive value must survive,
// and a valueFrom reference holds no secret at all — blanking it would destroy
// the very information that explains a misconfigured secretKeyRef.
func TestRedact_DoesNotOverReach(t *testing.T) {
	obj := unstructuredFrom(t, map[string]interface{}{
		"kind":     "StatefulSet",
		"metadata": map[string]interface{}{"name": "prod-server"},
		"spec": map[string]interface{}{"template": map[string]interface{}{"spec": map[string]interface{}{
			"containers": []interface{}{map[string]interface{}{
				"name": "neo4j",
				"env": []interface{}{
					map[string]interface{}{"name": "NEO4J_server_memory_heap_max__size", "value": "2G"},
					map[string]interface{}{"name": "DB_PASSWORD", "valueFrom": map[string]interface{}{
						"secretKeyRef": map[string]interface{}{"name": "neo4j-admin", "key": "password"},
					}},
				},
			}},
		}}},
	})

	out, _ := redactUnstructured(obj)
	rendered := renderObject(t, out)

	assert.Contains(t, rendered, "2G", "a non-sensitive literal must survive")
	assert.Contains(t, rendered, "neo4j-admin", "a secretKeyRef holds no secret and explains misconfiguration")
	assert.Contains(t, rendered, "key: password")
	assert.NotContains(t, rendered, redactedPlaceholder, "nothing here needed redacting")
}

// Redaction must not mutate the caller's object — for a read-only command,
// corrupting the live view would be a particularly silly bug.
func TestRedact_DoesNotMutateTheOriginal(t *testing.T) {
	obj := unstructuredFrom(t, map[string]interface{}{
		"kind":     "Secret",
		"metadata": map[string]interface{}{"name": "neo4j-admin"},
		"data":     map[string]interface{}{"password": "c3VwZXItc2VjcmV0"},
	})

	_, _ = redactUnstructured(obj)

	data, ok, err := unstructured.NestedMap(obj.Object, "data")
	require.NoError(t, err)
	require.True(t, ok, "the original object must still have its data map")
	assert.Equal(t, "c3VwZXItc2VjcmV0", data["password"], "the original must be untouched")
}

func TestLooksSensitive(t *testing.T) {
	for _, name := range []string{"NEO4J_PASSWORD", "db_passwd", "MY_SECRET", "API_KEY", "AUTH_HEADER", "PRIVATE_KEY_PEM"} {
		assert.True(t, looksSensitive(name), "%s should be treated as sensitive", name)
	}
	for _, name := range []string{"NEO4J_server_memory_heap_max__size", "HOSTNAME", "SERVER_INDEX"} {
		assert.False(t, looksSensitive(name), "%s should not be redacted", name)
	}
}

func TestRenderRedactions_WarnsThatItIsNotAGuarantee(t *testing.T) {
	out := renderRedactions([]string{"Secret \"x\": values withheld"})
	assert.Contains(t, out, "not a guarantee of safety",
		"the recipient must be told redaction cannot cover their own config or logs")
	assert.Contains(t, out, "Review the archive before sharing")

	empty := renderRedactions(nil)
	assert.Contains(t, empty, "nothing required redaction")
}

func TestWriteArchive_RoundTrips(t *testing.T) {
	target := filepath.Join(t.TempDir(), "bundle.tar.gz")
	require.NoError(t, writeArchive(target, []bundleFile{
		{name: "meta.txt", body: []byte("hello")},
		{name: "resources/Neo4jEnterpriseCluster/prod.yaml", body: []byte("kind: Neo4jEnterpriseCluster")},
	}))

	got := readArchive(t, target)
	assert.Equal(t, "hello", got["bundle/meta.txt"])
	assert.Equal(t, "kind: Neo4jEnterpriseCluster", got["bundle/resources/Neo4jEnterpriseCluster/prod.yaml"])
}

func renderObject(t *testing.T, obj *unstructured.Unstructured) string {
	t.Helper()
	b, err := yamlMarshal(obj.Object)
	require.NoError(t, err)
	return string(b)
}

func readArchive(t *testing.T, p string) map[string]string {
	t.Helper()
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	tr := tar.NewReader(gz)

	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		body, err := io.ReadAll(tr)
		require.NoError(t, err)
		out[hdr.Name] = string(body)
	}
	return out
}
