package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"github.com/neo4j-partners/neo4j-kubernetes-operator/internal/resources"
)

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func envCount(env []corev1.EnvVar, name string) int {
	n := 0
	for _, e := range env {
		if e.Name == name {
			n++
		}
	}
	return n
}

// mergePluginSecurityEnv must UNION additive allowlists across plugins (so GDS
// and APOC both survive) and use the correct Neo4j env-var name form.
func TestMergePluginSecurityEnv_UnionsAllowlistsAcrossPlugins(t *testing.T) {
	unrestricted := resources.Neo4jSettingEnvVarName("dbms.security.procedures.unrestricted")
	allowlist := resources.Neo4jSettingEnvVarName("dbms.security.procedures.allowlist")
	// Correct convention: lowercase, dots->underscores (no upper-casing).
	assert.Equal(t, "NEO4J_dbms_security_procedures_unrestricted", unrestricted)

	env := mergePluginSecurityEnv(nil, map[string]string{
		"dbms.security.procedures.unrestricted": "gds.*",
		"dbms.security.procedures.allowlist":    "gds.*",
	})
	env = mergePluginSecurityEnv(env, map[string]string{
		"dbms.security.procedures.unrestricted": "apoc.*",
		"dbms.security.procedures.allowlist":    "apoc.*",
	})

	assert.Equal(t, "gds.*,apoc.*", envValue(env, unrestricted), "GDS allowlist must not be clobbered by APOC")
	assert.Equal(t, "gds.*,apoc.*", envValue(env, allowlist))
	assert.Equal(t, 1, envCount(env, unrestricted), "exactly one env var per key")

	// Idempotent.
	before := envValue(env, unrestricted)
	env = mergePluginSecurityEnv(env, map[string]string{"dbms.security.procedures.unrestricted": "gds.*"})
	assert.Equal(t, before, envValue(env, unrestricted))

	// Scalar key overwritten in place.
	scalar := resources.Neo4jSettingEnvVarName("apoc.export.file.enabled")
	env = mergePluginSecurityEnv(env, map[string]string{"apoc.export.file.enabled": "true"})
	env = mergePluginSecurityEnv(env, map[string]string{"apoc.export.file.enabled": "false"})
	assert.Equal(t, "false", envValue(env, scalar))
}

// removePluginSecurityEnv must subtract only the uninstalled plugin's tokens,
// keep other plugins' tokens, and drop the env var when nothing remains.
func TestRemovePluginSecurityEnv_PrunesOnUninstall(t *testing.T) {
	unrestricted := resources.Neo4jSettingEnvVarName("dbms.security.procedures.unrestricted")

	// GDS + APOC installed.
	env := mergePluginSecurityEnv(nil, map[string]string{"dbms.security.procedures.unrestricted": "gds.*"})
	env = mergePluginSecurityEnv(env, map[string]string{"dbms.security.procedures.unrestricted": "apoc.*"})
	assert.Equal(t, "gds.*,apoc.*", envValue(env, unrestricted))

	// Uninstall APOC → only gds.* remains (GDS not lost).
	env = removePluginSecurityEnv(env, map[string]string{"dbms.security.procedures.unrestricted": "apoc.*"})
	assert.Equal(t, "gds.*", envValue(env, unrestricted))

	// Uninstall GDS → env var dropped entirely.
	env = removePluginSecurityEnv(env, map[string]string{"dbms.security.procedures.unrestricted": "gds.*"})
	assert.Equal(t, 0, envCount(env, unrestricted), "env var removed when no tokens remain")
}
