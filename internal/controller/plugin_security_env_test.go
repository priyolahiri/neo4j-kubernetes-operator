package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

// mergePluginSecurityEnv must UNION additive allowlists across plugins so one
// plugin's procedures aren't silently lost when another plugin reconciles
// (e.g. GDS then APOC). Scalar keys are set in place. Idempotent.
func TestMergePluginSecurityEnv_UnionsAllowlistsAcrossPlugins(t *testing.T) {
	envName := func(k string) string {
		return "NEO4J_" + strings.ToUpper(strings.ReplaceAll(k, ".", "_"))
	}
	get := func(env []corev1.EnvVar, name string) string {
		for _, e := range env {
			if e.Name == name {
				return e.Value
			}
		}
		return ""
	}
	count := func(env []corev1.EnvVar, name string) int {
		n := 0
		for _, e := range env {
			if e.Name == name {
				n++
			}
		}
		return n
	}

	// GDS reconcile, then APOC reconcile.
	env := mergePluginSecurityEnv(nil, map[string]string{
		"dbms.security.procedures.unrestricted": "gds.*",
		"dbms.security.procedures.allowlist":    "gds.*",
	})
	env = mergePluginSecurityEnv(env, map[string]string{
		"dbms.security.procedures.unrestricted": "apoc.*",
		"dbms.security.procedures.allowlist":    "apoc.*",
	})

	unrestricted := envName("dbms.security.procedures.unrestricted")
	assert.Equal(t, "gds.*,apoc.*", get(env, unrestricted), "GDS allowlist must not be clobbered by APOC")
	assert.Equal(t, "gds.*,apoc.*", get(env, envName("dbms.security.procedures.allowlist")))
	assert.Equal(t, 1, count(env, unrestricted), "exactly one env var per key (no duplicates)")

	// Idempotent: re-applying GDS changes nothing.
	before := get(env, unrestricted)
	env = mergePluginSecurityEnv(env, map[string]string{"dbms.security.procedures.unrestricted": "gds.*"})
	assert.Equal(t, before, get(env, unrestricted))

	// Scalar (non-additive) key is overwritten in place, not unioned.
	env = mergePluginSecurityEnv(env, map[string]string{"apoc.export.file.enabled": "true"})
	env = mergePluginSecurityEnv(env, map[string]string{"apoc.export.file.enabled": "false"})
	assert.Equal(t, "false", get(env, envName("apoc.export.file.enabled")))
}
