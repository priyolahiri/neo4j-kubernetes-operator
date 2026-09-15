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

package metrics

import (
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// labelsFor reads back the single build_info series.
func labelsFor(t *testing.T) map[string]string {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != "neo4j_operator_build_info" {
			continue
		}
		require.Len(t, mf.GetMetric(), 1, "build_info must be a single series")
		m := mf.GetMetric()[0]
		assert.Equal(t, float64(1), m.GetGauge().GetValue(),
			"build_info is the Prometheus build-metadata convention: always 1, detail in labels")
		out := map[string]string{}
		for _, l := range m.GetLabel() {
			out[l.GetName()] = l.GetValue()
		}
		return out
	}
	t.Fatal("neo4j_operator_build_info was not registered")
	return nil
}

// Values stamped by the Dockerfile's ldflags win, and are reported verbatim.
//
// Those ldflags were inert before this metric existed: cmd/main.go declared no
// version/buildDate/vcsRef, so `-X main.version=...` had no symbol to set and
// Go silently ignored it. CI passed VERSION/VCS_REF/BUILD_DATE on every build
// and none of it reached anywhere observable.
func TestSetBuildInfo_UsesStampedValues(t *testing.T) {
	buildInfo.Reset()
	SetBuildInfo("v1.15.0", "191ef05", "2026-09-14T12:00:00Z")

	got := labelsFor(t)
	assert.Equal(t, "v1.15.0", got["version"])
	assert.Equal(t, "191ef05", got["vcs_ref"])
	assert.Equal(t, "2026-09-14T12:00:00Z", got["build_date"])
	assert.Equal(t, runtime.Version(), got["go_version"])
}

// An unstamped binary must still report something true rather than nothing.
// The OPERATOR_VERSION env var is set on the Deployment by both the kustomize
// manifest and the Helm chart, so it is the most reliable fallback in a real
// cluster.
func TestSetBuildInfo_FallsBackToOperatorVersionEnv(t *testing.T) {
	t.Setenv(operatorVersionEnv, "v9.9.9-from-env")
	buildInfo.Reset()
	SetBuildInfo("dev", "", "")

	assert.Equal(t, "v9.9.9-from-env", labelsFor(t)["version"])
}

// No ldflags, no env var: whatever Go embedded, else "development". Never an
// empty label — an empty value reads as "absent" in most query languages,
// whereas "unknown" is a fact you can see.
func TestSetBuildInfo_NeverEmitsEmptyLabels(t *testing.T) {
	t.Setenv(operatorVersionEnv, "")
	buildInfo.Reset()
	SetBuildInfo("", "", "")

	got := labelsFor(t)
	for _, k := range []string{"version", "vcs_ref", "build_date", "go_version"} {
		assert.NotEmpty(t, got[k], "label %q must never be empty", k)
	}
	// Under `go test` the binary has no VCS stamp, so version lands on the
	// last fallback rather than a release string.
	assert.NotContains(t, got["version"], " ")
}

// Each field falls back independently: they are stamped by different
// mechanisms, so a missing one must not blank the others.
func TestSetBuildInfo_FieldsFallBackIndependently(t *testing.T) {
	buildInfo.Reset()
	SetBuildInfo("v1.15.0", "", "")

	got := labelsFor(t)
	assert.Equal(t, "v1.15.0", got["version"], "a stamped version survives missing siblings")
	assert.NotEmpty(t, got["vcs_ref"])
	assert.NotEmpty(t, got["build_date"])
}

// The metric has to be registered on the registry controller-runtime serves,
// or it exists in the process and nowhere a scrape can see it.
func TestBuildInfo_IsRegisteredOnTheServedRegistry(t *testing.T) {
	buildInfo.Reset()
	SetBuildInfo("v1.2.3", "abc1234", "2026-01-01T00:00:00Z")
	assert.Equal(t, 1, testutil.CollectAndCount(buildInfo),
		"build_info should expose exactly one series")
	assert.True(t, strings.HasPrefix(subsystem, "neo4j_operator"))
}

// "latest" is the literal value in the kustomize base, not a release. Reporting
// it would look like an answer while meaning "nobody stamped this".
func TestSetBuildInfo_RejectsPlaceholderVersions(t *testing.T) {
	for _, placeholder := range []string{"", "dev", "latest", "(devel)"} {
		t.Setenv(operatorVersionEnv, placeholder)
		buildInfo.Reset()
		SetBuildInfo(placeholder, "", "")

		got := labelsFor(t)["version"]
		assert.Equal(t, "development", got,
			"placeholder %q must not be reported as a version", placeholder)
	}
}
