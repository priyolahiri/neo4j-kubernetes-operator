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

package main

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

// `go install <module>@vX.Y.Z` is the install path the CLI docs list FIRST,
// and it cannot apply ldflags — there is no mechanism for a module to request
// them. So the binary it produces reported "dev".
//
// That was not a cosmetic problem. warnOnVersionSkew returns early on "dev":
//
//	if c == nil || version == "dev" { return }
//
// so every user who installed the documented way had skew detection silently
// disabled — exactly the case the check exists for, and invisible, because
// validate still runs and still reports, it just never mentions the skew.
func TestResolveVersion(t *testing.T) {
	buildInfo := func(v string) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) {
			return &debug.BuildInfo{Main: debug.Module{Version: v}}, true
		}
	}
	noBuildInfo := func() (*debug.BuildInfo, bool) { return nil, false }

	t.Run("ldflags win when present", func(t *testing.T) {
		// make build-cli and the release build stamp this; it is authoritative
		// even when build info disagrees, because it is the more specific
		// statement (git describe, so it can carry -dirty).
		assert.Equal(t, "v1.15.0", resolveVersion("v1.15.0", buildInfo("v9.9.9")))
	})

	t.Run("go install: the module version is used", func(t *testing.T) {
		assert.Equal(t, "v1.15.0", resolveVersion("dev", buildInfo("v1.15.0")))
	})

	// "(devel)" is what Go reports when VCS info is unavailable (-buildvcs=false,
	// or building outside a repository). Nothing to compare, so it stays "dev".
	t.Run("no VCS info stays dev", func(t *testing.T) {
		assert.Equal(t, "dev", resolveVersion("dev", buildInfo("(devel)")))
	})

	// A local build WITH VCS info does not report "(devel)" — Go derives the
	// version from the repository. Verified against a real binary from this
	// tree: mod ... v1.15.0+dirty, vcs.modified=true.
	//
	// These flow through to the skew check on purpose. A modified or
	// mid-branch tree does not carry a release's rules, so warning is correct;
	// the blanket "dev" this replaced suppressed the warning even for builds
	// that had genuinely diverged.
	t.Run("a dirty local build reports its VCS-derived version", func(t *testing.T) {
		assert.Equal(t, "v1.15.0+dirty", resolveVersion("dev", buildInfo("v1.15.0+dirty")))
	})

	t.Run("a pseudo-version from mid-branch is kept", func(t *testing.T) {
		const pseudo = "v1.15.1-0.20260914091500-191ef0573274"
		assert.Equal(t, pseudo, resolveVersion("dev", buildInfo(pseudo)))
	})

	t.Run("no build info at all stays dev", func(t *testing.T) {
		assert.Equal(t, "dev", resolveVersion("dev", noBuildInfo))
	})

	t.Run("empty module version stays dev", func(t *testing.T) {
		assert.Equal(t, "dev", resolveVersion("dev", buildInfo("")))
	})

	t.Run("nil build info with ok=true does not panic", func(t *testing.T) {
		assert.Equal(t, "dev", resolveVersion("dev", func() (*debug.BuildInfo, bool) {
			return nil, true
		}))
	})
}

// The point of the fix is that skew detection turns ON for a go-installed
// binary. Pin the predicate the skew check actually uses, so a future change
// that reintroduces "dev" for that path fails here rather than in the field.
func TestResolvedVersionEnablesSkewDetection(t *testing.T) {
	goInstalled := resolveVersion("dev", func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Main: debug.Module{Version: "v1.15.0"}}, true
	})
	assert.NotEqual(t, "dev", goInstalled,
		"warnOnVersionSkew returns early on \"dev\"; a go-installed binary must not report it")
}
