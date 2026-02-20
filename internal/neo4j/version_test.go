/*
Copyright 2025.

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
	"testing"
)

// ---------------------------------------------------------------------------
// TestParseVersion
// ---------------------------------------------------------------------------

func TestParseVersion(t *testing.T) {
	cases := []struct {
		input    string
		wantErr  bool
		major    int
		minor    int
		patch    int
		isCalver bool
	}{
		// Standard enterprise images
		{"5.26.0-enterprise", false, 5, 26, 0, false},
		{"2025.01.0-enterprise", false, 2025, 1, 0, true},

		// v-prefix stripped
		{"v5.26.3", false, 5, 26, 3, false},

		// No patch — defaults to 0
		{"5.26", false, 5, 26, 0, false},

		// CalVer year advance
		{"2026.03.1-enterprise", false, 2026, 3, 1, true},

		// Error cases
		{"", true, 0, 0, 0, false},
		{"not-a-version", true, 0, 0, 0, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			v, err := ParseVersion(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if v.Major != tc.major {
				t.Errorf("Major: expected %d, got %d", tc.major, v.Major)
			}
			if v.Minor != tc.minor {
				t.Errorf("Minor: expected %d, got %d", tc.minor, v.Minor)
			}
			if v.Patch != tc.patch {
				t.Errorf("Patch: expected %d, got %d", tc.patch, v.Patch)
			}
			if v.IsCalver != tc.isCalver {
				t.Errorf("IsCalver: expected %v, got %v", tc.isCalver, v.IsCalver)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestVersion_IsSupported
// ---------------------------------------------------------------------------

func TestVersion_IsSupported(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"5.26.0-enterprise", true},
		{"5.27.0-enterprise", false}, // 5.26.x is the final semver release; 5.27+ does not exist
		{"5.25.0-enterprise", false}, // below minimum 5.26
		{"6.0.0-enterprise", false},  // major 6 not supported
		{"2025.01.0-enterprise", true},
		{"2026.01.0-enterprise", true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			v, err := ParseVersion(tc.input)
			if err != nil {
				t.Fatalf("ParseVersion(%q): %v", tc.input, err)
			}
			if got := v.IsSupported(); got != tc.want {
				t.Errorf("IsSupported: expected %v, got %v", tc.want, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestVersion_Compare
// ---------------------------------------------------------------------------

func TestVersion_Compare(t *testing.T) {
	parse := func(s string) *Version {
		v, err := ParseVersion(s)
		if err != nil {
			t.Fatalf("ParseVersion(%q): %v", s, err)
		}
		return v
	}

	cases := []struct {
		a, b string
		want int
	}{
		// Less than
		{"5.26.0-enterprise", "5.27.0-enterprise", -1},
		{"5.26.0-enterprise", "5.26.1-enterprise", -1},
		{"2025.01.0-enterprise", "2025.02.0-enterprise", -1},

		// Equal
		{"5.26.0-enterprise", "5.26.0-enterprise", 0},
		{"2025.01.0-enterprise", "2025.01.0-enterprise", 0},

		// Greater than
		{"5.27.0-enterprise", "5.26.0-enterprise", 1},
		{"5.26.1-enterprise", "5.26.0-enterprise", 1},
		{"2025.02.0-enterprise", "2025.01.0-enterprise", 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.a+"_vs_"+tc.b, func(t *testing.T) {
			a, b := parse(tc.a), parse(tc.b)
			if got := a.Compare(b); got != tc.want {
				t.Errorf("Compare: expected %d, got %d", tc.want, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestGetImageVersion
// ---------------------------------------------------------------------------

func TestGetImageVersion(t *testing.T) {
	t.Run("valid enterprise image", func(t *testing.T) {
		v, err := GetImageVersion("neo4j:5.26.0-enterprise")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v.Major != 5 || v.Minor != 26 {
			t.Errorf("expected 5.26, got %d.%d", v.Major, v.Minor)
		}
	})

	t.Run("invalid image format", func(t *testing.T) {
		_, err := GetImageVersion("no-colon-here")
		if err == nil {
			t.Error("expected error for image without colon")
		}
	})
}

// ---------------------------------------------------------------------------
// TestGetKubernetesDiscoveryParameter
// ---------------------------------------------------------------------------

func TestGetKubernetesDiscoveryParameter(t *testing.T) {
	semver, _ := ParseVersion("5.26.0-enterprise")
	calver, _ := ParseVersion("2025.01.0-enterprise")

	if got := GetKubernetesDiscoveryParameter(semver); got != "dbms.kubernetes.discovery.v2.service_port_name" {
		t.Errorf("SemVer: expected v2 parameter, got %q", got)
	}
	if got := GetKubernetesDiscoveryParameter(calver); got != "dbms.kubernetes.discovery.service_port_name" {
		t.Errorf("CalVer: expected non-v2 parameter, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// TestGetBackupCommand / TestGetRestoreCommand (spot checks)
// ---------------------------------------------------------------------------

func TestGetBackupCommand(t *testing.T) {
	v, _ := ParseVersion("5.26.0-enterprise")

	cmd := GetBackupCommand(v, "mydb", "/backups/mydb", false)
	if !containsStr(cmd, "--to-path=/backups/mydb") {
		t.Errorf("expected --to-path flag in backup command: %q", cmd)
	}
	if !containsStr(cmd, "mydb") {
		t.Errorf("expected database name in backup command: %q", cmd)
	}
}

func TestGetRestoreCommand(t *testing.T) {
	v, _ := ParseVersion("5.26.0-enterprise")

	cmd := GetRestoreCommand(v, "mydb", "/backups/mydb")
	if !containsStr(cmd, "--from-path=/backups/mydb") {
		t.Errorf("expected --from-path flag in restore command: %q", cmd)
	}
	if !containsStr(cmd, "mydb") {
		t.Errorf("expected database name in restore command: %q", cmd)
	}
}

// ---------------------------------------------------------------------------
// TestSupports* feature flags
// ---------------------------------------------------------------------------

func TestSupportsCypherLanguageVersion(t *testing.T) {
	semver, _ := ParseVersion("5.26.0-enterprise")
	calver, _ := ParseVersion("2025.01.0-enterprise")

	if semver.SupportsCypherLanguageVersion() {
		t.Error("SemVer should not support Cypher language version")
	}
	if !calver.SupportsCypherLanguageVersion() {
		t.Error("CalVer should support Cypher language version")
	}
}

func TestSupportsAdvancedBackupFlags(t *testing.T) {
	semver, _ := ParseVersion("5.26.0-enterprise")
	earlyCalver, _ := ParseVersion("2025.01.0-enterprise")
	latestCalver, _ := ParseVersion("2025.11.0-enterprise")
	futureCalver, _ := ParseVersion("2026.01.0-enterprise")

	if semver.SupportsAdvancedBackupFlags() {
		t.Error("SemVer should not support advanced backup flags")
	}
	if earlyCalver.SupportsAdvancedBackupFlags() {
		t.Error("2025.01 should not support advanced backup flags")
	}
	if !latestCalver.SupportsAdvancedBackupFlags() {
		t.Error("2025.11 should support advanced backup flags")
	}
	if !futureCalver.SupportsAdvancedBackupFlags() {
		t.Error("2026.01 should support advanced backup flags")
	}
}

func TestSupportsSourceDatabaseFilter(t *testing.T) {
	calverEarly, _ := ParseVersion("2025.01.0-enterprise")
	calverSupported, _ := ParseVersion("2025.02.0-enterprise")
	semver, _ := ParseVersion("5.26.0-enterprise")

	if calverEarly.SupportsSourceDatabaseFilter() {
		t.Error("2025.01 should not support source database filter")
	}
	if !calverSupported.SupportsSourceDatabaseFilter() {
		t.Error("2025.02 should support source database filter")
	}
	if semver.SupportsSourceDatabaseFilter() {
		t.Error("SemVer should not support source database filter")
	}
}

// ---------------------------------------------------------------------------
// local helper
// ---------------------------------------------------------------------------

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}
