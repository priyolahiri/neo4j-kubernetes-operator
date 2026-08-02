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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Gomega's optional failure description is an ordinary function argument, so a
// format string's ARGUMENTS are evaluated when the assertion is BUILT — not when
// it fails. In an Eventually that polls a CR, that means the description reports
// the object as it was BEFORE the first poll: usually empty. Passing a lone
// `func() string` instead makes Gomega call it only on failure.
//
// This guard exists because prose did not hold the line. The trap was documented
// (knowledge id 92) and three sites were fixed — then a later scan found THREE
// MORE that the first grep had missed, one of which was in the very spec the
// investigation was about. So it is mechanical now, and it runs in `make
// test-unit`, which is a blocking gate.
//
// It deliberately lives in this package rather than test/integration: the
// integration package's own tests need a Kind cluster, so a guard there would
// not run in the unit lane.
func TestNoEagerGomegaStatusDescriptions(t *testing.T) {
	specs, err := filepath.Glob(filepath.Join("..", "..", "test", "integration", "*_test.go"))
	if err != nil {
		t.Fatalf("globbing integration specs: %v", err)
	}
	if len(specs) == 0 {
		t.Fatal("found no integration specs to scan — the guard would silently pass")
	}

	// A polling assertion's closing line, e.g.
	//   }, restoreTimeout, pollInterval).Should(Equal("Completed"),
	closing := regexp.MustCompile(`^\s*\},\s*[^)]*\)\.(Should|ShouldNot)\(`)

	var findings []string
	for _, path := range specs {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		lines := strings.Split(string(raw), "\n")
		for i, line := range lines {
			if !closing.MatchString(line) {
				continue
			}
			// Collect exactly this assertion statement, by following it until the
			// parentheses opened on the closing line balance again. Scanning a fixed
			// number of following lines instead would pick up the unrelated
			// statements that happen to come next — which is how the first version
			// of this guard produced a dozen false positives.
			// Count from the .Should( that opens the assertion, NOT from the start of
			// the line: the line also carries the ")" that closes Eventually(...),
			// which would balance the depth to zero immediately and leave this guard
			// inspecting a single line — silently never firing. (It did exactly that
			// until a deliberately reintroduced bug failed to trip it.)
			openIdx := strings.Index(line, ".Should(")
			if k := strings.Index(line, ".ShouldNot("); k >= 0 && (openIdx < 0 || k < openIdx) {
				openIdx = k
			}
			stmt, depth, from := "", 0, openIdx
			for j := i; j < len(lines); j++ {
				seg := lines[j]
				if j == i {
					seg = seg[from:]
				}
				stmt += seg + "\n"
				for _, r := range seg {
					switch r {
					case '(':
						depth++
					case ')':
						depth--
					}
				}
				if depth <= 0 {
					break
				}
			}
			// A lazy description is the correct form.
			if strings.Contains(stmt, "func() string") {
				continue
			}
			// Only a printf-style description can evaluate its args early, and only
			// a .Status.* argument makes that observable.
			if !strings.Contains(stmt, ".Status.") || !strings.Contains(stmt, "%") {
				continue
			}
			findings = append(findings, fmt.Sprintf("%s:%d  %s", path, i+1,
				strings.TrimSpace(strings.SplitN(stmt, "\n", 2)[0])))
		}
	}

	if len(findings) > 0 {
		t.Errorf("these Eventually/Consistently descriptions read .Status.* as printf ARGUMENTS, so they are "+
			"evaluated before the first poll and will report the object's pre-poll (usually empty) state. "+
			"Pass a lone func() string instead — Gomega calls that only on failure:\n  %s",
			strings.Join(findings, "\n  "))
	}
}
