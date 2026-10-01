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

package validation

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A validator that no aggregator and no reconciler constructs never runs, and
// nothing says so: its tests pass, its rules look enforced, and a rule written
// against it protects nobody. SecurityValidator sat in this package that way —
// unwired, with a 690-line test file and rules that read as live — until it was
// found by accident, and by then its rules had also drifted from what Neo4j
// accepts (it "deprecated" settings that still exist). This test fails the
// build when a *Validator type is declared here but never constructed anywhere
// that is itself reachable from a reconciler or command.
//
// Reachability is approximated at file level: a file that constructs a
// validator makes it reachable if any validator declared in that file is
// reachable, or if the file is outside internal/validation (a controller, a
// command). That is exact for this package's one-aggregator-per-file layout.
func TestEveryValidatorIsWiredIn(t *testing.T) {
	fset := token.NewFileSet()

	declaredIn := map[string]string{} // validator type -> declaring file (basename)
	valFiles, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range valFiles {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, d := range parsed.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				if _, isStruct := ts.Type.(*ast.StructType); isStruct && strings.HasSuffix(ts.Name.Name, "Validator") {
					declaredIn[ts.Name.Name] = f
				}
			}
		}
	}
	if len(declaredIn) < 10 {
		t.Fatalf("found only %d validator types; the scan is broken", len(declaredIn))
	}

	// Every non-test source file that could construct one, with its text.
	type srcFile struct {
		inValidation bool
		base         string
		text         string
	}
	var sources []srcFile
	for _, root := range []string{"..", "../../cmd"} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			dir := filepath.Dir(path)
			sources = append(sources, srcFile{
				inValidation: filepath.Clean(dir) == filepath.Clean("../validation"),
				base:         filepath.Base(path),
				text:         string(b),
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	constructs := func(text, typeName string) bool {
		re := regexp.MustCompile(`\bNew` + typeName + `\(|\b` + typeName + `\{`)
		return re.MatchString(text)
	}

	// Seed: validators constructed outside this package. Then iterate: a
	// validator constructed in a file that declares a reachable one is reachable.
	reachable := map[string]bool{}
	for name := range declaredIn {
		for _, s := range sources {
			if !s.inValidation && constructs(s.text, name) {
				reachable[name] = true
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for name, declFile := range declaredIn {
			if reachable[name] {
				continue
			}
			for _, s := range sources {
				if !s.inValidation || s.base == declFile || !constructs(s.text, name) {
					continue
				}
				for other, otherFile := range declaredIn {
					if otherFile == s.base && reachable[other] {
						reachable[name] = true
						changed = true
					}
				}
			}
		}
	}

	var dead []string
	for name := range declaredIn {
		if !reachable[name] {
			dead = append(dead, name+" ("+declaredIn[name]+")")
		}
	}
	sort.Strings(dead)
	if len(dead) > 0 {
		t.Errorf("these validators are declared but never constructed by any reconciler, command or "+
			"aggregator, so none of their rules ever run — wire them in or delete them:\n  %s",
			strings.Join(dead, "\n  "))
	}
}
