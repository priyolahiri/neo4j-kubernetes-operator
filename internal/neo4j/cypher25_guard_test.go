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

package neo4j

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// cypher25OnlySyntax is statement text that exists only in Cypher 25. A
// function that builds any of it must also call Cypher25. Extend the list when
// a new Cypher-25-only construct is found — that is the whole maintenance
// cost of this guard, and far cheaper than a release journey finding it.
var cypher25OnlySyntax = []*regexp.Regexp{
	regexp.MustCompile(`^\s*(SHOW|CREATE|ALTER|DROP|GRANT|REVOKE)\b.*\bAUTH RULES?\b`),
	regexp.MustCompile(`^\s*CREATE REPLICA DATABASE\b`),
	regexp.MustCompile(`\bdbms\.promoteReplicaDatabase\b`),
	regexp.MustCompile(`\bSET (GRAPH SHARD|PROPERTY SHARDS)\b`),
	regexp.MustCompile(`\bOIDC CREDENTIAL FORWARDING\b`),
}

// cypher25GuardDirs are the packages that build Cypher. In this package — the
// Bolt client — every function is checked. In controllers only functions that
// SEND Cypher themselves are: elsewhere a controller merely mentions syntax in
// an event or error message ("SHOW AUTH RULES failed"), which is not a
// statement.
var cypher25GuardDirs = map[string]bool{".": false, "../controller": true}

// sendsCypher reports whether fn hands a statement to the server directly.
func sendsCypher(fn *ast.FuncDecl) bool {
	sends := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				name := sel.Sel.Name
				sends = sends || strings.HasPrefix(name, "ExecuteCypher") || name == "Run" || name == "ExecuteQuery"
			}
		}
		return !sends
	})
	return sends
}

// TestCypher25Guard makes the Cypher 25 directive structural: a function
// that builds Cypher-25-only syntax without calling Cypher25 fails the build,
// and nothing but cypher25.go may spell the directive itself (a hand-written
// prefix is exactly the convention that was missed twice).
func TestCypher25Guard(t *testing.T) {
	fset := token.NewFileSet()
	checked := 0
	for dir, sendersOnly := range cypher25GuardDirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, name, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				checked++
				if sendersOnly && !sendsCypher(fn) {
					continue
				}
				checkCypher25Func(t, fset, filepath.Base(name), fn)
			}
		}
	}
	if checked < 100 {
		t.Fatalf("guard only saw %d functions; the directory list is probably wrong", checked)
	}
}

func checkCypher25Func(t *testing.T, fset *token.FileSet, file string, fn *ast.FuncDecl) {
	t.Helper()
	var needs *ast.BasicLit
	calls := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(x.Value)
			if err != nil {
				return true
			}
			if file != "cypher25.go" && strings.Contains(strings.ToUpper(text), "CYPHER 25 ") &&
				!strings.Contains(text, "`") { // a backticked Go doc snippet is not a statement
				t.Errorf("%s: spells the Cypher 25 directive by hand in %s; build the statement without it and wrap it in Cypher25()",
					fset.Position(x.Pos()), fn.Name.Name)
			}
			for _, re := range cypher25OnlySyntax {
				if re.MatchString(text) && needs == nil {
					needs = x
				}
			}
		case *ast.CallExpr:
			switch f := x.Fun.(type) {
			case *ast.Ident:
				calls = calls || f.Name == "Cypher25"
			case *ast.SelectorExpr:
				calls = calls || f.Sel.Name == "Cypher25"
			}
		}
		return true
	})
	if needs != nil && !calls {
		t.Errorf("%s: %s builds Cypher-25-only syntax (%s) but never calls Cypher25; CalVer servers parse `system` as Cypher 5 and will reject it",
			fset.Position(needs.Pos()), fn.Name.Name, needs.Value)
	}
}

func TestCypher25(t *testing.T) {
	for in, want := range map[string]string{
		"SHOW AUTH RULES":              "CYPHER 25 SHOW AUTH RULES",
		"  SHOW AUTH RULES":            "CYPHER 25 SHOW AUTH RULES",
		"CYPHER 25 SHOW AUTH RULES":    "CYPHER 25 SHOW AUTH RULES",
		"cypher 25 SHOW AUTH RULES":    "cypher 25 SHOW AUTH RULES",
		"\nCYPHER  25 SHOW AUTH RULES": "CYPHER  25 SHOW AUTH RULES",
	} {
		if got := Cypher25(in); got != want {
			t.Errorf("Cypher25(%q) = %q, want %q", in, got, want)
		}
	}
	if HasCypher25Prefix("CYPHER 5 RETURN 1") {
		t.Error("CYPHER 5 is not the Cypher 25 directive")
	}
}
