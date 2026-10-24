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
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AdvisoryWarningReasons tells `kubectl neo4j diagnose` which Warnings are
// announced again while they apply (operations.md id 168). A reason recorded
// through advisoryWarnings and missing from the list would be read as a
// one-time event, and lose its mark once the object's Ready condition changed.
func TestAdvisoryWarningReasons_ListsEveryRecordedReason(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	consts := map[string]string{}
	var recorded []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ValueSpec:
				for i, id := range n.Names {
					if i < len(n.Values) {
						if lit, ok := n.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							value, err := strconv.Unquote(lit.Value)
							require.NoError(t, err)
							consts[id.Name] = value
						}
					}
				}
			case *ast.CallExpr:
				sel, ok := n.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "record" && sel.Sel.Name != "recordScoped") || len(n.Args) < 3 {
					return true
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); !ok || inner.Sel.Name != "advisoryWarnings" {
					return true
				}
				reason, ok := n.Args[2].(*ast.Ident)
				require.True(t, ok, "%s: the reason of an advisory warning is a constant", fset.Position(n.Pos()))
				recorded = append(recorded, reason.Name)
			}
			return true
		})
	}
	require.NotEmpty(t, recorded, "found no advisoryWarnings.record calls; has the helper been renamed?")
	for _, name := range recorded {
		value, ok := consts[name]
		require.True(t, ok, "reason constant %s not found", name)
		if value == EventReasonUpgradeFailed {
			continue // a one-time failure everywhere but the standalone's pre-upgrade block
		}
		assert.True(t, slices.Contains(AdvisoryWarningReasons, value),
			"%s (%q) is recorded through advisoryWarnings but is not in AdvisoryWarningReasons", name, value)
	}
}
