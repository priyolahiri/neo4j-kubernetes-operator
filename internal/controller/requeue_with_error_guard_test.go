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
	"strings"
	"testing"
)

// TestNoRequeueWithError pins one rule across every reconciler in this
// package: never return a ctrl.Result that asks for a requeue together with a
// non-nil error.
//
// controller-runtime IGNORES the Result whenever the error is non-nil — it
// retries on its own error backoff and logs a warning saying so — so such a
// return always means something other than it says. 99 of them had
// accumulated before this guard. Pick one:
//   - return ctrl.Result{}, err              — a transient failure; let the
//     error backoff retry it and the error metrics count it;
//   - return ctrl.Result{RequeueAfter: d}, nil — an expected condition (a spec
//     the user must fix, a dependency not ready yet) that is already surfaced
//     in status, an event or a log, retried after d.
func TestNoRequeueWithError(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 2 {
				return true
			}
			if !requestsRequeue(ret.Results[0]) || isNilIdent(ret.Results[1]) {
				return true
			}
			t.Errorf("%s: returns a requeue Result together with a non-nil error; controller-runtime ignores the Result. "+
				"Return ctrl.Result{}, err or ctrl.Result{RequeueAfter: d}, nil (see TestNoRequeueWithError)",
				fset.Position(ret.Pos()))
			return true
		})
	}
}

// requestsRequeue reports whether expr is a ctrl.Result / reconcile.Result
// literal setting RequeueAfter or Requeue.
func requestsRequeue(expr ast.Expr) bool {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	sel, ok := lit.Type.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Result" {
		return false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && (key.Name == "RequeueAfter" || key.Name == "Requeue") {
			return true
		}
	}
	return false
}

func isNilIdent(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == "nil"
}
