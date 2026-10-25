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

// Command check-test-cr-updates reports a whole-object Update of one of the
// operator's custom resources in test code (docs/knowledge/operations.md,
// id 205).
//
// An Update sends the object as the Go types encode it. A non-pointer bool
// with omitempty is left out when it is false, so the API server applies the
// CRD default: a spec that sets monitoring.enabled to false, then Updates the
// cluster to add an annotation, stores monitoring.enabled: true and tests a
// cluster nobody applied. The operator patches its CRs (id 150); a test does
// the same with a merge patch (client.MergeFrom) or, in internal/controller,
// with patchObject, addFinalizer and removeFinalizer.
//
// The check is typed, not textual: it type-checks every test package from
// source against the export data `go list -export` builds, and reports a call
// whose method is controller-runtime's client.Writer Update (its options are
// client.UpdateOption, so Status().Update and SubResource writes are never
// matched) and whose object's type is declared under api/. A call that must
// Update on purpose carries a `cr-update-ok: <reason>` comment on its line or
// the line above.
//
// It scans *_test.go files, and every file of a package under test/ (the
// shared helpers the integration specs call).
//
// Usage: go run ./scripts/check-test-cr-updates [package patterns...]
// (default ./...). Exit 1 on a finding.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const (
	modulePath  = "github.com/priyolahiri/neo4j-kubernetes-operator"
	apiPrefix   = modulePath + "/api/"
	testPrefix  = modulePath + "/test/"
	clientPkg   = "sigs.k8s.io/controller-runtime/pkg/client"
	allowMarker = "cr-update-ok"
)

// listedPackage is the part of `go list -json` the check reads.
type listedPackage struct {
	Dir        string
	ImportPath string
	ForTest    string
	Export     string
	GoFiles    []string
	ImportMap  map[string]string
}

// finding is one whole-object Update of a CR.
type finding struct {
	Pos  token.Position
	Call string
	What string
}

func main() {
	patterns := os.Args[1:]
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	pkgs, err := goList(patterns)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-test-cr-updates: %v\n", err)
		os.Exit(2)
	}

	findings, warnings := check(pkgs)
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "check-test-cr-updates: WARN %s\n", w)
	}
	if len(findings) == 0 {
		fmt.Println("check-test-cr-updates: OK — no test Updates a whole operator CR.")
		return
	}
	for _, f := range findings {
		fmt.Fprintf(os.Stderr, "%s: %s Updates %s\n", f.Pos, f.Call, f.What)
	}
	fmt.Fprintf(os.Stderr, `
check-test-cr-updates: %d whole-object Update(s) of an operator CR in test code.
An Update sends the object as the Go types encode it: a false the CRD defaults
to true (monitoring.enabled, enforcePrivileges, a plugin's enabled, ...) is left
out and comes back true. Patch what the test changes instead:

    patchCR(ctx, obj, func() { obj.Spec.X = ... })   // test/integration, controller_test
    releaseFinalizers(ctx, obj)                      // the AfterEach cleanup
    patchObject(ctx, c, obj, func() { ... })         // package controller

A call that must Update on purpose takes a "// %s: <reason>" comment.
See docs/knowledge/operations.md, id 205.
`, len(findings), allowMarker)
	os.Exit(1)
}

// goList lists the packages matching patterns, with their test variants and
// every dependency's export data.
func goList(patterns []string) ([]listedPackage, error) {
	args := append([]string{"list", "-e", "-export", "-deps", "-test",
		"-json=Dir,ImportPath,ForTest,Export,GoFiles,ImportMap"}, patterns...)
	cmd := exec.CommandContext(context.Background(), "go", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var pkgs []listedPackage
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var p listedPackage
		if err := dec.Decode(&p); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode go list output: %w", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, nil
}

// check type-checks every package that holds test code and returns the
// whole-object CR Updates in it, sorted by position.
func check(pkgs []listedPackage) ([]finding, []string) {
	exports := map[string]string{}
	for _, p := range pkgs {
		if p.Export != "" {
			exports[p.ImportPath] = p.Export
		}
	}
	fset := token.NewFileSet()
	gc := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %q", path)
		}
		return os.Open(file)
	})

	var findings []finding
	var warnings []string
	for _, p := range pkgs {
		if !holdsTestCode(p) {
			continue
		}
		var files []*ast.File
		for _, name := range p.GoFiles {
			f, err := parser.ParseFile(fset, filepath.Join(p.Dir, name), nil, parser.ParseComments)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("parse %s: %v", name, err))
				continue
			}
			files = append(files, f)
		}
		info := &types.Info{
			Types:      map[ast.Expr]types.TypeAndValue{},
			Selections: map[*ast.SelectorExpr]*types.Selection{},
		}
		var firstErr error
		conf := types.Config{
			Importer: mappedImporter{importer: gc, importMap: p.ImportMap},
			Error: func(err error) {
				if firstErr == nil {
					firstErr = err
				}
			},
		}
		_, _ = conf.Check(packagePath(p.ImportPath), fset, files, info)
		if firstErr != nil {
			warnings = append(warnings, fmt.Sprintf("type-check %s (findings there may be incomplete): %v", p.ImportPath, firstErr))
		}
		for _, f := range files {
			name := fset.Position(f.Pos()).Filename
			if !strings.HasSuffix(name, "_test.go") && !strings.HasPrefix(packagePath(p.ImportPath), testPrefix) {
				continue
			}
			findings = append(findings, findUpdates(fset, f, info)...)
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		a, b := findings[i].Pos, findings[j].Pos
		if a.Filename != b.Filename {
			return a.Filename < b.Filename
		}
		return a.Line < b.Line
	})
	return dedupe(findings), warnings
}

// holdsTestCode says whether a listed package is one the check type-checks: a
// test variant (its files include the package's tests), or a package under
// test/ that has no tests of its own.
func holdsTestCode(p listedPackage) bool {
	if p.ForTest != "" {
		return !strings.HasSuffix(p.ImportPath, ".test")
	}
	if !strings.HasPrefix(p.ImportPath, testPrefix) || strings.HasSuffix(p.ImportPath, ".test") {
		return false
	}
	return true
}

// packagePath strips the test-variant suffix: "p [p.test]" is "p".
func packagePath(importPath string) string {
	if i := strings.Index(importPath, " ["); i >= 0 {
		return importPath[:i]
	}
	return importPath
}

// dedupe drops repeats: a package under test/ is listed both as itself and
// as its test variant, and both are scanned.
func dedupe(in []finding) []finding {
	var out []finding
	seen := map[string]bool{}
	for _, f := range in {
		key := fmt.Sprintf("%s:%d:%d", f.Pos.Filename, f.Pos.Line, f.Pos.Column)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, f)
	}
	return out
}

// mappedImporter resolves an import as the package's test build does: a test
// variant imports the variant of the package under test (ImportMap).
type mappedImporter struct {
	importer  types.Importer
	importMap map[string]string
}

func (m mappedImporter) Import(path string) (*types.Package, error) {
	if mapped, ok := m.importMap[path]; ok {
		path = mapped
	}
	return m.importer.Import(path)
}

// findUpdates returns the whole-object CR Updates in one type-checked file.
func findUpdates(fset *token.FileSet, file *ast.File, info *types.Info) []finding {
	allowed := map[int]bool{}
	for _, group := range file.Comments {
		for _, c := range group.List {
			if strings.Contains(c.Text, allowMarker) {
				allowed[fset.Position(c.Slash).Line] = true
			}
		}
	}

	var out []finding
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Update" {
			return true
		}
		selection := info.Selections[sel]
		if selection == nil || !isWriterUpdate(selection.Obj()) {
			return true
		}
		what := updatedObject(info.TypeOf(call.Args[1]), call.Ellipsis.IsValid())
		if what == "" {
			return true
		}
		pos := fset.Position(call.Pos())
		if allowed[pos.Line] || allowed[pos.Line-1] {
			return true
		}
		out = append(out, finding{Pos: pos, Call: types.ExprString(sel), What: what})
		return true
	})
	return out
}

// isWriterUpdate says whether obj is an Update method shaped like
// controller-runtime's client.Writer.Update: (ctx, obj, ...client.UpdateOption).
// A status or other subresource writer takes client.SubResourceUpdateOption
// and is not matched.
func isWriterUpdate(obj types.Object) bool {
	fn, ok := obj.(*types.Func)
	if !ok {
		return false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || !sig.Variadic() || sig.Params().Len() != 3 {
		return false
	}
	slice, ok := sig.Params().At(2).Type().(*types.Slice)
	if !ok {
		return false
	}
	named, ok := slice.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == clientPkg && named.Obj().Name() == "UpdateOption"
}

// updatedObject describes the object an Update writes when the check reports
// it, or returns "" when it does not: a whole operator CR, or an object whose
// static type is an interface or a type parameter (client.Object, T), which
// may be one. A pass-through that forwards its own options (an interceptor's
// `cl.Update(ctx, obj, opts...)`) writes what its caller chose and is left to
// the caller's call.
func updatedObject(t types.Type, forwardsOptions bool) string {
	if crType := operatorCRType(t); crType != "" {
		return "a whole " + crType
	}
	if t == nil || forwardsOptions || !types.IsInterface(t) {
		return ""
	}
	return "a whole " + types.TypeString(t, func(p *types.Package) string { return p.Name() }) +
		", which may be an operator CR (give it its concrete type, or patch it)"
}

// operatorCRType names the operator CR type t is (or points to), or returns ""
// when t is not one of the types declared under api/.
func operatorCRType(t types.Type) string {
	if t == nil {
		return ""
	}
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	if !strings.HasPrefix(named.Obj().Pkg().Path(), apiPrefix) {
		return ""
	}
	return named.Obj().Pkg().Name() + "." + named.Obj().Name()
}
