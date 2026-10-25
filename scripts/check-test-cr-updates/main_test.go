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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"testing"
)

// The packages the fixture imports, cut down to what the check reads: the
// shape of controller-runtime's writers, one operator CR and one child object.
var fakePackages = map[string]string{
	clientPkg: `package client
type Ctx interface{}
type Object interface{ GetName() string }
type UpdateOption interface{ ApplyToUpdate() }
type SubResourceUpdateOption interface{ ApplyToSubResourceUpdate() }
type Patch interface{}
type PatchOption interface{}
type Writer interface {
	Update(ctx Ctx, obj Object, opts ...UpdateOption) error
	Patch(ctx Ctx, obj Object, patch Patch, opts ...PatchOption) error
}
type SubResourceWriter interface {
	Update(ctx Ctx, obj Object, opts ...SubResourceUpdateOption) error
}
type Client interface {
	Writer
	Status() SubResourceWriter
}
`,
	apiPrefix + "v1beta1": `package v1beta1
type Neo4jEnterpriseCluster struct{ Name string }
func (c *Neo4jEnterpriseCluster) GetName() string { return c.Name }
`,
	"k8s.io/api/apps/v1": `package v1
type StatefulSet struct{ Name string }
func (s *StatefulSet) GetName() string { return s.Name }
`,
}

// fixture is a test file. Every call the check must report carries a
// "want" comment on its line.
const fixture = `package fixture

import (
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

type wrapper struct{ client.Client }

func cases(c client.Client, w wrapper, ctx client.Ctx) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	_ = c.Update(ctx, cluster) // want
	_ = w.Update(ctx, cluster) // want: a reconciler embeds its client
	var byValue neo4jv1beta1.Neo4jEnterpriseCluster
	_ = c.Update(ctx, &byValue) // want
	_ = c.Status().Update(ctx, cluster)
	_ = c.Update(ctx, &appsv1.StatefulSet{})
	_ = c.Patch(ctx, cluster, nil)
	var obj client.Object = cluster
	_ = c.Update(ctx, obj) // want: may be a CR
	// cr-update-ok: on purpose, marked on the line above
	_ = c.Update(ctx, cluster)
	_ = c.Update(ctx, cluster) // cr-update-ok: on purpose, marked on its line
}

func passThrough(c client.Client, ctx client.Ctx, obj client.Object, opts ...client.UpdateOption) error {
	return c.Update(ctx, obj, opts...)
}

func generic[T client.Object](c client.Client, ctx client.Ctx, o T) {
	_ = c.Update(ctx, o) // want: may be a CR
}

type notAWriter struct{}

func (notAWriter) Update(ctx client.Ctx, obj client.Object) error { return nil }

func other(ctx client.Ctx) {
	_ = notAWriter{}.Update(ctx, &neo4jv1beta1.Neo4jEnterpriseCluster{})
}
`

// fakeImporter type-checks the fake packages from source.
type fakeImporter struct {
	fset     *token.FileSet
	packages map[string]*types.Package
}

func (f *fakeImporter) Import(path string) (*types.Package, error) {
	if pkg, ok := f.packages[path]; ok {
		return pkg, nil
	}
	src, ok := fakePackages[path]
	if !ok {
		return nil, fmt.Errorf("no fake package %q", path)
	}
	file, err := parser.ParseFile(f.fset, path+"/fake.go", src, 0)
	if err != nil {
		return nil, err
	}
	pkg, err := (&types.Config{Importer: f}).Check(path, f.fset, []*ast.File{file}, nil)
	if err != nil {
		return nil, err
	}
	f.packages[path] = pkg
	return pkg, nil
}

func TestFindUpdates_ReportsAWholeObjectUpdateOfACR(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture_test.go", fixture, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: &fakeImporter{fset: fset, packages: map[string]*types.Package{}}}
	if _, err := conf.Check("fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatalf("type-check the fixture: %v", err)
	}

	var want []int
	for i, line := range strings.Split(fixture, "\n") {
		if strings.Contains(line, "// want") {
			want = append(want, i+1)
		}
	}
	var got []int
	for _, f := range findUpdates(fset, file, info) {
		got = append(got, f.Pos.Line)
		if !strings.HasPrefix(f.What, "a whole ") {
			t.Errorf("line %d: %q does not say what it writes", f.Pos.Line, f.What)
		}
	}
	sort.Ints(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("reported lines %v, want %v", got, want)
	}
}

func TestFindUpdates_NamesTheCRType(t *testing.T) {
	cluster := types.NewNamed(
		types.NewTypeName(token.NoPos, types.NewPackage(apiPrefix+"v1beta1", "v1beta1"), "Neo4jEnterpriseCluster", nil),
		types.NewStruct(nil, nil), nil)
	if got := updatedObject(types.NewPointer(cluster), false); got != "a whole v1beta1.Neo4jEnterpriseCluster" {
		t.Errorf("got %q", got)
	}
	sts := types.NewNamed(
		types.NewTypeName(token.NoPos, types.NewPackage("k8s.io/api/apps/v1", "v1"), "StatefulSet", nil),
		types.NewStruct(nil, nil), nil)
	if got := updatedObject(types.NewPointer(sts), false); got != "" {
		t.Errorf("a child object is not reported, got %q", got)
	}
}

func TestHoldsTestCode(t *testing.T) {
	for _, tc := range []struct {
		pkg  listedPackage
		want bool
	}{
		{listedPackage{ImportPath: modulePath + "/internal/controller"}, false},
		{listedPackage{ImportPath: modulePath + "/internal/controller [" + modulePath + "/internal/controller.test]", ForTest: modulePath + "/internal/controller"}, true},
		{listedPackage{ImportPath: modulePath + "/internal/controller_test [" + modulePath + "/internal/controller.test]", ForTest: modulePath + "/internal/controller"}, true},
		{listedPackage{ImportPath: modulePath + "/internal/controller.test", ForTest: modulePath + "/internal/controller"}, false},
		{listedPackage{ImportPath: modulePath + "/test/testutil"}, true},
		{listedPackage{ImportPath: modulePath + "/test/integration.test"}, false},
	} {
		if got := holdsTestCode(tc.pkg); got != tc.want {
			t.Errorf("holdsTestCode(%s) = %v, want %v", tc.pkg.ImportPath, got, tc.want)
		}
	}
}
