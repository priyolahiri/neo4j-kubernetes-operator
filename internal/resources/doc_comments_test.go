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

package resources

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// docSubject is the identifier a Go doc comment opens with, by convention the
// name of what it documents.
var docSubject = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*) `)

// A function's doc comment that opens with another function's name documents
// that one, or one that is gone: the doc of clusterStrictPeerValidationEnabled
// opened with the note on OperatorUDCPackagingValue, a function #560 deleted,
// and buildClusterSSLPolicyTrustConfig's own doc sat in the middle of it, so
// that function had none.
func TestFunctionDocCommentsOpenWithTheirOwnName(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Doc == nil {
				continue
			}
			match := docSubject.FindStringSubmatch(fn.Doc.Text())
			if len(match) < 2 || match[1] == fn.Name.Name {
				continue
			}
			// Only a subject shaped like a function name, with a capital
			// after its first letter: an ordinary first word ("Pod hardening
			// …", "The …") is not one.
			subject := match[1]
			if !strings.ContainsAny(subject[1:], "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
				continue
			}
			t.Errorf("%s: the doc comment of %s opens with %s", fset.Position(fn.Pos()), fn.Name.Name, subject)
		}
	}
}
