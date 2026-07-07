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

// Command check-apiref-drift guards the hand-written per-CRD reference pages in
// docs/api_reference/ against silent drift from the CRD types. Unlike the
// generated artifacts (CRD YAML, deepcopy, Helm, bundle) these pages are
// authored by hand — nothing regenerates them — so a new or renamed spec field
// can leave the docs stale (this is exactly how clusterRef/auraInstanceRef went
// undocumented). This check asserts, for every CRD, that a page exists and that
// every TOP-LEVEL spec field is mentioned (as a backticked `field`) somewhere on
// the page. It intentionally checks only top-level fields — nested sub-fields
// live in sub-type tables that are harder to map and lower-drift.
//
// Usage: go run ./scripts/check-apiref-drift [repoRoot]
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

type crdDoc struct {
	Spec struct {
		Names struct {
			Kind string `json:"kind"`
		} `json:"names"`
		Versions []struct {
			Schema struct {
				OpenAPIV3Schema struct {
					Properties struct {
						Spec struct {
							Properties map[string]any `json:"properties"`
						} `json:"spec"`
					} `json:"properties"`
				} `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	crdDir := filepath.Join(root, "config", "crd", "bases")
	refDir := filepath.Join(root, "docs", "api_reference")

	entries, err := os.ReadDir(crdDir)
	if err != nil {
		fatal("reading CRD bases dir %s: %v", crdDir, err)
	}

	var problems []string
	pagesChecked, fieldsChecked := 0, 0

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(crdDir, e.Name()))
		if err != nil {
			fatal("reading %s: %v", e.Name(), err)
		}
		var c crdDoc
		if err := yaml.Unmarshal(data, &c); err != nil {
			fatal("parsing %s: %v", e.Name(), err)
		}
		kind := c.Spec.Names.Kind
		if kind == "" {
			continue
		}

		// Union of top-level spec field names across all served versions.
		fields := map[string]bool{}
		for _, v := range c.Spec.Versions {
			for k := range v.Schema.OpenAPIV3Schema.Properties.Spec.Properties {
				fields[k] = true
			}
		}

		page := filepath.Join(refDir, strings.ToLower(kind)+".md")
		content, err := os.ReadFile(page)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: missing api_reference page (expected %s)", kind, page))
			continue
		}
		pagesChecked++
		body := string(content)

		var missing []string
		for f := range fields {
			fieldsChecked++
			if !strings.Contains(body, "`"+f+"`") {
				missing = append(missing, f)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			problems = append(problems, fmt.Sprintf("%s (%s): spec field(s) not documented: %s",
				kind, filepath.Base(page), strings.Join(missing, ", ")))
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		fmt.Fprintln(os.Stderr, "check-apiref-drift: FAILED — api_reference pages are out of sync with CRD spec fields:")
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "  - "+p)
		}
		fmt.Fprintln(os.Stderr, "\nFix: document each field as `fieldName` in the page's Spec table, or add the missing page.")
		fmt.Fprintln(os.Stderr, "(Only TOP-LEVEL spec fields are checked.)")
		os.Exit(1)
	}
	fmt.Printf("check-apiref-drift: OK — %d api_reference page(s), %d top-level spec field(s) all documented.\n",
		pagesChecked, fieldsChecked)
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "check-apiref-drift: "+format+"\n", a...)
	os.Exit(2)
}
