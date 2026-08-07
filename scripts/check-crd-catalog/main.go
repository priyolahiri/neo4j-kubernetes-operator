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

// Command check-crd-catalog guards the hand-written places that enumerate the
// operator's CRDs. Adding a CRD touches a lot of generated artifacts, all of
// which are covered by `make check-drift` — but the human-facing catalogues are
// authored by hand and nothing regenerates them, so a new Kind silently fails
// to appear.
//
// That is not hypothetical: the whole 12-CRD Aura suite was absent from the
// docs-site landing page (docs/index.md) from the moment it shipped until this
// check was written, while being correctly listed in README.md and the mkdocs
// nav the entire time. Two of three surfaces being right is exactly the shape
// of drift a human review misses.
//
// Surfaces checked, for every CRD Kind:
//
//   - docs/index.md — the docs-site landing page (mentions the Kind)
//   - README.md     — the GitHub landing page (mentions the Kind)
//   - mkdocs.yml    — nav entry pointing at the Kind's api_reference page,
//     without which the page exists but is unreachable
//   - docs/gitops/argocd-health-checks.yaml — a health check per Kind. This one
//     is not cosmetic: without an entry ArgoCD cannot tell whether the resource
//     is ready, so it reports the CR as Progressing forever and any sync-wave
//     or health gate behind it never completes.
//
// The per-page content of api_reference/ is a separate concern, covered by
// check-apiref-drift.
//
// Usage: go run ./scripts/check-crd-catalog [repoRoot]
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
	} `json:"spec"`
}

// surface is one hand-written catalogue that must mention every CRD.
type surface struct {
	// label is what the failure message calls this file.
	label string
	// path is relative to the repo root.
	path string
	// mentions reports whether the file accounts for the given Kind.
	mentions func(body, kind string) bool
	// fix is the remediation shown when a Kind is missing.
	fix string

	body string
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}

	surfaces := []*surface{
		{
			label: "docs/index.md",
			path:  filepath.Join("docs", "index.md"),
			// Backticked Kind, as the CRD tables on that page write it.
			mentions: func(body, kind string) bool { return strings.Contains(body, "`"+kind+"`") },
			fix:      "add the Kind to a Custom Resource Definitions table on the docs-site landing page",
		},
		{
			label:    "README.md",
			path:     "README.md",
			mentions: func(body, kind string) bool { return strings.Contains(body, "`"+kind+"`") },
			fix:      "add the Kind to a CRD table in the README",
		},
		{
			label: "mkdocs.yml nav",
			path:  "mkdocs.yml",
			// The nav must point at the Kind's api_reference page, or the page
			// is published but unreachable from the site navigation.
			mentions: func(body, kind string) bool {
				return strings.Contains(body, "api_reference/"+strings.ToLower(kind)+".md")
			},
			fix: "add `- <Kind>: api_reference/<kind>.md` under the CRD Reference nav section",
		},
		{
			label: "docs/gitops/argocd-health-checks.yaml",
			path:  filepath.Join("docs", "gitops", "argocd-health-checks.yaml"),
			// ArgoCD keys health customizations on
			// `resource.customizations.health.<group>_<Kind>`.
			mentions: func(body, kind string) bool {
				return strings.Contains(body, "resource.customizations.health.neo4j.neo4j.com_"+kind+":")
			},
			fix: "add a `resource.customizations.health.neo4j.neo4j.com_<Kind>` Lua block — " +
				"without it ArgoCD reports the CR as Progressing forever",
		},
	}

	for _, s := range surfaces {
		data, err := os.ReadFile(filepath.Join(root, s.path))
		if err != nil {
			fatal("reading %s: %v", s.path, err)
		}
		s.body = string(data)
	}

	kinds := readKinds(filepath.Join(root, "config", "crd", "bases"))
	if len(kinds) == 0 {
		fatal("no CRDs found under config/crd/bases — wrong repo root?")
	}

	var problems []string
	for _, s := range surfaces {
		var missing []string
		for _, kind := range kinds {
			if !s.mentions(s.body, kind) {
				missing = append(missing, kind)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			problems = append(problems, fmt.Sprintf("%s does not list %d CRD(s): %s\n      Fix: %s",
				s.label, len(missing), strings.Join(missing, ", "), s.fix))
		}
	}

	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, "check-crd-catalog: FAILED — hand-written CRD catalogues are out of sync:")
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "  - "+p)
		}
		fmt.Fprintln(os.Stderr, "\nThese pages are NOT generated. Adding a CRD means updating them by hand.")
		os.Exit(1)
	}

	fmt.Printf("check-crd-catalog: OK — all %d CRD(s) listed in docs/index.md, README.md, the mkdocs nav and the ArgoCD health checks.\n",
		len(kinds))
}

func readKinds(crdDir string) []string {
	entries, err := os.ReadDir(crdDir)
	if err != nil {
		fatal("reading CRD bases dir %s: %v", crdDir, err)
	}
	var kinds []string
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
		if c.Spec.Names.Kind != "" {
			kinds = append(kinds, c.Spec.Names.Kind)
		}
	}
	sort.Strings(kinds)
	return kinds
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "check-crd-catalog: "+format+"\n", a...)
	os.Exit(2)
}
