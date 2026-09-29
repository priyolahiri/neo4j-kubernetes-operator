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
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Privilege normalisation: matching spec.privileges against what Neo4j stores.
//
// Neo4j stores a privilege in its own form, not as written (see
// Client.ProbePrivilegeRendering for the rules, measured on 5.26 and CalVer).
// The role controller used to compare the spec text with the stored rows
// directly, so any statement written with a plural, a list, a default segment,
// a differently-cased database name or an alias never matched. Under
// enforcePrivileges (the default) that is worse than a false "drifted": the
// reconcile re-granted the spec statement — a no-op — and revoked the stored
// row as foreign, so the privilege was present on one reconcile and gone on the
// next, while PrivilegesSynced reported a match. The shipped sample role
// (`... NODES * TO ...`) did exactly this.
//
// So the desired set is the server's own rendering of each statement: granted
// once to a throwaway role, read back, cached. The operator never has to know
// the grammar, and the result is exact for any statement the server accepts.

// probeRolePrefix names the throwaway roles. A role with this prefix that
// outlives a reconcile was left by an operator crash mid-probe; it holds only
// the one privilege being probed and has no users.
const probeRolePrefix = "operator_privilege_probe_"

// renderingPlaceholderRole is the grantee cached rows are stored under, so
// one cache entry serves every role granting the same statement.
const renderingPlaceholderRole = "operator_rendering_grantee"

const (
	// renderingCacheTTL bounds how long a rendering is trusted. The key
	// already covers what changes it — the server image and the alias map —
	// so this only heals an upgrade that probed a not-yet-upgraded server.
	renderingCacheTTL = time.Hour
	// renderingCacheMax caps memory; exceeding it clears the cache, which
	// costs one probe per statement on the next reconcile of each role.
	renderingCacheMax = 4096
)

// privilegeProber is the one client call normalisation needs.
type privilegeProber interface {
	ProbePrivilegeRendering(ctx context.Context, probeRole, stmt string) ([]string, error)
}

type renderingEntry struct {
	rows []string // granted TO renderingPlaceholderRole
	at   time.Time
}

// privilegeRenderingCache is safe for concurrent reconciles.
type privilegeRenderingCache struct {
	mu      sync.Mutex
	entries map[string]renderingEntry
	now     func() time.Time
}

func newPrivilegeRenderingCache() *privilegeRenderingCache {
	return &privilegeRenderingCache{entries: map[string]renderingEntry{}, now: time.Now}
}

func (c *privilegeRenderingCache) get(key string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || c.now().Sub(e.at) > renderingCacheTTL {
		return nil, false
	}
	return e.rows, true
}

func (c *privilegeRenderingCache) put(key string, rows []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= renderingCacheMax {
		c.entries = map[string]renderingEntry{}
	}
	c.entries[key] = renderingEntry{rows: rows, at: c.now()}
}

// renderingScope identifies everything a statement's rendering depends on
// besides its text: which server (target UID), which version (image), and
// where each alias points (Neo4j stores a grant on an alias against the
// alias's target database).
func renderingScope(target ResolvedTarget, aliasFingerprint string) string {
	var uid, image string
	switch {
	case target.Cluster != nil:
		uid = string(target.Cluster.UID)
		image = target.Cluster.Spec.Image.Repo + ":" + target.Cluster.Spec.Image.Tag
	case target.Standalone != nil:
		uid = string(target.Standalone.UID)
		image = target.Standalone.Spec.Image.Repo + ":" + target.Standalone.Spec.Image.Tag
	}
	return uid + "\x00" + image + "\x00" + aliasFingerprint
}

// aliasFingerprint is a stable string of every alias → target mapping.
func aliasFingerprint(aliases []neo4jclient.AliasInfo) string {
	pairs := make([]string, 0, len(aliases))
	for _, a := range aliases {
		pairs = append(pairs, strings.ToLower(a.Name)+"="+strings.ToLower(a.Database)+"@"+a.URL)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// normaliseDesired returns the canonical rows the spec privileges should
// produce on the server, and a map from each row back to the spec statement
// that produces it (the statement that is executed when the row is missing).
//
// A statement that cannot be probed falls back to its textual canonical form,
// which is what the controller did before normalisation existed:
//   - one naming a database that does not exist (probe: false) — the server
//     would refuse the probe grant exactly as it refuses the real one, and the
//     statement is skipped and reported anyway;
//   - one the probe grant failed on — the real grant will fail the same way,
//     and that failure, not this one, is what the user should see.
func (r *Neo4jRoleReconciler) normaliseDesired(
	ctx context.Context, prober privilegeProber, scope, roleName string,
	stmts []string, probe func(stmt string) bool,
) ([]string, map[string]string) {
	logger := log.FromContext(ctx)
	cache := r.renderingCache()

	set := map[string]struct{}{}
	byCanonical := map[string]string{}
	add := func(canon, stmt string) {
		set[canon] = struct{}{}
		if _, exists := byCanonical[canon]; !exists {
			byCanonical[canon] = stmt // first spec statement wins for a row
		}
	}

	for _, stmt := range stmts {
		fallback := neo4jclient.CanonicalisePrivilegeStatement(stmt)
		if fallback == "" {
			continue
		}
		rows, ok := r.renderStatement(ctx, prober, cache, scope, stmt, probe)
		if !ok {
			add(fallback, stmt)
			continue
		}
		for _, row := range rows {
			mapped, err := neo4jclient.ReplacePrivilegeGrantee(row, roleName)
			if err != nil {
				logger.V(1).Info("stored privilege row has no grantee; using spec text", "row", row)
				add(fallback, stmt)
				continue
			}
			add(neo4jclient.CanonicalisePrivilegeStatement(mapped), stmt)
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, byCanonical
}

// renderStatement returns the stored rows for stmt, granted TO the
// placeholder role, from the cache or a fresh probe.
func (r *Neo4jRoleReconciler) renderStatement(
	ctx context.Context, prober privilegeProber, cache *privilegeRenderingCache,
	scope, stmt string, probe func(stmt string) bool,
) ([]string, bool) {
	logger := log.FromContext(ctx)

	keyed, err := neo4jclient.ReplacePrivilegeGrantee(stmt, renderingPlaceholderRole)
	if err != nil {
		return nil, false
	}
	key := scope + "\x00" + keyed
	if rows, ok := cache.get(key); ok {
		return rows, true
	}
	if probe != nil && !probe(stmt) {
		return nil, false
	}

	probeRole := probeRolePrefix + randomHex(8)
	probeStmt, err := neo4jclient.ReplacePrivilegeGrantee(stmt, probeRole)
	if err != nil {
		return nil, false
	}
	stored, err := prober.ProbePrivilegeRendering(ctx, probeRole, probeStmt)
	if err != nil {
		logger.V(1).Info("could not probe privilege rendering; comparing spec text", "statement", stmt, "error", err)
		return nil, false
	}
	if len(stored) == 0 {
		// Accepted but stored nothing: nothing to learn, and caching an empty
		// rendering would make the statement's own row look foreign.
		return nil, false
	}
	rows := make([]string, 0, len(stored))
	for _, row := range stored {
		placeholder, err := neo4jclient.ReplacePrivilegeGrantee(row, renderingPlaceholderRole)
		if err != nil {
			return nil, false
		}
		rows = append(rows, placeholder)
	}
	cache.put(key, rows)
	return rows, true
}

func (r *Neo4jRoleReconciler) renderingCache() *privilegeRenderingCache {
	r.renderingCacheOnce.Do(func() {
		if r.rendering == nil {
			r.rendering = newPrivilegeRenderingCache()
		}
	})
	return r.rendering
}

func randomHex(n int) string {
	b := make([]byte, n/2+1)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
