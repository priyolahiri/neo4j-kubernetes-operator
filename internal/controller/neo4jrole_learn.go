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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Learn mode (--privilege-normalisation=learn, the DEFAULT): the same goal as
// the probe in neo4jrole_normalise.go — diff against how Neo4j STORED each
// statement, not the spec text — without the probe's short-lived roles, which
// land in the security log. That was judged not acceptable as a default; the
// probe stays available as an opt-in for exact attribution.
//
// Instead of probing, it learns from the operator's own GRANT on the real
// role: read the role's rows, grant, read again, and the new rows are the
// statement's rendering. That is persisted in status, so it survives
// restarts, and it writes nothing to Neo4j that the operator was not going to
// write anyway.
//
// What it cannot see is a row that existed BEFORE the grant — the grant is
// then a no-op for that row. Two rules keep that from ever costing a
// privilege:
//
//   - Rows the role had when learn mode first met it are recorded as
//     pre-existing and never revoked automatically. They may be a spec
//     statement's own rows; learn mode cannot know, so it does not guess.
//   - Rows can hide a statement's rendering only while something else holds
//     them. When that something goes away — a pre-existing row disappears,
//     or rows of a statement removed from spec are revoked — every statement
//     is re-learned, so the grant that follows recreates whatever was hidden.
//
// It re-learns, too, when the server, its image or the alias layout changes.
// It does not MOVE a privilege granted through an alias that is later
// retargeted: the old target's rows are kept, as Neo4j itself keeps them.
// Probe mode does move it.

// Privilege normalisation modes.
const (
	PrivilegeNormalisationProbe = "probe"
	PrivilegeNormalisationLearn = "learn"
)

var privilegeNormalisationMode = PrivilegeNormalisationLearn

// SetPrivilegeNormalisation selects how every Neo4jRole reconciler in this
// process learns Neo4j's stored form of a privilege. Called once from main.
func SetPrivilegeNormalisation(mode string) error {
	switch mode {
	case PrivilegeNormalisationProbe, PrivilegeNormalisationLearn:
		privilegeNormalisationMode = mode
		return nil
	}
	return fmt.Errorf("--privilege-normalisation must be %q or %q, got %q",
		PrivilegeNormalisationProbe, PrivilegeNormalisationLearn, mode)
}

func (r *Neo4jRoleReconciler) learnMode() bool {
	if r.PrivilegeNormalisation != "" {
		return r.PrivilegeNormalisation == PrivilegeNormalisationLearn
	}
	return privilegeNormalisationMode == PrivilegeNormalisationLearn
}

// privilegeLearner is the one client call learn mode needs.
type privilegeLearner interface {
	GrantAndReadPrivileges(ctx context.Context, roleName, stmt string) (before, after []string, err error)
}

type learnedRendering struct {
	rows      []string // canonical
	ambiguous bool
}

// learnState is the persisted part of learn mode, as held in status.
type learnState struct {
	scope      string // digest
	renderings map[string]learnedRendering
	baseline   map[string]bool
}

func learnStateFromStatus(st neo4jv1beta1.Neo4jRoleStatus) learnState {
	s := learnState{
		scope:      st.PrivilegeRenderingScope,
		renderings: map[string]learnedRendering{},
		baseline:   map[string]bool{},
	}
	for _, r := range st.PrivilegeRenderings {
		s.renderings[r.Statement] = learnedRendering{rows: r.Rows, ambiguous: r.Ambiguous}
	}
	for _, row := range st.UnattributedPrivileges {
		s.baseline[row] = true
	}
	return s
}

// toStatus renders the state in spec order, so status is stable.
func (s learnState) toStatus(stmts []string) ([]neo4jv1beta1.PrivilegeRendering, []string) {
	var out []neo4jv1beta1.PrivilegeRendering
	seen := map[string]bool{}
	for _, stmt := range stmts {
		lr, ok := s.renderings[stmt]
		if !ok || seen[stmt] {
			continue
		}
		seen[stmt] = true
		out = append(out, neo4jv1beta1.PrivilegeRendering{Statement: stmt, Rows: lr.rows, Ambiguous: lr.ambiguous})
	}
	return out, sortedKeys(s.baseline)
}

// learnOutcome is one learning pass.
type learnOutcome struct {
	state       learnState
	desired     map[string]string // canonical row -> spec statement
	current     map[string]bool   // the role's rows after the pass
	granted     int               // GRANT/DENY statements executed
	ambiguous   []string          // statements whose rows are unknown
	unresolved  []string          // statements naming a missing database
	grantErrStm string            // statement whose grant failed, when err != nil
}

// scopeDigest keeps the NUL-separated renderingScope out of status.
func scopeDigest(scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return hex.EncodeToString(sum[:8])
}

// learnDesired runs one learning pass over the spec statements.
//
// current is the role's canonical rows now. forceRelearn re-grants every
// statement; it is set by the caller after revoking rows of statements that
// left the spec, since those rows may have been hiding another statement's.
func learnDesired(
	ctx context.Context, learner privilegeLearner, roleName string,
	stmts []string, resolves func(string) bool,
	prior learnState, scope string, current map[string]bool, forceRelearn bool,
) (learnOutcome, error) {
	digest := scopeDigest(scope)
	out := learnOutcome{
		state:   learnState{scope: digest, renderings: map[string]learnedRendering{}, baseline: map[string]bool{}},
		desired: map[string]string{},
		current: copySet(current),
	}

	relearnAll := forceRelearn
	if prior.scope == "" {
		// First time learn mode meets this role: whatever it already has is
		// unattributable.
		for row := range current {
			out.state.baseline[row] = true
		}
	} else {
		for row := range prior.baseline {
			if current[row] {
				out.state.baseline[row] = true
			} else {
				// It may have been hiding a statement's row.
				relearnAll = true
			}
		}
		if prior.scope != digest {
			relearnAll = true
		}
	}

	add := func(row, stmt string) {
		if _, exists := out.desired[row]; !exists {
			out.desired[row] = stmt
		}
	}

	seen := map[string]bool{}
	for _, stmt := range stmts {
		fallback := neo4jclient.CanonicalisePrivilegeStatement(stmt)
		if fallback == "" || seen[stmt] {
			continue
		}
		seen[stmt] = true
		p, had := prior.renderings[stmt]

		if resolves != nil && !resolves(stmt) {
			// Neither granted nor learned while its database is missing; a
			// rendering learned earlier is kept for when it returns.
			if had {
				out.state.renderings[stmt] = p
			}
			out.unresolved = append(out.unresolved, stmt)
			continue
		}

		if had && !relearnAll {
			if p.ambiguous {
				out.state.renderings[stmt] = p
				out.ambiguous = append(out.ambiguous, stmt)
				continue
			}
			if subsetOf(p.rows, out.current) {
				out.state.renderings[stmt] = p
				for _, row := range p.rows {
					add(row, stmt)
				}
				continue
			}
		}

		before, after, err := learner.GrantAndReadPrivileges(ctx, roleName, stmt)
		if err != nil {
			// Keep what was known about every statement this pass did not
			// reach. Losing a rendering is not harmless: the next grant of
			// that statement is a no-op, its rows would look foreign, and
			// they would be revoked.
			for st, lr := range prior.renderings {
				if _, done := out.state.renderings[st]; !done {
					out.state.renderings[st] = lr
				}
			}
			out.grantErrStm = stmt
			return out, err
		}
		out.granted++
		b, a := canonicalSet(before), canonicalSet(after)
		out.current = a

		rows := map[string]bool{}
		for row := range a {
			if !b[row] {
				rows[row] = true
			}
		}
		// Rows learned before that are still there remain the statement's:
		// the grant was a no-op for them, which is why they are not new.
		if had && !p.ambiguous {
			for _, row := range p.rows {
				if a[row] {
					rows[row] = true
				}
			}
		}
		// Everything already existed: if the statement is written in stored
		// form, its own text is the row.
		if len(rows) == 0 && a[fallback] {
			rows[fallback] = true
		}

		if len(rows) == 0 {
			out.state.renderings[stmt] = learnedRendering{ambiguous: true}
			out.ambiguous = append(out.ambiguous, stmt)
			continue
		}
		lr := learnedRendering{rows: sortedKeys(rows)}
		out.state.renderings[stmt] = lr
		for _, row := range lr.rows {
			add(row, stmt)
		}
	}

	// While any statement's rows are unknown, a row nobody claims may be one
	// of them — including one the operator granted just before a crash kept
	// it out of status. Treat it as unattributed rather than foreign. Rows of
	// statements that LEFT the spec are attributable, and stay revocable.
	if len(out.ambiguous) > 0 {
		orphans := orphanedRows(prior, stmts)
		for row := range out.current {
			if _, claimed := out.desired[row]; !claimed && !orphans[row] {
				out.state.baseline[row] = true
			}
		}
	}
	return out, nil
}

// orphanedRows are rows learned for statements that are no longer in spec.
func orphanedRows(prior learnState, stmts []string) map[string]bool {
	inSpec := map[string]bool{}
	for _, s := range stmts {
		inSpec[s] = true
	}
	out := map[string]bool{}
	for stmt, lr := range prior.renderings {
		if inSpec[stmt] {
			continue
		}
		for _, row := range lr.rows {
			out[row] = true
		}
	}
	return out
}

func canonicalSet(commands []string) map[string]bool {
	out := make(map[string]bool, len(commands))
	for _, c := range commands {
		if canon := neo4jclient.CanonicalisePrivilegeStatement(c); canon != "" {
			out[canon] = true
		}
	}
	return out
}

func subsetOf(rows []string, set map[string]bool) bool {
	for _, r := range rows {
		if !set[r] {
			return false
		}
	}
	return true
}

func copySet(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// reconcileLearned is the privilege diff in learn mode.
func (r *Neo4jRoleReconciler) reconcileLearned(
	ctx context.Context, nc *neo4jclient.Client, role *neo4jv1beta1.Neo4jRole,
	roleName, scope string, resolves func(string) bool, requeue time.Duration,
) (ctrl.Result, error) {
	current, _, _, err := r.fetchCurrentPrivileges(ctx, nc, roleName)
	if err != nil {
		return r.fail(ctx, role, "fetch current privileges failed", err, requeue)
	}
	prior := learnStateFromStatus(role.Status)
	stmts := role.Spec.Privileges

	// First meeting: record what the role already has BEFORE granting
	// anything. It makes the baseline durable ahead of the grants, and a CRD
	// too old to hold learn-mode status fails here — before a single
	// statement is sent — instead of after a round of grants on every
	// reconcile.
	if prior.scope == "" {
		first := learnState{scope: scopeDigest(scope), renderings: map[string]learnedRendering{}, baseline: toSet(current)}
		if err := r.setLearnState(ctx, role, first, stmts); err != nil {
			return r.fail(ctx, role, "persisting learned privilege renderings failed", err, requeue)
		}
		prior = first
	}

	lo, err := learnDesired(ctx, nc, roleName, stmts, resolves, prior, scope, toSet(current), false)
	if err != nil {
		// Record what was learned before the failure: those grants happened.
		_ = r.setLearnState(ctx, role, lo.state, stmts)
		return r.fail(ctx, role, fmt.Sprintf("apply privilege %q failed", lo.grantErrStm), err, requeue)
	}
	// Persist BEFORE revoking. If the renderings do not reach status, the
	// next reconcile cannot attribute the rows just granted, and revoking
	// against a desired set that exists only in memory is how a privilege
	// the spec asks for gets removed.
	if err := r.setLearnState(ctx, role, lo.state, stmts); err != nil {
		return r.fail(ctx, role, "persisting learned privilege renderings failed", err, requeue)
	}

	// Re-read: the learning grants changed the role.
	now, immutableSet, currentByCanonical, err := r.fetchCurrentPrivileges(ctx, nc, roleName)
	if err != nil {
		return r.fail(ctx, role, "fetch current privileges failed", err, requeue)
	}
	var toRemove []string
	if role.Spec.EnforcePrivileges {
		for _, row := range now {
			if _, wanted := lo.desired[row]; wanted || lo.state.baseline[row] {
				continue
			}
			toRemove = append(toRemove, row)
		}
	}

	drift, revoked, res, err := r.revokeUnwanted(ctx, nc, role, toRemove, immutableSet, currentByCanonical, requeue)
	if res != nil {
		return *res, err
	}

	// A revoked row of a statement that left the spec may have been hiding
	// one of another statement's rows (both grant the same thing). Re-learn
	// everything: the grants recreate whatever was hidden.
	applied := lo.granted
	orphans := orphanedRows(prior, stmts)
	for _, row := range revoked {
		if !orphans[row] {
			continue
		}
		after, _, _, err := r.fetchCurrentPrivileges(ctx, nc, roleName)
		if err != nil {
			return r.fail(ctx, role, "fetch current privileges failed", err, requeue)
		}
		relearned, err := learnDesired(ctx, nc, roleName, stmts, resolves, lo.state, scope, toSet(after), true)
		if err != nil {
			_ = r.setLearnState(ctx, role, relearned.state, stmts)
			return r.fail(ctx, role, fmt.Sprintf("apply privilege %q failed", relearned.grantErrStm), err, requeue)
		}
		if err := r.setLearnState(ctx, role, relearned.state, stmts); err != nil {
			return r.fail(ctx, role, "persisting learned privilege renderings failed", err, requeue)
		}
		lo, applied = relearned, applied+relearned.granted
		break
	}

	syncedStatus, syncedReason, syncedMsg := metav1.ConditionTrue, ConditionReasonPrivilegesSynced, "privileges match spec"
	var unattributed []string
	for _, row := range sortedKeys(lo.state.baseline) {
		if _, wanted := lo.desired[row]; !wanted {
			unattributed = append(unattributed, row)
		}
	}
	switch {
	case drift:
		syncedStatus, syncedReason, syncedMsg = metav1.ConditionFalse, ConditionReasonPrivilegesDrifted,
			"some privileges could not be reconciled (e.g. immutable). See events."
	case len(unattributed) > 0 || len(lo.ambiguous) > 0:
		syncedStatus, syncedReason = metav1.ConditionUnknown, ConditionReasonPrivilegesUnattributed
		syncedMsg = unattributedMessage(unattributed, lo.ambiguous)
	}
	return r.finishReconcile(ctx, nc, role, roleName, applied, len(revoked), len(lo.unresolved), drift,
		syncedStatus, syncedReason, syncedMsg, requeue)
}

func unattributedMessage(rows, ambiguous []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "learn mode (--privilege-normalisation=learn) cannot attribute %d privilege row(s) on this role "+
		"to a spec statement, so it does not revoke them", len(rows))
	if len(rows) > 0 {
		fmt.Fprintf(&b, ": %s", summarise(rows, 5))
	}
	b.WriteString(".")
	if len(ambiguous) > 0 {
		fmt.Fprintf(&b, " Statements whose stored rows are unknown, because everything they grant was already there: %s.",
			summarise(ambiguous, 5))
	}
	b.WriteString(" Remove unwanted rows by hand — learn mode re-learns once a row it could not attribute goes away — " +
		"or run the operator with --privilege-normalisation=probe, which attributes every row exactly.")
	return b.String()
}

func summarise(items []string, max int) string {
	if len(items) <= max {
		return quoteAndJoin(items)
	}
	return fmt.Sprintf("%s and %d more", quoteAndJoin(items[:max]), len(items)-max)
}

var errNeo4jRoleCRDOutdated = fmt.Errorf("the installed Neo4jRole CRD predates this operator and drops " +
	"status.privilegeRenderings, which --privilege-normalisation=learn needs; upgrade the CRDs " +
	"(kubectl apply -f the chart's crds/ directory — helm upgrade does not update CRDs)")

// setLearnState writes the learned renderings to status, only if they changed.
func (r *Neo4jRoleReconciler) setLearnState(ctx context.Context, role *neo4jv1beta1.Neo4jRole, st learnState, stmts []string) error {
	renderings, baseline := st.toStatus(stmts)
	update := func() error {
		latest := &neo4jv1beta1.Neo4jRole{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(role), latest); err != nil {
			return err
		}
		if latest.Status.PrivilegeRenderingScope == st.scope &&
			renderingsEqual(latest.Status.PrivilegeRenderings, renderings) &&
			stringSlicesEqual(latest.Status.UnattributedPrivileges, baseline) {
			return nil
		}
		latest.Status.PrivilegeRenderingScope = st.scope
		latest.Status.PrivilegeRenderings = renderings
		latest.Status.UnattributedPrivileges = baseline
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		// The API server PRUNES status fields its CRD does not declare, and
		// reports success. An operator upgraded without its CRDs — `helm
		// upgrade` never touches crds/ — would then forget every rendering
		// between reconciles and quietly stop enforcing. Say so instead.
		if latest.Status.PrivilegeRenderingScope != st.scope {
			return errNeo4jRoleCRDOutdated
		}
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultBackoff, update); err != nil {
		log.FromContext(ctx).Error(err, "failed to persist learned privilege renderings")
		return err
	}
	// Keep the in-hand object current for the rest of this reconcile.
	role.Status.PrivilegeRenderingScope = st.scope
	role.Status.PrivilegeRenderings = renderings
	role.Status.UnattributedPrivileges = baseline
	return nil
}

func renderingsEqual(a, b []neo4jv1beta1.PrivilegeRendering) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Statement != b[i].Statement || a[i].Ambiguous != b[i].Ambiguous || !stringSlicesEqual(a[i].Rows, b[i].Rows) {
			return false
		}
	}
	return true
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func toSet(rows []string) map[string]bool {
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		out[r] = true
	}
	return out
}
