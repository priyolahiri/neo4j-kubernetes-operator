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
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
)

// guidance is what a condition or phase means, and what to do about it.
type guidance struct {
	meaning string
	action  string
}

// conditionGuidance is keyed off the operator's OWN exported constants, not
// string literals.
//
// That is the whole anti-drift mechanism here: every entry in this map is a
// claim about operator behaviour, and this file does not own that behaviour. If
// a condition type is renamed or removed in internal/controller, this map stops
// compiling instead of quietly explaining something that no longer exists.
// TestConditionGuidanceCoversEveryConditionType keeps the reverse direction
// honest — a condition ADDED without guidance fails a test here.
var conditionGuidance = map[string]guidance{
	controller.ConditionTypeReady: {
		meaning: "the resource has reached its desired state.",
		action:  "Nothing to do.",
	},
	controller.ConditionTypeProgressing: {
		meaning: "the operator is actively working towards the desired state.",
		action:  "Wait. If it does not settle, check the operator log and the resource's events.",
	},
	controller.ConditionTypeAvailable: {
		meaning: "the deployment is serving, though not necessarily at full capacity.",
		action:  "Compare with ServersHealthy to see whether every server is contributing.",
	},
	controller.ConditionTypeDegraded: {
		meaning: "the resource is running but something is wrong.",
		action:  "Read the condition message, then `kubectl neo4j support-bundle` if it is not obvious.",
	},
	controller.ConditionTypeServersHealthy: {
		meaning: "every server reported Enabled and Available by SHOW SERVERS.",
		action:  "When false, the message names the unhealthy servers. Check their pod logs for OOMKilled (exit 137) — Enterprise needs at least 1.5Gi.",
	},
	controller.ConditionTypeDatabasesHealthy: {
		meaning: "every database's actual status matches its requested status. The system database is excluded.",
		action:  "When false, a database is likely stuck creating or has lost quorum. `kubectl neo4j cypher <name> -c \"SHOW DATABASES\"` shows the detail.",
	},
	controller.ConditionTypeServersPendingDrain: {
		meaning: "a scale-down is waiting for servers to hand off their data before they can be removed.",
		action:  "Wait. Removing the pods by hand loses the data those servers still hold.",
	},
	controller.ConditionTypeClusterNotReady: {
		meaning: "this resource depends on a Neo4j deployment that is not Ready yet.",
		action:  "This resolves itself once the deployment is Ready — apply order does not matter. Check the deployment with `kubectl neo4j status`.",
	},
	controller.ConditionTypePendingDependencies: {
		meaning: "something this resource references does not exist yet — commonly a Secret or a custom role.",
		action:  "Apply the missing dependency. The operator re-reconciles when it appears; you do not need to re-apply this resource.",
	},
	controller.ConditionTypePasswordSynced: {
		meaning: "the user's password in Neo4j matches the referenced Secret.",
		action:  "When false, check the Secret exists and has the expected key. A password beginning with \"-\" is rejected.",
	},
	controller.ConditionTypeRolesSynced: {
		meaning: "the user's roles in Neo4j match spec.roles.",
		action:  "When false, a listed role may not exist. Create the Neo4jRole; the user controller watches roles and retries when one lands.",
	},
	controller.ConditionTypePrivilegesSynced: {
		meaning: "the role's privileges in Neo4j match spec.privileges.",
		action:  "When false, read the message for the failing statement. With enforcePrivileges true the spec is authoritative and drift is reverted each loop.",
	},
	controller.ConditionTypeUserNotFound: {
		meaning: "a referenced Neo4j user does not exist.",
		action:  "Create the Neo4jUser, or correct the reference.",
	},
	controller.ConditionTypeOIDCProviderConfigured: {
		meaning: "the deployment's OIDC provider settings were accepted.",
		action:  "When false, the message names the setting at fault.",
	},
	controller.ConditionTypeAuthRuleVersionTooOld: {
		meaning: "the Neo4j version running is older than this auth rule requires.",
		action:  "Upgrade the deployment, or remove the rule. The operator will not apply it to an unsupported version.",
	},
}

// phaseGuidance covers phases that exist as constants in the API package. Only
// those: phases defined as inline literals in a controller are deliberately
// omitted rather than duplicated here as strings, since a string copy is
// exactly the drift this design avoids elsewhere.
//
// The shared lifecycle vocabulary (v1beta1.AllPhases) is covered in full, and
// TestEveryPhaseConstantHasGuidance fails if an entry is added without it.
// Before that existed this map held only the replica phases, so `explain`
// answered "no guidance for this phase — it may be newer than this CLI" for
// Ready, which every healthy resource reports.
var phaseGuidance = map[string]guidance{
	neo4jv1beta1.PhaseReady: {
		meaning: "the resource reached its desired state and the operator is holding it there.",
		action:  "Nothing to do. Drift is corrected on the next reconcile, so a hand-edit made outside the CR will be reverted.",
	},
	neo4jv1beta1.PhaseInstalled: {
		meaning: "the plugin is installed on every server and its configuration is applied.",
		action:  "Nothing to do. Verify from the database itself if you want proof, e.g. RETURN apoc.version().",
	},
	neo4jv1beta1.PhaseCompleted: {
		meaning: "a one-shot operation — a backup, a restore, or a promotion — finished successfully.",
		action:  "Nothing to do. status.history records what was written and where; for a promotion, status.observedLagTxIds records the RPO actually taken, which is worth keeping if you are writing up the incident.",
	},
	neo4jv1beta1.PhaseFailed: {
		meaning: "the operation will not be retried without a change: the operator judged the cause permanent.",
		action:  "status.message names the cause. Fix the spec or the cluster-side precondition it names, then re-apply — `kubectl neo4j preflight` checks the cluster-side ones before you do. On a Neo4jReplicaDatabase the usual cause is a broken differential chain (a bucket lifecycle rule expiring an old backup will do it), which requires recreating the replica.",
	},
	neo4jv1beta1.PhaseError: {
		meaning: "reconciliation hit an error it may recover from on a later pass.",
		action:  "Check status.message and the operator log. If it persists across several reconciles, treat it as Failed.",
	},
	neo4jv1beta1.PhaseInvalid: {
		meaning: "the spec was rejected by the operator's own validation, so nothing was created.",
		action:  "status.message names the field. `kubectl neo4j validate -f <file>` reports the same thing before you apply, and reports every error at once.",
	},
	neo4jv1beta1.PhaseDegraded: {
		meaning: "the resource is serving but not at full health — typically some servers or databases are unavailable.",
		action:  "Run `kubectl neo4j diagnose <Kind>/<name>`: the cause is usually one pod (OOMKilled at exit 137, unschedulable, or crash-looping) rather than the cluster as a whole.",
	},
	neo4jv1beta1.PhaseSuspended: {
		meaning: "reconciliation is deliberately paused for this resource.",
		action:  "Nothing will change until it is resumed. Expected during a maintenance window; unexpected otherwise.",
	},
	neo4jv1beta1.PhasePending: {
		meaning: "the resource is waiting on something that does not exist yet — a Secret, a referenced cluster, a database.",
		action:  "This is 'not yet', not 'wrong'. status.message names what it is waiting for; create that and it proceeds on its own. On a Neo4jReplicaDatabase it also means the replica has not been created in Neo4j yet: check the downstream is Ready and at least 2026.08, which CCDR replicas require.",
	},
	neo4jv1beta1.PhaseValidating: {
		meaning: "the operator is checking the spec before acting on it.",
		action:  "Wait. It moves to Invalid or onward within a reconcile.",
	},
	neo4jv1beta1.PhaseCreating: {
		meaning: "the underlying object is being created in Neo4j or in Kubernetes.",
		action:  "Wait. If it does not advance, `kubectl neo4j diagnose` reports the Kubernetes-level cause.",
	},
	neo4jv1beta1.PhaseForming: {
		meaning: "the servers are up and discovering each other, but the cluster has not formed a quorum yet.",
		action:  "Normal for the first two to three minutes. If it persists, check discovery: every server must resolve the others on port 6000, and SHOW SERVERS should list them all.",
	},
	neo4jv1beta1.PhaseInstalling: {
		meaning: "the plugin is being placed on the servers, which usually restarts pods.",
		action:  "Wait for the rollout. The plugin is not usable until every server has restarted.",
	},
	neo4jv1beta1.PhaseRunning: {
		meaning: "the operation is in progress.",
		action:  "Wait. For a backup or restore, the Job carries the detail: kubectl logs job/<name>.",
	},
	neo4jv1beta1.PhaseWaiting: {
		meaning: "the operator is waiting for the deployment it targets to become usable.",
		action:  "Explain the TARGET rather than this resource — it is the one that is not ready.",
	},
	neo4jv1beta1.PhaseUpgrading: {
		meaning: "servers are being rolled to a new image, one at a time.",
		action:  "Wait, and do not scale or change topology mid-upgrade. Progress shows as pods restarting in ordinal order.",
	},
	neo4jv1beta1.PhaseExpanding: {
		meaning: "the volumes are being grown.",
		action:  "Wait. This needs a StorageClass with allowVolumeExpansion: true — `kubectl neo4j preflight` checks that before you ask for it.",
	},
	neo4jv1beta1.PhaseUnknown: {
		meaning: "the operator could not determine the state.",
		action:  "Usually a connectivity problem between the operator and Neo4j rather than a broken deployment. Check status.diagnostics.collectionError and the operator log.",
	},
	neo4jv1beta1.ReplicaPhaseSeeding: {
		meaning: "the replica is loading its initial copy of the upstream database.",
		action:  "Wait. For backup-mode this is bounded by the size of the seed backup.",
	},
	neo4jv1beta1.ReplicaPhaseReplicating: {
		meaning: "the replica is online and following the upstream. This is the healthy steady state.",
		action:  "Nothing to do.",
	},
	neo4jv1beta1.ReplicaPhasePromoted: {
		meaning: "the replica was promoted to a standard read-write database. This is terminal and irreversible.",
		action:  "The replica CR is now inert and will never be reconciled again. Adopt the database with a Neo4jDatabase CR to manage it declaratively; deleting the replica CR will NOT drop it.",
	},
	neo4jv1beta1.PromotionPhasePromoting: {
		meaning: "the promotion procedure is running.",
		action:  "Wait. Promotion is one-way and cannot be re-attached to the upstream afterwards.",
	},
}

func runExplain(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := namespaceFlag(fs, "Namespace of the resource")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Explain what a status condition or phase means, and what to do about it.

Usage:
  kubectl neo4j explain <Kind>/<name> [-n <namespace>]   # explain a live resource
  kubectl neo4j explain <Condition|Phase>                # explain a term
  kubectl neo4j explain --list                           # everything it knows

Examples:
  kubectl neo4j explain Neo4jEnterpriseCluster/prod
  kubectl neo4j explain ServersHealthy

Flags:
`)
		fs.PrintDefaults()
	}
	list := fs.Bool("list", false, "List every condition and phase this command can explain")
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}

	if *list {
		renderExplainList(stdout)
		return exitOK
	}

	arg := fs.Arg(0)
	if arg == "" {
		fs.Usage()
		return exitUsage
	}

	// A slash means "go look at this live resource"; anything else is a term.
	if strings.Contains(arg, "/") {
		c, err := newClusterClient(*kubeconfig, *kubeContext)
		if err != nil {
			fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
			return exitUsage
		}
		ns := *namespace
		if ns == "" {
			ns = currentNamespace(*kubeconfig, *kubeContext)
		}
		if err := explainResource(context.Background(), c, ns, arg, stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return exitUsage
		}
		return exitOK
	}

	if !explainTerm(arg, stdout) {
		fmt.Fprintf(stderr, "no explanation for %q.\n", arg)
		fmt.Fprintln(stderr, "Try `kubectl neo4j explain --list`, or pass <Kind>/<name> to explain a live resource.")
		return exitUsage
	}
	return exitOK
}

// explainTerm looks up a condition type or phase by name, case-insensitively.
func explainTerm(term string, stdout *os.File) bool {
	for name, g := range conditionGuidance {
		if strings.EqualFold(name, term) {
			fmt.Fprintf(stdout, "%s (condition)\n  %s\n  → %s\n", name, g.meaning, g.action)
			return true
		}
	}
	for name, g := range phaseGuidance {
		if strings.EqualFold(name, term) {
			fmt.Fprintf(stdout, "%s (phase)\n  %s\n  → %s\n", name, g.meaning, g.action)
			return true
		}
	}
	return false
}

func explainResource(ctx context.Context, c client.Client, namespace, ref string, stdout *os.File) error {
	parts := strings.SplitN(ref, "/", 2)
	kind, name := parts[0], parts[1]

	var gvk *schema.GroupVersionKind
	for _, k := range registeredNeo4jKinds(c) {
		if strings.EqualFold(k.Kind, kind) {
			found := k
			gvk = &found
			break
		}
	}
	if gvk == nil {
		return fmt.Errorf("%q is not a Neo4j resource kind", kind)
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(*gvk)
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		return fmt.Errorf("could not read %s/%s in namespace %q: %w", gvk.Kind, name, namespace, err)
	}

	fmt.Fprintf(stdout, "%s/%s in namespace %s\n", gvk.Kind, name, namespace)

	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	if phase != "" {
		fmt.Fprintf(stdout, "\nphase: %s\n", phase)
		if g, ok := phaseGuidance[phase]; ok {
			fmt.Fprintf(stdout, "  %s\n  → %s\n", g.meaning, g.action)
		} else {
			// Explaining an unknown phase by guessing would be worse than
			// admitting the gap: this binary may simply predate it.
			fmt.Fprintf(stdout, "  (no guidance for this phase — it may be newer than this CLI, which carries %s rules)\n", version)
		}
	}

	conds, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found || len(conds) == 0 {
		fmt.Fprintln(stdout, "\nno conditions reported yet.")
		return nil
	}

	fmt.Fprintln(stdout)
	for _, raw := range conds {
		cond, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		ctype, _ := cond["type"].(string)
		cstatus, _ := cond["status"].(string)
		cmsg, _ := cond["message"].(string)

		mark := "✓"
		if cstatus != "True" {
			mark = "✗"
		}
		fmt.Fprintf(stdout, "%s %s = %s\n", mark, ctype, cstatus)
		if cmsg != "" {
			fmt.Fprintf(stdout, "    %s\n", cmsg)
		}
		if g, ok := conditionGuidance[ctype]; ok {
			fmt.Fprintf(stdout, "    %s\n    → %s\n", g.meaning, g.action)
		}
	}
	return nil
}

func renderExplainList(stdout *os.File) {
	fmt.Fprintln(stdout, "Conditions:")
	names := make([]string, 0, len(conditionGuidance))
	for n := range conditionGuidance {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(stdout, "  %s\n", n)
	}

	fmt.Fprintln(stdout, "\nPhases:")
	names = names[:0]
	for n := range phaseGuidance {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(stdout, "  %s\n", n)
	}
	fmt.Fprintf(stdout, "\nExplanations describe operator %s. A newer deployment may report\n", version)
	fmt.Fprintln(stdout, "conditions or phases this CLI does not know about.")
}
