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
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Exit codes are part of this command's contract with CI, so they are named
// rather than written as bare integers at the return sites.
const (
	exitOK      = 0 // no errors (warnings alone do not fail unless --strict)
	exitInvalid = 1 // at least one validation error, or a warning under --strict
	exitUsage   = 2 // bad flags, unreadable input, undecodable YAML
)

// validatorFn runs one kind's validator. The client is nil unless the user
// asked to connect (--connect / --context / --kubeconfig).
// validatorFn runs one kind's validator. The client is nil unless the user
// asked to connect. The third return is PENDING: transient dependency gaps
// (a password Secret not applied yet) that the operator routes to phase
// Pending rather than failing on. They are neither errors nor warnings, and
// dropping them would hide the difference between "wrong" and "not yet".
type validatorFn func(doc []byte, c client.Client) (errs field.ErrorList, warnings, pending []string, err error)

// kindValidator pairs a validator with whether it can run without a cluster.
type kindValidator struct {
	fn validatorFn
	// needsClient marks validators that resolve cross-references (a clusterRef,
	// a Secret, a role) through the API server. Run offline they would report
	// "not found" for things that merely are not reachable — a false error,
	// which is worse than not checking — so they are skipped unless connected.
	needsClient bool
	// connectedChecks names what a kind validated offline still checks only
	// with a connection, because the rule compares the manifest with the object
	// already in the cluster. Empty when everything runs offline.
	connectedChecks string
}

// validators is the full set of kinds this operator has a Go validator for.
//
// It is NOT every CRD. Of the operator's 27 kinds only these 13 have
// operator-side validation at all; the rest (the Aura suite, Neo4jRestore,
// Neo4jReplicaPromotion) are governed by their CRD schema alone, which
// `kubectl apply --dry-run=server` already enforces. Saying so precisely
// matters: an earlier version of this command told users those kinds needed a
// cluster connection, which implied --context would validate them. It will not.
//
// needsClient:false is a claim that the validator tolerates a nil client,
// established per kind and pinned by TestOfflineValidatorsAreNilClientSafe:
//   - Plugin — constructor takes no client; reading its target's image tag,
//     its one client call, is skipped on nil.
//   - Backup — constructor takes no client; resolving spec.shardedDatabase,
//     its one client call, is skipped on nil.
//   - DatabaseAlias, ReplicaDatabase — accept a client and never dereference it.
//   - CompositeDatabase — its one client call (the CalVer gate on
//     defaultCypherLanguage) returns early on nil.
//   - EnterpriseCluster, EnterpriseStandalone — their client calls (the admin
//     Secret, the live deployment's status) are skipped on nil.
var validators = map[string]kindValidator{
	// The same entry point the operator runs on every pass, after the same
	// defaults (the CRD's, which every kind gets in validateDoc, then the
	// operator's own ApplyDefaults), so a manifest is held to the same rules
	// here as when it is applied. The defaults go on the decoded copy that is
	// validated; nothing prints that copy, and ApplyDefaults adds no field an
	// error can then be reported on (operations.md id 144).
	"Neo4jEnterpriseCluster": {
		connectedChecks: "a Neo4jEnterpriseCluster's spec.image.tag against the version the cluster already runs, and its spec.storage.className against its volumes",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jEnterpriseCluster
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			ctx := context.Background()
			notChecked := withLiveClusterStatus(ctx, c, &obj)
			v := validation.NewClusterValidator(c)
			v.ApplyDefaults(ctx, &obj)
			out := v.ValidateWithWarnings(ctx, &obj)
			warnings := append(out.Warnings, out.NoEffectWarnings...)
			warnings = append(warnings, storageClassFindings(ctx, c, obj.Spec.Storage.ClassName, obj.Namespace, obj.Name,
				controller.ClusterDataVolumeClasses)...)
			return out.Errors, append(warnings, notChecked...), nil, nil
		},
	},
	// The operator applies no defaults of its own to a standalone before it
	// validates one, so neither does this: the CRD's, which the API server
	// stores, are in the document already (validateDoc).
	"Neo4jEnterpriseStandalone": {
		connectedChecks: "a Neo4jEnterpriseStandalone's spec.image.tag against the version it already runs, and its spec.storage.className against its volume",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jEnterpriseStandalone
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			ctx := context.Background()
			notChecked := withLiveStandaloneStatus(ctx, c, &obj)
			v := validation.NewStandaloneValidator()
			warnings := append(v.NoEffectWarnings(&obj), storageClassFindings(ctx, c, obj.Spec.Storage.ClassName,
				obj.Namespace, obj.Name, controller.StandaloneDataVolumeClasses)...)
			return v.Validate(ctx, c, &obj), append(warnings, notChecked...), nil, nil
		},
	},
	// Connected, a backup's spec.shardedDatabase is resolved as the backup
	// controller resolves it — the Neo4jShardedDatabase by its CR name, on the
	// backup's instanceRef — through the same function. The operator fails a
	// backup whose reference does not resolve; offline it passed here.
	"Neo4jBackup": {
		connectedChecks: "a Neo4jBackup's spec.instanceRef against the deployments that exist, and its spec.shardedDatabase against the Neo4jShardedDatabase it names",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jBackup
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			v := validation.NewBackupValidator()
			errs, warnings := v.Validate(&obj), v.NoEffectWarnings(&obj)
			if c == nil {
				return errs, warnings, nil, nil
			}
			// A target not created yet: the operator waits for it (phase
			// Waiting), as every kind waits for its deployment (id 182).
			pending, unread := deploymentRefFindings(c, field.NewPath("spec", "instanceRef"), obj.Namespace, obj.Spec.InstanceRef)
			warnings = append(warnings, unread...)
			_, refusal, err := validation.ResolveBackupShardedDatabase(context.Background(), c, &obj)
			switch {
			case err != nil:
				warnings = append(warnings, fmt.Sprintf(
					"could not read Neo4jShardedDatabase %s/%s, so spec.shardedDatabase was not checked: %v",
					obj.Namespace, obj.Spec.ShardedDatabase, err))
			case refusal != nil:
				errs = append(errs, refusal)
			}
			return errs, warnings, pending, nil
		},
	},
	// Connected, a Managed plugin's NEO4J_PLUGINS names are held to the image
	// its target runs, the target resolved as the plugin controller resolves it
	// (validation.PluginTargetImageTag): the operator refuses a name the image's
	// entrypoint does not install (operations.md id 170). Offline the image is
	// unknown, so only a name no supported release installs is reported, as a
	// warning.
	"Neo4jPlugin": {
		connectedChecks: "a Managed Neo4jPlugin's NEO4J_PLUGINS names against the image its target runs",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jPlugin
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			out := validation.NewPluginValidator().Validate(&obj)
			errs, warnings := out.Errors, out.Warnings
			var pending []string
			if c == nil {
				return errs, append(warnings, validation.ManagedPluginNameWarnings(&obj)...), nil, nil
			}
			tag, _, found, err := validation.PluginTargetImageTag(context.Background(), c, &obj)
			switch {
			case err != nil:
				warnings = append(warnings, fmt.Sprintf(
					"could not read the target %s/%s, so spec.name was checked against every supported Neo4j image, not the one it runs: %v",
					obj.Namespace, obj.Spec.ClusterRef, err))
				warnings = append(warnings, validation.ManagedPluginNameWarnings(&obj)...)
			case !found:
				// A target not applied yet is a wait for the operator, as for
				// every kind with a clusterRef (id 182).
				pending = append(pending, validation.DeploymentPending(clusterRefPath, obj.Spec.ClusterRef, obj.Namespace))
				warnings = append(warnings, fmt.Sprintf(
					"no Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone %s/%s, so spec.name was checked against every supported Neo4j image, not the one it will run",
					obj.Namespace, obj.Spec.ClusterRef))
				warnings = append(warnings, validation.ManagedPluginNameWarnings(&obj)...)
			default:
				// The operator's own refusal, word for word.
				errs = append(errs, validation.ManagedPluginImageErrors(&obj, tag)...)
				warnings = append(warnings, validation.ManagedPluginBuiltInWarnings(&obj, tag)...)
			}
			return errs, warnings, pending, nil
		},
	},
	// The alias, composite and replica validators do not resolve their
	// clusterRef; connected, it is resolved here as their reconcilers resolve
	// it, and a deployment that does not exist yet is pending (id 182).
	"Neo4jDatabaseAlias": {
		connectedChecks: "a Neo4jDatabaseAlias's spec.clusterRef against the deployments that exist",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jDatabaseAlias
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			out := validation.NewAliasValidator(c).Validate(context.Background(), &obj)
			pending, unread := deploymentRefFindings(c, clusterRefPath, obj.Namespace, obj.Spec.ClusterRef)
			return out.Errors, append(out.Warnings, unread...), pending, nil
		},
	},
	"Neo4jCompositeDatabase": {
		connectedChecks: "a Neo4jCompositeDatabase's spec.clusterRef against the deployments that exist",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jCompositeDatabase
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			out := validation.NewCompositeDatabaseValidator(c).Validate(context.Background(), &obj)
			pending, unread := deploymentRefFindings(c, clusterRefPath, obj.Namespace, obj.Spec.ClusterRef)
			return out.Errors, append(out.Warnings, unread...), pending, nil
		},
	},
	"Neo4jReplicaDatabase": {
		connectedChecks: "a Neo4jReplicaDatabase's spec.clusterRef against the deployments that exist",
		fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
			var obj neo4jv1beta1.Neo4jReplicaDatabase
			if err := yaml.Unmarshal(doc, &obj); err != nil {
				return nil, nil, nil, err
			}
			out := validation.NewReplicaValidator(c).Validate(context.Background(), &obj)
			pending, unread := deploymentRefFindings(c, clusterRefPath, obj.Namespace, obj.Spec.ClusterRef)
			return out.Errors, append(out.Warnings, unread...), pending, nil
		},
	},

	// --- require a cluster connection ---------------------------------
	"Neo4jDatabase": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jDatabase
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewDatabaseValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, withTargetPending(nil, out.TargetPending), nil
	}},
	"Neo4jUser": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jUser
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewUserValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, withTargetPending(out.Pending, out.TargetPending), nil
	}},
	"Neo4jRole": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jRole
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewRoleValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, withTargetPending(nil, out.TargetPending), nil
	}},
	"Neo4jRoleBinding": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jRoleBinding
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewRoleBindingValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, withTargetPending(nil, out.TargetPending), nil
	}},
	"Neo4jAuthRule": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jAuthRule
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewAuthRuleValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, withTargetPending(nil, out.TargetPending), nil
	}},
	// The reconciler's entry point (ValidateShardedDatabase) folds everything
	// into one error; Validate is the same rules with each finding apart, so
	// every error keeps its own field path. A cluster that does not exist yet
	// is a wait, as it is for the operator; a cluster that cannot be read
	// leaves the rules that need it unchecked, which is said.
	"Neo4jShardedDatabase": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jShardedDatabase
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewShardedDatabaseValidator(c).Validate(context.Background(), &obj)
		warnings := out.Warnings
		if out.ClusterReadErr != nil {
			warnings = append(warnings, fmt.Sprintf(
				"could not read Neo4jEnterpriseCluster %s/%s, so the rules that need the cluster were not checked: %v",
				obj.Namespace, obj.Spec.ClusterRef, out.ClusterReadErr))
		}
		return out.Errors, warnings, out.Pending, nil
	}},
}

// storageClassFindings is, connected, the operator's StorageClass advisory for
// a deployment that exists: spec.storage.className naming a class its data
// volumes do not use, which an apply cannot change (a volume keeps its class;
// operations.md id 102). The volumes are found as the operator's storage pass
// finds them (volumes), so the warning is the one the operator will raise. A
// read that fails says the check was not made; offline nothing is said.
func storageClassFindings(ctx context.Context, c client.Client, className, namespace, name string,
	volumes func(context.Context, client.Client, string, string) (map[string]string, error)) []string {
	if c == nil || className == "" {
		return nil
	}
	classes, err := volumes(ctx, c, namespace, name)
	if err != nil {
		return []string{fmt.Sprintf(
			"could not list the PersistentVolumeClaims of %s/%s, so spec.storage.className was not checked against its volumes: %v",
			namespace, name, err)}
	}
	if w := validation.StorageClassChangeWarning(className, classes); w != "" {
		return []string{w}
	}
	return nil
}

// clusterRefPath is spec.clusterRef, the reference most kinds resolve.
var clusterRefPath = field.NewPath("spec", "clusterRef")

// withTargetPending adds a validator's missing-deployment finding to pending.
func withTargetPending(pending []string, target string) []string {
	if target == "" {
		return pending
	}
	return append(pending, target)
}

// deploymentRefFindings resolves a reference to a cluster or standalone as the
// reconcilers resolve it, for the kinds whose validator does not: one that
// does not exist yet is pending, never an error, since the operator waits for
// it (operations.md id 182), and one that cannot be read is a warning that it
// was not checked. Offline it says nothing.
func deploymentRefFindings(c client.Client, path *field.Path, namespace, ref string) (pending, warnings []string) {
	if c == nil || ref == "" {
		return nil, nil
	}
	cluster, standalone, err := validation.LookupDeployment(context.Background(), c, namespace, ref)
	switch {
	case err != nil:
		return nil, []string{validation.DeploymentUnreadable(path, ref, err)}
	case cluster == nil && standalone == nil:
		return []string{validation.DeploymentPending(path, ref, namespace)}, nil
	}
	return nil, nil
}

// withLiveClusterStatus gives obj the status of the cluster already in the API
// server under its name: the status the operator reads when it validates the
// applied spec, which is how it knows the version the servers run. kubectl
// apply sends no status for a kind with a status subresource, so whatever the
// manifest says there is dropped first. Offline, and for a cluster not created
// yet, there is no status, and the rules that need one have nothing to compare.
// A read that fails for another reason is returned as a warning: the user asked
// for those rules, and they did not run.
func withLiveClusterStatus(ctx context.Context, c client.Client, obj *neo4jv1beta1.Neo4jEnterpriseCluster) []string {
	obj.Status = neo4jv1beta1.Neo4jEnterpriseClusterStatus{}
	if c == nil {
		return nil
	}
	live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	err := c.Get(ctx, client.ObjectKeyFromObject(obj), live)
	switch {
	case err == nil:
		obj.Status = live.Status
	case apierrors.IsNotFound(err):
	default:
		return []string{fmt.Sprintf(
			"could not read Neo4jEnterpriseCluster %s/%s, so spec.image.tag was not checked against the version it runs: %v",
			obj.Namespace, obj.Name, err)}
	}
	return nil
}

// withLiveStandaloneStatus is withLiveClusterStatus for a standalone: the
// version it runs is read from the status of the standalone already in the API
// server under its name, which is what the operator reads.
func withLiveStandaloneStatus(ctx context.Context, c client.Client, obj *neo4jv1beta1.Neo4jEnterpriseStandalone) []string {
	obj.Status = neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{}
	if c == nil {
		return nil
	}
	live := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	err := c.Get(ctx, client.ObjectKeyFromObject(obj), live)
	switch {
	case err == nil:
		obj.Status = live.Status
	case apierrors.IsNotFound(err):
	default:
		return []string{fmt.Sprintf(
			"could not read Neo4jEnterpriseStandalone %s/%s, so spec.image.tag was not checked against the version it runs: %v",
			obj.Namespace, obj.Name, err)}
	}
	return nil
}

// newObjectForKind returns an empty instance of a Neo4j CRD kind, or nil when
// the kind is not one of ours.
//
// Derived from the API scheme rather than a hand-written map. Every root type
// registers itself with SchemeBuilder, so a new CRD is typo-checked the day it
// is added — a list maintained by hand would silently omit it, which is the
// exact drift this check exists to catch.
//
// This deliberately covers ALL kinds, not just the 13 with operator-side
// validators. The other 14 (the Aura suite, Neo4jRestore,
// Neo4jReplicaPromotion) are governed by their CRD schema alone, and "no
// validator" is not "no spelling". A Neo4jReplicaPromotion with
// spec.replicaDatabaseRef instead of spec.replicaRef passed validate and was
// then refused by the API server, during the very journey that added this
// check.
func newObjectForKind(kind string) any {
	obj, err := neo4jScheme.New(neo4jv1beta1.GroupVersion.WithKind(kind))
	if err != nil {
		return nil
	}
	return obj
}

// neo4jScheme holds only this operator's types; it never needs a cluster.
var neo4jScheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(neo4jv1beta1.AddToScheme(s))
	return s
}()

// finding is one rendered line of output, decoupled from field.Error so that
// errors and warnings can be sorted and printed uniformly.
type finding struct {
	severity string // "error" | "warning"
	path     string
	detail   string
}

// docResult is the outcome for a single YAML document.
type docResult struct {
	source   string
	kind     string
	name     string
	skipped  string // non-empty = reason this document was not validated
	findings []finding
	// notCheckedOffline is the kind's connectedChecks when it was validated
	// without a connection: rules that did not run, which the summary names.
	notCheckedOffline string
}

func (d docResult) errorCount() int { return d.countOf("error") }

func (d docResult) warningCount() int { return d.countOf("warning") }

// pendingCount are transient dependency gaps — not yet satisfiable, not wrong.
// They never affect the exit code, including under --strict: failing CI because
// a Secret has not been applied yet would punish correct manifests.
func (d docResult) pendingCount() int { return d.countOf("pending") }

func (d docResult) countOf(sev string) int {
	n := 0
	for _, f := range d.findings {
		if f.severity == sev {
			n++
		}
	}
	return n
}

func runValidate(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var files multiFlag
	fs.Var(&files, "f", "Manifest file, directory, or - for stdin. Repeatable.")
	strict := fs.Bool("strict", false, "Treat warnings as errors (exit non-zero on warnings)")
	quiet := fs.Bool("quiet", false, "Print findings only; suppress the summary and banner")
	connect := fs.Bool("connect", false, "Connect to the cluster in the current kubeconfig context to run cross-reference checks")
	kubeContext := fs.String("context", "", "Kubeconfig context to use (implies --connect)")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file (implies --connect)")
	namespace := namespaceFlag(fs, "Namespace for objects whose manifest omits one")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Validate Neo4j CR manifests against the operator's own validators.

Usage:
  kubectl neo4j validate -f <file|dir|-> [-f ...] [--connect] [--strict] [--quiet]

Runs OFFLINE by default: no cluster connection is made, and kinds whose
validators resolve cross-references (clusterRef, Secrets, roles) are reported
as skipped. Pass --connect (or --context/--kubeconfig) to check those too, and
to check a cluster's or a standalone's spec.image.tag against the version it
already runs (a downgrade is refused).

A validator reports every error it finds, not just the first, so one run lists
them all. The exception is Neo4jShardedDatabase: when its clusterRef cannot be
read, that is the error reported, and its other rules wait for the cluster. A
clean run covers the rules that ran: offline, the summary names the ones that
compare a manifest with what already runs.

Not every kind has an operator-side validator. Those that do not are governed
by their CRD schema alone — use kubectl apply --dry-run=server for that.

Exit codes: 0 clean, 1 validation errors (or warnings with --strict), 2 usage.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if len(files) == 0 {
		fs.Usage()
		return exitUsage
	}

	// Connecting is opt-in. Defaulting to the current context would make a
	// command that is documented as offline fail in CI, where a kubeconfig
	// usually does not exist — so the user has to ask, either directly or by
	// naming a context/kubeconfig.
	var c client.Client
	wantCluster := *connect || *kubeContext != "" || *kubeconfig != ""
	if wantCluster {
		var err error
		c, err = newClusterClient(*kubeconfig, *kubeContext)
		if err != nil {
			fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
			return exitUsage
		}
		// A manifest without metadata.namespace lands, on apply, in the
		// context's namespace (else "default"); its cross-references are looked
		// up there too. Left empty, every lookup failed — read as "target not
		// there yet" — so --connect without -n warned about objects that
		// existed and passed checks it never made.
		*namespace = connectedNamespace(*namespace, *kubeconfig, *kubeContext)
	}

	inputs, err := expandInputs(files)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	var results []docResult
	for _, in := range inputs {
		rs, err := validateSource(in, c, *namespace)
		if err != nil {
			fmt.Fprintf(stderr, "error: %s: %v\n", in, err)
			return exitUsage
		}
		results = append(results, rs...)
	}

	if wantCluster && !*quiet {
		warnOnVersionSkew(c, stdout)
	}
	return report(results, stdout, *strict, *quiet, wantCluster)
}

// newClusterClient builds a direct (uncached) client from the user's kubeconfig.
// Uncached is deliberate: a CLI makes a handful of reads and exits, so an
// informer cache would cost more than it saves and would need list/watch
// permissions the user may not have.
func newClusterClient(kubeconfigPath, contextName string) (client.Client, error) {
	cfg, err := newClusterConfig(kubeconfigPath, contextName)
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

// newClusterConfig resolves the kubeconfig the same way kubectl does. Split out
// from newClusterClient because support-bundle needs the raw config too: pod
// logs are served by a subresource that the controller-runtime client does not
// expose, so it builds a client-go clientset from the same config.
func newClusterConfig(kubeconfigPath, contextName string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: contextName},
	).ClientConfig()
}

// warnOnVersionSkew compares this binary's version with the operator running in
// the cluster. Q4 of the design: a CLI carries the validation rules of the
// release it was built from, so a mismatch means rules silently added or
// removed relative to what will actually reconcile the manifest.
//
// Advisory only, and silent when it cannot tell: the operator may be in a
// namespace the user cannot list, which is not an error worth failing over.
func warnOnVersionSkew(c client.Client, stdout *os.File) {
	if c == nil || version == "dev" {
		return
	}
	ctx := context.Background()
	var deployments appsv1.DeploymentList
	if err := c.List(ctx, &deployments,
		client.MatchingLabels{"app.kubernetes.io/name": "neo4j-operator"}); err != nil {
		return
	}
	d, note := runningOperator(ctx, c, deployments.Items, time.Now())
	if note != "" {
		fmt.Fprintf(stdout, "version skew not checked: %s\n", note)
		return
	}
	if d == nil {
		return
	}
	deployed := deployedOperatorVersion(*d)
	if deployed == "" {
		return
	}
	if strings.TrimPrefix(deployed, "v") != strings.TrimPrefix(version, "v") {
		fmt.Fprintf(stdout,
			"⚠ version skew: this CLI carries %s rules, but the operator in %s/%s is %s.\n"+
				"  Rules added or removed between those releases are checked incorrectly here.\n",
			version, d.Namespace, d.Name, deployed)
	}
}

// runningOperator picks, from the operator Deployments it is given, the one
// that is running, by two signals: a Deployment with no available replica
// (status.availableReplicas) runs nothing, and of several that do, the one
// whose pod holds a current coordination.k8s.io Lease in its namespace — the
// operator's leader election, whose holderIdentity starts with the pod name —
// is the one reconciling. The first Deployment the list returned used to be
// taken, which once compared the CLI with a v1.22.0 operator scaled to 0 in
// another namespace (#573). Several running operators that report one version
// need no choice. Otherwise it cannot tell, and says why rather than guess:
// note is set and the Deployment is nil. No Deployment at all is nil and no
// note: the operator may be in a namespace this user cannot list.
func runningOperator(ctx context.Context, c client.Client, deployments []appsv1.Deployment, now time.Time) (*appsv1.Deployment, string) {
	if len(deployments) == 0 {
		return nil, ""
	}
	var running []*appsv1.Deployment
	for i := range deployments {
		if deployments[i].Status.AvailableReplicas > 0 {
			running = append(running, &deployments[i])
		}
	}
	switch len(running) {
	case 0:
		return nil, fmt.Sprintf("none of the %d operator Deployment(s) (%s) has an available replica",
			len(deployments), deploymentNames(deployments))
	case 1:
		return running[0], ""
	}
	if sameVersion(running) {
		return running[0], ""
	}
	var leaders []*appsv1.Deployment
	for _, d := range running {
		if holdsLeaderLease(ctx, c, d, now) {
			leaders = append(leaders, d)
		}
	}
	if len(leaders) == 1 {
		return leaders[0], ""
	}
	names := make([]appsv1.Deployment, 0, len(running))
	for _, d := range running {
		names = append(names, *d)
	}
	return nil, fmt.Sprintf("%d operators are running (%s) on different versions, and no single one holds its leader lease",
		len(running), deploymentNames(names))
}

// holdsLeaderLease reports whether one of d's pods holds a Lease in d's
// namespace that has not expired. A Lease this user cannot read counts as
// not held.
func holdsLeaderLease(ctx context.Context, c client.Client, d *appsv1.Deployment, now time.Time) bool {
	var leases coordinationv1.LeaseList
	if err := c.List(ctx, &leases, client.InNamespace(d.Namespace)); err != nil {
		return false
	}
	for _, l := range leases.Items {
		s := l.Spec
		if s.HolderIdentity == nil || !strings.HasPrefix(*s.HolderIdentity, d.Name+"-") {
			continue
		}
		if s.RenewTime == nil || s.LeaseDurationSeconds == nil {
			continue
		}
		if now.Before(s.RenewTime.Add(time.Duration(*s.LeaseDurationSeconds) * time.Second)) {
			return true
		}
	}
	return false
}

func sameVersion(ds []*appsv1.Deployment) bool {
	first := deployedOperatorVersion(*ds[0])
	for _, d := range ds[1:] {
		if deployedOperatorVersion(*d) != first {
			return false
		}
	}
	return first != ""
}

func deploymentNames(ds []appsv1.Deployment) string {
	names := make([]string, 0, len(ds))
	for _, d := range ds {
		v := deployedOperatorVersion(d)
		if v == "" {
			v = "version unknown"
		}
		names = append(names, fmt.Sprintf("%s/%s %s", d.Namespace, d.Name, v))
	}
	return strings.Join(names, ", ")
}

// deployedOperatorVersion reports which operator release a Deployment runs, or
// "" when it cannot tell.
//
// OPERATOR_VERSION is the intended source and is stamped by the Helm chart, the
// CSV, and the release workflow's manifest. It is NOT reliable on its own: the
// kustomize base carries the literal placeholder "latest"
// (config/manager/manager.yaml), which survives into any install that layers
// its own kustomization on config/default — a documented path, where users pin
// the image tag and have no reason to suspect a separate env var also needs
// patching. Such an install would silently never get a skew warning.
//
// So fall back to the image tag, which is the release actually running and the
// one thing such an install always sets. Tags that name no release ("latest",
// "dev", a digest pin) are rejected rather than reported as a version.
func deployedOperatorVersion(d appsv1.Deployment) string {
	var imageTag string
	for _, ctr := range d.Spec.Template.Spec.Containers {
		for _, env := range ctr.Env {
			if env.Name == "OPERATOR_VERSION" && isReleaseVersion(env.Value) {
				return env.Value
			}
		}
		if imageTag == "" {
			imageTag = tagFromImage(ctr.Image)
		}
	}
	if isReleaseVersion(imageTag) {
		return imageTag
	}
	return ""
}

// tagFromImage pulls the tag off an image reference, tolerating a registry port
// (the colon in "registry:5000/img" is not a tag separator) and a digest after
// the tag ("img:v1.22.0@sha256:…" runs v1.22.0), and returning "" for an image
// pinned by digest alone, which names no release.
func tagFromImage(image string) string {
	if at := strings.Index(image, "@"); at >= 0 {
		image = image[:at]
	}
	i := strings.LastIndex(image, ":")
	if i < 0 || strings.Contains(image[i+1:], "/") {
		return ""
	}
	return image[i+1:]
}

// isReleaseVersion rejects the stand-ins that appear when nothing stamped a
// real version. Mirrors isPlaceholderVersion in internal/metrics.
func isReleaseVersion(v string) bool {
	switch v {
	case "", "dev", "latest", "(devel)", "main", "edge":
		return false
	}
	return true
}

// validateSource reads one input (a path, or "-" for stdin) and validates every
// YAML document it contains.
// connectedNamespace is the namespace --connect resolves cross-references in
// for manifests that name none: -n when given, else the kubeconfig context's,
// else "default" — where kubectl apply would put them.
func connectedNamespace(flagValue, kubeconfig, kubeContext string) string {
	if flagValue != "" {
		return flagValue
	}
	return currentNamespace(kubeconfig, kubeContext)
}

func validateSource(source string, c client.Client, defaultNamespace string) ([]docResult, error) {
	var r io.Reader
	if source == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(source)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}

	var results []docResult
	// NewYAMLReader splits on "---" so a single file may hold many objects,
	// which is how people actually write manifests.
	reader := k8syaml.NewYAMLReader(bufio.NewReader(r))
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if isEmptyDoc(doc) {
			continue
		}
		res, err := validateDoc(doc, source, c, defaultNamespace)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

// isEmptyDoc reports whether a YAML document carries no content at all.
//
// A whitespace check is not sufficient: `---\n# a comment\n---` yields a
// document that is non-empty as bytes but decodes to nothing. Treating that as
// an object produces a spurious "no kind field" line for every commented
// separator in a hand-written manifest, which is exactly the kind of noise that
// makes people stop reading a linter's output.
func isEmptyDoc(doc []byte) bool {
	if len(strings.TrimSpace(string(doc))) == 0 {
		return true
	}
	var probe map[string]interface{}
	if err := yaml.Unmarshal(doc, &probe); err != nil {
		// Leave malformed YAML to validateDoc, which reports it properly.
		return false
	}
	return len(probe) == 0
}

// validateDoc decodes one document far enough to learn its Kind, then hands it
// to the operator's validator for that Kind.
func validateDoc(doc []byte, source string, c client.Client, defaultNamespace string) (docResult, error) {
	var meta struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata,omitempty"`
	}
	if err := yaml.Unmarshal(doc, &meta); err != nil {
		return docResult{}, fmt.Errorf("cannot parse YAML: %w", err)
	}

	res := docResult{source: source, kind: meta.Kind, name: meta.Name}

	switch {
	case meta.Kind == "":
		res.skipped = "no kind field — not a Kubernetes object"
		return res, nil
	case !strings.HasPrefix(meta.APIVersion, "neo4j.neo4j.com/"):
		res.skipped = fmt.Sprintf("not a Neo4j operator resource (apiVersion %q)", meta.APIVersion)
		return res, nil
	}

	// Unknown fields are checked BEFORE any skip decision: the check reads
	// only the document and the Go type, so a kind whose validator needs a
	// cluster has no reason to escape it. It used to sit after dispatch, which
	// meant `Neo4jShardedDatabase` and the five other cross-referencing kinds
	// were never typo-checked offline — and a bad spec.propertyShards sailed
	// through `validate` into a rejected apply during the v1.16.0 journey.
	//
	// A key the type does not have is a validation error, not a decode
	// failure: it gets a finding and exit 1 like every other error, rather
	// than aborting the file with exit 2 and hiding the rest of its findings.
	var unknownFindings []finding
	if obj := newObjectForKind(meta.Kind); obj != nil {
		for _, path := range unknownFieldPaths(doc, obj) {
			unknownFindings = append(unknownFindings, finding{"error", path,
				"Invalid value: unknown field: this kind has no such field. " +
					"Check the spelling against the API reference — the operator " +
					"ignores it and the API server rejects the manifest."})
		}
	}
	res.findings = append(res.findings, unknownFindings...)

	kv, ok := validators[meta.Kind]
	if !ok {
		// Precisely worded on purpose. These kinds have NO operator-side
		// validator, so no amount of connecting will check them — telling a
		// user to pass --context here would send them after something that
		// does not exist.
		res.skipped = fmt.Sprintf(
			"no operator-side validator for %s; its CRD schema still applies (kubectl apply --dry-run=server)", meta.Kind)
		return res, nil
	}
	if kv.needsClient && c == nil {
		// Both halves are true when a skipped kind has a typo: the
		// cross-reference rules did not run, and the unknown field did get
		// caught. The renderer shows the findings and then says what is still
		// unchecked, rather than picking one and hiding the other.
		res.skipped = fmt.Sprintf(
			"%s validation resolves cross-references; re-run with --connect to check it", meta.Kind)
		return res, nil
	}

	// A manifest may omit metadata.namespace and rely on the apply-time
	// default. Cross-reference lookups need one, so mirror kubectl: the
	// object's own namespace wins, else --namespace, else "default".
	if defaultNamespace != "" {
		doc = withDefaultNamespace(doc, meta.Namespace, defaultNamespace)
	}

	// The object the operator validates is the one the API server stored,
	// with the CRD's defaults in it; judge that one (validate_schema_defaults.go).
	doc = withSchemaDefaults(meta.Kind, meta.APIVersion, doc)

	errs, warns, pending, err := kv.fn(doc, c)
	if err != nil {
		return docResult{}, fmt.Errorf("cannot decode %s: %w", meta.Kind, err)
	}
	if c == nil {
		res.notCheckedOffline = kv.connectedChecks
	}
	for _, e := range errs {
		res.findings = append(res.findings, finding{"error", e.Field, e.ErrorBody()})
	}
	for _, w := range warns {
		res.findings = append(res.findings, finding{"warning", "", w})
	}
	for _, pnd := range pending {
		res.findings = append(res.findings, finding{"pending", "", pnd})
	}

	severityRank := map[string]int{"error": 0, "warning": 1, "pending": 2}
	sort.SliceStable(res.findings, func(i, j int) bool {
		ri, rj := severityRank[res.findings[i].severity], severityRank[res.findings[j].severity]
		if ri != rj {
			return ri < rj
		}
		return res.findings[i].path < res.findings[j].path
	})
	return res, nil
}

func report(results []docResult, stdout *os.File, strict, quiet, connected bool) int {
	totalErr, totalWarn, totalPending, validated, skipped := 0, 0, 0, 0, 0
	var notChecked []string

	for _, r := range results {
		// Skipped AND findings happens when a kind whose validator needs a
		// cluster carries an unknown field: the typo check is offline, so it
		// ran. Printing only "skipped" would hide a real error.
		if r.skipped != "" && len(r.findings) == 0 {
			skipped++
			if !quiet {
				fmt.Fprintf(stdout, "- %s: skipped — %s\n", describe(r), r.skipped)
			}
			continue
		}
		validated++
		if r.notCheckedOffline != "" && !slices.Contains(notChecked, r.notCheckedOffline) {
			notChecked = append(notChecked, r.notCheckedOffline)
		}
		totalErr += r.errorCount()
		totalWarn += r.warningCount()
		totalPending += r.pendingCount()

		if len(r.findings) == 0 {
			if !quiet {
				fmt.Fprintf(stdout, "✓ %s: ok\n", describe(r))
			}
			continue
		}
		fmt.Fprintf(stdout, "%s:\n", describe(r))
		for _, f := range r.findings {
			mark := "✗"
			switch f.severity {
			case "warning":
				mark = "⚠"
			case "pending":
				mark = "…"
			}
			if f.path != "" {
				fmt.Fprintf(stdout, "  %s %s: %s\n", mark, f.path, f.detail)
			} else {
				fmt.Fprintf(stdout, "  %s %s\n", mark, f.detail)
			}
		}
		// What was found is not all that was checked. Say what still was not,
		// so a reported typo is not mistaken for a clean bill of health.
		if r.skipped != "" && !quiet {
			fmt.Fprintf(stdout, "  - not fully checked — %s\n", r.skipped)
		}
	}

	if !quiet {
		summary := fmt.Sprintf("\n%d validated, %d skipped — %d error(s), %d warning(s)",
			validated, skipped, totalErr, totalWarn)
		if totalPending > 0 {
			summary += fmt.Sprintf(", %d pending", totalPending)
		}
		fmt.Fprintln(stdout, summary)
		if !connected && skipped > 0 {
			fmt.Fprintln(stdout, "run with --connect to also check kinds that resolve cross-references")
		}
		// Rules that compare a manifest with the object already in the
		// cluster cannot run without one; a clean offline run has not checked
		// them, and the operator will.
		for _, what := range notChecked {
			fmt.Fprintf(stdout, "run with --connect to also check %s\n", what)
		}
		// Offline output is only authoritative for the release it was built
		// from. Saying so is the cheap half of the version-skew answer.
		fmt.Fprintf(stdout, "validated against operator rules %s\n", version)
	}

	if totalErr > 0 || (strict && totalWarn > 0) {
		return exitInvalid
	}
	return exitOK
}

// withDefaultNamespace injects metadata.namespace into a document that omits
// one, so cross-reference lookups resolve against the namespace the user would
// actually apply into. Returns the document unchanged if it already has one.
func withDefaultNamespace(doc []byte, existing, fallback string) []byte {
	if existing != "" {
		return doc
	}
	var obj map[string]interface{}
	if err := yaml.Unmarshal(doc, &obj); err != nil {
		return doc
	}
	md, ok := obj["metadata"].(map[string]interface{})
	if !ok {
		return doc
	}
	md["namespace"] = fallback
	patched, err := yaml.Marshal(obj)
	if err != nil {
		return doc
	}
	return patched
}

func describe(r docResult) string {
	kind := r.kind
	if kind == "" {
		kind = "document"
	}
	if r.name == "" {
		return fmt.Sprintf("%s (%s)", kind, r.source)
	}
	return fmt.Sprintf("%s/%s (%s)", kind, r.name, r.source)
}

// expandInputs turns the -f values into a concrete file list, walking any
// directory one level for the YAML extensions people actually use.
func expandInputs(files []string) ([]string, error) {
	var out []string
	for _, f := range files {
		if f == "-" {
			out = append(out, f)
			continue
		}
		info, err := os.Stat(f)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			out = append(out, f)
			continue
		}
		entries, err := os.ReadDir(f)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			switch strings.ToLower(filepath.Ext(e.Name())) {
			case ".yaml", ".yml", ".json":
				out = append(out, filepath.Join(f, e.Name()))
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no manifest files found")
	}
	return out, nil
}

// multiFlag collects a repeatable string flag.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}
