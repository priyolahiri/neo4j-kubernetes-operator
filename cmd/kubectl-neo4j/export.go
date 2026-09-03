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

// export authors the downstream CR whose input lives in an upstream CR's
// status, on a DIFFERENT Kubernetes cluster.
//
// # Why this exists, and why it is narrow
//
// Several status fields on this operator's CRs exist purely to be pasted into
// another resource's spec — Neo4jBackup.status.replicationPullURI is declared
// with the comment that assembling it by hand "is the single most likely thing
// for a user to get wrong, so the operator publishes it instead".
//
// Within ONE Kubernetes cluster the operator has already closed that loop
// itself: Neo4jReplicaDatabase.spec.source.upstreamBackupRef resolves the pull
// URI live, and upstreamClusterRef resolves network addresses live. Rebuilding
// either here would be redundant, and worse than redundant — a generated
// literal goes stale the moment the upstream changes, where a ref does not.
//
// What neither ref can do is cross a cluster boundary. Both resolve through a
// Get against their own API server, and the CRD says so in as many words: for
// an upstream on a different Kubernetes cluster, "paste it from that CR's own
// status by hand". That paste is what this command replaces, and it is the
// whole of its scope.
//
// The manifest goes to stdout and nothing else does, so it can be redirected
// into a file or piped into `kubectl apply --context other-cluster` without
// stripping anything. Notes and warnings go to stderr.
//
// Every manifest is run through the operator's OWN ReplicaValidator before it
// is printed. A command that emitted something `validate` would then reject
// would be worse than no command.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

func runExport(args []string, stdout, stderr *os.File) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, exportUsage)
		return exitUsage
	}
	switch args[0] {
	case "replica-database":
		return runExportReplicaDatabase(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, exportUsage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "unknown export target %q\n\n%s", args[0], exportUsage)
		return exitUsage
	}
}

const exportUsage = `Author a downstream manifest from an upstream resource's status.

Usage:
  kubectl neo4j export replica-database <name> --from-backup <backup> [flags]
  kubectl neo4j export replica-database <name> --from-cluster <cluster> [flags]

For a downstream on a DIFFERENT Kubernetes cluster, where the operator's own
upstreamBackupRef / upstreamClusterRef cannot resolve. On the same cluster,
prefer those refs — they stay correct when the upstream changes.

Run "kubectl neo4j export replica-database -h" for flags.
`

func runExportReplicaDatabase(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("export replica-database", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fromBackup := fs.String("from-backup", "", "Upstream Neo4jBackup to read status.replicationPullURI from (backup mode)")
	fromCluster := fs.String("from-cluster", "", "Upstream Neo4jEnterpriseCluster to read replication addresses from (network mode)")
	clusterRef := fs.String("cluster-ref", "", "Downstream cluster or standalone that will host the replica (required)")
	upstreamDatabase := fs.String("upstream-database", "", "Database name on the upstream (required)")
	downstreamNamespace := fs.String("downstream-namespace", "", "Namespace to write into the manifest (default: the source namespace)")
	seedFromLatest := fs.Bool("seed-from-latest", false, "Also set source.seedURI from the newest successful backup run")
	namespace := namespaceFlag(fs, "Namespace of the upstream resource")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Author a Neo4jReplicaDatabase for a downstream on another Kubernetes cluster.

Usage:
  kubectl neo4j export replica-database <name> --from-backup <backup> \
      --cluster-ref <downstream> --upstream-database <db> [-n <namespace>]

Reads the upstream's status in THIS cluster and writes the manifest to stdout,
ready to redirect to a file or pipe into kubectl against the other cluster:

  kubectl neo4j export replica-database dr-copy --from-backup nightly \
      --cluster-ref dr --upstream-database neo4j > replica.yaml

Only stdout carries the manifest; notes and warnings go to stderr.

The result is checked against the operator's own ReplicaValidator before it is
printed, so it cannot emit something "kubectl neo4j validate" would reject.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}

	name := fs.Arg(0)
	switch {
	case name == "":
		fmt.Fprint(stderr, "error: a name for the replica database is required\n\n")
		fs.Usage()
		return exitUsage
	case *fromBackup == "" && *fromCluster == "":
		fmt.Fprintln(stderr, "error: one of --from-backup or --from-cluster is required")
		return exitUsage
	case *fromBackup != "" && *fromCluster != "":
		fmt.Fprintln(stderr, "error: --from-backup and --from-cluster are mutually exclusive —")
		fmt.Fprintln(stderr, "  backup mode pulls from object storage, network mode connects to the upstream.")
		return exitUsage
	case *clusterRef == "":
		fmt.Fprintln(stderr, "error: --cluster-ref is required — it names the downstream cluster that will host the replica")
		return exitUsage
	case *upstreamDatabase == "":
		fmt.Fprintln(stderr, "error: --upstream-database is required — it names the database on the upstream")
		return exitUsage
	}

	c, err := newClusterClient(*kubeconfig, *kubeContext)
	if err != nil {
		fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
		return exitUsage
	}
	ns := *namespace
	if ns == "" {
		ns = currentNamespace(*kubeconfig, *kubeContext)
	}
	outNS := *downstreamNamespace
	if outNS == "" {
		outNS = ns
	}

	ctx := context.Background()
	var replica *neo4jv1beta1.Neo4jReplicaDatabase
	var notes []string

	if *fromBackup != "" {
		replica, notes, err = replicaFromBackup(ctx, c, ns, outNS, name, *fromBackup, *clusterRef, *upstreamDatabase, *seedFromLatest)
	} else {
		replica, notes, err = replicaFromCluster(ctx, c, ns, outNS, name, *fromCluster, *clusterRef, *upstreamDatabase)
	}
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	// Validated with the operator's own validator, against the SAME client, so
	// the manifest this prints is one the operator would accept.
	result := validation.NewReplicaValidator(c).Validate(ctx, replica)
	for _, w := range result.Warnings {
		notes = append(notes, "warning: "+w)
	}
	if len(result.Errors) > 0 {
		fmt.Fprintln(stderr, "error: the manifest this would produce does not validate:")
		for _, e := range result.Errors {
			fmt.Fprintf(stderr, "  ✗ %s: %s\n", e.Field, e.ErrorBody())
		}
		fmt.Fprintln(stderr, "  Nothing was written. This is a bug in the CLI or a gap in the upstream's status —")
		fmt.Fprintln(stderr, "  please report it with the upstream resource's status attached.")
		return exitInvalid
	}

	body, err := yaml.Marshal(replica)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}
	fmt.Fprint(stdout, string(body))

	for _, n := range notes {
		fmt.Fprintln(stderr, n)
	}
	return exitOK
}

// replicaFromBackup builds a backup-mode replica from an upstream Neo4jBackup's
// published pull URI.
func replicaFromBackup(ctx context.Context, c client.Client, ns, outNS, name, backupName, clusterRef, upstreamDatabase string, seedFromLatest bool) (*neo4jv1beta1.Neo4jReplicaDatabase, []string, error) {
	var backup neo4jv1beta1.Neo4jBackup
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: backupName}, &backup); err != nil {
		return nil, nil, fmt.Errorf("reading Neo4jBackup %s/%s: %w", ns, backupName, err)
	}

	pullURI := backup.Status.ReplicationPullURI
	if pullURI == "" {
		return nil, nil, fmt.Errorf(
			"Neo4jBackup %s/%s has not published status.replicationPullURI.\n"+
				"  That field is populated only when spec.mode is \"replication-source\", and only\n"+
				"  after a run has completed. Check the backup's mode and that it has run at least once",
			ns, backupName)
	}

	replica := newReplicaSkeleton(name, outNS, clusterRef, upstreamDatabase)
	replica.Spec.Source.Mode = "backup"
	replica.Spec.Source.PullURI = pullURI

	notes := []string{
		fmt.Sprintf("source.pullURI taken from Neo4jBackup %s/%s status.replicationPullURI", ns, backupName),
	}
	if cloud := backup.Spec.Storage.Cloud; cloud != nil && cloud.CredentialsSecretRef != "" {
		notes = append(notes, fmt.Sprintf(
			"note: the upstream reads its bucket with Secret %q. The DOWNSTREAM cluster needs its own\n"+
				"      credentials for the same bucket — set source.credentialsSecretRef there, or bind a\n"+
				"      workload identity. This command cannot copy a Secret between clusters.",
			cloud.CredentialsSecretRef))
	}

	if seedFromLatest {
		artifact, err := latestArtifact(&backup)
		if err != nil {
			return nil, nil, err
		}
		replica.Spec.Source.SeedURI = strings.TrimSuffix(pullURI, "/") + "/" + artifact
		notes = append(notes, fmt.Sprintf(
			"source.seedURI built from the newest successful run's artifact %q, joined onto the pull URI", artifact))
	}
	return replica, notes, nil
}

// latestArtifact finds the newest successful run's artifact filename. It never
// guesses: a run whose filename was not captured (the operator parses it out of
// the Job's pod log, which can fail) is skipped rather than reconstructed, and
// running out of candidates is an error.
func latestArtifact(backup *neo4jv1beta1.Neo4jBackup) (string, error) {
	runs := make([]neo4jv1beta1.BackupRun, 0, len(backup.Status.History))
	for _, r := range backup.Status.History {
		if strings.EqualFold(r.Status, "Completed") || strings.EqualFold(r.Status, "Succeeded") {
			runs = append(runs, r)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[j].StartTime.Before(&runs[i].StartTime) })

	for _, r := range runs {
		if r.ArtifactFilename != "" {
			return r.ArtifactFilename, nil
		}
	}
	return "", fmt.Errorf(
		"--seed-from-latest: no successful run in status.history carries an artifactFilename.\n" +
			"  The operator parses that name out of the backup Job's pod log, so it can be empty\n" +
			"  even for a run that succeeded. Set source.seedURI by hand, or omit the flag —\n" +
			"  a replica can seed from the chain in pullURI alone")
}

// replicaFromCluster builds a network-mode replica from an upstream cluster's
// published addresses.
func replicaFromCluster(ctx context.Context, c client.Client, ns, outNS, name, clusterName, clusterRef, upstreamDatabase string) (*neo4jv1beta1.Neo4jReplicaDatabase, []string, error) {
	var upstream neo4jv1beta1.Neo4jEnterpriseCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: clusterName}, &upstream); err != nil {
		return nil, nil, fmt.Errorf("reading Neo4jEnterpriseCluster %s/%s: %w", ns, clusterName, err)
	}

	replica := newReplicaSkeleton(name, outNS, clusterRef, upstreamDatabase)
	replica.Spec.Source.Mode = "network"

	ccr := upstream.Status.CrossClusterReplication
	switch {
	case ccr != nil && len(ccr.Addresses) > 0:
		replica.Spec.Source.Addresses = ccr.Addresses
		notes := []string{fmt.Sprintf(
			"source.addresses taken from %s/%s status.crossClusterReplication.addresses", ns, clusterName)}
		if !ccr.Ready {
			notes = append(notes, "warning: the upstream's cross-cluster proxy does not report Ready yet.\n"+
				"         These addresses may not route until it does.")
		}
		return replica, notes, nil

	case len(upstream.Status.InternalAddresses) > 0:
		// Emitting these is still useful — a downstream in another NAMESPACE on
		// the same cluster can use them — but they are in-cluster DNS names, so
		// saying nothing here would hand someone an manifest that silently
		// cannot connect from a genuinely separate cluster.
		replica.Spec.Source.Addresses = upstream.Status.InternalAddresses
		return replica, []string{
			fmt.Sprintf("source.addresses taken from %s/%s status.internalAddresses", ns, clusterName),
			"warning: these are IN-CLUSTER addresses. They resolve across namespaces on this\n" +
				"         Kubernetes cluster, but NOT from a separate one.\n" +
				"         For a genuinely separate cluster, set spec.crossClusterReplication.enabled\n" +
				"         on the upstream and re-run this once its proxy reports Ready.\n" +
				"         For a downstream on THIS cluster, prefer source.upstreamClusterRef, which\n" +
				"         stays correct when the upstream's addresses change.",
		}, nil

	default:
		return nil, nil, fmt.Errorf(
			"Neo4jEnterpriseCluster %s/%s publishes no replication addresses.\n"+
				"  status.internalAddresses is populated once the cluster is running;\n"+
				"  status.crossClusterReplication.addresses needs spec.crossClusterReplication.enabled",
			ns, clusterName)
	}
}

func newReplicaSkeleton(name, ns, clusterRef, upstreamDatabase string) *neo4jv1beta1.Neo4jReplicaDatabase {
	return &neo4jv1beta1.Neo4jReplicaDatabase{
		TypeMeta: metav1.TypeMeta{
			APIVersion: neo4jv1beta1.GroupVersion.String(),
			Kind:       "Neo4jReplicaDatabase",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
			ClusterRef:       clusterRef,
			UpstreamDatabase: upstreamDatabase,
		},
	}
}
