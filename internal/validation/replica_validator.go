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

package validation

import (
	"context"
	"fmt"
	"net"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// replicaSeedSchemes are the object-storage schemes Neo4j's CloudSeedProvider
// accepts for seedURI / pullURI.
var replicaSeedSchemes = []string{"s3://", "gs://", "azb://", "https://", "http://", "ftp://", "file://"}

// ReplicaValidator validates Neo4jReplicaDatabase specs inline (invariant 1 —
// there is no admission webhook).
type ReplicaValidator struct {
	Client client.Client
}

// NewReplicaValidator constructs a ReplicaValidator.
func NewReplicaValidator(c client.Client) *ReplicaValidator {
	return &ReplicaValidator{Client: c}
}

// Validate checks a Neo4jReplicaDatabase spec.
func (v *ReplicaValidator) Validate(_ context.Context, replica *neo4jv1beta1.Neo4jReplicaDatabase) ValidationResult {
	var res ValidationResult
	specPath := field.NewPath("spec")

	name := replica.Spec.Name
	if name == "" {
		name = replica.Name
	}

	// Identifiers are interpolated into Cypher admin DDL, which accepts no
	// parameters for names. A backtick would close the quoting early.
	if strings.Contains(name, "`") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"database name may not contain a backtick"))
	}
	if strings.Contains(replica.Spec.UpstreamDatabase, "`") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("upstreamDatabase"),
			replica.Spec.UpstreamDatabase, "database name may not contain a backtick"))
	}

	srcPath := specPath.Child("source")
	src := replica.Spec.Source

	mode := src.Mode
	if mode == "" {
		mode = neo4jv1beta1.ReplicaSourceModeBackup
	}

	switch mode {
	case neo4jv1beta1.ReplicaSourceModeBackup:
		v.validateBackupSource(src, srcPath, &res)
	case neo4jv1beta1.ReplicaSourceModeNetwork:
		v.validateNetworkSource(src, srcPath, &res)
	}

	// Topology sanity. Neo4j itself enforces the cluster-size relationship;
	// this catches the obviously-wrong shapes early with a better message.
	if t := replica.Spec.Topology; t != nil {
		tPath := specPath.Child("topology")
		if t.Primaries < 0 {
			res.Errors = append(res.Errors, field.Invalid(tPath.Child("primaries"), t.Primaries,
				"must not be negative"))
		}
		if t.Secondaries < 0 {
			res.Errors = append(res.Errors, field.Invalid(tPath.Child("secondaries"), t.Secondaries,
				"must not be negative"))
		}
		if t.Primaries == 0 && t.Secondaries > 0 {
			res.Errors = append(res.Errors, field.Invalid(tPath.Child("primaries"), t.Primaries,
				"a replica with secondaries must have at least one primary"))
		}
	}

	// A replica named for the database it replicates is legal but usually a
	// mistake worth surfacing, because the name is permanent through
	// promotion.
	if name == replica.Spec.UpstreamDatabase {
		res.Warnings = append(res.Warnings, fmt.Sprintf(
			"replica %q has the same name as its upstream database; this is allowed, but note the name is "+
				"permanent (Cypher has no RENAME DATABASE) and will collide if this cluster ever also hosts "+
				"the upstream", name))
	}

	return res
}

func (v *ReplicaValidator) validateBackupSource(src neo4jv1beta1.ReplicaSourceSpec, srcPath *field.Path, res *ValidationResult) {
	hasPullURI := src.PullURI != ""
	hasBackupRef := src.UpstreamBackupRef != nil

	switch {
	case hasPullURI && hasBackupRef:
		res.Errors = append(res.Errors, field.Invalid(srcPath.Child("upstreamBackupRef"), src.UpstreamBackupRef,
			"source.pullURI and source.upstreamBackupRef are mutually exclusive; set exactly one"))
	case !hasPullURI && !hasBackupRef:
		res.Errors = append(res.Errors, field.Required(srcPath.Child("pullURI"),
			"backup-based replication requires either source.pullURI (the object-storage directory holding the "+
				"upstream's differential chain — read it from the upstream Neo4jBackup CR's "+
				"status.replicationPullURI) or source.upstreamBackupRef (only when the upstream Neo4jBackup is on "+
				"this same Kubernetes cluster)"))
	case hasBackupRef:
		if src.UpstreamBackupRef.Name == "" {
			res.Errors = append(res.Errors, field.Required(srcPath.Child("upstreamBackupRef").Child("name"),
				"required when source.upstreamBackupRef is set"))
		}
	default: // hasPullURI only
		if !hasSupportedScheme(src.PullURI) {
			res.Errors = append(res.Errors, field.Invalid(srcPath.Child("pullURI"), src.PullURI,
				"must use one of the supported schemes: "+strings.Join(replicaSeedSchemes, ", ")))
		}
	}

	if src.SeedURI != "" && !hasSupportedScheme(src.SeedURI) {
		res.Errors = append(res.Errors, field.Invalid(srcPath.Child("seedURI"), src.SeedURI,
			"must use one of the supported schemes: "+strings.Join(replicaSeedSchemes, ", ")))
	}

	// A seedURI outside the pullURI directory is the classic way to end up
	// with a replica that seeds successfully and then can never apply a
	// differential, because the chain it is following does not contain its
	// own starting point.
	if src.SeedURI != "" && src.PullURI != "" {
		base := strings.TrimSuffix(src.PullURI, "/")
		if !strings.HasPrefix(src.SeedURI, base+"/") {
			res.Warnings = append(res.Warnings, fmt.Sprintf(
				"seedURI %q is not inside pullURI %q; the seed must belong to the same backup chain or the "+
					"replica will seed and then fail to apply differentials", src.SeedURI, src.PullURI))
		}
	}

	if len(src.Addresses) > 0 {
		res.Warnings = append(res.Warnings, "source.addresses is ignored in backup mode")
	}
}

func (v *ReplicaValidator) validateNetworkSource(src neo4jv1beta1.ReplicaSourceSpec, srcPath *field.Path, res *ValidationResult) {
	hasAddresses := len(src.Addresses) > 0
	hasClusterRef := src.UpstreamClusterRef != nil

	switch {
	case hasAddresses && hasClusterRef:
		res.Errors = append(res.Errors, field.Invalid(srcPath.Child("upstreamClusterRef"), src.UpstreamClusterRef,
			"source.addresses and source.upstreamClusterRef are mutually exclusive; set exactly one"))
	case !hasAddresses && !hasClusterRef:
		res.Errors = append(res.Errors, field.Required(srcPath.Child("addresses"),
			"network replication requires either source.addresses (at least one upstream cluster endpoint, "+
				"host:port; one reachable address is sufficient, since the upstream hands back the addresses "+
				"the downstream actually uses) or source.upstreamClusterRef (only when the upstream "+
				"Neo4jEnterpriseCluster is on this same Kubernetes cluster). For a separate upstream cluster, set "+
				"spec.crossClusterReplication.enabled: true on it and read status.crossClusterReplication.addresses"))
	case hasClusterRef:
		if src.UpstreamClusterRef.Name == "" {
			res.Errors = append(res.Errors, field.Required(srcPath.Child("upstreamClusterRef").Child("name"),
				"required when source.upstreamClusterRef is set"))
		}
	default: // hasAddresses only
		for i, addr := range src.Addresses {
			host, port, err := net.SplitHostPort(addr)
			if err != nil || host == "" || port == "" {
				res.Errors = append(res.Errors, field.Invalid(srcPath.Child("addresses").Index(i), addr,
					"must be of the form host:port"))
			}
		}
	}

	// These fields belong to backup mode; set alongside network mode they are
	// silently ignored by Neo4j, which is worth surfacing rather than leaving
	// the user to wonder why a configured credential/URI had no effect.
	if src.PullURI != "" {
		res.Warnings = append(res.Warnings, "source.pullURI is ignored in network mode")
	}
	if src.SeedURI != "" {
		res.Warnings = append(res.Warnings, "source.seedURI is ignored in network mode")
	}
	if src.CredentialsSecretRef != "" {
		res.Warnings = append(res.Warnings, "source.credentialsSecretRef is ignored in network mode")
	}
}

func hasSupportedScheme(uri string) bool {
	for _, s := range replicaSeedSchemes {
		if strings.HasPrefix(uri, s) {
			return true
		}
	}
	return false
}
