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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func replicaCR(mutate func(*neo4jv1beta1.Neo4jReplicaDatabase)) *neo4jv1beta1.Neo4jReplicaDatabase {
	r := &neo4jv1beta1.Neo4jReplicaDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "foo-replica", Namespace: "dr"},
		Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{
			ClusterRef:       "dr-cluster",
			UpstreamDatabase: "foo",
			Source: neo4jv1beta1.ReplicaSourceSpec{
				Mode:    neo4jv1beta1.ReplicaSourceModeBackup,
				PullURI: "s3://backups/foo-chain/",
				SeedURI: "s3://backups/foo-chain/foo-2026-08-01.backup",
			},
		},
	}
	if mutate != nil {
		mutate(r)
	}
	return r
}

func TestReplicaValidator_ValidBackupSource(t *testing.T) {
	res := NewReplicaValidator(nil).Validate(context.Background(), replicaCR(nil))
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", res.Errors)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("expected no warnings, got %v", res.Warnings)
	}
}

func TestReplicaValidator_ValidNetworkSource(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:      neo4jv1beta1.ReplicaSourceModeNetwork,
			Addresses: []string{"upstream-0.example.com:16000"},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", res.Errors)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("expected no warnings, got %v", res.Warnings)
	}
}

func TestReplicaValidator_NetworkSourceRequiresAddresses(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{Mode: neo4jv1beta1.ReplicaSourceModeNetwork}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected missing addresses to be rejected")
	}
	if !strings.Contains(res.Errors.ToAggregate().Error(), "addresses") {
		t.Errorf("expected the error to mention addresses, got: %v", res.Errors)
	}
}

func TestReplicaValidator_NetworkSourceMalformedAddress(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:      neo4jv1beta1.ReplicaSourceModeNetwork,
			Addresses: []string{"upstream-0.example.com"}, // missing port
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected a host with no port to be rejected")
	}
}

func TestReplicaValidator_ValidUpstreamClusterRef(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:               neo4jv1beta1.ReplicaSourceModeNetwork,
			UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{Name: "prod-cluster"},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", res.Errors)
	}
}

// TestReplicaValidator_AddressesAndUpstreamClusterRefMutuallyExclusive pins
// that exactly one of source.addresses / source.upstreamClusterRef is
// required in network mode — setting both is ambiguous about which the
// controller should resolve from.
func TestReplicaValidator_AddressesAndUpstreamClusterRefMutuallyExclusive(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:               neo4jv1beta1.ReplicaSourceModeNetwork,
			Addresses:          []string{"upstream-0.example.com:6000"},
			UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{Name: "prod-cluster"},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected setting both addresses and upstreamClusterRef to be rejected")
	}
	if !strings.Contains(res.Errors.ToAggregate().Error(), "mutually exclusive") {
		t.Errorf("expected a mutual-exclusivity error, got: %v", res.Errors)
	}
}

func TestReplicaValidator_UpstreamClusterRefRequiresName(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:               neo4jv1beta1.ReplicaSourceModeNetwork,
			UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected an empty upstreamClusterRef.name to be rejected")
	}
}

func TestReplicaValidator_NetworkSourceWarnsOnBackupFields(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:                 neo4jv1beta1.ReplicaSourceModeNetwork,
			Addresses:            []string{"upstream-0.example.com:16000"},
			PullURI:              "s3://backups/foo-chain/",
			SeedURI:              "s3://backups/foo-chain/foo-2026-08-01.backup",
			CredentialsSecretRef: "creds",
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", res.Errors)
	}
	if len(res.Warnings) != 3 {
		t.Errorf("expected 3 warnings (pullURI, seedURI, credentialsSecretRef ignored), got %v", res.Warnings)
	}
}

func TestReplicaValidator_PullURIRequired(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source.PullURI = ""
		r.Spec.Source.SeedURI = ""
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected missing pullURI to be rejected")
	}
	if !strings.Contains(res.Errors.ToAggregate().Error(), "replicationPullURI") {
		t.Error("error should point the user at the upstream backup CR's status field")
	}
}

func TestReplicaValidator_ValidUpstreamBackupRef(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:              neo4jv1beta1.ReplicaSourceModeBackup,
			UpstreamBackupRef: &neo4jv1beta1.UpstreamBackupRef{Name: "foo-chain"},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", res.Errors)
	}
}

// TestReplicaValidator_PullURIAndUpstreamBackupRefMutuallyExclusive pins
// that exactly one of source.pullURI / source.upstreamBackupRef is required
// in backup mode — mirrors the network-mode addresses/upstreamClusterRef
// mutual-exclusivity rule.
func TestReplicaValidator_PullURIAndUpstreamBackupRefMutuallyExclusive(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:              neo4jv1beta1.ReplicaSourceModeBackup,
			PullURI:           "s3://backups/foo-chain/",
			UpstreamBackupRef: &neo4jv1beta1.UpstreamBackupRef{Name: "foo-chain"},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected setting both pullURI and upstreamBackupRef to be rejected")
	}
	if !strings.Contains(res.Errors.ToAggregate().Error(), "mutually exclusive") {
		t.Errorf("expected a mutual-exclusivity error, got: %v", res.Errors)
	}
}

func TestReplicaValidator_UpstreamBackupRefRequiresName(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
			Mode:              neo4jv1beta1.ReplicaSourceModeBackup,
			UpstreamBackupRef: &neo4jv1beta1.UpstreamBackupRef{},
		}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected an empty upstreamBackupRef.name to be rejected")
	}
}

func TestReplicaValidator_UnsupportedScheme(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source.PullURI = "wasb://bucket/chain/"
		r.Spec.Source.SeedURI = ""
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected unsupported scheme to be rejected")
	}
}

// A seed outside the pull directory produces a replica that seeds fine and
// then can never apply a differential, because the chain it follows does not
// contain its own starting point. Warn rather than reject: the operator cannot
// see the bucket, so it cannot be certain.
func TestReplicaValidator_SeedOutsidePullDirWarns(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Source.SeedURI = "s3://backups/some-other-chain/foo.backup"
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) != 0 {
		t.Fatalf("expected a warning not an error, got errors %v", res.Errors)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected a warning about the seed being outside the chain")
	}
}

func TestReplicaValidator_BacktickRejected(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Name = "foo`; DROP DATABASE neo4j; //"
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected a backtick in the database name to be rejected (Cypher injection guard)")
	}
}

func TestReplicaValidator_SecondariesWithoutPrimary(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Topology = &neo4jv1beta1.DatabaseTopology{Primaries: 0, Secondaries: 2}
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) == 0 {
		t.Fatal("expected secondaries-without-primary to be rejected")
	}
}

func TestReplicaValidator_SameNameAsUpstreamWarns(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Name = "foo"
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	if len(res.Errors) != 0 {
		t.Fatalf("same name as upstream is legal, got errors %v", res.Errors)
	}
	if len(res.Warnings) == 0 {
		t.Error("expected a warning that the name is permanent through promotion")
	}
}
