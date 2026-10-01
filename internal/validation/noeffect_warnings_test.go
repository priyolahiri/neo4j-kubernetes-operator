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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The tests in this file pin one rule (docs/knowledge/operations.md, "A schema
// field that no code reads must warn when set"): every field the schema accepts
// but nothing reads produces a warning shaped
//
//	spec.<path> is accepted but has no effect today: <what to do instead>
//
// and the CR is STILL ACCEPTED — the warning must never be an error. Each case
// therefore asserts both halves: the warning is present, and no error was added.
// A case that stops failing when its warning is deleted does not pin anything.

// requireNoEffectWarning asserts that warnings contains exactly one warning for
// path, in the standard shape, and that it names the alternative (instead).
func requireNoEffectWarning(t *testing.T, warnings []string, path, instead string) {
	t.Helper()
	prefix := path + " is accepted but has no effect today: "
	var matches []string
	for _, w := range warnings {
		if strings.HasPrefix(w, prefix) {
			matches = append(matches, w)
		}
	}
	require.Len(t, matches, 1, "want exactly one no-effect warning for %s in %q", path, warnings)
	assert.Contains(t, matches[0], instead, "the warning for %s must say what to do instead", path)
}

// noEffectCount counts warnings in the standard no-effect shape.
func noEffectCount(warnings []string) int {
	n := 0
	for _, w := range warnings {
		if strings.Contains(w, " is accepted but has no effect today: ") {
			n++
		}
	}
	return n
}

func TestNoEffectWarning_Shape(t *testing.T) {
	got := NoEffectWarning(field.NewPath("spec", "tls", "certificateSecret"), "use spec.tls.issuerRef")
	assert.Equal(t, "spec.tls.certificateSecret is accepted but has no effect today: use spec.tls.issuerRef", got)
}

func validClusterForWarnings() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0", PullPolicy: "IfNotPresent"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "fast-ssd", Size: "100Gi"},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
		},
	}
}

func TestClusterValidator_NoEffectWarnings(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = neo4jv1beta1.AddToScheme(scheme)
	validator := NewClusterValidator(fake.NewClientBuilder().WithScheme(scheme).Build())
	ctx := context.Background()

	t.Run("a cluster that sets none of them gets none", func(t *testing.T) {
		res := validator.ValidateCreateWithWarnings(ctx, validClusterForWarnings())
		assert.Empty(t, res.Errors)
		assert.Empty(t, res.NoEffectWarnings)
	})

	cases := []struct {
		name    string
		mutate  func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		path    string
		instead string
	}{
		{
			name: "tls.certificateSecret with tls disabled",
			mutate: func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
				c.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "disabled", CertificateSecret: "my-cert"}
			},
			path:    "spec.tls.certificateSecret",
			instead: "prod-tls-secret",
		},
		{
			// The warning is unconditional: cert-manager mode is the case a user
			// supplying their own Secret is most likely to be in.
			name: "tls.certificateSecret with cert-manager",
			mutate: func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
				c.Spec.TLS = &neo4jv1beta1.TLSSpec{
					Mode:              "cert-manager",
					IssuerRef:         &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
					CertificateSecret: "my-cert",
				}
			},
			path:    "spec.tls.certificateSecret",
			instead: "spec.tls.issuerRef",
		},
		{
			name: "topology.placement.nodeSelector",
			mutate: func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
				c.Spec.Topology.Placement = &neo4jv1beta1.PlacementConfig{
					NodeSelector: map[string]string{"workload": "database"},
				}
			},
			path:    "spec.topology.placement.nodeSelector",
			instead: "top-level spec.nodeSelector",
		},
		{
			name: "topology.placement.requiredDuringScheduling",
			mutate: func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
				c.Spec.Topology.Placement = &neo4jv1beta1.PlacementConfig{RequiredDuringScheduling: true}
			},
			path:    "spec.topology.placement.requiredDuringScheduling",
			instead: "antiAffinity.type: required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := validClusterForWarnings()
			tc.mutate(cluster)

			// Create and update both report it, and neither turns it into an error.
			create := validator.ValidateCreateWithWarnings(ctx, cluster)
			assert.Empty(t, create.Errors, "the cluster must still be accepted")
			requireNoEffectWarning(t, create.NoEffectWarnings, tc.path, tc.instead)

			update := validator.ValidateUpdateWithWarnings(ctx, validClusterForWarnings(), cluster)
			assert.Empty(t, update.Errors, "the update must still be accepted")
			requireNoEffectWarning(t, update.NoEffectWarnings, tc.path, tc.instead)

			// They are not topology warnings: that channel keeps its own reason.
			assert.Zero(t, noEffectCount(create.Warnings), "no-effect warnings belong in NoEffectWarnings")
		})
	}

	t.Run("placement fields that ARE read do not warn", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.Topology.Placement = &neo4jv1beta1.PlacementConfig{
			TopologySpread: &neo4jv1beta1.TopologySpreadConfig{Enabled: true},
			AntiAffinity:   &neo4jv1beta1.PodAntiAffinityConfig{Enabled: true},
		}
		res := validator.ValidateCreateWithWarnings(ctx, cluster)
		assert.Empty(t, res.NoEffectWarnings)
	})
}

func TestStandaloneValidator_NoEffectWarnings(t *testing.T) {
	v := NewStandaloneValidator()

	clean := validStandalone()
	assert.Empty(t, v.NoEffectWarnings(clean), "a standalone that sets none of them gets none")

	for _, mode := range []string{"cert-manager", "disabled"} {
		t.Run("tls.certificateSecret with mode "+mode, func(t *testing.T) {
			sa := validStandalone()
			sa.Spec.TLS = &neo4jv1beta1.TLSSpec{
				Mode:              mode,
				IssuerRef:         &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
				CertificateSecret: "my-cert",
			}
			// Still accepted: the warning adds nothing to the error list.
			assert.Empty(t, v.ValidateCreate(sa), "the standalone must still be accepted")
			requireNoEffectWarning(t, v.NoEffectWarnings(sa), "spec.tls.certificateSecret",
				"test-standalone-tls-secret")
		})
	}
}

// spec.tls.strictPeerValidation is read only by Neo4jEnterpriseCluster (it picks
// the cluster SSL policy); a standalone has no intra-cluster traffic and nothing
// reads it. The API reference said so, but a user who sets it to false on a
// standalone expecting the legacy trust_all posture got no hint it did nothing.
func TestStandaloneValidator_NoEffectWarnings_StrictPeerValidation(t *testing.T) {
	v := NewStandaloneValidator()
	withStrict := func(b *bool) *neo4jv1beta1.Neo4jEnterpriseStandalone {
		sa := validStandalone()
		sa.Spec.TLS = &neo4jv1beta1.TLSSpec{
			Mode:                 "cert-manager",
			IssuerRef:            &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
			StrictPeerValidation: b,
		}
		return sa
	}
	no, yes := false, true

	t.Run("an explicit false warns and is still accepted", func(t *testing.T) {
		sa := withStrict(&no)
		assert.Empty(t, v.ValidateCreate(sa), "the warning must not become an error")
		requireNoEffectWarning(t, v.NoEffectWarnings(sa), "spec.tls.strictPeerValidation",
			"Neo4jEnterpriseCluster")
	})

	// The CRD defaults the field to true wherever spec.tls is present, so a
	// standalone that never mentions it arrives here with true. Warning on that
	// would fire for every TLS standalone, about a value the user never wrote.
	t.Run("true, the CRD default, does not warn", func(t *testing.T) {
		assert.Zero(t, noEffectCount(v.NoEffectWarnings(withStrict(&yes))))
	})

	t.Run("unset does not warn", func(t *testing.T) {
		assert.Zero(t, noEffectCount(v.NoEffectWarnings(withStrict(nil))))
	})

	// The field IS read for clusters, so the cluster must not be told it is ignored.
	t.Run("a cluster is never warned", func(t *testing.T) {
		cluster := validClusterForWarnings()
		cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{
			Mode:                 "cert-manager",
			IssuerRef:            &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
			StrictPeerValidation: &no,
		}
		for _, w := range NewClusterValidator(nil).NoEffectWarnings(cluster) {
			assert.NotContains(t, w, "strictPeerValidation")
		}
	})
}

func validPluginForWarnings() *neo4jv1beta1.Neo4jPlugin {
	return &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "apoc"},
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "test-cluster",
			Name:       "apoc",
			Version:    "5.26.0",
			Enabled:    true,
		},
	}
}

func TestPluginValidator_NoEffectWarnings(t *testing.T) {
	v := NewPluginValidator()

	base := v.Validate(validPluginForWarnings())
	require.Empty(t, base.Errors)
	require.Zero(t, noEffectCount(base.Warnings), "a plugin that sets none of them gets none")

	cases := []struct {
		name    string
		mutate  func(*neo4jv1beta1.Neo4jPlugin)
		path    string
		instead string
	}{
		{
			name: "resources",
			mutate: func(p *neo4jv1beta1.Neo4jPlugin) {
				p.Spec.Resources = &neo4jv1beta1.PluginResourceRequirements{MemoryLimit: "1Gi", CPULimit: "500m"}
			},
			path:    "spec.resources",
			instead: "cluster or standalone spec.resources",
		},
		{
			name: "source.registry",
			mutate: func(p *neo4jv1beta1.Neo4jPlugin) {
				p.Spec.Source = &neo4jv1beta1.PluginSource{
					Type:     "official",
					Registry: &neo4jv1beta1.PluginRegistry{URL: "https://plugins.example.com"},
				}
			},
			path:    "spec.source.registry",
			instead: "spec.source.type url or custom with spec.source.url, spec.source.checksum and spec.source.authSecret, and spec.installMode VerifiedDownload",
		},
		{
			name: "source.registry.tls",
			mutate: func(p *neo4jv1beta1.Neo4jPlugin) {
				p.Spec.Source = &neo4jv1beta1.PluginSource{
					Type: "official",
					Registry: &neo4jv1beta1.PluginRegistry{
						URL: "https://plugins.example.com",
						TLS: &neo4jv1beta1.RegistryTLSConfig{InsecureSkipVerify: true},
					},
				}
			},
			path:    "spec.source.registry.tls",
			instead: "spec.trustedCASecrets",
		},
		{
			name: "security.securityPolicy",
			mutate: func(p *neo4jv1beta1.Neo4jPlugin) {
				p.Spec.Security = &neo4jv1beta1.PluginSecurity{SecurityPolicy: "strict"}
			},
			path:    "spec.security.securityPolicy",
			instead: "spec.security.allowedProcedures",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := validPluginForWarnings()
			tc.mutate(plugin)
			res := v.Validate(plugin)
			assert.Empty(t, res.Errors, "the plugin must still be accepted")
			requireNoEffectWarning(t, res.Warnings, tc.path, tc.instead)
		})
	}

	t.Run("registry with tls warns for both, once each", func(t *testing.T) {
		plugin := validPluginForWarnings()
		plugin.Spec.Source = &neo4jv1beta1.PluginSource{
			Type: "official",
			Registry: &neo4jv1beta1.PluginRegistry{
				URL: "https://plugins.example.com",
				TLS: &neo4jv1beta1.RegistryTLSConfig{CASecret: "ca"},
			},
		}
		res := v.Validate(plugin)
		assert.Empty(t, res.Errors)
		assert.Equal(t, 2, noEffectCount(res.Warnings), "registry and registry.tls: %q", res.Warnings)
	})

	t.Run("securityPolicy is still validated as well as warned about", func(t *testing.T) {
		plugin := validPluginForWarnings()
		plugin.Spec.Security = &neo4jv1beta1.PluginSecurity{SecurityPolicy: "bogus"}
		res := v.Validate(plugin)
		require.NotEmpty(t, res.Errors, "an unknown securityPolicy must still be rejected")
		assert.Contains(t, res.Errors.ToAggregate().Error(), "securityPolicy")
		requireNoEffectWarning(t, res.Warnings, "spec.security.securityPolicy", "spec.security.allowedProcedures")
	})

	t.Run("fields that ARE read do not warn", func(t *testing.T) {
		plugin := validPluginForWarnings()
		plugin.Spec.Security = &neo4jv1beta1.PluginSecurity{
			AllowedProcedures: []string{"apoc.*"},
			Sandbox:           true,
		}
		plugin.Spec.Source = &neo4jv1beta1.PluginSource{Type: "official"}
		res := v.Validate(plugin)
		assert.Empty(t, res.Errors)
		assert.Zero(t, noEffectCount(res.Warnings))
	})
}

func TestDatabaseValidator_NoEffectWarnings(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = neo4jv1beta1.AddToScheme(scheme)
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
		},
	}
	v := NewDatabaseValidator(fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build())

	dbWith := func(initial *neo4jv1beta1.InitialDataSpec) *neo4jv1beta1.Neo4jDatabase {
		return &neo4jv1beta1.Neo4jDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jDatabaseSpec{
				ClusterRef:  "prod",
				Name:        "movies",
				InitialData: initial,
			},
		}
	}
	const instead = "only spec.initialData.cypherStatements is executed"

	cases := []struct {
		name    string
		initial *neo4jv1beta1.InitialDataSpec
		path    string
	}{
		{"source dump", &neo4jv1beta1.InitialDataSpec{Source: "dump"}, "spec.initialData.source"},
		{"source csv", &neo4jv1beta1.InitialDataSpec{Source: "csv"}, "spec.initialData.source"},
		{"configMapRef", &neo4jv1beta1.InitialDataSpec{ConfigMapRef: "data"}, "spec.initialData.configMapRef"},
		{"secretRef", &neo4jv1beta1.InitialDataSpec{SecretRef: "data"}, "spec.initialData.secretRef"},
		{"storage", &neo4jv1beta1.InitialDataSpec{Storage: &neo4jv1beta1.StorageLocation{Type: "s3"}},
			"spec.initialData.storage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := v.Validate(context.Background(), dbWith(tc.initial))
			assert.Empty(t, res.Errors, "the database must still be accepted")
			requireNoEffectWarning(t, res.Warnings, tc.path, instead)
		})
	}

	t.Run("cypherStatements, with source cypher, is the supported shape and does not warn", func(t *testing.T) {
		// Five shipped examples set exactly this.
		res := v.Validate(context.Background(), dbWith(&neo4jv1beta1.InitialDataSpec{
			Source:           "cypher",
			CypherStatements: []string{"CREATE INDEX movie_title IF NOT EXISTS FOR (m:Movie) ON (m.title)"},
		}))
		assert.Empty(t, res.Errors)
		assert.Zero(t, noEffectCount(res.Warnings), "%q", res.Warnings)
	})

	t.Run("the warning survives an unresolvable clusterRef", func(t *testing.T) {
		// Validate returns early when the host is missing; the warning must not
		// depend on getting past that.
		db := dbWith(&neo4jv1beta1.InitialDataSpec{SecretRef: "data"})
		db.Spec.ClusterRef = "missing"
		res := v.Validate(context.Background(), db)
		require.NotEmpty(t, res.Errors, "a missing cluster is an error in its own right")
		requireNoEffectWarning(t, res.Warnings, "spec.initialData.secretRef", instead)
	})
}

func TestCloudStorageNoEffectWarnings(t *testing.T) {
	storage := func(identity *neo4jv1beta1.CloudIdentity) *neo4jv1beta1.StorageLocation {
		return &neo4jv1beta1.StorageLocation{
			Type: "s3", Bucket: "b",
			Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", Identity: identity},
		}
	}
	path := field.NewPath("spec", "storage")

	t.Run("nothing set", func(t *testing.T) {
		assert.Empty(t, CloudStorageNoEffectWarnings(nil, path, "sa"))
		assert.Empty(t, CloudStorageNoEffectWarnings(&neo4jv1beta1.StorageLocation{Type: "pvc"}, path, "sa"))
		assert.Empty(t, CloudStorageNoEffectWarnings(storage(nil), path, "sa"))
		assert.Empty(t, CloudStorageNoEffectWarnings(storage(&neo4jv1beta1.CloudIdentity{Provider: "aws"}), path, "sa"))
	})

	t.Run("serviceAccount", func(t *testing.T) {
		got := CloudStorageNoEffectWarnings(storage(&neo4jv1beta1.CloudIdentity{
			Provider: "aws", ServiceAccount: "my-sa",
		}), path, "neo4j-backup-sa")
		requireNoEffectWarning(t, got, "spec.storage.cloud.identity.serviceAccount", "neo4j-backup-sa")
		assert.Contains(t, got[0], "spec.storage.cloud.identity.autoCreate.annotations")
	})

	t.Run("autoCreate.enabled false is the only value that says anything", func(t *testing.T) {
		got := CloudStorageNoEffectWarnings(storage(&neo4jv1beta1.CloudIdentity{
			Provider: "aws", AutoCreate: &neo4jv1beta1.AutoCreateSpec{Enabled: false},
		}), path, "neo4j-backup-sa")
		requireNoEffectWarning(t, got, "spec.storage.cloud.identity.autoCreate.enabled", "autoCreate.annotations")

		// enabled defaults to true in the schema, so true cannot be told from "not
		// set" — and it is what the operator does anyway. The documented IRSA
		// setup (annotations, enabled left at its default) must stay silent.
		got = CloudStorageNoEffectWarnings(storage(&neo4jv1beta1.CloudIdentity{
			Provider: "aws",
			AutoCreate: &neo4jv1beta1.AutoCreateSpec{
				Enabled:     true,
				Annotations: map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/r"},
			},
		}), path, "neo4j-backup-sa")
		assert.Empty(t, got)
	})
}

func TestBackupValidator_NoEffectWarnings(t *testing.T) {
	v := NewBackupValidator()
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "test-cluster",
			AllDatabases: true,
			Storage: neo4jv1beta1.StorageLocation{
				Type: "s3", Bucket: "test-bucket",
				Cloud: &neo4jv1beta1.CloudBlock{
					Provider: "aws",
					Identity: &neo4jv1beta1.CloudIdentity{
						Provider: "aws", ServiceAccount: "my-sa",
						AutoCreate: &neo4jv1beta1.AutoCreateSpec{Enabled: false},
					},
				},
			},
		},
	}

	assert.Empty(t, v.Validate(backup), "the backup must still be accepted")
	warnings := v.NoEffectWarnings(backup)
	requireNoEffectWarning(t, warnings, "spec.storage.cloud.identity.serviceAccount", "neo4j-backup-sa")
	requireNoEffectWarning(t, warnings, "spec.storage.cloud.identity.autoCreate.enabled", "neo4j-backup-sa")

	backup.Spec.Storage.Cloud.Identity = &neo4jv1beta1.CloudIdentity{Provider: "aws"}
	assert.Empty(t, v.NoEffectWarnings(backup))
}

func TestRestoreNoEffectWarnings(t *testing.T) {
	identity := &neo4jv1beta1.CloudIdentity{Provider: "gcp", ServiceAccount: "my-sa"}
	cloud := func() *neo4jv1beta1.StorageLocation {
		return &neo4jv1beta1.StorageLocation{
			Type: "gcs", Bucket: "b",
			Cloud: &neo4jv1beta1.CloudBlock{Provider: "gcp", Identity: identity},
		}
	}

	assert.Empty(t, RestoreNoEffectWarnings(&neo4jv1beta1.Neo4jRestore{}), "an empty restore gets none")

	restore := &neo4jv1beta1.Neo4jRestore{
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef: "prod",
			Source: neo4jv1beta1.RestoreSource{
				Type:    "storage",
				Storage: cloud(),
				PITR: &neo4jv1beta1.PITRConfig{
					LogStorage: cloud(),
					BaseBackup: &neo4jv1beta1.BaseBackupSource{Type: "storage", Storage: cloud()},
				},
			},
		},
	}
	warnings := RestoreNoEffectWarnings(restore)
	requireNoEffectWarning(t, warnings, "spec.source.storage.cloud.identity.serviceAccount", "neo4j-restore-sa")
	requireNoEffectWarning(t, warnings, "spec.source.pitr.logStorage.cloud.identity.serviceAccount", "neo4j-restore-sa")
	requireNoEffectWarning(t, warnings, "spec.source.pitr.baseBackup.storage.cloud.identity.serviceAccount", "neo4j-restore-sa")
	assert.Len(t, warnings, 3)
}

func TestAuraInstanceNoEffectWarnings(t *testing.T) {
	inst := func(format string) *neo4jv1beta1.AuraInstance {
		return &neo4jv1beta1.AuraInstance{Spec: neo4jv1beta1.AuraInstanceSpec{ConnectionSecretFormat: format}}
	}
	for _, format := range []string{"", "neo4j-driver", "aura-dotenv", "jdbc", "servicebinding"} {
		assert.Empty(t, AuraInstanceNoEffectWarnings(inst(format)), "format %q is real", format)
	}
	requireNoEffectWarning(t, AuraInstanceNoEffectWarnings(inst("custom")),
		"spec.connectionSecretFormat", "neo4j-driver")
}
