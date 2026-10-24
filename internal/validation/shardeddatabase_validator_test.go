package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// shardedValidatorClient is a fake API server holding objs; getErr, when set,
// fails every read of a Neo4jEnterpriseCluster with it.
func shardedValidatorClient(t *testing.T, getErr error, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	if getErr != nil {
		b = b.WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok {
					return getErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}
	return b.Build()
}

// brokenShardedDB breaks three spec rules that need no cluster: a reserved
// name, no property shards and Cypher 5.
func brokenShardedDB(clusterRef string) *neo4jv1beta1.Neo4jShardedDatabase {
	return &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "products", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
			ClusterRef:            clusterRef,
			Name:                  "system",
			DefaultCypherLanguage: "5",
			PropertySharding: neo4jv1beta1.PropertyShardingConfiguration{
				PropertyShards:        0,
				GraphShard:            neo4jv1beta1.DatabaseTopology{Primaries: 1},
				PropertyShardTopology: neo4jv1beta1.PropertyShardTopology{Replicas: 1},
			},
		},
	}
}

// A cluster the validator could not read ended validation with the read error
// alone and dropped the spec errors already found: the user fixed one, applied
// again, and only then saw the rest (#532). Every spec error is reported
// whatever happens to the read.
func TestShardedDatabaseValidator_ReportsEverySpecErrorWhateverTheClusterRead(t *testing.T) {
	ctx := context.Background()
	specErrors := []string{"spec.name", "spec.propertySharding.propertyShards", "spec.defaultCypherLanguage"}

	t.Run("cluster not found: the spec errors, and a wait for the cluster", func(t *testing.T) {
		v := NewShardedDatabaseValidator(shardedValidatorClient(t, nil))
		err := v.ValidateShardedDatabase(ctx, brokenShardedDB("sc"))
		require.Error(t, err)
		for _, path := range specErrors {
			assert.Contains(t, err.Error(), path)
		}
		out := v.Validate(ctx, brokenShardedDB("sc"))
		assert.Len(t, out.Errors, len(specErrors))
		assert.Len(t, out.Pending, 1)
		assert.NoError(t, out.ClusterReadErr)
	})

	t.Run("cluster read fails: the spec errors and the read error", func(t *testing.T) {
		v := NewShardedDatabaseValidator(shardedValidatorClient(t, apierrors.NewServiceUnavailable("etcd is unhappy")))
		err := v.ValidateShardedDatabase(ctx, brokenShardedDB("sc"))
		require.Error(t, err)
		for _, path := range specErrors {
			assert.Contains(t, err.Error(), path)
		}
		assert.Contains(t, err.Error(), "etcd is unhappy")
	})

	t.Run("no clusterRef: required, with the others, and nothing is read", func(t *testing.T) {
		v := NewShardedDatabaseValidator(shardedValidatorClient(t, apierrors.NewServiceUnavailable("must not be read")))
		out := v.Validate(ctx, brokenShardedDB(""))
		assert.Len(t, out.Errors, len(specErrors)+1)
		assert.NoError(t, out.ClusterReadErr)
	})
}

// A cluster that does not exist yet is a wait for the reconciler (Pending,
// ClusterNotFound: operations.md id 145), so it is not a validation error. As
// one, the reconciler refused the database as Failed and never reached the
// wait.
func TestShardedDatabaseValidator_AMissingClusterIsNotAnError(t *testing.T) {
	ctx := context.Background()
	valid := brokenShardedDB("sc")
	valid.Spec.Name = "products"
	valid.Spec.DefaultCypherLanguage = "25"
	valid.Spec.PropertySharding.PropertyShards = 2

	v := NewShardedDatabaseValidator(shardedValidatorClient(t, nil))
	assert.NoError(t, v.ValidateShardedDatabase(ctx, valid))

	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "sc", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3}},
	}
	v = NewShardedDatabaseValidator(shardedValidatorClient(t, nil, cluster))
	err := v.ValidateShardedDatabase(ctx, valid)
	require.Error(t, err, "once the cluster is there, the rules that need it run")
	assert.Contains(t, err.Error(), "property sharding enabled")
}

func TestValidateShardedDatabaseName(t *testing.T) {
	v := &ShardedDatabaseValidator{}

	tests := []struct {
		name      string
		dbName    string
		expectErr bool
	}{
		{"valid simple name", "mydb", false},
		{"valid with hyphen and underscore", "my-db_123", false},
		{"empty name", "", true},
		{"reserved system", "system", true},
		{"reserved System case", "System", true},
		{"reserved neo4j", "neo4j", true},
		{"reserved NEO4J case", "NEO4J", true},
		{"invalid char @", "my@db", true},
		{"invalid char space", "my db", true},
		{"max length 63", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"too long 64", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.validateDatabaseName(tt.dbName)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCypherLanguage(t *testing.T) {
	v := &ShardedDatabaseValidator{}

	t.Run("valid 25", func(t *testing.T) {
		result := &ShardedDatabaseValidationResult{}
		db := &neo4jv1beta1.Neo4jShardedDatabase{}
		db.Spec.DefaultCypherLanguage = "25"
		v.validateCypherLanguage(db, result)
		assert.Empty(t, result.Errors)
	})

	t.Run("invalid 5", func(t *testing.T) {
		result := &ShardedDatabaseValidationResult{}
		db := &neo4jv1beta1.Neo4jShardedDatabase{}
		db.Spec.DefaultCypherLanguage = "5"
		v.validateCypherLanguage(db, result)
		assert.NotEmpty(t, result.Errors)
	})

	t.Run("empty", func(t *testing.T) {
		result := &ShardedDatabaseValidationResult{}
		db := &neo4jv1beta1.Neo4jShardedDatabase{}
		db.Spec.DefaultCypherLanguage = ""
		v.validateCypherLanguage(db, result)
		assert.NotEmpty(t, result.Errors)
	})
}

// TestValidateReplaceExisting pins the Phase 2c safety gates around the
// destructive drop-and-recreate path. replaceExisting=true must require:
//
//  1. force=true (mirrors Neo4jRestore safety semantics),
//  2. ifNotExists=false (the two contradict each other),
//  3. a seed source (seedURI / seedURIs / seedBackupRef) — recreating
//     without data would leave the database empty.
//
// validatePropertyShardingConfig hosts the check; the test invokes it
// directly to keep the surface narrow.
func TestValidateReplaceExisting(t *testing.T) {
	v := &ShardedDatabaseValidator{}
	baseSpec := func() neo4jv1beta1.Neo4jShardedDatabaseSpec {
		return neo4jv1beta1.Neo4jShardedDatabaseSpec{
			ClusterRef:            "my-cluster",
			Name:                  "products",
			DefaultCypherLanguage: "25",
			PropertySharding: neo4jv1beta1.PropertyShardingConfiguration{
				PropertyShards: 2,
				GraphShard:     neo4jv1beta1.DatabaseTopology{Primaries: 1},
				PropertyShardTopology: neo4jv1beta1.PropertyShardTopology{
					Replicas: 1,
				},
			},
		}
	}

	t.Run("replaceExisting without force is rejected", func(t *testing.T) {
		spec := baseSpec()
		spec.ReplaceExisting = true
		spec.Force = false
		spec.SeedBackupRef = "products-backup"
		result := &ShardedDatabaseValidationResult{}
		v.validatePropertyShardingConfig(&neo4jv1beta1.Neo4jShardedDatabase{Spec: spec}, result)
		assert.NotEmpty(t, result.Errors, "expected error for replaceExisting=true without force=true")
	})

	t.Run("replaceExisting with ifNotExists is rejected", func(t *testing.T) {
		spec := baseSpec()
		spec.ReplaceExisting = true
		spec.Force = true
		ifn := true
		spec.IfNotExists = &ifn
		spec.SeedBackupRef = "products-backup"
		result := &ShardedDatabaseValidationResult{}
		v.validatePropertyShardingConfig(&neo4jv1beta1.Neo4jShardedDatabase{Spec: spec}, result)
		errs := 0
		for _, e := range result.Errors {
			if e.Field == "spec.ifNotExists" {
				errs++
			}
		}
		assert.Equal(t, 1, errs, "expected ifNotExists mutex error; got %v", result.Errors)
	})

	t.Run("replaceExisting without seed source is rejected", func(t *testing.T) {
		spec := baseSpec()
		spec.ReplaceExisting = true
		spec.Force = true
		// no seed set
		result := &ShardedDatabaseValidationResult{}
		v.validatePropertyShardingConfig(&neo4jv1beta1.Neo4jShardedDatabase{Spec: spec}, result)
		errs := 0
		for _, e := range result.Errors {
			if e.Field == "spec.replaceExisting" {
				errs++
			}
		}
		assert.GreaterOrEqual(t, errs, 1, "expected error pointing at replaceExisting for missing seed source")
	})

	t.Run("valid: replaceExisting + force + seedBackupRef", func(t *testing.T) {
		spec := baseSpec()
		spec.ReplaceExisting = true
		spec.Force = true
		ifn := false
		spec.IfNotExists = &ifn
		spec.SeedBackupRef = "products-backup"
		result := &ShardedDatabaseValidationResult{}
		v.validatePropertyShardingConfig(&neo4jv1beta1.Neo4jShardedDatabase{Spec: spec}, result)
		assert.Empty(t, result.Errors, "expected no errors for the canonical destructive-restore config: got %v", result.Errors)
	})
}

func TestValidateBasicFields(t *testing.T) {
	v := &ShardedDatabaseValidator{}

	t.Run("missing clusterRef", func(t *testing.T) {
		result := &ShardedDatabaseValidationResult{}
		db := &neo4jv1beta1.Neo4jShardedDatabase{
			Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
				Name: "mydb",
			},
		}
		v.validateBasicFields(db, result)
		assert.NotEmpty(t, result.Errors)
	})

	t.Run("missing name", func(t *testing.T) {
		result := &ShardedDatabaseValidationResult{}
		db := &neo4jv1beta1.Neo4jShardedDatabase{
			Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
				ClusterRef: "my-cluster",
			},
		}
		v.validateBasicFields(db, result)
		assert.NotEmpty(t, result.Errors)
	})

	t.Run("valid basic fields", func(t *testing.T) {
		result := &ShardedDatabaseValidationResult{}
		db := &neo4jv1beta1.Neo4jShardedDatabase{
			Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
				ClusterRef: "my-cluster",
				Name:       "valid-db",
			},
		}
		v.validateBasicFields(db, result)
		assert.Empty(t, result.Errors)
	})
}
