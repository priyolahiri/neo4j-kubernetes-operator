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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// A StatefulSet's volume claim templates cannot change, and neither can a
// PVC's StorageClass, so an edit of spec.storage.className changed nothing for
// a running deployment and nothing said so (#532). It is an advisory
// ValidationWarning, announced when it starts to apply (not on every pass)
// and forgotten when the spec names the volumes' class again.
func TestReconcileStorageExpansion_WarnsWhileTheClassNamesNoVolumesClass(t *testing.T) {
	ctx := context.Background()

	t.Run("cluster", func(t *testing.T) {
		scheme := newStorageTestScheme()
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default", Generation: 1},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				Topology: neo4jv1beta1.TopologyConfiguration{Servers: 2},
				Storage:  neo4jv1beta1.StorageSpec{ClassName: "fast", Size: "10Gi"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster,
			makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true),
			makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 1, true)).Build()
		rec := record.NewFakeRecorder(100)
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}

		for range repeatPasses {
			_, err := r.reconcileStorageExpansion(ctx, cluster)
			require.NoError(t, err)
		}
		warnings := eventsWithReason(drainEvents(rec), EventReasonValidationWarning)
		require.Len(t, warnings, 1, "announced once, not on every pass")
		assert.Contains(t, warnings[0], `spec.storage.className "fast" has no effect`)
		assert.Contains(t, warnings[0], `data-ec-server-0, data-ec-server-1 use StorageClass "standard"`)

		// The validator's own ValidationWarning set is a different scope: its
		// pass, here with nothing to say, does not forget this one.
		r.advisoryWarnings.record(rec, cluster, EventReasonValidationWarning, nil)
		_, err := r.reconcileStorageExpansion(ctx, cluster)
		require.NoError(t, err)
		assert.Empty(t, drainEvents(rec))

		// Back to the volumes' class: forgotten, so a later edit is news again.
		cluster.Spec.Storage.ClassName = "standard"
		_, err = r.reconcileStorageExpansion(ctx, cluster)
		require.NoError(t, err)
		cluster.Spec.Storage.ClassName = "fast"
		_, err = r.reconcileStorageExpansion(ctx, cluster)
		require.NoError(t, err)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonValidationWarning), 1)
	})

	t.Run("standalone", func(t *testing.T) {
		scheme := newStandaloneTestScheme()
		rec := record.NewFakeRecorder(100)
		r := newStandaloneTestReconciler(scheme, makeStandalonePVC("sa", "default", "standard", "10Gi", 0, true))
		r.Recorder = rec
		standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				AcceptLicenseAgreement: "eval",
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "fast", Size: "10Gi"},
			},
		}
		for range repeatPasses {
			_, err := r.reconcileStandaloneStorageExpansion(ctx, standalone)
			require.NoError(t, err)
		}
		warnings := eventsWithReason(drainEvents(rec), EventReasonValidationWarning)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], `neo4j-data-sa-0 uses StorageClass "standard"`)
	})

	// No volume yet (a deployment being created), the volumes' own class, and
	// an empty className (the cluster default) say nothing.
	t.Run("nothing to say", func(t *testing.T) {
		scheme := newStorageTestScheme()
		for _, tc := range []struct {
			className string
			objs      []client.Object
		}{
			{"fast", nil},
			{"standard", []client.Object{makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true)}},
			{"", []client.Object{makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true)}},
		} {
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					Storage: neo4jv1beta1.StorageSpec{ClassName: tc.className, Size: "10Gi"},
				},
			}
			rec := record.NewFakeRecorder(10)
			r := &Neo4jEnterpriseClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objs...).Build(), Scheme: scheme, Recorder: rec,
			}
			_, err := r.reconcileStorageExpansion(ctx, cluster)
			require.NoError(t, err)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonValidationWarning),
				"className %q with %d volume(s)", tc.className, len(tc.objs))
		}
	})
}

// Volumes of more than one class are each named with theirs.
func TestDataVolumeClasses(t *testing.T) {
	pvcs := []client.Object{
		makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true),
		makePVC("ec", "default", "ec-server", "data", "", "10Gi", 1, true),
	}
	r := &Neo4jEnterpriseClusterReconciler{Client: fake.NewClientBuilder().WithScheme(newStorageTestScheme()).WithObjects(pvcs...).Build()}
	found, err := r.findPVCsForStatefulSet(context.Background(), "default", "ec", "ec-server", "data")
	require.NoError(t, err)
	classes := dataVolumeClasses(found)
	assert.Equal(t, map[string]string{"data-ec-server-0": "standard", "data-ec-server-1": ""}, classes)
	w := validation.StorageClassChangeWarning("fast", classes)
	assert.True(t, strings.Contains(w, `(data-ec-server-1 uses no StorageClass; data-ec-server-0 uses StorageClass "standard")`), w)
}
