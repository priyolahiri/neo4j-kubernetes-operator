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

// ScaledWithoutRestart names the servers a scale adds, and only those the
// server StatefulSet does not run yet (operations.md id 230). On the v1.24.0
// journey (#619) every scale-down set back while the drain held the replicas
// announced "Adding servers 3-4 as system secondaries" and logged "Scaled …
// from 3 to 5", eight times in all, although servers 3 and 4 never stopped.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func TestScale_AnnouncesOnlyTheServersItAdds(t *testing.T) {
	cases := []struct {
		name     string
		replicas int32 // the server StatefulSet's, as the drain left it
		want     string
	}{
		{"a scale-up 3 → 5", 3, "Adding servers 3-4 as system secondaries; the running servers keep running"},
		{"a scale-down 5 → 3 set back while the drain held 5 replicas", 5, ""},
		{"a 5 → 3 → 4 → 5 with 4 replicas running", 4, "Adding server 4 as a system secondary; the running servers keep running"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			before := scaledCluster("s619", "2026.08.1-enterprise", 3)
			after := scaledCluster("s619", "2026.08.1-enterprise", 5)
			sts := serverSTS(after.Name, "default")
			sts.Spec.Replicas = ptr.To(tc.replicas)
			fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
				WithObjects(after, sts, resources.BuildConfigMapForEnterprise(before)).Build()
			rec := record.NewFakeRecorder(10)
			cm := NewConfigMapManager(fc).WithRecorder(rec)
			cm.LiveConfig = nil

			require.NoError(t, cm.ReconcileConfigMap(ctx, after))

			close(rec.Events)
			var got []string
			for e := range rec.Events {
				got = append(got, e)
			}
			if tc.want == "" {
				assert.Empty(t, got, "nothing is added, nothing is announced")
				return
			}
			require.Len(t, got, 1)
			assert.True(t, strings.HasSuffix(got[0], EventReasonScaledWithoutRestart+" "+tc.want), got[0])
		})
	}
}
