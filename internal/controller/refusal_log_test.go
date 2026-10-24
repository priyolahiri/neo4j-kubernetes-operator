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

// A spec the operator refuses is a status the user has to fix, not a
// controller error. The cluster and the standalone logged an ERROR on every
// pass while it stayed refused — every 6–8 seconds, 28 lines in about four
// minutes on the #562 walk. It is logged at info when the refusal is
// announced, at V(1) on the passes in between (knowledge rule 136).

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func TestReconcile_ARefusedSpecIsLoggedOnceNotAsAnError(t *testing.T) {
	for _, tc := range validationFailedCases() {
		if tc.name != "Neo4jEnterpriseCluster" && tc.name != "Neo4jEnterpriseStandalone" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(tc.obj).
				WithStatusSubresource(tc.obj).Build()
			r := tc.newReconciler(c, record.NewFakeRecorder(100))
			for range 3 {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc.obj)})
				require.NoError(t, err)
			}
			assert.Empty(t, sink.errors(), "a refusal is not a controller error")
			assert.Len(t, sink.mentioning(0, "validation failed"), 1, "logged when it is announced")
			assert.Len(t, sink.mentioning(1, "validation failed"), 2, "and at V(1) on the passes in between")
		})
	}
}
