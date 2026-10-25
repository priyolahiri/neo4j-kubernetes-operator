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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// spec.auraFleetManagement adds fleet-management to NEO4J_PLUGINS, and the
// entrypoints of 5.26.0 to 5.26.10 and 2025.01 to 2025.07 do not list it: they
// exit on it, and the servers crash-loop (#563's table, #573). The cluster and
// standalone validators had no version check (operations.md id 191).
func TestFleetManagement_IsRefusedOnAnImageWhoseEntrypointLacksIt(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	clusterValidator := NewClusterValidator(fake.NewClientBuilder().WithScheme(scheme).Build())
	standaloneValidator := NewStandaloneValidator()
	fleet := &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}

	clusterErrs := func(tag string, spec *neo4jv1beta1.AuraFleetManagementSpec) field.ErrorList {
		c := validClusterForWarnings()
		c.Spec.Image.Tag = tag
		c.Spec.AuraFleetManagement = spec
		return clusterValidator.ValidateCreateWithWarnings(context.Background(), c).Errors
	}
	standaloneErrs := func(tag string, spec *neo4jv1beta1.AuraFleetManagementSpec) field.ErrorList {
		s := validStandalone()
		s.Spec.Image.Tag = tag
		s.Spec.AuraFleetManagement = spec
		return standaloneValidator.ValidateCreate(s)
	}

	refused := []string{"5.26.0-enterprise", "5.26.10-enterprise", "2025.01.0-enterprise", "2025.07.2-enterprise", "2025.01-enterprise"}
	accepted := []string{"5.26.11-enterprise", "5.26.32-enterprise", "5.26-enterprise", "2025.08.0-enterprise", "2026.08.1-enterprise", "2026.08-enterprise"}

	for name, errsOf := range map[string]func(string, *neo4jv1beta1.AuraFleetManagementSpec) field.ErrorList{
		"cluster": clusterErrs, "standalone": standaloneErrs,
	} {
		for _, tag := range refused {
			t.Run(name+" refuses "+tag, func(t *testing.T) {
				errs := fleetErrors(errsOf(tag, fleet))
				require.Len(t, errs, 1, "fleet management on %s", tag)
				assert.Equal(t, "spec.auraFleetManagement.enabled", errs[0].Field)
				assert.Contains(t, errs[0].Detail, "5.26.11 or a later 5.26, or 2025.08.0 or later",
					"the message names the first releases that install it")
				assert.Contains(t, errs[0].Detail, "neo4j:"+tag)
			})
			t.Run(name+" leaves "+tag+" alone without fleet management", func(t *testing.T) {
				assert.Empty(t, fleetErrors(errsOf(tag, nil)))
				assert.Empty(t, fleetErrors(errsOf(tag, &neo4jv1beta1.AuraFleetManagementSpec{Enabled: false})))
			})
		}
		for _, tag := range accepted {
			t.Run(name+" accepts "+tag, func(t *testing.T) {
				assert.Empty(t, fleetErrors(errsOf(tag, fleet)))
			})
		}
	}
}

// fleetErrors keeps the errors about spec.auraFleetManagement.
func fleetErrors(errs field.ErrorList) field.ErrorList {
	var out field.ErrorList
	for _, e := range errs {
		if strings.HasPrefix(e.Field, "spec.auraFleetManagement") {
			out = append(out, e)
		}
	}
	return out
}
