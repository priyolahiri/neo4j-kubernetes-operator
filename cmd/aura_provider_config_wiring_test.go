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
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/controller"
)

// fieldsOnlyManager answers the three calls a reconciler's construction makes;
// any other Manager method panics on the nil embedded interface.
type fieldsOnlyManager struct{ ctrl.Manager }

func (fieldsOnlyManager) GetClient() client.Client   { return nil }
func (fieldsOnlyManager) GetScheme() *runtime.Scheme { return runtime.NewScheme() }
func (fieldsOnlyManager) GetEventRecorderFor(string) record.EventRecorder {
	return record.NewFakeRecorder(1)
}

// The AuraProviderConfig reconciler validates its credentials again every
// RequeueAfter. Both registries wired it the 30s every other controller gets —
// a token exchange and a ListInstances call per config every 30s, against a
// trial account's 25 Aura API calls a minute — overriding the controller's own
// 10 minutes, for no reason anyone wrote down (operations.md id 184).
func TestAuraProviderConfig_IsWiredWithItsOwnRecheckInterval(t *testing.T) {
	mgr := fieldsOnlyManager{}

	factory, ok := devControllerRegistry(mgr)["auraproviderconfig"]
	if !ok {
		t.Fatal("dev mode registers no auraproviderconfig controller")
	}
	built, _ := factory()
	dev, ok := built.(*controller.AuraProviderConfigReconciler)
	if !ok {
		t.Fatalf("auraproviderconfig builds %T", built)
	}
	if dev.RequeueAfter != 0 {
		t.Errorf("dev mode wires RequeueAfter %v; leave it unset so the credentials are validated every 10 minutes", dev.RequeueAfter)
	}

	if prod := newAuraProviderConfigReconciler(mgr); prod.RequeueAfter != 0 {
		t.Errorf("production wires RequeueAfter %v; leave it unset so the credentials are validated every 10 minutes", prod.RequeueAfter)
	}
}
