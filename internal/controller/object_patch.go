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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// The operator never Updates a Neo4jEnterpriseCluster or a
// Neo4jEnterpriseStandalone: it patches what it changed (operations.md id 150).
//
// An Update sends the whole object as the Go types encode it, and that is not
// the object the user applied:
//   - the reconcile's copy of a cluster carries the defaults ApplyDefaults
//     filled in, and an Update wrote every one of them into the stored spec;
//   - a bool the CRD defaults to true that the user set to false is omitted
//     (omitempty), and the API server defaults it to true again
//     (spec.monitoring.enabled, spec.upgradeStrategy.autoPauseOnFailure, ...);
//   - a quantity is encoded in its canonical spelling ("0.5" becomes "500m").
//
// A JSON merge patch computed between two copies of the same object carries
// only what changed between them, so none of that reaches the API server.

// mergePatchSince is the patch that writes what changed on an object since
// base, a copy of it taken before the change. It carries base's
// resourceVersion, so a concurrent write is a conflict, as it is for an
// Update: a merge patch replaces a list whole (metadata.finalizers,
// spec.env), and without the lock a writer in between would lose its entry.
func mergePatchSince(base client.Object) client.Patch {
	return client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
}

// patchObject applies mutate to obj and writes only what mutate changed.
func patchObject(ctx context.Context, c client.Client, obj client.Object, mutate func()) error {
	base, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("cannot copy %T to patch it", obj)
	}
	mutate()
	return c.Patch(ctx, obj, mergePatchSince(base))
}

// addFinalizer adds finalizer to obj and writes metadata.finalizers alone.
func addFinalizer(ctx context.Context, c client.Client, obj client.Object, finalizer string) error {
	return patchObject(ctx, c, obj, func() { controllerutil.AddFinalizer(obj, finalizer) })
}

// removeFinalizer removes finalizer from obj and writes metadata.finalizers
// alone.
func removeFinalizer(ctx context.Context, c client.Client, obj client.Object, finalizer string) error {
	return patchObject(ctx, c, obj, func() { controllerutil.RemoveFinalizer(obj, finalizer) })
}
