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
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The operator never Updates one of its custom resources: it patches what it
// changed (operations.md id 150). Child objects it renders (StatefulSets,
// Services, ConfigMaps) are its own and are not covered by this.
//
// An Update sends the whole object as the Go types encode it, and that is not
// the object the user applied:
//   - the reconcile's copy of a cluster carries the defaults ApplyDefaults
//     filled in, and an Update wrote every one of them into the stored spec;
//   - a bool the CRD defaults to true that the user set to false is omitted
//     (omitempty), and the API server defaults it to true again
//     (Neo4jRole spec.enforcePrivileges, Neo4jPlugin spec.enabled, a cluster's
//     spec.monitoring.enabled, ...); a false with no default is dropped;
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
//
// The write is a JSON patch that tests the finalizer is at the index this copy
// has it and removes it there, not the resourceVersion lock of patchObject. A
// deletion's copy is often older than the object: the pass wrote status first
// (a cluster's teardown, a wait of id 178), and a status write moves the
// resourceVersion without touching metadata.finalizers, so the locked patch
// was a Conflict — logged at ERROR by the caller and as "Reconciler error",
// although the retry succeeded the same second. The test keeps what the lock
// protected: a merge patch without it would replace the whole list with this
// copy's and drop a finalizer another writer added since, while a removal at a
// tested index leaves every other entry. When the list moved under the copy
// (an entry before this one removed or inserted), the test fails, the object
// is read again and the removal retried, with backoff (knowledge rule 189).
//
// An object the API server no longer has is released already: a pass that
// read it before an earlier pass released the finalizer (the cache lags the
// deletion it allowed) finds nothing to patch. That is success, logged at
// V(1). Returned, the NotFound was logged at ERROR, by the caller and again
// by controller-runtime, on routine deletions of every kind (knowledge rule
// 189, as #572 did for the cluster).
func removeFinalizer(ctx context.Context, c client.Client, obj client.Object, finalizer string) error {
	reread := false
	err := retry.OnError(retry.DefaultBackoff, finalizerListMoved, func() error {
		if reread {
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				return err
			}
		}
		reread = true
		i := slices.Index(obj.GetFinalizers(), finalizer)
		if i < 0 {
			return nil // released already
		}
		patch, err := finalizerRemoval(i, finalizer)
		if err != nil {
			return err
		}
		return c.Patch(ctx, obj, patch)
	})
	if apierrors.IsNotFound(err) {
		log.FromContext(ctx).V(1).Info("Already deleted; its finalizer was released by an earlier pass",
			"kind", fmt.Sprintf("%T", obj), "name", obj.GetName(), "finalizer", finalizer)
		return nil
	}
	return err
}

// removeFinalizerAt is removeFinalizer for a reconciler whose release starts
// from the request rather than a copy (the Aura kinds): it reads the object
// at key into obj first. An object that is gone is released already.
func removeFinalizerAt(ctx context.Context, c client.Client, key client.ObjectKey, obj client.Object, finalizer string) error {
	if err := c.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			log.FromContext(ctx).V(1).Info("Already deleted; its finalizer was released by an earlier pass",
				"kind", fmt.Sprintf("%T", obj), "name", key.Name, "finalizer", finalizer)
			return nil
		}
		return err
	}
	return removeFinalizer(ctx, c, obj, finalizer)
}

// finalizerRemoval is the JSON patch that removes finalizer from index i of
// metadata.finalizers, if it is still there.
func finalizerRemoval(i int, finalizer string) (client.Patch, error) {
	path := fmt.Sprintf("/metadata/finalizers/%d", i)
	ops, err := json.Marshal([]map[string]string{
		{"op": "test", "path": path, "value": finalizer},
		{"op": "remove", "path": path},
	})
	if err != nil {
		return nil, err
	}
	return client.RawPatch(types.JSONPatchType, ops), nil
}

// finalizerListMoved reports a removal whose test failed: the finalizer is no
// longer at the index the copy had it. The API server answers a failed test
// with 422 (Invalid); the fake client returns the patch library's error.
func finalizerListMoved(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsConflict(err) || strings.Contains(err.Error(), "test failed")
}
