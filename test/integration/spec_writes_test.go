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

package integration_test

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The specs write the operator's CRs as a user's `kubectl patch` does: a merge
// patch of what the spec changed, never a whole-object Update
// (docs/knowledge/operations.md ids 150 and 205). An Update sends the object
// as the Go types encode it. A false the CRD defaults to true is left out and
// the API server defaults it back to true, so a spec that Updates a cluster to
// change one field goes on to test a cluster nobody applied.

// patchCR applies mutate to obj and writes only what mutate changed. Like
// `kubectl patch --type merge`, it carries no resourceVersion: the operator
// writes status, its finalizers and its annotations while a spec holds a copy,
// and a Conflict on those says nothing about the field the spec changes. A
// list in the patch replaces the stored one whole.
//
// A merge patch from Go cannot set a field the CRD defaults to true to false:
// the false is left out of the patch, as a removal, and the API server
// defaults it again. A spec that needs the false writes it as JSON
// (patchJSON).
func patchCR(ctx context.Context, obj client.Object, mutate func()) error {
	base, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("cannot copy %T to patch it", obj)
	}
	mutate()
	return k8sClient.Patch(ctx, obj, client.MergeFrom(base))
}

// patchJSON writes a JSON merge patch as written, so a false in it is stored.
func patchJSON(ctx context.Context, obj client.Object, patch string) error {
	return k8sClient.Patch(ctx, obj, client.RawPatch(types.MergePatchType, []byte(patch)))
}

// releaseFinalizers removes every finalizer from obj, whatever this copy holds
// and whatever was written since: the finalizer half of the MANDATORY
// AfterEach cleanup.
func releaseFinalizers(ctx context.Context, obj client.Object) error {
	return patchJSON(ctx, obj, `{"metadata":{"finalizers":null}}`)
}
