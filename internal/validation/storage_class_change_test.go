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
	"testing"

	"github.com/stretchr/testify/assert"
)

// spec.storage.className is read once, when a volume is created. A change
// afterwards reaches no volume, so it is said whenever the spec names a class
// a data volume does not use (#532), and only then.
func TestStorageClassChangeWarning(t *testing.T) {
	volumes := map[string]string{"data-c-server-0": "standard", "data-c-server-1": "standard"}

	w := StorageClassChangeWarning("fast", volumes)
	assert.Contains(t, w, `spec.storage.className "fast" has no effect on the data volumes that already exist`)
	assert.Contains(t, w, `data-c-server-0, data-c-server-1 use StorageClass "standard"`)
	assert.Contains(t, w, "back it up and restore it")

	assert.Empty(t, StorageClassChangeWarning("standard", volumes), "the class the volumes use")
	assert.Empty(t, StorageClassChangeWarning("", volumes), "empty: the cluster default, which a volume names once created")
	assert.Empty(t, StorageClassChangeWarning("fast", nil), "no volume yet: the class applies when they are created")

	mixed := StorageClassChangeWarning("fast", map[string]string{"data-c-server-0": "fast", "data-c-server-1": ""})
	assert.Contains(t, mixed, "data-c-server-1 uses no StorageClass")
	assert.NotContains(t, mixed, "data-c-server-0", "a volume of the named class is not listed")
}
