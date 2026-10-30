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
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// logChildWrite logs the create-or-update of one of a deployment's child
// objects: at info when it created or changed the object, at V(1) when the
// object was already as rendered and nothing was written. Since a pass that
// changes nothing writes nothing (operations.md id 235), an idle standalone
// logged five "Successfully created or updated …" lines at info on every
// pass, about every six seconds, for writes that never happened (#624).
func logChildWrite(logger logr.Logger, op controllerutil.OperationResult, what, name string) {
	if op == controllerutil.OperationResultNone {
		logger.V(1).Info(what+" already as rendered; nothing written", "name", name)
		return
	}
	logger.Info("Successfully "+string(op)+" "+what, "name", name)
}
