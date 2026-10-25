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

import "github.com/go-logr/logr"

// logRefusal logs a spec the operator refuses — a validation failure, a
// missing StorageClass — the way its event is announced (knowledge rule 136):
// at info when the status write that records it changed the status (the
// refusal starts or its message changes), at V(1) on the passes in between.
// A refusal is a status the user has to fix, not a controller error: logged
// as an ERROR before the status write, it filled the log with one line per
// pass, every 6–8 seconds, for as long as the spec stayed refused (#562 walk).
// A lasting failure the operator retries on every pass and announces once
// through lastAnnounced — a fleet-management patch the StatefulSet cannot
// take — is logged the same way, announced being what lastAnnounced said.
func logRefusal(logger logr.Logger, announced bool, msg string, err error) {
	if announced {
		logger.Info(msg, "reason", err.Error())
		return
	}
	logger.V(1).Info(msg, "reason", err.Error())
}
