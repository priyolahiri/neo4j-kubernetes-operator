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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// routeHostGeneratedAnnotation is what OpenShift's API server sets on a Route
// whose host it generated.
const routeHostGeneratedAnnotation = "openshift.io/host.generated"

// routeRenderedSpecFields and routeRenderedTLSFields are the fields of a
// Route's spec the operator renders (resources.buildRoute); host is rendered
// only when the user names one.
var (
	routeRenderedSpecFields = []string{"to", "port", "path"}
	routeRenderedTLSFields  = []string{"termination", "insecureEdgeTerminationPolicy"}
)

// applyDesiredRoute is the mutate step of every Route's CreateOrUpdate (the
// cluster's and the standalone's Route, and their MCP Routes). It writes onto
// the live Route what the operator renders — its labels, its annotations,
// spec.to, spec.port, spec.path, spec.host when the user names one, and the
// termination fields of spec.tls — and keeps everything else (operations.md
// id 148).
//
// It used to replace spec wholesale. OpenShift's API server fills in fields
// the operator does not render: it defaults spec.wildcardPolicy to None, and
// generates spec.host for a Route that names none (marking it with
// openshift.io/host.generated) and ignores an update that clears it. Each pass
// then saw a Route that differed from the one it wanted and sent an update the
// API server turned back into the same object (#536). Keeping them leaves a
// Route that is already right untouched. A certificate added to the Route by
// hand (spec.tls.certificate and key, or spec.tls.externalCertificate), which
// the operator does not render, is kept the same way.
func applyDesiredRoute(live, desired *unstructured.Unstructured) {
	live.SetLabels(desired.GetLabels())
	annotations := desired.GetAnnotations()
	if v, ok := live.GetAnnotations()[routeHostGeneratedAnnotation]; ok {
		if _, rendered := annotations[routeHostGeneratedAnnotation]; !rendered {
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[routeHostGeneratedAnnotation] = v
		}
	}
	live.SetAnnotations(annotations)

	// NestedMap returns deep copies, so neither object shares a map with the
	// other afterwards.
	want, _, _ := unstructured.NestedMap(desired.Object, "spec")
	spec, _, _ := unstructured.NestedMap(live.Object, "spec")
	if spec == nil {
		spec = map[string]any{}
	}
	for _, field := range routeRenderedSpecFields {
		if v, ok := want[field]; ok {
			spec[field] = v
		} else {
			delete(spec, field)
		}
	}
	if host, ok := want["host"]; ok {
		spec["host"] = host
	}
	if wantTLS, ok := want["tls"].(map[string]any); ok {
		tls, _ := spec["tls"].(map[string]any)
		if tls == nil {
			tls = map[string]any{}
		}
		for _, field := range routeRenderedTLSFields {
			if v, ok := wantTLS[field]; ok {
				tls[field] = v
			} else {
				delete(tls, field)
			}
		}
		spec["tls"] = tls
	} else {
		// No termination asked for: the Route is plain HTTP, and a certificate
		// left on it would have nothing to terminate.
		delete(spec, "tls")
	}
	live.Object["spec"] = spec
}
