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

// Unknown-field detection.
//
// A misspelled field is the most common manifest error there is, and until
// this existed `validate` could not see one: it decoded leniently, so an
// unrecognised key was dropped in silence and the document reported clean.
// `spec.auth.secretRef` (for `adminSecret`) passed with "0 error(s)" and was
// then refused by the API server with a strict-decoding error — in the tool
// whose whole premise is moving spec errors from after-apply to before.
//
// Why not just yaml.UnmarshalStrict: it aborts the document on the FIRST
// unknown key, reports only the leaf name ("secretRef", not
// "spec.auth.secretRef"), and its failure is a decode error rather than a
// validation finding — so it would exit 2 like an unreadable file instead of
// 1, and would break the "every error, not the first" contract that #354
// established. Walking the document against the type's own json tags gives
// every unknown field, each with the path a user can actually go and fix.

import (
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"
)

// unknownFieldPaths returns the dotted path of every key in doc that the type
// of obj has no field for, sorted. obj must be a pointer to a struct.
//
// Fields under metadata are not walked: it is an ObjectMeta owned by
// Kubernetes, the API server validates it, and reflecting over it would report
// perfectly legal keys as unknown.
func unknownFieldPaths(doc []byte, obj any) []string {
	var raw map[string]any
	if err := yaml.Unmarshal(doc, &raw); err != nil || raw == nil {
		// Undecodable YAML is reported by the caller's own decode; nothing to
		// add here, and guessing would double up the message.
		return nil
	}

	t := reflect.TypeOf(obj)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}

	var out []string
	walkUnknown(raw, t, "", &out)
	sort.Strings(out)
	return out
}

// skipWalk are keys whose contents belong to Kubernetes rather than to this
// operator's spec, so their inner fields are never ours to judge.
var skipWalk = map[string]bool{
	"metadata": true, "status": true,
}

func walkUnknown(node map[string]any, t reflect.Type, prefix string, out *[]string) {
	known := jsonFields(t)
	for key, val := range node {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		ft, ok := known[key]
		if !ok {
			*out = append(*out, path)
			continue
		}
		if skipWalk[key] && prefix == "" {
			continue
		}
		walkValue(val, ft, path, out)
	}
}

var jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()

// walkValue descends into one decoded value against the Go type it decodes
// into. It recurses only where both sides still have named structure:
//
//   - a JSON object against a struct   -> check its keys (walkUnknown)
//   - a JSON object against a map      -> the KEYS are user data and are never
//     judged (spec.config, labels, driverSettings), but the VALUES may still be
//     structs, so they are walked
//   - a JSON array against a slice     -> every element is walked against the
//     element type, reported as path[i]. This is what catches a typo inside
//     spec.topology.serverRoles[0] or spec.env[0] (corev1.EnvVar); []string and
//     []int elements are scalars and have nothing to walk.
//
// Types that decode themselves (resource.Quantity, intstr.IntOrString,
// metav1.Time, runtime.RawExtension, apiextensions JSON, ...) own their wire
// shape: reflecting over their Go fields would report perfectly legal keys as
// unknown, so they are opaque.
func walkValue(val any, ft reflect.Type, path string, out *[]string) {
	for ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
	}
	if reflect.PointerTo(ft).Implements(jsonUnmarshalerType) {
		return
	}
	switch v := val.(type) {
	case map[string]any:
		switch ft.Kind() {
		case reflect.Struct:
			walkUnknown(v, ft, path, out)
		case reflect.Map:
			for k, mv := range v {
				walkValue(mv, ft.Elem(), path+"["+k+"]", out)
			}
		}
	case []any:
		if ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array {
			for i, item := range v {
				walkValue(item, ft.Elem(), path+"["+strconv.Itoa(i)+"]", out)
			}
		}
	}
}

// jsonFields maps a struct's serialised names to their types, following
// embedded structs so TypeMeta's apiVersion/kind are recognised.
func jsonFields(t reflect.Type) map[string]reflect.Type {
	out := map[string]reflect.Type{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if f.Anonymous && (name == "" || strings.Contains(tag, "inline")) {
			et := f.Type
			for et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct {
				for k, v := range jsonFields(et) {
					out[k] = v
				}
			}
			continue
		}
		if name == "" || name == "-" {
			continue
		}
		out[name] = f.Type
	}
	return out
}
