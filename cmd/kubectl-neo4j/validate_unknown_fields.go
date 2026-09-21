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
	"reflect"
	"sort"
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
		// Recurse only where both sides are still a struct with named keys.
		child, isMap := val.(map[string]any)
		if !isMap {
			continue
		}
		et := ft
		for et.Kind() == reflect.Pointer {
			et = et.Elem()
		}
		if et.Kind() != reflect.Struct {
			// A map[string]X (spec.config, driverSettings, labels) accepts any
			// key by design — walking into it would report user data as an
			// unknown field.
			continue
		}
		walkUnknown(child, et, path, out)
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
