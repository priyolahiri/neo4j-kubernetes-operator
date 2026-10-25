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

package resources

import "strings"

// maxLabelValueLength is Kubernetes' limit on a label value.
const maxLabelValueLength = 63

// VersionLabelValue is the app.kubernetes.io/version label value for an image
// tag: the tag without a digest it carries, cut to a valid label value. A
// label value is at most 63 characters of letters, digits, '-', '_' and '.',
// beginning and ending with a letter or digit, so the raw tag
// "2026.08.1-enterprise@sha256:…" — which spec.image.tag accepts — made the
// API server refuse every object labelled with it, and the cluster went
// Failed on its first pass (#562 walk). A tag is otherwise made of label
// characters already; anything else is dropped, as is what a cut leaves at
// either end that a label may not begin or end with. "" when nothing is left.
func VersionLabelValue(tag string) string {
	if at := strings.Index(tag, "@"); at >= 0 {
		tag = tag[:at]
	}
	var b strings.Builder
	for _, c := range tag {
		if isLabelAlnum(c) || c == '-' || c == '_' || c == '.' {
			b.WriteRune(c)
		}
	}
	value := b.String()
	if len(value) > maxLabelValueLength {
		value = value[:maxLabelValueLength]
	}
	return strings.TrimFunc(value, func(c rune) bool { return !isLabelAlnum(c) })
}

func isLabelAlnum(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
