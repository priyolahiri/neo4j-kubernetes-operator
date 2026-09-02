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

import "flag"

// namespaceFlag registers --namespace together with -n, the short form every
// kubectl user reaches for first.
//
// The standard library's flag package has no concept of a short alias, so the
// two names are registered separately against one variable. Before this
// existed, every command's usage text read "-n <namespace>" while only the long
// form actually parsed — the command printed its own help in response to the
// documented invocation, which is a particularly annoying way to be wrong.
func namespaceFlag(fs *flag.FlagSet, usage string) *string {
	var ns string
	fs.StringVar(&ns, "namespace", "", usage)
	fs.StringVar(&ns, "n", "", "Short form of --namespace")
	return &ns
}
