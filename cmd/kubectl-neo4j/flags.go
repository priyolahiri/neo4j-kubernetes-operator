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

import (
	"flag"
	"strings"
)

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

// parseFlags parses args with flags allowed ANYWHERE, including after a
// positional argument.
//
// The standard library's flag package stops at the first non-flag argument, so
// `kubectl neo4j diagnose Kind/name -n prod` parsed zero flags and silently
// looked in the default namespace — then reported "not found", which reads as
// "your resource does not exist" rather than "your flag was dropped". kubectl
// itself accepts flags in any position, and every one of this CLI's own
// documented examples puts -n after the positional, so matching kubectl is the
// only defensible behaviour.
//
// Rather than take a dependency for this, permute: hoist flags (and the values
// they consume) ahead of the positionals, then hand the result to flag.Parse.
func parseFlags(fs *flag.FlagSet, args []string) error {
	return fs.Parse(permuteArgs(fs, args))
}

// permuteArgs reorders args so every flag precedes every positional, preserving
// relative order within each group. Everything after a bare "--" is positional,
// as it is everywhere else in Unix.
func permuteArgs(fs *flag.FlagSet, args []string) []string {
	flags := make([]string, 0, len(args))
	positional := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if len(arg) < 2 || arg[0] != '-' {
			positional = append(positional, arg)
			continue
		}

		flags = append(flags, arg)
		// "--flag=value" carries its own value; "--flag value" consumes the
		// next argument unless the flag is boolean. An unknown flag is passed
		// through untouched so flag.Parse reports it rather than this code
		// guessing what it meant.
		name := strings.TrimLeft(arg, "-")
		if strings.Contains(name, "=") {
			continue
		}
		f := fs.Lookup(name)
		if f == nil || isBoolFlag(f) {
			continue
		}
		if i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}

	// Terminate the flag section explicitly. Without this, a positional that
	// merely LOOKS like a flag — anything after a user's own "--" — would be
	// re-parsed as one once it had been hoisted behind the real flags.
	flags = append(flags, "--")
	return append(flags, positional...)
}

// isBoolFlag reports whether f is a boolean flag, which takes no value. This is
// the same duck-typed check flag.Parse itself uses.
func isBoolFlag(f *flag.Flag) bool {
	bf, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && bf.IsBoolFlag()
}
