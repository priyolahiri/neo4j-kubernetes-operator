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
	"testing"

	"github.com/stretchr/testify/require"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// gatheredSeries is one series read back from the registry the operator
// actually serves on /metrics (controller-runtime's), so a test sees exactly
// what a scrape would: the collector must be registered, labelled as
// documented, and populated.
type gatheredSeries struct {
	found bool
	// value is the gauge value or counter total.
	value float64
	// count and sum are a histogram's sample count and sum.
	count uint64
	sum   float64
}

// gatherSeries returns the series of the named family whose labels include
// every pair in want. The registry is process-global, so tests choose names
// unique to themselves rather than resetting shared state.
func gatherSeries(t *testing.T, family string, want map[string]string) gatheredSeries {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != family {
			continue
		}
		for _, m := range fam.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			s := gatheredSeries{found: true}
			switch {
			case m.GetHistogram() != nil:
				s.count = m.GetHistogram().GetSampleCount()
				s.sum = m.GetHistogram().GetSampleSum()
			case m.GetCounter() != nil:
				s.value = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				s.value = m.GetGauge().GetValue()
			}
			return s
		}
	}
	return gatheredSeries{}
}
