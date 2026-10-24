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

import "sync"

// lastAnnounced remembers, per key, what a Normal event that no status write
// records last said, so the event is emitted when what it reports changes and
// not on every pass (operations.md id 136). An event that reports a state the
// operator writes to status is announced from that write instead; this is for
// what the operator only computes — a topology placement, an image change it
// holds back. The memory is this process's: an operator restart announces each
// once more, which is fine. The zero value is ready to use and safe for
// clusters reconciled in parallel (id 129).
type lastAnnounced struct {
	said sync.Map // key → message
}

// changed records message under key and reports whether it differs from what
// was last recorded there, or nothing was.
func (l *lastAnnounced) changed(key, message string) bool {
	previous, loaded := l.said.Swap(key, message)
	return !loaded || previous != message
}

// forget drops key, so the next message under it is announced: the condition
// it reported is over, or its object was deleted.
func (l *lastAnnounced) forget(key string) {
	l.said.Delete(key)
}
