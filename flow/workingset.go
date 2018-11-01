// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

// Workingset tracks a set of pending flows and their
// initial states.
type workingset struct {
	pending map[*Flow]State
	counts  [Max]int
}

func newWorkingset() *workingset {
	return &workingset{
		pending: make(map[*Flow]State),
	}
}

// Add tracks the provided flow in the working set. Its state
// is recorded. Add panics if a flow is added more than once.
func (w *workingset) Add(f *Flow) {
	if w.Pending(f) {
		panic("flow already pending")
	}
	w.pending[f] = f.State
	w.counts[f.State]++
}

// Pending returns whether the provided flow is tracked by
// this working set.
func (w *workingset) Pending(f *Flow) bool {
	_, ok := w.pending[f]
	return ok
}

// Done removes the provided flow from the working set.
func (w *workingset) Done(f *Flow) {
	state, ok := w.pending[f]
	if !ok {
		panic("remove nonexistent flow")
	}
	delete(w.pending, f)
	w.counts[state]--
}

// N returns the total number of pending flows tracked by
// this working set.
func (w *workingset) N() int {
	return len(w.pending)
}

// NState returns the number of pending flows in the provided
// state.
func (w *workingset) NState(state State) int {
	return w.counts[state]
}
