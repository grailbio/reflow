// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import (
	"context"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
)

var (
	MemMultiplier         = memMultiplier
	OomRetryMaxExecMemory = oomRetryMaxExecMemory
)

func PhysicalDigests(f *Flow) []digest.Digest {
	return f.physicalDigests()
}

func DepAssertions(f *Flow) []*reflow.Assertions {
	return f.depAssertions()
}

type AssertionsBatchCache = assertionsBatchCache

func NewAssertionsBatchCache(e *Eval) *AssertionsBatchCache {
	return e.newAssertionsBatchCache()
}

func RefreshAssertions(ctx context.Context, e *Eval, a []*reflow.Assertions, cache *AssertionsBatchCache) ([]*reflow.Assertions, error) {
	return e.refreshAssertions(ctx, a, cache)
}

func OomAdjust(specified, used reflow.Resources) reflow.Resources {
	return oomAdjust(specified, used)
}

// FindFlowCopy finds the copy of a given flow in the flow graph maintained by the Eval.
// Useful for performing assertions on Flow properties post-evaluation.
func (e *Eval) FindFlowCopy(f *Flow) *Flow {
	v := e.root.Visitor()
	for v.Walk() {
		if f.Digest() == v.Digest() {
			return v.Flow
		}
		v.Visit()
	}
	return nil
}
