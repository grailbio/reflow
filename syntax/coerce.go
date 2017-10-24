// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

var errMatch = errors.New("match error")

func coerceMatch(v values.T, t *types.T, p Path) (values.T, error) {
	if p.Done() {
		return v, nil
	}
	if f, ok := v.(*reflow.Flow); ok {
		return coerceMatchFlow(f, t, p), nil
	}
	v, t, ok, p := p.Match(v, t)
	if !ok {
		return nil, errMatch
	}
	return coerceMatch(v, t, p)
}

func coerceMatchFlow(f *reflow.Flow, t *types.T, p Path) *reflow.Flow {
	return &reflow.Flow{
		Op:         reflow.OpK,
		Deps:       []*reflow.Flow{f},
		FlowDigest: p.Digest(),
		K: func(vs []values.T) *reflow.Flow {
			v, t, ok, p := p.Match(vs[0], t)
			if !ok {
				return &reflow.Flow{
					Op:  reflow.OpVal,
					Err: errors.Recover(errMatch),
				}
			}
			if p.Done() {
				return flow(v, t)
			}
			if f, ok := v.(*reflow.Flow); ok {
				return coerceMatchFlow(f, t, p)
			}
			v, err := coerceMatch(v, t, p)
			if err != nil {
				return &reflow.Flow{
					Op:  reflow.OpVal,
					Err: errors.Recover(err),
				}
			}
			return flow(v, t)
		},
	}
}
