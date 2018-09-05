// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func coerceMatch(v values.T, t *types.T, pos scanner.Position, p Path) (values.T, error) {
	if p.Done() {
		return v, nil
	}
	if f, ok := v.(*flow.Flow); ok {
		return coerceMatchFlow(f, t, pos, p), nil
	}
	v, t, ok, p, err := p.Match(v, t)
	if !ok {
		return nil, errors.E(pos.String(), err)
	}
	return coerceMatch(v, t, pos, p)
}

func coerceMatchFlow(f *flow.Flow, t *types.T, pos scanner.Position, p Path) *flow.Flow {
	return &flow.Flow{
		Op:         flow.K,
		Deps:       []*flow.Flow{f},
		FlowDigest: p.Digest(),
		K: func(vs []values.T) *flow.Flow {
			v, t, p := vs[0], t, p
			for {
				var (
					err error
					ok  bool
				)
				v, t, ok, p, err = p.Match(v, t)
				if !ok {
					return &flow.Flow{
						Op:  flow.Val,
						Err: errors.Recover(errors.E(pos.String(), err)),
					}
				}
				if p.Done() {
					return toFlow(v, t)
				}
				if f, ok := v.(*flow.Flow); ok {
					return coerceMatchFlow(f, t, pos, p)
				}
			}
		},
	}
}
