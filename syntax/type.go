package syntax

import "github.com/grailbio/reflow/types"

var typeError = &types.T{Kind: types.ErrorKind}

// unify unifies ts into a single type.
func unify(ts ...*types.T) *types.T {
	if len(ts) == 0 {
		return types.Bottom
	}
	t := ts[0]
	for _, tt := range ts {
		t = t.Unify(tt)
		if t.Kind == types.ErrorKind {
			return t
		}
	}
	return t
}
