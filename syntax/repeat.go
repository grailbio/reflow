// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// repeatExecs traverses the given flow DAG, repeats each exec found,
// compares the results and returns a flow node that
// returns a list of bool for each repeated exec under the given flow.
func repeatExecs(f *flow.Flow, n int) (*flow.Flow, error) {
	r := repeater{root: f, times: n, fm: newFlowMap()}
	return r.Comparer()
}

type repeater struct {
	root  *flow.Flow
	times int
	fm    *flowMap
}

// Comparer collects an aggregate flow node that combines the result of comparing
// the repetitions of each exec node found under the root node.
func (r *repeater) Comparer() (*flow.Flow, error) {
	if r.times > 1 {
		r.collect(r.root)
	}
	return &flow.Flow{
		Op:         flow.K,
		Deps:       []*flow.Flow{r.root},
		FlowDigest: reflow.Digester.FromString("grail.com/reflow/syntax.repeat.Comparer"),
		K: func(vs []values.T) *flow.Flow {
			// At this time, we know the root has been fully traversed
			// so we should've collected all the repeat/comparison flow nodes.
			// TODO(swami): Make this faster. This method causes us to wait for the root node
			// to finish before potentially firing off repetitions, but we can definitely make this faster.
			deps := r.fm.Values()
			if len(deps) == 0 && r.times > 1 {
				return &flow.Flow{Op: flow.Val, Err: errors.Recover(errors.New("no execs to repeat"))}
			}
			return &flow.Flow{
				Op:         flow.K,
				Deps:       deps,
				FlowDigest: reflow.Digester.FromString("grail.com/reflow/syntax.repeat.ComparerAggregator"),
				K: func(vs []values.T) *flow.Flow {
					result := true
					for _, v := range vs {
						tuple := v.(values.Tuple)
						fmt.Fprintln(os.Stderr, "repeated exec result %s", tuple[0].(string))
						if !tuple[1].(bool) {
							result = false
							break
						}
					}
					return &flow.Flow{Op: flow.Val, FlowDigest: values.Digest(result, types.Bool), Value: result}
				},
			}
		},
	}, nil
}

// collect recursively finds exec nodes and collects flow nodes representing
// the repetition and result comparison for each of them.
func (r *repeater) collect(f *flow.Flow) {
	for i := range f.Deps {
		r.collect(f.Deps[i])
	}
	if f.Op == flow.K {
		orig := f.K
		f.K = func(vs []values.T) *flow.Flow {
			k := orig(vs)
			r.collect(k)
			return k
		}
	}
	fr := repeatAndCompare(f, r.times)
	if fr != nil {
		r.fm.Put(f, fr)
	}
}

// repeatAndCompare returns a flow which compares the result of the given flow
// with n-1 duplicates of it, if applicable, or returns nil.
// Only applies to execs that aren't flagged as 'nondeterministic'.
func repeatAndCompare(f *flow.Flow, n int) *flow.Flow {
	// Skip non-execs
	if f.Op != flow.Exec {
		return nil
	}
	// Skip non-deterministic execs
	if f.NonDeterministic {
		return nil
	}
	groupId := f.Digest().Short()
	deps := make([]*flow.Flow, n)
	deps[0] = f
	// Make n-1 copies of f and modify each to make its digest unique and different from the original.
	for index := 1; index < n; index++ {
		fi := f.Copy()
		fi.Ident = fmt.Sprintf("%s_%s.repeat.%d", f.Ident, groupId, index)
		// Indirectly modify the digest of the copy
		fi.ExtraDigest = reflow.Digester.FromString(fmt.Sprintf("%d", index))
		deps[index] = fi
	}
	// Add a comparison node to the flowMap for this flow.
	return &flow.Flow{
		Op:         flow.K,
		Deps:       deps,
		FlowDigest: reflow.Digester.FromString("grail.com/reflow/syntax.repeat.repeatAndCompare"),
		K: func(vs []values.T) *flow.Flow {
			vdm := make(map[digest.Digest]bool)
			vds := make([]string, len(vs))
			for vi, v := range vs {
				// TODO(swami): Compare filesets deeply and provide meaningful errors
				vd := values.Digest(v, types.Fileset)
				vdm[vd] = true
				vds[vi] = vd.String()
			}
			var result values.Tuple
			if len(vdm) > 1 {
				result = values.Tuple{fmt.Sprintf("%s(%s): %d unique values:\n%s\n%s",
					f.Ident, groupId, len(vdm), f.Position, strings.Join(vds, "\n")), false}
			} else {
				result = values.Tuple{fmt.Sprintf("%s(%s): all same", f.Ident, groupId), true}
			}
			return &flow.Flow{
				Op:         flow.Val,
				FlowDigest: values.Digest(result, types.Tuple(&types.Field{T: types.String}, &types.Field{T: types.Bool})),
				Value:      result,
			}
		},
	}
}

// flowMap maps a flow to another flow.
type flowMap struct {
	mu sync.Mutex
	m  map[digest.Digest]*flow.Flow
}

func newFlowMap() *flowMap {
	return &flowMap{m: make(map[digest.Digest]*flow.Flow)}
}

// Puts the given key-value mapping into this flowmap.
func (s *flowMap) Put(k, v *flow.Flow) {
	d := k.Digest()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[d]; ok {
		return
	}
	s.m[d] = v
}

// Values returns a slice containing the values of this map.
func (s *flowMap) Values() []*flow.Flow {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := make([]*flow.Flow, 0, len(s.m))
	for _, v := range s.m {
		vs = append(vs, v)
	}
	return vs
}
