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

var errNoExecsToRepeat = errors.E(errors.Precondition, errors.New("no execs to repeat"))

// repeatExecs traverses the given flow DAG, repeats each exec found,
// compares the results and returns a flow node that returns a bool
// representing whether or not all repeated execs produced equal values.
func repeatExecs(f *flow.Flow, n int) (*flow.Flow, error) {
	r := repeater{root: f, times: n, repeated: newFlowMap(), skippedSet: newFlowMap()}
	return r.Comparer()
}

type repeater struct {
	root  *flow.Flow
	times int
	// repeated maps a flow f to a flow representing all repeats of f.
	repeated *flowMap
	// skippedSet maps a flow to itself and is used as a (goroutine-safe) set.
	skippedSet *flowMap
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
			deps := r.repeated.Values()
			skipped := r.skippedSet.Values()
			if len(deps) == 0 && r.times > 1 {
				if len(skipped) > 0 {
					fmt.Fprintf(os.Stderr, "no repeated execs (%d were skipped)\n", len(skipped))
					return &flow.Flow{Op: flow.Val, FlowDigest: values.Digest(true, types.Bool), Value: true}
				}
				return &flow.Flow{Op: flow.Val, Err: errors.Recover(errNoExecsToRepeat)}
			}
			return &flow.Flow{
				Op:         flow.K,
				Deps:       deps,
				FlowDigest: reflow.Digester.FromString("grail.com/reflow/syntax.repeat.ComparerAggregator"),
				K: func(vs []values.T) *flow.Flow {
					result := true
					for _, v := range vs {
						tuple := v.(values.Tuple)
						fmt.Fprintf(os.Stderr, "repeated exec result %s\n", tuple[0].(string))
						result = result && tuple[1].(bool)
					}
					if len(skipped) > 0 {
						skippedFs := make([]string, len(skipped))
						for i, s := range skipped {
							skippedFs[i] = fmt.Sprintf("%s(%s)", s.Ident, s.Digest().Short())
						}
						fmt.Fprintf(os.Stderr, "skipped %d repeated execs: %s\n", len(skipped), strings.Join(skippedFs, ", "))
					}
					return &flow.Flow{Op: flow.Val, FlowDigest: values.Digest(result, types.Bool), Value: result}
				},
			}
		},
	}, nil
}

// collect recursively finds exec nodes and collects flow nodes representing
// the repetition and result comparison for each of them.
// Execs flagged as 'nondeterministic' are skipped.
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
	// Skip non-deterministic execs
	if f.NonDeterministic {
		r.skippedSet.Put(f, f)
		return
	}
	if fr != nil {
		r.repeated.Put(f, fr)
	}
}

// repeatAndCompare returns a flow which compares the result of the given flow
// with n-1 duplicates of it, if applicable, or returns nil.
func repeatAndCompare(f *flow.Flow, n int) *flow.Flow {
	// Skip non-execs
	if f.Op != flow.Exec {
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
			var alldiffs []string
			fs0 := vs[0].(reflow.Fileset)
			for vi, v := range vs[1:] {
				var diffs []string
				fs := v.(reflow.Fileset)
				seen := make(map[int]bool)
				n := f.NExecArg()
				for i := 0; i < n; i++ {
					earg := f.ExecArg(i)
					if !earg.Out || seen[earg.Index] {
						continue
					}
					a, b := fs0.List[earg.Index], fs.List[earg.Index]
					if diff, hasDiff := a.Diff(b); hasDiff {
						diffs = append(diffs, fmt.Sprintf("%s %s", f.Argstrs[i], diff))
					}
					seen[earg.Index] = true
				}
				if len(diffs) > 0 {
					alldiffs = append(alldiffs, fmt.Sprintf("repeat 0 vs repeat %d:\n%s", vi+1, strings.Join(diffs, "\n")))
				}
			}
			res, b := "all same", true
			if len(alldiffs) > 0 {
				res, b = fmt.Sprintf("%s\n%s", f.Position, strings.Join(alldiffs, "\n")), false
			}
			result := values.Tuple{fmt.Sprintf("%s(%s): %s", f.Ident, groupId, res), b}
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
