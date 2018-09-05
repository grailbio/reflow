// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/values"
	"golang.org/x/sync/errgroup"
)

type writeback struct {
	Flow *Flow
	Fsid digest.Digest
}

// Repair performs cache-repair for flows. Repair can forward-migrate
// Reflow caches when more key types are added.
//
// Repair works by simulating evaluation using logical cache keys (and
// performing direct evaluation on the cached metadata where it can)
// and then writing new cache keys back to the assoc.
type Repair struct {
	// EvalConfig is the repair's configuration. Only Assoc and Repository need
	// to be configured.
	EvalConfig
	// GetLimit is applied to the assoc's get requests.
	GetLimit *limiter.Limiter
	// NumWrites is incremented for each new assoc entry written by the repair job.
	NumWrites int64

	writebacks chan writeback
	g          *errgroup.Group
}

// NewRepair returns a new repair configured with the provided EvalConfig.
//
// The caller must call (*Repair).Go before submitting evaluations through
// (*Repair.Do).
func NewRepair(config EvalConfig) *Repair {
	r := &Repair{
		EvalConfig: config,
		writebacks: make(chan writeback, 1024),
	}
	if r.CacheLookupTimeout == time.Duration(0) {
		r.CacheLookupTimeout = defaultCacheLookupTimeout
	}
	return r
}

// Do repairs the flow f. Repair is performed by using cached
// evaluations to populate values, and, when the cache is missing
// entries and a value can be computed immediately (i.e., without
// consuling an executor), computing that value. Flows that are
// successfully evaluated this way (sustaining no errors) are written
// back with their completed set of cache keys.
//
// Only OpExec flows are written back.
func (r *Repair) Do(ctx context.Context, f *Flow) {
	if f.State == Done {
		return
	}
	var (
		fs   reflow.Fileset
		fsid digest.Digest
		hit  bool
	)
	r.Log.Debugf("Repair.Do(%v)", f)
	keys := f.CacheKeys()
	for _, key := range keys {
		if r.GetLimit != nil {
			if err := r.GetLimit.Acquire(ctx, 1); err != nil {
				r.Log.Error(err)
				continue
			}
		}
		ctx, cancel := context.WithTimeout(ctx, r.CacheLookupTimeout)
		var err error
		key, fsid, err = r.Assoc.Get(ctx, assoc.Fileset, key)
		if r.GetLimit != nil {
			r.GetLimit.Release(1)
		}
		cancel()
		if err != nil {
			if !errors.Is(errors.NotExist, err) {
				r.Log.Errorf("assoc.Get %v: %v", f, err)
			}
			continue
		}
		err = unmarshal(ctx, r.Repository, fsid, &fs)
		if err == nil {
			hit = true
			break
		}
		if !errors.Is(errors.NotExist, err) {
			r.Log.Errorf("unmarshal %v: %v", fsid, err)
		}
	}
	// Now, evaluate all of our dependencies. Then evaluate our node
	// (and possibly re-evaluate depending on if it's a dynamic node).
	var err error
	for _, dep := range f.Deps {
		r.Do(ctx, dep)
		if e := dep.Err; e != nil && err == nil {
			err = e
		}
	}
	switch {
	case hit && err == nil:
		r.eval(f)
		if f.State == Done && f.Err != nil {
			// We can recover the value even if we can't evaluate it ourselves.
			f.Err = nil
			f.Value = fs
		}
	case hit:
		f.State = Done
		f.Value = fs
	case err != nil:
		f.Err = errors.Recover(err)
		f.State = Done
	default:
		r.eval(f)
	}
	// We may have to recur evaluation in case the flow was forked.
	if f.State != Done {
		r.Do(ctx, f)
	}
	if f.Op != Exec {
		return
	}
	if f.Err != nil {
		r.Log.Printf("cannot write back %v: %v", f, f.Err)
		return
	} else if !hit {
		panic("inconsistency")
	}
	r.writebacks <- writeback{f, fsid}
}

// eval performs a one-step, immediate evaluation of f. Non-immediate
// evaluations (i.e., those requiring an executor) are failed
// outright.
func (r *Repair) eval(f *Flow) {
	switch f.Op {
	case OpIntern, Exec:
		f.Err = errors.Recover(errors.New("cannot recompute execs or interns"))
		f.State = Done
	case Extern:
		// Externs always have empty return values; we can safely "compute" it.
		f.State = Done
	case Groupby:
		v := f.Deps[0].Value.(reflow.Fileset)
		groups := map[string]reflow.Fileset{}
		for path, file := range v.Map {
			idx := f.Re.FindStringSubmatch(path)
			if len(idx) != 2 {
				continue
			}
			v, ok := groups[idx[1]]
			if !ok {
				v = reflow.Fileset{Map: map[string]reflow.File{}}
				groups[idx[1]] = v
			}
			v.Map[path] = file
		}
		keys := make([]string, len(groups))
		i := 0
		for k := range groups {
			keys[i] = k
			i++
		}
		sort.Strings(keys)
		fs := reflow.Fileset{List: make([]reflow.Fileset, len(groups))}
		for i, k := range keys {
			fs.List[i] = groups[k]
		}
		f.Value = fs
		f.State = Done
	case Map:
		v := f.Deps[0].Value.(reflow.Fileset)
		ff := &Flow{
			Op:   Merge,
			Deps: make([]*Flow, len(v.List)),
		}
		for i := range v.List {
			ff.Deps[i] = f.MapFunc(filesetFlow(v.List[i]))
		}
		f.Fork(ff)
		f.Parent.State = Done
	case Collect:
		v := f.Deps[0].Value.(reflow.Fileset)
		fileset := map[string]reflow.File{}
		for path, file := range v.Map {
			if !f.Re.MatchString(path) {
				continue
			}
			dst := f.Re.ReplaceAllString(path, f.Repl)
			fileset[dst] = file
		}
		f.Value = reflow.Fileset{Map: fileset}
		f.State = Done
	case Merge:
		list := make([]reflow.Fileset, len(f.Deps))
		for i, dep := range f.Deps {
			list[i] = dep.Value.(reflow.Fileset)
		}
		f.Value = reflow.Fileset{List: list}
		f.State = Done
	case Val:
		f.State = Done
	case Pullup:
		v := &reflow.Fileset{List: make([]reflow.Fileset, len(f.Deps))}
		for i, dep := range f.Deps {
			v.List[i] = dep.Value.(reflow.Fileset)
		}
		f.Value = v.Pullup()
		f.State = Done
	case K:
		vs := make([]values.T, len(f.Deps))
		for i, dep := range f.Deps {
			vs[i] = dep.Value
		}
		ff := f.K(vs)
		f.Fork(ff)
		f.Parent.State = Done
	case Coerce:
		if v, err := f.Coerce(f.Deps[0].Value); err != nil {
			f.Err = errors.Recover(err)
		} else {
			f.Value = v
		}
		f.State = Done
	case Requirements:
		f.Value = f.Deps[0].Value
		f.State = Done
	case Data:
		id := Digester.FromBytes(f.Data)
		f.Value = reflow.Fileset{Map: map[string]reflow.File{".": {id, int64(len(f.Data))}}}
		f.State = Done
	default:
		panic(fmt.Sprintf("bug %v", f))
	}
}

// Go starts the repair's background writeback threads, writing back to
// the configured assoc with the provided maximum concurrency.
func (r *Repair) Go(ctx context.Context, concurrency int) {
	r.g, ctx = errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		r.g.Go(func() error {
			for wb := range r.writebacks {
				r.Log.Printf("write back %s %s %s", wb.Flow.Ident, wb.Flow, wb.Fsid)
				for _, key := range wb.Flow.CacheKeys() {
					err := r.Assoc.Store(ctx, assoc.Fileset, key, wb.Fsid)
					switch {
					case errors.Is(errors.Precondition, err):
					case err == nil:
						atomic.AddInt64(&r.NumWrites, 1)
					default:
						r.Log.Errorf("assoc.Put: %v", err)
					}
				}
			}
			return nil
		})
	}
}

// Done should be called after all evaluation is complete. Done
// returns after all outstanding writebacks have been performed.
func (r *Repair) Done() error {
	close(r.writebacks)
	return r.g.Wait()
}
