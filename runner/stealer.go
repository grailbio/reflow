// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
)

// Stealer is a work-stealer. It periodically queries additional
// resource requirements from an Eval, attempts to allocate
// additional allocs from a cluster, and then launches workers that
// steal work from the same Eval. Work stealers free their allocs
// when there is no more work to be stolen.
type Stealer struct {
	Cluster Cluster
	Log     *log.Logger
	Labels  pool.Labels
}

// Go polls the Eval e for required resources, allocates new allocs
// and spins up workers as needed. Go returns when the provided
// context is complete.
func (s *Stealer) Go(ctx context.Context, e *reflow.Eval) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	var n int
poll:
	for {
		select {
		case <-ticker.C:
			need := e.Need()
			if need.IsZero() {
				continue poll
			}
			n++
			s.Log.Debugf("need %v; starting new task stealing worker", need)
			actx, acancel := context.WithTimeout(ctx, allocTimeout)
			alloc, err := s.Cluster.Allocate(actx, need.Min, need.Max, s.Labels)
			acancel()
			if err != nil {
				continue poll
			}
			w := &worker{
				Executor: alloc,
				Eval:     e,
				Log:      s.Log.Tee(nil, alloc.ID()+": "),
			}
			wctx, wcancel := context.WithCancel(ctx)
			go func() {
				err := pool.Keepalive(wctx, s.Log, alloc)
				if err != wctx.Err() {
					s.Log.Errorf("worker %s died: %v", alloc.ID(), err)
				}
				wcancel()
			}()
			go func() {
				w.Go(wctx)
				wcancel()
				alloc.Free(context.Background())
			}()
		case <-ctx.Done():
			return
		}
	}
}
