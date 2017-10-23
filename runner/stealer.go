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
	Cache   reflow.Cache
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
			if need.Memory == 0 { // proxy for zero
				continue poll
			}
			n++
			min, _ := e.Requirements()
			s.Log.Debugf("need min(%v) max(%v); starting new task stealing worker", min, need)
			actx, acancel := context.WithTimeout(ctx, allocTimeout)
			alloc, err := s.Cluster.Allocate(actx, min, need, s.Labels)
			acancel()
			if err != nil {
				continue poll
			}
			w := &worker{
				Executor: alloc,
				Eval:     e,
				Cache:    s.Cache,
				Log:      s.Log.Tee(nil, alloc.ID()+": "),
			}
			wctx, wcancel := context.WithCancel(ctx)
			go func() {
				err := pool.Keepalive(wctx, alloc)
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
