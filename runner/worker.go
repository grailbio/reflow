// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/log"
)

const (
	numTries           = 3
	defaultMaxIdleTime = 5 * time.Minute
)

// A worker steals work (execs) from an evaluator, transfers required
// objects, performs the exec locally, and then transmits the results
// back to the primary alloc. Workers also asynchronously cache
// successful results. Workers commit suicide after being idle for
// maxIdleTime.
type worker struct {
	// Executor is the executor for this worker.
	Executor reflow.Executor

	// Eval is the primary evaluator from which we steal work.
	Eval *flow.Eval

	// Log is a logger to which status and errors are reported.
	Log *log.Logger

	// MaxIdleTime determines how long a worker is allowed to remain
	// idle before stopping work. It is set to 5 minutes if not set.
	MaxIdleTime time.Duration
}

// Go starts the worker. Go returns after the worker has remained
// idle for more than maxIdleTime, or if the context is canceled.
// Go will wait to return until all pending tasks are complete.
func (w *worker) Go(ctx context.Context) {
	if w.MaxIdleTime == time.Duration(0) {
		w.MaxIdleTime = defaultMaxIdleTime
	}
	stealer := w.Eval.Stealer()
	defer stealer.Close()
	var (
		available = w.Executor.Resources()
		done      = make(chan *flow.Flow, 1024)
		timer     = time.NewTimer(w.MaxIdleTime)
		npending  int
	)
	for {
		var idlech <-chan time.Time
		if npending == 0 {
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(w.MaxIdleTime)
			idlech = timer.C
		}
		select {
		case f := <-stealer.Admit(available):
			if f == nil {
				continue
			}
			w.Log.Debugf("stole %v", f)
			available.Sub(available, f.Resources)
			w.Eval.Mutate(f, flow.Execing)
			npending++
			go func(f *flow.Flow) {
				if err := w.do(ctx, f); err != nil {
					w.Log.Errorf("eval %v: %v", f, err)
				}
				if f.State != flow.Done {
					// If we failed to complete the flow, hand it back
					// to the evaluator ready to run again.
					w.Eval.Mutate(f, flow.Ready)
				}
				done <- f
			}(f)
		case f := <-done:
			available.Add(available, f.Resources)
			stealer.Return(f)
			npending--
		case <-idlech:
			return
		case <-ctx.Done():
			for npending > 0 {
				f := <-done
				available.Add(available, f.Resources)
				npending--
				stealer.Return(f)
			}
			return
		}
	}
}

// do evaluates the flow f.
func (w *worker) do(ctx context.Context, f *flow.Flow) (err error) {
	for _, dep := range f.Deps {
		if dep.Err != nil {
			w.Eval.Mutate(f, dep.Err, flow.Done)
			return
		}
	}
	args := make([]reflow.Arg, f.NExecArg())
	for i := range args {
		earg := f.ExecArg(i)
		if earg.Out {
			args[i].Out = true
			args[i].Index = earg.Index
		} else {
			fs := f.Deps[earg.Index].Value.(reflow.Fileset)
			args[i].Fileset = &fs
		}
	}

	var files []reflow.File
	for _, arg := range args {
		if !arg.Out {
			files = append(files, arg.Fileset.Files()...)
		}
	}
	var size data.Size
	for _, f := range files {
		size += data.Size(f.Size)
	}

	begin := time.Now()
	defer func() {
		if err != nil {
			w.Log.Errorf("eval %s runtime error: %v", f.ExecString(false), err)
		} else if f.State == flow.Done {
			f.Runtime = time.Since(begin)
			w.Log.Debug(f.ExecString(false))
		}
	}()

	type state int
	const (
		stateUpload state = iota
		statePut
		stateWait
		stateInspect
		stateResult
		statePromote
		stateDownload
		stateDone
	)
	var (
		x reflow.Exec
		r reflow.Result
		n = 0
		s = stateUpload
	)
	for n < numTries && s < stateDone {
		switch s {
		case stateUpload:
			begin := time.Now()
			w.Log.Debugf("transferring %s (%d files) to worker repository for flow %v", size, len(files), f)
			err = w.Eval.Transferer.Transfer(ctx, w.Executor.Repository(), w.Eval.Executor.Repository(), files...)
			if err == nil {
				w.Log.Debugf("transferred %s (%d files) to worker repository for flow %v in %v", size, len(files), f, time.Since(begin))
			} else {
				w.Log.Errorf("transfer error: %v", err)
			}
		case statePut:
			x, err = w.Executor.Put(ctx, f.Digest(), reflow.ExecConfig{
				Type:        "exec",
				Ident:       f.Ident,
				Image:       f.Image,
				Cmd:         f.Cmd,
				Args:        args,
				Resources:   f.Resources,
				OutputIsDir: f.OutputIsDir,
			})
			if err == nil {
				f.Exec = x
				w.Eval.LogFlow(ctx, f)
			}
		case stateWait:
			err = x.Wait(ctx)
		case stateInspect:
			f.Inspect, err = x.Inspect(ctx)
		case stateResult:
			r, err = x.Result(ctx)
			if err == nil {
				w.Eval.Mutate(f, r.Fileset, flow.Incr)
			}
		case statePromote:
			// TODO(marius): make the exec's staging repository directly
			// accessible through the API and download directly from here.
			// this way we don't need promotion and GC becomes a simpler problem
			// for the case of stealers.
			err = x.Promote(ctx)
		case stateDownload:
			fs := f.Value.(reflow.Fileset)
			files := fs.Files()
			size = data.Size(0)
			for _, f := range files {
				size += data.Size(f.Size)
			}
			begin := time.Now()
			w.Log.Debugf("transferring %s (%d files) from worker repository", size, len(files))
			err = w.Eval.Transferer.Transfer(ctx, w.Eval.Executor.Repository(), w.Executor.Repository(), files...)
			if err == nil {
				w.Log.Debugf("transferred %s (%d files) from worker repository in %v", size, len(files), time.Since(begin))
			} else {
				w.Log.Errorf("transfer error: %v", err)
			}
		default:
			panic("unhandled state")
		}
		if err != nil {
			if errors.Is(errors.Temporary, err) || errors.Is(errors.Timeout, err) {
				n++
			} else {
				break
			}
		} else {
			n = 0
			s++
		}
	}
	if err != nil {
		if s > stateResult {
			w.Eval.Mutate(f, flow.Decr)
		}
		return err
	}
	w.Eval.Mutate(f, r.Err, flow.Done)
	bgctx := flow.Background(ctx)
	go func() {
		err := w.Eval.CacheWrite(bgctx, f, w.Executor.Repository())
		if err != nil {
			w.Log.Errorf("cache write %v: %v", f, err)
		}
		bgctx.Complete()
		w.Eval.Mutate(f, flow.Decr)
	}()
	return nil
}
