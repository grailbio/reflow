// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

//go:generate stringer -type=Mutation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/status"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/liveset/bloomlive"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/predictor"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/values"
	"github.com/willf/bloom"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/simple"
)

const (
	// The minimum amount of memory we allocate for an exec.
	minExecMemory = 500 << 20
	// The minimum number of CPUs we allocate for an exec.
	minExecCPU = 1

	numExecTries = 5

	// printAllTasks can be set to aid testing and debugging.
	printAllTasks = false

	// maxOOMRetries is the maximum number of times a task can be retried due to an OOM.
	maxOOMRetries = 3

	// memMultiplier is the increase in memory that will be allocated to a task which OOMs.
	memMultiplier = 1.5

	// memSuggestThreshold is the minimum fraction of allocated memory an exec can use before a suggestion is
	// displayed to use less memory.
	memSuggestThreshold = 0.6
)

const defaultCacheLookupTimeout = 20 * time.Minute

// stateStatusOrder defines the order in which differenet flow
// statuses are rendered.
var stateStatusOrder = []State{
	Execing, Running, Transfer, Ready, Done,

	// DEBUG:
	NeedLookup, Lookup, NeedTransfer, TODO,
}

// Snapshotter provides an interface for snapshotting source URL data into
// unloaded filesets.
type Snapshotter interface {
	Snapshot(ctx context.Context, url string) (reflow.Fileset, error)
}

// EvalConfig provides runtime configuration for evaluation instances.
type EvalConfig struct {
	// The executor to which execs are submitted.
	Executor reflow.Executor

	// Scheduler is used to run tasks. Either a Scheduler or Executor
	// must be defined. Note that the plan is to deprecate using
	// executors directly from the evaluator, leaving Scheduler the
	// only option, and at which time we can simplify some aspects
	// of the flow evaluator.
	//
	// The scheduler must use the same repository as the evaluator.
	Scheduler *sched.Scheduler

	// Predictor is used to predict the tasks' resource usage. It
	// will only be used if a Scheduler is defined.
	Predictor *predictor.Predictor

	// Snapshotter is used to snapshot source URLs into unloaded
	// filesets. If non-nil, then files are delay-loaded.
	Snapshotter Snapshotter

	// An (optional) logger to which the evaluation transcript is printed.
	Log *log.Logger

	// DotWriter is an (optional) writer where the evaluator will write the flowgraph to in dot format.
	DotWriter io.Writer

	// Status gets evaluation status reports.
	Status *status.Group

	// An (optional) logger to print evaluation trace.
	Trace *log.Logger

	// Transferer is used to arrange transfers between repositories,
	// including nodes and caches.
	Transferer reflow.Transferer

	// Repository is the main, shared repository between evaluations.
	Repository reflow.Repository

	// Assoc is the main, shared assoc that is used to store cache and
	// metadata associations.
	Assoc assoc.Assoc

	// AssertionGenerator is the implementation for generating assertions.
	AssertionGenerator reflow.AssertionGenerator

	// Assert is the policy to use for asserting cached Assertions.
	Assert reflow.Assert

	// TaskDB is the db to which run/tasks information and keepalives are maintained.
	TaskDB taskdb.TaskDB

	// RunID is a unique identifier for the run
	RunID taskdb.RunID

	// CacheMode determines whether the evaluator reads from
	// or writees to the cache. If CacheMode is nonzero, Assoc,
	// Repository, and Transferer must be non-nil.
	CacheMode infra2.CacheMode

	// NoCacheExtern determines whether externs are cached.
	NoCacheExtern bool

	// GC tells whether Eval should perform garbage collection
	// after each exec has completed.
	GC bool

	// RecomputeEmpty determines whether cached empty values
	// are recomputed.
	RecomputeEmpty bool

	// BottomUp determines whether we perform bottom-up only
	// evaluation, skipping the top-down phase.
	BottomUp bool

	// PostUseChecksum indicates whether input filesets are checksummed after use.
	PostUseChecksum bool

	// Config stores the flow config to be used.
	Config Config

	// ImageMap stores the canonical names of the images.
	// A canonical name has a fully qualified registry host,
	// and image digest instead of image tag.
	ImageMap map[string]string

	// CacheLookupTimeout is the timeout for cache lookups.
	// After the timeout expires, a cache lookup is considered
	// a miss.
	CacheLookupTimeout time.Duration

	// Invalidate is a function that determines whether or not f's cached
	// results should be invalidated.
	Invalidate func(f *Flow) bool

	// Labels is the labels for this run.
	Labels pool.Labels
}

// String returns a human-readable form of the evaluation configuration.
func (e EvalConfig) String() string {
	var b bytes.Buffer
	if e.Executor != nil {
		fmt.Fprintf(&b, "executor %T", e.Executor)
	} else {
		fmt.Fprintf(&b, "scheduler %T", e.Scheduler)
	}
	if e.Snapshotter != nil {
		fmt.Fprintf(&b, " snapshotter %T", e.Snapshotter)
	}
	if e.Transferer != nil {
		fmt.Fprintf(&b, " transferer %T", e.Transferer)
	}
	if e.Repository != nil {
		repo := fmt.Sprintf("%T", e.Repository)
		if u := e.Repository.URL(); u != nil {
			repo += fmt.Sprintf(",url=%s", u)
		}
		fmt.Fprintf(&b, " repository %s", repo)
	}
	if e.Assoc != nil {
		fmt.Fprintf(&b, " assoc %s", e.Assoc)
	}
	if e.TaskDB != nil {
		fmt.Fprintf(&b, " taskdb %s", e.TaskDB)
	}
	if e.Predictor != nil {
		fmt.Fprintf(&b, " predictor %T", e.Predictor)
	}

	var flags []string
	if e.NoCacheExtern {
		flags = append(flags, "nocacheextern")
	} else {
		flags = append(flags, "cacheextern")
	}
	if e.CacheMode == infra2.CacheOff {
		flags = append(flags, "nocache")
	} else {
		if e.CacheMode.Reading() {
			flags = append(flags, "cacheread")
		}
		if e.CacheMode.Writing() {
			flags = append(flags, "cachewrite")
		}
	}
	if e.GC {
		flags = append(flags, "gc")
	} else {
		flags = append(flags, "nogc")
	}
	if e.RecomputeEmpty {
		flags = append(flags, "recomputeempty")
	} else {
		flags = append(flags, "norecomputeempty")
	}
	if e.BottomUp {
		flags = append(flags, "bottomup")
	} else {
		flags = append(flags, "topdown")
	}
	if e.PostUseChecksum {
		flags = append(flags, "postusechecksum")
	}
	fmt.Fprintf(&b, " flags %s", strings.Join(flags, ","))
	fmt.Fprintf(&b, " flowconfig %s", e.Config)
	fmt.Fprintf(&b, " cachelookuptimeout %s", e.CacheLookupTimeout)
	fmt.Fprintf(&b, " imagemap %v", e.ImageMap)
	return b.String()
}

// Eval is an evaluator for Flows.
type Eval struct {
	// EvalConfig is the evaluation configuration used in this
	// evaluation.
	EvalConfig

	root *Flow

	// assertions is an accumulation of assertions from computed flows (cache-hit or not)
	// used to ensure that no two flows can be computed with conflicting assertions.
	assertions *reflow.Assertions

	// A channel indicating how much extra resources are needed
	// in order to avoid queueing.
	needch chan reflow.Requirements
	// A channel for evaluation errors.
	errors chan error
	// Total and currently available resources.
	total, available reflow.Resources
	// The number of Flows stolen.
	nstolen int
	// Contains pending (currently executing) flows.
	pending *workingset

	// Roots stores the set of roots to be visited in the next
	// evaluation iteration.
	roots FlowVisitor

	// Ticker for reporting.
	ticker *time.Ticker
	// Total execution time.
	totalTime time.Duration
	// Informational channels for printing status.
	needLog []*Flow

	// Repo is the repository from which execs have object access.
	repo reflow.Repository

	// these maintain status printing state
	begin                           time.Time
	prevStateCounts, prevByteCounts counters

	wakeupch chan bool

	// Channels that support work stealing.
	returnch   chan *Flow
	newStealer chan *Stealer
	// stealer is the head of the stealer list
	stealer *Stealer

	needCollect             bool
	live                    *bloom.BloomFilter
	nlive                   int
	livebytes, maxlivebytes data.Size
	muGC                    sync.RWMutex
	writers                 *writer
	writersMu               sync.Mutex

	// marshalLimiter is the number concurrent of marshal/unmarshaling of cached filesets to do.
	// In case of large batch jobs, loading too many of them in parallel causes OOMs,
	// so we limit how many we load concurrently.
	// TODO(swami): Better solution is to use a more optimized file format (instead of JSON).
	marshalLimiter *limiter.Limiter

	flowgraph *simple.DirectedGraph
}

// NewEval creates and initializes a new evaluator using the provided
// evaluation configuration and root flow.
func NewEval(root *Flow, config EvalConfig) *Eval {
	if (config.Assoc == nil || config.Repository == nil) && config.CacheMode != infra2.CacheOff {
		switch {
		case config.Assoc == nil && config.Repository == nil:
			config.Log.Printf("turning caching off because assoc and repository are not configured")
		case config.Assoc == nil:
			config.Log.Printf("turning caching off because assoc is not configured")
		case config.Repository == nil:
			config.Log.Printf("turning caching off because repository is not configured")
		}
		config.CacheMode = infra2.CacheOff
	}

	e := &Eval{
		EvalConfig:     config,
		root:           root.Canonicalize(config.Config),
		assertions:     reflow.NewAssertions(),
		needch:         make(chan reflow.Requirements),
		errors:         make(chan error),
		returnch:       make(chan *Flow, 1024),
		newStealer:     make(chan *Stealer),
		wakeupch:       make(chan bool, 1),
		pending:        newWorkingset(),
		marshalLimiter: limiter.New(),
		flowgraph:      simple.NewDirectedGraph(),
	}
	// Limit the number of concurrent marshal/unmarshal to the number of CPUs we have.
	e.marshalLimiter.Release(runtime.NumCPU())

	if config.Executor != nil {
		e.repo = config.Executor.Repository()
		e.total = config.Executor.Resources()
	} else {
		e.repo = e.Repository
	}
	// We only support delayed loads when using a scheduler.
	if e.Scheduler == nil {
		e.Snapshotter = nil
	}
	if e.CacheLookupTimeout == time.Duration(0) {
		e.CacheLookupTimeout = defaultCacheLookupTimeout
	}
	e.available = e.total
	if e.Log == nil && printAllTasks {
		e.Log = log.Std
	}
	return e
}

// Requirements returns the minimum and maximum resource
// requirements for this Eval's flow.
func (e *Eval) Requirements() reflow.Requirements {
	return e.root.Requirements()
}

// Flow returns the root flow of this eval.
func (e *Eval) Flow() *Flow {
	return e.root
}

// Value returns the root value of this eval.
func (e *Eval) Value() values.T {
	return e.root.Value
}

// Err returns the root evaluation error, if any.
func (e *Eval) Err() error {
	if e.root.Err == nil {
		return nil
	}
	return e.root.Err
}

// Do evaluates a flow (as provided in Init) and returns its value,
// or error.
//
// There are two evaluation modes, configured by EvalConfig.BottomUp.
//
// When BottomUp is true, the Flow is evaluated in bottom-up mode.
// Each node's dependencies are evaluated (recursively); a node is
// evaluated when all of its dependencies are complete (and error
// free). Before a node is run, its result is first looked up in the
// configured cache. If there is a cache hit, evaluation without any
// work done. Only the node's value is downloaded; its objects are
// fetched lazily. When a node is ready to be evaluated, we check
// that all of the objects that it depends on are present in the
// executor's repository; missing objects are retrieved from cache.
// If these objects are not present in the cache (this can happen if
// the object is removed from the cache's repository after the cache
// lookup was done but before the transfer began), evaluation fails
// with a restartable error.
//
// When BottomUp is false, the flow is evaluated first top-down, and
// then bottom up. In this mode, objects are looked up first in the
// top-down phase; a nodes dependencies are explored only on cache
// miss. Once this phase is complete, evaluation proceeds in
// bottom-up mode. Object retrievial is as in bottom-up mode.
//
// Eval keeps track of the evaluation state of each node; these are
// described in the documentation for State.
//
// Evaluation is performed by simplification: ready nodes are added
// to a todo list. Single-step evaluation yields either a fully
// evaluated node (where (*Flow).Value is set to its result) or by a
// new Flow node (whose (*Flow).Parent is always set to its
// ancestor). Evaluations are restartable.
//
// Eval permits supplementary workers to steal nodes to evaluate.
// These workers are responsible for transferring any necessary data
// between the Eval's repository and the worker's. Once a Flow node
// has been stolen, it is owned by the worker until it is returned;
// the worker must set the Flow node's state appropriately.
//
// This provides a simple evaluation scheme that also does not leave
// any parallelism "on the ground".
//
// Eval employs a conservative admission controller to ensure that we
// do not exceed available resources.
//
// The root flow is canonicalized before evaluation.
//
// Eval reclaims unreachable objects after each exec has completed
// and e.GC is set to true.
//
// TODO(marius): wait for all nodes to complete before returning
// (early) when cancelling...
//
// TODO(marius): explore making use of CAS flow states, so that we
// don't have to separately track pending nodes internally (so we
// don't clobber stolen nodes).
//
// TODO(marius): permit "steal-only" mode. The only provision for
// this setup is that the parent must contain some sort of global
// repository (e.g., S3).
func (e *Eval) Do(ctx context.Context) error {
	defer func() {
		if e.DotWriter != nil {
			b, err := dot.Marshal(e.flowgraph, fmt.Sprintf("reflow flowgraph %v", e.EvalConfig.RunID.ID()), "", "")
			if err != nil {
				e.Log.Debugf("err dot marshal: %v", err)
				return
			}
			_, err = e.DotWriter.Write(b)
			if err != nil {
				e.Log.Debugf("err writing dot file: %v", err)
			}
		}
	}()
	e.Log.Debugf("evaluating with configuration: %s", e.EvalConfig)
	e.begin = time.Now()
	defer func() {
		e.totalTime = time.Since(e.begin)
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.ticker = time.NewTicker(10 * time.Second)
	defer e.ticker.Stop()

	root := e.root
	e.roots.Push(root)
	e.printDeps(root, false)

	var (
		todo  FlowVisitor
		tasks []*sched.Task // The set of tasks to be submitted after this iteration.
		flows []*Flow       // The set of flows corresponding to the tasks to be submitted after this iteration.
	)
	for root.State != Done {
		if root.Digest().IsZero() {
			panic("invalid flow, zero digest: " + root.DebugString())
		}

		// This is the meat of the evaluation: we gather Flows that are
		// ready (until steady state), and then execute this batch. At the
		// end of each iteration, we wait for one task to complete, and
		// gather any new Flows that have become ready.

		nroots := len(e.roots.q)
		todo.Reset()
		visited := make(flowOnce)
		for e.roots.Walk() {
			e.todo(e.roots.Flow, visited, &todo)
		}
		e.roots.Reset()
		e.Trace.Debugf("todo %d from %d roots", len(todo.q), nroots)

		// LookupFlows consists of all the flows that need to be looked in the cache in this round of flow scheduling.
		var lookupFlows []*Flow
	dequeue:
		for todo.Walk() {
			f := todo.Flow
			e.printDeps(f, true)
			if e.pending.Pending(f) {
				continue
			}
			switch f.Op {
			case Exec, Intern, Extern:
				if !f.TaskID.IsValid() {
					f.TaskID = taskdb.NewTaskID()
				}
			}
			if f.Op == Exec {
				if f.Resources["mem"] < minExecMemory {
					f.Resources["mem"] = minExecMemory
				}
				if f.Resources["cpu"] < minExecCPU {
					f.Resources["cpu"] = minExecCPU
				}
			}
			if e.ImageMap != nil && f.OriginalImage == "" {
				f.OriginalImage = f.Image
				if img, ok := e.ImageMap[f.Image]; ok {
					f.Image = img
				}
			}
			if e.Snapshotter != nil && f.Op == Intern && (f.State == Ready || f.State == NeedTransfer) && !f.MustIntern {
				// In this case we don't display status, since we're not doing
				// any appreciable work here, and it's confusing to the user.
				e.Mutate(f, Running, NoStatus)
				e.pending.Add(f)
				e.step(f, func(f *Flow) error {
					fs, err := e.Snapshotter.Snapshot(ctx, f.URL.String())
					if err != nil {
						e.Log.Printf("must intern %q: resolve: %v", f.URL, err)
						e.Mutate(f, Ready, MustIntern)
					} else {
						e.Mutate(f, fs, Done)
					}
					return nil
				})
				continue dequeue
			} else if e.Scheduler != nil && f.Op.External() {
				switch f.State {
				case NeedTransfer, Ready:
					// If we're using a scheduler, then we can skip transfer, and
					// submit directly to the scheduler.
					e.Mutate(f, NeedSubmit)
				}
			} else if f.Op.External() && f.MustIntern {
				// TODO(pgopal/swami): Remove this (see https://phabricator.grailbio.com/D45057).
				// This check was adedd to ensure MustIntern flows work properly in evaluator mode.
				e.Mutate(f, Ready)
			}

			switch f.State {
			case NeedLookup:
				// TODO(marius): we should perform batch lookups
				// as the underyling APIs (e.g., to DynamoDB) do not
				// bundle requests automatically.
				e.Mutate(f, Lookup)
				e.pending.Add(f)
				lookupFlows = append(lookupFlows, f)
			case NeedTransfer:
				e.Mutate(f, Transfer)
				e.pending.Add(f)
				e.step(f, func(f *Flow) error {
					files, err := e.needTransfer(ctx, f)
					if err != nil {
						e.Log.Errorf("need transfer: %v", err)
					} else if len(files) == 0 {
						// No transfer needed; we're ready to go.
						e.Mutate(f, Ready)
						return nil
					}
					// Compute the transfer size, so that we can log it. Note that
					// this is of course subject to race conditions: when multiple
					// execs concurrently require the same objects, these may be
					// reported multiple times in aggregate transfer size.
					seen := make(map[digest.Digest]bool)
					for _, file := range files {
						dig := file.Digest()
						if seen[dig] {
							continue
						}
						f.TransferSize += data.Size(file.Size)
						seen[dig] = true
					}
					e.Mutate(f, Refresh) // TransferSize is updated
					e.LogFlow(ctx, f)
					return e.transfer(ctx, f)
				})
			case Ready:
				if !e.total.Available(f.Resources) {
					// TODO(marius): we could also attach this error to the node.
					return errors.E(errors.ResourcesExhausted,
						errors.Errorf("eval %v: requested resources %v exceeds total available %v",
							f.Ident, f.Resources, e.total))
				}
				if !e.available.Available(f.Resources) {
					e.roots.Push(f)
					continue dequeue
				}
				e.available.Sub(e.available, f.Resources)
				state := Running
				if f.Op.External() {
					state = Execing
				}
				e.Mutate(f, state, Reserve(f.Resources))
				e.pending.Add(f)
				e.step(f, func(f *Flow) error { return e.eval(ctx, f) })
			case NeedSubmit:
				var err *errors.Error
				// Propagate errors immediately. We have to do this manually
				// here since we're not going through the evaluator.
				for _, dep := range f.Deps {
					if err = dep.Err; err != nil {
						break
					}
				}
				e.pending.Add(f)
				if err != nil {
					go func(err *errors.Error) {
						e.Mutate(f, err, Done)
						e.returnch <- f
					}(err)
					break
				}
				e.Mutate(f, Execing, Reserve(f.Resources))
				task := e.newTask(f)
				tasks = append(tasks, task)
				flows = append(flows, f)
				e.step(f, func(f *Flow) error {
					if err := e.taskWait(ctx, f, task); err != nil {
						return err
					}
					// The Predictor can lower a task's memory requirements and cause a non-OOM memory error. This is
					// because the linux OOM killer does not catch all OOM errors. In order to prevent such an error from
					// causing a reflow program to fail, assume that if the Predictor lowers the memory of a task and
					// that task returns a non-OOM error, the task should be retried with its original resources.
					if task.Result.Err != nil && !errors.Is(errors.OOM, task.Result.Err) && f.Reserved["mem"] < f.Resources["mem"] {
						var err error
						if task, err = e.retryTask(ctx, f, f.Resources, "Predictor", "default resources"); err != nil {
							return err
						}
					}
					// Retry OOMs if needed.
					for retries := 0; retries < maxOOMRetries && task.Result.Err != nil && errors.Is(errors.OOM, task.Result.Err); retries++ {
						resources := oomAdjust(f.Resources, task.Config.Resources)
						msg := fmt.Sprintf("%v of memory (%v/%v)", data.Size(resources["mem"]), retries+1, maxOOMRetries)
						var err error
						if task, err = e.retryTask(ctx, f, resources, "OOM", msg); err != nil {
							return err
						}
					}
					// Write to the cache only if a task was successfully completed.
					if e.CacheMode.Writing() && task.Err == nil && task.Result.Err == nil {
						e.Mutate(f, Incr) // just so the cache write can decr it
						e.cacheWriteAsync(ctx, f)
					}
					return nil
				})
			}
		}
		if len(lookupFlows) > 0 {
			go e.batchLookup(ctx, lookupFlows...)
		}
		// Delay task submission until we have gathered all potential tasks
		// that can be scheduled concurrently. This is represented by the
		// set of tasks that are currently either performing cache lookups
		// (Lookup) or else are undergoing local evaluation (Running). This
		// helps the scheduler better allocate underlying resources since
		// we always submit the largest available working set.
		if e.Scheduler != nil && len(tasks) > 0 && e.pending.NState(Lookup)+e.pending.NState(Running) == 0 {
			e.reviseResources(ctx, tasks, flows)
			e.Scheduler.Submit(tasks...)
			tasks = tasks[:0]
			flows = flows[:0]
		}
		if root.State == Done {
			break
		}
		if e.pending.N() == 0 && root.State != Done {
			var states [Max][]*Flow
			for v := e.root.Visitor(); v.Walk(); v.Visit() {
				states[v.State] = append(states[v.State], v.Flow)
			}
			var s [Max]string
			for i := range states {
				n, tasks := accumulate(states[i])
				s[i] = fmt.Sprintf("%s:%d<%s>", State(i).Name(), n, tasks)
			}
			e.Log.Printf("pending %d", e.pending.N())
			e.Log.Printf("eval %s", strings.Join(s[:], " "))
			panic("scheduler is stuck")
		}
		if err := e.wait(ctx); err != nil {
			return err
		}
	}
	// In the case of error, we return immediately. On success, we flush
	// all pending tasks so that all logs are properly displayed. We
	// also perform another collection, so that the executor may be
	// archived without data.
	if root.Err != nil {
		return nil
	}
	for e.pending.N() > 0 {
		if err := e.wait(ctx); err != nil {
			return err
		}
	}
	e.collect(ctx)
	for _, f := range e.needLog {
		e.LogFlow(ctx, f)
	}
	e.needLog = nil
	return nil
}

// LogSummary prints an execution summary to an io.Writer.
func (e *Eval) LogSummary(log *log.Logger) {
	var n int
	type aggregate struct {
		N, Ncache               int
		Runtime                 stats
		CPU, Memory, Disk, Temp stats
		Transfer                data.Size
		Requested               reflow.Resources
	}
	stats := map[string]aggregate{}

	for v := e.root.Visitor(); v.Walk(); v.Visit() {
		if v.Parent != nil {
			v.Push(v.Parent)
		}
		// Skip nodes that were skipped due to caching.
		if v.State < Done {
			continue
		}
		switch v.Op {
		case Exec, Intern, Extern:
		default:
			continue
		}

		ident := v.Ident
		if ident == "" {
			ident = "?"
		}
		a := stats[ident]
		a.N++
		if v.Cached {
			a.Ncache++
		}
		a.Transfer += v.TransferSize
		if len(v.Inspect.Profile) == 0 {
			n++
			stats[ident] = a
			continue
		}
		if v.Op == Exec {
			a.CPU.Add(v.Inspect.Profile["cpu"].Mean)
			a.Memory.Add(v.Inspect.Profile["mem"].Max)
			a.Disk.Add(v.Inspect.Profile["disk"].Max)
			a.Temp.Add(v.Inspect.Profile["tmp"].Max)
			a.Requested = v.Inspect.Config.Resources
		}
		if d := v.Inspect.Runtime().Minutes(); d > 0 {
			a.Runtime.Add(d)
		}
		n++
		stats[ident] = a
	}
	if n == 0 {
		return
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "total n=%d time=%s\n", n, round(e.totalTime))
	var tw tabwriter.Writer
	tw.Init(newPrefixWriter(&b, "\t"), 4, 4, 1, ' ', 0)
	fmt.Fprintln(&tw, "ident\tn\tncache\ttransfer\truntime(m)\tcpu\tmem(GiB)\tdisk(GiB)\ttmp(GiB)\trequested")
	idents := make([]string, 0, len(stats))
	for ident := range stats {
		idents = append(idents, ident)
	}
	sort.Strings(idents)
	var warningIdents []string
	const byteScale = 1.0 / (1 << 30)
	for _, ident := range idents {
		stats := stats[ident]
		fmt.Fprintf(&tw, "%s\t%d\t%d\t%s", ident, stats.N, stats.Ncache, stats.Transfer)
		if stats.CPU.N() > 0 {
			fmt.Fprintf(&tw, "\t%s\t%s\t%s\t%s\t%s\t%s",
				stats.Runtime.Summary("%.0f"),
				stats.CPU.Summary("%.1f"),
				stats.Memory.SummaryScaled("%.1f", byteScale),
				stats.Disk.SummaryScaled("%.1f", byteScale),
				stats.Temp.SummaryScaled("%.1f", byteScale),
				stats.Requested,
			)
			reqMem, minMem := stats.Requested["mem"], float64(minExecMemory)
			if memRatio := stats.Memory.Mean() / reqMem; memRatio <= memSuggestThreshold && reqMem > minMem {
				warningIdents = append(warningIdents, ident)
			}
		} else {
			fmt.Fprint(&tw, "\t\t\t\t\t")
		}
		fmt.Fprint(&tw, "\n")
	}
	if len(warningIdents) > 0 {
		fmt.Fprintf(&tw, "warning: reduce memory requirements for over-allocating execs: %s", strings.Join(warningIdents, ", "))
	}
	tw.Flush()
	log.Printf(b.String())
}

// Need returns the total resource requirements needed in order to
// avoid queueing work.
func (e *Eval) Need() reflow.Requirements {
	return <-e.needch
}

// Stealer returns Stealer from which flow nodes may be stolen. This
// permits an external worker to perform the work implied by the
// return Flow, which is always in Ready state. When the external
// worker has completed processing (or decided not to process after
// all), the node must be returned via Return.
func (e *Eval) Stealer() *Stealer {
	s := newStealer(e)
	e.newStealer <- s
	return s
}

// Step asynchronously invokes proc on the provided flow. Once
// processing is complete, the flow is returned. If an error is
// returned, the error is communicated to the evaluation loop.
func (e *Eval) step(f *Flow, proc func(f *Flow) error) {
	go func() {
		err := proc(f)
		if err != nil {
			e.errors <- err
		} else {
			e.returnch <- f
		}
	}()
}

// wakeup wakes up a sleeping evaluator. Wakeup requests garbage collection
// when needCollect is true.
func (e *Eval) wakeup(needCollect bool) {
	select {
	case e.wakeupch <- needCollect:
	default:
	}
}

// wait returns when the next flow has completed. It returns an error
// if it completed with an error.
func (e *Eval) wait(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			return
		}
		for {
			select {
			case f := <-e.returnch:
				e.returnFlow(f)
			default:
				return
			}
		}
	}()

	for {
		// Collapse concurrent wakeups.
		select {
		case needCollect := <-e.wakeupch:
			e.needCollect = e.needCollect || needCollect
		default:
		}
		// Collect closed stealers.
		for p := &e.stealer; *p != nil; {
			if (*p).closed {
				*p = (*p).next
			}
			if *p != nil {
				p = &(*p).next
			}
		}
		if e.needCollect {
			e.collect(ctx)
			e.needCollect = false
		}

		// Compute needed resources and find stealable nodes.
		var (
			need             reflow.Requirements
			nready, nrunning int
		)
		for v := e.root.Visitor(); v.Walk(); {
			switch {
			case e.pending.Pending(v.Flow):
				if v.State == Transfer {
					nrunning++
				} else {
					switch v.Op {
					case Exec, Intern, Extern:
						nrunning++
					}
				}
				continue
			case v.State < Ready:
				v.Visit()
			case v.State == Ready:
				nready++
				admitted := false
				for s := e.stealer; s != nil; s = s.next {
					if admitted = s.admit(v.Flow); admitted {
						e.pending.Add(v.Flow)
						e.nstolen++
						break
					}
				}
				if !admitted {
					need.AddParallel(v.Resources)
				}
			}
		}
		for _, f := range e.needLog {
			e.LogFlow(ctx, f)
		}
		e.needLog = nil
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e.needCollect = <-e.wakeupch:
		case e.needch <- need:
		case s := <-e.newStealer:
			s.next = e.stealer
			e.stealer = s
		case f := <-e.returnch:
			e.returnFlow(f)
			return nil
		case err := <-e.errors:
			return err
		case <-e.ticker.C:
			e.reportStatus()
		}
	}
}

func (e *Eval) reportStatus() {
	if e.Status == nil {
		return
	}
	var stateCounts, byteCounts counters
	for v := e.root.Visitor(); v.Walk(); v.Visit() {
		if f := v.Parent; f != nil {
			// Push the parent's children but not the parent itself,
			// otherwise it looks like we're reporting maps twice.
			for _, dep := range f.Deps {
				v.Push(dep)
			}
		}
		switch v.Op {
		case Exec, Intern, Extern:
		default:
			continue
		}
		switch v.State {
		case Transfer:
			byteCounts.Incr(v.State, v.Ident, int(v.TransferSize))
			fallthrough
		case Ready, Done, TODO, Running, Execing:
			stateCounts.Incr(v.State, v.Ident, 1)
		}
	}
	e.prevStateCounts, e.prevByteCounts = stateCounts, byteCounts
	var b bytes.Buffer
	elapsed := time.Since(e.begin)
	var dur string
	switch {
	case elapsed < 10*time.Minute:
		dur = round(elapsed).String()
	case elapsed < time.Hour:
		dur = fmt.Sprintf("%dm", int(elapsed.Minutes()))
	default:
		dur = fmt.Sprintf("%dh%dm", int(elapsed.Hours()), int(elapsed.Minutes()-60*elapsed.Hours()))
	}
	fmt.Fprintf(&b, "elapsed: %s", dur)
	for _, state := range []State{Execing, Running, Transfer, Ready} {
		n := stateCounts.N(state)
		if n == 0 {
			continue
		}
		fmt.Fprintf(&b, ", %s:%d", humanState[state], n)
	}
	fmt.Fprintf(&b, ", completed: %d/%d",
		stateCounts.N(Done),
		stateCounts.N(Done)+stateCounts.N(Execing)+stateCounts.N(Running)+stateCounts.N(Transfer)+stateCounts.N(TODO))
	e.Status.Print(b.String())
}

func (e *Eval) returnFlow(f *Flow) {
	e.pending.Done(f)
	switch f.State {
	case Done:
		for _, flow := range f.Dirty {
			delete(flow.Pending, f)
			if len(flow.Pending) == 0 {
				e.roots.Push(flow)
			}
		}
	default:
		// Might need re-evaluation, so we need to re-traverse.
		e.roots.Push(f)
	}
	e.needCollect = true
	e.available.Add(e.available, f.Reserved)
	e.Mutate(f, Unreserve(f.Reserved))
	if f.Tracked && f.State == Done {
		e.needLog = append(e.needLog, f)
	}
}

// Collect reclaims unreachable objects from the executor's repository.
// Objects are considered live if:
//
//	(1) They are part of the frontier of flow nodes in Done state.
//	    These are the nodes for which not all dependent nodes are processed
//	    yet, and thus data dependencies still exist. Values attached to nodes
//	    behind this frontier are dead, since they are not dependencies of
//	    any runnable node, or any node that will become runnable.
//	(2) They are part of a liveset, as managed by (*Eval).Live and (*Eval).Dead.
//	    These are values that must be retained for other reasons, for example
//	    because they are being uploaded to cache.
//
// This scheme requires that all files in the repository which may
// become live must be accounted for either through (1) or (2). In
// order to guarantee this for execs and interns, we write these
// results into staging repositories which are not subject to
// reclamation. Once the full result is available in the staging
// repository, they are promoted to the main repository, only after
// being declared live (by atomically, and nonconcurrently with the
// garbage collector, setting the corresponding node's state to
// Done, and setting the node's Value).
func (e *Eval) collect(ctx context.Context) {
	if !e.GC {
		return
	}
	// We have two roots for garbage collection:
	// - the Flow itself, for which we find the frontier of completed nodes;
	//   we know that no waiting of ready node can depend on any value behind
	//   this frontier.
	// - the set of writers, whose objects must remain live until they are fully
	//   transferred to or from cache.
	//
	// TODO(marius): ideally we'd rewrite the flow graph itself ot account for
	// cache writes, so that they are handled uniformly here (and elsewhere).
	e.muGC.Lock()
	defer e.muGC.Unlock()
	// Collect all live values
	var livevals []*reflow.Fileset
	e.nlive = 0
	for v := e.root.Visitor(); v.Walk(); {
		fs, ok := v.Value.(reflow.Fileset)
		switch {
		case ok && v.State == Done:
			if n := fs.N(); n > 0 {
				e.nlive += n
				livevals = append(livevals, &fs)
			}
		default:
			v.Visit()
			// Unevaluated maps must also be traversed, since they
			// may introduce dependencies that would otherwise be
			// garbage collected: they're just not evident yet.
			if v.Op == Map {
				v.Push(v.MapFlow)
			}
		}
	}
	var nwriter int
	for w := e.writers; w != nil; w = w.next {
		nwriter++
		fs, _ := w.Flow.Value.(reflow.Fileset)
		if n := fs.N(); n > 0 {
			e.nlive += fs.N()
			livevals = append(livevals, &fs)
		}
	}
	// Construct a bloom filter with a 0.1% false positive rate
	// for our live set.
	var live *bloom.BloomFilter
	if e.nlive > 0 {
		live = bloom.NewWithEstimates(uint(e.nlive), 0.001)
	} else {
		live = bloom.New(64, 1)
	}
	var b bytes.Buffer
	e.livebytes = data.Size(0)
	// Add live files to the filter, count live unique objects.
	e.nlive = 0
	for _, v := range livevals {
		for _, file := range v.Files() {
			b.Reset()
			if _, err := digest.WriteDigest(&b, file.ID); err != nil {
				panic("failed to write file digest " + file.ID.String() + ": " + err.Error())
			}
			e.livebytes += data.Size(file.Size)
			live.Add(b.Bytes())
			e.nlive++
		}
	}
	if e.livebytes > e.maxlivebytes {
		e.maxlivebytes = e.livebytes
	}
	if e.live == nil || !e.live.Equal(live) {
		if err := e.Executor.Repository().Collect(ctx, bloomlive.New(live)); err != nil {
			e.Log.Errorf("collect: %v", err)
		}
	}
	e.live = live
}

// Dirty determines whether node f is (transitively) dirty, and must
// be recomputed. Dirty considers only visible nodes; it does not
// incur extra computation, thus dirtying does not work when dirtying
// nodes are hidden behind maps, continuations, or coercions.
func (e *Eval) dirty(f *Flow) bool {
	if !e.NoCacheExtern {
		return false
	}
	if f.Op == Extern {
		return true
	}
	for _, dep := range f.Deps {
		if e.dirty(dep) {
			return true
		}
	}
	return false
}

// Valid tells whether f's cached results should be considered valid.
func (e *Eval) valid(f *Flow) bool {
	if e.Invalidate == nil {
		return true
	}
	invalid := e.Invalidate(f)
	if invalid {
		e.Log.Debugf("invalidated %v", f)
	}
	return !invalid
}

// Todo adds to e.list the set of ready Flows in f. Todo adds all nodes
// that require evaluation to the provided visitor.
func (e *Eval) todo(f *Flow, visited flowOnce, v *FlowVisitor) {
	if f == nil || !visited.Visit(f) {
		return
	}
	for _, dep := range f.Deps {
		if dep.ExecDepIncorrectCacheKeyBug {
			// If the dependency was affected by the bug, then so is the parent.
			f.ExecDepIncorrectCacheKeyBug = true
			break
		}
	}
	switch f.State {
	case Init:
		f.Pending = make(map[*Flow]bool)
		for _, dep := range f.Deps {
			if dep.State != Done {
				f.Pending[dep] = true
				dep.Dirty = append(dep.Dirty, f)
			}
		}
		switch f.Op {
		case Intern, Exec, Extern:
			if !e.BottomUp && e.CacheMode.Reading() && !e.dirty(f) {
				v.Push(f)
				e.Mutate(f, NeedLookup)
				return
			}
		}
		e.Mutate(f, TODO)
		fallthrough
	case TODO:
		for _, dep := range f.Deps {
			e.todo(dep, visited, v)
		}
		// In the case of multiple dependencies, we short-circuit
		// computation on error. This is because we want to return early,
		// in case it can be dealt with (e.g., by restarting evaluation).
		for _, dep := range f.Deps {
			if dep == nil {
				panic(fmt.Sprintf("op %s n %d", f.Op, len(f.Deps)))
			}
			if dep.State == Done && dep.Err != nil {
				e.Mutate(f, Ready)
				v.Push(f)
				return
			}
		}
		for _, dep := range f.Deps {
			if dep.State != Done {
				return
			}
		}
		// The node is ready to run. This is done according to the evaluator's mode.
		switch f.Op {
		case Intern, Exec, Extern:
			// We're ready to run. If we're in bottom up mode, this means we're ready
			// for our cache lookup.
			if e.BottomUp && e.CacheMode.Reading() {
				e.Mutate(f, NeedLookup)
			} else {
				if e.CacheMode.Reading() {
					e.Mutate(f, NeedTransfer)
				} else {
					e.Mutate(f, Ready)
				}
			}
		default:
			// Other nodes can be computed immediately,
			// and do not need access to the objects.
			e.Mutate(f, Ready)
		}
		v.Push(f)
	default:
		v.Push(f)
	}
}

type kCtx struct {
	context.Context
	repo reflow.Repository
}

func (k kCtx) Repository() reflow.Repository {
	return k.repo
}

// Eval performs a one-step simplification of f. It must be called
// only after all of f's dependencies are ready.
//
// eval also caches results of successful execs if e.Cache is defined.
func (e *Eval) eval(ctx context.Context, f *Flow) (err error) {
	// Propagate errors immediately.
	for _, dep := range f.Deps {
		if err := dep.Err; err != nil {
			e.Mutate(f, err, Done)
			return nil
		}
	}

	// There is a little bit of concurrency trickery here: we must
	// modify Flow's state only after any modifications have been done,
	// in order to make sure we don't race with concurrent e.todos
	// (which may make subsequent Flows available for execution before
	// we have made the necessary modifications here). In effect,
	// f.State acts as a barrier: it is modified only by one routine
	// (the one invoking simplify), but may be read by many.
	begin := time.Now()
	if f.Op != Val {
		defer func() {
			if err != nil {
				// Don't print cancellation errors, since they are follow-on errors
				// from upstream ones that have already been reported.
				/*
					if !errors.Is(errors.E(errors.Canceled), err) {
						e.Log.Errorf("eval %s runtime error: %v", f.ExecString(false), err)
					}
				*/
			} else if f.State == Done && f.Op != K && f.Op != Kctx && f.Op != Coerce {
				f.Runtime = time.Since(begin)
			}
		}()
	}

	switch f.Op {
	case Intern, Extern, Exec:
		var name string
		switch f.Op {
		case Extern:
			name = fmt.Sprintf("extern %s %s", f.URL, data.Size(f.Deps[0].Value.(reflow.Fileset).Size()))
		case Intern:
			name = fmt.Sprintf("intern %s", f.URL)
		case Exec:
			name = fmt.Sprintf("exec %s", f.AbbrevCmd())
		}
		ctx, done := trace.Start(ctx, trace.Exec, f.Digest(), name)
		trace.Note(ctx, "ident", f.Ident)
		defer done()
		if err := e.exec(ctx, f); err != nil {
			return err
		}
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
		e.Mutate(f, fs, Incr, Done)
	case Map:
		v := f.Deps[0].Value.(reflow.Fileset)
		ff := &Flow{
			Op:   Merge,
			Deps: make([]*Flow, len(v.List)),
		}
		for i := range v.List {
			ff.Deps[i] = f.MapFunc(filesetFlow(v.List[i]))
		}
		e.Mutate(f, Fork(ff), Init)
		e.Mutate(f.Parent, Done)
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
		e.Mutate(f, reflow.Fileset{Map: fileset}, Incr, Done)
	case Merge:
		list := make([]reflow.Fileset, len(f.Deps))
		for i, dep := range f.Deps {
			list[i] = dep.Value.(reflow.Fileset)
		}
		e.Mutate(f, reflow.Fileset{List: list}, Incr, Done)
	case Val:
		e.Mutate(f, Incr, Done)
	case Pullup:
		v := &reflow.Fileset{List: make([]reflow.Fileset, len(f.Deps))}
		for i, dep := range f.Deps {
			v.List[i] = dep.Value.(reflow.Fileset)
		}
		e.Mutate(f, v.Pullup(), Incr, Done)
	case K:
		vs := make([]values.T, len(f.Deps))
		for i, dep := range f.Deps {
			vs[i] = dep.Value
		}

		ff := f.K(vs)
		e.Mutate(f, Fork(ff), Init)
		e.Mutate(f.Parent, Done)
	case Kctx:
		vs := make([]values.T, len(f.Deps))
		for i, dep := range f.Deps {
			vs[i] = dep.Value
		}
		ff := f.Kctx(kCtx{ctx, e.repo}, vs)
		e.Mutate(f, Fork(ff), Init)
		e.Mutate(f.Parent, Done)
	case Coerce:
		if v, err := f.Coerce(f.Deps[0].Value); err != nil {
			e.Mutate(f, err, Incr, Done)
		} else {
			e.Mutate(f, Value{v}, Incr, Done)
		}
	case Requirements:
		e.Mutate(f, Value{f.Deps[0].Value}, Incr, Done)
	case Data:
		if id, err := e.repo.Put(ctx, bytes.NewReader(f.Data)); err != nil {
			e.Mutate(f, err, Incr, Done)
		} else {
			e.Mutate(f, reflow.Fileset{
				Map: map[string]reflow.File{
					".": {ID: id, Size: int64(len(f.Data))},
				},
			}, Incr, Done)
		}
	default:
		panic(fmt.Sprintf("bug %v", f))
	}
	switch f.Op {
	case Intern, Extern, Exec:
		if e.TaskDB != nil {
			e.taskdbWriteAsync(ctx, f.Op, f.Inspect, f.Exec, f.TaskID)
		}
	}
	if !e.CacheMode.Writing() {
		e.Mutate(f, Decr)
		return nil
	}
	// We're currently pretty conservative in what we choose to cache:
	// we don't cache interns, nor error values. We should revisit this
	// in the future.
	// TODO(marius): it may be valuable to cache interns as well, since
	// they might be overfetched, and then wittled down later. It is
	// also extra protection for reproducibility, though ideally this will
	// be tackled by filesets.
	e.cacheWriteAsync(ctx, f)
	return nil
}

// CacheWrite writes the cache entry for flow f, with objects in the provided
// source repository. CacheWrite returns nil on success, or else the first error
// encountered.
func (e *Eval) CacheWrite(ctx context.Context, f *Flow, repo reflow.Repository) error {
	switch f.Op {
	case Intern, Extern, Exec:
	default:
		return nil
	}
	// We currently only cache fileset values.
	fs, ok := f.Value.(reflow.Fileset)
	if !ok {
		return nil
	}
	// We don't cache errors, and only completed nodes.
	if f.Err != nil || f.State != Done {
		return nil
	}
	if f.Op == Data {
		return nil
	}
	if e.NoCacheExtern && f.Op == Extern {
		return nil
	}
	keys := f.CacheKeys()
	if len(keys) == 0 {
		return nil
	}
	if err := e.Transferer.Transfer(ctx, e.Repository, repo, fs.Files()...); err != nil {
		return err
	}
	_ = e.marshalLimiter.Acquire(ctx, 1)
	id, err := marshal(ctx, e.Repository, fs)
	e.marshalLimiter.Release(1)
	if err != nil {
		return err
	}
	// Write a mapping for each cache key.
	g, ctx := errgroup.WithContext(ctx)
	for i := range keys {
		key := keys[i]
		g.Go(func() error {
			return e.Assoc.Store(ctx, assoc.Fileset, key, id)
		})
	}
	return g.Wait()
}

func (e *Eval) cacheWriteAsync(ctx context.Context, f *Flow) {
	bgctx := Background(ctx)
	go func() {
		err := e.CacheWrite(bgctx, f, e.repo)
		if err != nil {
			e.Log.Errorf("cache write %v: %v", f, err)
		}
		bgctx.Complete()
		e.Mutate(f, Decr)
	}()
}

func (e *Eval) taskdbWrite(ctx context.Context, op Op, inspect reflow.ExecInspect, exec reflow.Exec, id taskdb.TaskID) error {
	if !op.External() {
		return nil
	}
	var (
		err            error
		stdout, stderr digest.Digest
		pid            digest.Digest
	)
	g, ctx := errgroup.WithContext(ctx)
	if pid, err = marshal(ctx, e.Repository, inspect); err != nil {
		log.Errorf("repository put profile: %v", err)
	}
	if exec != nil {
		if rc, err := exec.Logs(ctx, true, false, false); err == nil {
			if stdout, err = e.Repository.Put(ctx, rc); err != nil {
				log.Errorf("repository put stdout: %v", err)
			}
			rc.Close()
		}
		if rc, err := exec.Logs(ctx, false, true, false); err == nil {
			if stderr, err = e.Repository.Put(ctx, rc); err != nil {
				log.Errorf("repository put stderr: %v", err)
			}
			rc.Close()
		}
	}
	if e.TaskDB != nil {
		g.Go(func() error {
			err := e.TaskDB.SetTaskAttrs(ctx, id, stdout, stderr, pid)
			if err != nil {
				e.Log.Debugf("taskdb settaskattrs: %v", err)
			}
			return nil
		})
	}
	return g.Wait()
}

// TODO(dnicolaou): Change to: taskdbWriteAsync(ctx context.Context, op Op, task *sched.Task) once nonscheduler mode is
// removed.
func (e *Eval) taskdbWriteAsync(ctx context.Context, op Op, inspect reflow.ExecInspect, exec reflow.Exec, id taskdb.TaskID) {
	bgctx := Background(ctx)
	go func() {
		err := e.taskdbWrite(bgctx, op, inspect, exec, id)
		if err != nil {
			e.Log.Errorf("taskdb write %v: %v", id, err)
		}
		bgctx.Complete()
	}()
}

// lookupFailed marks the flow f as having failed lookup. Lookup
// failure is treated differently depending on evaluation mode. In
// bottom-up mode, we're only looked up if our dependencies are met,
// and we always compute on a cache miss, thus we now need to make
// sure our dependencies are available, and the node is marked
// NeedTransfer. In top-down mode, we need to continue traversing
// the graph, and the node is marked TODO.
func (e *Eval) lookupFailed(f *Flow) {
	if e.BottomUp {
		e.Mutate(f, NeedTransfer)
	} else {
		e.Mutate(f, TODO)
	}
}

// BatchLookup performs a cache lookup of a set of flow nodes.
func (e *Eval) batchLookup(ctx context.Context, flows ...*Flow) {
	batch := make(assoc.Batch)
	for _, f := range flows {
		if !e.valid(f) || !e.CacheMode.Reading() || e.NoCacheExtern && (f.Op == Extern || f == e.root) {
			e.lookupFailed(f)
			continue
		}
		keys := f.CacheKeys()
		if len(keys) == 0 {
			// This can't be true now, but in the future it could be valid for nodes
			// to present no cache keys.
			e.lookupFailed(f)
			continue
		}
		for _, key := range keys {
			batch.Add(assoc.Key{assoc.Fileset, key})
		}
		if e.Log.At(log.DebugLevel) {
			e.Log.Debugf("cache.Lookup flow: %s (%s) keys: %s\n", f.Digest().Short(), f.Ident, strings.Join(
				func() []string {
					strs := make([]string, len(keys))
					for i, key := range keys {
						strs[i] = key.Short()
					}
					return strs
				}(), ", "))
		}
	}
	{
		ctx, cancel := context.WithTimeout(ctx, e.CacheLookupTimeout)
		err := e.Assoc.BatchGet(ctx, batch)
		cancel()
		if err != nil {
			e.Log.Errorf("assoc.BatchGet: %v", err)
			for _, f := range flows {
				e.step(f, func(f *Flow) error {
					e.lookupFailed(f)
					return nil
				})
			}
			return
		}
	}
	bg := e.newAssertionsBatchCache()
	for _, f := range flows {
		e.step(f, func(f *Flow) error {
			var (
				keys = f.CacheKeys()
				fs   reflow.Fileset
				fsid digest.Digest
				err  error
			)
			for _, key := range keys {
				res, ok := batch[assoc.Key{Kind: assoc.Fileset, Digest: key}]
				if !ok || res.Digest.IsZero() || res.Error != nil {
					if ok && res.Error != nil {
						e.Log.Errorf("assoc.BatchGet: %v %v", key, res.Error)
					}
					continue
				}
				_ = e.marshalLimiter.Acquire(ctx, 1)
				err = unmarshal(ctx, e.Repository, res.Digest, &fs)
				e.marshalLimiter.Release(1)
				if err == nil {
					e.Log.Debugf("cache.Lookup flow: %s (%s) result from key: %s, value: %s\n", f.Digest().Short(), f.Ident, key.Short(), res.Digest)
					fsid = res.Digest
					break
				}
				if !errors.Is(errors.NotExist, err) {
					e.Log.Errorf("unmarshal %v: %v", res.Digest, err)
				}
			}
			// Nothing was found, so there is no read repair to do.
			// Fail the lookup early.
			if fsid.IsZero() {
				e.lookupFailed(f)
				return nil
			}
			// Make sure all of the files are present in the repository.
			// If they are not, we consider this a cache miss.
			missing, err := missing(ctx, e.Repository, fs.Files()...)
			switch {
			case err != nil:
				if err != ctx.Err() {
					e.Log.Errorf("missing %v: %v", fs, err)
				}
			case len(missing) != 0:
				var total int64
				for _, file := range missing {
					total += file.Size
				}
				err = errors.E(
					errors.NotExist, "cache.Lookup",
					errors.Errorf("missing %d files (%s)", len(missing), data.Size(total)))
			}
			if err != nil {
				e.lookupFailed(f)
				return nil
			}
			// If the cached fileset has viable non-empty assertions, assert them.
			if a, size := reflow.NonEmptyAssertions(fs.Assertions()...); size > 0 {
				// Check if the assertions are internally consistent for the cached fileset.
				if err = e.assertionsConsistent(f, a); err != nil {
					e.Log.Debugf("assertions consistent: %v", err)
					e.lookupFailed(f)
					return nil
				}
				anew, err := e.refreshAssertions(ctx, a, bg)
				if err != nil {
					e.Log.Debugf("refresh assertions: %v", err)
					e.lookupFailed(f)
					return nil
				}
				if !e.Assert(ctx, a, anew) {
					if e.Log.At(log.DebugLevel) {
						if diff := reflow.PrettyDiff(a, anew); diff != "" {
							e.Log.Debugf("flow %s assertions diff:\n%s\n", f.Digest().Short(), diff)
						}
					}
					e.lookupFailed(f)
					return nil
				}
			}
			if e.RecomputeEmpty && fs.AnyEmpty() {
				e.Log.Debugf("recomputing empty value for %v", f)
				e.lookupFailed(f)
				return nil
			}
			// Perform read repair: asynchronously write back all non existent keys.
			writeback := keys[:0]
			for _, key := range keys {
				if res, ok := batch[assoc.Key{Kind: assoc.Fileset, Digest: key}]; !ok || res.Digest.IsZero() || res.Error != nil {
					writeback = append(writeback, key)
				}
			}
			bgctx := Background(ctx)
			go func() {
				for _, key := range writeback {
					if err := e.Assoc.Store(bgctx, assoc.Fileset, key, fsid); err != nil {
						if !errors.Is(errors.Precondition, err) {
							e.Log.Errorf("assoc write for read repair %v %v: %v", f, key, err)
						}
					}
				}
				bgctx.Complete()
			}()
			// The node is marked done. If the needed objects are not later
			// found in the cache's repository, the node will be marked for
			// recomputation.
			e.Mutate(f, fs, Cached, Done)
			if e.BottomUp {
				e.LogFlow(ctx, f)
			}
			return nil
		})
	}
}

// assertionsBatchCache supports caching generated assertions in a batch.
type assertionsBatchCache struct {
	ag reflow.AssertionGenerator
	o  once.Map
	m  sync.Map // map[reflow.AssertionKey]*reflow.Assertions
}

func (e *Eval) newAssertionsBatchCache() *assertionsBatchCache {
	return &assertionsBatchCache{ag: e.AssertionGenerator}
}

// Generate calls the given AssertionGenerator with the given reflow.AssertionKey.
func (g *assertionsBatchCache) Generate(ctx context.Context, ak reflow.AssertionKey) (*reflow.Assertions, error) {
	err := g.o.Do(ak, func() error {
		assertions, err := g.ag.Generate(ctx, ak)
		if err != nil {
			return err
		}
		g.m.Store(ak, assertions)
		return nil
	})
	if err != nil {
		return nil, err
	}
	v, _ := g.m.Load(ak)
	return v.(*reflow.Assertions), nil
}

// refreshAssertions returns assertions with current properties for each subject in the given assertions.
// If provided, the assertionsBatchCache will be used to generate/cache assertions when necessary.
// For each subject, the current value is computed:
// - from the assertions (if it exists)
// - by invoking assertion generators directly or fetching from the assertionsBatchCache (if provided)
func (e *Eval) refreshAssertions(ctx context.Context, list []*reflow.Assertions, bg *assertionsBatchCache) ([]*reflow.Assertions, error) {
	var refreshed []*reflow.Assertions
	var toGenerate []reflow.AssertionKey
	for _, a := range list {
		newA, missing := e.assertions.Filter(a)
		refreshed = append(refreshed, newA)
		toGenerate = append(toGenerate, missing...)
	}
	if len(toGenerate) == 0 {
		refreshed, _ = reflow.NonEmptyAssertions(refreshed...)
		return refreshed, nil
	}
	genAs := make([]*reflow.Assertions, len(toGenerate))
	if err := traverse.Each(len(toGenerate), func(i int) error {
		var err error
		if bg != nil {
			genAs[i], err = bg.Generate(ctx, toGenerate[i])
		} else {
			genAs[i], err = e.AssertionGenerator.Generate(ctx, toGenerate[i])
		}
		return err
	}); err != nil {
		return nil, err
	}
	refreshed = append(refreshed, genAs...)
	refreshed, _ = reflow.NonEmptyAssertions(refreshed...)
	return refreshed, nil
}

// needTransfer returns the file objects that require transfer from flow f.
// It should be called only when caching is enabled.
func (e *Eval) needTransfer(ctx context.Context, f *Flow) ([]reflow.File, error) {
	fs := reflow.Fileset{List: make([]reflow.Fileset, len(f.Deps))}
	for i := range f.Deps {
		fs.List[i] = f.Deps[i].Value.(reflow.Fileset)
	}
	if fs.N() == 0 {
		return nil, nil
	}
	return e.Transferer.NeedTransfer(ctx, e.Executor.Repository(), fs.Files()...)
}

// transfer performs data transfers a node's dependent values. this
// is only done for execs and externs, thus its dependencies are
// guaranteed to contain Fileset dependencies directly.
func (e *Eval) transfer(ctx context.Context, f *Flow) error {
	fs := reflow.Fileset{List: make([]reflow.Fileset, len(f.Deps))}
	for i := range f.Deps {
		fs.List[i] = f.Deps[i].Value.(reflow.Fileset)
	}
	var name string
	switch f.Op {
	case Extern:
		name = fmt.Sprintf("xfer extern %s %s", f.URL, data.Size(f.Deps[0].Value.(reflow.Fileset).Size()))
	case Exec:
		name = fmt.Sprintf("xfer exec %s", f.AbbrevCmd())
	}

	ctx, done := trace.Start(ctx, trace.Transfer, f.Digest(), name)
	trace.Note(ctx, "files", fs.String())
	trace.Note(ctx, "size", float64(fs.Size()))
	defer done()
	err := e.Transferer.Transfer(ctx, e.Executor.Repository(), e.Repository, fs.Files()...)
	if err == nil {
		e.Mutate(f, Ready)
		return nil
	}
	trace.Note(ctx, "error", err.Error())
	e.Log.Errorf("cache transfer %v error: %v", f, err)
	// Errors.Unavailable is considered a transient error, so the
	// underlying runner should restart evaluation.
	return errors.E(errors.Unavailable, "cache.Transfer", err)
}

// assignExecId assigns an Exec ID appropriate for the given Flow
// by merging its digest with the assertions it depends on.
func (e *Eval) assignExecId(ctx context.Context, f *Flow) error {
	as, err := e.refreshAssertions(ctx, f.depAssertions(), nil)
	if err != nil {
		return errors.E("AssignExecID", f.Digest(), err)
	}
	state, err := reflow.MergeAssertions(as...)
	if err != nil {
		return errors.E("AssignExecID", f.Digest(), err)
	}
	f.ExecId = reflow.Digester.FromDigests(f.Digest(), state.Digest())
	return nil
}

// assertionsConsistent checks whether the given set of assertions
// are consistent with the given flow's dependencies.
//
// assertionsConsistent is valid only for Intern, Extern, and Exec ops
// and should be called only after the flow's dependencies
// are done, for it to return a meaningful result.
func (e *Eval) assertionsConsistent(f *Flow, list []*reflow.Assertions) error {
	if !f.Op.External() {
		return nil
	}
	// Add assertions of dependencies of flow.
	as := f.depAssertions()
	as = append(as, list...)
	_, err := reflow.MergeAssertions(as...)
	return err
}

// propagateAssertions propagates assertions from this flow's dependencies (if any)
// to its output.  This must be called after the flow is computed but before
// it is marked as Done.
// propagateAssertions is valid only for Intern and Exec ops.
func (e *Eval) propagateAssertions(f *Flow) error {
	if !f.Op.External() || f.Op == Extern {
		return nil
	}
	fs, ok := f.Value.(reflow.Fileset)
	if !ok {
		return nil
	}
	return fs.AddAssertions(f.depAssertions()...)
}

// exec performs and waits for an exec with the given config.
// exec tries each step up to numExecTries. Exec returns a value
// pointer which has been registered as live.
func (e *Eval) exec(ctx context.Context, f *Flow) error {
	type state int
	const (
		statePut state = iota
		stateWait
		stateInspect
		stateResult
		stateVerify
		statePromote
		stateDone
	)
	var (
		err     error
		x       reflow.Exec
		r       reflow.Result
		n       = 0
		s       = statePut
		id      = f.Digest()
		cfg     = f.ExecConfig()
		tcancel context.CancelFunc
		tctx    context.Context
	)

	// TODO(marius): we should distinguish between fatal and nonfatal errors.
	// The fatal ones are useless to retry.

	defer func() {
		if tcancel == nil {
			return
		}
		terr := err
		if err == nil {
			terr = r.Err
		}
		if tdbErr := e.TaskDB.SetTaskComplete(context.Background(), f.TaskID, terr, time.Now()); tdbErr != nil {
			e.Log.Debugf("taskdb settaskcomplete: %v\n", tdbErr)
		}
		tcancel()
	}()
	for n < numExecTries && s < stateDone {
		switch s {
		case statePut:
			if e.TaskDB != nil {
				// disable govet check due to https://github.com/golang/go/issues/29587
				tctx, tcancel = context.WithCancel(ctx) //nolint: govet
				err = e.TaskDB.CreateTask(tctx, f.TaskID, e.RunID, id, taskdb.NewImgCmdID(cfg.Image, cfg.Cmd), cfg.Ident, "")
				if err != nil {
					e.Log.Debugf("taskdb createtask: %v\n", err)
				}
				go func() { _ = taskdb.KeepTaskAlive(tctx, e.TaskDB, f.TaskID) }()
			}
			x, err = e.Executor.Put(ctx, f.ExecId, cfg)
			if err == nil {
				f.Exec = x
				e.LogFlow(ctx, f)
			}
		case stateWait:
			err = x.Wait(ctx)
			if e.TaskDB != nil {
				if tdbErr := e.TaskDB.SetTaskResult(tctx, f.TaskID, x.ID()); tdbErr != nil {
					e.Log.Debugf("taskdb settaskresult: %v\n", tdbErr)
				}
				tcancel()
			}
		case stateInspect:
			f.Inspect, err = x.Inspect(ctx)
		case stateResult:
			r, err = x.Result(ctx)
		case stateVerify:
			err = traverse.Each(f.NExecArg(), func(i int) error {
				earg := f.ExecArg(i)
				if earg.Out {
					return nil
				}
				fs := f.Deps[earg.Index].Value.(reflow.Fileset)
				return e.Executor.VerifyIntegrity(ctx, fs)
			})
		case statePromote:
			e.Mutate(f, r.Fileset, Incr, Propagate)
			err = x.Promote(ctx)
		}
		if err != nil {
			n++
		} else {
			n = 0
			s++
			if s == stateVerify && !e.PostUseChecksum {
				s++
			}
		}
	}
	if err != nil {
		if s > stateResult {
			e.Mutate(f, Decr)
		}
		return err
	}
	e.Mutate(f, r.Err, Done)
	return nil
}

// Live registers value v as being live. Live implements a safepoint:
// it returns only when the value v has been considered live with
// respect to the garbage collector.
func (e *Eval) incr(f *Flow) {
	e.writersMu.Lock()
	e.writers = &writer{f, e.writers}
	e.writersMu.Unlock()
}

// Dead unregisters the value v as being live, and (asynchronously)
// requests a garbage collection.
func (e *Eval) decr(f *Flow) {
	e.writersMu.Lock()
	defer e.writersMu.Unlock()
	for p := &e.writers; *p != nil; {
		if (*p).Flow == f {
			*p = (*p).next
			return
		}
		if *p != nil {
			p = &(*p).next
		}
	}
	e.wakeup(true)
}

// Fork is a an argument to (*Eval).Mutate to indicate a fork mutation.
type Fork *Flow

// Value is an argument to (*Eval).Mutate to indicate a set-value mutation.
type Value struct{ Value values.T }

// Mutation is a type of mutation.
type Mutation int

// Reserve adds resources to the flow's reservation.
type Reserve reflow.Resources

// Unreserve subtracts resources from the flow's reservation.
type Unreserve reflow.Resources

// Status amends the task's status string.
type Status string

const (
	// Incr is the mutation that increments the reference count used for
	// GC.
	Incr Mutation = iota
	// Decr is the mutation that decrements the reference count used for
	// GC.
	Decr
	// Cached is the mutation that sets the flow's flag.
	Cached
	// Refresh is the mutation that refreshes the status of the flow node.
	Refresh
	// MustIntern sets the flow's MustIntern flag to true.
	MustIntern
	// NoStatus indicates that a flow node's status should not be updated.
	NoStatus
	// Propagate is the mutation that propagates a flow's dependency assertions
	// to the flow's result Fileset.  Results in a no-op if the flow has no result fileset.
	Propagate
)

// Mutate safely applies a set of mutations vis-a-vis the garbage
// collector. Mutations may be applied concurrently with each other;
// mutations are not applied during garbage collection.
func (e *Eval) Mutate(f *Flow, muts ...interface{}) {
	if e.Trace != nil {
		strs := make([]string, len(muts))
		for i := range muts {
			strs[i] = fmt.Sprint(muts[i])
		}
		e.Trace.Printf("mutate %s: %v", f, strings.Join(strs, ", "))
	}
	e.muGC.RLock()
	var (
		prevState, thisState State
		refresh              bool
		statusOk             = true
	)
	for _, mut := range muts {
		switch arg := mut.(type) {
		case error:
			if arg != nil {
				f.Err = errors.Recover(arg)
			}
		case State:
			prevState = f.State
			thisState = arg
			f.State = arg
		case reflow.Fileset:
			f.Value = values.T(arg)
		case Fork:
			f.Fork(arg)
		case Value:
			f.Value = arg.Value
		case Mutation:
			switch arg {
			case Incr:
				e.incr(f)
			case Decr:
				e.decr(f)
			case Cached:
				f.Cached = true
			case Refresh:
				refresh = true
			case MustIntern:
				f.MustIntern = true
			case NoStatus:
				statusOk = false
			case Propagate:
				if err := e.propagateAssertions(f); err != nil {
					panic(fmt.Errorf("unexpected propagation error: %v", err))
				}
			}
		case Reserve:
			f.Reserved.Add(f.Reserved, reflow.Resources(arg))
		case Unreserve:
			f.Reserved.Sub(f.Reserved, reflow.Resources(arg))
		default:
			panic(fmt.Sprintf("invalid argument type %T", arg))
		}
	}
	e.muGC.RUnlock()
	// Assign an ExecId for Execing (external) flows. In the scheduler case, f.ExecId is a random digest and is only set
	// if the digest is its zero value.
	if f.Op.External() && f.State == Execing {
		switch {
		case e.Scheduler == nil:
			if err := e.assignExecId(context.Background(), f); err != nil {
				f.Err = errors.Recover(errors.E("assign execid", f.Digest(), errors.Temporary, err))
			}
		// TODO(dnicolaou): Remove ExecId from scheduler mode once eval_test.go scheduler tests no longer
		// require ExecId to keep track of execs and their respective tasks. f.ExecId.IsZero() check exists
		// to ensure that eval_test.go scheduler tests will pass because in these tests, each flow is provided
		// an ExecId that must remain unchanged.
		case e.Scheduler != nil && f.ExecId.IsZero():
			f.ExecId = reflow.Digester.Rand(nil)
		}
	}
	// When a flow is done (without errors), add all its assertions to the vector clock.
	if f.Op.External() && f.State == Done && f.Err == nil {
		err := e.assertions.AddFrom(f.Value.(reflow.Fileset).Assertions()...)
		if err != nil {
			f.Err = errors.Recover(errors.E("adding assertions", f.Digest(), errors.Temporary, err))
		}
	}
	// Update task status, if applicable.
	if e.Status == nil {
		return
	}
	switch f.Op {
	case Exec, Intern, Extern:
	default:
		return
	}
	if (thisState == Transfer || thisState == Running || thisState == Execing) && f.Status == nil && statusOk {
		// TODO(marius): digest? fmt("%-*s %s", n, ident, f.Digest().Short())
		f.Status = e.Status.Start(f.Ident)
	}
	if f.Status == nil || (!refresh && prevState == thisState) {
		return
	}
	var status string
	switch f.State {
	case Done:
		if f.Err != nil {
			status = fmt.Sprintf("%s error %v", f.Op, f.Err)
		} else {
			switch f.Op {
			case Extern:
				status = "done"
			case Exec, Intern:
				status = fmt.Sprintf("done %s", data.Size(f.Value.(reflow.Fileset).Size()))
			}
		}
	case Running, Execing:
		switch f.Op {
		case Extern:
			var sz string
			if fs, ok := f.Deps[0].Value.(reflow.Fileset); ok {
				sz = fmt.Sprintf(" %s", data.Size(fs.Size()))
			}
			status = fmt.Sprintf("%s%s", f.URL, sz)
		case Intern:
			status = fmt.Sprintf("%s", f.URL)
		case Exec:
			status = f.AbbrevCmd()
		}
	case Transfer:
		if f.TransferSize > 0 {
			status = fmt.Sprintf("%s", data.Size(f.TransferSize))
		}
	case Ready:
		status = "waiting"
	}
	if f.StatusAux != "" {
		status = fmt.Sprintf("%s (%s) %s", f.Op, f.StatusAux, status)
	} else {
		status = f.Op.String() + " " + status
	}
	f.Status.Print(status)
	if f.State == Done {
		f.Status.Done()
		f.Status = nil
	}
}

const (
	nabbrev      = 60
	nabbrevImage = 40
)

var statusPrinters = [maxOp]struct {
	// Print run status, when a task is kicked off
	// (transfer or exec).
	run func(io.Writer, *Flow)
	// Print detailed task information. These are printed
	// when log=debug and on errors.
	debug func(io.Writer, *Flow)
}{
	Exec: {
		run: func(w io.Writer, f *Flow) { io.WriteString(w, f.AbbrevCmd()) },
		debug: func(w io.Writer, f *Flow) {
			argv := make([]interface{}, len(f.Argstrs))
			for i := range f.Argstrs {
				argv[i] = f.Argstrs[i]
			}
			cmd := fmt.Sprintf(f.Cmd, argv...)

			if f.Exec != nil {
				fmt.Fprintln(w, f.Exec.URI())
			}
			fmt.Fprintln(w, f.Image)

			fmt.Fprintln(w, "command:")
			lines := strings.Split(cmd, "\n")
			// Trim empty prefix and suffix lines.
			for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
				lines = lines[1:]
			}
			for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
				lines = lines[:len(lines)-1]
			}
			// Strip common prefix
			switch len(lines) {
			case 0:
			case 1:
				lines[0] = strings.TrimSpace(lines[0])
			default:
				// The first line can't be empty because we trim them above,
				// so this determines our prefix.
				var prefix = spacePrefix(lines[0])
				for i := range lines {
					lines[i] = strings.TrimRightFunc(lines[i], unicode.IsSpace)
					// Skip empty lines; they shouldn't be able to mess up our prefix.
					if lines[i] == "" {
						continue
					}
					if !strings.HasPrefix(lines[i], prefix) {
						linePrefix := spacePrefix(lines[i])
						if strings.HasPrefix(prefix, linePrefix) {
							prefix = linePrefix
						} else {
							prefix = ""
						}
					}
				}
				for i, line := range lines {
					lines[i] = strings.TrimPrefix(line, prefix)
				}
			}
			for _, line := range lines {
				fmt.Fprintln(w, "   ", line)
			}
			fmt.Fprintln(w, "where:")
			for i, arg := range f.Argstrs {
				if f.ExecArg(i).Out {
					continue
				}
				fmt.Fprintf(w, "    %s = \n", arg)
				if fs, ok := f.Deps[f.ExecArg(i).Index].Value.(reflow.Fileset); ok {
					printFileset(w, "        ", fs)
				} else {
					fmt.Fprintln(w, "        (cached)")
				}
			}
			if f.State != Done {
				return
			}
			if f.Err != nil {
				if f.Exec != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if rc, err := f.Exec.Logs(ctx, true, false, false); err == nil {
						fmt.Fprintln(w, "stdout:")
						s := bufio.NewScanner(rc)
						for s.Scan() {
							fmt.Fprintln(w, "   ", s.Text())
						}
						rc.Close()
					} else {
						fmt.Fprintf(w, "error retrieving stdout: %v\n", err)
					}
					if rc, err := f.Exec.Logs(ctx, false, true, false); err == nil {
						fmt.Fprintln(w, "stderr:")
						s := bufio.NewScanner(rc)
						for s.Scan() {
							fmt.Fprintln(w, "   ", s.Text())
						}
						rc.Close()
					} else {
						fmt.Fprintf(w, "error retrieving stderr: %v\n", err)
					}
				}
			} else {
				fmt.Fprintln(w, "result:")
				seen := make(map[int]bool)
				n := f.NExecArg()
				for i := 0; i < n; i++ {
					earg := f.ExecArg(i)
					if !earg.Out || seen[earg.Index] {
						continue
					}
					fmt.Fprintf(w, "    %s =\n", f.Argstrs[i])
					printFileset(w, "        ", f.Value.(reflow.Fileset).List[earg.Index])
					seen[earg.Index] = true
				}
			}
			fmt.Fprintln(w, "profile:")
			profile := f.Inspect.Profile
			fmt.Fprintf(w, "    cpu mean=%.1f max=%.1f\n", profile["cpu"].Mean, profile["cpu"].Max)
			fmt.Fprintf(w, "    mem mean=%s max=%s\n", data.Size(profile["mem"].Mean), data.Size(profile["mem"].Max))
			fmt.Fprintf(w, "    disk mean=%s max=%s\n", data.Size(profile["disk"].Mean), data.Size(profile["disk"].Max))
			fmt.Fprintf(w, "    tmp mean=%s max=%s\n", data.Size(profile["tmp"].Mean), data.Size(profile["tmp"].Max))
		},
	},
	Intern: {
		run: func(w io.Writer, f *Flow) {
			url := leftabbrev(f.URL.String(), nabbrev)
			io.WriteString(w, url)
		},
		debug: func(w io.Writer, f *Flow) {
			if f.State != Done {
				return
			}
			if f.Err == nil {
				fmt.Fprintln(w, "result:")
				if fs, ok := f.Value.(reflow.Fileset); ok {
					printFileset(w, "    ", fs)
				} else {
					fmt.Fprintln(w, "    (cached)")
				}
			}
		},
	},
	Extern: {
		run: func(w io.Writer, f *Flow) {
			url := leftabbrev(f.URL.String(), nabbrev)
			io.WriteString(w, url)
			io.WriteString(w, " ")
			// The dep may not have a fileset in case the extern was satisfied from cache directly.
			if fs, ok := f.Deps[0].Value.(reflow.Fileset); ok {
				io.WriteString(w, data.Size(fs.Size()).String())
			}
		},
	},
	Pullup: {
		debug: func(w io.Writer, f *Flow) {
			fmt.Fprintln(w, "value:")
			printFileset(w, "    ", f.Value.(reflow.Fileset))
		},
	},
}

// LogFlow logs flow f's state, and then tracks it for future logging.
func (e *Eval) LogFlow(ctx context.Context, f *Flow) {
	f.Tracked = f.State != Done
	var b bytes.Buffer
	if f.State == Done {
		if f.Cached {
			b.WriteString("(<-) ")
		} else {
			b.WriteString(" <-  ")
		}
	} else {
		b.WriteString(" ->  ")
	}
	state := f.State.String()
	switch f.State {
	case Running:
		state = "run"
	case Execing:
		state = "exec"
	case Done:
		if f.Err != nil {
			state = "err"
		} else {
			state = "ok"
		}
	case Transfer:
		state = "xfer"
	}
	fmt.Fprintf(&b, "%-12s %s %-4s %6s ", f.Ident, f.Digest().Short(), state, f.Op.String())
	pr := statusPrinters[f.Op]
	switch f.State {
	case Running, Execing:
		if pr.run != nil {
			pr.run(&b, f)
		}
	case Transfer:
		b.WriteString(f.TransferSize.String())
	case Done:
		printRuntimeStats(&b, f)
	}
	if f.Err != nil {
		fmt.Fprintf(&b, "\n\terror %v\n", f.Err)
		fmt.Fprintf(&b, "\t%s\n", f.Position)
		if pr.debug != nil {
			pr.debug(newPrefixWriter(&b, "\t"), f)
		}
	}
	if f.Err != nil {
		e.Log.Print(b.String())
	} else {
		e.Log.Debug(b.String())
	}

	// We perform debug logging for successful flows with a debug
	// printer and also for cache transfers, where having extra
	// digest information is helpful.
	if f.State != Transfer && (f.Err != nil || pr.debug == nil) {
		return
	}
	b.Reset()
	fmt.Fprintf(&b, "%s %v %s:\n", f.Ident, f.Digest().Short(), f.Position)
	if f.Op == Exec {
		logResources := make(reflow.Resources)
		switch {
		case f.State == Done:
			// Get resources from ExecInspect if the flow is done
			// because `f.Reserved` may have already been cleared
			// by `returnFlow()`.
			logResources.Set(f.Inspect.Config.Resources)
		default:
			logResources.Set(f.Reserved)
		}
		fmt.Fprintf(&b, "\tresources: %s\n", logResources)
	}
	for _, key := range f.CacheKeys() {
		fmt.Fprintf(&b, "\t%s\n", key)
	}
	if f.State == Transfer {
		fmt.Fprintf(&b, "\ttransfer: %s\n", f.Digest())
	}
	if pr.debug != nil {
		pr.debug(newPrefixWriter(&b, "\t"), f)
	}
	if f.Err != nil {
		e.Log.Print(b.String())
	} else {
		e.Log.Debug(b.String())
	}
}

// taskWait waits for a task to finish running and updates the flow, cache, and taskdb accordingly.
func (e *Eval) taskWait(ctx context.Context, f *Flow, task *sched.Task) error {
	if err := task.Wait(ctx, sched.TaskRunning); err != nil {
		return err
	}
	// Grab the task's exec so that it can be logged properly.
	f.Exec = task.Exec
	e.LogFlow(ctx, f)
	if err := task.Wait(ctx, sched.TaskDone); err != nil {
		return err
	}
	f.Inspect = task.Inspect
	if task.Err != nil {
		e.Mutate(f, task.Err, Done)
	} else {
		e.Mutate(f, task.Result.Err, task.Result.Fileset, Propagate, Done)
	}
	if e.TaskDB != nil {
		e.taskdbWriteAsync(ctx, f.Op, task.Inspect, task.Exec, task.ID)
	}
	return nil
}

func (e *Eval) newTask(f *Flow) *sched.Task {
	t := sched.NewTask()
	t.ID = taskdb.TaskID(f.ExecId)
	t.RunID = e.RunID
	t.FlowID = f.Digest()
	t.Config = f.ExecConfig()
	t.Log = e.Log.Tee(nil, fmt.Sprintf("scheduler task %s (flow %s): ", t.ID.IDShort(), t.FlowID.Short()))
	return t
}

// reviseResources revises the resources of the submitted tasks and flows, if applicable.
func (e *Eval) reviseResources(ctx context.Context, tasks []*sched.Task, flows []*Flow) {
	if e.Predictor == nil {
		return
	}
	predictions := e.Predictor.Predict(ctx, tasks...)
	for i, task := range tasks {
		if predicted, ok := predictions[task]; ok {
			oldResources := task.Config.Resources.String()
			f := flows[i]
			newReserved := make(reflow.Resources)
			newReserved.Set(f.Reserved)
			for k, v := range predicted.Resources {
				newReserved[k] = v
			}
			newReserved["mem"] = math.Max(newReserved["mem"], minExecMemory)
			e.Mutate(f, Unreserve(f.Reserved), Reserve(newReserved))
			task.Config = f.ExecConfig()
			e.Log.Debugf("task %s (flow %s): modifying resources from %s to %s", task.ID.IDShort(), task.FlowID.Short(), oldResources, task.Config.Resources)
			task.ExpectedDuration = predicted.Duration
			e.Log.Debugf("task %s (flow %s): predicted duration %s", task.ID.IDShort(), task.FlowID.Short(), predicted.Duration.Round(time.Second))
		}
	}
}

// retryTask retries a task with the specified resources and waits for it to complete.
func (e *Eval) retryTask(ctx context.Context, f *Flow, resources reflow.Resources, retryType, msg string) (*sched.Task, error) {
	// Apply ExecReset so that the exec can be resubmitted to the scheduler with the flow's
	// exec runtime parameters reset.
	f.ExecReset()
	e.Mutate(f, Unreserve(f.Reserved), Reserve(resources), Execing)
	task := e.newTask(f)
	e.Log.Printf("flow %s: %s: re-submitting task %s with %s", f.Digest().Short(), retryType, task.ID.IDShort(), msg)
	e.Scheduler.Submit(task)
	return task, e.taskWait(ctx, f, task)
}

// oomAdjust returns a new set of resources with increased memory.
// TODO(dnicolaou): Adjust based on actual used memory instead of allocated.
func oomAdjust(specified, used reflow.Resources) reflow.Resources {
	newResources := make(reflow.Resources)
	newResources.Set(used)
	if specified["mem"] > used["mem"] {
		newResources["mem"] = specified["mem"]
	} else {
		newResources["mem"] *= memMultiplier
	}
	return newResources
}

func accumulate(flows []*Flow) (int, string) {
	count := map[string]int{}
	n := 0
	for _, f := range flows {
		id := f.Ident
		if id == "" {
			id = "anon"
		}
		count[id]++
		n++
	}
	ids := make([]string, len(count))
	i := 0
	for k := range count {
		ids[i] = k
		i++
	}
	sort.Strings(ids)
	for i, id := range ids {
		ids[i] = fmt.Sprintf("%s:%d", id, count[id])
	}
	return n, strings.Join(ids, " ")
}

// A Stealer coordinates work stealing with an Eval. A Stealer
// instance is obtained by (*Eval).Stealer.
type Stealer struct {
	mu     sync.Mutex
	e      *Eval
	max    reflow.Resources
	closed bool
	c      chan *Flow
	next   *Stealer
}

func newStealer(e *Eval) *Stealer {
	return &Stealer{e: e}
}

// Admit returns a channel that will return a stolen Flow node that
// makes use of at most max resources. Only one Admit can be active
// at a time: if Admit is called while another is outstanding, the first
// Admit is cancelled, closing its channel.
func (s *Stealer) Admit(max reflow.Resources) <-chan *Flow {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c != nil {
		select {
		case f := <-s.c:
			s.e.returnch <- f
		default:
		}
		close(s.c)
	}
	s.max = max
	s.c = make(chan *Flow, 1)
	s.e.wakeup(false)
	return s.c
}

func (s *Stealer) admit(f *Flow) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c == nil || !s.max.Available(f.Resources) {
		return false
	}
	s.max = reflow.Resources{}
	s.c <- f
	return true
}

// Return returns a stolen Flow to the evaluator.
func (s *Stealer) Return(f *Flow) {
	s.e.returnch <- f
}

// Close discards the Stealer and returns any potential pending
// Flows. Admit should not be called after Close.
func (s *Stealer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c != nil {
		select {
		case f := <-s.c:
			s.e.returnch <- f
		default:
		}
		close(s.c)
	}
	s.max = reflow.Resources{}
	s.closed = true
	s.e.wakeup(false)
}

type writer struct {
	*Flow
	next *writer
}

func round(d time.Duration) time.Duration {
	return d - d%time.Second
}

func spacePrefix(s string) string {
	var prefix string
	for _, r := range s {
		if !unicode.IsSpace(r) {
			break
		}
		prefix += string(r)
	}
	return prefix
}

func printRuntimeStats(b *bytes.Buffer, f *Flow) {
	b.WriteString(round(f.Inspect.Runtime()).String())
	if fs, ok := f.Value.(reflow.Fileset); ok {
		b.WriteString(" ")
		b.WriteString(data.Size(fs.Size()).String())
	} else {
		b.WriteString(" ?")
	}
}

func printFileset(w io.Writer, prefix string, fs reflow.Fileset) {
	switch {
	case len(fs.List) > 0:
		for i := range fs.List {
			fmt.Fprintf(w, "%slist[%d]:\n", prefix, i)
			printFileset(w, prefix+"  ", fs.List[i])
		}
	case len(fs.Map) > 0:
		var keys []string
		for key := range fs.Map {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			file := fs.Map[key]
			fmt.Fprintf(w, "%s%s %s %s\n", prefix, key, file.ID, data.Size(file.Size))
		}
	}
}

// Below are some utilities for dealing with repositories and assocs.
// They are in package reflow in order to avoid too much dependencies
// on other packages, but we should refactor Reflow's dependencies to
// accomodate better for this.

// Marshal marshals the value v and stores it in the provided
// repository. The digest of the contents of the marshaled content is
// returned.
func marshal(ctx context.Context, repo reflow.Repository, v interface{}) (digest.Digest, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return digest.Digest{}, err
	}
	return repo.Put(ctx, bytes.NewReader(b))
}

// Unmarshal unmarshals the value named by digest k into v.
// If the value does not exist in repository, an error is returned.
func unmarshal(ctx context.Context, repo reflow.Repository, k digest.Digest, v interface{}) error {
	rc, err := repo.Get(ctx, k)
	if err != nil {
		return err
	}
	defer rc.Close()
	return json.NewDecoder(rc).Decode(v)
}

// Missing returns the files in files that are missing from
// repository r. Missing returns an error if any underlying
// call fails.
func missing(ctx context.Context, r reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	exists := make([]bool, len(files))
	g, ctx := errgroup.WithContext(ctx)
	for i, file := range files {
		i, file := i, file
		g.Go(func() error {
			_, err := r.Stat(ctx, file.ID)
			if err == nil {
				exists[i] = true
			} else if errors.Is(errors.NotExist, err) {
				return nil
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	all := files
	files = nil
	for i := range exists {
		if !exists[i] {
			files = append(files, all[i])
		}
	}
	return files, nil
}

type counters [Max]map[string]int

func (c *counters) Incr(state State, name string, n int) {
	if c[state] == nil {
		c[state] = make(map[string]int)
	}
	c[state][name] += n
}

func (c counters) Equal(d counters) bool {
	return equal(c, d) && equal(d, c)
}

func (c counters) N(state State) int {
	var n int
	for _, v := range c[state] {
		n += v
	}
	return n
}

func equal(c, d counters) bool {
	for state := range c {
		cc, dc := c[state], d[state]
		for key, val := range cc {
			if val != dc[key] {
				return false
			}
		}
	}
	return true
}

var humanState = map[State]string{
	Running:  "running",
	Execing:  "executing",
	Transfer: "transferring",
	Ready:    "waiting",
}
