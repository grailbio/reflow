// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

//go:generate stringer -type=Mutation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/status"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/predictor"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/values"
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

	// oomRetryMaxExecMemory is max memory allowed for execs being retried due to OOM.
	oomRetryMaxExecMemory = 800 << 30

	// memSuggestThreshold is the minimum fraction of allocated memory an exec can use before a suggestion is
	// displayed to use less memory.
	memSuggestThreshold = 0.6

	// maxFilesInFilesetForNoConcurrenyLimit is the maximum number of files a fileset can have
	// to avoid being throttled in the case of concurrent marshaling of the fileset.
	// TODO(swami): Tune this based on cached filesets
	maxFilesInFilesetForNoConcurrenyLimit = 100

	// maxSizeForNoConcurrencyLimit is the maximum size of the serialized fileset
	// which can be unmarshalled without a concurrency limit.
	// TODO(swami): Tune this based on cached filesets
	maxSizeForNoConcurrencyLimit = 1 * 1024 * 1024 // 1MiB

	// sizePerToken helps determine the number of tokens to acquire from the limiter
	// based on the size of the file to unmarshal.
	// If the file is too big (ie, bigger than sizePerToken * numCPUs), then we simply
	// prevent more than one file of such size to be processed concurrently.
	sizePerToken = 16 * 1024 * 1024 // 16MiB
)

const defaultCacheLookupTimeout = 20 * time.Minute

// stateStatusOrder defines the order in which differenet flow
// statuses are rendered.
var stateStatusOrder = []State{
	Execing, Running, Ready, Done,

	// DEBUG:
	NeedLookup, Lookup, TODO,
}

// Snapshotter provides an interface for snapshotting source URL data into
// unloaded filesets.
type Snapshotter interface {
	Snapshot(ctx context.Context, url string) (reflow.Fileset, error)
}

// EvalConfig provides runtime configuration for evaluation instances.
type EvalConfig struct {

	// Scheduler is used to run tasks.
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

	// Repository is the main, shared repository between evaluations.
	Repository reflow.Repository

	// Assoc is the main, shared assoc that is used to store cache and
	// metadata associations.
	Assoc assoc.Assoc

	// AssertionGenerator is the implementation for generating assertions.
	AssertionGenerator reflow.AssertionGenerator

	// Assert is the policy to use for asserting cached Assertions.
	Assert reflow.Assert

	// RunID is a unique identifier for the run
	RunID taskdb.RunID

	// CacheMode determines whether the evaluator reads from
	// or writees to the cache. If CacheMode is nonzero, Assoc,
	// Repository, and Transferer must be non-nil.
	CacheMode infra2.CacheMode

	// NoCacheExtern determines whether externs are cached.
	NoCacheExtern bool

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
	if e.Scheduler != nil {
		fmt.Fprintf(&b, "scheduler %T", e.Scheduler)
	}
	if e.Snapshotter != nil {
		fmt.Fprintf(&b, " snapshotter %T", e.Snapshotter)
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
	if e.DotWriter != nil {
		fmt.Fprintf(&b, " dotwriter(%T)", e.DotWriter)
	}
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
	assertions *reflow.RWAssertions

	// A channel for evaluation errors.
	errors chan error
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

	// these maintain status printing state
	begin           time.Time
	prevStateCounts counters

	returnch chan *Flow

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
		EvalConfig: config,
		root:       root.Canonicalize(config.Config),
		assertions: reflow.NewRWAssertions(reflow.NewAssertions()),
		errors:     make(chan error),
		returnch:   make(chan *Flow, 1024),
		pending:    newWorkingset(),
	}

	// We require a snapshotter for delayed loads when using a scheduler.
	if e.Scheduler != nil && e.Snapshotter == nil {
		panic("snapshotter must be set when scheduler is set")
	}
	if e.CacheLookupTimeout == time.Duration(0) {
		e.CacheLookupTimeout = defaultCacheLookupTimeout
	}
	if e.Log == nil && printAllTasks {
		e.Log = log.Std
	}
	if e.DotWriter != nil {
		e.flowgraph = simple.NewDirectedGraph()
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
// This provides a simple evaluation scheme that also does not leave
// any parallelism "on the ground".
//
// Eval employs a conservative admission controller to ensure that we
// do not exceed available resources.
//
// The root flow is canonicalized before evaluation.
//
// TODO(marius): wait for all nodes to complete before returning
// (early) when cancelling...
//
func (e *Eval) Do(ctx context.Context) error {
	defer func() {
		if e.DotWriter != nil && e.flowgraph != nil {
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
			if e.Snapshotter != nil && f.Op == Intern && f.State == Ready && !f.MustIntern {
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
			} else if f.Op.External() && f.State == Ready {
				// If we're using a scheduler, then we can skip transfer, and
				// submit directly to the scheduler.
				e.Mutate(f, NeedSubmit)
			}

			switch f.State {
			case NeedLookup:
				// TODO(marius): we should perform batch lookups
				// as the underyling APIs (e.g., to DynamoDB) do not
				// bundle requests automatically.
				e.Mutate(f, Lookup)
				e.pending.Add(f)
				lookupFlows = append(lookupFlows, f)
			case Ready:
				state := Running
				if f.Op.External() {
					state = Execing
				}
				e.Mutate(f, state, SetReserved(f.Resources))
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
				e.Mutate(f, Execing, SetReserved(f.Resources))
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
						msg := fmt.Sprintf("%v of memory (%v/%v) due to OOM error: %s",
							data.Size(resources["mem"]), retries+1, maxOOMRetries, task.Result.Err)
						var err error
						if task, err = e.retryTask(ctx, f, resources, "OOM", msg); err != nil {
							return err
						}
					}
					// Write to the cache only if a task was successfully completed.
					if e.CacheMode.Writing() && task.Err == nil && task.Result.Err == nil {
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
	fmt.Fprintln(&tw, "ident\tn\tncache\truntime(m)\tcpu\tmem(GiB)\tdisk(GiB)\ttmp(GiB)\trequested")
	idents := make([]string, 0, len(stats))
	for ident := range stats {
		idents = append(idents, ident)
	}
	sort.Strings(idents)
	var warningIdents []string
	const byteScale = 1.0 / (1 << 30)
	for _, ident := range idents {
		stats := stats[ident]
		fmt.Fprintf(&tw, "%s\t%d\t%d", ident, stats.N, stats.Ncache)
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
		for _, f := range e.needLog {
			e.LogFlow(ctx, f)
		}
		e.needLog = nil
		select {
		case <-ctx.Done():
			return ctx.Err()
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
	var stateCounts counters
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
		case Ready, Done, TODO, Running, Execing:
			stateCounts.Incr(v.State, v.Ident, 1)
		}
	}
	e.prevStateCounts = stateCounts
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
	for _, state := range []State{Execing, Running, Ready} {
		n := stateCounts.N(state)
		if n == 0 {
			continue
		}
		fmt.Fprintf(&b, ", %s:%d", humanState[state], n)
	}
	fmt.Fprintf(&b, ", completed: %d/%d",
		stateCounts.N(Done),
		stateCounts.N(Done)+stateCounts.N(Execing)+stateCounts.N(Running)+stateCounts.N(TODO))
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
	e.Mutate(f, SetReserved(nil))
	if f.Tracked && f.State == Done {
		e.needLog = append(e.needLog, f)
	}
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
				e.Mutate(f, Ready)
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

	// TODO(swami): Remove this conditional panic.
	//  Temporarily keeping this here to make sure nothing breaks.
	if f.Op.External() {
		panic(fmt.Sprintf("unexpected external op: %s", f))
	}

	switch f.Op {
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
		e.Mutate(f, fs, Done)
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
		e.Mutate(f, reflow.Fileset{Map: fileset}, Done)
	case Merge:
		list := make([]reflow.Fileset, len(f.Deps))
		for i, dep := range f.Deps {
			list[i] = dep.Value.(reflow.Fileset)
		}
		e.Mutate(f, reflow.Fileset{List: list}, Done)
	case Val:
		e.Mutate(f, Done)
	case Pullup:
		v := &reflow.Fileset{List: make([]reflow.Fileset, len(f.Deps))}
		for i, dep := range f.Deps {
			v.List[i] = dep.Value.(reflow.Fileset)
		}
		e.Mutate(f, v.Pullup(), Done)
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
		ff := f.Kctx(kCtx{ctx, e.Repository}, vs)
		e.Mutate(f, Fork(ff), Init)
		e.Mutate(f.Parent, Done)
	case Coerce:
		if v, err := f.Coerce(f.Deps[0].Value); err != nil {
			e.Mutate(f, err, Done)
		} else {
			e.Mutate(f, Value{v}, Done)
		}
	case Requirements:
		e.Mutate(f, Value{f.Deps[0].Value}, Done)
	case Data:
		if id, err := e.Repository.Put(ctx, bytes.NewReader(f.Data)); err != nil {
			e.Mutate(f, err, Done)
		} else {
			e.Mutate(f, reflow.Fileset{
				Map: map[string]reflow.File{
					".": {ID: id, Size: int64(len(f.Data))},
				},
			}, Done)
		}
	default:
		panic(fmt.Sprintf("bug %v", f))
	}

	if !e.CacheMode.Writing() {
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
func (e *Eval) CacheWrite(ctx context.Context, f *Flow) error {
	var endTrace func()
	ctx, endTrace = trace.Start(ctx, trace.Cache, f.Digest(), fmt.Sprintf("cache write %s", f.Digest().Short()))
	defer endTrace()
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
	// We expect the fileset to contain only resolved files and to already exist
	// in the shared repository (because the scheduler should've transferred them).
	if missing, err := repository.Missing(ctx, e.Repository, fs.Files()...); err != nil {
		return err
	} else if n := len(missing); n > 0 {
		b := new(bytes.Buffer)
		fmt.Fprintf(b, "shared repo (%s) missing %d files: ", e.Repository.URL(), n)
		for i, f := range missing {
			if i > 0 {
				fmt.Fprint(b, ", ")
			}
			fmt.Fprint(b, f.Short())
		}
		return errors.E("CacheWrite", f.Digest(), b.String())
	}
	id, err := marshal(ctx, e.Repository, &fs)
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
		err := e.CacheWrite(bgctx, f)
		if err != nil {
			e.Log.Errorf("cache write %v: %v", f, err)
		}
		bgctx.Complete()
	}()
}

// lookupFailed marks the flow f as having failed lookup. Lookup
// failure is treated differently depending on evaluation mode. In
// bottom-up mode, we're only looked up if our dependencies are met,
// and we always compute on a cache miss, thus we now need to make
// sure our dependencies are available, and the node is marked Ready.
//
// Note that all Ready external nodes will immediately move to NeedSubmit within the evaluation loop
// and since we only lookup external nodes (in fact cache only external nodes for that matter),
// these could directly move to NeedSubmit.
//
// In top-down mode, we need to continue traversing the graph, and the node is marked TODO.
func (e *Eval) lookupFailed(f *Flow) {
	if e.BottomUp {
		e.Mutate(f, Ready)
	} else {
		e.Mutate(f, TODO)
	}
}

var batchCacheDigest = reflow.Digester.FromString("batch cache lookup")

// BatchLookup performs a cache lookup of a set of flow nodes.
func (e *Eval) batchLookup(ctx context.Context, flows ...*Flow) {
	var endTrace func()
	ctx, endTrace = trace.Start(ctx, trace.Cache, batchCacheDigest, fmt.Sprintf("batch cache lookup (%d)", len(flows)))
	var wg sync.WaitGroup // will be incremented for each parallel cache lookup
	defer func() {
		go func() {
			// to ensure an accurate trace duration, wait for all of the per-flow operations
			// to complete before ending the trace
			wg.Wait()
			endTrace()
		}()
	}()

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
		wg.Add(1)
		trace.Note(ctx, "FlowID", f.Digest().Short())
		e.step(f, func(f *Flow) error {
			defer wg.Done()
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
				err = unmarshal(ctx, e.Repository, res.Digest, &fs)
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
				e.Log.Error(err)
			}
			if err != nil {
				e.lookupFailed(f)
				return nil
			}
			// If the cached fileset has viable non-empty assertions, assert them.
			if a := fs.Assertions(); !a.IsEmpty() {
				// Check if the assertions are internally consistent for the cached fileset.
				if err = e.assertionsConsistent(f, []*reflow.Assertions{a}); err != nil {
					e.Log.Debugf("assertions consistent: %v", err)
					e.lookupFailed(f)
					return nil
				}
				anew, err := e.refreshAssertions(ctx, []*reflow.Assertions{a}, bg)
				if err != nil {
					e.Log.Debugf("refresh assertions: %v", err)
					e.lookupFailed(f)
					return nil
				}
				if !e.Assert(ctx, []*reflow.Assertions{a}, anew) {
					if e.Log.At(log.DebugLevel) {
						if diff := reflow.PrettyDiff([]*reflow.Assertions{a}, anew); diff != "" {
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
		refreshed, _ = reflow.DistinctAssertions(refreshed...)
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
	refreshed, _ = reflow.DistinctAssertions(append(refreshed, genAs...)...)
	return refreshed, nil
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
	da, err := reflow.MergeAssertions(f.depAssertions()...)
	if err != nil {
		return err
	}
	return fs.AddAssertions(da)
}

// Fork is a an argument to (*Eval).Mutate to indicate a fork mutation.
type Fork *Flow

// Value is an argument to (*Eval).Mutate to indicate a set-value mutation.
type Value struct{ Value values.T }

// Mutation is a type of mutation.
type Mutation int

// SetReserved sets the flow's Reserved resources.
type SetReserved reflow.Resources

// Status amends the task's status string.
type Status string

const (
	Invalid Mutation = iota
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

// Mutate safely applies a set of mutations which may be applied concurrently with each other.
func (e *Eval) Mutate(f *Flow, muts ...interface{}) {
	if e.Trace != nil {
		strs := make([]string, len(muts))
		for i := range muts {
			strs[i] = fmt.Sprint(muts[i])
		}
		e.Trace.Printf("mutate %s: %v", f, strings.Join(strs, ", "))
	}
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
		case SetReserved:
			f.Reserved.Set(reflow.Resources(arg))
		default:
			panic(fmt.Sprintf("invalid argument type %T", arg))
		}
	}
	// When a flow is done (without errors), add all its assertions to the vector clock.
	if f.Op.External() && f.State == Done && f.Err == nil {
		err := e.assertions.AddFrom(f.Value.(reflow.Fileset).Assertions())
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
	if (thisState == Running || thisState == Execing) && f.Status == nil && statusOk {
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
	case Ready:
		status = "waiting"
	}
	status = f.Op.String() + " " + status
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
				if f.Exec != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					// Print RemoteLogs objects / repository digests for logging objects, if present.
					if !f.ExecStdout.Digest.IsZero() {
						_, _ = fmt.Fprintf(w, "stdout: %s\n", f.ExecStdout)
					} else if loc, err := f.Exec.RemoteLogs(ctx, true); err == nil {
						_, _ = fmt.Fprintf(w, "stdout location: %s\n", loc)
					}
					if !f.ExecStderr.Digest.IsZero() {
						_, _ = fmt.Fprintf(w, "stderr: %s\n", f.ExecStderr)
					} else if loc, err := f.Exec.RemoteLogs(ctx, false); err == nil {
						_, _ = fmt.Fprintf(w, "stderr location: %s\n", loc)
					}
				}
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
			for _, k := range []string{"cpu", "mem", "disk", "tmp"} {
				prof := profile[k]
				dur := prof.Last.Sub(prof.First).Round(time.Second)
				switch k {
				case "cpu":
					fmt.Fprintf(w, "    cpu mean=%.1f max=%.1f (N=%d, duration=%s)\n", prof.Mean, prof.Max, prof.N, dur)
				default:
					fmt.Fprintf(w, "    %s mean=%s max=%s (N=%d, duration=%s)\n", k, data.Size(prof.Mean), data.Size(prof.Max), prof.N, dur)
				}
			}
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
	}
	fmt.Fprintf(&b, "%-12s %s %-4s %6s ", f.Ident, f.Digest().Short(), state, f.Op.String())
	pr := statusPrinters[f.Op]
	switch f.State {
	case Running, Execing:
		if pr.run != nil {
			pr.run(&b, f)
		}
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
	if f.Err != nil || pr.debug == nil {
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
	f.ExecStdout = task.Stdout
	f.ExecStderr = task.Stderr
	if task.Err != nil {
		e.Mutate(f, task.Err, Done)
	} else {
		e.Mutate(f, task.Result.Err, task.Result.Fileset, Propagate, Done)
	}
	return nil
}

func (e *Eval) newTask(f *Flow) *sched.Task {
	// TODO(swami): Consider encapsulating task fields (where applicable) and passing at construction.
	t := sched.NewTask()
	t.ID = taskdb.TaskID(reflow.Digester.Rand(nil))
	t.RunID = e.RunID
	t.FlowID = f.Digest()
	t.Config = f.ExecConfig()
	t.Repository = e.Repository
	t.PostUseChecksum = e.PostUseChecksum
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
			e.Mutate(f, SetReserved(newReserved))
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
	e.Mutate(f, SetReserved(resources), Execing)
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
	mem := used["mem"]
	if mem > oomRetryMaxExecMemory {
		mem = oomRetryMaxExecMemory
	}
	if mem < specified["mem"] {
		// If we used lesser than what was specified (eg: predicted usage was lower)
		// then we try with what was specified.
		mem = specified["mem"]
	} else {
		// Increase memory
		mem *= memMultiplier
		// But cap it to oomRetryMaxExecMemory
		if mem > oomRetryMaxExecMemory {
			mem = oomRetryMaxExecMemory
		}
	}
	newResources["mem"] = mem
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
func marshal(ctx context.Context, repo reflow.Repository, fs *reflow.Fileset) (digest.Digest, error) {
	if fs.N() > maxFilesInFilesetForNoConcurrenyLimit {
		limiter := reflow.GetFilesetOpLimiter()
		if err := limiter.Acquire(ctx, 1); err != nil {
			return digest.Digest{}, errors.E("eval.marshal: unable to acquire token", errors.Unavailable, err)
		}
		defer limiter.Release(1)
	}
	return repository.Marshal(ctx, repo, fs)
}

// Unmarshal unmarshals the value named by digest k into v.
// If the value does not exist in repository, an error is returned.
func unmarshal(ctx context.Context, repo reflow.Repository, k digest.Digest, fs *reflow.Fileset) error {
	f, err := repo.Stat(ctx, k)
	if err != nil {
		return errors.E("eval.unmarshal: stat", k, err)
	}
	var limited string
	if f.Size > maxSizeForNoConcurrencyLimit {
		var (
			limiter   = reflow.GetFilesetOpLimiter()
			numTokens = int(f.Size / sizePerToken)
		)
		if numTokens < 1 {
			numTokens = 1
		}
		if numTokens > limiter.Limit() {
			numTokens = limiter.Limit()/2 + 1
		}
		limited = fmt.Sprintf("limited (%d tokens)", numTokens)
		if err = limiter.Acquire(ctx, numTokens); err != nil {
			return errors.E("eval.unmarshal: unable to acquire token", errors.Unavailable, err)
		}
		defer limiter.Release(numTokens)
	}
	start := time.Now()
	err = repository.Unmarshal(ctx, repo, k, fs)
	log.Printf("unmarshal fileset: %s %d took:%s %s", data.Size(f.Size), f.Size, time.Since(start).Round(10*time.Millisecond), limited)
	return err
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
	Running: "running",
	Execing: "executing",
	Ready:   "waiting",
}
