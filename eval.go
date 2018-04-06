// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

//go:generate stringer -type=Mutation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset/bloomlive"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/values"
	"github.com/willf/bloom"
	"golang.org/x/sync/errgroup"
)

const (
	// The minimum amount of memory we allocate for an exec.
	minExecMemory = 500 << 20
	// The minimum number of CPUs we allocate for an exec.
	minExecCPU = 1

	numExecTries = 5

	// printAllTasks can be set to aid testing and debugging.
	printAllTasks = false
)

const defaultCacheLookupTimeout = time.Minute

// stateStatusOrder defines the order in which differenet flow
// statuses are rendered.
var stateStatusOrder = []FlowState{
	FlowRunning, FlowTransfer, FlowReady, FlowDone,

	// DEBUG:
	FlowNeedLookup, FlowLookup, FlowNeedTransfer, FlowTODO,
}

// CacheMode is a bitmask that tells how caching is to be used
// in the evaluator.
type CacheMode int

const (
	// CacheOff is CacheMode's default value and indicates
	// no caching (read or write) is to be performed.
	CacheOff CacheMode = 0
	// CacheRead indicates that cache lookups should be performed
	// during evaluation.
	CacheRead CacheMode = 1 << iota
	// CacheWrite indicates that the evaluator should write evaluation
	// results to the cache.
	CacheWrite
)

// Reading returns whether the cache mode contains CacheRead.
func (m CacheMode) Reading() bool {
	return m&CacheRead == CacheRead
}

// Writing returns whether the cache mode contains CacheWrite.
func (m CacheMode) Writing() bool {
	return m&CacheWrite == CacheWrite
}

// EvalConfig provides runtime configuration for evaluation instances.
type EvalConfig struct {
	// The executor to which execs are submitted.
	Executor Executor

	// An (optional) logger to which the evaluation transcript is printed.
	Log *log.Logger

	// Status gets evaluation status reports.
	Status *status.Group

	// An (optional) logger to print evaluation trace.
	Trace *log.Logger

	// Transferer is used to arrange transfers between repositories,
	// including nodes and caches.
	Transferer Transferer

	// Repository is the main, shared repository between evaluations.
	Repository Repository

	// Assoc is the main, shared assoc that is used to store cache and
	// metadata associations.
	Assoc assoc.Assoc

	// CacheMode determines whether the evaluator reads from
	// or writees to the cache. If CacheMode is nonzero, Assoc,
	// Repository, and Transferer must be non-nil.
	CacheMode CacheMode

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

	// Config stores the flow config to be used.
	Config Config

	// CacheLookupTimeout is the timeout for cache lookups.
	// After the timeout expires, a cache lookup is considered
	// a miss.
	CacheLookupTimeout time.Duration

	// Invalidate is a function that determines whether or not f's cached
	// results should be invalidated.
	Invalidate func(f *Flow) bool
}

// String returns a human-readable form of the evaluation configuration.
func (e EvalConfig) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "executor %T transferer %T", e.Executor, e.Transferer)
	var flags []string
	if e.NoCacheExtern {
		flags = append(flags, "nocacheextern")
	} else {
		flags = append(flags, "cacheextern")
	}
	if e.CacheMode == CacheOff {
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
	fmt.Fprintf(&b, " flags %s", strings.Join(flags, ","))
	fmt.Fprintf(&b, " flowconfig %s", e.Config)
	fmt.Fprintf(&b, " cachelookuptimeout %s", e.CacheLookupTimeout)
	return b.String()
}

// Eval is an evaluator for Flows.
type Eval struct {
	// EvalConfig is the evaluation configuration used in this
	// evaluation.
	EvalConfig

	root *Flow

	// The list of Flows available for execution.
	list []*Flow
	// The set of completed flows, used for reporting.
	completed []*Flow
	// The set of cached flows, used for reporting
	cached []*Flow
	// A channel indicating how much extra resources are needed
	// in order to avoid queueing.
	needch chan Requirements
	// A channel for evaluation errors.
	errors chan error
	// Total and currently available resources.
	total, available Resources
	// The number of Flows stolen.
	nstolen int
	// Contains pending (currently executing) flows.
	// TODO(marius): encode this into the flow state as well,
	// so that execution state is not straddled across the two.
	pending map[*Flow]bool
	// Ticker for reporting.
	ticker *time.Ticker
	// Total execution time.
	totalTime time.Duration
	// Informational channels for printing status.
	needLog []*Flow

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
}

// NewEval creates and initializes a new evaluator using the provided
// evaluation configuration and root flow.
func NewEval(root *Flow, config EvalConfig) *Eval {
	if (config.Assoc == nil || config.Repository == nil) && config.CacheMode != CacheOff {
		switch {
		case config.Assoc == nil && config.Repository == nil:
			config.Log.Printf("turning caching off because assoc and repository are not configured")
		case config.Assoc == nil:
			config.Log.Printf("turning caching off because assoc is not configured")
		case config.Repository == nil:
			config.Log.Printf("turning caching off because repository is not configured")
		}
		config.CacheMode = CacheOff
	}

	e := &Eval{
		EvalConfig: config,
		root:       root.Canonicalize(config.Config),
		needch:     make(chan Requirements),
		errors:     make(chan error),
		returnch:   make(chan *Flow, 1024),
		newStealer: make(chan *Stealer),
		wakeupch:   make(chan bool, 1),
		pending:    map[*Flow]bool{}, // TODO: name e.ready; but really we should just traverse the tree
		total:      config.Executor.Resources(),
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
func (e *Eval) Requirements() Requirements {
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
// described in the documentation for FlowState.
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
	e.Log.Debugf("evaluating with configuration: %s", e.EvalConfig)
	e.begin = time.Now()
	defer func() {
		e.totalTime = time.Since(e.begin)
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.ticker = time.NewTicker(10 * time.Second)
	defer e.ticker.Stop()
	flow := e.root

	for flow.State != FlowDone {
		if flow.Digest().IsZero() {
			panic("invalid flow, zero digest: " + flow.DebugString())
		}

		// This is the meat of the evaluation: we gather Flows that are
		// ready (until steady state), and then execute this batch. At the
		// end of each iteration, we wait for one task to complete, and
		// gather any new Flows that have become ready.
		e.todo(flow)
	dequeue:
		for v := flow.Visitor(); v.Walk(); {
			f := v.Flow
			if e.pending[f] {
				continue
			}
			// If our state is >= Ready, all of our children are completed,
			// so no need to traverse.
			if f.State < FlowReady {
				v.Visit()
			}

			if v.Op == OpExec {
				if v.Resources["mem"] < minExecMemory {
					v.Resources["mem"] = minExecMemory
				}
				if v.Resources["cpu"] < minExecCPU {
					v.Resources["cpu"] = minExecCPU
				}
			}
			switch f.State {
			case FlowNeedLookup:
				// TODO(marius): we should perform batch lookups
				// as the underyling APIs (e.g., to DynamoDB) do not
				// bundle requests automatically.
				e.Mutate(f, FlowLookup)
				e.pending[f] = true
				go func(f *Flow) {
					e.lookup(ctx, f)
					e.returnch <- f
				}(f)
			case FlowNeedTransfer:
				e.Mutate(f, FlowTransfer)
				e.pending[f] = true
				go func(f *Flow) {
					files, err := e.needTransfer(ctx, f)
					if err != nil {
						e.Log.Errorf("need transfer: %v", err)
					} else if len(files) == 0 {
						// No transfer needed; we're ready to go.
						e.Mutate(f, FlowReady)
						e.returnch <- f
						return
					}
					// Compute the transfer size, so that we can log it. Note that
					// this is of course subject to race conditions: when multiple
					// execs concurrently require the same objects, these may be
					// reported multiple times in aggregate transfer size.
					seen := make(map[File]bool)
					for _, file := range files {
						if seen[file] {
							continue
						}
						f.TransferSize += data.Size(file.Size)
						seen[file] = true
					}
					e.Mutate(f, Refresh) // TransferSize is updated
					e.LogFlow(ctx, f)
					if err := e.transfer(ctx, f); err != nil {
						e.errors <- err
					} else {
						e.returnch <- f
					}
				}(f)

			case FlowReady:
				if !e.total.Available(f.Resources) {
					// TODO(marius): we could also attach this error to the node.
					return errors.E(errors.ResourcesExhausted,
						errors.Errorf("eval: requested resources %v exceeds total available %v", f.Resources, e.total))
				}
				if !e.available.Available(f.Resources) {
					continue dequeue
				}
				e.available.Sub(e.available, f.Resources)
				e.Mutate(f, FlowRunning, Reserve(f.Resources))
				e.pending[f] = true
				go func(f *Flow) {
					if err := e.eval(ctx, f); err != nil {
						e.errors <- err
					} else {
						e.returnch <- f
					}
				}(f)
			}
		}
		if flow.State == FlowDone {
			break
		}
		if len(e.pending) == 0 && flow.State != FlowDone {
			var states [FlowMax][]*Flow
			for v := e.root.Visitor(); v.Walk(); v.Visit() {
				states[v.State] = append(states[v.State], v.Flow)
			}
			var s [FlowMax]string
			for i := range states {
				n, tasks := accumulate(states[i])
				s[i] = fmt.Sprintf("%s:%d<%s>", FlowState(i).Name(), n, tasks)
			}
			e.Log.Printf("pending %d", len(e.pending))
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
	if flow.Err != nil {
		return nil
	}
	for len(e.pending) > 0 {
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
	}
	stats := map[string]aggregate{}

	for v := e.root.Visitor(); v.Walk(); v.Visit() {
		if v.Parent != nil {
			v.Push(v.Parent)
		}
		// Skip nodes that were skipped due to caching.
		if v.State < FlowDone {
			continue
		}
		switch v.Op {
		case OpExec, OpIntern, OpExtern:
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
		if v.Op == OpExec {
			a.CPU.Add(v.Inspect.Profile["cpu"].Mean)
			a.Memory.Add(v.Inspect.Profile["mem"].Max / (1 << 30))
			a.Disk.Add(v.Inspect.Profile["disk"].Max / (1 << 30))
			a.Temp.Add(v.Inspect.Profile["tmp"].Max / (1 << 30))
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
	fmt.Fprintln(&tw, "ident\tn\tncache\ttransfer\truntime(m)\tcpu\tmem(GiB)\tdisk(GiB)\ttmp(GiB)")
	for ident, stats := range stats {
		fmt.Fprintf(&tw, "%s\t%d\t%d\t%s", ident, stats.N, stats.Ncache, stats.Transfer)
		if stats.CPU.N() > 0 {
			fmt.Fprintf(&tw, "\t%s\t%s\t%s\t%s\t%s",
				stats.Runtime.Summary("%.0f"),
				stats.CPU.Summary("%.1f"),
				stats.Memory.Summary("%.1f"),
				stats.Disk.Summary("%.1f"),
				stats.Temp.Summary("%.1f"),
			)
		} else {
			fmt.Fprint(&tw, "\t\t\t\t\t")
		}
		fmt.Fprint(&tw, "\n")
	}
	tw.Flush()
	log.Printf(b.String())
}

// Need returns the total resource requirements needed in order to
// avoid queueing work.
func (e *Eval) Need() Requirements {
	return <-e.needch
}

// Stealer returns Stealer from which flow nodes may be stolen. This
// permits an external worker to perform the work implied by the
// return Flow, which is always in FlowReady state. When the external
// worker has completed processing (or decided not to process after
// all), the node must be returned via Return.
func (e *Eval) Stealer() *Stealer {
	s := newStealer(e)
	e.newStealer <- s
	return s
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
			need             Requirements
			nready, nrunning int
		)
		for v := e.root.Visitor(); v.Walk(); {
			switch {
			case e.pending[v.Flow]:
				if v.State == FlowTransfer {
					nrunning++
				} else {
					switch v.Op {
					case OpExec, OpIntern, OpExtern:
						nrunning++
					}
				}
				continue
			case v.State < FlowReady:
				v.Visit()
			case v.State == FlowReady:
				nready++
				admitted := false
				for s := e.stealer; s != nil; s = s.next {
					if admitted = s.admit(v.Flow); admitted {
						e.pending[v.Flow] = true
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
		case OpExec, OpIntern, OpExtern:
		default:
			continue
		}
		switch v.State {
		case FlowTransfer:
			byteCounts.Incr(v.State, v.Ident, int(v.TransferSize))
			fallthrough
		case FlowReady, FlowDone, FlowTODO, FlowRunning:
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
	for _, state := range []FlowState{FlowRunning, FlowTransfer, FlowReady} {
		n := stateCounts.N(state)
		if n == 0 {
			continue
		}
		fmt.Fprintf(&b, ", %s:%d", humanState[state], n)
	}
	fmt.Fprintf(&b, ", completed: %d/%d",
		stateCounts.N(FlowDone),
		stateCounts.N(FlowDone)+stateCounts.N(FlowRunning)+stateCounts.N(FlowTransfer)+stateCounts.N(FlowTODO))
	e.Status.Print(b.String())
}

func (e *Eval) returnFlow(f *Flow) {
	delete(e.pending, f)
	e.needCollect = true
	e.available.Add(e.available, f.Reserved)
	e.Mutate(f, Unreserve(f.Reserved))
	if f.Tracked && f.State == FlowDone {
		e.needLog = append(e.needLog, f)
	}
}

// collect reclaims unreachable objects from the executor's repository.
// Objects are considered live if:
//
//	(1) They are part of the frontier of flow nodes in FlowDone state.
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
// FlowDone, and setting the node's Value).
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
	var livevals []*Fileset
	e.nlive = 0
	for v := e.root.Visitor(); v.Walk(); {
		fs, ok := v.Value.(Fileset)
		switch {
		case ok && v.State == FlowDone:
			if n := fs.N(); n > 0 {
				e.nlive += n
				livevals = append(livevals, &fs)
			}
		default:
			v.Visit()
			// Unevaluated maps must also be traversed, since they
			// may introduce dependencies that would otherwise be
			// garbage collected: they're just not evident yet.
			if v.Op == OpMap {
				v.Push(v.MapFlow)
			}
		}
	}
	var nwriter int
	for w := e.writers; w != nil; w = w.next {
		nwriter++
		fs, _ := w.Flow.Value.(Fileset)
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

// dirty determines whether node f is (transitively) dirty, and must
// be recomputed. Dirty considers only visible nodes; it does not
// incur extra computation, thus dirtying does not work when dirtying
// nodes are hidden behind maps, continuations, or coercions.
func (e *Eval) dirty(f *Flow) bool {
	if !e.NoCacheExtern {
		return false
	}
	if f.Op == OpExtern {
		return true
	}
	for _, dep := range f.Deps {
		if e.dirty(dep) {
			return true
		}
	}
	return false
}

// valid tells whether f's cached results should be considered valid.
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

// todo adds to e.list the set of ready Flows in f.
func (e *Eval) todo(f *Flow) {
	if f == nil {
		return
	}
	switch f.State {
	case FlowInit:
		switch f.Op {
		case OpIntern, OpExec, OpExtern:
			if !e.BottomUp && e.CacheMode.Reading() && !e.dirty(f) {
				e.Mutate(f, FlowNeedLookup)
				return
			}
		}
		e.Mutate(f, FlowTODO)
		fallthrough
	case FlowTODO:
		for _, dep := range f.Deps {
			e.todo(dep)
		}
		// In the case of multiple dependencies, we short-circuit
		// computation on error. This is because we want to return early,
		// in case it can be dealt with (e.g., by restarting evaluation).
		for _, dep := range f.Deps {
			if dep.State == FlowDone && dep.Err != nil {
				e.Mutate(f, FlowReady)
				return
			}
		}
		for _, dep := range f.Deps {
			if dep.State != FlowDone {
				return
			}
		}
		// The node is ready to run. This is done according to the evaluator's mode.
		switch f.Op {
		case OpIntern, OpExec, OpExtern:
			// We're ready to run. If we're in bottom up mode, this means we're ready
			// for our cache lookup.
			if e.BottomUp {
				e.Mutate(f, FlowNeedLookup)
			} else {
				if e.CacheMode.Reading() {
					e.Mutate(f, FlowNeedTransfer)
				} else {
					e.Mutate(f, FlowReady)
				}
			}
		default:
			// Other nodes can be computed immediately,
			// and do not need access to the objects.
			e.Mutate(f, FlowReady)
		}
	}
}

// eval performs a one-step simplification of f. It must be called
// only after all of f's dependencies are ready.
//
// eval also caches results of successful execs if e.Cache is defined.
func (e *Eval) eval(ctx context.Context, f *Flow) (err error) {
	// Propagate errors immediately.
	for _, dep := range f.Deps {
		if err := dep.Err; err != nil {
			e.Mutate(f, err, FlowDone)
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
	if f.Op != OpVal {
		defer func() {
			if err != nil {
				// Don't print cancellation errors, since they are follow-on errors
				// from upstream ones that have already been reported.
				/*
					if !errors.Is(errors.E(errors.Canceled), err) {
						e.Log.Errorf("eval %s runtime error: %v", f.ExecString(false), err)
					}
				*/
			} else if f.State == FlowDone && f.Op != OpK && f.Op != OpCoerce {
				f.Runtime = time.Since(begin)
			}
		}()
	}

	switch f.Op {
	case OpIntern, OpExtern, OpExec:
		if err := e.exec(ctx, f); err != nil {
			return err
		}
	case OpGroupby:
		v := f.Deps[0].Value.(Fileset)
		groups := map[string]Fileset{}
		for path, file := range v.Map {
			idx := f.Re.FindStringSubmatch(path)
			if len(idx) != 2 {
				continue
			}
			v, ok := groups[idx[1]]
			if !ok {
				v = Fileset{Map: map[string]File{}}
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
		fs := Fileset{List: make([]Fileset, len(groups))}
		for i, k := range keys {
			fs.List[i] = groups[k]
		}
		e.Mutate(f, fs, Incr, FlowDone)
	case OpMap:
		v := f.Deps[0].Value.(Fileset)
		ff := &Flow{
			Op:   OpMerge,
			Deps: make([]*Flow, len(v.List)),
		}
		for i := range v.List {
			ff.Deps[i] = f.MapFunc(v.List[i].Flow())
		}
		e.Mutate(f, Fork(ff), FlowTODO)
		e.Mutate(f.Parent, FlowDone)
	case OpCollect:
		v := f.Deps[0].Value.(Fileset)
		fileset := map[string]File{}
		for path, file := range v.Map {
			if !f.Re.MatchString(path) {
				continue
			}
			dst := f.Re.ReplaceAllString(path, f.Repl)
			fileset[dst] = file
		}
		e.Mutate(f, Fileset{Map: fileset}, Incr, FlowDone)
	case OpMerge:
		list := make([]Fileset, len(f.Deps))
		for i, dep := range f.Deps {
			list[i] = dep.Value.(Fileset)
		}
		e.Mutate(f, Fileset{List: list}, Incr, FlowDone)
	case OpVal:
		e.Mutate(f, Incr, FlowDone)
	case OpPullup:
		v := &Fileset{List: make([]Fileset, len(f.Deps))}
		for i, dep := range f.Deps {
			v.List[i] = dep.Value.(Fileset)
		}
		e.Mutate(f, v.Pullup(), Incr, FlowDone)
	case OpK:
		vs := make([]values.T, len(f.Deps))
		for i, dep := range f.Deps {
			vs[i] = dep.Value
		}
		ff := f.K(vs)
		e.Mutate(f, Fork(ff), FlowTODO)
		e.Mutate(f.Parent, FlowDone)
	case OpCoerce:
		if v, err := f.Coerce(f.Deps[0].Value); err != nil {
			e.Mutate(f, err, Incr, FlowDone)
		} else {
			e.Mutate(f, Value{v}, Incr, FlowDone)
		}
	case OpRequirements:
		e.Mutate(f, Value{f.Deps[0].Value}, Incr, FlowDone)
	case OpData:
		if id, err := e.Executor.Repository().Put(ctx, bytes.NewReader(f.Data)); err != nil {
			e.Mutate(f, err, Incr, FlowDone)
		} else {
			e.Mutate(f, Fileset{Map: map[string]File{".": {id, int64(len(f.Data))}}}, Incr, FlowDone)
		}
	default:
		panic(fmt.Sprintf("bug %v", f))
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
	bgctx := Background(ctx)
	go func() {
		err := e.CacheWrite(bgctx, f, e.Executor.Repository())
		if err != nil {
			e.Log.Errorf("cache write %v: %v", f, err)
		}
		bgctx.Complete()
		e.Mutate(f, Decr)
	}()
	return nil
}

// CacheWrite writes the cache entry for flow f, with objects in the provided
// source repository. CacheWrite returns nil on success, or else the first error
// encountered.
func (e *Eval) CacheWrite(ctx context.Context, f *Flow, repo Repository) error {
	switch f.Op {
	case OpIntern, OpExtern, OpExec:
	default:
		return nil
	}
	// We currently only cache fileset values.
	fs, ok := f.Value.(Fileset)
	if !ok {
		return nil
	}
	// We don't cache errors, and only completed nodes.
	if f.Err != nil || f.State != FlowDone {
		return nil
	}
	if f.Op == OpData {
		return nil
	}
	if e.NoCacheExtern && f.Op == OpExtern {
		return nil
	}
	keys := f.CacheKeys()
	if len(keys) == 0 {
		return nil
	}
	if err := e.Transferer.Transfer(ctx, e.Repository, repo, fs.Files()...); err != nil {
		return err
	}
	id, err := marshal(ctx, e.Repository, fs)
	if err != nil {
		return err
	}
	// Write a mapping for each cache key.
	g, ctx := errgroup.WithContext(ctx)
	for i := range keys {
		key := keys[i]
		g.Go(func() error {
			return e.Assoc.Put(ctx, assoc.Fileset, digest.Digest{}, key, id)
		})
	}
	return g.Wait()
}

// lookupFailed marks the flow f as having failed lookup. Lookup
// failure is treated differently depending on evaluation mode. In
// bottom-up mode, we're only looked up if our dependencies are met,
// and we always compute on a cache miss, thus we now need to make
// sure our dependencies are available, and the node is marked
// FlowNeedTransfer. In top-down mode, we need to continue traversing
// the graph, and the node is marked FlowTODO.
func (e *Eval) lookupFailed(f *Flow) {
	if e.BottomUp {
		e.Mutate(f, FlowNeedTransfer)
	} else {
		e.Mutate(f, FlowTODO)
	}
}

// lookup performs a cache lookup of node f.
func (e *Eval) lookup(ctx context.Context, f *Flow) {
	if !e.valid(f) || !e.CacheMode.Reading() || e.NoCacheExtern && (f.Op == OpExtern || f == e.root) {
		e.lookupFailed(f)
		return
	}
	var (
		keys = f.CacheKeys()
		fs   Fileset
		fsid digest.Digest
		err  error
	)
	if len(keys) == 0 {
		// This can't be true now, but in the future it could be valid for nodes
		// to present no cache keys.
		e.lookupFailed(f)
		return
	}
	which := -1
	// The assoc lookups can produce a very high rate of lookups, especially
	// when restarting large workflows. This is exacerbated by the fact that
	// we don't perform batch lookups, and also perform "blind" read-repair:
	// since we don't know which keys are missing, we write back all of the
	// candidates. Here, the former would alleviate the latter.
	//
	// For now, we have the band-aids of concurrency limiting and large
	// timeouts.
	//
	// TODO(marius): push multiple lookups into the assoc,
	// so that these requests can be batched underneath,
	// then perform precise read repair.
	for i, key := range keys {
		ctx, cancel := context.WithTimeout(ctx, e.CacheLookupTimeout)
		key, fsid, err = e.Assoc.Get(ctx, assoc.Fileset, key)
		cancel()
		if err != nil {
			if !errors.Is(errors.NotExist, err) {
				e.Log.Errorf("assoc.Get %v: %v", f, err)
			}
			continue
		}
		err = unmarshal(ctx, e.Repository, fsid, &fs)
		if err == nil {
			which = i
			break
		}
		if !errors.Is(errors.NotExist, err) {
			e.Log.Errorf("unmarshal %v: %v", fsid, err)
		}
	}
	// Nothing was found, so there is no read repair to do.
	// Fail the lookup early.
	if which < 0 {
		e.lookupFailed(f)
		return
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
	if err == nil && e.RecomputeEmpty && fs.AnyEmpty() {
		e.Log.Debugf("recomputing empty value for %v", f)
	} else if err == nil {
		// Perform read repair: asynchronously write back all of the other
		// keys (which are synonymous by definition).
		keys = append(keys[:which], keys[which+1:]...)
		bgctx := Background(ctx)
		go func() {
			for _, key := range keys {
				if err := e.Assoc.Put(bgctx, assoc.Fileset, digest.Digest{}, key, fsid); err != nil {
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
		e.Mutate(f, fs, Cached, FlowDone)
		if e.BottomUp {
			e.LogFlow(ctx, f)
		}
		return
	}
	e.lookupFailed(f)
}

// needTransfer returns the file objects that require transfer from flow f.
// It should be called only when caching is enabled.
func (e *Eval) needTransfer(ctx context.Context, f *Flow) ([]File, error) {
	fs := Fileset{List: make([]Fileset, len(f.Deps))}
	for i := range f.Deps {
		fs.List[i] = f.Deps[i].Value.(Fileset)
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
	fs := Fileset{List: make([]Fileset, len(f.Deps))}
	for i := range f.Deps {
		fs.List[i] = f.Deps[i].Value.(Fileset)
	}
	err := e.Transferer.Transfer(ctx, e.Executor.Repository(), e.Repository, fs.Files()...)
	if err == nil {
		e.Mutate(f, FlowReady)
		return nil
	}
	e.Log.Errorf("cache transfer %v error: %v", f, err)
	// Errors.Unavailable is considered a transient error, so the
	// underlying runner should restart evaluation.
	return errors.E(errors.Unavailable, "cache.Transfer", err)
}

// exec performs and waits for an exec with the given config.
// exec tries each step up to numExecTries. Exec returns a value
// pointer which has been registered as live.
func (e *Eval) exec(ctx context.Context, f *Flow) error {
	var cfg ExecConfig
	switch f.Op {
	case OpIntern:
		cfg = ExecConfig{
			Type:  "intern",
			Ident: f.Ident,
			URL:   f.URL.String(),
		}
	case OpExtern:
		fs := f.Deps[0].Value.(Fileset)
		cfg = ExecConfig{
			Type:  "extern",
			Ident: f.Ident,
			URL:   f.URL.String(),
			Args:  []Arg{{Fileset: &fs}},
		}
	case OpExec:
		if f.Argmap == nil {
			f.Argmap = make([]ExecArg, len(f.Deps))
			for i := range f.Deps {
				f.Argmap[i] = ExecArg{Index: i}
			}
		}
		args := make([]Arg, f.NExecArg())
		for i := range args {
			earg := f.ExecArg(i)
			if earg.Out {
				args[i].Out = true
				args[i].Index = earg.Index
			} else {
				fs := f.Deps[earg.Index].Value.(Fileset)
				args[i].Fileset = &fs
			}
		}

		cfg = ExecConfig{
			Type:        "exec",
			Ident:       f.Ident,
			Image:       f.Image,
			Cmd:         f.Cmd,
			Args:        args,
			Resources:   f.Resources,
			OutputIsDir: f.OutputIsDir,
		}
	default:
		panic("bad op")
	}

	type state int
	const (
		statePut state = iota
		stateWait
		stateInspect
		stateResult
		statePromote
		stateDone
	)
	var (
		err error
		x   Exec
		r   Result
		n   = 0
		s   = statePut
		id  = f.Digest()
	)
	// TODO(marius): we should distinguish between fatal and nonfatal errors.
	// The fatal ones are useless to retry.
	for n < numExecTries && s < stateDone {
		switch s {
		case statePut:
			x, err = e.Executor.Put(ctx, id, cfg)
			if err == nil {
				f.Exec = x
				e.LogFlow(ctx, f)
			}
		case stateWait:
			err = x.Wait(ctx)
		case stateInspect:
			f.Inspect, err = x.Inspect(ctx)
		case stateResult:
			r, err = x.Result(ctx)
			if err == nil {
				e.Mutate(f, r.Fileset, Incr)
			}
		case statePromote:
			err = x.Promote(ctx)
		}
		if err != nil {
			n++
		} else {
			n = 0
			s++
		}
	}
	if err != nil {
		if s > stateResult {
			e.Mutate(f, Decr)
		}
		return err
	}
	e.Mutate(f, r.Err, FlowDone)
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
type Reserve Resources

// Unreserve subtracts resources from the flow's reservation.
type Unreserve Resources

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
		prevState, thisState FlowState
		refresh              bool
	)
	for _, mut := range muts {
		switch arg := mut.(type) {
		case error:
			if arg != nil {
				f.Err = errors.Recover(arg)
			}
		case FlowState:
			prevState = f.State
			thisState = arg
			f.State = arg
		case Fileset:
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
			}
		case Reserve:
			f.Reserved.Add(f.Reserved, Resources(arg))
		case Unreserve:
			f.Reserved.Sub(f.Reserved, Resources(arg))
		default:
			panic("invalid argument " + fmt.Sprint(arg))
		}
	}
	e.muGC.RUnlock()
	// Update task status, if applicable.
	if e.Status == nil {
		return
	}
	switch f.Op {
	case OpExec, OpIntern, OpExtern:
	default:
		return
	}
	if prevState < FlowTransfer && thisState >= FlowTransfer && f.Status == nil {
		// TODO(marius): digest? fmt("%-*s %s", n, ident, f.Digest().Short())
		f.Status = e.Status.Start(f.Ident)
	}
	if f.Status == nil || (!refresh && prevState == thisState) {
		return
	}
	switch f.State {
	case FlowDone:
		if f.Err != nil {
			f.Status.Printf("%s error %v", f.Op, f.Err)
		} else {
			switch f.Op {
			case OpExtern:
				f.Status.Print("extern done")
			case OpExec, OpIntern:
				f.Status.Printf("%s done %s", f.Op, data.Size(f.Value.(Fileset).Size()))
			}
		}
		f.Status.Done()
		f.Status = nil
	case FlowRunning:
		switch f.Op {
		case OpExtern:
			f.Status.Printf("extern %s %s", f.URL, data.Size(f.Deps[0].Value.(Fileset).Size()))
		case OpIntern:
			f.Status.Printf("intern %s", f.URL)
		case OpExec:
			f.Status.Printf("exec %s", abbrevCmd(f))
		}
	case FlowTransfer:
		if f.TransferSize == 0 {
			f.Status.Print("transfer")
		} else {
			f.Status.Printf("transfer %s", data.Size(f.TransferSize))
		}
	case FlowReady:
		f.Status.Print("waiting")
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
	OpExec: {
		run: func(w io.Writer, f *Flow) { io.WriteString(w, abbrevCmd(f)) },
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
				if fs, ok := f.Deps[f.ExecArg(i).Index].Value.(Fileset); ok {
					printFileset(w, "        ", fs)
				} else {
					fmt.Fprintln(w, "        (cached)")
				}
			}
			if f.State != FlowDone {
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
					printFileset(w, "        ", f.Value.(Fileset).List[earg.Index])
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
	OpIntern: {
		run: func(w io.Writer, f *Flow) {
			url := leftabbrev(f.URL.String(), nabbrev)
			io.WriteString(w, url)
		},
		debug: func(w io.Writer, f *Flow) {
			if f.State != FlowDone {
				return
			}
			if f.Err == nil {
				fmt.Fprintln(w, "result:")
				if fs, ok := f.Value.(Fileset); ok {
					printFileset(w, "    ", fs)
				} else {
					fmt.Fprintln(w, "    (cached)")
				}
			}
		},
	},
	OpExtern: {
		run: func(w io.Writer, f *Flow) {
			url := leftabbrev(f.URL.String(), nabbrev)
			io.WriteString(w, url)
			io.WriteString(w, " ")
			// The dep may not have a fileset in case the extern was satisfied from cache directly.
			if fs, ok := f.Deps[0].Value.(Fileset); ok {
				io.WriteString(w, data.Size(fs.Size()).String())
			}
		},
	},
	OpPullup: {
		debug: func(w io.Writer, f *Flow) {
			fmt.Fprintln(w, "value:")
			printFileset(w, "    ", f.Value.(Fileset))
		},
	},
}

// LogFlow logs flow f's state, and then tracks it for future logging.
func (e *Eval) LogFlow(ctx context.Context, f *Flow) {
	f.Tracked = f.State != FlowDone
	var b bytes.Buffer
	if f.State == FlowDone {
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
	case FlowRunning:
		state = "run"
	case FlowDone:
		if f.Err != nil {
			state = "err"
		} else {
			state = "ok"
		}
	case FlowTransfer:
		state = "xfer"
	}
	fmt.Fprintf(&b, "%-12s %s %-4s %6s ", f.Ident, f.Digest().Short(), state, f.Op.String())
	pr := statusPrinters[f.Op]
	switch f.State {
	case FlowRunning:
		if pr.run != nil {
			pr.run(&b, f)
		}
	case FlowTransfer:
		b.WriteString(f.TransferSize.String())
	case FlowDone:
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
	if f.State != FlowTransfer && (f.Err != nil || pr.debug == nil) {
		return
	}
	b.Reset()
	fmt.Fprintf(&b, "%s %v %s:\n", f.Ident, f.Digest().Short(), f.Position)
	if f.Op == OpExec {
		fmt.Fprintf(&b, "\tresources: %s\n", f.Resources)
	}
	for _, key := range f.CacheKeys() {
		fmt.Fprintf(&b, "\t%s\n", key)
	}
	if f.State == FlowTransfer {
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
	max    Resources
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
func (s *Stealer) Admit(max Resources) <-chan *Flow {
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
	s.max = Resources{}
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
	s.max = Resources{}
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
	if fs, ok := f.Value.(Fileset); ok {
		b.WriteString(" ")
		b.WriteString(data.Size(fs.Size()).String())
	} else {
		b.WriteString(" ?")
	}
}

func printFileset(w io.Writer, prefix string, fs Fileset) {
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
func marshal(ctx context.Context, repo Repository, v interface{}) (digest.Digest, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return digest.Digest{}, err
	}
	return repo.Put(ctx, bytes.NewReader(b))
}

// Unmarshal unmarshals the value named by digest k into v.
// If the value does not exist in repository, an error is returned.
func unmarshal(ctx context.Context, repo Repository, k digest.Digest, v interface{}) error {
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
func missing(ctx context.Context, r Repository, files ...File) ([]File, error) {
	exists := make([]bool, len(files))
	g, gctx := errgroup.WithContext(ctx)
	for i, file := range files {
		i, file := i, file
		g.Go(func() error {
			ctx, cancel := context.WithTimeout(gctx, 10*time.Second)
			_, err := r.Stat(ctx, file.ID)
			cancel()
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
	if err := ctx.Err(); err != nil {
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

type counters [FlowMax]map[string]int

func (c *counters) Incr(state FlowState, name string, n int) {
	if c[state] == nil {
		c[state] = make(map[string]int)
	}
	c[state][name] += n
}

func (c counters) Equal(d counters) bool {
	return equal(c, d) && equal(d, c)
}

func (c counters) N(state FlowState) int {
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

var humanState = map[FlowState]string{
	FlowRunning:  "running",
	FlowTransfer: "transferring",
	FlowReady:    "waiting",
}

func abbrevCmd(f *Flow) string {

	argv := make([]interface{}, len(f.Argstrs))
	for i := range f.Argstrs {
		argv[i] = f.Argstrs[i]
	}
	cmd := fmt.Sprintf(f.Cmd, argv...)
	// Special case: if we start with a command with an absolute path,
	// abbreviate to basename.
	cmd = strings.TrimSpace(cmd)
	cmd = trimpath(cmd)
	cmd = trimspace(cmd)
	cmd = abbrev(cmd, nabbrev)
	return fmt.Sprintf("%s %s", leftabbrev(f.Image, nabbrevImage), cmd)
}
