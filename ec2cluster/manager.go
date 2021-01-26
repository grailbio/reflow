// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
)

const (
	// defaultRefreshInterval defines how often the manager performs internal refreshes.
	defaultRefreshInterval = time.Minute

	// defaultDrainTimeout defines how long to wait to drain additional resource allocation waiters.
	defaultDrainTimeout = 50 * time.Millisecond

	// instanceLaunchTimeout is the maximum duration allotted for an instance to be
	// launched and recognized by the manager when refreshing the cluster's state.
	instanceLaunchTimeout = 5 * time.Minute
)

// InstanceSpec is a specification representing an instance configuration.
type InstanceSpec struct {
	Type      string
	Resources reflow.Resources
}

// Instance creates a ManagedInstance for this specification with the given id.
func (i InstanceSpec) Instance(id string) ManagedInstance { return ManagedInstance{i, id} }

// ManagedInstance represents a concrete instance with a given specification and ID.
type ManagedInstance struct {
	InstanceSpec
	ID string
}

// Valid returns whether this is a valid instance (with a non-empty ID)
func (m ManagedInstance) Valid() bool { return m.ID != "" }

// ManagedCluster is a cluster which can be managed.
type ManagedCluster interface {

	// Launch launches an instance with the given specification.
	Launch(ctx context.Context, spec InstanceSpec) ManagedInstance

	// Refresh refreshes the managed cluster.
	Refresh(ctx context.Context) (map[string]bool, error)

	// Available returns any available instance specification that can satisfy the need.
	// The returned InstanceSpec should be subsequently be 'Launch'able.
	Available(need reflow.Resources) (InstanceSpec, bool)

	// Notify notifies the managed cluster of the currently waiting and pending
	// amount of resources.
	Notify(waiting, pending reflow.Resources)
}

type waiter struct {
	reflow.Requirements
	ctx context.Context
	c   chan struct{}
}

func (w *waiter) notify() {
	close(w.c)
}

// Manager manages the cluster by fulfilling requests to allocate instances asynchronously,
// while delegating the actual launching of a new instance to the ManagedCluster.
// Once created, a manager needs to be initialized.
type Manager struct {
	cluster ManagedCluster

	// maxInstances is the maximum number of concurrent instances permitted.
	maxInstances int
	// maxPending is the maximum number of pending instances permitted.
	maxPending int

	// npending is the current number of pending instances.
	npending int32

	// Logger for manager events.
	log *log.Logger

	waitc  chan *waiter
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu   sync.Mutex
	cond *ctxsync.Cond
	pool map[string]bool

	sync chan struct{}

	refreshInterval, launchTimeout, drainTimeout time.Duration
}

// NewManager creates a manager for the given managed cluster with the specified parameters.
func NewManager(c ManagedCluster, maxInstances, maxPending int, log *log.Logger) *Manager {
	m := &Manager{
		cluster:         c,
		maxInstances:    maxInstances,
		maxPending:      maxPending,
		log:             log,
		refreshInterval: defaultRefreshInterval,
		launchTimeout:   instanceLaunchTimeout,
		drainTimeout:    defaultDrainTimeout,
	}
	return m
}

// Start initializes and starts the cluster manager (and its management goroutines)
func (m *Manager) Start() {
	var ctx context.Context
	ctx, m.cancel = context.WithCancel(context.Background())
	m.waitc = make(chan *waiter)
	m.sync = make(chan struct{})
	m.cond = ctxsync.NewCond(&m.mu)
	m.pool = make(map[string]bool)

	// This go-routine maintains the state of the cluster by periodically `Refresh`ing it.
	// Refreshing the cluster (periodic/forced-immediate) is achieved by communicating with this go-routine.
	m.wg.Add(1)
	go m.maintain(ctx)
	// Sync forces an immediate syncing of cluster state (and will block until its complete)
	m.forceSync()
	// This go-routine services requests to expand cluster capacity
	m.wg.Add(1)
	go m.loop(ctx)
}

// Shutdown shuts down the manager and waits for all its go-routines to finish.
func (m *Manager) Shutdown() {
	if m.cancel == nil { // Start was never called
		return
	}
	m.cancel()
	m.wg.Wait()
}

// Allocate requests the Manager to allocate an instance for the given requirements
// Returns a channel the caller can wait on for confirmation request has been fulfilled.
func (m *Manager) Allocate(ctx context.Context, req reflow.Requirements) <-chan struct{} {
	w := &waiter{
		Requirements: req,
		ctx:          ctx,
		c:            make(chan struct{}),
	}
	m.waitc <- w
	return w.c
}

func (m *Manager) nPendingAdd(delta int) {
	atomic.AddInt32(&m.npending, int32(delta))
}

func (m *Manager) nPending() int {
	return int(atomic.LoadInt32(&m.npending))
}

func (m *Manager) nPool() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pool)
}

// getInstanceAllocations returns the instances needed to satisfy the waiters.
// It uses a greedy algorithm to group as many waiter requests as possible into a instance.
func (m *Manager) getInstanceAllocations(waiters []*waiter) (todo []InstanceSpec) {
	var resources []reflow.Resources
	for _, w := range waiters {
		width := w.Width
		if width == 0 {
			width = 1
		}
		for i := width; i > 0; i-- {
			resources = append(resources, w.Min)
		}
	}
	var (
		need        reflow.Resources
		group       int
		oldMin, min InstanceSpec
		ok          bool
		i           int
	)
	for i < len(resources) {
		res := resources[i]
		need.Add(need, res)
		min, ok = m.cluster.Available(need)
		switch {
		case group == 0 && !ok:
			i++
			need.Set(reflow.Resources{})
			m.log.Debugf("no currently available instance type can satisfy resource requirements %v", res)
		case !ok:
			todo = append(todo, oldMin)
			need.Set(reflow.Resources{})
			group = 0
		case ok:
			oldMin = min
			group++
			i++
		}
	}
	if group > 0 {
		todo = append(todo, oldMin)
	}
	return
}

// loop services requests to expand the cluster's capacity.
func (m *Manager) loop(pctx context.Context) {
	var (
		waiters  []*waiter
		launched sync.WaitGroup
		pending  reflow.Resources
		done     = make(chan ManagedInstance)
	)
	defer func() {
		// Before we exit, we cancel all launchers (make sure they are done) and notify all waiters.
		launched.Wait()
		m.wg.Done()
	}()

	launch := func(spec InstanceSpec) {
		defer launched.Done()
		ctx, cancel := context.WithTimeout(pctx, m.launchTimeout)
		defer cancel()
		i := m.cluster.Launch(ctx, spec)
		if i.Valid() {
			// While the instance is ready, we wait for it to be recognized by the managed cluster.
			if err := m.wait(ctx, i.ID); err != nil {
				i = spec.Instance("")
			}
		}
		// If pctx is done, then there's nobody listening on `done` channel
		if pctx.Err() != nil {
			return
		}
		done <- i
	}

	for {
		var needPoll bool
		// Here we try to pack resource requests. First, we order each
		// request by the "magnitude" of the request (as defined by
		// (Resources).ScaledDistance) and then greedily pack the requests
		// until there is no instance type that can accommodate them.
		sort.Slice(waiters, func(i, j int) bool {
			return waiters[i].Min.ScaledDistance(nil) < waiters[j].Min.ScaledDistance(nil)
		})
		if m.log.At(log.DebugLevel) && m.nPending() > 0 {
			s := make([]string, len(waiters))
			for i, w := range waiters {
				s[i] = fmt.Sprintf("waiter%d%s", i, w.Min)
			}
			m.log.Debugf("pending%s %s", pending, strings.Join(s, ", "))
		}
		var waiting reflow.Resources
		for _, w := range waiters {
			waiting.Add(waiting, w.Max())
		}
		// First skip waiters that are already getting their resources
		// satisfied.
		//
		// TODO(marius): this should take into account the actual
		// granularity of the pending instances. This doesn't matter too
		// much since allocation is ordered by size, and thus we'll make
		// progress since no instances smaller than the smallest allocation
		// are ever launched. But it could be wasteful if there's a lot of
		// churn.
		var (
			i       int
			howmuch reflow.Resources
		)
		for i < len(waiters) {
			howmuch.Add(howmuch, waiters[i].Min)
			if !pending.Available(howmuch) {
				break
			}
			i++
		}
		needMore := len(waiters) > 0 && i != len(waiters)
		todo := m.getInstanceAllocations(waiters[i:])

		if needMore && len(todo) == 0 {
			var r reflow.Requirements
			for _, w := range waiters[i:] {
				r.Add(w.Requirements)
			}
			m.log.Printf("resource requirements are unsatisfiable by current instance selection: %s", r)
			needPoll = true
			goto sleep
		}
		for len(todo) > 0 && m.nPending() < m.maxPending && m.nPool()+m.nPending() < m.maxInstances {
			var spec InstanceSpec
			spec, todo = todo[0], todo[1:]
			pending.Add(pending, spec.Resources)
			m.nPendingAdd(1)
			m.log.Debugf("launch %v%v pending%v", spec.Type, spec.Resources, pending)
			launched.Add(1)
			go launch(spec)
		}
		if len(todo) > 0 && m.nPool() >= m.maxInstances {
			m.log.Debugf("cannot schedule more instances (max instances %d reached)", m.maxInstances)
			needPoll = true
		}
	sleep:
		var pollch <-chan time.Time
		if needPoll {
			pollch = time.After(m.refreshInterval)
		}
		m.cluster.Notify(waiting, pending)
		select {
		case <-pctx.Done():
			return
		case <-pollch:
		case inst := <-done:
			pending.Sub(pending, inst.Resources)
			m.nPendingAdd(-1)
			// If we didn't actually get an instance, can't notify any waiters.
			if !inst.Valid() {
				continue
			}
			var (
				ws        []*waiter
				available = inst.Resources
				nnotify   int
			)
			for _, w := range waiters {
				if w.ctx.Err() != nil {
					continue
				}
				if available.Available(w.Min) {
					var tmp reflow.Resources
					tmp.Min(w.Max(), available)
					available.Sub(available, tmp)
					w.notify()
					nnotify++
				} else {
					ws = append(ws, w)
				}
			}
			waiters = ws
			m.log.Debugf("added instance %s resources%s pending%s available%s npending:%d waiters:%d notified:%d",
				inst.Type, inst.Resources, pending, available, m.nPending(), len(waiters), nnotify)
		case w := <-m.waitc:
			// If there was one waiter, we'll wait upto `drainTimeout` each time we find more.
			// After `drainTimeout` of no new waiters, we'll continue the servicing loop.
			var drained bool
			t := time.NewTimer(m.drainTimeout)
			for !drained {
				waiters = append(waiters, w)
				t.Reset(m.drainTimeout)
				select {
				case w = <-m.waitc:
				case <-t.C:
					drained = true
				}
			}
			var ws []*waiter
			for _, w := range waiters {
				if w.ctx.Err() == nil {
					ws = append(ws, w)
				}
			}
			waiters = ws
			t.Stop()
		}
	}
}

// wait waits until the given instance ID is recognized by the managed cluster or if the ctx is complete.
// Returns the context's error if it completes while waiting.
func (m *Manager) wait(ctx context.Context, id string) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if _, ok := m.pool[id]; ok {
			break
		}
		select {
		case m.sync <- struct{}{}:
		default:
		}
		err = m.cond.Wait(ctx)
		if err != nil {
			break
		}
	}
	return
}

// forceSync forces an immediate sync/refresh of the managed cluster's state.
func (m *Manager) forceSync() {
	m.sync <- struct{}{}
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.cond.Wait(context.Background())
}

// maintain periodically refreshes the managed cluster. Also services requests
// (through calls to `forceSync` or `wait`) to refresh the managed cluster's state.
func (m *Manager) maintain(ctx context.Context) {
	defer m.wg.Done()
	tick := time.NewTicker(m.refreshInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-m.sync:
		case <-ctx.Done():
			return
		}
		idSet, err := m.cluster.Refresh(ctx)
		if err != nil {
			m.log.Errorf("maintain: %v", err)
			continue
		}
		m.mu.Lock()
		m.pool = idSet
		m.cond.Broadcast()
		m.mu.Unlock()
	}
}
