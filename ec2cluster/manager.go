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
	instanceLaunchTimeout = 10 * time.Minute
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

	// Refresh refreshes the managed cluster and returns a mapping of instance ID to instance type.
	Refresh(ctx context.Context) (map[string]string, error)

	// Available returns any available instance specification that can satisfy the need.
	// The returned InstanceSpec should be subsequently be 'Launch'able.
	Available(need reflow.Resources, maxPrice float64) (InstanceSpec, bool)

	// Notify notifies the managed cluster of the currently waiting and pending
	// amount of resources.
	Notify(waiting, pending reflow.Resources)

	// InstancePriceUSD returns the maximum hourly price bid in USD for the given instance type.
	InstancePriceUSD(typ string) float64

	// CheapestInstancePriceUSD returns the minimum hourly price bid in USD for all known instance types.
	CheapestInstancePriceUSD() float64
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

	// maxHourlyCostUSD is the maximum hourly cost of the pool that is permitted (in USD).
	maxHourlyCostUSD float64
	// maxPending is the maximum number of pending instances permitted.
	maxPending int

	// Logger for manager events.
	log *log.Logger

	waitc  chan *waiter
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu   sync.Mutex
	cond *ctxsync.Cond
	// pool maps the instanceIds of running instances to their types.
	pool map[string]string
	// npending is the current number of pending instances.
	npending int32
	// pendinghourlycostusd is the hourly cost of pending instances permitted (in USD).
	pendinghourlycostusd float64

	sync chan struct{}

	refreshInterval, launchTimeout, drainTimeout time.Duration
}

// NewManager creates a manager for the given managed cluster with the specified parameters.
func NewManager(c ManagedCluster, maxHourlyCostUSD float64, maxPendingInstances int, log *log.Logger) *Manager {
	m := &Manager{
		cluster:          c,
		log:              log,
		maxHourlyCostUSD: maxHourlyCostUSD,
		maxPending:       maxPendingInstances,
		refreshInterval:  defaultRefreshInterval,
		launchTimeout:    instanceLaunchTimeout,
		drainTimeout:     defaultDrainTimeout,
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
	m.pool = make(map[string]string)

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

// SetTimeouts sets the various timeout durations (primarily used for integration testing).
func (m *Manager) SetTimeouts(refreshInterval, drainTimeout, launchTimeout time.Duration) {
	m.refreshInterval = refreshInterval
	m.drainTimeout = drainTimeout
	m.launchTimeout = launchTimeout
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

// nPool returns the count of instances in the pool.
func (m *Manager) nPool() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pool)
}

// nPending returns the count of pending instances.
func (m *Manager) nPending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int(m.npending)
}

// pendingHourlyCostUSD returns the hourly cost of pending instances.
func (m *Manager) pendingHourlyCostUSD() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pendinghourlycostusd
}

// markPending adds the given instance type to the pending tallies on the manager.
func (m *Manager) markPending(spec InstanceSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendinghourlycostusd += m.cluster.InstancePriceUSD(spec.Type)
	m.npending += 1
}

// markDonePending subtracts the given instance type from the pending tallies on the manager.
func (m *Manager) markDonePending(spec InstanceSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendinghourlycostusd -= m.cluster.InstancePriceUSD(spec.Type)
	m.npending -= 1
}

// hourlyCostUSD returns the hourly cost in USD of the pool.
func (m *Manager) hourlyCostUSD() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0.0
	for _, typ := range m.pool {
		total += m.cluster.InstancePriceUSD(typ)
	}
	return total
}

// remainingBudgetUSD returns the remaining budget in USD for launching new instances.
func (m *Manager) remainingBudgetUSD(includePending bool) float64 {
	c := m.hourlyCostUSD()
	if includePending {
		c += m.pendingHourlyCostUSD()
	}
	return m.maxHourlyCostUSD - c
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
	// Return early if we don't have any budget to work with.
	if budget, cheapest := m.remainingBudgetUSD(false), m.cluster.CheapestInstancePriceUSD(); budget-cheapest < 0 {
		return
	}
	// Return available instances within the remaining budget. The total price of all InstanceSpecs appended to todo
	// can exceed the remaining budget; it is up to the caller to ensure that only affordable InstanceSpecs are launched.
	for i < len(resources) {
		res := resources[i]
		need.Add(need, res)
		min, ok = m.cluster.Available(need, m.remainingBudgetUSD(false))
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
		t        = time.NewTimer(time.Minute)
	)
	t.Stop() // stop the timer immediately, we don't need it yet.
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
		// Attempt to signal on the `done` channel. If pctx is done, then there's nobody listening
		// on `done` channel and we should return early.
		select {
		case <-pctx.Done():
			return
		case done <- i:
			return
		}
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
		for len(todo) > 0 && m.nPending() < m.maxPending && m.cluster.InstancePriceUSD(todo[0].Type) <= m.remainingBudgetUSD(true) {
			var spec InstanceSpec
			spec, todo = todo[0], todo[1:]
			pending.Add(pending, spec.Resources)
			m.markPending(spec)
			m.log.Debugf("launch %v%v pending%v", spec.Type, spec.Resources, pending)
			launched.Add(1)
			go launch(spec)
		}
		if budget, cheapest := m.remainingBudgetUSD(false), m.cluster.CheapestInstancePriceUSD(); len(todo) > 0 && budget-cheapest < 0 {
			m.log.Printf("cannot schedule more instances: remaining budget $%.2f (cheapest instance price $%.2f)", budget, cheapest)
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
			m.markDonePending(inst.InstanceSpec)
			// If we didn't actually get an instance, can't notify any waiters.
			if !inst.Valid() {
				continue
			}
			var (
				available = inst.Resources
				nnotify   int
			)
			n := 0
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
					waiters[n] = w
					n++
				}
			}
			waiters = waiters[:n]
			m.log.Debugf("added instance %s resources%s pending%s available%s waiters:%d notified:%d",
				inst.Type, inst.Resources, pending, available, len(waiters), nnotify)
		case w := <-m.waitc:
			// If there was one waiter, we'll wait upto `drainTimeout` each time we find more.
			// After `drainTimeout` of no new waiters, we'll continue the servicing loop.
			var drained bool
			for !drained {
				waiters = append(waiters, w)
				t.Reset(m.drainTimeout)
				select {
				case w = <-m.waitc:
					if !t.Stop() {
						<-t.C
					}
				case <-t.C:
					drained = true
				}
			}
			n := 0
			for _, w := range waiters {
				if w.ctx.Err() == nil {
					waiters[n] = w
					n++
				}
			}
			waiters = waiters[:n]
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
		typesByID, err := m.cluster.Refresh(ctx)
		if err != nil {
			m.log.Errorf("maintain: %v", err)
			continue
		}
		m.mu.Lock()
		m.pool = typesByID
		m.cond.Broadcast()
		m.mu.Unlock()
	}
}
