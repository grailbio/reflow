// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

const offerID = "1"

var errOfferExpired = errors.New("offer expired")

// AllocManager manages the creation and destruction of Allocs and is responsible
// for managing all the underlying resources necessary for an Alloc.
type AllocManager interface {
	// Name returns the name of the alloc manager
	Name() string

	// New creates a new alloc with the given metadata and an initial keepalive duration.
	New(ctx context.Context, id string, meta AllocMeta, keepalive time.Duration, existing []Alloc) (Alloc, error)

	// Kill kills the given alloc.
	Kill(a Alloc) error
}

// ResourcePool implements a resource pool backed by an alloc manager.
// ResourcePool simply manages a given set of resources by allowing the creation
// of allocs within it using a subset of the total resourecs.
// The underlying AllocManager is responsible for creating and destroying the actual allocs.
type ResourcePool struct {
	manager AllocManager

	mu        sync.Mutex
	allocs    map[string]Alloc // the set of active allocs
	resources reflow.Resources // the total amount of available resources
	stopped   bool

	log *log.Logger
}

func NewResourcePool(manager AllocManager, log *log.Logger) ResourcePool {
	return ResourcePool{manager: manager, log: log}
}

func (p *ResourcePool) Init(r reflow.Resources, m map[string]Alloc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resources.Set(r)
	p.allocs = make(map[string]Alloc)
	for id, a := range m {
		p.allocs[id] = a
	}
}

func (p *ResourcePool) Resources() reflow.Resources {
	p.mu.Lock()
	defer p.mu.Unlock()
	var r reflow.Resources
	r.Set(p.resources)
	return r
}

// Available returns the amount of currently available resources:
// The total less what is occupied by active allocs.
func (p *ResourcePool) Available() reflow.Resources {
	p.mu.Lock()
	defer p.mu.Unlock()
	var reserved reflow.Resources
	for _, alloc := range p.allocs {
		if !AllocExpired(alloc) {
			reserved.Add(reserved, alloc.Resources())
		}
	}
	var avail reflow.Resources
	avail.Sub(p.resources, reserved)
	return avail
}

// New creates a new alloc with the given meta. new collects expired
// allocs as needed to make room for the resource requirements as
// indicated by meta.
func (p *ResourcePool) New(ctx context.Context, meta AllocMeta) (Alloc, error) {
	if meta.Want.Equal(nil) {
		return nil, errors.E("ResourcePool.New", errors.Precondition, fmt.Errorf("illegal request for an empty resources: %s", meta.Want))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, errors.Errorf("alloc %v: shutting down", meta)
	}
	var (
		total   = p.resources
		used    reflow.Resources
		expired []Alloc
	)
	for _, alloc := range p.allocs {
		used.Add(used, alloc.Resources())
		if AllocExpired(alloc) {
			expired = append(expired, alloc)
		}
	}
	// ACHTUNG N²! (But n is small.)
	n := 0
	collect := expired[:]
	// TODO: preferentially prefer those allocs which will give us the
	// resource types we need.
	p.log.Printf("alloc total%s used%s want%s", total, used, meta.Want)
	var free reflow.Resources
	for {
		free.Sub(total, used)
		if free.Available(meta.Want) || len(expired) == 0 {
			break
		}
		max := 0
		for i := 1; i < len(expired); i++ {
			if AllocExpiredBy(expired[i]) > AllocExpiredBy(expired[max]) {
				max = i
			}
		}
		alloc := expired[max]
		expired[0], expired[max] = expired[max], expired[0]
		expired = expired[1:]
		used.Sub(used, alloc.Resources())
		n++
	}
	collect = collect[:n]
	if !free.Available(meta.Want) {
		return nil, errors.E("alloc", errors.NotExist, errOfferExpired)
	}
	remainingIds := map[string]bool{}
	for id := range p.allocs {
		remainingIds[id] = true
	}
	for _, alloc := range collect {
		delete(remainingIds, alloc.ID())
	}
	var remaining []Alloc
	for id, alloc := range p.allocs {
		if _, ok := remainingIds[id]; ok {
			remaining = append(remaining, alloc)
		}
	}
	id := newID()
	alloc, err := p.manager.New(ctx, id, meta, keepaliveInterval, remaining)
	if err != nil {
		return nil, err
	}
	p.allocs[id] = alloc
	for _, alloc := range collect {
		delete(p.allocs, alloc.ID())
		if err := p.manager.Kill(alloc); err != nil {
			p.log.Errorf("error killing alloc: %s", err)
		} else {
			p.log.Printf("alloc reclaim %s", alloc.ID())
		}
	}
	return alloc, nil
}

// Free frees alloc a from this ResourcePool and invokes `AllocManager.Kill` on it.
func (p *ResourcePool) Free(a Alloc) error {
	id := a.ID()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allocs[id] != a {
		return nil
	}
	delete(p.allocs, id)
	return p.manager.Kill(a)
}

// Alive tells whether an alloc's lease is current.
func (p *ResourcePool) Alive(a Alloc) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocs[a.ID()] == a
}

// ID returns the ID of the resource pool.
func (p *ResourcePool) ID() string {
	return fmt.Sprintf("resourcepool(%s)", p.manager.Name())
}

// Offer looks up the an offer by ID.
func (p *ResourcePool) Offer(ctx context.Context, id string) (Offer, error) {
	offers, err := p.Offers(ctx)
	if err != nil {
		return nil, err
	}
	if len(offers) == 0 {
		return nil, errors.E("offer", id, errors.NotExist, errOfferExpired)
	}
	if id != offerID {
		return nil, errors.E("offer", id, errors.NotExist, errOfferExpired)
	}
	return offers[0], nil
}

// Offers enumerates all the current offers of this ResourcePool. It
// always returns either no offers, when there are no more
// available resources, or 1 offer comprising the entirety of
// available resources.
func (p *ResourcePool) Offers(ctx context.Context) ([]Offer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, nil
	}
	var reserved reflow.Resources
	for _, alloc := range p.allocs {
		if !AllocExpired(alloc) {
			reserved.Add(reserved, alloc.Resources())
		}
	}
	var available reflow.Resources
	available.Sub(p.resources, reserved)
	if available["mem"] == 0 || available["cpu"] == 0 || available["disk"] == 0 {
		return nil, nil
	}
	return []Offer{&offer{p, offerID, available}}, nil
}

// Alloc looks up an alloc by ID.
func (p *ResourcePool) Alloc(ctx context.Context, id string) (Alloc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if alloc, ok := p.allocs[id]; ok {
		return alloc, nil
	}
	return nil, errors.E("alloc", id, errors.NotExist)
}

// Allocs lists all the active allocs in this resource pool.
func (p *ResourcePool) Allocs(ctx context.Context) ([]Alloc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	allocs := make([]Alloc, len(p.allocs))
	i := 0
	for _, a := range p.allocs {
		allocs[i] = a
		i++
	}
	return allocs, nil
}

// StopIfIdle stops the pool if it is idle. Returns whether the pool was stopped.
// If the pool was not stopped (ie, it was not idle), returns the current max duration
// to expiry of all allocs in the resource pool.   Note that further alloc
// keepalive calls can make the pool unstoppable after the given duration passes.
func (p *ResourcePool) StopIfIdleFor(d time.Duration) (bool, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var (
		idle            = true
		maxTimeToExpiry time.Duration
	)
	for _, alloc := range p.allocs {
		expiredBy := AllocExpiredBy(alloc)
		if expiredBy < d {
			idle = false
		}
		// if alloc isn't expired, expiredBy is negative.
		if maxTimeToExpiry > expiredBy {
			maxTimeToExpiry = expiredBy
		}
	}
	if idle {
		p.stopped = true
		return true, 0
	}
	return false, -maxTimeToExpiry
}

type offer struct {
	m         *ResourcePool
	id        string
	resources reflow.Resources
}

func (o *offer) ID() string                  { return o.id }
func (o *offer) Pool() Pool                  { return o.m }
func (o *offer) Available() reflow.Resources { return o.resources }
func (o *offer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	return o.m.New(ctx, meta)
}

// newID generates a random hex string.
func newID() string {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b[:])
}
