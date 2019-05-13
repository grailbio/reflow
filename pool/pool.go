// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package pool implements resource pools for reflow. Reflow manages
// resources in units of "allocs" -- an a resource allocation that
// exists on a single machine, and to which is attached a shared
// repository with the results of all execs within that Alloc. Allocs
// are leased-- they must be kept alive to guarantee continuity; they
// are collected as a unit.
package pool

import (
	"context"
	"fmt"
	"os/user"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/traverse"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"golang.org/x/sync/errgroup"
)

const (
	keepaliveInterval    = 2 * time.Minute
	keepaliveTimeout     = 10 * time.Second
	keepaliveMaxInterval = 5 * time.Minute
	keepaliveTries       = 5

	offersTimeout = 10 * time.Second

	pollInterval = 10 * time.Second
)

func init() {
	infra.Register(make(Labels))
}

// Alloc represent a resource allocation attached to a single
// executor, a reservation of resources on a single node.
type Alloc interface {
	reflow.Executor

	// Pool returns the pool from which the alloc is reserved.
	Pool() Pool

	// ID returns the ID of alloc in the pool. The format of the ID is opaque.
	ID() string

	// Keepalive maintains the lease of this Alloc. It must be called again
	// before the expiration of the returned duration. The user may also
	// request a maintenance interval. This is just a hint and may not be
	// respected by the Alloc.
	Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error)

	// Inspect returns Alloc metadata.
	Inspect(ctx context.Context) (AllocInspect, error)

	// Free frees the alloc. Pending tasks are killed but its Repository
	// is not collected. Some implementations may implement "zombie"
	// allocs so that they can be inspected after Free is called.
	Free(ctx context.Context) error
}

// Labels represents a set of metadata labels for a run.
type Labels map[string]string

// Add returns a copy of Labels l with an added key and value.
func (l Labels) Add(k, v string) Labels {
	m := l.Copy()
	m[k] = v
	return m
}

// Copy returns a copy of l.
func (l Labels) Copy() Labels {
	m := make(Labels)
	for k, v := range l {
		m[k] = v
	}
	return m
}

// AllocMeta contains Alloc requester metadata.
type AllocMeta struct {
	Want   reflow.Resources
	Owner  string
	Labels Labels
}

// AllocInspect contains Alloc metadata.
type AllocInspect struct {
	ID            string
	Resources     reflow.Resources
	Meta          AllocMeta
	Created       time.Time
	LastKeepalive time.Time
	Expires       time.Time
}

// keepalive returns the interval to the next keepalive.
func keepalive(ctx context.Context, alloc Alloc) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, keepaliveTimeout)
	defer cancel()
	return alloc.Keepalive(ctx, keepaliveInterval)
}

// Keepalive maintains the lease on alloc until it expires (e.g., by
// calling Free), or until the passed-in context is cancelled.
// Keepalive retries errors by exponential backoffs with a fixed
// configuration.
func Keepalive(ctx context.Context, log *log.Logger, alloc Alloc) error {
	for {
		var (
			iv   time.Duration
			err  error
			wait = 2 * time.Second
			last time.Time
		)
		for i := 0; i < keepaliveTries; i++ {
			if !last.IsZero() && time.Since(last) > iv {
				log.Errorf("failed to maintain keepalive within interval %s", iv)
			}
			iv, err = keepalive(ctx, alloc)
			if err == nil || errors.Is(errors.Fatal, err) {
				break
			}
			// Context errors indicate that our caller has given up.
			// We blindly retry other errors.
			if err := ctx.Err(); err != nil {
				return err
			}
			time.Sleep(wait)
			wait *= time.Duration(2)
		}
		if err != nil {
			return err
		}
		last = time.Now()
		// Add some wiggle room.
		iv -= 30 * time.Second
		if iv < 0*time.Second {
			continue
		}
		if iv > keepaliveMaxInterval {
			iv = keepaliveMaxInterval
		}
		select {
		case <-time.After(iv):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Offer represents an offer of resources, from which an Alloc can be created.
type Offer interface {
	// ID returns the ID of the offer. It is an opaque string.
	ID() string

	// Pool returns the pool from which this Offer is extended.
	Pool() Pool

	// Available returns the amount of total available resources
	// that can be accepted.
	Available() reflow.Resources

	// Accept accepts this Offer with the given Alloc metadata. The
	// metadata includes how many resources are requested. Accept may
	// return ErrOfferExpired if another client accepted the offer
	// first.
	Accept(ctx context.Context, meta AllocMeta) (Alloc, error)
}

// OfferJSON is the JSON structure used to describe offers.
type OfferJSON struct {
	// The ID of the offer.
	ID string
	// The amount of available resources the offer represents.
	Available reflow.Resources
}

// Pool is a resource pool which manages a set of allocs.
type Pool interface {
	// ID returns the ID of the pool. It is an opaque string.
	ID() string

	// Alloc returns the Alloc named by an ID.
	Alloc(ctx context.Context, id string) (Alloc, error)

	// Allocs enumerates the available Allocs in this Pool.
	Allocs(ctx context.Context) ([]Alloc, error)

	// Offer returns the Offer identified by an id.
	Offer(ctx context.Context, id string) (Offer, error)

	// Offers returns the set of current Offers from this Pool.
	// TODO(marius): it would be good to have a scanning/long-poll
	// version of this so that clients do not have to do their own polling.
	Offers(ctx context.Context) ([]Offer, error)
}

var (
	errUnavailable  = errors.New("no allocs available in pool")
	errTooManyTries = errors.New("too many tries")
)

// Allocate attempts to place an Alloc on a pool with the given
// resource requirements.
func Allocate(ctx context.Context, pool Pool, req reflow.Requirements, labels Labels) (Alloc, error) {
	const maxRetries = 6
	for n := 0; n < maxRetries; n++ {
		offers, err := pool.Offers(ctx)
		if err != nil {
			return nil, err
		}
		pick := Pick(offers, req.Min, req.Max())
		if pick == nil {
			return nil, errors.E(errors.Unavailable, errUnavailable)
		}
		// Pick the smallest of max and what's available. If memory, disk,
		// or CPU are left zero, we grab the whole alloc so that we don't
		// unnecessarily leave resources on the table; they can become
		// useful later in execution, and it leaves the rest of the offer
		// unusable anyway. We do the same if it's a wide request.
		avail := pick.Available()
		var want reflow.Resources
		want.Min(req.Max(), avail)
		var tmp reflow.Resources
		tmp.Sub(avail, want)
		if tmp["cpu"] <= 0 || tmp["mem"] <= 0 || tmp["disk"] <= 0 || req.Wide() {
			want.Set(avail)
		}
		meta := AllocMeta{Want: want, Labels: labels}

		// TODO(marius): include more flow metadata here.
		// (expr, parameters, etc.)
		u, err := user.Current()
		if err != nil {
			meta.Owner = "<unknown>"
		} else {
			meta.Owner = fmt.Sprintf("%s <%s>", u.Name, u.Username)
		}
		alloc, err := pick.Accept(ctx, meta)
		if err == nil {
			return alloc, err
		}
		if !errors.Is(errors.NotExist, err) {
			return nil, err
		}
	}
	return nil, errors.E(errors.Unavailable, errTooManyTries)
}

// Pick selects an offer best matching a minimum and maximum resource
// requirements. It picks the offer which has at least the minimum
// amount of resources but as close to maximum as possible.
func Pick(offers []Offer, min, max reflow.Resources) Offer {
	var pick Offer
	var distance float64
	for _, offer := range offers {
		switch {
		case !offer.Available().Available(min):
			continue
		case pick == nil:
			pick = offer
			distance = offer.Available().ScaledDistance(max)
		default:
			curDist := offer.Available().ScaledDistance(max)
			if curDist < distance {
				pick = offer
				distance = curDist
			}
		}
	}
	return pick
}

// Mux is a Pool implementation that multiplexes and aggregates
// multiple underlying pools. Mux uses a URI naming scheme to
// address allocs and offers. Namely, the ID the underlying pool,
// followed by '/' and then the ID of the alloc or offer. For example,
// the URI
//
//	1.worker.us-west-2a.reflowy.eng.aws.grail.com:9000/4640204a5fd6ce42
//
// Names the alloc with ID "4640204a5fd6ce42" of the pool named
// 1.worker.us-west-2a.reflowy.eng.aws.grail.com:9000.
type Mux struct {
	pools atomic.Value
}

// SetPools sets the Mux's underlying pools.
func (m *Mux) SetPools(pools []Pool) {
	m.pools.Store(pools)
}

// Pools retrieves the Mux's underlying pools.
func (m *Mux) Pools() []Pool {
	p := m.pools.Load()
	if p == nil {
		return nil
	}
	return p.([]Pool)
}

// Size tells how many pools the Mux comprises.
func (m *Mux) Size() int {
	return len(m.Pools())
}

// ID returns the ID of this pool. It is always empty.
func (m *Mux) ID() string { return "" }

// Alloc returns an alloc named by a URI.
func (m *Mux) Alloc(ctx context.Context, uri string) (Alloc, error) {
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) != 2 {
		return nil, errors.Errorf("alloc %v: invalid URI", uri)
	}
	poolID, allocID := parts[0], parts[1]
	for _, p := range m.Pools() {
		if p.ID() == poolID {
			return p.Alloc(ctx, allocID)
		}
	}
	return nil, errors.E("alloc", uri, errors.NotExist)
}

// Allocs returns the current set of allocs over all underlying pools.
func (m *Mux) Allocs(ctx context.Context) ([]Alloc, error) {
	pools := m.Pools()
	allocss := make([][]Alloc, len(pools))
	err := traverse.Each(len(allocss), func(i int) error {
		var err error
		allocss[i], err = pools[i].Allocs(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	var allocs []Alloc
	for _, a := range allocss {
		allocs = append(allocs, a...)
	}
	return allocs, nil
}

// Offer looks up the offer named by the given URI.
func (m *Mux) Offer(ctx context.Context, uri string) (Offer, error) {
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) != 2 {
		return nil, errors.Errorf("offer %v: bad URI", uri)
	}
	poolID, offerID := parts[0], parts[1]
	for _, p := range m.Pools() {
		if p.ID() == poolID {
			return p.Offer(ctx, offerID)
		}
	}
	return nil, errors.E("offer", uri, errors.NotExist)
}

// Offers enumerates all the offers available from the underlying
// pools. Offers applies a timeout to the underlying requests;
// requests that do not meet the deadline are simply dropped.
func (m *Mux) Offers(ctx context.Context) ([]Offer, error) {
	pools := m.Pools()
	offerss := make([][]Offer, len(pools))
	deadline := time.Now().Add(offersTimeout)
	var cancel func()
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		// TODO(marius): we should give this some wiggle room.
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}
	err := traverse.Each(len(offerss), func(i int) error {
		var err error
		offerss[i], err = pools[i].Offers(ctx)
		if err == context.DeadlineExceeded {
			err = nil
		}
		return nil
	})
	cancel()
	if err != nil {
		return nil, err
	}
	var offers []Offer
	for _, o := range offerss {
		offers = append(offers, o...)
	}
	return offers, nil
}

// Allocs fetches all of the allocs from the provided pool. If it
// encounters any failure (e.g., due to a context timeout), they are
// logged, but ignored. The returned slice contains all the
// successfuly fetched allocs.
func Allocs(ctx context.Context, pool Pool, log *log.Logger) []Alloc {
	p, ok := pool.(interface {
		Pools() []Pool
	})
	if !ok {
		allocs, err := pool.Allocs(ctx)
		if err != nil {
			log.Errorf("allocs %v: %v", pool, err)
		}
		return allocs
	}
	pools := p.Pools()
	allocss := make([][]Alloc, len(pools))
	g, ctx := errgroup.WithContext(ctx)
	for i := range pools {
		i := i
		g.Go(func() error {
			var err error
			allocss[i], err = pools[i].Allocs(ctx)
			if err != nil {
				log.Errorf("allocs %v: %v", pools[i], err)
			}
			return nil
		})
	}
	g.Wait()
	var allocs []Alloc
	for _, a := range allocss {
		allocs = append(allocs, a...)
	}
	return allocs
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
