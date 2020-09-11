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
	"container/heap"
	"context"
	"fmt"
	"os/user"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"golang.org/x/sync/errgroup"
)

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
		alloc, err := allocate(ctx, pool, req, labels)
		if err == nil {
			return alloc, nil
		}
		if err != errUnavailable {
			return nil, errors.E(errors.Unavailable, err)
		}
	}
	return nil, errors.E(errors.Unavailable, errTooManyTries)
}

const maxOffersToConsider = 10

func allocate(ctx context.Context, pool Pool, req reflow.Requirements, labels Labels) (Alloc, error) {
	offers, err := pool.Offers(ctx)
	if err != nil {
		return nil, err
	}
	// TODO(swami): Instead of fixed n, should we vary based on number of offers ?
	ordered := pickN(offers, maxOffersToConsider, req.Min, req.Max())

	for _, pick := range ordered {
		// pick the smallest of max and what's available. If memory, disk,
		// or CPU are left zero, we grab the whole alloc so that we don't
		// unnecessarily leave resources on the table; they can become
		// useful later in execution, and it leaves the rest of the offer
		// unusable anyway. We do the same if it's a wide request.
		avail := pick.Available()
		var want reflow.Resources
		want.Min(req.Max(), avail)
		var tmp reflow.Resources
		tmp.Sub(avail, want)
		if tmp["cpu"] <= 0 || tmp["mem"] <= 0 || tmp["disk"] <= 0 {
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
	return nil, errUnavailable
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

// pick selects an offer best matching a minimum and maximum resource
// requirements. It picks the offer which has at least the minimum
// amount of resources but as close to maximum as possible.
func pick(offers []Offer, min, max reflow.Resources) Offer {
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

// pickN returns upto n offers in decreasing order of "best match" defined as follows:
// - all offers >= max appear first, in increasing order of distance from max.
// - offers less than max appear next, again in increasing order of distance from max.
// - offers less than min are omitted.
func pickN(offers []Offer, n int, min, max reflow.Resources) []Offer {
	q := &offerq{max: max}
	for _, offer := range offers {
		if !offer.Available().Available(min) {
			continue
		}
		heap.Push(q, offer)
		// prune the heap if larger than n.
		if q.Len() > n {
			heap.Pop(q)
		}
	}
	// return the reverse of the queue.
	ordered := make([]Offer, q.Len())
	for i := len(ordered) - 1; i >= 0; i-- {
		x := heap.Pop(q)
		ordered[i] = x.(Offer)
	}
	return ordered
}

// offerq implements a priority queue of offers, ordered in the following manner:
// - offers less than max appear first, in decreasing order of distance from max.
// - offers >= max appear next, again in decreasing order of distance from max.
type offerq struct {
	max    reflow.Resources
	offers []Offer
}

// Len implements sort.Interface/heap.Interface.
func (q offerq) Len() int { return len(q.offers) }

// Less implements sort.Interface/heap.Interface.
func (q offerq) Less(i, j int) bool {
	ri, rj := q.offers[i].Available(), q.offers[j].Available()
	availi, availj := ri.Available(q.max), rj.Available(q.max)
	disti, distj := ri.ScaledDistance(q.max), rj.ScaledDistance(q.max)
	switch {
	case availi && availj:
		return disti > distj
	case !availi && !availj:
		return disti > distj
	case !availi:
		return true
	}
	return false
}

// Swap implements heap.Interface/sort.Interface
func (q offerq) Swap(i, j int) {
	q.offers[i], q.offers[j] = q.offers[j], q.offers[i]
}

// Push implements heap.Interface.
func (q *offerq) Push(x interface{}) {
	o := x.(Offer)
	q.offers = append(q.offers, o)
}

// Pop implements heap.Interface.
func (q *offerq) Pop() interface{} {
	old := q.offers
	n := len(old)
	x := old[n-1]
	q.offers = old[0 : n-1]
	return x
}
