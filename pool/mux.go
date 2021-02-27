// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/errors"
)

const offersTimeout = 10 * time.Second

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
	pools  atomic.Value
	cached bool
}

// SetCaching sets the caching behavior (true turns caching on).
func (m *Mux) SetCaching(b bool) {
	if len(m.Pools()) > 0 {
		panic("cannot turn on caching on non-empty pool")
	}
	m.cached = b
}

// SetPools sets the Mux's underlying pools.
func (m *Mux) SetPools(pools []Pool) {
	if !m.cached {
		m.pools.Store(pools)
		return
	}
	cPools := make([]Pool, len(pools))
	for i, p := range pools {
		if cp, ok := p.(*cachingPool); ok {
			cPools[i] = cp
		} else {
			cPools[i] = CachingPool(p)
		}
	}
	m.pools.Store(cPools)
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
