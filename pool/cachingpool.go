// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/reflow/errors"

	"github.com/grailbio/reflow"
)

const emptyOffersTtl = 60 * time.Second

type cachingPool struct {
	Pool
	mu         sync.Mutex
	offers     []Offer
	expiration time.Time
}

// CachingPool returns a Pool which caches the offers from the given pool.
// If the underlying pool returned no offers, then the cached result expires after emptyOffersTtl.
// Cached offers will expire if all them have outdated (ie, a discrepancy is detected between
// the cached offer and the underlying one)
func CachingPool(p Pool) Pool {
	return &cachingPool{Pool: p}
}

// Offers returns the set of current (possibly cached, but valid) Offers from the underlying Pool.
func (p *cachingPool) Offers(ctx context.Context) (offers []Offer, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.offers) == 0 && time.Now().Before(p.expiration) {
		return
	}
	var valid []Offer
	for _, o := range p.offers {
		if !o.(*trackedOffer).Outdated() {
			valid = append(valid, o)
		}

	}
	if len(valid) == 0 {
		offers, err = p.refresh(ctx)
		return
	}
	for _, o := range valid {
		// Skip empty offers (reduces overhead when used with pool.Allocate)
		if !o.Available().Equal(nil) {
			offers = append(offers, o)
		}
	}
	return
}

// refresh refreshes this pool's cached offers (by delegating to the underlying pool)
func (p *cachingPool) refresh(ctx context.Context) (offers []Offer, err error) {
	if offers, err = p.Pool.Offers(ctx); err != nil {
		return
	}
	p.expiration = time.Now().Add(emptyOffersTtl)
	p.offers = make([]Offer, len(offers))
	for i, o := range offers {
		p.offers[i] = newTrackedOffer(o)
	}
	offers = p.offers
	return
}

// trackedOffer tracks resource requests already accepted on this offer and
// reduces the amount of resources available.
// trackedOffer will become outdated if there's any indication that the underlying offer has drifted.
// Once outdated, this offer will never be accepted.
type trackedOffer struct {
	Offer
	mu        sync.RWMutex
	available reflow.Resources
	outdated  bool
}

// newTrackedOffer returns a tracked offer which tracks the given offer.
func newTrackedOffer(o Offer) *trackedOffer {
	return &trackedOffer{Offer: o, available: o.Available()}
}

// Outdated returns whether the offer is outdated
func (o *trackedOffer) Outdated() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.outdated
}

// Available returns the amount of currently available resources that can be accepted.
func (o *trackedOffer) Available() reflow.Resources {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.available
}

// Accept accepts this Offer with the given Alloc metadata.
func (o *trackedOffer) Accept(ctx context.Context, meta AllocMeta) (a Alloc, err error) {
	o.mu.RLock()
	if !o.viable(meta.Want) {
		o.mu.RUnlock()
		err = errors.E(fmt.Sprintf("offer %s", o.ID()), errors.NotExist, errOfferExpired)
		return
	}
	o.mu.RUnlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	// check again after acquiring write lock.
	if !o.viable(meta.Want) {
		err = errors.E(fmt.Sprintf("offer %s", o.ID()), errors.NotExist, errOfferExpired)
		return
	}
	a, err = o.Offer.Accept(ctx, meta)
	if err == nil {
		o.available.Sub(o.available, a.Resources())
	} else {
		// Other than transient errors, this indicates that the offer has drifted and hence outdated.
		o.outdated = true
	}
	return
}

// viable returns if the offer is viable for the given resources
// viable assumes that this offer's mutex is locked.
func (o *trackedOffer) viable(r reflow.Resources) bool {
	return o.available.Available(r) && !o.outdated
}
