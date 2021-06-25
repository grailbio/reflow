// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

type TestPool struct {
	ResourcePool
	nOffersCalls int32
	nAcceptCalls int32
	name         string
}

type poolFreeingAlloc struct {
	Alloc
	p *TestPool
}

func (a *poolFreeingAlloc) Free(ctx context.Context) error {
	return a.p.ResourcePool.Free(a)
}

func NewTestPool(r reflow.Resources) *TestPool {
	return NewNamedTestPool("testpool", r)
}

func NewNamedTestPool(name string, r reflow.Resources) *TestPool {
	p := &TestPool{name: name}
	p.ResourcePool = NewResourcePool(p, nil)
	p.Init(r, nil)
	return p
}

func (p *TestPool) Name() string {
	return p.name
}

func (p *TestPool) New(ctx context.Context, id string, meta AllocMeta, ka time.Duration, existing []Alloc) (Alloc, error) {
	a := &poolFreeingAlloc{&inspectAlloc{
		Alloc: resourceAlloc{idAlloc(id), meta.Want},
		inspect: AllocInspect{
			Created: time.Now(),
			Expires: time.Now().Add(ka),
		},
	}, p}
	return a, nil
}

func (p *TestPool) Kill(a Alloc) error {
	return nil
}

func (p *TestPool) Offers(ctx context.Context) (offers []Offer, err error) {
	atomic.AddInt32(&p.nOffersCalls, 1)
	off, err := p.ResourcePool.Offers(ctx)
	offers = make([]Offer, len(off))
	for i, o := range off {
		offers[i] = &acceptCountingOffer{o, &p.nAcceptCalls}
	}
	return offers, err
}

func equal(x, y []Offer) bool {
	if len(x) != len(y) {
		return false
	}
	for i, xo := range x {
		if xo.ID() != y[i].ID() {
			return false
		}
	}
	return true
}

func assertCachingPool(t *testing.T, p *TestPool, cp Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	want, _ := p.Offers(ctx)
	atomic.StoreInt32(&p.nOffersCalls, 0)
	err := traverse.Each(10, func(i int) error {
		got, err := cp.Offers(ctx)
		if err != nil {
			return err
		}
		if !equal(got, want) {
			return fmt.Errorf("got %v, want %v", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := atomic.LoadInt32(&p.nOffersCalls), int32(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCachingPoolEmpty(t *testing.T) {
	p := NewTestPool(nil)
	cp := CachingPool(p)
	assertCachingPool(t, p, cp)
}

func TestCachingPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NewTestPool(large)
	cp := CachingPool(p)
	assertCachingPool(t, p, cp)
	offers, err := cp.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(offers), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	a, err := offers[0].Accept(ctx, AllocMeta{Want: large})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := a.Resources(), large; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	_, err = offers[0].Accept(ctx, AllocMeta{Want: large})
	if err == nil || !errors.Is(errors.NotExist, err) {
		t.Errorf("must get NotExist error")
	}

	if got, want := atomic.LoadInt32(&p.nOffersCalls), int32(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCachingPoolInvalidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NewTestPool(large)
	cp := CachingPool(p)

	cached, err := cp.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := atomic.LoadInt32(&p.nOffersCalls), int32(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(cached), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Simulate some other client snatchng part of the offer
	offers, err := p.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := atomic.LoadInt32(&p.nOffersCalls), int32(2); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(offers), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	a, err := offers[0].Accept(ctx, AllocMeta{Want: small})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := a.Resources(), small; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Now let's try to accept the cached offer fully.
	if _, err = cached[0].Accept(ctx, AllocMeta{Want: large}); err == nil || !errors.Is(errors.NotExist, err) {
		t.Errorf("must get NotExist error")
	}

	// Now call the caching pool again (it should refresh the offers)
	cached, err = cp.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := atomic.LoadInt32(&p.nOffersCalls), int32(3); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(cached), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cached[0].Available(), scale(small, 3); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

type acceptCountingOffer struct {
	Offer
	nAcceptCalls *int32
}

func (o *acceptCountingOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	atomic.AddInt32(o.nAcceptCalls, 1)
	return o.Offer.Accept(ctx, meta)
}

type acceptOffer struct {
	mu           sync.Mutex
	id           string
	r            reflow.Resources
	nAcceptCalls int32
}

func (o *acceptOffer) ID() string { return o.id }
func (*acceptOffer) Pool() Pool   { panic("not implemented") }

func (o *acceptOffer) Available() reflow.Resources {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.r
}

func (o *acceptOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	atomic.AddInt32(&o.nAcceptCalls, 1)
	sleepRand()
	if !o.r.Available(meta.Want) {
		return nil, errors.E(errors.NotExist)
	}
	o.r.Sub(o.r, meta.Want)
	return &inspectAlloc{Alloc: resourceAlloc{idAlloc(newID()), meta.Want}, inspect: AllocInspect{Created: time.Now()}}, nil
}

func TestTrackedOffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ao := &acceptOffer{id: newID(), r: medium}
	o := newTrackedOffer(ao)
	if got, want := o.Available(), medium; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if o.Outdated() {
		t.Errorf("must not be outdated")
	}
	allocs := verifyAllocations(t, o, 10, small, 2, nil)
	if got, want := len(allocs), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	_ = allocs[0].Free(ctx)
	if got, want := o.Available(), small; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	_ = allocs[1].Free(ctx)
	if got, want := o.Available(), medium; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func verifyAllocations(t *testing.T, o *trackedOffer, nAllocs int, r reflow.Resources, wantAccepts int, wantR reflow.Resources) (allocs []Alloc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	allocs = make([]Alloc, nAllocs)
	err := traverse.Each(nAllocs, func(i int) (err error) {
		allocs[i], err = o.Accept(ctx, AllocMeta{Want: r})
		if errors.Is(errors.NotExist, err) {
			err = nil
		}
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Outdated() {
		t.Errorf("must not be outdated")
	}
	ao := o.Offer.(*acceptOffer)
	if got, want := int(atomic.LoadInt32(&ao.nAcceptCalls)), wantAccepts; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	allocs = nonNil(allocs)
	for _, a := range allocs {
		if got, want := a.Resources(), r; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	if got, want := o.Available(), wantR; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	return
}

func TestTrackedOfferOutdated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ao := &acceptOffer{id: newID(), r: medium}
	o := newTrackedOffer(ao)

	// Simulate some other client snatching the underlying offer.
	a, err := ao.Accept(ctx, AllocMeta{Want: medium})
	if err != nil {
		t.Fatal(err)
	}
	// Reset after call by other client.
	atomic.StoreInt32(&ao.nAcceptCalls, int32(0))

	if got, want := a.Resources(), medium; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if o.Outdated() {
		t.Errorf("must not be outdated")
	}

	allocs := make([]Alloc, 5)
	err = traverse.Each(5, func(i int) (err error) {
		allocs[i], err = o.Accept(ctx, AllocMeta{Want: medium})
		if errors.Is(errors.NotExist, err) {
			err = nil
		}
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	if !o.Outdated() {
		t.Errorf("must be outdated")
	}
	// We expect only one call to the underlying offer despite 5 calls to the tracked offer.
	if got, want := atomic.LoadInt32(&ao.nAcceptCalls), int32(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	n := 0
	for _, a := range allocs {
		if a == nil {
			continue
		}
		n++
		if got, want := a.Resources(), small; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func nonNil(allocs []Alloc) []Alloc {
	var nonNil []Alloc
	for _, a := range allocs {
		if a == nil {
			continue
		}
		nonNil = append(nonNil, a)
	}
	return nonNil
}
