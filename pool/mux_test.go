// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

type idAlloc string

func (a idAlloc) ID() string { return string(a) }
func (idAlloc) Put(ctx context.Context, id digest.Digest, exec reflow.ExecConfig) (reflow.Exec, error) {
	panic("not implemented")
}
func (idAlloc) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	panic("not implemented")
}
func (idAlloc) Remove(ctx context.Context, id digest.Digest) error { panic("not implemented") }
func (idAlloc) Execs(ctx context.Context) ([]reflow.Exec, error)   { panic("not implemented") }
func (idAlloc) Load(context.Context, *url.URL, reflow.Fileset) (reflow.Fileset, error) {
	panic("not implemented")
}
func (idAlloc) VerifyIntegrity(ctx context.Context, fs reflow.Fileset) error { panic("not implemented") }
func (idAlloc) Unload(context.Context, reflow.Fileset) error                 { panic("not implemented") }
func (idAlloc) Resources() reflow.Resources                                  { panic("not implemented") }
func (idAlloc) Repository() reflow.Repository                                { panic("not implemented") }
func (idAlloc) Pool() Pool                                                   { panic("not implemented") }
func (idAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	panic("not implemented")
}
func (idAlloc) Inspect(ctx context.Context) (AllocInspect, error) { panic("not implemented") }
func (idAlloc) Free(ctx context.Context) error                    { panic("not implemented") }

type inspectAlloc struct {
	Alloc
	inspect AllocInspect
}

func (a *inspectAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	a.inspect.LastKeepalive = time.Now()
	a.inspect.Expires = a.inspect.LastKeepalive.Add(interval)
	return interval, nil
}

func (a *inspectAlloc) Inspect(ctx context.Context) (AllocInspect, error) {
	return a.inspect, nil
}

func (a *inspectAlloc) Free(ctx context.Context) error {
	return nil
}

type resourceAlloc struct {
	Alloc
	r reflow.Resources
}

func (a resourceAlloc) Resources() reflow.Resources {
	return a.r
}

type idOffer string

func (o idOffer) ID() string                { return string(o) }
func (idOffer) Pool() Pool                  { panic("not implemented") }
func (idOffer) Available() reflow.Resources { panic("not implemented") }
func (idOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	panic("not implemented")
}

type idPool string

func (p idPool) ID() string                                          { return string(p) }
func (p idPool) Alloc(ctx context.Context, id string) (Alloc, error) { return idAlloc(id), nil }
func (idPool) Allocs(ctx context.Context) ([]Alloc, error)           { panic("not implemented") }
func (p idPool) Offer(ctx context.Context, id string) (Offer, error) { return idOffer(id), nil }
func (idPool) Offers(ctx context.Context) ([]Offer, error)           { panic("not implemented") }

func TestMux(t *testing.T) {
	ctx := context.Background()
	var mux Mux
	mux.SetPools([]Pool{idPool("a"), idPool("b"), idPool("c")})
	offer, err := mux.Offer(ctx, "a/blah")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := offer.ID(), "blah"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	alloc, err := mux.Alloc(ctx, "c/ok")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := alloc.ID(), "ok"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	allocFn := func(id string) error {
		_, err := mux.Alloc(ctx, id)
		return err
	}
	offerFn := func(id string) error {
		_, err := mux.Offer(ctx, id)
		return err
	}
	cases := []struct {
		f    func(string) error
		arg  string
		kind errors.Kind
	}{
		{allocFn, "blah", errors.Other},
		{offerFn, "blah", errors.Other},
		{allocFn, "foo/bar", errors.NotExist},
		{offerFn, "foo/bar", errors.NotExist},
	}
	for _, c := range cases {
		err := c.f(c.arg)
		if err == nil {
			t.Errorf("expected error for %s", c.arg)
			continue
		}
		if c.kind != errors.Other && !errors.Is(c.kind, err) {
			t.Errorf("got %v, want %v", errors.Recover(err).Kind, c.kind)
			t.Errorf("%v", errors.Recover(err).Kind == c.kind)
		}
	}
}

func createPools(n int, r reflow.Resources, name string) (pools []Pool) {
	for i := 0; i < n; i++ {
		p := NewNamedTestPool(name, r)
		pools = append(pools, p)
	}
	return
}

func TestMuxScaleWithCaching(t *testing.T) {
	nSmall, nMedium, nLarge := 20, 20, 20
	var pools []Pool
	pools = append(pools, createPools(nSmall, small, "small")...)
	pools = append(pools, createPools(nMedium, medium, "medium")...)
	pools = append(pools, createPools(nLarge, large, "large")...)
	var (
		mux Mux
		wg  sync.WaitGroup
	)
	mux.SetCaching(true)
	mux.SetPools(pools)

	nAllocs := nSmall + 2*nMedium + 4*nLarge
	if got, want := allocateMux(t, mux, nAllocs, 100*time.Millisecond, &wg), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	wg.Wait()
	verifyCallCounts(t, pools, 1, 1)
	if got, want := allocateMux(t, mux, nAllocs, 100*time.Millisecond, &wg), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	wg.Wait()
	verifyCallCounts(t, pools, 1, 2)
}

func allocateMux(t *testing.T, mux Mux, n int, allocLifetime time.Duration, wg *sync.WaitGroup) int {
	var nFails int32
	ctx := context.Background()
	wg.Add(n)
	err := traverse.Each(n, func(i int) error {
		a, err := Allocate(ctx, &mux, reflow.Requirements{Min: small}, nil)
		if err != nil {
			atomic.AddInt32(&nFails, 1)
			return nil
		}
		if got, want := a.Resources(), small; !got.Equal(want) {
			atomic.AddInt32(&nFails, 1)
		}
		// Free the alloc later
		time.AfterFunc(allocLifetime, func() {
			defer wg.Done()
			if err := a.Free(ctx); err != nil {
				t.Fatal(err)
			}
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return int(atomic.LoadInt32(&nFails))
}

func verifyCallCounts(t *testing.T, pools []Pool, wantNOffers int32, nAcceptsFactor int) {
	t.Helper()
	mOffers, mAccepts := make(map[int32]int), make(map[int32]int)
	for _, p := range pools {
		tp := p.(*TestPool)
		n := atomic.LoadInt32(&tp.nOffersCalls)
		if got, want := n, wantNOffers; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		mOffers[n] = mOffers[n] + 1
		var wantAccepts int
		switch tp.Name() {
		case "small":
			wantAccepts = 1
		case "medium":
			wantAccepts = 2
		case "large":
			wantAccepts = 4
		}
		n = atomic.LoadInt32(&tp.nAcceptCalls)
		if got, want := int(n), wantAccepts*nAcceptsFactor; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		mAccepts[n] = mAccepts[n] + 1
	}
	print(t, "mOffers", mOffers)
	print(t, "mAccepts", mAccepts)
}

func print(t *testing.T, pref string, m map[int32]int) {
	keys, i := make([]int, len(m)), 0
	for k := range m {
		keys[i] = int(k)
		i++
	}
	sort.Ints(keys)
	for _, k := range keys {
		t.Logf("%s[%d]: %d", pref, k, m[int32(k)])
	}
}
