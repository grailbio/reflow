// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/errors"

	"github.com/grailbio/reflow"
)

var (
	small  = reflow.Resources{"mem": 10, "cpu": 1, "disk": 20}
	medium = scale(small, 2)
	large  = scale(small, 4)
)

type resourceOffer struct{ reflow.Resources }

func (*resourceOffer) ID() string                    { panic("not implemented") }
func (*resourceOffer) Pool() Pool                    { panic("not implemented") }
func (r *resourceOffer) Available() reflow.Resources { return r.Resources }
func (*resourceOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	panic("not implemented")
}

func TestPick(t *testing.T) {
	offers := []Offer{
		&resourceOffer{small},
		&resourceOffer{medium},
		&resourceOffer{large},
	}
	for _, offer := range offers {
		if got, want := pick(offers, offer.Available(), offer.Available()), offer; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		var tmp reflow.Resources
		tmp.Scale(offer.Available(), .5)
		if got, want := pick(offers, tmp, offer.Available()), offer; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		tmp.Scale(offer.Available(), 10)
		if got, want := pick(offers, offer.Available(), tmp), offers[2]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	const G = 1 << 30
	var (
		min = &resourceOffer{
			reflow.Resources{"mem": 10 * G, "cpu": 1, "disk": 20 * G}}
		max = &resourceOffer{
			reflow.Resources{"mem": 20 * G, "cpu": 1, "disk": 20 * G}}
		o1 = &resourceOffer{
			reflow.Resources{"mem": 28 * G, "cpu": 1, "disk": 20 * G}}
		o2 = &resourceOffer{
			reflow.Resources{"mem": 18 * G, "cpu": 1, "disk": 20 * G}}
		o3 = &resourceOffer{
			reflow.Resources{"mem": 19 * G, "cpu": 1, "disk": 20 * G}}
		offers1 = []Offer{o1, o2, o3}
		offers2 = []Offer{o3, o2, o1}
	)
	if got, want := pick(offers1, min.Available(), max.Available()), offers1[2]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := pick(offers2, min.Available(), max.Available()), offers2[0]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestOrder(t *testing.T) {
	for i, tc := range []struct {
		min, max reflow.Resources
		offers   []Offer
		wantByN  map[int][]Offer
	}{
		{
			large, scale(large, 1.2),
			offers(medium, scale(medium, 1.2)),
			map[int][]Offer{2: offers()},
		},
		{
			scale(medium, 0.9), scale(medium, 1.2),
			offers(large, medium, small),
			map[int][]Offer{
				2: offers(large, medium),
				1: offers(large),
			},
		},
		{
			scale(large, 0.9), scale(large, 1.2),
			offers(large, small, medium, small, scale(large, 1.1)),
			map[int][]Offer{
				2: offers(scale(large, 1.1), large),
				4: offers(scale(large, 1.1), large),
			},
		},
		{
			scale(medium, 0.9), scale(medium, 1.2),
			offers(scale(small, 1.5), scale(medium, 1.1), large, small, large, medium),
			map[int][]Offer{
				2: offers(large, large),
				4: offers(large, large, scale(medium, 1.1), medium),
			},
		},
		{
			scale(medium, 1.2), scale(large, 1.2),
			offers(large, medium, small, scale(medium, 1.1), scale(large, 1.1)),
			map[int][]Offer{
				1: offers(scale(large, 1.1)),
				3: offers(scale(large, 1.1), large),
			},
		},
		{
			medium, large,
			offers(scale(medium, 1.1), scale(medium, 1.2), scale(medium, 1.3), large, scale(large, 1.1)),
			map[int][]Offer{
				1: offers(large),
				2: offers(large, scale(large, 1.1)),
			},
		},
		{
			scale(medium, 0.9), large,
			offers(medium, medium, medium, medium, scale(large, 1.1)),
			map[int][]Offer{
				2: offers(scale(large, 1.1), medium),
				4: offers(scale(large, 1.1), medium, medium, medium),
			},
		},
	} {
		for n, want := range tc.wantByN {
			got := pickN(tc.offers, n, tc.min, tc.max)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("case %d: n %d: got %v, want %v", i, n, got, want)
			}
		}
	}
}

type testOffer struct {
	resourceOffer
	accepted int32
}

func (o *testOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	sleepRand()
	if atomic.CompareAndSwapInt32(&o.accepted, 0, 1) {
		return idAlloc(fmt.Sprintf("%s", meta.Want)), nil
	}
	return nil, errors.E(errors.NotExist)
}

type testPool struct {
	idPool
	o *testOffer
}

func (p testPool) Offers(ctx context.Context) ([]Offer, error) {
	sleepRand()
	if atomic.LoadInt32(&p.o.accepted) == 1 {
		return nil, nil
	}
	return []Offer{p.o}, nil
}

func newTestPool(id string, r reflow.Resources) testPool {
	return testPool{idPool(id), &testOffer{resourceOffer: resourceOffer{r}}}
}

func TestAllocateScaleSmall(t *testing.T) {
	assertAllocateNoErrors(t, 20)
}

func TestAllocateScaleLarge(t *testing.T) {
	assertAllocateNoErrors(t, 200)
}

const maxErrPercent = 1

func assertAllocateNoErrors(t *testing.T, n int) {
	rand.Seed(time.Now().Unix())
	var m Mux
	pools := make([]Pool, n)
	for i := 0; i < n; i++ {
		pools[i] = newTestPool(fmt.Sprintf("pool-%d", i), small)
	}
	m.SetPools(pools)

	var nerr int32
	_ = traverse.Limit(10).Each(n, func(i int) error {
		if _, err := Allocate(context.Background(), &m, reflow.Requirements{Min: small}, nil); err != nil {
			atomic.AddInt32(&nerr, 1)
		}
		return nil
	})
	if got, want := atomic.LoadInt32(&nerr), n*maxErrPercent/100; got > int32(want) {
		t.Errorf("got %v, want <= %v (%d%% of %d)", got, want, maxErrPercent, n)
	}
}

func sleepRand() {
	r := time.Duration(rand.Intn(30))
	time.Sleep(r * time.Millisecond) // Simulate a small delay
}

func scale(in reflow.Resources, factor float64) (r reflow.Resources) {
	r.Scale(in, factor)
	return
}

func offers(rs ...reflow.Resources) []Offer {
	offers := make([]Offer, len(rs))
	for i := 0; i < len(rs); i++ {
		offers[i] = &resourceOffer{rs[i]}
	}
	return offers
}
