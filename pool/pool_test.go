// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
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
func (idAlloc) Unload(context.Context, reflow.Fileset) error { panic("not implemented") }
func (idAlloc) Resources() reflow.Resources                  { panic("not implemented") }
func (idAlloc) Repository() reflow.Repository                { panic("not implemented") }
func (idAlloc) Pool() Pool                                   { panic("not implemented") }
func (idAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	panic("not implemented")
}
func (idAlloc) Inspect(ctx context.Context) (AllocInspect, error) { panic("not implemented") }
func (idAlloc) Free(ctx context.Context) error                    { panic("not implemented") }

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

type resourceOffer struct{ reflow.Resources }

func (*resourceOffer) ID() string                    { panic("not implemented") }
func (*resourceOffer) Pool() Pool                    { panic("not implemented") }
func (r *resourceOffer) Available() reflow.Resources { return r.Resources }
func (*resourceOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	panic("not implemented")
}

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

func TestPick(t *testing.T) {
	small := reflow.Resources{"mem": 10, "cpu": 1, "disk": 20}
	var medium, large reflow.Resources
	medium.Scale(small, 2)
	large.Scale(medium, 2)
	offers := []Offer{
		&resourceOffer{small},
		&resourceOffer{medium},
		&resourceOffer{large},
	}
	for _, offer := range offers {
		if got, want := Pick(offers, offer.Available(), offer.Available()), offer; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		var tmp reflow.Resources
		tmp.Scale(offer.Available(), .5)
		if got, want := Pick(offers, tmp, offer.Available()), offer; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		tmp.Scale(offer.Available(), 10)
		if got, want := Pick(offers, offer.Available(), tmp), offers[2]; got != want {
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
	if got, want := Pick(offers1, min.Available(), max.Available()), offers1[2]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := Pick(offers2, min.Available(), max.Available()), offers2[0]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
