package pool

import (
	"context"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

type idAlloc string

func (a idAlloc) ID() string { return string(a) }
func (a idAlloc) Put(ctx context.Context, id digest.Digest, exec reflow.ExecConfig) (reflow.Exec, error) {
	panic("not implemented")
}
func (a idAlloc) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	panic("not implemented")
}
func (a idAlloc) Remove(ctx context.Context, id digest.Digest) error { panic("not implemented") }
func (a idAlloc) Execs(ctx context.Context) ([]reflow.Exec, error)   { panic("not implemented") }
func (a idAlloc) Resources() reflow.Resources                        { panic("not implemented") }
func (a idAlloc) Repository() reflow.Repository                      { panic("not implemented") }
func (a idAlloc) Pool() Pool                                         { panic("not implemented") }
func (a idAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	panic("not implemented")
}
func (a idAlloc) Inspect(ctx context.Context) (AllocInspect, error) { panic("not implemented") }
func (a idAlloc) Free(ctx context.Context) error                    { panic("not implemented") }

type idOffer string

func (o idOffer) ID() string                  { return string(o) }
func (o idOffer) Pool() Pool                  { panic("not implemented") }
func (o idOffer) Available() reflow.Resources { panic("not implemented") }
func (o idOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	panic("not implemented")
}

type idPool string

func (p idPool) ID() string                                          { return string(p) }
func (p idPool) Alloc(ctx context.Context, id string) (Alloc, error) { return idAlloc(id), nil }
func (p idPool) Allocs(ctx context.Context) ([]Alloc, error)         { panic("not implemented") }
func (p idPool) Offer(ctx context.Context, id string) (Offer, error) { return idOffer(id), nil }
func (p idPool) Offers(ctx context.Context) ([]Offer, error)         { panic("not implemented") }

type resourceOffer reflow.Resources

func (resourceOffer) ID() string                    { panic("not implemented") }
func (resourceOffer) Pool() Pool                    { panic("not implemented") }
func (r resourceOffer) Available() reflow.Resources { return reflow.Resources(r) }
func (resourceOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
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
		if !errors.Match(c.kind, err) {
			t.Errorf("got %v, want %v", errors.Recover(err).Kind, c.kind)
			t.Errorf("%v", errors.Recover(err).Kind == c.kind)
		}
	}
}

func TestPick(t *testing.T) {
	var (
		small  = reflow.Resources{Memory: 10, CPU: 1, Disk: 20}
		medium = small.Scale(2)
		large  = medium.Scale(2)
		offers = []Offer{
			resourceOffer(small),
			resourceOffer(medium),
			resourceOffer(large),
		}
	)
	for _, offer := range offers {
		if got, want := Pick(offers, offer.Available(), offer.Available()), offer; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := Pick(offers, offer.Available().Scale(.5), offer.Available()), offer; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := Pick(offers, offer.Available(), offer.Available().Scale(10)), offers[2]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	const G = 1 << 30
	var (
		min = resourceOffer(
			reflow.Resources{Memory: 10 * G, CPU: 1, Disk: 20 * G})
		max = resourceOffer(
			reflow.Resources{Memory: 20 * G, CPU: 1, Disk: 20 * G})
		o1 = resourceOffer(
			reflow.Resources{Memory: 28 * G, CPU: 1, Disk: 20 * G})
		o2 = resourceOffer(
			reflow.Resources{Memory: 18 * G, CPU: 1, Disk: 20 * G})
		o3 = resourceOffer(
			reflow.Resources{Memory: 19 * G, CPU: 1, Disk: 20 * G})
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
