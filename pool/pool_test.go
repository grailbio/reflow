// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"testing"

	"github.com/grailbio/reflow"
)

type resourceOffer struct{ reflow.Resources }

func (*resourceOffer) ID() string                    { panic("not implemented") }
func (*resourceOffer) Pool() Pool                    { panic("not implemented") }
func (r *resourceOffer) Available() reflow.Resources { return r.Resources }
func (*resourceOffer) Accept(ctx context.Context, meta AllocMeta) (Alloc, error) {
	panic("not implemented")
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
