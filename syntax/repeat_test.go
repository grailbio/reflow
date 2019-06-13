// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

const (
	numUniqueValues = 20
	numFlows        = 500
)

func valFlow(v string) *flow.Flow {
	return &flow.Flow{Op: flow.Val, FlowDigest: values.Digest(v, types.String), Value: v}
}

func TestFlowMap(t *testing.T) {
	wants := make([]*flow.Flow, numUniqueValues)
	flows := make([]*flow.Flow, numFlows)
	for i := 0; i < numUniqueValues; i++ {
		f := valFlow(fmt.Sprintf("value_%d", i))
		wants[i] = f
		flows[i] = f
	}
	// fill up the rest randomly sampled from the unique set
	for i := numUniqueValues; i < numFlows; i++ {
		flows[i] = valFlow(wants[rand.Intn(numUniqueValues)].Value.(string))
	}
	s := newFlowMap()
	_ = traverse.Limit(10).Each(numFlows, func(i int) error {
		s.Put(flows[i], flows[i])
		return nil
	})
	gots := s.Values()
	if got, want := len(gots), numUniqueValues; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	sort.Slice(gots, func(i, j int) bool { return gots[i].Value.(string) < gots[j].Value.(string) })
	sort.Slice(wants, func(i, j int) bool { return wants[i].Value.(string) < wants[j].Value.(string) })
	for i := 0; i < numUniqueValues; i++ {
		if got, want := gots[i].Digest(), wants[i].Digest(); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
