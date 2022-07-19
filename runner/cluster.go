// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
)

// Cluster is a kind of pool.Pool that also allows the user to
// directly reserve an alloc. This way, the cluster can be responsive
// to demand.
type Cluster interface {
	pool.Pool

	// Allocate reserves an alloc of at least min, and at most max resources.
	// The cluster may scale elastically in order to meet this demand.
	// Labels are passed down to the underlying pool.
	Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error)

	// CanAllocate returns whether this cluster can allocate the given amount of resources.
	CanAllocate(reflow.Resources) (bool, error)

	// MaxAlloc returns the max resources which can be obtained in a single alloc from this cluster.
	MaxAlloc() reflow.Resources

	// GetName returns the name of the cluster
	GetName() string
}

// TracingCluster is a cluster that traces the actions of an underlying
// cluster manager.
type TracingCluster struct {
	Cluster
}
