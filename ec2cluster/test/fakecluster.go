package test

import (
	"context"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/pool"
)

func init() {
	infra.Register("fakecluster", new(Cluster))
}

// Cluster is a fake cluster infra provider and should be used only in tests.
type Cluster struct {
	*ec2cluster.Cluster
}

// Init implements infra.Provider.
func (c *Cluster) Init() error {
	c.Cluster = &ec2cluster.Cluster{}
	return nil
}

func (c *Cluster) CanAllocate(r reflow.Resources) (bool, error) {
	return true, nil
}

// Allocate is a no op.
func (c *Cluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error) {
	return nil, nil
}

func (c *Cluster) Shutdown() error { return nil }
