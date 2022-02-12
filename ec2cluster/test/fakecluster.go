package test

import (
	"context"
	"flag"

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

// GetName implements runner.Cluster
func (c *Cluster) GetName() string { return "fakecluster" }

// Flags implements infra.Provider
func (r *Cluster) Flags(flags *flag.FlagSet) {
	var ignoredString string
	var ignoredBool bool
	flags.StringVar(&ignoredString, "instancetypes", "", "(IGNORED) instance types")
	flags.BoolVar(&ignoredBool, "usespot", false, "(IGNORED) use spot or ondemand")
}
