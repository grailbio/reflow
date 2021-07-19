package test

import (
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/test/testutil"
)

func init() {
	infra.Register("inmemassoc", new(InMemoryAssoc))
}

// InMemoryAssoc is an in-memory assoc infra provider
type InMemoryAssoc struct {
	*testutil.InmemoryAssoc
	assoc.AssocFlagsTrait
}

// Init implements infra.Provider
func (r *InMemoryAssoc) Init() error {
	r.InmemoryAssoc = testutil.NewInmemoryAssoc()
	return nil
}
