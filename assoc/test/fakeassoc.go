package test

import (
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/assoc"
	infra2 "github.com/grailbio/reflow/infra"
)

func init() {
	infra.Register("fakeassoc", new(Assoc))
}

// Assoc is a fake assoc infra provider and should be used only in tests.
type Assoc struct {
	assoc.Assoc
	infra2.TableNameFlagsTrait
}
