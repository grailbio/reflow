package test

import (
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
)

func init() {
	infra.Register("fakes3", new(Repository))
}

// Repository is a fake repos infra provider and should be used only in tests.
type Repository struct {
	reflow.Repository
}
