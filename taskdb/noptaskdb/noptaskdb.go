package noptaskdb

import (
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/test/testutil"
)

func init() {
	infra.Register("noptaskdb", new(NopTaskDB))
}

// NopTaskDB is a nop-TaskDB infra provider
type NopTaskDB struct {
	taskdb.TaskDB
}

// Init implements infra.Provider
func (t *NopTaskDB) Init() error {
	t.TaskDB = testutil.NewNopTaskDB(nil)
	return nil
}
