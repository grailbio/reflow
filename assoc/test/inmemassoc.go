package test

import (
	"sync"

	"github.com/grailbio/infra"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/test/testutil"
)

func init() {
	infra.Register("inmemassoc", new(InMemoryAssoc))
}

// InMemoryAssoc is an in-memory assoc infra provider
type InMemoryAssoc struct {
	*testutil.InmemoryAssoc
	infra2.TableNameFlagsTrait
}

var (
	mu     sync.Mutex
	assocs = make(map[string]*testutil.InmemoryAssoc)
)

// Init implements infra.Provider
func (a *InMemoryAssoc) Init() error {
	mu.Lock()
	defer mu.Unlock()
	if ea, ok := assocs[a.TableName]; ok {
		a.InmemoryAssoc = ea
		return nil
	}
	a.InmemoryAssoc = testutil.NewInmemoryAssoc()
	assocs[a.TableName] = a.InmemoryAssoc
	return nil
}

func GetInMemoryAssoc(tableName string) *testutil.InmemoryAssoc {
	mu.Lock()
	defer mu.Unlock()
	return assocs[tableName]
}
