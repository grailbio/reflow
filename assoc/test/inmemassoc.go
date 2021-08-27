package test

import (
	"sync"

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

var (
	makeOnce sync.Once
	mu       sync.Mutex
	assocs   map[string]*testutil.InmemoryAssoc
)

// Init implements infra.Provider
func (a *InMemoryAssoc) Init() error {
	makeOnce.Do(func() {
		assocs = make(map[string]*testutil.InmemoryAssoc)
	})
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
