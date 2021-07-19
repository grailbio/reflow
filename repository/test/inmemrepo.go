package test

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/test/testutil"
)

func init() {
	infra.Register("inmemrepo", new(InMemoryRepository))
}

// Repository is a fake repos infra provider and should be used only in tests.
type InMemoryRepository struct {
	*testutil.InmemoryRepository
	repository.RepoFlagsTrait
}

var (
	registerOnce sync.Once
	mu           sync.Mutex
	repos        map[string]*InMemoryRepository
)

// Init implements infra.Provider
func (r *InMemoryRepository) Init() error {
	registerOnce.Do(func() {
		repos = make(map[string]*InMemoryRepository)
		repository.RegisterScheme("inmemory", func(u *url.URL) (reflow.Repository, error) {
			if repo, ok := repos[u.Host]; ok {
				return repo, nil
			} else {
				panic(fmt.Sprintf("trying to get in-memory repo %s which we never registered", u.Host))
			}
		})
	})
	mu.Lock()
	defer mu.Unlock()
	r.InmemoryRepository = testutil.NewInmemoryRepository(r.RepoFlagsTrait.BucketName)
	repos[r.InmemoryRepository.URL().Host] = r
	return nil
}

func GetInMemoryRepo(bucketName string) *InMemoryRepository {
	mu.Lock()
	defer mu.Unlock()
	return repos[bucketName]
}
