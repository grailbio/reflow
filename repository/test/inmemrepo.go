package test

import (
	"context"
	"sync"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/testblob"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/repository/blobrepo"
)

func init() {
	infra.Register("inmemrepo", new(InMemoryBlobRepository))
}

const scheme = "inmemory"

// InMemoryBlobRepository is a blob backed repository which stores values in memory
type InMemoryBlobRepository struct {
	*blobrepo.Repository
	infra2.BucketNameFlagsTrait
}

var (
	store        blob.Store
	registerOnce sync.Once
)

// Init implements infra.Provider
func (r *InMemoryBlobRepository) Init() error {
	registerOnce.Do(func() {
		store = testblob.New(scheme)
		blobrepo.Register(scheme, store)
	})
	bucket, err := store.Bucket(context.Background(), r.BucketNameFlagsTrait.BucketName)
	if err != nil {
		return err
	}
	r.Repository = &blobrepo.Repository{Bucket: bucket}
	return nil
}

func GetFiles(ctx context.Context, bucketName string) ([]reflow.File, error) {
	bucket, err := store.Bucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	const withMetadata = true
	scan := bucket.Scan("", withMetadata)
	var files []reflow.File
	for scan.Scan(ctx) {
		_, file := scan.Key(), scan.File()
		files = append(files, file)
	}
	return files, nil
}
