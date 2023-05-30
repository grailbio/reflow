package noprepo

import (
	"context"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob/nopblob"
	"github.com/grailbio/reflow/liveset"
	"github.com/grailbio/reflow/repository/blobrepo"
)

func init() {
	infra.Register("noprepo", new(Repository))
}

// Repository is a no-op Repository provider.
type Repository struct {
	reflow.Repository
}

// Init implements infra.Provider.
func (r *Repository) Init() error {
	r.Repository = InitRepo()
	return nil
}

func InitRepo() reflow.Repository {
	blob := nopblob.NewStore()
	blobrepo.Register("nop", blob)
	return &Repository{}
}

func (r *Repository) Collect(context.Context, liveset.Liveset) error {
	return nil
}

func (r *Repository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryrun bool) error {
	return nil
}

func (r *Repository) Stat(context.Context, digest.Digest) (reflow.File, error) {
	return reflow.File{}, nil
}

func (r *Repository) Get(context.Context, digest.Digest) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (r *Repository) Put(context.Context, io.Reader) (digest.Digest, error) {
	return digest.Digest{}, nil
}

func (r *Repository) WriteTo(context.Context, digest.Digest, *url.URL) error {
	return nil
}

func (r *Repository) ReadFrom(context.Context, digest.Digest, *url.URL) error {
	return nil
}

func (r *Repository) URL() *url.URL {
	return &url.URL{Scheme: "nop"}
}
