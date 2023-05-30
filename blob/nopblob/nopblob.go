package nopblob

import (
	"context"
	"io"
	"strings"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
)

// Store implements blob.Store.
type Store struct {
	buckets map[string]*Bucket
}

func NewStore() *Store {
	return &Store{
		buckets: make(map[string]*Bucket),
	}
}

func (s *Store) Bucket(ctx context.Context, bucket string) (blob.Bucket, error) {
	return &Bucket{bucket: bucket}, nil
}

// Bucket implements blob.Bucket.
type Bucket struct {
	bucket string
}

func (b *Bucket) File(ctx context.Context, key string) (reflow.File, error) {
	return reflow.File{}, nil
}

func (b *Bucket) Scan(prefix string, withMetadata bool) blob.Scanner {
	return &nopScanner{}
}

func (b *Bucket) Download(ctx context.Context, key, etag string, size int64, w io.WriterAt) (int64, error) {
	return 0, nil
}

func (b *Bucket) Get(ctx context.Context, key, etag string) (io.ReadCloser, reflow.File, error) {
	return io.NopCloser(strings.NewReader("")), reflow.File{}, nil
}

func (b *Bucket) Put(ctx context.Context, key string, size int64, body io.Reader, contentHash string) error {
	return nil
}

func (b *Bucket) Snapshot(ctx context.Context, prefix string) (reflow.Fileset, error) {
	return reflow.Fileset{}, nil
}

func (b *Bucket) Copy(ctx context.Context, src, dst, contentHash string) error {
	return nil
}

func (b *Bucket) CopyFrom(ctx context.Context, srcBucket blob.Bucket, src, dst string) error {
	return nil
}

func (b *Bucket) Delete(ctx context.Context, keys ...string) error {
	return nil
}

func (b *Bucket) Location() string {
	return ""
}

// nopScanner implements blob.Scanner.
type nopScanner struct{}

func (s *nopScanner) Scan(ctx context.Context) bool {
	return false
}

func (s *nopScanner) Err() error {
	return nil
}

func (s *nopScanner) File() reflow.File {
	return reflow.File{}
}

func (s *nopScanner) Key() string {
	return ""
}
