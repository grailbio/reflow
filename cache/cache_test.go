// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/testutil"
)

type assoc map[digest.Digest]digest.Digest

func (a assoc) Map(k, v digest.Digest) error {
	a[k] = v
	return nil
}

func (a assoc) Unmap(k digest.Digest) error {
	delete(a, k)
	return nil
}

func (a assoc) Lookup(k digest.Digest) (digest.Digest, error) {
	v, ok := a[k]
	if !ok {
		return digest.Digest{}, errors.E("lookup", k, errors.NotExist)
	}
	return v, nil
}

type transferer struct{}

func (transferer) Transfer(ctx context.Context, dst, src reflow.Repository, files ...reflow.File) error {
	for _, f := range files {
		if err := repository.Transfer(ctx, dst, src, f.ID); err != nil {
			return err
		}
	}
	return nil
}

func (transferer) NeedTransfer(ctx context.Context, dst reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	return repository.Missing(ctx, dst, files...)
}

func TestCache(t *testing.T) {
	ctx := context.Background()
	cache := &Cache{
		Repo:       testutil.NewInmemory(),
		Assoc:      make(assoc),
		Transferer: transferer{},
	}
	key := reflow.Digester.FromString
	if _, err := cache.Lookup(ctx, key("hello")); err == nil || !errors.Is(errors.NotExist, err) {
		t.Errorf("expected notexist error, got %v", err)
	}

	local := testutil.NewInmemory()
	files := make([]reflow.File, 10)
	for i := range files {
		contents := fmt.Sprintf("file%d", i)
		id, err := local.Put(ctx, bytes.NewReader([]byte(contents)))
		if err != nil {
			t.Fatal(err)
		}
		files[i] = reflow.File{ID: id, Size: int64(len(contents))}
	}

	// Try to write a value for which there are missing objects.
	v := reflow.Fileset{
		Map: map[string]reflow.File{
			"file0":    files[0],
			"notexist": reflow.File{ID: key("nope"), Size: 10},
		},
	}
	err := cache.Write(ctx, key("key1"), v, local)
	if err == nil || !errors.Is(errors.NotExist, err) {
		t.Errorf("expected nontexist error, got %v", err)
	}

	v = reflow.Fileset{
		Map: map[string]reflow.File{
			"file1":     files[1],
			"dir/file2": files[2],
		},
	}
	if err := cache.Write(ctx, key("key2"), v, local); err != nil {
		t.Errorf("unexpected error %v", err)
	}
	v2, err := cache.Lookup(ctx, key("key2"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := v, v2; got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got, want)
	}
}
