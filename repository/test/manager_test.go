// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/filerepo"
	"github.com/grailbio/testutil"
)

func mustInstall(t *testing.T, r reflow.Repository, contents string) reflow.File {
	id, err := r.Put(context.Background(), bytes.NewReader([]byte(contents)))
	if err != nil {
		t.Fatal(err)
	}
	return reflow.File{ID: id, Size: int64(len(contents))}
}

func mustRepositories(t *testing.T) (r1, r2 *filerepo.Repository, cleanup func()) {
	dir1, cleanup1 := testutil.TempDir(t, "", "manager-")
	dir2, cleanup2 := testutil.TempDir(t, "", "manager-")
	return &filerepo.Repository{Root: dir1}, &filerepo.Repository{Root: dir2}, func() {
		cleanup1()
		cleanup2()
	}
}

func TestManager(t *testing.T) {
	ctx := context.Background()
	r1, r2, cleanup := mustRepositories(t)
	defer cleanup()
	file1, file2 := mustInstall(t, r1, "test1"), mustInstall(t, r2, "test2")
	var m repository.Manager
	m.PendingTransfers = repository.NewLimits(1)
	m.Stat = repository.NewLimits(1)
	if err := m.Transfer(ctx, r2, r1, file1, file2); err != nil {
		t.Fatal(err)
	}
	if ok, err := r2.Contains(file1.ID); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("file missing")
	}
}

func TestLimits(t *testing.T) {
	ctx := context.Background()
	r1, r2, cleanup := mustRepositories(t)
	defer cleanup()
	var m repository.Manager
	m.PendingTransfers = repository.NewLimits(1)
	m.Stat = repository.NewLimits(1)

	file1, file2 := mustInstall(t, r1, "test1"), mustInstall(t, r1, "test2")
	t2 := &testRepository{Repository: r2}
	t2.Check = func(r *reporter, calls []*call) {
		var nput int
		for _, c := range calls {
			if c.Type == callPut {
				nput++
			}
		}
		if nput > 1 {
			r.Errorf("expected at most 1 concurrent put; got %d", nput)
		}
	}

	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			if err := m.Transfer(ctx, t2, r1, file1, file2); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	t2.Report(t)
}
