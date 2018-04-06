// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package repository_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/test/testutil"
)

func file(contents string) reflow.File {
	return reflow.File{
		ID:   reflow.Digester.FromString(contents),
		Size: int64(len(contents)),
	}
}

func readcloser(contents string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(contents)))
}

func TestManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Set up two concurrent transfers. One file is overlapping and should be
	// single-flighted.
	var (
		r1 = testutil.NewWaitRepository("r1")
		r2 = testutil.NewWaitRepository("r2")
		c1 = make(chan error)
		c2 = make(chan error)

		x = file("x")
		y = file("y")
		z = file("z")
	)
	for _, transferErr := range []error{nil, errors.New("unexpected")} {
		var status status.Status
		m := &repository.Manager{
			PendingTransfers: repository.NewLimits(10),
			Stat:             repository.NewLimits(10),
			Status:           status.Group("transfer"),
		}
		go func() {
			c1 <- m.Transfer(ctx, r2, r1, x, y)
		}()
		go func() {
			c2 <- m.Transfer(ctx, r2, r1, x, z)
		}()
		notexist := testutil.RepositoryCall{ReplyErr: errors.E(errors.NotExist)}
		r2.Call(testutil.RepositoryStat, x.ID) <- notexist
		r2.Call(testutil.RepositoryStat, x.ID) <- notexist
		r2.Call(testutil.RepositoryStat, y.ID) <- notexist
		r2.Call(testutil.RepositoryStat, z.ID) <- notexist

		// At this point, we expect each file to be transferred exactly once, concurrently.
		r1.Call(testutil.RepositoryGet, x.ID) <- testutil.RepositoryCall{ReplyReadCloser: readcloser("x")}
		r1.Call(testutil.RepositoryGet, y.ID) <- testutil.RepositoryCall{ReplyReadCloser: readcloser("y")}
		r1.Call(testutil.RepositoryGet, z.ID) <- testutil.RepositoryCall{ReplyReadCloser: readcloser("z")}
		if got, want := m.Status.Value().Status, "done: 0 0B, transferring: 3 3B, waiting: 0 0B"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}

		r2.Call(testutil.RepositoryPut, y.ID) <- testutil.RepositoryCall{}
		r2.Call(testutil.RepositoryPut, z.ID) <- testutil.RepositoryCall{}
		// This has to be last to resolve the race of the remainder of the files
		// begin cancelled.
		r2.Call(testutil.RepositoryPut, x.ID) <- testutil.RepositoryCall{ReplyErr: transferErr}

		if transferErr != nil {
			transferErr = errors.E("transfer", x.ID, transferErr)
		}
		if got, want := <-c1, transferErr; !errors.Match(want, got) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := <-c2, transferErr; !errors.Match(want, got) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := m.Status.Value().Status, "done: 3 3B, transferring: 0 0B, waiting: 0 0B"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
