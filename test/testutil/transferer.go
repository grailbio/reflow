// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/repository"
)

// Transferer is a simple transferer for testing; it does not apply
// any limits or other policies.
var Transferer reflow.Transferer = &transferer{}

type transferer struct{}

func (t *transferer) Transfer(ctx context.Context, dst reflow.Repository, src reflow.Repository, files ...reflow.File) error {
	for _, file := range files {
		if err := repository.Transfer(ctx, dst, src, file.ID); err != nil {
			return err
		}
	}
	return nil
}

func (t *transferer) NeedTransfer(ctx context.Context, dst reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	return repository.Missing(ctx, dst, files...)
}

// WaitTransferer is a transfer manager for testing. It lets the test
// code rendezvous with the caller. In this way, Transferer provides
// both a "mock" transfer manager implementation and also permits
// concurrency control for the tester.
type WaitTransferer struct {
	mu        sync.Mutex
	transfers map[digest.Digest]chan error
}

// Init initializes a WaitTransferer.
func (t *WaitTransferer) Init() {
	t.transfers = map[digest.Digest]chan error{}
}

func (t *WaitTransferer) transfer(dst, src reflow.Repository, files ...reflow.File) chan error {
	dw := reflow.Digester.NewWriter()
	fmt.Fprintf(dw, "%p%p", dst, src)
	sort.Slice(files, func(i, j int) bool {
		return files[i].ID.Less(files[j].ID)
	})
	for i := range files {
		fmt.Fprintf(dw, "%s%d", files[i].ID, files[i].Size)
	}
	d := dw.Digest()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.transfers[d] == nil {
		t.transfers[d] = make(chan error)
	}
	return t.transfers[d]
}

// Ok rendezvous the call named by the destination repository,
// source repository and fileset with succcess.
func (t *WaitTransferer) Ok(dst, src reflow.Repository, files ...reflow.File) {
	t.transfer(dst, src, files...) <- nil
}

// Err rendezvous the call named by the destination repository,
// source repository and fileset with failure.
func (t *WaitTransferer) Err(err error, dst, src reflow.Repository, files ...reflow.File) {
	t.transfer(dst, src, files...) <- err
}

// Transfer waits for the tester to rendezvous, returning its result.
func (t *WaitTransferer) Transfer(ctx context.Context, dst, src reflow.Repository, files ...reflow.File) error {
	select {
	case err := <-t.transfer(dst, src, files...):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NeedTransfer returns all the missing objects in the destination repository.
// The test transferer assumes they are all missing.
func (t *WaitTransferer) NeedTransfer(ctx context.Context, dst reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	return repository.Missing(ctx, dst, files...)
}
