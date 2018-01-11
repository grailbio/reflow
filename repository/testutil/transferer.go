// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/repository"
)

// Transferer is a simple transferer that does not apply any limits or
// other policies.
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
