// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package repository

import (
	"context"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"golang.org/x/sync/errgroup"
)

// Missing returns the files in files that are missing from
// repository r. Missing returns an error if any underlying
// call fails.
func Missing(ctx context.Context, r reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	exists := make([]bool, len(files))
	g, _ := errgroup.WithContext(ctx)
	for _, file := range files {
		if file.IsRef() {
			return nil, errors.E("missing", errors.Invalid, errors.Errorf("unresolved file: %v", file))
		}
	}
	for i, file := range files {
		i, file := i, file
		g.Go(func() (err error) {
			if _, err = r.Stat(ctx, file.ID); err == nil {
				exists[i] = true
			} else if errors.Is(errors.NotExist, err) {
				err = nil
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	all := files
	files = nil
	for i := range exists {
		if !exists[i] {
			files = append(files, all[i])
		}
	}
	return files, nil
}
