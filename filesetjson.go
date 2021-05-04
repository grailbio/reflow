// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/grailbio/base/limiter"
)

type FilesetLimiter struct {
	*limiter.Limiter
	n int
}

func newFilesetLimiter(limit int) *FilesetLimiter {
	l := &FilesetLimiter{limiter.New(), limit}
	l.Release(limit)
	return l
}

func (l *FilesetLimiter) Limit() int {
	return l.n
}

var (
	// filesetOpLimiter is the number concurrent Fileset marshal/unmarshaling operations.
	// When large number of these operations are done concurrently (especially if the Filesets are large),
	// marshaling/unmarshaling too many of them can cause OOMs,
	// TODO(swami): Better solution is to use a more optimized Fileset format (either JSON or other).
	filesetOpLimiter *FilesetLimiter
	limiterOnce      sync.Once
	filesetOpLimit   = runtime.NumCPU()
)

// SetFilesetOpConcurrencyLimit sets the limit of concurrent fileset operations.
// For a successful reset of the limit, this should be called before a call is made
// to GetFilesetOpLimiter, ie before any reflow evaluations have started, otherwise this will panic.
func SetFilesetOpConcurrencyLimit(limit int) {
	if filesetOpLimiter != nil {
		panic(fmt.Sprintf("cannot reset marshal limit to %d (already set at %d)", limit, filesetOpLimiter.Limit()))
	}
	if limit <= 0 {
		return
	}
	filesetOpLimit = limit
}

func GetFilesetOpLimiter() *FilesetLimiter {
	limiterOnce.Do(func() {
		filesetOpLimiter = newFilesetLimiter(filesetOpLimit)
	})
	return filesetOpLimiter
}
