// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"github.com/grailbio/base/limiter"
)

type FilesetLimiter struct {
	*limiter.Limiter
	n int
}

func NewFilesetLimiter(limit int) *FilesetLimiter {
	l := &FilesetLimiter{limiter.New(), limit}
	l.Release(limit)
	return l
}

func (l *FilesetLimiter) Limit() int {
	return l.n
}
