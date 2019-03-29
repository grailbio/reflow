// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow_test

import (
	"context"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
)

type waitAssoc struct {
	assoc.Assoc
	tick chan struct{}
}

func newWaitAssoc() *waitAssoc {
	return &waitAssoc{tick: make(chan struct{})}
}

func (w *waitAssoc) Tick() {
	w.tick <- struct{}{}
}

func (w *waitAssoc) Get(ctx context.Context, kind assoc.Kind, k digest.Digest) (kexp, v digest.Digest, err error) {
	<-w.tick
	err = errors.E(errors.NotExist)
	return
}

func (w *waitAssoc) BatchGet(ctx context.Context, batch assoc.Batch) (err error) {
	for i := 0; i < len(batch); i++ {
		<-w.tick
	}
	err = errors.E(errors.NotExist)
	return
}
