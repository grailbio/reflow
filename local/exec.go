// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"net/url"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/blobrepo"

	"github.com/grailbio/reflow"
)

// execState describes the current state of an exec.
type execState int

const (
	execUnstarted execState = iota // the exec state machine has yet to start
	execInit                       // the exec state machine has started
	execCreated                    // the Docker container has been created
	execRunning                    // the Docker container is running
	execComplete                   // the Docker container has completed running; the results are available
)

const errExecNotComplete = "exec not complete"

type exec interface {
	reflow.Exec
	Go(context.Context)
	WaitUntil(execState) error
	Kill(context.Context) error
}

func saveInspect(ctx context.Context, state execState, insp reflow.ExecInspect, repo *url.URL) (d digest.Digest, err error) {
	if state != execComplete {
		err = errors.Errorf("cannot marshal, exec not complete: %s", insp.State)
		return
	}
	brepo, rerr := blobrepo.Dial(repo)
	if rerr != nil {
		err = errors.E("blobrepo.Dial", rerr)
		return
	}
	return repository.Marshal(ctx, brepo, insp)
}
