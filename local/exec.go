// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"fmt"
	"net/url"

	"golang.org/x/sync/errgroup"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/blobrepo"
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

func saveExecLog(ctx context.Context, e reflow.Exec, repo reflow.Repository, stdout bool) (logRef reflow.RepoObjectRef, err error) {
	log, logErr := e.Logs(ctx, stdout, !stdout, false)
	if logErr != nil {
		err = errors.E(fmt.Sprintf("e.Logs(stdout %t)", stdout), logErr)
		return
	}
	defer log.Close()
	if d, pErr := repo.Put(ctx, log); pErr == nil {
		logRef = reflow.RepoObjectRef{RepoURL: repo.URL(), Digest: d}
	} else {
		err = errors.E("repo.Put", pErr)
		return
	}
	return
}

func saveInspect(ctx context.Context, insp reflow.ExecInspect, repo reflow.Repository) (inspect reflow.RepoObjectRef, err error) {
	d, err := repository.Marshal(ctx, repo, insp)
	if err != nil {
		return
	}
	inspect = reflow.RepoObjectRef{RepoURL: repo.URL(), Digest: d}
	return
}

// Saving of Inspect and logs is best effort, meaning retryable errors from saving to the repo are not returned.
func saveExecInfo(ctx context.Context, state execState, e reflow.Exec, insp reflow.ExecInspect, repoUrl *url.URL, saveLogsToRepo bool) (
	runInfo reflow.ExecRunInfo, err error) {
	if state != execComplete {
		err = fmt.Errorf("cannot save exec info, exec not complete: %s", insp.State)
		return
	}
	runInfo = insp.RunInfo()
	repo, rerr := blobrepo.Dial(repoUrl)
	if rerr != nil {
		err = errors.E("blobrepo.Dial", rerr)
		return
	}

	if !saveLogsToRepo {
		var serr error
		runInfo.InspectDigest, serr = saveInspect(ctx, insp, repo)
		if serr != nil && errors.NonRetryable(serr) {
			err = serr
		}
		return
	}

	g, _ := errgroup.WithContext(ctx)
	var m errors.Multi
	g.Go(func() error {
		var saveErr error
		runInfo.InspectDigest, saveErr = saveInspect(ctx, insp, repo)
		if saveErr != nil && errors.NonRetryable(saveErr) {
			m.Add(saveErr)
		}
		return nil
	})
	g.Go(func() error {
		var saveErr error
		runInfo.Stdout, saveErr = saveExecLog(ctx, e, repo, true)
		if saveErr != nil && errors.NonRetryable(saveErr) {
			m.Add(saveErr)
		}
		return nil
	})
	g.Go(func() error {
		var saveErr error
		runInfo.Stderr, saveErr = saveExecLog(ctx, e, repo, false)
		if saveErr != nil && errors.NonRetryable(saveErr) {
			m.Add(saveErr)
		}
		return nil
	})
	_ = g.Wait()
	err = m.Combined()
	return
}
