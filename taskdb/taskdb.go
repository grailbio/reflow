// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package taskdb defines interfaces and data types for storing and querying reflow
// runs and tasks. It also provides a function to keep a lease on a run/task.
package taskdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/pool"
)

// Kind describes the kind of mapping.
type Kind int

// MappingHandlerFunc is a convenience type to avoid having to declare a struct
// to implement the MappingHandler interface.
type MappingHandlerFunc func(k, v digest.Digest, kind Kind, taskType string, labels []string)

// HandleMapping implements the MappingHandler interface.
func (h MappingHandlerFunc) HandleMapping(k, v digest.Digest, kind Kind, taskType string, labels []string) {
	h(k, v, kind, taskType, labels)
}

// MappingHandler is an interface for handling a mapping while scanning.
type MappingHandler interface {
	// HandleMapping handles a scanned association.
	HandleMapping(k, v digest.Digest, kind Kind, taskType string, labels []string)
}

// TaskDB is the interface to read/write run and task information to a run db.
type TaskDB interface {
	// CreateRun creates a new Run with the provided id and user.
	CreateRun(ctx context.Context, id digest.Digest, user string) error
	// SetRunAttrs sets the reflow bundle and corresponding args for this run.
	SetRunAttrs(ctx context.Context, id, bundle digest.Digest, args []string) error
	// CreateTask creates a new task with the provided id, runid, flowid and uri.
	CreateTask(ctx context.Context, id, run, flowid digest.Digest, uri string) error
	// SetTaskResult sets the result of the task post completion.
	SetTaskResult(ctx context.Context, id, result digest.Digest) error
	// SetTaskLogs updates the task log ids.
	SetTaskAttrs(ctx context.Context, id, stdout, stderr, inspect digest.Digest) error
	// Keepalive updates the keepalive timer for the specified id. Updating the keepalive timer
	// allows the querying methods (Runs, Tasks) to see which runs/tasks are active and which are dead/complete.
	Keepalive(ctx context.Context, id digest.Digest, keepalive time.Time) error
	// Runs looks up a runs which matches query. If error is not nil, then some error (retrieval, parse) could have
	// occurred. The returned slice will still contain information about the runs that did not cause an error.
	Runs(ctx context.Context, query Query) ([]Run, error)
	// Tasks returns all the tasks with the specified run id. If error is not nil, then some error (retrieval, parse)
	// could have occurred. The returned slice will still contain information about the runs that did not cause an
	// error.
	Tasks(ctx context.Context, query Query) ([]Task, error)
	// Scan calls the handler function for every association in the mapping.
	// Note that the handler function may be called asynchronously from multiple threads.
	Scan(ctx context.Context, kind Kind, handler MappingHandler) error
}

// Run is the run info stored in the taskdb.
type Run struct {
	// ID is the run id.
	ID digest.Digest
	// Labels is the labels specified during the invocation.
	Labels pool.Labels
	// User is the specified config.User()
	User string
	// Keepalive is the keepalive lease on the run.
	Keepalive time.Time
	// Start is the time the run was started.
	Start time.Time
}

func (r Run) String() string {
	labels := make([]string, 0, len(r.Labels))
	for k, v := range r.Labels {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	return fmt.Sprintf("run %s %s %s %s %s", r.ID.Short(), r.User, strings.Join(labels, ","), r.Start.String(), r.Keepalive.String())
}

// Task is the task info stored in the taskdb.
type Task struct {
	// ID is the task id.
	ID digest.Digest
	// RunID is the run id that created this task.
	RunID digest.Digest
	// FlowID is the flow id of this task.
	FlowID digest.Digest
	// ResultID is the id of the result, if non zero.
	ResultID digest.Digest
	// Keepalive is the keepalive lease on the task.
	Keepalive time.Time
	// Start is the time the task was started.
	Start time.Time
	// URI is the uri of the task.
	URI string
	// Stdout, Stderr and Inspect are the stdout, stderr and inspect ids of the task.
	Stdout, Stderr, Inspect digest.Digest
}

func (t Task) String() string {
	return fmt.Sprintf("task %s %s %s %s %s", t.ID.Short(), t.RunID.Short(), t.FlowID.Short(), t.Start.String(), t.Keepalive.String())
}

// Query is the generic query struct for the TaskDB querying interface.  All fields
// are optional. If nothing is specified, the query looks up ids that have
// keepalive updated in the last 30 minutes for any user. If a user filter is
// specified, all queries are restricted to runs/tasks created by the user.
// If id is specified, runs/tasks with id is looked up. If RunID is specified, tasks with
// RunID are looked up. If Since is specified, runs/tasks whose keepalive is within
// that time frame are looked up.
type Query struct {
	// ID is the run/task id being queried.
	ID digest.Digest
	// RunID is the runid of the tasks.
	RunID digest.Digest
	// Since queries for runs/tasks that were active past this time.
	Since time.Time
	// User looks up the runs/tasks that are created by the user. If empty, the user filter is dropped.
	User string
}

var (
	keepaliveTries    = 5
	keepaliveInterval = 2 * time.Minute
	wait              = 10 * time.Second
	policy            = retry.MaxTries(retry.Backoff(wait, keepaliveInterval, 1), keepaliveTries)
)

// Keepalive keeps 'id' alive in the task db until the provided context is canceled.
func Keepalive(ctx context.Context, taskdb TaskDB, id digest.Digest) error {
	for {
		var err error
		for retries := 0; ; retries++ {
			t := time.Now().Add(keepaliveInterval)
			err := taskdb.Keepalive(ctx, id, t)
			if err == nil || errors.Is(errors.Fatal, err) {
				break
			}
			if err = retry.Wait(ctx, policy, retries); err != nil {
				return err
			}
		}
		if err != nil && errors.Is(errors.Fatal, err) {
			return err
		}
		select {
		case <-time.After(keepaliveInterval - 30*time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
