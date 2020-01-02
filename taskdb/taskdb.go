// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package taskdb defines interfaces and data types for storing and querying reflow
// runs and tasks. It also provides a function to keep a lease on a run/task.
package taskdb

import (
	"context"
	"crypto"
	_ "crypto/sha256"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/pool"
)

// RunID is a unique identifier for a run.
type RunID digest.Digest

// NewRunID produces a random RunID.
func NewRunID() RunID {
	return RunID(idDigester.Rand(nil))
}

// ID returns the string representation of the RunID.
func (r RunID) ID() string {
	return digest.Digest(r).String()
}

// IDShort returns a short string representation of the RunID.
func (r RunID) IDShort() string {
	return digest.Digest(r).HexN(4)
}

// IsValid returns whether or not the RunID has been set.
func (r RunID) IsValid() bool {
	return !digest.Digest(r).IsZero()
}

// TaskID is a unique identifier for a task.
type TaskID digest.Digest

// NewTaskID produces a random TaskID.
func NewTaskID() TaskID {
	return TaskID(idDigester.Rand(nil))
}

// ID returns the string representation of the TaskID.
func (t TaskID) ID() string {
	return digest.Digest(t).String()
}

// IDShort returns a short string representation of the TaskID.
func (t TaskID) IDShort() string {
	return digest.Digest(t).HexN(4)
}

// IsValid returns whether or not the TaskID has been set.
func (t TaskID) IsValid() bool {
	return !digest.Digest(t).IsZero()
}

// ImgCmdID describes the behavior of an exec.
// It is a digest of an exec's docker image + cmd.
type ImgCmdID digest.Digest

// NewImgCmdID returns an ImgCmdID created
// from an image and a cmd.
func NewImgCmdID(image, cmd string) ImgCmdID {
	w := idDigester.NewWriter()
	_, _ = io.WriteString(w, image)
	_, _ = io.WriteString(w, cmd)
	return ImgCmdID(w.Digest())
}

// ID returns the string representation of the ImgCmdID.
func (b ImgCmdID) ID() string {
	return digest.Digest(b).String()
}

// IsValid returns whether or not the ImgCmdID is valid.
func (b ImgCmdID) IsValid() bool {
	return !digest.Digest(b).IsZero()
}

// idDigester is the digester used to compute taskdb ID (TaskID, RunID, ImgCmdID)
// digests. We use a SHA256 digest.
var idDigester = digest.Digester(crypto.SHA256)

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
	CreateRun(ctx context.Context, id RunID, user string) error
	// SetRunAttrs sets the reflow bundle and corresponding args for this run.
	SetRunAttrs(ctx context.Context, id RunID, bundle digest.Digest, args []string) error
	// CreateTask creates a new task in the taskdb with the provided taskID, runID and flowID, imgCmdID, ident, and uri.
	CreateTask(ctx context.Context, id TaskID, runID RunID, flowID digest.Digest, imgCmdID ImgCmdID, ident, uri string) error
	// SetTaskResult sets the result of the task post completion.
	SetTaskResult(ctx context.Context, id TaskID, result digest.Digest) error
	// SetTaskLogs updates the task log ids.
	SetTaskAttrs(ctx context.Context, id TaskID, stdout, stderr, inspect digest.Digest) error
	// KeepRunAlive updates the keepalive timer for the specified run id. Updating the keepalive timer
	// allows the querying methods (Runs, Tasks) to see which runs/tasks are active and which are dead/complete.
	KeepRunAlive(ctx context.Context, id RunID, keepalive time.Time) error
	// KeepTaskAlive updates the keepalive timer for the specified task id. Updating the keepalive timer
	// allows the querying methods (Runs, Tasks) to see which runs/tasks are active and which are dead/complete.
	KeepTaskAlive(ctx context.Context, id TaskID, keepalive time.Time) error
	// Runs looks up a runs which matches query. If error is not nil, then some error (retrieval, parse) could have
	// occurred. The returned slice will still contain information about the runs that did not cause an error.
	Runs(ctx context.Context, runQuery RunQuery) ([]Run, error)
	// Tasks returns all the tasks with the specified run id. If error is not nil, then some error (retrieval, parse)
	// could have occurred. The returned slice will still contain information about the runs that did not cause an
	// error.
	Tasks(ctx context.Context, taskQuery TaskQuery) ([]Task, error)
	// Scan calls the handler function for every association in the mapping.
	// Note that the handler function may be called asynchronously from multiple threads.
	Scan(ctx context.Context, kind Kind, handler MappingHandler) error
}

// Run is the run info stored in the taskdb.
type Run struct {
	// ID is the run id.
	ID RunID
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
	return fmt.Sprintf("run %s %s %s %s %s", r.ID.IDShort(), r.User, strings.Join(labels, ","), r.Start.String(), r.Keepalive.String())
}

// Task is the task info stored in the taskdb.
type Task struct {
	// ID is the task id.
	ID TaskID
	// RunID is the run id that created this task.
	RunID RunID
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
	return fmt.Sprintf("task %s %s %s %s %s", t.ID.IDShort(), t.RunID.IDShort(), t.FlowID.Short(), t.Start.String(), t.Keepalive.String())
}

// TaskQuery is a query struct for the TaskDB query interface to query for tasks. All fields
// are optional. If nothing is specified, the query looks up ids that have
// keepalive updated in the last 30 minutes for any user. If a user filter is
// specified, all queries are restricted to runs/tasks created by the user. ID is the TaskID
// of a task. If ID is specified, tasks with ID are looked up. If RunID is specified, tasks with RunID are looked up.
// If Since is specified, runs/tasks whose keepalive is within that time frame are looked up.
type TaskQuery struct {
	// ID is the task id being queried.
	ID TaskID
	// RunID is the runid of the tasks.
	RunID RunID
	// Since queries for runs/tasks that were active past this time.
	Since time.Time
	// User looks up the runs/tasks that are created by the user. If empty, the user filter is dropped.
	User string
}

// RunQuery is a query struct for the TaskDB query interface to query for runs. All fields
// are optional. If nothing is specified, the query looks up ids that have
// keepalive updated in the last 30 minutes for any user. If a user filter is
// specified, all queries are restricted to runs/tasks created by the user. ID is the RunID
// of a run. If ID is specified, runs with ID are looked up. If Since is specified, runs/tasks
// whose keepalive is within that time frame are looked up.
type RunQuery struct {
	// ID is the run id being queried.
	ID RunID
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

// KeepRunAlive keeps runID alive in the task db until the provided context is canceled.
func KeepRunAlive(ctx context.Context, taskdb TaskDB, id RunID) error {
	return keepAlive(ctx, func(keepalive time.Time) error {
		return taskdb.KeepRunAlive(ctx, id, keepalive)
	})
}

// KeepTaskAlive keeps taskID alive in the task db until the provided context is canceled.
func KeepTaskAlive(ctx context.Context, taskdb TaskDB, id TaskID) error {
	return keepAlive(ctx, func(keepalive time.Time) error {
		return taskdb.KeepTaskAlive(ctx, id, keepalive)
	})
}

func keepAlive(ctx context.Context, keepAliveFunc func(keepalive time.Time) error) error {
	for {
		var err error
		for retries := 0; ; retries++ {
			t := time.Now().Add(keepaliveInterval)
			err = keepAliveFunc(t)
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
