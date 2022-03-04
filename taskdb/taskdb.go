// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package taskdb defines interfaces and data types for storing and querying reflow
// runs, tasks, pools and allocs. It also provides a function to keep a lease on a run/task/pool/alloc.
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
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/pool"
)

// RunID is a unique identifier for a run.
type RunID digest.Digest

func init() {
	infra.Register("runid", new(RunID))
}

// Init implements infra.Provider
func (r *RunID) Init() error {
	*r = NewRunID()
	return nil
}

// NewRunID produces a random RunID.
func NewRunID() RunID {
	return RunID(idDigester.Rand(nil))
}

// ID returns the string representation of the RunID.
func (r RunID) ID() string {
	return digest.Digest(r).String()
}

// Hex returns the padded hexadecimal representation of the Digest.
func (r RunID) Hex() string {
	return digest.Digest(r).Hex()
}

// IDShort returns a short string representation of the RunID.
func (r RunID) IDShort() string {
	return digest.Digest(r).HexN(4)
}

// IsValid returns whether or not the RunID has been set.
func (r RunID) IsValid() bool {
	return !digest.Digest(r).IsZero()
}

// MarshalJSON marshals the RunID into JSON format.
func (r RunID) MarshalJSON() ([]byte, error) {
	return digest.Digest(r).MarshalJSON()
}

// UnmarshalJSON unmarshals a RunID from JSON data.
func (r *RunID) UnmarshalJSON(b []byte) error {
	return (*digest.Digest)(r).UnmarshalJSON(b)
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
	// SetRunComplete marsk the run as complete.
	SetRunComplete(ctx context.Context, id RunID, runLog, evalGraph, trace digest.Digest, end time.Time) error
	// KeepRunAlive updates the keepalive timer for the specified run id. Updating the keepalive timer
	// allows the querying methods (Runs, Tasks) to see which runs/tasks are active and which are dead/complete.
	KeepRunAlive(ctx context.Context, id RunID, keepalive time.Time) error

	// CreateTask creates a new task in the taskdb with the provided task.
	CreateTask(ctx context.Context, task Task) error
	// SetTaskResult sets the result of the task post completion.
	SetTaskResult(ctx context.Context, id TaskID, result digest.Digest) error
	// SetTaskUri updates the task URI.
	SetTaskUri(ctx context.Context, id TaskID, uri string) error
	// SetTaskAttrs updates the task log ids.
	SetTaskAttrs(ctx context.Context, id TaskID, stdout, stderr, inspect digest.Digest) error
	// SetTaskComplete mark the task as completed as of the given end time with the error (if any)
	SetTaskComplete(ctx context.Context, id TaskID, err error, end time.Time) error

	// KeepTaskAlive updates the keepalive timer for the specified task id. Updating the keepalive timer
	// allows the querying methods (Runs, Tasks) to see which runs/tasks are active and which are dead/complete.
	KeepTaskAlive(ctx context.Context, id TaskID, keepalive time.Time) error

	// StartAlloc creates a new alloc in the taskdb with the provided parameters.
	StartAlloc(ctx context.Context, allocID reflow.StringDigest, poolID digest.Digest, resources reflow.Resources, start time.Time) error
	// StartPool creates a new pool in the taskdb with the provided parameters.
	StartPool(ctx context.Context, pool Pool) error

	// SetResources sets the resources field in the taskdb for the row with the given id.
	SetResources(ctx context.Context, id digest.Digest, resources reflow.Resources) error
	// KeepIDAlive updates the keepalive timer for the specified id.
	KeepIDAlive(ctx context.Context, id digest.Digest, keepalive time.Time) error
	// SetEndTime sets the end time for the given id.
	SetEndTime(ctx context.Context, id digest.Digest, end time.Time) error

	// Runs looks up a runs which matches query.
	// If error is not nil, then some error (retrieval, parse) could have occurred.
	// The returned slice will still contain information about the runs that did not cause an error.
	Runs(ctx context.Context, runQuery RunQuery) ([]Run, error)

	// Tasks returns all the tasks with the specified run id.
	// If error is not nil, then some error (retrieval, parse) could have occurred.
	// The returned slice will still contain information about the runs that did not cause an error.
	Tasks(ctx context.Context, taskQuery TaskQuery) ([]Task, error)

	// Allocs looks up Allocs which match query.
	// If error is not nil, then some error (retrieval, parse) could have occurred.
	// The returned slice will still contain information about the Allocs that did not cause an error.
	Allocs(ctx context.Context, query AllocQuery) ([]Alloc, error)

	// Pools returns all the pools matching the given pool query.
	// If error is not nil, then some error (retrieval, parse) could have occurred.
	// The returned slice will still contain information about the pools that did not cause an error.
	Pools(ctx context.Context, poolQuery PoolQuery) ([]PoolRow, error)

	// Scan calls the handler function for every association in the mapping.
	// Note that the handler function may be called asynchronously from multiple threads.
	Scan(ctx context.Context, kind Kind, handler MappingHandler) error

	// Repository returns the repository associated with this TaskDB.
	Repository() reflow.Repository
}

// TimeFields are various common fields found in all taskdb row types.
type TimeFields struct {
	// Start is the time the taskdb row was started.
	Start time.Time
	// Keepalive is the keepalive lease on the row.
	Keepalive time.Time
	// End is the time the taskdb row ended (if it has).
	End time.Time
}

// StartEnd returns the start and end times.
func (c TimeFields) StartEnd() (st, et time.Time) {
	st = c.Start
	et = c.Keepalive
	if t := c.End; !t.IsZero() {
		et = t
	}
	return
}

// Run is the run info stored in the taskdb.
type Run struct {
	TimeFields
	// ID is the run id.
	ID RunID
	// Labels is the labels specified during the invocation.
	Labels pool.Labels
	// User is the specified config.User()
	User string
	// Various logs and other run info generated for the run.
	RunLog, EvalGraph, Trace digest.Digest
	// For backwards compatibility
	// TODO(awissmann): Remove these after transition to runlog
	ExecLog, SysLog digest.Digest
}

func (r Run) String() string {
	labels := make([]string, 0, len(r.Labels))
	for k, v := range r.Labels {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	et := r.Keepalive
	if !r.End.IsZero() {
		et = r.End
	}
	return fmt.Sprintf("run %s %s %s %s %s", r.ID.IDShort(), r.User, strings.Join(labels, ","), r.Start.String(), et.String())
}

// Task is the task info stored in the taskdb.
type Task struct {
	TimeFields
	// ID is the task id.
	ID TaskID
	// RunID is the run id that created this task.
	RunID RunID
	// AllocID is the id of the alloc where this task was attempted.
	// This should be set to the digest of 'allocID' using which StartAlloc was called for this alloc.
	AllocID digest.Digest
	// FlowID is the flow id of this task.
	FlowID digest.Digest
	// ResultID is the id of the result, if non zero.
	ResultID digest.Digest
	// ImgCmdID is the ID of the underlying exec. It is a digest of an exec's image and cmd.
	// ImgCmdID has a many-to-many relationship with Ident.
	ImgCmdID ImgCmdID
	// Ident is the human-readable name of the underlying exec. Ident has a many-to-many
	// relationship with ImgCmdID.
	Ident string
	// Attempt stores the (zero-based) current attempt number for this task.
	Attempt int
	// Err stores the error for failed tasks
	Err errors.Error
	// Resources is the amount of resources reserved for this task.
	// Note that this may not represent actual utilized resources necessarily.
	Resources reflow.Resources
	// URI is the uri of the task.
	URI string
	// Stdout, Stderr and Inspect are the stdout, stderr and inspect ids of the task.
	Stdout, Stderr, Inspect digest.Digest

	// Alloc is the Alloc this task was executed on.
	Alloc *Alloc
}

func (t Task) String() string {
	et := t.Keepalive
	if !t.End.IsZero() {
		et = t.End
	}
	return fmt.Sprintf("task %s %s %s %s %s", t.ID.IDShort(), t.RunID.IDShort(), t.FlowID.Short(), t.Start.String(), et.String())
}

// ClusterID is the identifier of a cluster.
type ClusterID struct {
	// ClusterName is the name of the cluster.
	ClusterName string
	// User is the user of the cluster.
	User string
	// ReflowVersion is the version of reflow which started the cluster.
	ReflowVersion string
}

// Pool is the initial pool info with which a pool is started in taskdb.
type Pool struct {
	TimeFields
	ClusterID

	// PoolID is the Pool identifier.
	PoolID reflow.StringDigest
	// PoolType is the value of PoolType for the Pool
	PoolType string
	// Resources is the amount of resources in this Pool.
	Resources reflow.Resources
	// URI is the value of URI for the Pool.
	URI string
}

// PoolRow is the pool row retrieved from taskdb.
type PoolRow struct {
	Pool
	// ID is the id of the Pool.
	// This should be the value of Pool.PoolID.Digest().
	ID digest.Digest
}

// Alloc is info stored in the taskdb for Allocs.
type Alloc struct {
	TimeFields
	// ID is the id of the Alloc.
	ID digest.Digest
	// PoolID is the id of the pool this alloc belongs to.
	PoolID digest.Digest
	// Resources is the amount of resources in this Alloc.
	Resources reflow.Resources
	// URI is the value of URI for the Pool.
	URI string
	// Pool is the Pool this alloc belongs to.
	Pool *PoolRow
}

// TaskQuery is the task-querying struct for TaskDB.Tasks. There are two ways to query tasks:
//
// 1. Only ID/RunID/ImgCmdID/Ident specified: Query tasks with the corresponding ID.
//    When ImgCmdID/Ident is specified, the returned TaskDB.Tasks have only some of the fields set.
//
// 2. Since + Until specified: Query tasks whose keepalive is within that time frame.
//
// If either ID or RunID is provided, then if WithAlloc is also set, then the allocs
// that the tasks were executed on are also fetched (along with their Pools)
type TaskQuery struct {
	// ID is the task id being queried.
	ID TaskID
	// RunID is the runid of the tasks.
	RunID RunID
	// ImgCmdID is the behavior id of the task's exec.
	// It is calculated from an exec's resolved docker image and command.
	ImgCmdID ImgCmdID
	// Ident is the human-readable identifier of the task's exec.
	Ident string
	// Since, Until queries for tasks that were active past Since until Until.
	Since, Until time.Time
	// Limit is the maximum number of tasks a query will return. If Limit <= 0, the query will return
	// all matching tasks. It is possible to get more tasks than is specified than the Limit because
	// querying is stopped once there are at least as many tasks as Limit.
	Limit int64
	// WithAlloc determines if each Tasks's Alloc should also be returned.
	// WithAlloc is applicable only if either ID or RunID is specified.
	WithAlloc bool
}

// RunQuery is the run-querying struct for TaskDB.Runs. There are two ways to query runs:
//
// 1. Only ID specified: Query runs with the corresponding ID.
//
// 2. Since + Until + User specified: Query runs whose keepalive is within that time frame that belong to the specified User.
// If User is not specified, the query will return results for all users.
type RunQuery struct {
	// ID is the run id being queried.
	ID RunID
	// Since, Until queries for runs that were active past Since until Until.
	Since, Until time.Time
	// User looks up the runs that are created by the user. If empty, the user filter is dropped.
	User string
}

// PoolQuery is the querying struct for TaskDB Pools. There are two ways to query:
//
// 1. Only IDs specified: Query Pools with the corresponding IDs.
//
// 2. Since + Until + Cluster specified: Query pools matching the cluster description whose keepalive is within that time frame.
type PoolQuery struct {
	// IDs are the list of Alloc ids being queried.
	IDs []digest.Digest
	// Since, Until queries for pools that were active past Since until Until.
	Since, Until time.Time
	// Cluster looks up the pools that match the given cluster description.
	Cluster ClusterID
}

// AllocQuery is the querying struct for TaskDB Allocs (and the Pool they belong to).
type AllocQuery struct {
	// IDs are the list of Alloc ids being queried.
	IDs []digest.Digest
}

var (
	keepaliveTries    = 5
	keepaliveInterval = 2 * time.Minute
	wait              = 10 * time.Second
	ivOffset          = 30 * time.Second
	policy            = retry.Jitter(retry.MaxRetries(retry.Backoff(wait, keepaliveInterval, 1.2), keepaliveTries), 0.2)
)

// KeepRunAlive keeps runID alive in the task db until the provided context is canceled.
func KeepRunAlive(ctx context.Context, taskdb TaskDB, id RunID) error {
	return keepAliveAndEnd(ctx, func(keepalive time.Time) error {
		return taskdb.KeepRunAlive(ctx, id, keepalive)
	},
		func() error { return ctx.Err() },
	)
}

// KeepTaskAlive keeps taskID alive in the task db until the provided context is canceled.
func KeepTaskAlive(ctx context.Context, taskdb TaskDB, id TaskID) error {
	return keepAliveAndEnd(ctx, func(keepalive time.Time) error {
		return taskdb.KeepTaskAlive(ctx, id, keepalive)
	},
		func() error { return ctx.Err() },
	)
}

// KeepIDAliveAndEnd keeps id alive in the task db until the provided context is canceled,
// and upon cancellation, updates the id with the end time.  `endTimeout` is the timeout
// for the context to update end time once `ctx` is cancelled.
func KeepIDAliveAndEnd(ctx context.Context, taskdb TaskDB, id digest.Digest, endTimeout time.Duration) error {
	if taskdb == nil {
		return fmt.Errorf("taskdb.KeepIDAliveAndEnd %s: no taskdb", id)
	}
	return keepAliveAndEnd(ctx,
		func(keepalive time.Time) error {
			return taskdb.KeepIDAlive(ctx, id, keepalive)
		},
		// End func
		func() error {
			ectx, ecancel := context.WithTimeout(context.Background(), endTimeout)
			defer ecancel()

			var err error
			for retries := 0; ; retries++ {
				err = taskdb.SetEndTime(ectx, id, time.Now())
				if err == nil || errors.Is(errors.Fatal, err) {
					break
				}
				if err = retry.Wait(ectx, policy, retries); err != nil {
					break
				}
			}
			return err
		},
	)
}

func keepAliveAndEnd(ctx context.Context, keepAliveFunc func(keepalive time.Time) error, endFunc func() error) error {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return endFunc()
		}
		var err error
		for retries := 0; ; retries++ {
			err = keepAliveFunc(time.Now().Add(keepaliveInterval))
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
		t.Reset(keepaliveInterval - ivOffset)
	}
}
