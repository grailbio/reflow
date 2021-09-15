package inmemorytaskdb

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/test/testutil"
)

func init() {
	infra.Register("inmemorytaskdb", new(TaskDB))
}

type TaskDB struct {
	taskdb.TaskDB
	infra2.TableNameFlagsTrait
	infra2.BucketNameFlagsTrait
}

// Init implements infra.Provider
func (t *TaskDB) Init() error {
	if t.TableName == "" {
		return fmt.Errorf("TaskDB table name cannot be empty")
	}

	if t.BucketName == "" {
		return fmt.Errorf("TaskDB bucket name cannot be empty")
	}
	t.TaskDB = NewInmemoryTaskDB(t.TableName, t.BucketName)
	return nil
}

// Flags implements infra.Provider.
func (t *TaskDB) Flags(flags *flag.FlagSet) {
	t.TableNameFlagsTrait.Flags(flags)
	t.BucketNameFlagsTrait.Flags(flags)
}

type InmemoryTaskDB struct {
	taskdb.TaskDB
	tableName string
	repo     reflow.Repository
	mu       sync.Mutex
	numCalls map[string]int
	tasks    map[taskdb.TaskID]taskdb.Task
}

type tableRepo struct {
	tableName, repoName string
}

var (
	mu   sync.Mutex
	tdbs = make(map[tableRepo]*InmemoryTaskDB)
)

func NewInmemoryTaskDB(tableName, repoName string) *InmemoryTaskDB {
	tr := tableRepo{tableName, repoName}
	mu.Lock()
	defer mu.Unlock()
	if tdb, ok := tdbs[tr]; ok && tdb != nil {
		return tdb
	}
	tdb := &InmemoryTaskDB{
		tableName: tableName,
		repo:     testutil.NewInmemoryRepository(repoName),
		numCalls: make(map[string]int),
		tasks:    make(map[taskdb.TaskID]taskdb.Task),
	}
	tdbs[tr] = tdb
	return tdb
}

func GetInmemoryTaskDB(tableName, repoName string) *InmemoryTaskDB {
	tr := tableRepo{tableName, repoName}
	mu.Lock()
	defer mu.Unlock()
	return tdbs[tr]
}

func (t *InmemoryTaskDB) NumCalls(callType string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.numCalls[callType]
}

func (t *InmemoryTaskDB) CreateRun(ctx context.Context, id taskdb.RunID, user string) error {
	callType := "CreateRun"
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numCalls[callType] = t.numCalls[callType] + 1
	// TODO(swami): Implement, ie, store and allow retrieval.
	return nil
}

func (t *InmemoryTaskDB) SetRunAttrs(ctx context.Context, id taskdb.RunID, bundle digest.Digest, args []string) error {
	// TODO(swami): Implement, ie, store and allow retrieval.
	return nil
}

func (t *InmemoryTaskDB) SetRunComplete(ctx context.Context, id taskdb.RunID, execLog, sysLog, evalGraph, trace digest.Digest, end time.Time) error {
	// TODO(swami): Implement, ie, store and allow retrieval.
	return nil
}

func (t *InmemoryTaskDB) KeepRunAlive(ctx context.Context, id taskdb.RunID, keepalive time.Time) error {
	// TODO(swami): Implement, ie, store and allow retrieval.
	return nil
}

func (t *InmemoryTaskDB) Tasks(ctx context.Context, taskQuery taskdb.TaskQuery) ([]taskdb.Task, error) {
	tasks := make([]taskdb.Task, 0, len(t.tasks))
	for _, tsk := range t.tasks {
		tasks = append(tasks, tsk)
	}
	return tasks, nil
}

// CreateTask creates a new task in the taskdb with the provided task.
func (t *InmemoryTaskDB) CreateTask(ctx context.Context, task taskdb.Task) error {
	callType := "CreateTask"
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numCalls[callType] = t.numCalls[callType] + 1
	t.tasks[task.ID] = task
	return nil
}

// SetTaskResult sets the result of the task post completion.
func (t *InmemoryTaskDB) SetTaskResult(ctx context.Context, id taskdb.TaskID, result digest.Digest) error {
	callType := "SetTaskResult"
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numCalls[callType] = t.numCalls[callType] + 1
	if tsk, ok := t.tasks[id]; ok {
		tsk.ResultID = result
		t.tasks[id] = tsk
	}
	return nil
}

// SetTaskUri updates the task URI.
func (t *InmemoryTaskDB) SetTaskUri(ctx context.Context, id taskdb.TaskID, uri string) error {
	callType := "SetTaskUri"
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numCalls[callType] = t.numCalls[callType] + 1
	if tsk, ok := t.tasks[id]; ok {
		tsk.URI = uri
		t.tasks[id] = tsk
	}
	return nil
}

func (t *InmemoryTaskDB) SetTaskAttrs(ctx context.Context, id taskdb.TaskID, stdout, stderr, inspect digest.Digest) error {
	return nil
}

// SetTaskComplete mark the task as completed as of the given end time with the error (if any)
func (t *InmemoryTaskDB) SetTaskComplete(ctx context.Context, id taskdb.TaskID, err error, end time.Time) error {
	callType := "SetTaskComplete"
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numCalls[callType] = t.numCalls[callType] + 1
	if tsk, ok := t.tasks[id]; ok {
		tsk.End = end
		t.tasks[id] = tsk
	}
	return nil
}

func (t *InmemoryTaskDB) KeepTaskAlive(ctx context.Context, id taskdb.TaskID, keepalive time.Time) error {
	callType := "KeepTaskAlive"
	t.mu.Lock()
	defer t.mu.Unlock()
	t.numCalls[callType] = t.numCalls[callType] + 1
	if tsk, ok := t.tasks[id]; ok {
		tsk.Keepalive = keepalive
		t.tasks[id] = tsk
	}
	return nil
}

func (t *InmemoryTaskDB) Repository() reflow.Repository {
	return t.repo
}
