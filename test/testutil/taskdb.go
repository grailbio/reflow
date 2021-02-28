package testutil

import (
	"context"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/taskdb"
)

// NewNopTaskDB returns a nop taskdb (for tests).
func NewNopTaskDB() taskdb.TaskDB {
	return nopTaskDB{}
}

type nopTaskDB struct{}

// CreateRun is a no op.
func (n nopTaskDB) CreateRun(ctx context.Context, id taskdb.RunID, user string) error {
	return nil
}

// SetRunAttrs is a no op.
func (n nopTaskDB) SetRunAttrs(ctx context.Context, id taskdb.RunID, bundle digest.Digest, args []string) error {
	return nil
}

// SetRunComplete is a no op.
func (n nopTaskDB) SetRunComplete(ctx context.Context, id taskdb.RunID, execLog, sysLog, evalGraph, trace digest.Digest, end time.Time) error {
	return nil
}

// CreateTask is a no op.
func (n nopTaskDB) CreateTask(ctx context.Context, task taskdb.Task) error {
	return nil
}

// SetTaskResult is a no op.
func (n nopTaskDB) SetTaskResult(ctx context.Context, id taskdb.TaskID, result digest.Digest) error {
	return nil
}

// SetTaskUri does nothing.
func (n nopTaskDB) SetTaskUri(ctx context.Context, id taskdb.TaskID, uri string) error {
	return nil
}

// SetTaskAttrs does nothing.
func (n nopTaskDB) SetTaskAttrs(ctx context.Context, id taskdb.TaskID, stdout, stderr, inspect digest.Digest) error {
	return nil
}

// SetTaskComplete does nothing.
func (n nopTaskDB) SetTaskComplete(ctx context.Context, id taskdb.TaskID, err error, end time.Time) error {
	return nil
}

// KeepRunAlive does nothing.
func (n nopTaskDB) KeepRunAlive(ctx context.Context, id taskdb.RunID, keepalive time.Time) error {
	return nil
}

// KeepTaskAlive does nothing.
func (n nopTaskDB) KeepTaskAlive(ctx context.Context, id taskdb.TaskID, keepalive time.Time) error {
	return nil
}

// StartAlloc does nothing.
func (n nopTaskDB) StartAlloc(ctx context.Context, allocID reflow.StringDigest, poolID digest.Digest, resources reflow.Resources, start time.Time) error {
	return nil
}

// StartPool does nothing.
func (n nopTaskDB) StartPool(ctx context.Context, poolID reflow.StringDigest, url, poolType string, resources reflow.Resources, start time.Time) error {
	return nil
}

// SetResources does nothing.
func (n nopTaskDB) SetResources(ctx context.Context, id digest.Digest, resources reflow.Resources) error {
	return nil
}

// KeepIDAlive does nothing.
func (n nopTaskDB) KeepIDAlive(ctx context.Context, id digest.Digest, keepalive time.Time) error {
	return nil
}

// SetEndTime does nothing.
func (n nopTaskDB) SetEndTime(ctx context.Context, id digest.Digest, end time.Time) error {
	return nil
}

// Runs does nothing.
func (n nopTaskDB) Runs(ctx context.Context, query taskdb.RunQuery) ([]taskdb.Run, error) {
	return []taskdb.Run{}, nil

}

// Tasks does nothing.
func (n nopTaskDB) Tasks(ctx context.Context, query taskdb.TaskQuery) ([]taskdb.Task, error) {
	return []taskdb.Task{}, nil
}

// Scan does nothing.
func (n nopTaskDB) Scan(ctx context.Context, kind taskdb.Kind, handler taskdb.MappingHandler) error {
	return nil
}
