package testutil

import (
	"context"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/taskdb"
)

// NewNopTaskDB returns a nop taskdb (for tests).
func NewNopTaskDB() taskdb.TaskDB {
	return nopTaskDB{}
}

type nopTaskDB struct{}

// CreateRun is a no op.
func (n nopTaskDB) CreateRun(ctx context.Context, id digest.Digest, user string) error {
	return nil
}

// SetRunAttrs is a no op.
func (n nopTaskDB) SetRunAttrs(ctx context.Context, id, bundle digest.Digest, args []string) error {
	return nil
}

// CreateTask is a no op.
func (n nopTaskDB) CreateTask(ctx context.Context, id digest.Digest, run digest.Digest, flowid digest.Digest, uri string) error {
	return nil
}

// SetTaskResult is a no op.
func (n nopTaskDB) SetTaskResult(ctx context.Context, id digest.Digest, flowid digest.Digest) error {
	return nil
}

// Keepalive does nothing.
func (n nopTaskDB) Keepalive(ctx context.Context, id digest.Digest, keepalive time.Time) error {
	return nil
}

// Runs does nothing.
func (n nopTaskDB) Runs(ctx context.Context, query taskdb.Query) ([]taskdb.Run, error) {
	return []taskdb.Run{}, nil

}

// Tasks returns all the tasks with the specified run id.
func (n nopTaskDB) Tasks(ctx context.Context, query taskdb.Query) ([]taskdb.Task, error) {
	return []taskdb.Task{}, nil
}

// SetTaskAttrs does nothing.
func (n nopTaskDB) SetTaskAttrs(ctx context.Context, id, stdout, stderr, inspect digest.Digest) error {
	return nil
}

// Scan does nothing.
func (n nopTaskDB) Scan(ctx context.Context, kind taskdb.Kind, handler taskdb.MappingHandler) error {
	return nil
}
