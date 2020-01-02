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
func (n nopTaskDB) CreateRun(ctx context.Context, id taskdb.RunID, user string) error {
	return nil
}

// SetRunAttrs is a no op.
func (n nopTaskDB) SetRunAttrs(ctx context.Context, id taskdb.RunID, bundle digest.Digest, args []string) error {
	return nil
}

// CreateTask is a no op.
func (n nopTaskDB) CreateTask(ctx context.Context, id taskdb.TaskID, runID taskdb.RunID, flowID digest.Digest, imgCmdID taskdb.ImgCmdID, ident, uri string) error {
	return nil
}

// SetTaskResult is a no op.
func (n nopTaskDB) SetTaskResult(ctx context.Context, id taskdb.TaskID, result digest.Digest) error {
	return nil
}

// SetTaskAttrs does nothing.
func (n nopTaskDB) SetTaskAttrs(ctx context.Context, id taskdb.TaskID, stdout, stderr, inspect digest.Digest) error {
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
