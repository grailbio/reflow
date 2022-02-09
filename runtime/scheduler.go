package runtime

import (
	"fmt"
	"strings"

	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
)

const (
	// The amount of outstanding number of transfers
	// between each repository.
	defaultTransferLimit = 20

	// The number of concurrent stat operations that can
	// be performed against a repository.
	statLimit = 200
)

// newScheduler returns a new scheduler with the specified configuration.
// Cancelling the returned context.CancelFunc stops the scheduler.
func newScheduler(config infra.Config, logger *log.Logger) (*sched.Scheduler, error) {
	var (
		err   error
		tdb   taskdb.TaskDB
		repo  reflow.Repository
		limit int
	)
	if err = config.Instance(&tdb); err != nil {
		if !strings.HasPrefix(err.Error(), "no providers for type taskdb.TaskDB") {
			return nil, err
		}
		logger.Debug(err)
	}
	if err = config.Instance(&repo); err != nil {
		return nil, err
	}
	if limit, err = transferLimit(config); err != nil {
		return nil, err
	}
	transferer := &repository.Manager{
		Status:           new(status.Status).Group("transfers"),
		PendingTransfers: repository.NewLimits(limit),
		Stat:             repository.NewLimits(statLimit),
		Log:              logger,
	}
	if repo != nil {
		transferer.PendingTransfers.Set(repo.URL().String(), int(^uint(0)>>1))
	}
	scheduler := sched.New()

	scheduler.Transferer = transferer
	scheduler.Log = logger.Tee(nil, "scheduler: ")
	scheduler.TaskDB = tdb
	scheduler.ExportStats()

	return scheduler, nil
}

// TransferLimit returns the configured transfer limit.
func transferLimit(config infra.Config) (int, error) {
	lim := config.Value("transferlimit")
	if lim == nil {
		return defaultTransferLimit, nil
	}
	v, ok := lim.(int)
	if !ok {
		return 0, errors.New(fmt.Sprintf("non-integer limit %v", lim))
	}
	return v, nil
}
