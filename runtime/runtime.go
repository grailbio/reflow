package runtime

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/status"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/sched"
)

// RuntimeParams defines the set of parameters necessary to initialize a ReflowRuntime.
type RuntimeParams struct {

	// Config is the main configuration for the reflow runtime.
	Config infra.Config

	// Logger is the top-level logger passed to the various underlying components.
	// Logger is optional and overrides the Config provided logger (if any).
	Logger *log.Logger

	// Optional parameters

	// Status is the top-level status object.  If provided, various internal components
	// will create groups under this status object for reporting purposes.
	Status *status.Status
}

// NewRuntime creates a new reflow runtime from the given parameters.
// NewRuntime returns an error upon failure to setup and initialize a runtime.
func NewRuntime(p RuntimeParams) (ReflowRuntime, error) {
	rt := &runtime{RuntimeParams: p}
	return rt, rt.init()
}

// ReflowRuntime represents a reflow runtime environment for submitting reflow runs.
// ReflowRuntime manages all underlying infrastructure components necessary to
// process reflow runs (eg: Cluster, Scheduler, etc)
type ReflowRuntime interface {

	// Start starts the reflow runtime.
	// Start uses the provided context to run the necessary background processes,
	// upon cancellation of which, these processes are stopped and the runtime becomes unviable.
	Start(ctx context.Context)

	// Scheduler returns the underlying scheduler.
	// TODO(swami):  Remove this.  This is exposed temporarily and shouldn't need to be eventually.
	Scheduler() *sched.Scheduler

	// ClusterName returns the name of the underlying reflow cluster.
	// TODO(swami) Can we work around this instead of polluting this interface ?
	ClusterName() string
}

// runtime implements ReflowRuntime.
type runtime struct {
	RuntimeParams

	cluster   runner.Cluster
	scheduler *sched.Scheduler
	rtLog     *log.Logger

	startOnce once.Task
}

// Start implements ReflowRuntime.
func (r *runtime) Start(ctx context.Context) {
	_ = r.startOnce.Do(func() error {
		r.doStart(ctx)
		return nil
	})
}

// Scheduler implements ReflowRuntime.
func (r *runtime) Scheduler() *sched.Scheduler {
	return r.scheduler
}

// ClusterName implements ReflowRuntime.
func (r *runtime) ClusterName() string {
	return r.cluster.GetName()
}

func (r *runtime) init() (err error) {
	if r.Logger == nil {
		if err = r.Config.Instance(&r.Logger); err != nil {
			return errors.E("runtime.Init", "logger", errors.Fatal, err)
		}
	}
	r.rtLog = r.Logger.Tee(nil, "reflow runtime: ")

	if r.cluster, err = ClusterInstance(r.Config); err != nil {
		return errors.E("runtime.Init", "cluster", errors.Fatal, err)
	}

	if r.scheduler, err = newScheduler(r.Config, r.Logger); err != nil {
		return errors.E("runtime.Init", "scheduler", errors.Fatal, err)
	}
	r.scheduler.Cluster = r.cluster

	var sess *session.Session
	if err = r.Config.Instance(&sess); err != nil {
		return errors.E("runtime.Init", "session", errors.Fatal, err)
	}
	r.scheduler.Mux = blob.Mux{"s3": s3blob.New(sess)}

	r.setupStatus()

	return nil
}

func (r *runtime) setupStatus() {
	if r.Status == nil {
		return
	}
	if ec, ok := r.cluster.(*ec2cluster.Cluster); ok {
		ec.Status = r.Status.Group("ec2cluster")
	}
	t := r.scheduler.Transferer
	if m, ok := t.(*repository.Manager); ok {
		m.Status = r.Status.Group("transfers")
	}
}

func (r *runtime) doStart(ctx context.Context) {
	var wg sync.WaitGroup
	r.rtLog.Printf("===== started =====")

	if ec, ok := r.cluster.(*ec2cluster.Cluster); ok {
		ec.Start(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				ec.Shutdown()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = r.scheduler.Do(ctx)
	}()

	go func() {
		wg.Wait()
		r.rtLog.Printf("===== shutdown =====")
	}()
}
