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
	infra2 "github.com/grailbio/reflow/infra"
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

	// NewRunner returns a new reflow runner.
	NewRunner(RunnerParams) (ReflowRunner, error)
}

// runtime implements ReflowRuntime.
type runtime struct {
	RuntimeParams

	sess      *session.Session
	cluster   runner.Cluster
	scheduler *sched.Scheduler
	predCfg   *infra2.PredictorConfig
	rtLog     *log.Logger

	startOnce once.Task
}

// Start implements ReflowRuntime.
func (rt *runtime) Start(ctx context.Context) {
	_ = rt.startOnce.Do(func() error {
		rt.doStart(ctx)
		return nil
	})
}

// Scheduler implements ReflowRuntime.
func (rt *runtime) Scheduler() *sched.Scheduler {
	return rt.scheduler
}

// ClusterName implements ReflowRuntime.
func (rt *runtime) ClusterName() string {
	return rt.cluster.GetName()
}

func (rt *runtime) init() (err error) {
	if rt.Logger == nil {
		if err = rt.Config.Instance(&rt.Logger); err != nil {
			return errors.E("runtime.Init", "logger", errors.Fatal, err)
		}
	}
	rt.rtLog = rt.Logger.Tee(nil, "reflow runtime: ")

	if rt.cluster, err = ClusterInstance(rt.Config); err != nil {
		return errors.E("runtime.Init", "cluster", errors.Fatal, err)
	}

	if rt.scheduler, err = newScheduler(rt.Config, rt.Logger); err != nil {
		return errors.E("runtime.Init", "scheduler", errors.Fatal, err)
	}
	rt.scheduler.Cluster = rt.cluster

	if err = rt.Config.Instance(&rt.sess); err != nil {
		return errors.E("runtime.Init", "session", errors.Fatal, err)
	}
	rt.scheduler.Mux = blob.Mux{"s3": s3blob.New(rt.sess)}

	// We do not validate predictor config in the runtime because
	// - The default predictor config will not validate on non-EC2 machines (eg: laptops), preventing runs.
	// - It may not be needed by the run (and if needed, it'll be validated in NewRunner).
	if rt.predCfg, err = PredictorConfig(rt.Config, false /* no validation */); err != nil {
		return errors.E("runtime.Init", "predictor_config", errors.Fatal, err)
	}

	rt.setupStatus()

	return nil
}

func (rt *runtime) setupStatus() {
	if rt.Status == nil {
		return
	}
	if ec, ok := rt.cluster.(*ec2cluster.Cluster); ok {
		ec.Status = rt.Status.Group("ec2cluster")
	}
	t := rt.scheduler.Transferer
	if m, ok := t.(*repository.Manager); ok {
		m.Status = rt.Status.Group("transfers")
	}
}

func (rt *runtime) doStart(ctx context.Context) {
	var wg sync.WaitGroup
	rt.rtLog.Printf("===== started =====")

	if ec, ok := rt.cluster.(*ec2cluster.Cluster); ok {
		ec.Start(ctx, &wg)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = rt.scheduler.Do(ctx)
	}()

	go func() {
		wg.Wait()
		rt.rtLog.Printf("===== shutdown =====")
	}()
}
