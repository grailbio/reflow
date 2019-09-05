// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

//go:generate stringer -type=execState

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/filerepo"
)

// Exec directory layout:
//	<exec>/arg/n/m...
//	<exec>/out
//	<exec>/manifest.json	--	contains final output, only after things are done.
const (
	inspectPath  = "inspect.json"
	manifestPath = "manifest.json"
	objectPath   = "obj"
)

var dockerUser = fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())

// dockerExec is a (local) exec attached to a local executor, from which it
// is given its own subdirectory to operate. exec is responsible for
// the lifecycle of an exec through an executor. It maintains a state
// machine (invoked by exec.Go) to see the exec through completion.
// Before every state change, exec saves its state to manifestPath,
// and is always recoverable from the previous state.
type dockerExec struct {
	// The Executor that owns this exec.
	Executor *Executor
	// The (possibly nil) Logger that logs exec's actions, for external consumption.
	Log *log.Logger

	id      digest.Digest
	client  *client.Client
	repo    *filerepo.Repository
	staging filerepo.Repository
	stdout  *log.Logger
	stderr  *log.Logger

	mu   sync.Mutex
	cond *sync.Cond

	// Manifest stores the serializable state of the exec.
	Manifest
	err error
}

var retryPolicy = retry.MaxTries(retry.Backoff(time.Second, 10*time.Second, 1.5), 5)

// newExec creates a new exec with parent executor x.
func newDockerExec(id digest.Digest, x *Executor, cfg reflow.ExecConfig, stdout, stderr *log.Logger) *dockerExec {
	e := &dockerExec{
		Executor: x,
		// Fill in from executor:
		Log:    x.Log.Tee(nil, fmt.Sprintf("%s: ", id)),
		repo:   x.FileRepository,
		id:     id,
		client: x.Client,
		stdout: stdout,
		stderr: stderr,
	}
	e.staging.Root = e.path(objectsDir)
	e.staging.Log = e.Log
	e.Config = cfg
	e.Manifest.Type = execDocker
	e.Manifest.Created = time.Now()
	e.cond = sync.NewCond(&e.mu)
	return e
}

// TODO(marius): checksum the manifest file (and other state) to identify
// partial writes. (This is likely not a problem in this case since the JSON
// struct would be incomplete.)
// This could also be made more resilient by creating a backup file
// before saving the new state.
func (e *dockerExec) save(state execState) error {
	if err := os.MkdirAll(e.path(), 0777); err != nil {
		return err
	}
	path := e.path(manifestPath)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	manifest := e.Manifest
	manifest.State = state
	if err := json.NewEncoder(f).Encode(manifest); err != nil {
		os.Remove(path)
		f.Close()
		return err
	}
	f.Close()
	return nil
}

// containerName returns the name of the container for
// this exec. It is uniquely determined by the exec's ID and directory.
func (e *dockerExec) containerName() string {
	pathHex := reflow.Digester.FromString(e.path()).Short()
	return fmt.Sprintf("reflow-%s-%s-%s", e.Executor.ID, e.id.Hex(), pathHex)
}

// create sets up the exec's filesystem layout environment and
// instantiates its container. It is not run. The arguments are
// materialized to a the 'arg' directory in the exec's run directory,
// and are passed into the container as /arg. The output object is
// placed in 'obj': the run directory is bound into the container as
// '/return', and $out is set to /return/obj. This arrangement permits
// for 'obj' to be either a file or a directory.
//
// We use Docker's host networking mode. In the future we'd like to
// disable networking altogether (except for special execs like those
// associated with interns and externs).
func (e *dockerExec) create(ctx context.Context) (execState, error) {
	if _, err := e.client.ContainerInspect(ctx, e.containerName()); err == nil {
		return execCreated, nil
	} else if !client.IsErrNotFound(err) {
		return execInit, errors.E("ContainerInspect", e.containerName(), kind(err), err)
	}
	// TODO: it might be worthwhile doing image pulling as a separate state.
	for retries := 0; ; retries++ {
		err := e.Executor.ensureImage(ctx, e.Config.Image)
		if err == nil {
			break
		}
		e.Log.Errorf("error ensuring image %s: %v", e.Config.Image, err)
		if err := retry.Wait(ctx, retryPolicy, retries); err != nil {
			return execInit, errors.E(errors.Unavailable, fmt.Sprintf("failed to pull image %s: %s", e.Config.Image, err))
		}
	}
	// Map the products to input arguments and volume bindings for
	// the container. Currently we map the whole repository (named by
	// the digest) and then include the cut in the arguments passed to
	// the job.
	args := make([]interface{}, len(e.Config.Args))
	for i, iv := range e.Config.Args {
		if iv.Out {
			which := strconv.Itoa(iv.Index)
			args[i] = path.Join("/return", which)
		} else {
			flat := iv.Fileset.Flatten()
			argv := make([]string, len(flat))
			for j, jv := range flat {
				argPath := fmt.Sprintf("arg/%d/%d", i, j)
				binds := map[string]digest.Digest{}
				for path, file := range jv.Map {
					binds[path] = file.ID
				}
				if err := e.repo.Materialize(e.path(argPath), binds); err != nil {
					return execInit, err
				}
				argv[j] = "/" + argPath
			}
			args[i] = strings.Join(argv, " ")
		}
	}
	// Set up temporary directory.
	os.MkdirAll(e.path("tmp"), 0777)
	os.MkdirAll(e.path("return"), 0777)
	hostConfig := &container.HostConfig{
		Binds: []string{
			e.hostPath("arg") + ":/arg",
			e.hostPath("tmp") + ":/tmp",
			e.hostPath("return") + ":/return",
		},
		NetworkMode: container.NetworkMode("host"),
		// Try to ensure that jobs we control get killed before the reflowlet,
		// so that we don't lose adjacent tasks unnecessarily and so that
		// errors are more sensible to the user.
		OomScoreAdj: 1000,
	}
	/*		TODO: introduce strict mode for this
	if mem := e.Config.Resources.Memory; mem > 0 {
		hostConfig.Resources.Memory = int64(mem)
	}
	*/
	env := []string{
		"tmp=/tmp",
		"TMPDIR=/tmp",
		"HOME=/tmp",
	}
	if outputs := e.Config.OutputIsDir; outputs != nil {
		for i, isdir := range outputs {
			if isdir {
				os.MkdirAll(e.path("return", strconv.Itoa(i)), 0777)
			}
		}
	} else {
		env = append(env, "out=/return/default")
	}
	// TODO(marius): this is a hack for Earl to use the AWS tool.
	if e.Config.NeedAWSCreds {
		creds, err := e.Executor.AWSCreds.Get()
		if err != nil {
			// We mark this as temporary, because most of the time it is.
			// TODO(marius): can we get better error classification from
			// the AWS SDK?
			return execInit, errors.E("run", e.ID, errors.Temporary, err)
		}
		// TODO(marius): region?
		env = append(env, "AWS_ACCESS_KEY_ID="+creds.AccessKeyID)
		env = append(env, "AWS_SECRET_ACCESS_KEY="+creds.SecretAccessKey)
		env = append(env, "AWS_SESSION_TOKEN="+creds.SessionToken)
	}
	config := &container.Config{
		Image: e.Config.Image,
		// We use a login shell here as many Docker images are configured
		// with /root/.profile, etc.
		Entrypoint: []string{"/bin/bash", "-e", "-l", "-o", "pipefail", "-c", fmt.Sprintf(e.Config.Cmd, args...)},
		Cmd:        []string{},
		Env:        env,
		Labels:     map[string]string{"reflow-id": e.id.Hex()},
		User:       dockerUser,
	}
	networkingConfig := &network.NetworkingConfig{}
	if _, err := e.client.ContainerCreate(ctx, config, hostConfig, networkingConfig, e.containerName()); err != nil {
		return execInit, errors.E(
			"ContainerCreate",
			kind(err),
			e.containerName(),
			fmt.Sprint(config), fmt.Sprint(hostConfig), fmt.Sprint(networkingConfig),
			err,
		)
	}
	return execCreated, nil
}

func scanLines(input io.ReadCloser, output *log.Logger) error {
	r, w := io.Pipe()
	go func() {
		stdcopy.StdCopy(w, w, input)
		w.Close()
	}()
	s := bufio.NewScanner(r)
	for s.Scan() {
		output.Print(s.Text())
	}
	return s.Err()
}

// start starts the container that's been set up by exec.create.
func (e *dockerExec) start(ctx context.Context) (execState, error) {
	if err := e.client.ContainerStart(ctx, e.containerName(), types.ContainerStartOptions{}); err != nil {
		return execCreated, errors.E("ContainerStart", e.containerName(), kind(err), err)
	}
	var err error
	e.Docker, err = e.client.ContainerInspect(ctx, e.containerName())
	if err != nil {
		e.Log.Errorf("error inspecting container %q: %v", e.containerName(), err)
	}

	if e.stdout != nil {
		rcStdout, err := e.client.ContainerLogs(ctx, e.containerName(),
			types.ContainerLogsOptions{ShowStdout: true, Follow: true})
		if err != nil {
			e.Log.Errorf("docker.containerlogs %q: %v", e.containerName(), err)
		} else {
			go func() {
				err := scanLines(rcStdout, e.stdout)
				if err != nil {
					log.Errorf("scanlines stdout: %v", err)
				}
				rcStdout.Close()
			}()
		}
	}
	if e.stderr != nil {
		rcStderr, err := e.client.ContainerLogs(ctx, e.containerName(),
			types.ContainerLogsOptions{ShowStderr: true, Follow: true})
		if err != nil {
			e.Log.Errorf("docker.containerlogs %q: %v", e.containerName(), err)
		} else {
			go func() {
				err := scanLines(rcStderr, e.stderr)
				if err != nil {
					log.Errorf("scanlines stderr: %v", err)
				}
				rcStderr.Close()
			}()
		}
	}
	return execRunning, nil
}

// wait waits for the container complete and performs teardown:
// - save log files to the exec directory;
// - inspect the docker container and save its output to exec.Manifest.Docker;
// - install the results into the repository;
// - remove (de-link) the argument directory.
func (e *dockerExec) wait(ctx context.Context) (state execState, err error) {
	// We start profiling here. Note that if the executor is restarted,
	// and thus reattaches to the container, it will lose samples.
	profc := make(chan stats)
	profctx, cancelprof := context.WithCancel(ctx)
	go func() {
		stats, err := e.profile(profctx)
		if err != nil {
			e.Log.Errorf("profile: %v", err)
		}
		profc <- stats
	}()

	// The documentation for ContainerWait seems to imply that both channels will
	// be sent. In practice it's one or the other, and it's also not buffered. Cool API.
	respc, errc := e.client.ContainerWait(ctx, e.containerName(), container.WaitConditionNotRunning)
	var code int64
	select {
	case err := <-errc:
		return execInit, errors.E("ContainerWait", e.containerName(), kind(err), err)
	case resp := <-respc:
		code = resp.StatusCode
	}
	// Best-effort writing of log files.
	rc, err := e.client.ContainerLogs(
		ctx, e.containerName(),
		types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err == nil {
		// TODO: these should be put into the repository.
		stderr, err := os.Create(e.path("stderr"))
		if err != nil {
			e.Log.Errorf("failed to stderr log file %q: %s", e.path("stderr"), err)
			stderr = nil
		}
		stdout, err := os.Create(e.path("stdout"))
		if err != nil {
			e.Log.Errorf("failed to stdout log file %q: %s", e.path("stdout"), err)
			stdout = nil
		}
		_, err = stdcopy.StdCopy(stdout, stderr, rc)
		if err != nil {
			e.Log.Errorf("failed to copy stdout and stderr logs: %s", err)
		}
		rc.Close()
		if stderr != nil {
			stderr.Close()
		}
		if stdout != nil {
			stdout.Close()
		}
	}
	e.Docker, err = e.client.ContainerInspect(ctx, e.containerName())
	if err != nil {
		return execInit, errors.E("ContainerInspect", e.containerName(), kind(err), err)
	}
	// Docker can return inconsistent return codes between a ContainerWait and
	// a ContainerInspect call. If either of these calls return a non zero exit code,
	// we use that as the exit status.
	if code == 0 && e.Docker.State.ExitCode != 0 {
		code = int64(e.Docker.State.ExitCode)
	}

	// Retrieve the profile before we clean up the results.
	cancelprof()
	e.Manifest.Stats = <-profc

	finishedAt, err := time.Parse(time.RFC3339Nano, e.Docker.State.FinishedAt)
	if err != nil {
		return execInit, errors.E(errors.Invalid, errors.Errorf("parsing docker time %s: %v", e.Docker.State.FinishedAt, err))
	}
	// The Docker daemon does not reliably report the container's exit
	// status correctly, and, what's worse, ContainerWait can return
	// successfully while the container is still running. This appears
	// to happen during system shutdown (e.g., the Docker daemon is
	// killed before Reflow) and also on system restart (Docker daemon
	// restores container state from disk).
	//
	// We are not currently able to distinguish between a system restart
	// and a successful exit.
	//
	// This appears to be fixed in Docker/Moby 1.13, but we are not yet
	// ready to adopt this. See:
	// 	https://github.com/moby/moby/issues/31262
	//
	// TODO(marius): either upgrade to Docker/Moby 1.13, or else add
	// some sort of epoch detection (Docker isn't helpful here either,
	// but system start time might be a good proxy.)
	switch {
	// ContainerWait returns while the container is in running state
	// (explicitly, or without a finish time). This happens during
	// system shutdown.
	case e.Docker.State.Running || finishedAt.IsZero():
		return execInit, errors.E(
			"exec", e.id, errors.Temporary,
			errors.New("container returned in running state; docker daemon likely shutting down"))
	// The remaining appear to be true completions.
	case code == 137 || e.Docker.State.OOMKilled:
		e.Manifest.Result.Err = errors.Recover(errors.E("exec", e.id, errors.Temporary, errors.New("killed by the OOM killer")))
	case code == 0:
		if err := e.install(ctx); err != nil {
			return execInit, err
		}
	default:
		e.Manifest.Result.Err = errors.Recover(errors.E("exec", e.id, errors.Errorf("exited with code %d", code)))
	}

	// Clean up args. TODO(marius): replace these with symlinks to sha256s also?
	if err := os.RemoveAll(e.path("arg")); err != nil {
		e.Log.Errorf("failed to remove arg path: %v", err)
	}
	if err := os.RemoveAll(e.path("tmp")); err != nil {
		e.Log.Errorf("failed to remove tmpdir: %v", err)
	}
	return execComplete, nil
}

// profile profiles the container and returns a profile when its
// context is cancelled or when the container stops.
func (e *dockerExec) profile(ctx context.Context) (stats, error) {
	// Sample disk usage every minute.
	// TODO(marius): perform a final disk usage check before returning
	const diskPeriod = time.Minute
	var (
		lastDiskTime time.Time
		stats        = make(stats)
		paths        = map[string]string{"tmp": e.path("tmp"), "disk": e.path("return")}
	)
	resp, err := e.client.ContainerStats(ctx, e.containerName(), true /*stream*/)
	if err != nil {
		return nil, errors.E("ContainerStats", kind(err), err)
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	gauges := make(reflow.Gauges)
	for {
		var v types.StatsJSON
		if err := dec.Decode(&v); err != nil {
			if err == io.EOF {
				return stats, nil
			}
			dec = json.NewDecoder(io.MultiReader(dec.Buffered(), resp.Body))
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return stats, nil
			}
		}
		var (
			deltaCPU = float64(v.CPUStats.CPUUsage.TotalUsage - v.PreCPUStats.CPUUsage.TotalUsage)
			deltaSys = float64(v.CPUStats.SystemUsage - v.PreCPUStats.SystemUsage)
			// TODO(marius): switch to stats.CPUStats.OnlineCPUs once we update the
			// Docker client.
			ncpu = float64(len(v.CPUStats.CPUUsage.PercpuUsage))
		)
		if deltaSys > 0 {
			// We compute the CPU time here by looking at the proportion of
			// this container's CPU time to total system time. This is normalized
			// and so needs to be multiplied by the  number of CPUs to get a
			// portable load number.
			load := ncpu * deltaCPU / deltaSys
			stats.Observe("cpu", load)
			gauges["cpu"] = load
		}

		// We exclude page cache memory since this is not counted towards
		// your limits.
		mem := float64(v.MemoryStats.Usage - v.MemoryStats.Stats["cache"])
		stats.Observe("mem", mem)
		gauges["mem"] = mem

		if time.Since(lastDiskTime) >= diskPeriod {
			for k, path := range paths {
				n, err := du(path)
				if err != nil {
					e.Log.Errorf("du %s: %v", path, err)
					continue
				}
				gauges[k] = float64(n)
				stats.Observe(k, float64(n))
			}
			lastDiskTime = time.Now()
		}
		e.Manifest.Gauges = gauges.Snapshot()
	}
}

// Go runs the exec's state machine. It resumes from the saved state
// when possible; if no state exists, it begins from execUnstarted,
// and immediately transitions to execInit.
func (e *dockerExec) Go(ctx context.Context) {
	os.MkdirAll(e.path(), 0777)
	/*
		if f, err := os.OpenFile(e.path("log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600); err != nil {
			e.printf("failed to open local log file: %v", err)
		} else {
			e.logger = log.New(f, "", log.Llongfile|log.LstdFlags)
			defer func() {
				e.logger = nil
				f.Close()
			}()
		}
	*/
	for state, err := e.getState(); err == nil && state != execComplete; e.setState(state, err) {
		switch state {
		case execUnstarted:
			state = execInit
		case execInit:
			state, err = e.create(ctx)
		case execCreated:
			state, err = e.start(ctx)
		case execRunning:
			state, err = e.wait(ctx)
		default:
			panic("bug")
		}
		if err == nil {
			err = e.save(state)
		}
		if state == execComplete {
			if err := e.client.ContainerRemove(context.Background(), e.containerName(), types.ContainerRemoveOptions{}); err != nil {
				e.Log.Errorf("failed to remove container %s: %s", e.containerName(), err)
			}
		}
	}
}

// Logs returns the stdout and/or stderr log files. Logs returns live
// logs from the Docker daemon if the exec is still running;
// otherwise the saved logs are returned.
//
// Note that this is a bit racy (e.g., we could switch states between
// the state check and acting on that state here); but we don't worry
// too much about it, as this is used for diagnostics purposes, and
// can easily be retried.
func (e *dockerExec) Logs(ctx context.Context, stdout, stderr, follow bool) (io.ReadCloser, error) {
	state, err := e.getState()
	if err != nil {
		return nil, err
	}
	if !stdout && !stderr {
		return nil, errors.Errorf("logs %v %v %v: must specify at least one of stdout, stderr", e.id, stdout, stderr)
	}
	switch state {
	case execUnstarted, execInit, execCreated:
		return nil, errors.Errorf("logs %v %v %v: exec not yet started", e.id, stdout, stderr)
	case execRunning:
		// Note that this is technically racy (we may be competing with the completion
		// routine), but since this is for user interaction, it's probably not a big deal.
		opts := types.ContainerLogsOptions{ShowStdout: stdout, ShowStderr: stderr, Follow: follow}
		rc, err := e.client.ContainerLogs(ctx, e.containerName(), opts)
		if err != nil {
			return nil, errors.E("ContainerLogs", e.containerName, fmt.Sprint(opts), kind(err), err)
		}
		r, w := io.Pipe()
		go func() {
			stdcopy.StdCopy(w, w, rc)
			w.Close()
		}()
		return newAllCloser(r, rc), nil
	case execComplete:
		// This doesn't really make sense for materialized logs. When
		// querying a live Docker container, we get interleaved log lines;
		// here we simply concatenate stderr to stdout. Since we cannot
		// stay true to this interface, we should perhaps permit only one
		// log file to be retrieved at a time.
		var files []*os.File
		if stdout {
			file, err := os.Open(e.path("stdout"))
			if err != nil {
				return nil, err
			}
			files = append(files, file)
		}
		if stderr {
			file, err := os.Open(e.path("stderr"))
			if err != nil {
				return nil, err
			}
			files = append(files, file)
		}
		readers := make([]io.Reader, len(files))
		closers := make([]io.Closer, len(files))
		for i, f := range files {
			readers[i] = f
			closers[i] = f
		}
		return newAllCloser(io.MultiReader(readers...), closers...), nil
	}
	panic("bug")
}

func (e *dockerExec) Shell(ctx context.Context) (io.ReadWriteCloser, error) {
	state, err := e.getState()
	if err != nil {
		return nil, err
	}
	switch state {
	case execRunning:
		c := types.ExecConfig{
			Cmd:          []string{"/bin/bash"},
			AttachStdin:  true,
			AttachStdout: true,
			AttachStderr: true,
			Tty:          true,
			DetachKeys:   "ctrl-p,ctrl-q",
		}
		response, err := e.client.ContainerExecCreate(ctx, e.containerName(), c)
		if err != nil {
			return nil, err
		}
		conn, err := e.client.ContainerExecAttach(ctx, response.ID, types.ExecStartCheck{})
		if err != nil {
			return nil, err
		}
		return conn.Conn, nil
	default:
		return nil, errors.New("cannot shell into a non-running exec")
	}
}

// Inspect returns the current state of the exec.
func (e *dockerExec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
	inspect := reflow.ExecInspect{
		Created: e.Manifest.Created,
		Config:  e.Config,
		Docker:  e.Docker,
		Profile: e.Manifest.Stats.Profile(),
		Gauges:  e.Manifest.Gauges,
	}
	state, err := e.getState()
	if err != nil {
		inspect.Error = errors.Recover(err)
	}
	switch state {
	case execUnstarted, execInit:
		inspect.State = "initializing"
		inspect.Status = "the exec is still initializing"
	case execCreated:
		inspect.State = "created"
		inspect.Status = "the exec container was created"
	case execRunning:
		top, err := e.client.ContainerTop(ctx, e.containerName(), []string{"auwx"})
		if err != nil {
			e.Log.Errorf("top %s: %v", e.containerName(), err)
		} else {
			var i int
			for ; i < len(top.Titles); i++ {
				if top.Titles[i] == "COMMAND" {
					break
				}
			}
			if i != len(top.Titles) {
				inspect.Commands = make([]string, len(top.Processes))
				for j, proc := range top.Processes {
					inspect.Commands[j] = proc[i]
				}
			}
		}
		inspect.State = "running"
		inspect.Status = "the exec container is running"
	case execComplete:
		inspect.State = "complete"
		inspect.Status = "the exec container has completed"
	}
	return inspect, nil
}

// Value returns the value computed by the exec.
func (e *dockerExec) Result(ctx context.Context) (reflow.Result, error) {
	state, err := e.getState()
	if err != nil {
		return reflow.Result{}, err
	}
	if state != execComplete {
		return reflow.Result{}, errors.Errorf("result %v: exec not complete", e.id)
	}
	return e.Manifest.Result, nil
}

func (e *dockerExec) Promote(ctx context.Context) error {
	return e.repo.Vacuum(ctx, &e.staging)
}

// Kill kills the exec's container and removes it entirely.
func (e *dockerExec) Kill(ctx context.Context) error {
	e.client.ContainerKill(ctx, e.containerName(), "KILL")
	if err := e.Wait(ctx); err != nil {
		return err
	}
	return os.RemoveAll(e.path())
}

// WaitUntil returns when the object state reaches at least min, or
// an error occurs.
func (e *dockerExec) WaitUntil(min execState) error {
	e.mu.Lock()
	for e.State < min && e.err == nil {
		e.cond.Wait()
	}
	e.mu.Unlock()
	return e.err
}

// Wait waits until the exec reaches completion.
func (e *dockerExec) Wait(ctx context.Context) error {
	return e.WaitUntil(execComplete)
}

// URI returns a URI For this exec based on its executor's URI.
func (e *dockerExec) URI() string { return e.Executor.URI() + "/" + e.id.Hex() }

// ID returns this exec's ID.
func (e *dockerExec) ID() digest.Digest { return e.id }

// path constructs a path in the exec's directory.
func (e *dockerExec) path(elems ...string) string {
	return e.Executor.execPath(e.id, elems...)
}

// path constructs a host path in the exec's directory.
func (e *dockerExec) hostPath(elems ...string) string {
	return e.Executor.execHostPath(e.id, elems...)
}

// setState sets the current state and error. It broadcasts
// on the exec's condition variable to wake up all waiters.
func (e *dockerExec) setState(state execState, err error) {
	e.mu.Lock()
	e.State = state
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
}

// getState returns the current state of the exec.
func (e *dockerExec) getState() (execState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.State, e.err
}

// install installs the exec's result object into the repository.
// install removes the original copy of each object, replacing it
// with a symlink to the digest of that object; this is to aid with
// debugging.
func (e *dockerExec) install(ctx context.Context) error {
	if e.Manifest.Result.Fileset.Map != nil || e.Manifest.Result.Fileset.List != nil {
		return nil
	}
	if outputs := e.Config.OutputIsDir; outputs != nil {
		e.Manifest.Result.Fileset.List = make([]reflow.Fileset, len(outputs))
		for i := range outputs {
			var err error
			e.Manifest.Result.Fileset.List[i], err =
				e.Executor.install(ctx, e.path("return", strconv.Itoa(i)), true, &e.staging)
			if err != nil {
				return err
			}
		}
		return nil
	}
	var err error
	e.Manifest.Result.Fileset, err = e.Executor.install(ctx, e.path("return", "default"), true, &e.staging)
	return err
}

// allCloser defines a io.ReadCloser over a number of a reader
// and multiple closers.
type allCloser struct {
	io.Reader
	closers []io.Closer
}

func newAllCloser(r io.Reader, closers ...io.Closer) io.ReadCloser {
	return &allCloser{r, closers}
}

func (c *allCloser) Close() error {
	var err error
	for _, c := range c.closers {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return err
}

// Kind returns the kind of a docker error.
func kind(err error) errors.Kind {
	switch {
	case client.IsErrNotFound(err):
		return errors.NotExist
	case client.IsErrUnauthorized(err):
		return errors.NotAllowed
	default:
		// Liberally pick unavailable as the default error, so that lower
		// layers can retry errors that may be fruitfully retried.
		// This is always safe to do, but may cause extra work.
		return errors.Unavailable
	}
}
