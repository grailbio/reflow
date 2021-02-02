// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"docker.io/go-docker/api/types"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/errors"
)

// Result is the result of an exec.
type Result struct {
	// Fileset is the fileset produced by an exec.
	Fileset Fileset `json:",omitempty"`

	// Err is error produced by an exec.
	Err *errors.Error `json:",omitempty"`
}

// String renders a human-readable string of this result.
func (r Result) String() string {
	if err := r.Err; err != nil {
		return "error<" + err.Error() + ">"
	}
	return r.Fileset.String()
}

// Short renders an abbreviated human-readable string of this result.
func (r Result) Short() string {
	if r.Err != nil {
		return r.String()
	}
	return r.Fileset.Short()
}

// Equal tells whether r is equal to s.
func (r Result) Equal(s Result) bool {
	if !r.Fileset.Equal(s.Fileset) {
		return false
	}
	if (r.Err == nil) != (s.Err == nil) {
		return false
	}
	if r.Err != nil && r.Err.Error() != s.Err.Error() {
		return false
	}
	return true
}

// Arg represents an exec argument (either input or output).
type Arg struct {
	// Out is true if this is an output argument.
	Out bool
	// Fileset is the fileset used as an input argument.
	Fileset *Fileset `json:",omitempty"`
	// Index is the output argument index.
	Index int
}

// ExecConfig contains all the necessary information to perform an
// exec.
type ExecConfig struct {
	// The type of exec: "exec", "intern", "extern"
	Type string

	// A human-readable name for the exec.
	Ident string

	// intern, extern: the URL from which data is fetched or to which
	// data is pushed.
	URL string

	// exec: the docker image used to perform an exec
	Image string

	// The docker image that is specified by the user
	OriginalImage string

	// exec: the Sprintf-able command that is to be run inside of the
	// Docker image.
	Cmd string

	// exec: the set of arguments (one per %s in Cmd) passed to the command
	// extern: the single argument which is to be exported
	Args []Arg

	// exec: the resource requirements for the exec
	Resources

	// NeedAWSCreds indicates the exec needs AWS credentials defined in
	// its environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
	// AWS_SESSION_TOKEN will be available with the user's default
	// credentials.
	NeedAWSCreds bool

	// NeedDockerAccess indicates that the exec needs access to the host docker daemon
	NeedDockerAccess bool

	// OutputIsDir tells whether an output argument (by index)
	// is a directory.
	OutputIsDir []bool `json:",omitempty"`
}

func (e ExecConfig) String() string {
	s := fmt.Sprintf("execconfig %s", e.Type)
	switch e.Type {
	case "intern", "extern":
		s += fmt.Sprintf(" url %s", e.URL)
	case "exec":
		args := make([]string, len(e.Args))
		for i, a := range e.Args {
			if a.Out {
				args[i] = fmt.Sprintf("out[%d]", a.Index)
			} else {
				args[i] = a.Fileset.Short()
			}
		}
		s += fmt.Sprintf(" image %s cmd %q args [%s]", e.Image, e.Cmd, strings.Join(args, ", "))
	}
	s += fmt.Sprintf(" resources %s", e.Resources)
	return s
}

// Profile stores keyed statistical summaries (currently: mean, max, N).
type Profile map[string]struct {
	Max, Mean, Var float64
	N              int64
	First, Last    time.Time
}

func (p Profile) String() string {
	var keys []string
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for i, k := range keys {
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "%s: mean %v max %v N %v var %v", k, p[k].Mean, p[k].Max, p[k].N, p[k].Var)
	}
	return b.String()
}

// Gauges stores a set of named gauges.
type Gauges map[string]float64

// Snapshot returns a snapshot of the gauge values g.
func (g Gauges) Snapshot() Gauges {
	h := make(Gauges)
	for k, v := range g {
		h[k] = v
	}
	return h
}

// ExecInspect describes the current state of an Exec.
type ExecInspect struct {
	Created time.Time
	Config  ExecConfig
	State   string        // "created", "waiting", "running", .., "zombie"
	Status  string        // human readable status
	Error   *errors.Error `json:",omitempty"` // non-nil runtime on error
	Profile Profile

	// Gauges are used to export realtime exec stats. They are used only
	// while the Exec is in running state.
	Gauges Gauges
	// Commands running from top, for live inspection.
	Commands []string
	// Docker inspect output.
	Docker types.ContainerJSON
	// ExecError stores exec result errors.
	ExecError *errors.Error `json:",omitempty"`
}

// Runtime computes the exec's runtime based on Docker's timestamps.
func (e ExecInspect) Runtime() time.Duration {
	const dockerFmt = "2006-01-02T15:04:05.999999999Z"
	if e.Docker.ContainerJSONBase == nil || e.Docker.State == nil {
		return time.Duration(0)
	}
	state := e.Docker.State
	start, err := time.Parse(dockerFmt, state.StartedAt)
	if err != nil {
		return time.Duration(0)
	}
	end, err := time.Parse(dockerFmt, state.FinishedAt)
	if err != nil {
		return time.Duration(0)
	}
	diff := end.Sub(start)
	if diff < time.Duration(0) {
		diff = time.Duration(0)
	}
	return diff
}

// An Exec computes a Value. It is created from an ExecConfig; the
// Exec interface permits waiting on completion, and inspection of
// results as well as ongoing execution.
type Exec interface {
	// ID returns the digest of the exec. This is equivalent to the Digest of the value computed
	// by the Exec.
	ID() digest.Digest

	// URI names execs in a process-agnostic fashion.
	URI() string

	// Result returns the exec's result after it has been completed.
	Result(ctx context.Context) (Result, error)

	// Inspect inspects the exec. It can be called at any point in the Exec's lifetime.
	Inspect(ctx context.Context) (ExecInspect, error)

	// Wait awaits completion of the Exec.
	Wait(ctx context.Context) error

	// Logs returns the standard error and/or standard output of the Exec.
	// If it is called during execution, and if follow is true, it follows
	// the logs until completion of execution.
	// Completed Execs return the full set of available logs.
	Logs(ctx context.Context, stdout, stderr, follow bool) (io.ReadCloser, error)

	// Shell invokes /bin/bash inside an Exec. It can be invoked only when
	// the Exec is executing. r provides the shell input. The returned read
	// closer has the shell output. The caller has to close the read closer
	// once done.
	// TODO(pgopal) - Implement shell for zombie execs.
	Shell(ctx context.Context) (io.ReadWriteCloser, error)

	// Promote installs this exec's objects into the alloc's repository.
	// Promote assumes that the Exec is complete. i.e. Wait returned successfully.
	Promote(context.Context) error
}

// Executor manages Execs and their values.
type Executor interface {
	// Put creates a new Exec at id. It is idempotent.
	Put(ctx context.Context, id digest.Digest, exec ExecConfig) (Exec, error)

	// Get retrieves the Exec named id.
	Get(ctx context.Context, id digest.Digest) (Exec, error)

	// Remove deletes an Exec.
	Remove(ctx context.Context, id digest.Digest) error

	// Execs lists all Execs known to the Executor.
	Execs(ctx context.Context) ([]Exec, error)

	// Load fetches missing files into the executor's repository. Load fetches
	// resolved files from the specified backing repository and unresolved files
	// directly from the source. The resolved fileset is returned and is available
	// on the executor on successful return. The client has to explicitly unload the
	// files to free them.
	Load(ctx context.Context, repo *url.URL, fileset Fileset) (Fileset, error)

	// VerifyIntegrity verifies the integrity of the given set of files
	VerifyIntegrity(ctx context.Context, fileset Fileset) error

	// Unload the data from the executor's repository. Any use of the unloaded files
	// after the successful return of Unload is undefined.
	Unload(ctx context.Context, fileset Fileset) error

	// Resources indicates the total amount of resources available at the Executor.
	Resources() Resources

	// Repository returns the Repository associated with this Executor.
	Repository() Repository
}
