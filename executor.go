// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"docker.io/go-docker/api/types"
	"github.com/grailbio/base/data"
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

// Resources describes a set of labeled resources. Each resource is
// described by a string label and assigned a value. The zero value
// of Resources represents the resources with zeros for all labels.
type Resources map[string]float64

// String renders a Resources. All nonzero-valued labels are included;
// mem, cpu, and disk are always included regardless of their value.
func (r Resources) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	r.writeResources(&b)
	b.WriteString("}")
	return b.String()
}

func (r Resources) writeResources(b *bytes.Buffer) {
	if r["mem"] != 0 || r["cpu"] != 0 || r["disk"] != 0 {
		fmt.Fprintf(b, "mem:%s cpu:%g disk:%s", data.Size(r["mem"]), r["cpu"], data.Size(r["disk"]))
	}
	var keys []string
	for key := range r {
		switch key {
		case "mem", "cpu", "disk":
		default:
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if r[key] == 0 {
			continue
		}
		fmt.Fprintf(b, " %s:%g", key, r[key])
	}
}

// Available tells if s resources are available from r.
func (r Resources) Available(s Resources) bool {
	for key := range s {
		if r[key] < s[key] {
			return false
		}
	}
	return true
}

// Sub sets r to the difference x[key]-y[key] for all keys and returns r.
func (r *Resources) Sub(x, y Resources) *Resources {
	r.Set(x)
	for key := range y {
		(*r)[key] = x[key] - y[key]
	}
	return r
}

// Add sets r to the sum x[key]+y[key] for all keys and returns r.
func (r *Resources) Add(x, y Resources) *Resources {
	r.Set(x)
	for key := range y {
		(*r)[key] += y[key]
	}
	return r
}

// Set sets r[key]=s[key] for all keys and returns r.
func (r *Resources) Set(s Resources) *Resources {
	*r = make(Resources)
	for key, val := range s {
		(*r)[key] = val
	}
	return r
}

// Min sets r to the minimum min(x[key], y[key]) for all keys
// and returns r.
func (r *Resources) Min(x, y Resources) *Resources {
	r.Set(x)
	for key, val := range y {
		if val < (*r)[key] {
			(*r)[key] = val
		}
	}
	return r
}

// Max sets r to the maximum max(x[key], y[key]) for all keys
// and returns r.
func (r *Resources) Max(x, y Resources) *Resources {
	r.Set(x)
	for key, val := range y {
		if val > (*r)[key] {
			(*r)[key] = val
		}
	}
	return r
}

// Scale sets r to the scaled resources s[key]*factor for all keys
// and returns r.
func (r *Resources) Scale(s Resources, factor float64) *Resources {
	if *r == nil {
		*r = make(Resources)
	}
	for key, val := range s {
		(*r)[key] = val * factor
	}
	return r
}

// ScaledDistance returns the distance between two resources computed as a sum
// of the differences in memory, cpu and disk with some predefined scaling.
func (r Resources) ScaledDistance(u Resources) float64 {
	// Consider 6G Memory and 1 CPU are somewhat the same cost
	// when we compute "distance" between the resources.
	// % reflow ec2instances | awk '{s += $2/$3; n++} END{print s/n}'
	// 5.98788
	const (
		G             = 1 << 30
		memoryScaling = 1.0 / (6 * G)
		cpuScaling    = 1
	)
	return math.Abs(float64(r["mem"])-float64(u["mem"]))*memoryScaling +
		math.Abs(float64(r["cpu"])-float64(u["cpu"]))*cpuScaling
}

// Equal tells whether the resources r and s are equal in all dimensions
// of both r and s.
func (r Resources) Equal(s Resources) bool {
	for key, val := range s {
		if r[key] != val {
			return false
		}
	}
	for key, val := range r {
		if s[key] != val {
			return false
		}
	}
	return true
}

// Requirements stores resource requirements, comprising the minimum
// amount of acceptable resources and a width.
type Requirements struct {
	// Min is the smallest amount of resources that must be allocated
	// to satisfy the requirements.
	Min Resources
	// Width is the width of the requirements. A width of zero indicates
	// a "narrow" job: minimum describes the exact resources needed.
	// Widths greater than zero are "wide" requests: they require some
	// multiple of the minimum requirement. The distinction between a
	// width of zero and a width of one is a little subtle: width
	// represents the smallest acceptable width, and thus a width of 1
	// can be taken as a hint to allocate a higher multiple of the
	// minimum requirements, whereas a width of 0 represents a precise
	// requirement: allocating any more is likely to be wasteful.
	Width int
}

// Wide returns whether these requirements represent a
// wide resource request.
func (r *Requirements) Wide() bool {
	return r.Width > 0
}

// AddParallel adds the provided resources s to the requirements,
// and also increases the requirement's width by one.
func (r *Requirements) AddParallel(s Resources) {
	r.Min.Max(r.Min, s)
	r.Width++
}

// AddSerial adds the provided resources s to the requirements.
func (r *Requirements) AddSerial(s Resources) {
	r.Min.Max(r.Min, s)
}

// Max is the maximum amount of resources represented by this
// resource request.
func (r *Requirements) Max() Resources {
	var max Resources
	max.Scale(r.Min, float64(1+r.Width))
	return max
}

// Add adds the provided requirements s to the requirements r.
// R's minimum requirements are set to the larger of the two;
// the two widths are added.
func (r *Requirements) Add(s Requirements) {
	r.Min.Max(r.Min, s.Min)
	r.Width += s.Width
}

// Equal reports whether r and s represent the same requirements.
func (r Requirements) Equal(s Requirements) bool {
	return r.Min.Equal(s.Min) && r.Width == s.Width
}

// String renders a human-readable representation of r.
func (r Requirements) String() string {
	s := r.Min.String()
	if r.Width > 1 {
		return s + fmt.Sprintf("#%d", r.Width)
	}
	return s
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

	// Unload the data from the executor's repository. Any use of the unloaded files
	// after the successful return of Unload is undefined.
	Unload(ctx context.Context, fileset Fileset) error

	// Resources indicates the total amount of resources available at the Executor.
	Resources() Resources

	// Repository returns the Repository associated with this Executor.
	Repository() Repository
}
