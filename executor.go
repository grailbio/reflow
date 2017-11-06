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
	"sort"
	"strings"
	"time"

	"github.com/docker/engine-api/types"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/values"
)

// File represents a name-by-hash file.
type File struct {
	// The digest of the contents of the file.
	ID digest.Digest

	// The size of the file.
	Size int64
}

// Fileset is the result of an evaluated flow. Values may either be
// lists of values or Filesets. Filesets are a map of paths to Files.
type Fileset struct {
	List []Fileset       `json:",omitempty"`
	Map  map[string]File `json:"Fileset,omitempty"`
}

// Equal reports whether v is equal to w.
// Two values are equal when they produce the same digest.
func (v Fileset) Equal(w Fileset) bool {
	return v.Digest() == w.Digest()
}

// Empty tells whether this value is empty, that is, it contains
// no files.
func (v Fileset) Empty() bool {
	for _, fs := range v.List {
		if !fs.Empty() {
			return false
		}
	}
	return len(v.Map) == 0
}

// Flatten is a convenience function to flatten (shallowly) the value
// v, returning a list of Values. If the value is a list value, the
// list is returned; otherwise a unary list of the value v is
// returned.
func (v Fileset) Flatten() []Fileset {
	switch {
	case v.List != nil:
		return v.List
	default:
		return []Fileset{v}
	}
}

// Files returns the set of Files that comprise the value.
func (v Fileset) Files() []File {
	fs := map[File]bool{}
	v.files(fs)
	files := make([]File, len(fs))
	i := 0
	for f := range fs {
		files[i] = f
		i++
	}
	return files
}

// N returns the number of files (not necessarily unique) in this value.
func (v Fileset) N() int {
	var n int
	for _, v := range v.List {
		n += v.N()
	}
	n += len(v.Map)
	return n
}

// Size returns the total size of this value.
func (v Fileset) Size() int64 {
	var s int64
	for _, v := range v.List {
		s += v.Size()
	}
	for _, f := range v.Map {
		s += f.Size
	}
	return s
}

func (v Fileset) files(fs map[File]bool) {
	for i := range v.List {
		v.List[i].files(fs)
	}
	if v.Map != nil {
		for _, f := range v.Map {
			fs[f] = true
		}
	}
}

// Short returns a short, human-readable string representing the
// value. Its intended use if for pretty-printed output. In
// particular, hashes are abbreviated, and lists display only the
// first member, followed by ellipsis. For example, a list of values
// is printed as:
//	list<val<sample.fastq.gz=f2c59c40>, ...50MB>
func (v Fileset) Short() string {
	switch {
	case v.List != nil:
		s := "list<"
		if len(v.List) != 0 {
			s += v.List[0].Short()
			if len(v.List) > 1 {
				s += ", ..."
			} else {
				s += " "
			}
		}
		s += data.Size(v.Size()).String() + ">"
		return s
	case len(v.Map) == 0:
		return "val<>"
	default:
		paths := make([]string, len(v.Map))
		i := 0
		for path := range v.Map {
			paths[i] = path
			i++
		}
		sort.Strings(paths)
		path := paths[0]
		file := v.Map[path]
		s := fmt.Sprintf("val<%s=%s", path, file.ID.Short())
		if len(paths) > 1 {
			s += ", ..."
		} else {
			s += " "
		}
		s += data.Size(v.Size()).String() + ">"
		return s
	}
}

// String returns a full, human-readable string representing the value v.
// Unlike Short, string is fully descriptive: it contains the full digest and
// lists are complete. For example:
//	list<sample.fastq.gz=sha256:f2c59c40a1d71c0c2af12d38a2276d9df49073c08360d72320847efebc820160>,
//	  sample2.fastq.gz=sha256:59eb82c49448e349486b29540ad71f4ddd7f53e5a204d50997f054d05c939adb>>
func (v Fileset) String() string {
	switch {
	case v.List != nil:
		vals := make([]string, len(v.List))
		for i := range v.List {
			vals[i] = v.List[i].String()
		}
		return fmt.Sprintf("list<%s>", strings.Join(vals, ", "))
	case len(v.Map) == 0:
		return "void"
	default:
		// TODO(marius): should we include the bindings here?
		paths := make([]string, len(v.Map))
		i := 0
		for path := range v.Map {
			paths[i] = path
			i++
		}
		sort.Strings(paths)
		binds := make([]string, len(paths))
		for i, path := range paths {
			binds[i] = fmt.Sprintf("%s=%s", path, v.Map[path].ID)
		}
		return fmt.Sprintf("obj<%s>", strings.Join(binds, ", "))
	}
}

// Digest returns a digest representing the value. Digests preserve
// semantics: two values with the same digest are considered to be
// equivalent.
func (v Fileset) Digest() digest.Digest {
	w := Digester.NewWriter()
	v.WriteDigest(w)
	return w.Digest()
}

// WriteDigest writes the digestible material for v to w. The
// io.Writer is assumed to be produced by a Digester, and hence
// infallible. Errors are not checked.
func (v Fileset) WriteDigest(w io.Writer) {
	switch {
	case v.List != nil:
		for i := range v.List {
			v.List[i].WriteDigest(w)
		}
	default:
		paths := make([]string, len(v.Map))
		i := 0
		for path := range v.Map {
			paths[i] = path
			i++
		}
		sort.Strings(paths)
		for _, path := range paths {
			io.WriteString(w, path)
			digest.WriteDigest(w, v.Map[path].ID)
		}
	}
}

// Flow returns the Flow which evaluates to the constant Value v.
func (v Fileset) Flow() *Flow {
	return &Flow{Op: OpVal, Value: values.T(v), State: FlowDone}
}

// Pullup merges this value (tree) into a single toplevel fileset.
func (v Fileset) Pullup() Fileset {
	if v.List == nil {
		return v
	}
	p := Fileset{Map: map[string]File{}}
	v.pullup(p.Map)
	return p
}

func (v Fileset) pullup(m map[string]File) {
	for k, f := range v.Map {
		m[k] = f
	}
	for _, v := range v.List {
		v.pullup(m)
	}
}

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

// ExecConfig  contains all the necessary information to perform an
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

// Profile stores keyed statistical summaries (currently: mean, max).
type Profile map[string]struct{ Max, Mean float64 }

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
		fmt.Fprintf(&b, "%s: mean %v max %v", k, p[k].Mean, p[k].Max)
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

	Docker types.ContainerJSON // Docker inspect output.
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
	if end.Before(start) {
		end = time.Now()
	}
	return end.Sub(start)
}

// Resources describes a set of resources.
type Resources struct {
	Memory uint64
	CPU    uint16
	Disk   uint64
}

var (
	// MaxResources is the maximum value of Resources.
	MaxResources = Resources{Memory: math.MaxUint64, CPU: math.MaxUint16, Disk: math.MaxUint64}
	// MinResources is the minimum value of Resources.
	MinResources = Resources{Memory: 0, CPU: 0, Disk: 0}
)

// String renders a Resources.
func (r Resources) String() string {
	switch {
	case r == MaxResources:
		return "mem max cpu max disk max"
	case r == MinResources:
		return "mem min cpu min disk min"
	default:
		return fmt.Sprintf("mem %d cpu %d disk %d", r.Memory, r.CPU, r.Disk)
	}
}

// Available tells if s resources are available from r.
func (r Resources) Available(s Resources) bool {
	return r.Memory >= s.Memory && r.CPU >= s.CPU && r.Disk >= s.Disk
}

// Sub subtracts s resources from r.
func (r Resources) Sub(s Resources) Resources {
	t := Resources{}
	if r.Memory > s.Memory {
		t.Memory = r.Memory - s.Memory
	} else {
		t.Memory = 0
	}
	if r.CPU > s.CPU {
		t.CPU = r.CPU - s.CPU
	} else {
		t.CPU = 0
	}
	if r.Disk > s.Disk {
		t.Disk = r.Disk - s.Disk
	} else {
		t.Disk = 0
	}
	return t
}

// Add adds s resources to r.
func (r Resources) Add(s Resources) Resources {
	add := r
	if r.Memory > math.MaxUint64-s.Memory {
		add.Memory = math.MaxUint64
	} else {
		add.Memory += s.Memory
	}
	if r.CPU > math.MaxUint16-s.CPU {
		add.CPU = math.MaxUint16
	} else {
		add.CPU += s.CPU
	}
	if r.Disk > math.MaxUint64-s.Disk {
		add.Disk = math.MaxUint64
	} else {
		add.Disk += s.Disk
	}
	return add
}

// Min returns the minimum resources available from both
// r and s.
func (r Resources) Min(s Resources) Resources {
	min := s
	if r.Memory < s.Memory {
		min.Memory = r.Memory
	}
	if r.CPU < s.CPU {
		min.CPU = r.CPU
	}
	if r.Disk < s.Disk {
		min.Disk = r.Disk
	}
	return min
}

// Max returns the maximum resources availble from either
// r or s.
func (r Resources) Max(s Resources) Resources {
	max := r
	if r.Memory < s.Memory {
		max.Memory = s.Memory
	}
	if r.CPU < s.CPU {
		max.CPU = s.CPU
	}
	if r.Disk < s.Disk {
		max.Disk = s.Disk
	}
	return max
}

// Units tells how many units of size u must be provided
// to satisfy a resource requirement of r.
func (r Resources) Units(u Resources) int {
	var mem, cpu, disk float64
	if u.Memory > 0 {
		mem = float64(r.Memory) / float64(u.Memory)
	}
	if u.CPU > 0 {
		cpu = float64(r.CPU) / float64(u.CPU)
	}
	if u.Disk > 0 {
		disk = float64(r.Disk) / float64(u.Disk)
	}
	max := mem
	if max < cpu {
		max = cpu
	}
	if max < disk {
		max = disk
	}
	return int(math.Ceil(max))
}

// Scale returns a Resources where each resources dimension is scaled
// by the given factor.
func (r Resources) Scale(factor float64) Resources {
	return Resources{
		Memory: uint64(factor * float64(r.Memory)),
		CPU:    uint16(factor * float64(r.CPU)),
		Disk:   uint64(factor * float64(r.Disk)),
	}
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
	return math.Abs(float64(r.Memory)-float64(u.Memory))*memoryScaling +
		math.Abs(float64(r.CPU)-float64(u.CPU))*cpuScaling
}

// LessAny tells whether r contains fewer resources than s, along
// any dimension.
func (r Resources) LessAny(s Resources) bool {
	return r.Memory < s.Memory || r.CPU < s.CPU || r.Disk < s.Disk
}

// LessEqualAny tells whether r contains fewer resources or equal
// resources to s, along any dimension.
func (r Resources) LessEqualAny(s Resources) bool {
	return r.Memory <= s.Memory || r.CPU <= s.CPU || r.Disk <= s.Disk
}

// LessAll tells whether r contains fewer resources than s, along
// all dimensions.
func (r Resources) LessAll(s Resources) bool {
	return r.Memory < s.Memory && r.CPU < s.CPU && r.Disk < s.Disk
}

// LessEqualAll tells whether r contains fewer resources or equal
// resources to s, along all dimensions.
func (r Resources) LessEqualAll(s Resources) bool {
	return r.Memory <= s.Memory && r.CPU <= s.CPU && r.Disk <= s.Disk
}

// IsZeroAll tells whether Resources r is zero in all dimensions.
func (r Resources) IsZeroAll() bool {
	return r == Resources{}
}

// IsZeroAny tells whether Resources r is zero in any dimension.
func (r Resources) IsZeroAny() bool {
	return r.Memory == 0 || r.CPU == 0 || r.Disk == 0
}

// Requirements stores a range of resource requirements. Minimum
// requirements indicate the smallest acceptable unit of resources,
// while max is the maximum (total) amount of useful resources.
type Requirements struct {
	Min, Max Resources
}

// IsZero tells whether there are zero requirements.
func (r Requirements) IsZero() bool {
	return r.Min.IsZeroAll()
}

// Add returns r with s as added requirements. S is added to the max,
// and r's min is adjusted to account for all of s's resources. This
// way, Requirements reflects the smallest amount of resources that
// can fit any of the discrete resources added.
func (r Requirements) Add(s Resources) Requirements {
	return Requirements{
		Min: r.Min.Max(s),
		Max: r.Max.Add(s),
	}
}

// String renders a human-readable representation of r.
func (r Requirements) String() string {
	return fmt.Sprintf("min(%v) max(%v)", r.Min, r.Max)
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
	Promote(context.Context) error
}

// Executor manages Execs and their values.
type Executor interface {
	// Put creates a new Exec at id. It it idempotent.
	Put(ctx context.Context, id digest.Digest, exec ExecConfig) (Exec, error)

	// Get retrieves the Exec named id.
	Get(ctx context.Context, id digest.Digest) (Exec, error)

	// Remove deletes an Exec.
	Remove(ctx context.Context, id digest.Digest) error

	// Execs lists all Execs known to the Executor.
	Execs(ctx context.Context) ([]Exec, error)

	// Resources indicates the total amount of resources available at the Executor.
	Resources() Resources

	// Repository returns the Repository associated with this Executor.
	Repository() Repository
}
