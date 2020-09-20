// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

//go:generate stringer -type=State

import (
	"bytes"
	"context"
	"crypto"
	_ "crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time" // This is imported for the sha256 implementation, which is always required for Reflow.

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/values"
)

// Universe is the global namespace for digest computation.
var Universe string

// Digester is the Digester used throughout reflow. We use a SHA256 digest.
var Digester = digest.Digester(crypto.SHA256)

// Config stores flow configuration information. Configs modulate
// Flow behavior.
type Config struct {
	// HashV1 should be set to true if the flow should use the legacy
	// "v1" hash algorithm.
	HashV1 bool
}

// Merge merges config d into config c.
func (c *Config) Merge(d Config) {
	c.HashV1 = c.HashV1 || d.HashV1
}

// IsZero tells whether this config stores any non-default config.
func (c Config) IsZero() bool {
	return c == Config{}
}

// String returns a summary of the configuration c.
func (c Config) String() string {
	if c.HashV1 {
		return "hashv1"
	}
	return "hashv2"
}

// Op is an enum representing operations that may be
// performed in a Flow.
type Op int

const (
	// Exec runs a command in a docker container on the inputs
	// represented by the Flow's dependencies.
	Exec Op = 1 + iota
	// Intern imports datasets from URLs.
	Intern
	// Extern exports values to URLs.
	Extern
	// Groupby applies a regular expression to group an input value.
	Groupby
	// Map applies a function (which returns a Flow) to each element
	// in the input.
	Map
	// Collect filters and rewrites values.
	Collect
	// Merge merges a set of flows.
	Merge
	// Val returns a value.
	Val
	// Pullup merges a set of results into one value.
	Pullup
	// K is a flow continuation.
	K
	// Coerce is a flow value coercion. (Errors allowed.)
	Coerce
	// Requirements modifies the flow's requirements.
	Requirements
	// Data evaluates to a literal (inline) piece of data.
	Data
	// Kctx is a flow continuation with access to the evaluator context.
	Kctx

	maxOp
)

var opStrings = [maxOp]string{
	0:            "BROKEN",
	Exec:         "exec",
	Intern:       "intern",
	Extern:       "extern",
	Groupby:      "groupby",
	Map:          "map",
	Collect:      "collect",
	Merge:        "merge",
	Val:          "val",
	Pullup:       "pullup",
	K:            "k",
	Coerce:       "coerce",
	Requirements: "requirements",
	Data:         "data",
	Kctx:         "kctx",
}

func (o Op) String() string {
	return opStrings[o]
}

// External returns whether the op requires external execution.
func (o Op) External() bool {
	switch o {
	case Exec, Intern, Extern:
		return true
	}
	return false
}

// State is an enum representing the state of a Flow node during
// evaluation.
type State int64

// State denotes a Flow node's state during evaluation. Flows
// begin their life in Init, where they remain until they are
// examined by the evaluator. The precise state transitions depend on
// the evaluation mode (whether it is evaluating bottom-up or
// top-down, and whether a cache is used), but generally follow the
// order in which they are laid out here.
const (
	// Init indicates that the flow is initialized but not evaluated
	Init State = iota

	// NeedLookup indicates that the evaluator should perform a
	// cache lookup on the flow node.
	NeedLookup
	// Lookup indicates that the evaluator is currently performing a
	// cache lookup of the flow node. After a successful cache lookup,
	// the node is transfered to Done, and the (cached) value is
	// attached to the flow node. The objects may not be transfered into
	// the evaluator's repository.
	Lookup

	// TODO indicates that the evaluator should consider the node
	// for evaluation once its dependencies are completed.
	TODO

	// NeedTransfer indicates that the evaluator should transfer all
	// objects needed for execution into the evaluator's repository.
	NeedTransfer
	// Transfer indicates that the evaluator is currently
	// transferring the flow's dependent objects from cache.
	Transfer

	// Ready indicates that the node is ready for evaluation and should
	// be scheduled by the evaluator. A node is ready only once all of its
	// dependent objects are available in the evaluator's repository.
	Ready

	// NeedSubmit indicates the task is ready to be submitted to the
	// scheduler.
	NeedSubmit

	// Running indicates that the node is currently being evaluated by
	// the evaluator.
	Running

	Execing

	// Done indicates that the node has completed evaluation.
	Done

	// Max is the number of flow states.
	Max
)

// Name returns the FlowStat's string name.
func (s State) Name() string {
	switch s {
	case Init:
		return "init"
	case NeedLookup:
		return "needlookup"
	case Lookup:
		return "lookup"
	case NeedTransfer:
		return "needtransfer"
	case Transfer:
		return "transfer"
	case TODO:
		return "todo"
	case Ready:
		return "ready"
	case Running:
		return "running"
	case Done:
		return "done"
	default:
		return "unknown"
	}
}

// ExecArg indexes arguments to dependencies.
type ExecArg struct {
	// Out tells whether this argument is an output argument.
	Out bool
	// Index is the dependency index represented by this argument.
	Index int
}

// KContext is the context provided to a continuation (Kctx).
type KContext interface {
	// Context is supplied context.
	context.Context
	// Repository returns the repository.
	Repository() reflow.Repository
}

// Flow defines an AST for data flows. It is a logical union of ops
// as defined by type Op. Child nodes witness computational
// dependencies and must therefore be evaluated before its parents.
type Flow struct {
	// The operation represented by this node. See Op
	// for definitions.
	Op Op

	// Parent is set when a node is Forked.
	Parent *Flow

	// Deps holds this Flow's data dependencies.
	Deps []*Flow

	// Config stores this Flow's config.
	Config Config

	Image   string                                 // OpExec
	Cmd     string                                 // OpExec
	URL     *url.URL                               // OpIntern, Extern
	Re      *regexp.Regexp                         // Groupby, Collect
	Repl    string                                 // Collect
	MapFunc func(*Flow) *Flow                      // Map, MapMerge
	MapFlow *Flow                                  // Map
	K       func(vs []values.T) *Flow              // K
	Kctx    func(ctx KContext, v []values.T) *Flow // Kctx
	Coerce  func(values.T) (values.T, error)       // Coerce

	// ArgMap maps exec arguments to dependencies. (OpExec).
	Argmap []ExecArg
	// OutputIsDir tells whether the output i is a directory.
	OutputIsDir []bool

	// Original fields if this Flow was rewritten with canonical values.
	OriginalImage string

	// Argstrs stores a symbolic argument name, used for pretty printing
	// and debugging.
	Argstrs []string

	// FlowDigest stores, for Val, K and Coerce, a digest representing
	// just the operation or value.
	FlowDigest digest.Digest

	// ExtraDigest is considered as an additional digestible material of this flow
	// and included in the the flow's logical and physical digest computation.
	ExtraDigest digest.Digest

	// A human-readable identifier for the node, for use in
	// debugging output, etc.
	Ident string

	// Source code position of this node.
	Position string

	// The following are used during evaluation.

	// State stores the evaluation state of the node; see State
	// for details.
	State State

	// Resources indicates the expected resource usage of this node.
	// Currently it is only defined for OpExec.
	Resources reflow.Resources

	// Reserved stores the amount of resources that have been reserved
	// on behalf of this node.
	Reserved reflow.Resources

	// FlowRequirements stores the requirements indicated by
	// Requirements.
	FlowRequirements reflow.Requirements

	// Value stores the Value to which the node was evaluated.
	Value values.T
	// Err stores any evaluation error that occurred during flow evaluation.
	Err *errors.Error

	// The total runtime for evaluating this node.
	Runtime time.Duration

	// The current owning executor of this Flow.
	Owner reflow.Executor

	// The exec working on this node.
	Exec reflow.Exec

	// The exec id assigned to this node which is used to submit the flow to an executor.
	// This is different from the flow's digest, should be set only by the evaluator
	// because it encompasses assertions of the flow's dependencies (See Eval.assignExecId).
	// TODO(dnicolaou): Remove ExecId from scheduler mode once ExecId no longer required by scheduler
	// tests in eval_test.go.
	ExecId digest.Digest

	// Cached stores whether the flow was retrieved from cache.
	Cached bool

	// The amount of data to be transferred.
	TransferSize data.Size

	// Inspect stores an exec's inspect output.
	Inspect reflow.ExecInspect

	// TaskID is the identifier used to store this flow's Task representation in TaskDB.
	// This is only used for non-scheduler mode because in scheduler mode,
	// the corresponding task object will contain the relevant identifier.
	// TODO(dnicolaou): Remove TaskID once nonscheduler mode is removed.
	TaskID taskdb.TaskID

	Tracked bool

	Status *status.Task
	// StatusAux is the flow's auxilliary status string, printed
	// alongside the task's status.
	StatusAux string

	Data []byte // Data

	// MustIntern is set to true if an OpIntern must be
	// fully interned and cannot be pre-resolved.
	MustIntern bool

	// Dirty is used by the evaluator to track which nodes are dirtied
	// by this node: once the node has been evaluated, these flows
	// may be eligible for evaluation.
	Dirty []*Flow

	// Pending maintains a map of this node's dependent nodes that
	// are pending evaluation. It is maintained by the evaluator to trigger
	// evaluation.
	Pending map[*Flow]bool

	// NonDeterministic, in the case of Execs, denotes if the exec is non-deterministic.
	NonDeterministic bool

	// ExecDepIncorrectCacheKeyBug is set for nodes that are known to be impacted by a bug
	// which causes the cache keys to be incorrectly computed.
	// See https://github.com/grailbio/reflow/pull/128 or T41260.
	ExecDepIncorrectCacheKeyBug bool

	digestOnce sync.Once
	digest     digest.Digest
}

// Copy performs a shallow copy of the Flow.
func (f *Flow) Copy() *Flow {
	c := *f
	c.digestOnce = sync.Once{}
	c.Deps = make([]*Flow, len(f.Deps))
	for i := range f.Deps {
		c.Deps[i] = f.Deps[i]
	}
	return &c
}

// MapInit initializes Flow.MapFlow from the supplied MapFunc.
func (f *Flow) MapInit() {
	f.MapFlow = f.MapFunc(&Flow{Op: Val, Value: values.T(reflow.Fileset{})})
}

// Label labels this flow with ident. It then recursively
// labels its ancestors. Labeling stops when a node is
// already labeled.
func (f *Flow) Label(ident string) {
	if f.Ident != "" {
		return
	}
	f.Ident = ident
	for _, dep := range f.Deps {
		dep.Label(ident)
	}
}

// Requirements computes the minimum and maximum resource
// requirements for this flow. It currently assumes that the width of
// any map operation is infinite.
//
// BUG(marius): Requirements cannot accurately determine disk space
// requirements as they depend on object liveness. They are currently
// treated as entirely ephemeral.
//
// TODO(marius): include width hints on map nodes
//
// TODO(marius): account for static parallelism too.
func (f *Flow) Requirements() (req reflow.Requirements) {
	return f.requirements(make(map[*Flow]reflow.Requirements))
}

// ExecReset resets all flow parameters related to running
// a single exec.
func (f *Flow) ExecReset() {
	// TODO(dnicolaou): Remove ExecId reset once it is no longer required
	// for scheduler mode unit tests in eval_test.go.
	f.ExecId = digest.Digest{}
	f.Exec = nil
	f.Err = nil
	f.Inspect = reflow.ExecInspect{}
}

func (f *Flow) requirements(m map[*Flow]reflow.Requirements) (req reflow.Requirements) {
	if r, ok := m[f]; ok {
		return r
	}
	for _, ff := range f.Deps {
		req.Add(ff.requirements(m))
	}
	switch f.Op {
	case Map:
		req.Add(f.MapFlow.requirements(m))
		req.Width = 1 // We set it to wide; we can't assume how wide.
	case Exec:
		req.AddSerial(f.Resources)
	case Requirements:
		req.Add(f.FlowRequirements)
	}
	m[f] = req
	return
}

// Fork creates a new fork of this flow. The current version of Flow f
// becomes the parent flow.
func (f *Flow) Fork(flow *Flow) {
	// This must be set first.
	f.Parent = f.Copy()

	f.Op = flow.Op
	f.Deps = flow.Deps
	f.Image = flow.Image
	f.Cmd = flow.Cmd
	f.URL = flow.URL
	f.Re = flow.Re
	f.Repl = flow.Repl
	f.MapFunc = flow.MapFunc
	f.Ident = flow.Ident
	f.Resources = flow.Resources
	f.Value = flow.Value
	f.K = flow.K
	f.Kctx = flow.Kctx
	f.Argmap = flow.Argmap
	f.Coerce = flow.Coerce
	f.OutputIsDir = flow.OutputIsDir
	f.Err = flow.Err
}

// Strings returns a shallow and human readable string representation of the flow.
func (f *Flow) String() string {
	s := fmt.Sprintf("flow %s state %s %s %s", f.Digest().Short(), f.State, f.Resources, f.Op)
	if f.ExecDepIncorrectCacheKeyBug {
		s += " (affected by bug T41260)"
	}
	switch f.Op {
	case Exec:
		s += fmt.Sprintf(" image %s cmd %q", f.Image, f.Cmd)
	case Intern:
		s += fmt.Sprintf(" url %q", f.URL)
	case Extern:
		s += fmt.Sprintf(" url %q", f.URL)
	case Groupby:
		s += fmt.Sprintf(" re %s", f.Re.String())
	case Collect:
		s += fmt.Sprintf(" re %s repl %s", f.Re.String(), f.Repl)
	case Val:
		s += fmt.Sprintf(" val %s", f.Value)
	case Data:
		s += fmt.Sprintf(" data %s", Digester.FromBytes(f.Data))
	}
	if len(f.Deps) != 0 {
		deps := make([]string, len(f.Deps))
		for i := range f.Deps {
			deps[i] = f.Deps[i].Digest().Short()
		}
		s += fmt.Sprintf(" deps %s", strings.Join(deps, ","))
	}
	return s
}

// DebugString returns a human readable representation of the flow appropriate
// for debugging.
func (f *Flow) DebugString() string {
	dstr := f.Digest().Short()
	for _, d := range f.physicalDigests() {
		dstr += "/" + d.Short()
	}
	b := new(bytes.Buffer)
	switch f.Op {
	case Exec:
		fmt.Fprintf(b, "exec<%s>(image(%s), resources(%s), cmd(%q)", dstr, f.Image, f.Resources, f.Cmd)
		if f.Argmap != nil {
			args := make([]string, len(f.Argmap))
			for i, arg := range f.Argmap {
				if arg.Out {
					args[i] = fmt.Sprintf("out(%d)", arg.Index)
				} else {
					args[i] = fmt.Sprintf("in(%d)", arg.Index)
				}
			}
			fmt.Fprintf(b, "args(%s)", strings.Join(args, ", "))
		}
	case Intern:
		fmt.Fprintf(b, "intern<%s>(%q", dstr, f.URL)
	case Extern:
		fmt.Fprintf(b, "extern<%s>(%q", dstr, f.URL)
	case Groupby:
		fmt.Fprintf(b, "groupby<%s>(re(%s)", dstr, f.Re)
	case Map:
		fmt.Fprintf(b, "map<%s>(", dstr)
	case Collect:
		fmt.Fprintf(b, "collect<%s>(re(%s), repl(%s)", dstr, f.Re, f.Repl)
	case Merge:
		fmt.Fprintf(b, "merge<%s>(", dstr)
	case Val:
		fmt.Fprintf(b, "val<%s>(val(%v)", dstr, f.Value)
	case Pullup:
		fmt.Fprintf(b, "pullup<%s>(", dstr)
	case K:
		fmt.Fprintf(b, "k<%s>(", dstr)
	case Kctx:
		fmt.Fprintf(b, "k1<%s>(", dstr)
	case Coerce:
		fmt.Fprintf(b, "coerce<%s>(", dstr)
	case Requirements:
		fmt.Fprintf(b, "requirements<%s>(min(%s), width(%d)",
			dstr, f.FlowRequirements.Min, f.FlowRequirements.Width)
	case Data:
		fmt.Fprintf(b, "data<%s>(%s)", dstr, Digester.FromBytes(f.Data))
	}
	if len(f.Deps) != 0 {
		deps := make([]string, len(f.Deps))
		for i := range f.Deps {
			deps[i] = f.Deps[i].DebugString()
		}
		fmt.Fprintf(b, "deps(%s)", strings.Join(deps, ","))
	}
	b.WriteString(")")
	return b.String()
}

// ExecString renders a string representing the operation performed
// by this node. Cache should be set to true if the result was
// retrieved from cache; in this case, values from dependencies are
// not rendered in this case since they may not be available.  The
// returned string has the following format:
//
//	how:digest(ident) shortvalue = execstring (runtime transfer rate)
//
// where "execstring" is a string indicating the operation performed.
//
// For example, the ExecString of a node that interns a directory of
// FASTQs looks like this:
//
// 	ecae46a4(inputfastq) val<161216_E00472_0063_AH7LWNALXX/CNVS-LUAD-120587007-cfDNA-WGS-Rep1_S1_L001_R1_001.fastq.gz=87f7ca18, ...492.45GB> = intern "s3://grail-avalon/samples/CNVS-LUAD-120587007-cfDNA-WGS-Rep1/fastq/" (1h29m33.908587144s 93.83MB/s)
func (f *Flow) ExecString(cache bool) string {
	how := "eval"
	switch {
	case f.Err != nil:
		how = "error"
	case cache:
		how = "cache"
	}
	var s string
	if cache {
		switch f.Op {
		case Exec:
			argv := make([]interface{}, f.NExecArg())
			for i := range argv {
				earg := f.ExecArg(i)
				if earg.Out {
					argv[i] = fmt.Sprintf("<out(%d)>", earg.Index)
				} else {
					argv[i] = "<in(" + f.Deps[earg.Index].Digest().Short() + ")>"
				}
			}
			s = fmt.Sprintf("exec %q", fmt.Sprintf(f.Cmd, argv...))
		case Intern:
			s = fmt.Sprintf("intern %q", f.URL)
		case Extern:
			s = fmt.Sprintf("extern flow(%v) %q", f.Deps[0].Digest().Short(), f.URL)
		case Groupby:
			s = fmt.Sprintf("groupby %q flow(%v) ", f.Re.String(), f.Deps[0].Digest().Short())
		case Map:
			s = fmt.Sprintf("map <opaque> flow(%v)", f.Deps[0].Digest().Short())
		case Collect:
			s = fmt.Sprintf("collect %q %s flow(%s)", f.Re, f.Repl, f.Deps[0].Digest().Short())
		case Merge:
			argv := make([]string, len(f.Deps))
			for i, dep := range f.Deps {
				argv[i] = "flow(" + dep.Digest().Short() + ")"
			}
			s = "merge " + strings.Join(argv, " ")
		case Val:
			if fs, ok := f.Value.(reflow.Fileset); ok {
				s = fmt.Sprintf("val %v", fs.Short())
			} else {
				s = "val ?"
			}
		case Pullup:
			args := make([]string, len(f.Deps))
			for i, dep := range f.Deps {
				args[i] = dep.Digest().Short()
			}
			s = "pullup " + strings.Join(args, " ")
		case Data:
			s = fmt.Sprintf("data %s", Digester.FromBytes(f.Data))
		}
	} else {
		switch f.Op {
		case Exec:
			argv := make([]interface{}, f.NExecArg())
			for i := range argv {
				earg := f.ExecArg(i)
				if earg.Out {
					argv[i] = fmt.Sprintf("<out(%d)>", earg.Index)
				} else {
					if fs, ok := f.Deps[earg.Index].Value.(reflow.Fileset); ok {
						argv[i] = "<in(" + fs.Short() + ")>"
					} else {
						argv[i] = "<in(?)>"
					}
				}
			}
			s = fmt.Sprintf("exec %q", fmt.Sprintf(f.Cmd, argv...))
		case Intern:
			s = fmt.Sprintf("intern %q", f.URL)
		case Extern:
			if fs, ok := f.Deps[0].Value.(reflow.Fileset); ok {
				s = fmt.Sprintf("extern %v %q", fs.Short(), f.URL)
			} else {
				s = fmt.Sprintf("extern ? %q", f.URL)
			}
		case Groupby:
			if fs, ok := f.Deps[0].Value.(reflow.Fileset); ok {
				s = fmt.Sprintf("groupby %q %v ", f.Re.String(), fs.Short())
			} else {
				s = fmt.Sprintf("groupby %q ?", f.Re.String())
			}
		case Map:
			if fs, ok := f.Deps[0].Value.(reflow.Fileset); ok {
				s = fmt.Sprintf("map <opaque> %v", fs.Short())
			} else {
				s = "map <opaque> ?"
			}
		case Collect:
			if fs, ok := f.Deps[0].Value.(reflow.Fileset); ok {
				s = fmt.Sprintf("collect %q %s %s", f.Re, f.Repl, fs.Short())
			} else {
				s = fmt.Sprintf("collect %q %s ?", f.Re, f.Repl)
			}
		case Merge:
			if fs, ok := f.Value.(reflow.Fileset); ok {
				argv := make([]string, len(fs.List))
				for i := range fs.List {
					argv[i] = fs.List[i].Short()
				}
				s = "merge " + strings.Join(argv, " ")
			} else {
				s = "merge ?"
			}
		case Val:
			if fs, ok := f.Value.(reflow.Fileset); ok {
				s = fmt.Sprintf("val %v", fs.Short())
			} else {
				s = "val ?"
			}
		case Pullup:
			args := make([]string, len(f.Deps))
			for i, dep := range f.Deps {
				if fs, ok := dep.Value.(reflow.Fileset); ok {
					args[i] = fs.Short()
				} else {
					args[i] = "?"
				}
			}
			s = "pullup " + strings.Join(args, " ")
		case Data:
			s = fmt.Sprintf("data %s", Digester.FromBytes(f.Data))
		}
	}
	var rate string
	if v, ok := f.Value.(reflow.Fileset); ok {
		if sec := f.Runtime.Seconds(); sec > 0 {
			bps := v.Size() / int64(math.Ceil(sec))
			rate = " " + data.Size(bps).String() + "/s"
		}
	}
	short := "?"
	if fs, ok := f.Value.(reflow.Fileset); ok {
		short = fs.Short()
	}
	return fmt.Sprintf("%s:%s(%s) %s = %s (%s%s)", how, f.Digest().Short(), f.Ident, short, s, round(f.Runtime), rate)
}

// NExecArg returns the number of exec arguments of this node. If
// f.Argmap is defined, it returns the length of the argument map, or
// else the number of dependencies.
func (f *Flow) NExecArg() int {
	if f.Argmap != nil {
		return len(f.Argmap)
	}
	return len(f.Deps)
}

// ExecArg returns the ith ExecArg. It is drawn from f.Argmap if it
// is defined, or else it just the i'th input argument.
//
// ExecArg panics if i >= f.NExecArg().
func (f *Flow) ExecArg(i int) ExecArg {
	if f.Argmap != nil {
		return f.Argmap[i]
	} else if i < len(f.Deps) && i >= 0 {
		return ExecArg{Index: i}
	} else {
		panic("execarg out of bounds")
	}
}

// setArgmap defines f.Argmap if it isn't already.
func (f *Flow) setArgmap() {
	if f.Argmap == nil {
		f.Argmap = make([]ExecArg, len(f.Deps))
		for i := range f.Deps {
			f.Argmap[i] = ExecArg{Index: i}
		}
	}
}

// ExecConfig returns the flow's exec configuration. The flows dependencies
// must already be computed before invoking ExecConfig. ExecConfig is valid
// only for Intern, Extern, and Exec ops.
func (f *Flow) ExecConfig() reflow.ExecConfig {
	switch f.Op {
	case Intern:
		return reflow.ExecConfig{
			Type:  "intern",
			Ident: f.Ident,
			URL:   f.URL.String(),
		}
	case Extern:
		fs := f.Deps[0].Value.(reflow.Fileset)
		return reflow.ExecConfig{
			Type:  "extern",
			Ident: f.Ident,
			URL:   f.URL.String(),
			Args:  []reflow.Arg{{Fileset: &fs}},
		}
	case Exec:
		f.setArgmap()
		args := make([]reflow.Arg, f.NExecArg())
		for i := range args {
			earg := f.ExecArg(i)
			if earg.Out {
				args[i].Out = true
				args[i].Index = earg.Index
			} else {
				fs := f.Deps[earg.Index].Value.(reflow.Fileset)
				args[i].Fileset = &fs
			}
		}

		image, aws, docker := ImageQualifiers(f.Image)
		_, aws2, docker2 := ImageQualifiers(f.OriginalImage)
		// Make a copy of all slices and maps passed to the
		// ExecConfig. This will prevent flow properties
		// from being unsafely modified if the ExecConfig is
		// manually changed, like it is after resource Prediction.
		var (
			reserved    = make(reflow.Resources)
			outputIsDir = make([]bool, len(f.OutputIsDir))
		)
		reserved.Set(f.Reserved)
		copy(outputIsDir, f.OutputIsDir)
		return reflow.ExecConfig{
			Type:             "exec",
			Ident:            f.Ident,
			Image:            image,
			OriginalImage:    f.OriginalImage,
			NeedAWSCreds:     aws || aws2,
			NeedDockerAccess: docker || docker2,
			Cmd:              f.Cmd,
			Args:             args,
			Resources:        reserved,
			OutputIsDir:      outputIsDir,
		}
	default:
		panic("no exec config for op " + f.Op.String())
	}
}

// depAssertions returns the assertions of this flow's dependencies.
// The flows dependencies must already be computed before invoking depAssertions.
// depAssertions is valid only for Extern, and Exec ops.
func (f *Flow) depAssertions() []*reflow.Assertions {
	var depAs []*reflow.Assertions
	switch f.Op {
	case Extern:
		if f.Deps[0].Value == nil {
			break
		}
		depAs = f.Deps[0].Value.(reflow.Fileset).Assertions()
	case Exec:
		f.setArgmap()
		for i := 0; i < f.NExecArg(); i++ {
			earg := f.ExecArg(i)
			if earg.Out {
				continue
			}
			if f.Deps[earg.Index].Value == nil {
				continue
			}
			depAs = append(depAs, f.Deps[earg.Index].Value.(reflow.Fileset).Assertions()...)
		}
	}
	return depAs
}

// Digest produces a digest of Flow f. The digest captures the
// entirety of the Flows semantics: two flows with the same digest
// must evaluate to the same value. Map Flows are canonicalized
// by passing a no-op Flow to its MapFunc.
func (f *Flow) Digest() digest.Digest {
	// This could be much more efficient if we composed Digest()
	// instead of WriteDigest.
	f.digestOnce.Do(f.computeDigest)
	return f.digest
}

func (f *Flow) computeDigest() {
	w := Digester.NewWriter()
	f.WriteDigest(w)
	f.digest = w.Digest()
}

func must(n int, err error) {
	if err != nil {
		panic(err)
	}
}

// WriteDigest writes the digestible material of f to w. The
// io.Writer is assumed to be produced by a Digester, and hence
// infallible. Errors are not checked.
func (f *Flow) WriteDigest(w io.Writer) {
	if Universe != "" {
		io.WriteString(w, Universe)
	}
	if f.Op == Requirements {
		f.Deps[0].WriteDigest(w)
		return
	}
	if p := f.Parent; p != nil {
		p.WriteDigest(w)
		return
	}
	// TODO(marius): write these in a canonical order. Requires care to
	// re-map the argmap, and account for legacy execs. This cannot be
	// done for Ks, since these rely on argument ordering; but even
	// here it could be remapped if the mapping were carried as extra
	// metadata.
	for _, dep := range f.Deps {
		if f.Config.HashV1 {
			dep.WriteDigest(w)
		} else {
			must(digest.WriteDigest(w, dep.Digest()))
		}
	}

	io.WriteString(w, f.Op.DigestString())
	switch f.Op {
	case Intern, Extern:
		io.WriteString(w, f.URL.String())
	case Exec:
		io.WriteString(w, f.Image)
		io.WriteString(w, f.Cmd)
		for _, arg := range f.Argmap {
			if arg.Out {
				writeN(w, -arg.Index)
			} else {
				writeN(w, arg.Index)
			}
		}
	case Groupby:
		io.WriteString(w, f.Re.String())
	case Map:
		f.MapFlow.WriteDigest(w)
	case Collect:
		io.WriteString(w, f.Re.String())
		io.WriteString(w, f.Repl)
	case Val:
		if f.Err != nil {
			// Scramble the digest in the case of an error. While we don't
			// cache errors, this is needed so that we can distinguish between
			// empty Vals and erroneous ones while canonicalizing flow
			// nodes.
			//
			// If we do decide to cache errors, we'll need to digest the error
			// itself.
			digest.WriteDigest(w, Digester.Rand(nil))
			return
		}
		if v, ok := f.Value.(reflow.Fileset); ok {
			v.WriteDigest(w)
		} else {
			if f.FlowDigest.IsZero() {
				panic("invalid flow digest")
			}
			digest.WriteDigest(w, f.FlowDigest)
		}
	case K, Kctx, Coerce:
		if f.FlowDigest.IsZero() {
			panic("invalid flow digest")
		}
		digest.WriteDigest(w, f.FlowDigest)
	case Pullup:
	case Data:
		w.Write(f.Data)
	}
	if !f.ExtraDigest.IsZero() {
		digest.WriteDigest(w, f.ExtraDigest)
	}
}

// PhysicalDigest returns the digest for this node substituting the
// image name in the node with the provided one, if an exec node.
func (f *Flow) physicalDigest(image string) digest.Digest {
	w := Digester.NewWriter()
	for _, dep := range f.Deps {
		dep.Value.(reflow.Fileset).WriteDigest(w)
	}
	switch f.Op {
	case Extern:
		io.WriteString(w, f.URL.String())
	case Exec:
		io.WriteString(w, image)
		io.WriteString(w, f.Cmd)
		f.setArgmap()
		for _, arg := range f.Argmap {
			if arg.Out {
				writeN(w, -arg.Index)
			} else {
				writeN(w, arg.Index)
			}
		}
	}
	if !f.ExtraDigest.IsZero() {
		digest.WriteDigest(w, f.ExtraDigest)
	}
	return w.Digest()
}

// physicalDigests computes the physical digests of the Flow f,
// reflecting the actual underlying operation to be performed, and
// not the logical one. If there are multiple representations of
// the underlying operation, then multiple digests are returned, in
// the order of most concrete to least concrete.
//
// If physicalDigests is called on nodes whose dependencies
// are not fully resolved (i.e., state Done, contains a Fileset
// value), or on nodes not of type OpExec, or OpExtern, a nil
// slice is returned. This is because the physical input values
// must be available to compute the digest.
func (f *Flow) physicalDigests() []digest.Digest {
	switch f.Op {
	case Extern, Exec:
	default:
		return nil
	}

	for _, dep := range f.Deps {
		if dep.State != Done || dep.Err != nil {
			return nil
		}
	}

	digests := make([]digest.Digest, 1, 2)
	switch f.Op {
	case Extern:
		digests[0] = f.physicalDigest("")
	case Exec:
		digests[0] = f.physicalDigest(f.Image)
		if f.OriginalImage != "" && f.OriginalImage != f.Image {
			digests = append(digests, f.physicalDigest(f.OriginalImage))
		}
	}

	return digests
}

// CacheKeys returns all the valid cache keys for this flow node.
// They are returned in order from most concrete to least concrete.
func (f *Flow) CacheKeys() []digest.Digest {
	return append(f.physicalDigests(), f.Digest())
}

// Visitor returns a new FlowVisitor rooted at this node.
func (f *Flow) Visitor() *FlowVisitor {
	v := &FlowVisitor{}
	v.Push(f)
	return v
}

// Canonicalize returns a canonical version of Flow f, where
// semantically equivalent flows (as per Flow.Digest) are collapsed
// into one.
func (f *Flow) Canonicalize(config Config) *Flow {
	return f.canonicalize(newFlowMap(), config)
}

func (f *Flow) canonicalize(m *flowMap, config Config) *Flow {
	if flow := m.Get(f); flow != nil {
		return flow
	}
	f = f.Copy()
	// f.Copy() resets f.digestOnce. This is needed in case the digest
	// needs to be recomputed after setting the flow's config.
	f.Config.Merge(config)
	for i := range f.Deps {
		f.Deps[i] = f.Deps[i].canonicalize(m, config)
	}
	if f.MapFunc != nil {
		orig := f.MapFunc
		f.MapFunc = func(flow *Flow) *Flow {
			return orig(flow.canonicalize(m, config)).canonicalize(m, config)
		}
		f.MapInit()
	}
	if f.K != nil {
		orig := f.K
		f.K = func(vs []values.T) *Flow {
			return orig(vs).canonicalize(m, config)
		}
	}
	return m.Put(f)
}

// FlowVisitor implements a convenient visitor for flow graphs.
type FlowVisitor struct {
	*Flow
	q    []*Flow
	seen flowOnce
}

// Reset resets the flow visitor state.
func (v *FlowVisitor) Reset() {
	v.seen.Forget()
	v.q = v.q[:0]
}

// Push pushes node f onto visitor stack.
func (v *FlowVisitor) Push(f *Flow) {
	if v.seen == nil {
		v.seen = make(flowOnce)
	}
	v.q = append(v.q, f)
}

// Visit pushes the current node's children on to the visitor stack,
// including both data and control dependencies.
func (v *FlowVisitor) Visit() {
	v.q = append(v.q, v.Flow.Deps...)
}

// Walk visits the next flow node on the stack. Walk returns false
// when it runs out of nodes to visit; it also  guarantees that each
// node is visited only once.
func (v *FlowVisitor) Walk() bool {
	for len(v.q) > 0 {
		v.Flow = v.q[len(v.q)-1]
		v.q = v.q[:len(v.q)-1]
		if v.Flow != nil && v.seen.Visit(v.Flow) {
			return true
		}
	}
	return false
}
// FlowMap is a map of flows where the hash is the flow's digest unless it is an intern.
// If an intern is set to MustIntern, then mustInternDigest is added to the hash. This
// is done so we don't canonicalize a flow where the file has to be fully resolved to a flow
// that can be completed only with a file reference.
type flowMap struct {
	sync.Mutex
	flows map[digest.Digest]*Flow
}

func newFlowMap() *flowMap {
	return &flowMap{flows: map[digest.Digest]*Flow{}}
}

var mustInternDigest = reflow.Digester.FromString("internMustInternDigest")

func (m *flowMap) flowDigest(flow *Flow) digest.Digest {
	d := flow.Digest()
	if flow.MustIntern {
		d.Mix(mustInternDigest)
	}
	return d
}

func (m *flowMap) Get(flow *Flow) *Flow {
	d := m.flowDigest(flow)
	m.Lock()
	f := m.flows[d]
	m.Unlock()
	return f
}

func (m *flowMap) Put(flow *Flow) *Flow {
	d := m.flowDigest(flow)
	m.Lock()
	defer m.Unlock()
	if f := m.flows[d]; f != nil {
		return f
	}
	m.flows[d] = flow
	return flow
}

// FlowOnce maintains a set of visited flows, so that each may
// be visited only once.
type flowOnce map[*Flow]struct{}

// Visit returns true the first time flow f is visited.
func (o flowOnce) Visit(f *Flow) bool {
	if _, ok := o[f]; ok {
		return false
	}
	o[f] = struct{}{}
	return true
}

// Forget removes all visited flows.
func (o flowOnce) Forget() {
	for f := range o {
		delete(o, f)
	}
}

func writeN(w io.Writer, n int) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(n))
	w.Write(b[:])
}

// AbbrevCmd returns the abbreviated command line for an exec flow.
func (f *Flow) AbbrevCmd() string {
	if f.Op != Exec {
		return ""
	}
	argv := make([]interface{}, len(f.Argstrs))
	for i := range f.Argstrs {
		argv[i] = f.Argstrs[i]
	}
	cmd := fmt.Sprintf(f.Cmd, argv...)
	// Special case: if we start with a command with an absolute path,
	// abbreviate to basename.
	cmd = strings.TrimSpace(cmd)
	cmd = trimpath(cmd)
	cmd = trimspace(cmd)
	cmd = abbrev(cmd, nabbrev)
	return fmt.Sprintf("%s %s", leftabbrev(f.Image, nabbrevImage), cmd)
}

// ImageQualifiers analyzes the given image for the presence of backdoor qualifiers,
// strips the image of them, and returns a boolean for each known qualifier if present.
func ImageQualifiers(image string) (img string, aws, docker bool) {
	img = image
	aws = strings.Contains(image, "$aws")
	docker = strings.Contains(image, "$docker")
	img = strings.ReplaceAll(img, "$aws", "")
	img = strings.ReplaceAll(img, "$docker", "")
	return
}
