package reflow

//go:generate stringer -type=FlowState

import (
	"bytes"
	"crypto"
	"encoding/binary"
	"math"
	"time"
	// This is imported for the sha256 implementation, which is always required
	// for Reflow.
	_ "crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/values"
	"grail.com/lib/data"
	"grail.com/lib/digest"
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

// Op is an enum representing operations that may be
// performed in a Flow.
type Op int

const (
	// OpExec runs a command in a docker container on the inputs
	// represented by the Flow's dependencies.
	OpExec Op = 1 + iota
	// OpIntern imports datasets from URLs.
	OpIntern
	// OpExtern exports values to URLs.
	OpExtern
	// OpGroupby applies a regular expression to group an input value.
	OpGroupby
	// OpMap applies a function (which returns a Flow) to each element
	// in the input.
	OpMap
	// OpCollect filters and rewrites values.
	OpCollect
	// OpMerge merges a set of flows.
	OpMerge
	// OpVal returns a value.
	OpVal
	// OpPullup merges a set of results into one value.
	OpPullup
	// OpK is a flow continuation.
	OpK
	// OpCoerce is a flow value coercion. (Errors allowed.)
	OpCoerce
	// OpRequirements modifies the flow's requirements.
	OpRequirements
	// OpData evaluates to a literal (inline) piece of data.
	OpData

	maxOp
)

var opStrings = [maxOp]string{
	0:              "BROKEN",
	OpExec:         "exec",
	OpIntern:       "intern",
	OpExtern:       "extern",
	OpGroupby:      "groupby",
	OpMap:          "map",
	OpCollect:      "collect",
	OpMerge:        "merge",
	OpVal:          "val",
	OpPullup:       "pullup",
	OpK:            "k",
	OpCoerce:       "coerce",
	OpRequirements: "requirements",
	OpData:         "data",
}

func (o Op) String() string {
	return opStrings[o]
}

// FlowState is an enum representing the state of a Flow node during
// evaluation.
type FlowState int64

const (
	// FlowInit indicates that the flow is initialized but not evaluated
	FlowInit FlowState = iota

	// FlowNeedLookup indicates that the evaluator should perform a
	// cache lookup on the flow node.
	FlowNeedLookup
	// FlowLookup indicates that the evaluator is currently performing
	// a cache lookup of the flow node.
	FlowLookup

	// FlowNeedTransfer indicates that the evaluator should transfer the
	// flow's objects from cache.
	FlowNeedTransfer
	// FlowTransfer indicates that the evalutor is currently
	// transferring the flow's objects from cache.
	FlowTransfer

	// FlowTODO indicates that the evaluator should consider the node
	// for evaluation once its dependencies are completed.
	FlowTODO
	// FlowReady indicates that the node is ready for evaluation and should
	// be scheduled by the evaluator.
	FlowReady
	// FlowRunning indicates that the node is currently being evaluated by
	// the evaluator.
	FlowRunning

	// FlowDone indicates that the node has completed evaluation.
	FlowDone

	// FlowMax is the number of flow states.
	FlowMax
)

// Name returns the FlowStat's string name.
func (s FlowState) Name() string {
	switch s {
	case FlowInit:
		return "init"
	case FlowNeedLookup:
		return "needlookup"
	case FlowLookup:
		return "lookup"
	case FlowNeedTransfer:
		return "needtransfer"
	case FlowTransfer:
		return "transfer"
	case FlowTODO:
		return "todo"
	case FlowReady:
		return "ready"
	case FlowRunning:
		return "running"
	case FlowDone:
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

// Flow defines an AST for data flows. It is a logical union of ops
// as defined by type Op. Child nodes witness computational
// dependencies and must therefore be evaluated before its parents.
type Flow struct {
	// The operation represented by this node. See Op
	// for definitions.
	Op Op

	// Parent is set when a node is Forked.
	Parent *Flow

	// Deps holds this Flow's dependencies.
	Deps []*Flow

	// Config stores this Flow's config.
	Config Config

	Image   string                           // OpExec
	Cmd     string                           // OpExec
	URL     *url.URL                         // OpIntern, OpExtern
	Re      *regexp.Regexp                   // OpGroupby, OpCollect
	Repl    string                           // OpCollect
	MapFunc func(*Flow) *Flow                // OpMap, OpMapMerge
	MapFlow *Flow                            // OpMap
	K       func(vs []values.T) *Flow        // OpK
	Coerce  func(values.T) (values.T, error) // OpCoerce
	// ArgMap maps exec arguments to dependencies. (OpExec).
	Argmap []ExecArg
	// OutputIsDir tells whether the output i is a directory.
	OutputIsDir []bool

	// Argstrs stores a symbolic argument name, used for pretty printing
	// and debugging.
	Argstrs []string

	// FlowDigest stores, for OpVal and OpK, a digest representing
	// just the operation or value.
	FlowDigest digest.Digest

	// A human-readable identifier for the node, for use in
	// debugging output, etc.
	Ident string

	// Source code position of this node.
	Position string

	// The following are used during evaluation.

	// State stores the evaluation state of the node; see FlowState
	// for details.
	State FlowState

	// Resources indicates the expected resource usage of this node.
	// Currently it is only defined for OpExec.
	Resources Resources

	// Reserved stores the amount of resources that have been reserved
	// on behalf of this node.
	Reserved Resources

	// FlowRequirements stores the requirements indicated by
	// OpRequirements.
	FlowRequirements struct{ Min, Max Resources }

	// Value stores the Value to which the node was evaluated.
	Value values.T
	// Err stores any evaluation error that occured during flow evaluation.
	Err *errors.Error

	// The total runtime for evaluating this node.
	Runtime time.Duration

	// The current owning executor of this Flow.
	Owner Executor

	// The exec working on this node.
	Exec Exec

	// Cached stores whether the flow was retrieved from cache.
	Cached bool

	// Inspect stores an exec's inspect output.
	Inspect ExecInspect

	Tracked bool

	Data []byte // OpData

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
	f.MapFlow = f.MapFunc(&Flow{Op: OpVal, Value: values.T(Fileset{})})
}

// DeepCopy performs a deep copy of a Flow.
func (f *Flow) DeepCopy() *Flow {
	f = f.Copy()
	for i := range f.Deps {
		f.Deps[i] = f.Deps[i].DeepCopy()
	}
	if f.MapFlow != nil {
		f.MapFlow = f.MapFlow.DeepCopy()
	}
	return f
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
func (f *Flow) Requirements() (min, max Resources) {
	for _, ff := range f.Deps {
		ffmin, ffmax := ff.Requirements()
		min = min.Max(ffmin)
		max = max.Max(ffmax)
	}
	switch f.Op {
	case OpMap:
		mapmin, _ := f.MapFlow.Requirements()
		min = min.Max(mapmin)
		max = MaxResources
	case OpExec:
		min = min.Max(f.Resources)
		max = max.Max(f.Resources)
	case OpRequirements:
		min = min.Max(f.FlowRequirements.Min)
		max = max.Max(f.FlowRequirements.Max)
	}
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
	f.Argmap = flow.Argmap
	f.Coerce = flow.Coerce
	f.OutputIsDir = flow.OutputIsDir
	f.Err = flow.Err
}

// Strings returns a shallow and human readable string representation of the flow.
func (f *Flow) String() string {
	s := fmt.Sprintf("flow %s state %s %s %s", f.Digest().Short(), f.State, f.Resources, f.Op)
	switch f.Op {
	case OpExec:
		s += fmt.Sprintf(" image %s cmd %q", f.Image, f.Cmd)
	case OpIntern:
		s += fmt.Sprintf(" url %q", f.URL)
	case OpExtern:
		s += fmt.Sprintf(" url %q", f.URL)
	case OpGroupby:
		s += fmt.Sprintf(" re %s", f.Re.String())
	case OpCollect:
		s += fmt.Sprintf(" re %s repl %s", f.Re.String(), f.Repl)
	case OpVal:
		s += fmt.Sprintf(" val %s", f.Value)
	case OpData:
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
	b := new(bytes.Buffer)
	switch f.Op {
	case OpExec:
		fmt.Fprintf(b, "exec<%s>(image(%s), resources(%s), cmd(%q)", f.Digest().Short(), f.Image, f.Resources, f.Cmd)
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
	case OpIntern:
		fmt.Fprintf(b, "intern<%s>(%q", f.Digest().Short(), f.URL)
	case OpExtern:
		fmt.Fprintf(b, "extern<%s>(%q", f.Digest().Short(), f.URL)
	case OpGroupby:
		fmt.Fprintf(b, "groupby<%s>(re(%s)", f.Digest().Short(), f.Re)
	case OpMap:
		fmt.Fprintf(b, "map<%s>(", f.Digest().Short())
	case OpCollect:
		fmt.Fprintf(b, "collect<%s>(re(%s), repl(%s)", f.Digest().Short(), f.Re, f.Repl)
	case OpMerge:
		fmt.Fprintf(b, "merge<%s>(", f.Digest().Short())
	case OpVal:
		fmt.Fprintf(b, "val<%s>(val(%v)", f.Digest().Short(), f.Value)
	case OpPullup:
		fmt.Fprintf(b, "pullup<%s>(", f.Digest().Short())
	case OpK:
		fmt.Fprintf(b, "k<%s>(", f.Digest().Short())
	case OpCoerce:
		fmt.Fprintf(b, "coerce<%s>(", f.Digest().Short())
	case OpRequirements:
		fmt.Fprintf(b, "requirements<%s>(min(%s), max(%s)",
			f.Digest().Short(), f.FlowRequirements.Min, f.FlowRequirements.Max)
	case OpData:
		fmt.Fprintf(b, "data<%s>(%s)", f.Digest().Short(), Digester.FromBytes(f.Data))
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
		case OpExec:
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
		case OpIntern:
			s = fmt.Sprintf("intern %q", f.URL)
		case OpExtern:
			s = fmt.Sprintf("extern flow(%v) %q", f.Deps[0].Digest().Short(), f.URL)
		case OpGroupby:
			s = fmt.Sprintf("groupby %q flow(%v) ", f.Re.String(), f.Deps[0].Digest().Short())
		case OpMap:
			s = fmt.Sprintf("map <opaque> flow(%v)", f.Deps[0].Digest().Short())
		case OpCollect:
			s = fmt.Sprintf("collect %q %s flow(%s)", f.Re, f.Repl, f.Deps[0].Digest().Short())
		case OpMerge:
			argv := make([]string, len(f.Deps))
			for i, dep := range f.Deps {
				argv[i] = "flow(" + dep.Digest().Short() + ")"
			}
			s = "merge " + strings.Join(argv, " ")
		case OpVal:
			if fs, ok := f.Value.(Fileset); ok {
				s = fmt.Sprintf("val %v", fs.Short())
			} else {
				s = "val ?"
			}
		case OpPullup:
			args := make([]string, len(f.Deps))
			for i, dep := range f.Deps {
				args[i] = dep.Digest().Short()
			}
			s = "pullup " + strings.Join(args, " ")
		case OpData:
			s = fmt.Sprintf("data %s", Digester.FromBytes(f.Data))
		}
	} else {
		switch f.Op {
		case OpExec:
			argv := make([]interface{}, f.NExecArg())
			for i := range argv {
				earg := f.ExecArg(i)
				if earg.Out {
					argv[i] = fmt.Sprintf("<out(%d)>", earg.Index)
				} else {
					if fs, ok := f.Deps[earg.Index].Value.(Fileset); ok {
						argv[i] = "<in(" + fs.Short() + ")>"
					} else {
						argv[i] = "<in(?)>"
					}
				}
			}
			s = fmt.Sprintf("exec %q", fmt.Sprintf(f.Cmd, argv...))
		case OpIntern:
			s = fmt.Sprintf("intern %q", f.URL)
		case OpExtern:
			if fs, ok := f.Deps[0].Value.(Fileset); ok {
				s = fmt.Sprintf("extern %v %q", fs.Short(), f.URL)
			} else {
				s = fmt.Sprintf("extern ? %q", f.URL)
			}
		case OpGroupby:
			if fs, ok := f.Deps[0].Value.(Fileset); ok {
				s = fmt.Sprintf("groupby %q %v ", f.Re.String(), fs.Short())
			} else {
				s = fmt.Sprintf("groupby %q ?", f.Re.String())
			}
		case OpMap:
			if fs, ok := f.Deps[0].Value.(Fileset); ok {
				s = fmt.Sprintf("map <opaque> %v", fs.Short())
			} else {
				s = "map <opaque> ?"
			}
		case OpCollect:
			if fs, ok := f.Deps[0].Value.(Fileset); ok {
				s = fmt.Sprintf("collect %q %s %s", f.Re, f.Repl, fs.Short())
			} else {
				s = fmt.Sprintf("collect %q %s ?", f.Re, f.Repl)
			}
		case OpMerge:
			if fs, ok := f.Value.(Fileset); ok {
				argv := make([]string, len(fs.List))
				for i := range fs.List {
					argv[i] = fs.List[i].Short()
				}
				s = "merge " + strings.Join(argv, " ")
			} else {
				s = "merge ?"
			}
		case OpVal:
			if fs, ok := f.Value.(Fileset); ok {
				s = fmt.Sprintf("val %v", fs.Short())
			} else {
				s = "val ?"
			}
		case OpPullup:
			args := make([]string, len(f.Deps))
			for i, dep := range f.Deps {
				if fs, ok := dep.Value.(Fileset); ok {
					args[i] = fs.Short()
				} else {
					args[i] = "?"
				}
			}
			s = "pullup " + strings.Join(args, " ")
		case OpData:
			s = fmt.Sprintf("data %s", Digester.FromBytes(f.Data))
		}
	}
	var rate string
	if v, ok := f.Value.(Fileset); ok {
		if sec := f.Runtime.Seconds(); sec > 0 {
			bps := v.Size() / int64(math.Ceil(sec))
			rate = " " + data.Size(bps).String() + "/s"
		}
	}
	short := "?"
	if fs, ok := f.Value.(Fileset); ok {
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

// Digest produces a digest of Flow f. The digest captures the
// entirety of the Flows semantics: two flows with the same digest
// must evaluate to the same value. OpMap Flows are canonicalized
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
	if f.Op == OpRequirements {
		f.Deps[0].WriteDigest(w)
		return
	}
	if p := f.Parent; p != nil {
		p.WriteDigest(w)
		return
	}
	for _, dep := range f.Deps {
		if f.Config.HashV1 {
			dep.WriteDigest(w)
		} else {
			must(digest.WriteDigest(w, dep.Digest()))
		}
	}

	io.WriteString(w, f.Op.DigestString())
	switch f.Op {
	case OpIntern, OpExtern:
		io.WriteString(w, f.URL.String())
	case OpExec:
		io.WriteString(w, f.Image)
		io.WriteString(w, f.Cmd)
		for _, arg := range f.Argmap {
			if arg.Out {
				writeN(w, -arg.Index)
			} else {
				writeN(w, arg.Index)
			}
		}
	case OpGroupby:
		io.WriteString(w, f.Re.String())
	case OpMap:
		f.MapFlow.WriteDigest(w)
	case OpCollect:
		io.WriteString(w, f.Re.String())
		io.WriteString(w, f.Repl)
	case OpVal:
		if f.Err != nil {
			return
		}
		if v, ok := f.Value.(Fileset); ok {
			v.WriteDigest(w)
		} else {
			if f.FlowDigest.IsZero() {
				panic("invalid flow digest")
			}
			digest.WriteDigest(w, f.FlowDigest)
		}
	case OpK, OpCoerce:
		if f.FlowDigest.IsZero() {
			panic("invalid flow digest")
		}
		digest.WriteDigest(w, f.FlowDigest)
	case OpPullup:
	case OpData:
		w.Write(f.Data)
	}
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
	seen map[*Flow]bool
}

// Push pushes node f onto visitor stack.
func (v *FlowVisitor) Push(f *Flow) {
	if v.seen == nil {
		v.seen = map[*Flow]bool{}
	}
	v.q = append([]*Flow{f}, v.q...)
}

// Visit pushes the current node's children on to the visitor stack.
func (v *FlowVisitor) Visit() {
	v.q = append(v.q, v.Flow.Deps...)
}

// Walk visits the next flow node on the stack. Walk returns false
// when it runs out of nodes to visit; it also  guarantees that each
// node is visited only once.
func (v *FlowVisitor) Walk() bool {
	for len(v.q) > 0 {
		v.Flow, v.q = v.q[0], v.q[1:]
		if v.Flow == nil || v.seen[v.Flow] {
			continue
		}
		v.seen[v.Flow] = true
		return true
	}
	return false
}

type flowMap struct {
	sync.Mutex
	flows map[digest.Digest]*Flow
}

func newFlowMap() *flowMap {
	return &flowMap{flows: map[digest.Digest]*Flow{}}
}

func (m *flowMap) Get(flow *Flow) *Flow {
	d := flow.Digest()
	m.Lock()
	f := m.flows[d]
	m.Unlock()
	return f
}

func (m *flowMap) Put(flow *Flow) *Flow {
	d := flow.Digest()
	m.Lock()
	defer m.Unlock()
	if f := m.flows[d]; f != nil {
		return f
	}
	m.flows[d] = flow
	return flow
}

func writeN(w io.Writer, n int) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(n))
	w.Write(b[:])
}
