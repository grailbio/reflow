package runtime

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
)

const defaultFlowDir = "/tmp/flow"

type FlagName string

const (
	Unknown FlagName = "unknown"
	// CommonRunFlags flag names
	FlagNameAssert       FlagName = "assert"
	FlagNameEvalStrategy FlagName = "eval"
	FlagNameInvalidate   FlagName = "invalidate"
	// Deprecated: externs are no longer cached.
	// TODO(pfialho): remove flag.
	FlagNameNoCacheExtern   FlagName = "nocacheextern"
	FlagNamePostUseChecksum FlagName = "postusechecksum"
	FlagNameRecomputeEmpty  FlagName = "recomputeempty"
	// RunFlags flag names
	FlagNameBackgroundTimeout FlagName = "backgroundtimeout"
	FlagNameDotGraph          FlagName = "dotgraph"
	FlagNamePred              FlagName = "pred"
	FlagNameTrace             FlagName = "traceflow"
)

// CommonRunFlags are the run flags that are common across various run modes (run, batch, etc)
type CommonRunFlags struct {
	// Assert is the policy used to assert cached flow result compatibility. e.g. never, exact.
	Assert string
	// EvalStrategy is the evaluation strategy. Supported modes are "topdown" and "bottomup".
	EvalStrategy string
	// Invalidate is a regular expression for node identifiers that should be invalidated.
	Invalidate string
	// PostUseChecksum indicates whether input filesets are checksummed after use.
	PostUseChecksum bool
	// RecomputeEmpty indicates if cache results with empty filesets be automatically recomputed.
	RecomputeEmpty bool
}

// Flags adds the common run flags to the provided flagset.
func (r *CommonRunFlags) Flags(flags *flag.FlagSet) {
	r.flagsLimited(flags, "", nil)
}

// flagsLimited adds flags to the provided flagset with the given prefix but,
// limited by the set of flag names defined in names.
func (r *CommonRunFlags) flagsLimited(flags *flag.FlagSet, prefix string, names map[FlagName]bool) {
	if names == nil || names[FlagNameAssert] {
		flags.StringVar(&r.Assert, prefix+string(FlagNameAssert), "never", `values: "never", "exact"

This flag determines the policy used to assert the properties of inputs (mainly 
S3 objects, if any) that were used to compute the currently cached result (if 
any).

With "never", the cached result is always accepted (even if any of the input S3 
objects have changed since). 

With "exact", it is asserted that the properties of inputs are exactly the same 
as they were when the cached result was computed. If different, then the cached 
result is not accepted and re-computation is triggered.

Note: for S3 objects, "exact" includes modified time as well. If an S3 object 
has the same content as it did before, but was "touched", then it is not "exact" 
anymore (meaning, the cached result will not be accepted).`)
	}

	if names == nil || names[FlagNameEvalStrategy] {
		flags.StringVar(&r.EvalStrategy, prefix+string(FlagNameEvalStrategy), "topdown", `values: "topdown", "bottomup"

This flag determines the strategy used to traverse the computation graph 
representing your program. Picture your program as a directed acyclic graph with 
the result indicated by a root node at the top and dependent nodes branching 
down from it. 

In "topdown" mode, we start at the top of the graph (root node/Main value) and
explore dependent nodes to build up the tree. If an encountered node is cached, 
we stop there and don't traverse the subtree below that node.

In "bottomup" mode, we start at the top of the graph (root node/Main value) and
work our way down the tree, just like "topdown". However, we don't look up nodes 
in the cache and instead explore all of the dependencies recursively. This 
provides visibility into all the nodes in the graph and they will appear in the 
logs and dotgraph.

Note that "bottomup" mode doesn't mean we re-compute all the nodes, we still 
make use of cached values when evaluating nodes. The main difference is the way
nodes are explored.

There are subtleties with caching and the evaluation strategy that could affect 
performance in some specific cases, but we won't go into those details here. 
Reach out to the reflow team for more information.`)
	}
	if names == nil || names[FlagNameInvalidate] {
		flags.StringVar(&r.Invalidate, prefix+"invalidate", "", "regular expression for node identifiers that should be invalidated")
	}
	if names == nil || names[FlagNameNoCacheExtern] {
		var vacuum bool
		flags.BoolVar(&vacuum, prefix+string(FlagNameNoCacheExtern), false, `don't cache extern ops

DEPRECATED. This flag may still be set, but the program will always behave as if
"nocacheextern=true" because externs are no longer cached.`)
	}
	if names == nil || names[FlagNamePostUseChecksum] {
		flags.BoolVar(&r.PostUseChecksum, prefix+string(FlagNamePostUseChecksum), false, `checksum exec input files after use

If this flag is provided, Reflow verifies the input data for every exec upon 
completion to ensure that it did not change or get corrupted thus invalidating 
the result of that exec.`)
	}
	if names == nil || names[FlagNameRecomputeEmpty] {
		// TODO(pboyapalli): [SYSINFRA-553] determine if we can remove this flag
		flags.BoolVar(&r.RecomputeEmpty, prefix+string(FlagNameRecomputeEmpty), false, `recompute empty cache values

If this flag is set, reflow will recompute cache values when the result fileset 
of an exec is empty or contains any empty values. This flag was added in D7592 
to address a Docker related bug. Generally users should not need to set this 
flag and it may be removed soon.`)
	}
}

// Err checks if the flag values are consistent and valid.
func (r *CommonRunFlags) Err() error {
	switch r.EvalStrategy {
	case "topdown", "bottomup":
	default:
		return fmt.Errorf("invalid evaluation strategy %s", r.EvalStrategy)
	}
	if r.Invalidate != "" {
		_, err := regexp.Compile(r.Invalidate)
		if err != nil {
			return err
		}
	}
	return nil
}

// Configure stores the RunFlags's configuration into the provided
// EvalConfig.
func (r *CommonRunFlags) Configure(c *flow.EvalConfig) (err error) {
	if c.Assert, err = asserter(r.Assert); err != nil {
		return err
	}
	c.RecomputeEmpty = r.RecomputeEmpty
	c.BottomUp = r.EvalStrategy == "bottomup"
	c.PostUseChecksum = r.PostUseChecksum
	if r.Invalidate != "" {
		re := regexp.MustCompile(r.Invalidate)
		c.Invalidate = func(f *flow.Flow) bool {
			return re.MatchString(f.Ident)
		}
	}
	return nil
}

// RunFlags is the supported flags and parameters for a run.
type RunFlags struct {
	CommonRunFlags
	// LocalDir is the directory where execution state is store in Local mode.
	LocalDir string
	// Dir is the where execution state is stored in local mode (alias for backwards compatibility).
	Dir string
	// Local enables execution using the local docker instance.
	Local bool
	// Trace when set enable tracing flow evaluation.
	Trace bool
	Cache bool
	Pred  bool
	// DotGraph enables computation of an evaluation graph.
	DotGraph bool

	// BackgroundTimeout is the duration to wait for background tasks (such as cache writes, etc) to complete.
	// ie, this is the amount of time we wait after the user's program execution finishes but before reflow exits.
	BackgroundTimeout time.Duration
}

// flags adds all run flags to the provided flagset (including localflags).
func (r *RunFlags) Flags(flags *flag.FlagSet) {
	r.flagsLimited(flags, "", nil)
	r.localFlags(flags)
}

// localFlags adds local-only run flags to the provided flagset.
func (r *RunFlags) localFlags(flags *flag.FlagSet) {
	flags.BoolVar(&r.Local, "local", false, "execute flow on the Local Docker instance")
	flags.StringVar(&r.LocalDir, "localdir", defaultFlowDir, "directory where execution state is stored in Local mode")
	flags.StringVar(&r.Dir, "dir", "", "directory where execution state is stored in Local mode (alias for Local Dir for backwards compatibility)")
}

// FlagsNoLocal adds run flags to the provided flagset with the given prefix excluding flags pertaining to local runs.
func (r *RunFlags) FlagsNoLocal(flags *flag.FlagSet, prefix string) {
	r.flagsLimited(flags, prefix, nil)
}

// flagsLimited adds flags to the provided flagset with the given prefix but,
// limited by the set of flag names defined in names.
func (r *RunFlags) flagsLimited(flags *flag.FlagSet, prefix string, names map[FlagName]bool) {
	r.CommonRunFlags.flagsLimited(flags, prefix, names)
	if names == nil || names[FlagNameBackgroundTimeout] {
		flags.DurationVar(&r.BackgroundTimeout, prefix+string(FlagNameBackgroundTimeout), 10*time.Minute, "timeout for background tasks")
	}
	if names == nil || names[FlagNameDotGraph] {
		flags.BoolVar(&r.DotGraph, prefix+string(FlagNameDotGraph), true, `produce an evaluation graph for the run

If this flag is provided, a Graphviz dot format file will be output to the runs 
directory (usually ~/.reflow/runs). To visualize, install the tool (graphviz.org) 
and run:
	> dot ~/.reflow/runs/<runid>.gv > ~/some_file.svg"
Other output file formats also are available.

When running a large reflow module with lots of nodes, it is advisable to 
disable dotgraph generation to avoid slowing down the overall execution time 
of your run.`)
	}
	if names == nil || names[FlagNamePred] {
		flags.BoolVar(&r.Pred, prefix+string(FlagNamePred), false, `predict exec memory requirements to optimize resource usage

When enabled, memory requirements for each exec will be predicted based on 
previous executions of that exec (if available). If a prediction is made, it 
will override the specified memory.

If the exec fails for any reason, with predicted resources, reflow will 
always retry the exec with the resources specified.

Additionally, reflow always retries execs that fail due to a detectable OOM 
error, using 50% more resources each time, upto 3 times, before giving up.

Both "taskdb" and "predictorconfig" need to be configured for prediction to 
work, see "reflow config -help"`)
	}
	if names == nil || names[FlagNameTrace] {
		flags.BoolVar(&r.Trace, prefix+string(FlagNameTrace), false, "logs a trace of flow evaluation for debugging; not to be confused with -tracer (see reflow config -help)")
	}
}

// Override overrides the appropriate RunFlags fields based on the given map of flag names to values.
// This is specifically used where overriding only a subset of flags is desired,
// without inheriting the default values of the flags that aren't being overridden.
func (r *RunFlags) Override(overrides map[string]string) (err error) {
	defer func() {
		if err != nil {
			err = errors.E("runflags.override", err)
		}
	}()
	var (
		args  []string
		names = make(map[FlagName]bool)
	)
	for k, v := range overrides {
		args = append(args, fmt.Sprintf("--%s=%s", k, v))
		names[FlagName(k)] = true
	}

	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	r.flagsLimited(fs, "", names)

	if err = fs.Parse(args); err != nil {
		err = fmt.Errorf("parsing args [%s]: %v", strings.Join(args, ","), err)
		return
	}
	err = r.Err()
	return
}

// Err checks if the flag values are consistent and valid.
func (r *RunFlags) Err() error {
	return r.CommonRunFlags.Err()
}

// needAssocAndRepo determines whether an assoc and repo is needed based on this run flags.
// We need assoc and repo if either the run is non-local, or a local run with cache enabled.
func (r *RunFlags) needAssocAndRepo() bool {
	return !r.Local || r.Cache
}
