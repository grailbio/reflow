package tool

import (
	"encoding/json"
	"flag"
	"fmt"
	"regexp"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/runner"
)

const defaultFlowDir = "/tmp/flow"

// CommonRunFlags are the run flags that are common across various run modes (run, batch, etc)
type CommonRunFlags struct {
	// NoCacheExtern indicates if extern operations should be written to cache.
	NoCacheExtern bool
	// RecomputeEmpty indicates if cache results with empty filesets be automatically recomputed.
	RecomputeEmpty bool
	// EvalStrategy is the evaluation strategy. Supported modes are "topdown" and "bottomup".
	EvalStrategy string
	// Invalidate is a regular expression for node identifiers that should be invalidated.
	Invalidate string
	// Assert is the policy used to assert cached flow result compatibility. e.g. never, exact.
	Assert string
	// Use scalable scheduler instead of the work stealer mode.
	Sched bool
	// PostUseChecksum indicates whether input filesets are checksummed after use.
	PostUseChecksum bool
}

// Flags adds the common run flags to the provided flagset.
func (r *CommonRunFlags) Flags(flags *flag.FlagSet) {
	var vacuum bool
	flags.BoolVar(&vacuum, "gc", false, "(DEPRECATED) enable garbage collection during evaluation")
	// TODO(pboyapalli): [SYSINFRA-554] modify extern caching so that we can drop the nocacheextern flag
	flags.BoolVar(&r.NoCacheExtern, "nocacheextern", false, `don't cache extern ops

For all extern operations, "nocacheextern=true" acts like "cache=off" and 
"nocacheextern=false" acts like "cache=readwrite". With "nocacheextern=true", 
extern operations that have a cache hit will result in the fileset being copied 
to the extern URL again.`)

	// TODO(pboyapalli): [SYSINFRA-553] determine if we can remove this flag
	flags.BoolVar(&r.RecomputeEmpty, "recomputeempty", false, `recompute empty cache values

If this flag is set, reflow will recompute cache values when the result fileset 
of an exec is empty or contains any empty values. This flag was added in D7592 
to address a Docker related bug. Generally users should not need to set this 
flag and it may be removed soon.`)
	flags.StringVar(&r.EvalStrategy, "eval", "topdown", `values: "topdown", "bottomup"

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
	flags.StringVar(&r.Invalidate, "invalidate", "", "regular expression for node identifiers that should be invalidated")
	flags.StringVar(&r.Assert, "assert", "never", `values: "never", "exact"

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
	flags.BoolVar(&r.Sched, "sched", true, "use scalable scheduler instead of work stealing")
	flags.BoolVar(&r.PostUseChecksum, "postusechecksum", false, `checksum exec input files after use

If this flag is provided, Reflow verifies the input data for every exec upon 
completion to ensure that it did not change or get corrupted thus invalidating 
the result of that exec.`)
}

// Err checks if the flag values are consistent and valid.
func (r *CommonRunFlags) Err() error {
	if !r.Sched {
		return fmt.Errorf("non-scheduler mode is not supported (must have -sched)")
	}
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
	c.NoCacheExtern = r.NoCacheExtern
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
	// Resources overrides the resources reflow is permitted to use in local mode (instead of using up the entire machine).
	Resources reflow.Resources
	Cache     bool
	Pred      bool
	// DotGraph enables computation of an evaluation graph.
	DotGraph bool

	// BackgroundTimeout is the duration to wait for background tasks (such as cache writes, etc) to complete.
	// ie, this is the amount of time we wait after the user's program execution finishes but before reflow exits.
	BackgroundTimeout time.Duration

	// Cluster is the externally specified cluster provider. If non-nil, this cluster provider overrides the one specified in the reflow config.
	Cluster runner.Cluster

	resourcesFlag string
	needAss       bool
	needRepo      bool
}

// Flags adds run flags to the provided flagset.
func (r *RunFlags) Flags(flags *flag.FlagSet) {
	r.CommonRunFlags.Flags(flags)
	flags.BoolVar(&r.Local, "local", false, "execute flow on the Local Docker instance")
	flags.StringVar(&r.LocalDir, "localdir", defaultFlowDir, "directory where execution state is stored in Local mode")
	flags.StringVar(&r.Dir, "dir", "", "directory where execution state is stored in Local mode (alias for Local Dir for backwards compatibility)")
	flags.BoolVar(&r.Trace, "traceflow", false, "logs a trace of flow evaluation for debugging; not to be confused with -tracer (see \"reflow config -help\")")
	flags.StringVar(&r.resourcesFlag, "resources", "", "override offered resources in local mode (JSON formatted reflow.Resources)")
	flags.BoolVar(&r.Pred, "pred", false, `predict exec memory requirements to optimize resource usage

When enabled, memory requirements for each exec will be predicted based on 
previous executions of that exec (if available). If a prediction is made, it 
will override the specified memory.

If the exec fails for any reason, with predicted resources, reflow will 
always retry the exec with the resources specified.

Additionally, reflow always retries execs that fail due to a detectable OOM 
error, using 50% more resources each time, upto 3 times, before giving up.

Both "taskdb" and "predictorconfig" need to be configured for prediction to 
work, see "reflow config -help"`)
	flags.BoolVar(&r.DotGraph, "dotgraph", true, `produce an evaluation graph for the run

If this flag is provided, a Graphviz dot format file will be output to the runs 
directory (usually ~/.reflow/runs). To visualize, install the tool (graphviz.org) 
and run:
	> dot ~/.reflow/runs/<runid>.gv > ~/some_file.svg"
Other output file formats also are available.

When running a large reflow module with lots of nodes, it is advisable to 
disable dotgraph generation to avoid slowing down the overall execution time 
of your run.`)
	flags.DurationVar(&r.BackgroundTimeout, "backgroundtimeout", 10*time.Minute, "timeout for background tasks")
}

// Err checks if the flag values are consistent and valid.
func (r *RunFlags) Err() error {
	if r.Local {
		if r.resourcesFlag != "" {
			if err := json.Unmarshal([]byte(r.resourcesFlag), &r.Resources); err != nil {
				return fmt.Errorf("-resources: %s", err)
			}
		}
		if r.Cache {
			r.needAss = true
			r.needRepo = true
		}
	} else {
		if r.resourcesFlag != "" {
			return errors.New("-resources can only be used in local mode")
		}
		r.needAss = true
		r.needRepo = true
	}
	if !r.Sched && r.Pred {
		return errors.New("-pred cannot be used without -sched")
	}
	return nil
}
