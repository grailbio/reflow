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
	// GC indiciates if objects should be garbage collected (valid only in v0 flows).
	GC bool
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
	flags.BoolVar(&r.GC, "gc", false, "enable garbage collection during evaluation")
	flags.BoolVar(&r.NoCacheExtern, "nocacheextern", false, "don't cache extern ops")
	flags.BoolVar(&r.RecomputeEmpty, "recomputeempty", false, "recompute empty cache values")
	flags.StringVar(&r.EvalStrategy, "eval", "topdown", "evaluation strategy")
	flags.StringVar(&r.Invalidate, "invalidate", "", "regular expression for node identifiers that should be invalidated")
	flags.StringVar(&r.Assert, "assert", "never", "policy used to Assert cached flow result compatibility (eg: never, exact)")
	flags.BoolVar(&r.Sched, "sched", true, "use scalable scheduler instead of work stealing")
	flags.BoolVar(&r.PostUseChecksum, "postusechecksum", false, "checksum files after use")
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
func (r *CommonRunFlags) Configure(c *flow.EvalConfig) error {
	var err error
	c.Assert, err = asserter(r.Assert)
	if err != nil {
		return err
	}
	c.NoCacheExtern = r.NoCacheExtern
	c.GC = r.GC
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
	flags.BoolVar(&r.Trace, "trace", false, "trace flow evaluation")
	flags.StringVar(&r.resourcesFlag, "resources", "", "override offered resources in local mode (JSON formatted reflow.Resources)")
	flags.BoolVar(&r.Pred, "pred", false, "use predictor to optimize resource usage. sched must also be true for the predictor to be used")
	flags.BoolVar(&r.DotGraph, "dotgraph", true, "produce an evaluation graph for the run")
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
