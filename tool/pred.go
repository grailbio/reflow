package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/predictor"
	"github.com/grailbio/reflow/runtime"
	"github.com/grailbio/reflow/taskdb"
)

var predCols = []headerDesc{
	{"i", "profile number"},
	{"N", "number of samples in the profile"},
	{"max", "max value of the statistic across the samples"},
	{"mean", "mean value of the statistic across the samples"},
	{"var", "variance of the statistic across the samples"},
	{"first", "first time at which the statistic was profiled"},
	{"last", "last time at which the statistic was profiled"},
}

type profResults struct {
	arg   string
	profs []reflow.Profile
}

func (c *Cmd) pred(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("pred", flag.ExitOnError)
	maxInspectFlag := flags.Int("maxinspect", 0, "max number of inspect to process (if unspecified, default from predictor config is used)")
	nameFlag := flags.String("name", "", "if given, only profiles for this statistic are printed along with its predicted value")

	help := `Pred displays statistics the predictor will use for the given identifiers.
Provided args should be either ImgCmdId (digests) or exec identifiers.

For each arg, and for each statistic profiled, the tool prints:
` + description(predCols) + `

pred processes MaxInspect number of inspects specified in the predictor config (which can be overrided by -maxinspect).

If -name is provided, pred will only print statistics for the given name and also display the predicted value for that statistic.
Valid values for -name are "mem" and "duration".

`
	c.Parse(flags, args, help, "pred names...")
	if flags.NArg() == 0 {
		flags.Usage()
	}

	cfg, err := runtime.PredictorConfig(c.Config, true)
	if err != nil {
		c.Fatal(err)
	}
	var tdb taskdb.TaskDB
	if err := c.Config.Instance(&tdb); err != nil {
		c.Fatalf("pred needs taskdb: %v", err)
	}
	mi := cfg.MaxInspect
	if *maxInspectFlag > 0 {
		mi = *maxInspectFlag
	}
	p := predictor.New(tdb, c.Log.Tee(nil, "predictor: "), cfg.MinData, mi, cfg.MemPercentile)
	results := make(chan profResults)
	go func() {
		_ = traverse.Each(len(flags.Args()), func(i int) error {
			arg := flags.Args()[i]
			var q predictor.ProfileQuery
			if d, err := digest.Parse(arg); err == nil {
				q.ImgCmdId = taskdb.ImgCmdID(d)
			} else {
				q.Ident = arg
			}
			profs, err := p.QueryProfiles(ctx, q)
			switch {
			case err != nil:
				c.Log.Errorf("invalid param %s: %v", arg, err)
			case len(profs) == 0:
				c.Log.Errorf("no data for param %s", arg)
			default:
				results <- profResults{arg, profs}
			}
			return nil
		})
		close(results)
	}()

	var (
		tw tabwriter.Writer
		w  io.Writer
	)
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer func() { _ = tw.Flush() }()
	w = &tw

	for r := range results {
		arg, profs := r.arg, r.profs
		distKeys := map[string]bool{}
		for _, p := range profs {
			for k, v := range p {
				if v.N > 1 {
					distKeys[k] = true
				}
			}
		}
		keys := make([]string, 0, len(distKeys))
		for k := range distKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintln(w, "predictor profiles for arg: ", arg)
		for _, k := range keys {
			if *nameFlag != "" && k != *nameFlag {
				continue
			}
			sort.Slice(profs, func(i, j int) bool {
				return profs[i][k].First.Before(profs[j][k].First)
			})
			fmt.Fprintf(w, "%s:\t%s\n", k, header(predCols))
			for i, p := range profs {
				v := p[k]
				if v.N < 2 {
					continue
				}
				fmt.Fprintf(w, "\t")
				switch k {
				case "mem", "disk", "tmp":
					fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%f", i, v.N, data.Size(v.Max), data.Size(v.Mean), v.Var)
				default:
					fmt.Fprintf(w, "%d\t%d\t%5.1f\t%5.1f\t%5.1f", i, v.N, v.Max, v.Mean, v.Var)
				}
				fmt.Fprintf(w, "\t%s\t%s\n", v.First.Format(time.RFC3339), v.Last.Format(time.RFC3339))
			}
			fmt.Fprintln(w, "")
		}
		if *nameFlag == "" {
			continue
		}
		if v, n, err := p.QueryPercentile(profs, *nameFlag, cfg.MemPercentile); err != nil {
			c.Log.Errorf("QueryPercentile arg %s: %v", arg, err)
		} else {
			switch *nameFlag {
			case "mem", "disk", "tmp":
				fmt.Fprintf(w, "predicted %s value for arg %s = %s (from %d samples)\n", *nameFlag, arg, data.Size(v), n)
			default:
				fmt.Fprintf(w, "predicted %s value for arg %s = %f (from %d samples)\n", *nameFlag, arg, v, n)
			}
			fmt.Fprintln(w, "")
		}
	}
}
