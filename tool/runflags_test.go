package tool

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunFlags(t *testing.T) {
	for _, tt := range []struct {
		prefix  string
		args    []string
		want    RunFlags
		wantErr bool
	}{
		{"", []string{}, RunFlags{
			CommonRunFlags: CommonRunFlags{
				EvalStrategy: "topdown",
				Assert:       "never",
				Sched:        true,
			},
			DotGraph:          true,
			BackgroundTimeout: 10 * time.Minute,
		}, false},
		{"", []string{"--pred=true"}, RunFlags{
			CommonRunFlags: CommonRunFlags{
				EvalStrategy: "topdown",
				Assert:       "never",
				Sched:        true,
			},
			Pred:              true,
			DotGraph:          true,
			BackgroundTimeout: 10 * time.Minute,
		}, false},
		{"prefix_", []string{}, RunFlags{
			CommonRunFlags: CommonRunFlags{
				EvalStrategy: "topdown",
				Assert:       "never",
				Sched:        true,
			},
			DotGraph:          true,
			BackgroundTimeout: 10 * time.Minute,
		}, false},
		{"prefix_", []string{"--prefix_pred=true"}, RunFlags{
			CommonRunFlags: CommonRunFlags{
				EvalStrategy: "topdown",
				Assert:       "never",
				Sched:        true,
			},
			Pred:              true,
			DotGraph:          true,
			BackgroundTimeout: 10 * time.Minute,
		}, false},
		{"prefix_", []string{"--pred=true"}, RunFlags{}, true},
		{"", []string{"--prefix_pred=true"}, RunFlags{}, true},
	} {
		var got RunFlags
		if err := parseRunFlags(&got, tt.prefix, tt.args); tt.wantErr {
			if err == nil {
				t.Error("got no error, want error")
			}
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		want := checkErr(t, tt.want)
		require.Equal(t, want, got)
	}
}

type comparison struct {
	got, want interface{}
}

func compareValues(t *testing.T, comparisons []comparison) {
	t.Helper()
	for _, tt := range comparisons {
		if tt.got != tt.want {
			t.Errorf("got %v, want %v", tt.got, tt.want)
		}
	}
}

func TestOverride(t *testing.T) {
	var rf RunFlags
	if err := parseRunFlags(&rf, "", nil); err != nil {
		t.Fatal(err)
	}
	rf.Pred = false
	rf.BackgroundTimeout = 10 * time.Minute
	compareValues(t, []comparison{
		{rf.BackgroundTimeout, 10 * time.Minute},
		{rf.Pred, false},
	})
	if err := rf.Override(map[string]string{"pred": "true"}); err != nil {
		t.Fatal(err)
	}
	compareValues(t, []comparison{
		{rf.BackgroundTimeout, 10 * time.Minute},
		{rf.Pred, true},
	})
	if err := rf.Override(map[string]string{"backgroundtimeout": "20m"}); err != nil {
		t.Fatal(err)
	}
	compareValues(t, []comparison{
		{rf.BackgroundTimeout, 20 * time.Minute},
		{rf.Pred, true},
	})
}

func parseRunFlags(flags *RunFlags, prefix string, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.flagsLimited(fs, prefix, nil)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := flags.Err(); err != nil {
		return fmt.Errorf("parsing args [%s]: %v", strings.Join(args, ","), err)
	}
	return nil
}

func checkErr(t *testing.T, rf RunFlags) RunFlags {
	if err := rf.Err(); err != nil {
		t.Fatal(err)
	}
	return rf
}
