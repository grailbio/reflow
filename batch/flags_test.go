package batch

import (
	"flag"
	"testing"
)

func TestParseFlags(t *testing.T) {
	testCases := []struct {
		// Prefer cmdArgs over runArgs over defaults.
		defaults map[string]string
		runArgs  map[string]string
		cmdArgs  []string
		expected map[string]string
	}{
		{
			map[string]string{"argB": "defaultB"},
			map[string]string{"argA": "runA", "argB": "runB"},
			[]string{"-argA", "cmdA"},
			map[string]string{"argA": "cmdA", "argB": "runB"},
		},
		{
			map[string]string{},
			map[string]string{"argA": "runA", "argB": "runB"},
			[]string{"-argB", "cmdB"},
			map[string]string{"argA": "runA", "argB": "cmdB"},
		},
		{
			map[string]string{"argA": "defaultA", "argB": "defaultB"},
			map[string]string{"argA": "runA"},
			[]string{},
			map[string]string{"argA": "runA", "argB": "defaultB"},
		},
	}

	for _, testCase := range testCases {
		var flags flag.FlagSet
		var argA, argB string
		flags.StringVar(&argA, "argA", "", "")
		flags.StringVar(&argB, "argB", "", "")
		for k, v := range testCase.defaults {
			_ = flags.Set(k, v)
		}

		if err := parseFlags(&flags, testCase.runArgs, testCase.cmdArgs); err != nil {
			t.Fatalf("parse error for runArgs %v, and cmdArgs %v: %v", testCase.runArgs, testCase.cmdArgs, err)
		}

		var numVisited int
		flags.VisitAll(func(f *flag.Flag) {
			numVisited++
			if got, want := f.Value.String(), testCase.expected[f.Name]; got != want {
				t.Errorf("unexpected flag value: %v, expected: %v", got, want)
			}
		})
		if numVisited != 2 {
			t.Errorf("not all flags were visited")
		}
	}
}

func TestParseFlagsErrors(t *testing.T) {
	testCases := []struct {
		runArgs map[string]string
		cmdArgs []string
		pass    bool
	}{
		// Specifying only the defaults works.
		{
			map[string]string{},
			[]string{},
			true,
		},
		// Specifying a non-existent run arg fails.
		{
			map[string]string{"argZ": "a"},
			[]string{},
			false,
		},
		// Specifying one non-parsable and one parsable run arg fails.
		{
			map[string]string{"argA": "notAnInt", "argB": "1.0"},
			[]string{},
			false,
		},
		// Specifying a non-parsable cmd arg fails.
		{
			map[string]string{},
			[]string{"--argA=notAnInt"},
			false,
		},
	}

	for _, testCase := range testCases {
		var flags flag.FlagSet
		var argA, argB int
		flags.IntVar(&argA, "argA", 0, "")
		flags.IntVar(&argB, "argB", 0, "")

		if got, want := (parseFlags(&flags, testCase.runArgs, testCase.cmdArgs) == nil), testCase.pass; got != want {
			t.Errorf("expected %v, actual %v for runArgs %v and cmdArgs %v", want, got, testCase.runArgs, testCase.cmdArgs)
		}
	}
}
