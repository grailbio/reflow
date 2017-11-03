// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

type evalResult struct {
	Program string
	Flow    *reflow.Flow
	Type    *types.T
	Params  map[string]string
	Args    []string
	V1      bool
}

// eval evaluates a Reflow program to a Flow.
func (c *Cmd) eval(args []string) (evalResult, error) {
	if len(args) == 0 {
		return evalResult{}, errors.New("no program provided")
	}
	var file string
	file, args = args[0], args[1:]
	er := evalResult{Params: make(map[string]string)}
	var err error
	er.Program, err = filepath.Abs(file)
	if err != nil {
		return evalResult{}, err
	}
	switch ext := filepath.Ext(file); ext {
	case ".reflow":
		f, err := os.Open(file)
		if err != nil {
			return evalResult{}, err
		}
		prog := &lang.Program{File: file, Errors: os.Stderr}
		if err := prog.ParseAndTypecheck(f); err != nil {
			return evalResult{}, fmt.Errorf("type error: %s", err)
		}
		flags := prog.Flags()
		flags.Usage = func() {
			fmt.Fprintf(os.Stderr, "usage of %s:\n", file)
			flags.PrintDefaults()
			os.Exit(2)
		}
		flags.Parse(args)
		flags.VisitAll(func(f *flag.Flag) {
			if f.Value.String() == "" {
				fmt.Fprintf(os.Stderr, "parameter %q is undefined\n", f.Name)
				flags.Usage()
			}
			er.Params[f.Name] = f.Value.String()
		})
		prog.Args = flags.Args()
		er.Args = prog.Args
		er.Flow = prog.Eval()
		return er, nil
	case ".rf":
		er.V1 = true
		sess := syntax.NewSession()
		m, err := sess.Open(file)
		if err != nil {
			return evalResult{}, err
		}
		var maintyp *types.T
		for _, f := range m.Type().Fields {
			if f.Name == "Main" {
				maintyp = f.T
				break
			}
		}
		if maintyp == nil {
			return evalResult{}, fmt.Errorf("module %v does not define symbol Main", file)
		}
		flags, err := m.Flags(sess, sess.Values)
		if err != nil {
			c.Fatal(err)
		}
		flags.Usage = func() {
			fmt.Fprintf(os.Stderr, "usage of %s:\n", file)
			flags.PrintDefaults()
			os.Exit(2)
		}
		flags.Parse(args)
		env := sess.Values.Push()
		if err := m.FlagEnv(flags, env); err != nil {
			fmt.Fprintln(os.Stderr, err)
			flags.Usage()
		}
		v, err := m.Make(sess, env)
		if err != nil {
			return evalResult{}, err
		}
		v = v.(values.Module)["Main"]
		v = syntax.Force(v, maintyp)
		er.Type = maintyp
		flags.VisitAll(func(f *flag.Flag) {
			er.Params[f.Name] = f.Value.String()
		})
		switch v := v.(type) {
		case *reflow.Flow:
			if min, _ := v.Requirements(); min.IsZeroAll() {
				c.Fatal("flow does not have resource requirements; add a @requires annotation to val Main")
			}
			er.Flow = v
			return er, nil
		default:
			er.Flow = &reflow.Flow{Op: reflow.OpVal, Value: v}
			return er, nil
		}
	default:
		return evalResult{}, fmt.Errorf("unknown file extension %q", ext)
	}
}

func sprintval(v values.T, t *types.T) string {
	if t == nil {
		return fmt.Sprint(v)
	}
	return values.Sprint(v, t)
}
