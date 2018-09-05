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
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// EvalResult contains the program Flow, params and args.
type EvalResult struct {
	Program string
	Flow    *flow.Flow
	Type    *types.T
	Params  map[string]string
	Args    []string
	V1      bool
	Bundle  *syntax.Bundle
}

// Eval evaluates a Reflow program to a Flow. It can evaluate both legacy (".reflow")
// and modern (".rf") programs. It interprets flags as module parameters.
func (c *Cmd) Eval(args []string) (EvalResult, error) {
	if len(args) == 0 {
		return EvalResult{}, errors.New("no program provided")
	}
	var file string
	file, args = args[0], args[1:]
	er := EvalResult{Params: make(map[string]string)}
	var err error
	er.Program, err = filepath.Abs(file)
	if err != nil {
		return EvalResult{}, err
	}
	switch ext := filepath.Ext(file); ext {
	case ".reflow":
		f, err := os.Open(file)
		if err != nil {
			return EvalResult{}, err
		}
		prog := &lang.Program{File: file, Errors: os.Stderr}
		if err := prog.ParseAndTypecheck(f); err != nil {
			return EvalResult{}, fmt.Errorf("type error: %s", err)
		}
		flags := prog.Flags()
		flags.Usage = func() {
			fmt.Fprintf(os.Stderr, "usage of %s:\n", file)
			flags.PrintDefaults()
			c.Exit(2)
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
	case ".rf", ".rfx":
		sess := syntax.NewSession(nil)
		if err != nil {
			return EvalResult{}, err
		}
		er, err := c.evalV1(sess, file, args)
		if err != nil {
			return EvalResult{}, err
		}
		er.Bundle = sess.Bundle()
		return er, nil
	default:
		return EvalResult{}, fmt.Errorf("unknown file extension %q", ext)
	}
}

// EvalV1 is a helper function to evaluats a reflow v1 program.
func (c *Cmd) evalV1(sess *syntax.Session, file string, args []string) (EvalResult, error) {
	er := EvalResult{Params: make(map[string]string)}
	er.V1 = true
	er.Args = args
	var err error
	er.Program, err = filepath.Abs(file)
	if err != nil {
		return EvalResult{}, err
	}
	sess.Stderr = c.Stderr
	m, err := sess.Open(file)
	if err != nil {
		return EvalResult{}, err
	}
	var maintyp *types.T
	for _, f := range m.Type().Fields {
		if f.Name == "Main" {
			maintyp = f.T
			break
		}
	}
	if maintyp == nil {
		return EvalResult{}, fmt.Errorf("module %v does not define symbol Main", file)
	}
	flags, err := m.Flags(sess, sess.Values)
	if err != nil {
		c.Fatal(err)
	}
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage of %s:\n", file)
		flags.PrintDefaults()
		c.Exit(2)
	}
	flags.Parse(args)
	env := sess.Values.Push()
	if err := m.FlagEnv(flags, env, types.NewEnv()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flags.Usage()
	}
	v, err := m.Make(sess, env)
	if err != nil {
		return EvalResult{}, err
	}
	v = v.(values.Module)["Main"]
	v = syntax.Force(v, maintyp)
	er.Type = maintyp
	flags.VisitAll(func(f *flag.Flag) {
		er.Params[f.Name] = f.Value.String()
	})
	switch v := v.(type) {
	case *flow.Flow:
		if v.Requirements().Equal(reflow.Requirements{}) {
			c.Fatal("flow does not have resource requirements; add a @requires annotation to val Main")
		}
		er.Flow = v
		return er, nil
	default:
		er.Flow = &flow.Flow{Op: flow.Val, Value: v}
		return er, nil
	}
}

func sprintval(v values.T, t *types.T) string {
	if t == nil {
		return fmt.Sprint(v)
	}
	return values.Sprint(v, t)
}
