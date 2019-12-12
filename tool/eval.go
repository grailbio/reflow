// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// Eval represents the evaluation of a single module.
// Evaluations are performed by Cmd.Eval.
type Eval struct {
	// InputArgs is the raw input arguments of the evaluation, as passed
	// in by user.
	InputArgs []string
	// Program stores the reflow program's path.
	Program string
	// Params stores the evaluation's module parameters and raw values.
	Params map[string]string
	// Args stores the evaluation's command line arugments.
	Args []string
	// V1 tells whether this program is a "V1" (".rf") program.
	V1 bool
	// Bundle stores a v1 bundle associated with this evaluation.
	Bundle *syntax.Bundle
	// Images is the list of images that were parsed from a syntax session.
	Images []string
	// ImageMap stores a mapping between image names and resolved
	// image names, to be used in evaluation.
	ImageMap map[string]string
	// Type is the module type of the toplevel module that has been
	// evaluated.
	Type *types.T
	// Module is the module value that was evaluated.
	Module values.Module
}

// MainType returns the type of the module's Main identifier.
func (e *Eval) MainType() *types.T {
	return e.Type.Field("Main")
}

// Main returns the flow that represents the module's Main.
func (e *Eval) Main() *flow.Flow {
	v := e.Module["Main"]
	if v == nil {
		return nil
	}
	v = syntax.Force(v, e.MainType())
	switch v := v.(type) {
	case *flow.Flow:
		return v
	default:
		return &flow.Flow{Op: flow.Val, Value: v}
	}
}

// Run evaluates a reflow program to a flow. It can evaluate both
// legacy (".reflow") and modern (".rf") programs. It interprets
// flags as module parameters. Input arguments and options are
// specified in the passed-in Eval; results are deposited there, too.
func (e *Eval) Run() error {
	if len(e.InputArgs) == 0 && len(e.Program) == 0 {
		return errors.New("no program provided")
	}

	var (
		file string
		args []string
	)
	file, args = e.Program, e.Args
	if len(e.InputArgs) > 0 {
		e.Program, e.Args = e.InputArgs[0], e.InputArgs[1:]
		file, args = e.Program, e.Args
	}
	var err error
	e.Program, err = filepath.Abs(file)
	if err != nil {
		return err
	}
	switch ext := filepath.Ext(file); ext {
	case ".reflow":
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		prog := &lang.Program{File: file, Errors: os.Stderr}
		if err := prog.ParseAndTypecheck(f); err != nil {
			return fmt.Errorf("type error: %s", err)
		}
		flags := prog.Flags()
		flags.Usage = func() {
			fmt.Fprintf(os.Stderr, "usage of %s:\n", file)
			flags.PrintDefaults()
			os.Exit(2)
		}
		e.Params = make(map[string]string)
		if err = flags.Parse(args); err != nil {
			return err
		}
		flags.VisitAll(func(f *flag.Flag) {
			if f.Value.String() == "" {
				fmt.Fprintf(os.Stderr, "parameter %q is undefined\n", f.Name)
				flags.Usage()
			}
			e.Params[f.Name] = f.Value.String()
		})
		prog.Args = flags.Args()
		e.Args = prog.Args
		e.Module = values.Module{
			"Main": prog.Eval(),
		}
		e.Type = prog.ModuleType()
		return nil
	case ".rf", ".rfx":
		sess := syntax.NewSession(nil)
		if err := e.evalV1(sess); err != nil {
			return err
		}
		e.Bundle = sess.Bundle()
		e.Images = sess.Images()
		return nil
	default:
		return fmt.Errorf("unknown file extension %q", ext)
	}
}

// EvalV1 is a helper function to evaluate a reflow v1 program.
func (e *Eval) evalV1(sess *syntax.Session) error {
	file, args := e.Program, e.Args
	e.Params = make(map[string]string)
	e.V1 = true
	e.Args = args
	var err error
	e.Program, err = filepath.Abs(file)
	if err != nil {
		return err
	}
	m, err := sess.Open(file)
	if err != nil {
		return err
	}
	flags, err := m.Flags(sess, sess.Values)
	if err != nil {
		return errors.E(errors.Fatal, err)
	}
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage of %s:\n", file)
		flags.PrintDefaults()
		os.Exit(2)
	}
	if err = flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		err = fmt.Errorf("unrecognized parameters: %s", strings.Join(flags.Args(), " "))
		fmt.Fprintln(os.Stderr, err)
		flags.Usage()
	}
	env := sess.Values.Push()
	if err := m.FlagEnv(flags, env, types.NewEnv()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flags.Usage()
	}
	v, err := m.Make(sess, env)
	if err != nil {
		return err
	}
	e.Module = v.(values.Module)
	e.Type = m.Type(nil)
	flags.VisitAll(func(f *flag.Flag) {
		e.Params[f.Name] = f.Value.String()
	})
	return err
}

// Resolve images resolves the images in an evaluated program.
func (e *Eval) ResolveImages(config infra.Config) error {
	// resolve images is only supported for v1 flows as of this writing.
	if !e.V1 {
		return nil
	}
	var (
		awsSession *session.Session
		err        error
	)
	if err = config.Instance(&awsSession); err != nil {
		return err
	}
	r := ImageResolver{
		Authenticator: ec2authenticator.New(awsSession),
	}
	e.ImageMap, err = r.ResolveImages(context.Background(), e.Images)
	return err
}

func sprintval(v values.T, t *types.T) string {
	if t == nil {
		return fmt.Sprint(v)
	}
	return values.Sprint(v, t)
}
