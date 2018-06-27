// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// V0module implements a module bridge to "v0" reflow scripts. The
// module's parameters correspond with the script's parameters, and
// it produces a value exporting toplevel identifiers.
type v0module struct {
	params map[string]string
	path   string
	typ    *types.T
	source []byte
}

// Source implements Module.Source.
func (m *v0module) Source() []byte {
	return m.source
}

// Eager returns true.
func (m *v0module) Eager() bool { return true }

func (m *v0module) new(args []string) *lang.Program {
	prog := &lang.Program{
		File: m.path,
		Args: args,
		// Typechecking happens before the v0module is constructed,
		// so we can safely discard messages here.
		Errors: ioutil.Discard,
	}
	f, err := os.Open(m.path)
	if err != nil {
		panic(fmt.Sprintf("failed to open file %s: %v", m.path, err))
	}
	defer f.Close()
	if err := prog.ParseAndTypecheck(f); err != nil {
		panic(err)
	}
	return prog
}

func (m *v0module) Make(sess *Session, params *values.Env) (values.T, error) {
	var args []string
	if v := params.Value("args"); v != nil {
		l := v.(values.List)
		args = make([]string, len(l))
		for i := range l {
			args[i] = l[i].(string)
		}
	}
	prog := m.new(args)
	flags := prog.Flags()
	var flagerr error
	flags.VisitAll(func(f *flag.Flag) {
		if err := f.Value.Set(params.Value(f.Name).(string)); err != nil && flagerr == nil {
			flagerr = err
		}
	})
	if err := flagerr; err != nil {
		return nil, err
	}
	return prog.ModuleValue()
}

func (m *v0module) Flags(sess *Session, env *values.Env) (*flag.FlagSet, error) {
	return nil, errors.New("flags not supported for v0 modules")
}

func (m *v0module) FlagEnv(flags *flag.FlagSet, env *values.Env) error {
	return errors.New("flags not supported for v0 modules")
}

func (m *v0module) ParamErr(env *types.Env) error {
	params := make(map[string]bool)
	for k := range m.params {
		params[k] = true
	}
	for id, t := range env.Symbols() {
		switch id {
		case "args":
			if t.Kind != types.ListKind || t.Elem.Kind != types.StringKind {
				return fmt.Errorf("invalid type %s for parameter %s (type [string])", t, id)
			}
		default:
			if !params[id] {
				return fmt.Errorf("module has no parameter %s", id)
			}
			if t.Kind != types.StringKind {
				return fmt.Errorf("invalid type %s for parameter %s (type string)", t, id)
			}
		}
		delete(params, id)
	}
	if len(params) > 0 {
		var missing []string
		for id := range params {
			missing = append(missing, id)
		}
		if len(missing) == 1 {
			return fmt.Errorf("missing module parameter %s", missing[0])
		}
		return fmt.Errorf("missing module parameters %s", strings.Join(missing, ", "))
	}
	return nil
}

func (m *v0module) Params() []Param {
	var params []Param
	for ident, doc := range m.params {
		p := Param{
			Ident:    ident,
			Doc:      doc,
			Required: true,
			Type:     types.String,
		}
		params = append(params, p)
	}
	return append(params, Param{
		Ident:    "args",
		Doc:      "Reflow v0 arguments",
		Required: false,
		Type:     types.List(types.String),
	})
}

func (m *v0module) Doc(ident string) string {
	return ""
}

func (m *v0module) Type() *types.T {
	return m.typ
}
