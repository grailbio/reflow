// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// A Session is a compiler session. It's responsible for opening,
// parsing and type checking modules.
type Session struct {
	Types  *types.Env
	Values *values.Env

	path    string
	modules map[string]Module

	mu sync.Mutex

	// images is a collection of Docker image names from exec expressions.
	// It's populated during expression evaluation. Values are all true.
	images map[string]bool
}

// NewSession creates and initializes a session.
func NewSession() *Session {
	s := &Session{modules: map[string]Module{}, images: map[string]bool{}}
	s.Types, s.Values = Stdlib()
	return s
}

// Open parses and type checks, and then returns the module at the given
// path. It then returns the module and any associated error.
func (s *Session) Open(path string) (Module, error) {
	if strings.HasPrefix(path, "$/") {
		m := lib[path[2:]]
		if m == nil {
			return nil, fmt.Errorf("no system module named %s", path[2:])
		}
		return m, nil
	}
	if s == nil {
		return nil, errors.New("nil session")
	}
	if strings.HasPrefix(path, "./") {
		path = filepath.Join(s.path, path)
	}
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if m := s.modules[abspath]; m != nil {
		return m, nil
	}
	switch ext := filepath.Ext(abspath); ext {
	default:
		return nil, fmt.Errorf("unknown module extension %s", ext)
	case ".rf":
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		lx := &Parser{
			File: abspath,
			Body: f,
			Mode: ParseModule,
		}
		if err := lx.Parse(); err != nil {
			return nil, err
		}
		save := s.path
		s.path = filepath.Dir(path)
		if err := lx.Module.Init(s, s.Types); err != nil {
			s.path = save
			return nil, err
		}
		s.path = save
		// Label each toplevel declaration with the module name.
		base := filepath.Base(path)
		ext := filepath.Ext(base)
		base = strings.TrimSuffix(base, ext)
		for _, decl := range lx.Module.Decls {
			decl.Ident = base + "." + decl.Ident
		}
		s.modules[path] = lx.Module
		return lx.Module, nil
	case ".reflow":
		// Construct a synthetic module from a Reflow "v0" script.
		prog := &lang.Program{
			File: path,
			// This doesn't go through the same error reporting path as
			// everything else, but this is here just for compatibility, so
			// we'll live.
			Errors: os.Stderr,
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if err := prog.ParseAndTypecheck(f); err != nil {
			return nil, err
		}
		// We have provide v0module with the path, and not the prog,
		// since it can potentially mint multiple instances.
		m := &v0module{
			params: make(map[string]string),
			path:   path,
			typ:    prog.ModuleType(),
		}
		flags := prog.Flags()
		flags.VisitAll(func(f *flag.Flag) {
			// Instead of name mangling, simply reject modules whose
			// parameters are not valid Reflow names.
			if f.Name == "args" {
				err = errors.New("reserved parameter name args")
			}
			if !isValidIdent(f.Name) {
				err = fmt.Errorf("param %q is not a valid Reflow identifier", f.Name)
			}
			m.params[f.Name] = f.Usage
		})
		if err != nil {
			return nil, err
		}
		s.modules[path] = m
		return m, nil
	}

}

// SeeImage records an image name. Call during expression evaluation.
func (s *Session) SeeImage(image string) {
	s.mu.Lock()
	s.images[image] = true
	s.mu.Unlock()
}

// Images returns images encountered so far during expression evaluation.
func (s *Session) Images() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var images []string
	for imageName := range s.images {
		images = append(images, imageName)
	}
	return images
}
