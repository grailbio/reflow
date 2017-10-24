// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// A Session is a compiler session. It's responsible for opening,
// parsing and type checking modules.
type Session struct {
	Types  *types.Env
	Values *values.Env

	path    string
	modules map[string]*Module

	// images is a collection of Docker image names from exec expressions.
	// It's populated during expression evaluation. Values are all true.
	images map[string]bool
}

// NewSession creates and initializes a session.
func NewSession() *Session {
	s := &Session{modules: map[string]*Module{}, images: map[string]bool{}}
	s.Types, s.Values = Stdlib()
	return s
}

// Open parses and type checks, and then returns the module at the given
// path. It then returns the module and any associated error.
func (s *Session) Open(path string) (*Module, error) {
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
}

// SeeImage records an image name. Call during expression evaluation.
func (s *Session) SeeImage(image string) {
	s.images[image] = true
}

// Images returns images encountered so far during expression evaluation.
func (s *Session) Images() []string {
	var images []string
	for imageName := range s.images {
		images = append(images, imageName)
	}
	return images
}
