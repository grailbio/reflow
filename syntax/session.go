// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/lang"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// A Session is a compiler session. It's responsible for opening,
// parsing and type checking modules.
type Session struct {
	// Stdout and stderr is the writer to which standard output and error are written.
	Stdout, Stderr io.Writer

	// Stdwarn is the writer to which warnings are printed.
	Stdwarn io.Writer

	Types  *types.Env
	Values *values.Env

	src Sourcer

	path    string
	modules map[string]Module
	// Entrypoint is the first module opened.
	entrypoint     Module
	entrypointPath string

	mu sync.Mutex

	nwarn int

	// images is a collection of Docker image names from exec expressions.
	// It's populated during expression evaluation. Values are all true.
	images map[string]bool
}

// NewSession creates and initializes a session, reading
// source bytes from the provided sourcer.
//
// If src is nil, the default Sourcer is selected.
func NewSession(src Sourcer) *Session {
	s := &Session{modules: map[string]Module{}, images: map[string]bool{}, src: src}
	if s.src == nil {
		s.src = Filesystem
	}
	s.Types, s.Values = Stdlib()
	return s
}

// Open parses and type checks, and then returns the module at the given path.
// If Source is set and if the given module path is present in it, then it reads the module source from it.
// It then returns the module and any associated error.
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
	if m, ok := s.modules[path]; ok {
		return m, nil
	}
	source, err := s.src.Source(path)
	if err != nil {
		return nil, err
	}
	var (
		mod              Module
		modulePath       = filepath.Dir(path)
		assignEntrypoint = s.entrypoint == nil
	)
	switch ext := filepath.Ext(path); ext {
	default:
		return nil, fmt.Errorf("unknown module extension %s", ext)
	case ".rf": // Regular reflow module.
		lx := &Parser{
			File: path,
			Body: bytes.NewReader(source),
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
		lx.Module.source = source
		s.modules[path] = lx.Module
		mod = lx.Module
	case ".rfx": // Reflow bundles.
		bundle, err := OpenBundle(bytes.NewReader(source), int64(len(source)))
		if err != nil {
			return nil, err
		}
		// Find the entrypoint; type check and evaluate it in a new, isolated
		// session.
		entry, args, entrypointPath, err := bundle.Entrypoint()
		if err != nil {
			return nil, err
		}
		sess := NewSession(bundle)
		sess.path = entrypointPath
		lx := &Parser{
			File: path,
			Body: bytes.NewReader(entry),
			Mode: ParseModule,
		}
		if err := lx.Parse(); err != nil {
			return nil, err
		}
		// The module is initalized inside of its own, isolated session.
		if err := lx.Module.Init(sess, sess.Types); err != nil {
			return nil, err
		}
		if err := lx.Module.InjectArgs(sess, args); err != nil {
			return nil, err
		}
		// We treat the module monolithically: its source is the contents
		// of the bundle itself.
		lx.Module.source = source
		s.modules[path] = lx.Module
		mod = lx.Module
	case ".reflow": // Reflow "v0" script.
		prog := &lang.Program{
			File: path,
			// This doesn't go through the same error reporting path as
			// everything else, but this is here just for compatibility, so
			// we'll live.
			Errors: os.Stderr,
		}
		if err := prog.ParseAndTypecheck(bytes.NewReader(source)); err != nil {
			return nil, err
		}
		// We have provide v0module with the path, and not the prog,
		// since it can potentially mint multiple instances.
		m := &v0module{
			params: make(map[string]string),
			path:   path,
			typ:    prog.ModuleType(),
			source: source,
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
		mod = m
	}
	if assignEntrypoint {
		s.entrypoint = mod
		s.entrypointPath = modulePath
	}
	return mod, nil
}

// Bundle creates a bundle that represents a self-contained Reflow
// module. Its entry point is the first module that was opened in the
// session.
func (s *Session) Bundle() *Bundle {
	bundle := new(Bundle)
	bundle.files = make(map[digest.Digest][]byte)
	bundle.manifest.Files = make(map[string]digest.Digest)
	for path, mod := range s.modules {
		var (
			p = mod.Source()
			d = reflow.Digester.FromBytes(p)
		)
		bundle.files[d] = p
		bundle.manifest.Files[path] = d
		if mod == s.entrypoint {
			bundle.manifest.Entrypoint = d
			bundle.manifest.Args = mod.InjectedArgs()
		}
	}
	bundle.manifest.EntrypointPath = s.entrypointPath
	return bundle
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

// Warn formats a message in the manner of fmt.Sprint and
// writes it as session warning.
func (s *Session) Warn(pos scanner.Position, v ...interface{}) {
	if s == nil || s.Stdwarn == nil {
		return
	}
	fmt.Fprintf(s.Stdwarn, "%s: warning: %s\n", pos, fmt.Sprint(v...))
	s.mu.Lock()
	s.nwarn++
	s.mu.Unlock()
}

// Warnf formats a message in the manner of fmt.Sprintf and
// writes it as a session warning.
func (s *Session) Warnf(pos scanner.Position, format string, v ...interface{}) {
	if s == nil || s.Stdwarn == nil {
		return
	}
	fmt.Fprintf(s.Stdwarn, "%s: warning: %s\n", pos, fmt.Sprintf(format, v...))
	s.mu.Lock()
	s.nwarn++
	s.mu.Unlock()
}

// NWarn returns the number of warnings emitted in this session.
func (s *Session) NWarn() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nwarn
}

// Sourcer is an interface that provides access to Reflow source
// files.
type Sourcer interface {
	// Source returns the source bytes for the provided path.
	Source(path string) ([]byte, error)
}

// Filesystem is a Sourcer that reads from the local file system.
var Filesystem Sourcer = filesystem{}

type filesystem struct{}

func (filesystem) Source(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
