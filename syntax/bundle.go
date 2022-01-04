// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/reflow/log"
)

const manifestPath = "manifest"

type bundleManifest struct {
	Entrypoint     digest.Digest
	EntrypointPath string
	Args           []string
	// File stores a mapping of paths in this bundle to the hash
	// of the the file's contents. The files are stored by hash directly
	// in the zip file.
	Files map[string]digest.Digest
}

// Bundle represents a self-contained Reflow module. A bundle
// contains all necessary sources, arguments, and image references
// that are required to instantiate the module.
//
// Bundle implements Sourcer.
type Bundle struct {
	manifest bundleManifest
	files    map[digest.Digest][]byte
}

// Source retrieves the source bytes associated with
// the provided path.
func (b *Bundle) Source(path string) (p []byte, d digest.Digest, err error) {
	var ok bool
	if d, ok = b.manifest.Files[path]; !ok {
		err = os.ErrNotExist
		return
	}
	if p, ok = b.files[d]; !ok {
		err = fmt.Errorf("invalid bundle: file %s (%v) is missing", path, d)
	}
	return
}

// Entrypoint returns the bundle's entrypoint: its source,
// command line arguments (which parameterize the module),
// or an error.
func (b *Bundle) Entrypoint() (source []byte, args []string, path string, err error) {
	p := b.files[b.manifest.Entrypoint]
	if p == nil {
		return nil, nil, "", errors.New("invalid bundle: entrypoint module is missing")
	}
	return p, b.manifest.Args, b.manifest.EntrypointPath, nil
}

// WriteTo writes an archive (ZIP formatted) of this bundle to the provided
// io.Writer. Archives written by Write can be opened by OpenBundleModule.
func (b *Bundle) WriteTo(w io.Writer) error { // "go vet" complaint expected
	z := zip.NewWriter(w)
	digests := make([]digest.Digest, 0, len(b.files))
	for d := range b.files {
		digests = append(digests, d)
	}
	sort.Slice(digests, func(i, j int) bool {
		return digests[i].String() < digests[j].String()
	})
	for _, d := range digests {
		data := b.files[d]
		f, err := z.Create(d.String())
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
			return err
		}
	}
	f, err := z.Create(manifestPath)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(b.manifest); err != nil {
		return err
	}
	return z.Close()
}

var (
	// bundleOnce makes sure we load a bundle (identified by its source digest) only once.
	bundleOnce once.Map
	// bundleModCache maps a bundle's digest to a Bundle object.
	bundleModCache sync.Map // map[digest.Digest]*ModuleImpl
)

// OpenBundleModule opens a bundle archive saved by Bundle.Write and parses it into a Module.
// The arguments are:
// src: The source code (expected to be a bundle archive saved using Bundle.Write.
// size: Size of the source code bytes.
// srcPath: Path of the source which was used to fetch the source from the underlying Sourcer.
// srcDigest: Digest of the underlying source code
// OpenBundleModule uses a cache to retrieve known already parsed bundles (by digest).
func OpenBundleModule(src io.ReaderAt, size int64, srcPath string, srcDigest digest.Digest) (Module, error) {
	err := bundleOnce.Do(srcDigest, func() error {
		bundle, err := openBundle(src, size)
		if err != nil {
			return err
		}
		mod, err := parseBundle(bundle, srcPath)
		if err != nil {
			return err
		}
		bundleModCache.Store(srcDigest, mod)
		return nil
	})
	if err != nil {
		return nil, err
	}
	v, _ := bundleModCache.Load(srcDigest)
	return v.(*ModuleImpl), nil
}

// openBundle opens a bundle archive saved by Bundle.Write.
func openBundle(r io.ReaderAt, size int64) (*Bundle, error) {
	z, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}
	bundle := &Bundle{files: make(map[digest.Digest][]byte)}
	var manifest *zip.File
	for _, file := range z.File {
		if file.Name == manifestPath {
			manifest = file
			continue
		}
		d, err := digest.Parse(file.Name)
		if err != nil {
			log.Printf("unexpected file name %s in bundle", file.Name)
			continue
		}
		f, err := file.Open()
		if err != nil {
			return nil, err
		}
		bundle.files[d], err = ioutil.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
	}
	if manifest == nil {
		return nil, errors.New("bundle is missing manifest")
	}
	f, err := manifest.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&bundle.manifest); err != nil {
		return nil, err
	}
	return bundle, nil
}

// parseBundle parses a bundle into a module.
func parseBundle(bundle *Bundle, path string) (*ModuleImpl, error) {
	// Find the entrypoint; type check and evaluate it in a new, isolated session.
	entry, args, entrypointPath, err := bundle.Entrypoint()
	if err != nil {
		return nil, err
	}
	sess := NewSession(bundle)
	sess.path = entrypointPath
	lx := Parser{
		File: path,
		Body: bytes.NewReader(entry),
		Mode: ParseModule,
	}
	if err = lx.Parse(); err != nil {
		return nil, err
	}
	mod := lx.Module
	if err = mod.Init(sess, sess.Types); err != nil {
		return nil, err
	}
	if err = mod.InjectArgs(sess, args); err != nil {
		return nil, err
	}
	return mod, nil
}
