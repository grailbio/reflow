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

	"github.com/grailbio/base/digest"
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
func (b *Bundle) Source(path string) ([]byte, error) {
	d, ok := b.manifest.Files[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	p, ok := b.files[d]
	if !ok {
		return nil, fmt.Errorf("invalid bundle: file %s (%v) is missing", path, d)
	}
	return p, nil
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
// io.Writer. Archives written by Write can be opened by OpenBundle.
func (b *Bundle) WriteTo(w io.Writer) error {
	z := zip.NewWriter(w)
	for d, data := range b.files {
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

// OpenBundle opens a bundle archive saved by Bundle.Write.
func OpenBundle(r io.ReaderAt, size int64) (*Bundle, error) {
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
		f.Close()
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
