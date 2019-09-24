// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package filerepo implements a filesystem-backed repository. It stores
// objects in a directory on disk; the objects are named by the
// string representation of their digest, i.e., of the form
// sha256:d60e67ce9....
package filerepo

import (
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"golang.org/x/sync/singleflight"
)

// Repository implements a filesystem-backed Repository.
type Repository struct {
	// The root directory for this repository. This directory contains
	// all objects.
	Root string

	Log *log.Logger

	// RepoURL may be set to a URL that represents this repository.
	RepoURL *url.URL

	read, write singleflight.Group
}

// Path returns the filesystem directory and full path of the object with a given digest.
func (r *Repository) Path(id digest.Digest) (dir, path string) {
	dir = filepath.Join(r.Root, id.Hex()[:2])
	return dir, filepath.Join(dir, id.Hex()[2:])
}

// Install links the given file into the repository, named by digest.
func (r *Repository) Install(file string) (reflow.File, error) {
	f, err := os.Open(file)
	if err != nil {
		return reflow.File{}, err
	}
	defer f.Close()
	w := reflow.Digester.NewWriter()
	n, err := io.Copy(w, f)
	if err != nil {
		return reflow.File{}, err
	}
	d := w.Digest()
	return reflow.File{ID: d, Size: n}, r.InstallDigest(d, file)
}

// InstallDigest installs a file at the given digest. The caller guarantees
// that the file's bytes have the digest d.
func (r *Repository) InstallDigest(d digest.Digest, file string) error {
	file, err := filepath.EvalSymlinks(file)
	if err != nil {
		return err
	}
	dir, path := r.Path(d)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}
	err = os.Link(file, path)
	if os.IsExist(err) {
		err = nil
	}
	if err != nil {
		// Copy if file was reported to be on a different device.
		if linkErr, ok := err.(*os.LinkError); ok && linkErr.Err == syscall.EXDEV {
			f, ferr := os.Open(file)
			if ferr != nil {
				return ferr
			}
			_, err = r.Put(context.Background(), f)
		}
	}
	return err
}

// Stat retrieves metadata for files stored in the repository.
func (r *Repository) Stat(ctx context.Context, id digest.Digest) (reflow.File, error) {
	_, path := r.Path(id)
	info, err := os.Stat(path)
	if err != nil {
		return reflow.File{}, errors.E("stat", r.Root, id, err)
	}
	return reflow.File{ID: id, Size: info.Size()}, nil
}

// Get retrieves the object named by a digest.
func (r *Repository) Get(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	_, path := r.Path(id)
	rc, err := os.Open(path)
	if err != nil {
		return nil, errors.E("get", r.Root, id, err)
	}
	return rc, nil
}

// ReadFrom installs an object directly from a foreign repository. If
// the foreign repository supports supports GetFile, it is used to
// download directly.
//
//	GetFile(ctx context.Context, id digest.Digest, w io.WriterAt) (int64, error)
//
// This is used by implementations like S3, which can download
// multiple chunks at once, and requires an io.WriterAt so that
// chunks needn't be buffered in memory until the download is
// contiguous.
//
// ReadFrom singleflights concurrent downloads from the same key
// regardless of repository origin.
func (r *Repository) ReadFrom(ctx context.Context, id digest.Digest, u *url.URL) error {
	if ok, _ := r.Contains(id); ok {
		return nil
	}
	_, err, _ := r.read.Do(id.String(), func() (interface{}, error) {
		repo, err := repository.Dial(u.String())
		if err != nil {
			return nil, err
		}
		type getFiler interface {
			GetFile(ctx context.Context, id digest.Digest, w io.WriterAt) (int64, error)
		}
		if gf, ok := repo.(getFiler); ok {
			temp, err := r.TempFile("getfile-")
			if err != nil {
				return nil, err
			}
			defer os.Remove(temp.Name())
			_, err = gf.GetFile(ctx, id, temp)
			if err != nil {
				return nil, err
			}
			return nil, r.InstallDigest(id, temp.Name())
		}

		rc, err := repo.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		id2, err := r.Put(ctx, rc)
		if err != nil {
			return nil, err
		}
		if id != id2 {
			return nil, errors.E("readfrom", u.String(), errors.Integrity, errors.Errorf("%v != %v", id, id2))
		}
		return nil, nil
	})
	return err
}

// WriteTo writes an object directly to a foreign repository.
// If the foreign repository supports the PutFile method, it is
// used.
//
// 	PutFile(context.Context, reflow.File, io.Reader) error
//
// PutFile is useful for implementations like S3 which can upload
// multiple chunks at a time, and requires direct reader (and,
// dynamically, io.Seeker) access.
//
// WriteTo singleflights requests of the same ID and url.
func (r *Repository) WriteTo(ctx context.Context, id digest.Digest, u *url.URL) error {
	if ok, _ := r.Contains(id); !ok {
		return errors.E("writeto", r.Root, id, u.String(), errors.NotExist)
	}
	_, err, _ := r.write.Do(id.String()+u.String(), func() (interface{}, error) {
		repo, err := repository.Dial(u.String())
		if err != nil {
			return nil, err
		}
		dir, path := r.Path(id)
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, errors.E("get", r.Root, id, err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		type putFiler interface {
			PutFile(context.Context, reflow.File, io.Reader) error
		}
		if pf, ok := repo.(putFiler); ok {
			return nil, pf.PutFile(ctx, reflow.File{ID: id, Size: info.Size()}, file)
		}
		id2, err := repo.Put(ctx, file)
		if err != nil {
			return nil, err
		}
		if id != id2 {
			return nil, errors.E("writeto", r.Root, id, errors.Integrity, errors.Errorf("%v != %v", id, id2))
		}
		return nil, nil
	})
	return err
}

// URL returns the url of this repository.
func (r *Repository) URL() *url.URL {
	return r.RepoURL
}

// Contains tells whether the repository has an object with a digest.
func (r *Repository) Contains(id digest.Digest) (bool, error) {
	_, path := r.Path(id)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// Put installs an object into the repository. Its digest identity is returned.
func (r *Repository) Put(ctx context.Context, body io.Reader) (digest.Digest, error) {
	temp, err := r.TempFile("create-")
	if err != nil {
		return digest.Digest{}, err
	}
	defer os.Remove(temp.Name())
	dw := reflow.Digester.NewWriter()
	done := make(chan error, 1)
	// This is a workaround to make sure that copies respect
	// context cancellations. Note that the underlying copy is
	// not actually cancelled, so this could lead to goroutine
	// leaks.
	go func() {
		_, err = io.Copy(temp, io.TeeReader(body, dw))
		temp.Close()
		done <- err
	}()
	select {
	case <-ctx.Done():
		return digest.Digest{}, ctx.Err()
	case err := <-done:
		if err != nil {
			return digest.Digest{}, err
		}
		dgst := dw.Digest()
		return dgst, r.InstallDigest(dgst, temp.Name())
	}
}

// Materialize takes a mapping of path-to-object, and hardlinks the
// corresponding objects from the repository into the given root.
func (r *Repository) Materialize(root string, binds map[string]digest.Digest) error {
	dirsMade := map[string]bool{}
	for path, id := range binds {
		path = filepath.Join(root, path)
		dir := filepath.Dir(path)
		if !dirsMade[dir] {
			if err := os.MkdirAll(dir, 0777); err != nil {
				return err
			}
			// TODO(marius): also insert parents
			dirsMade[dir] = true
		}
		os.Remove(path) // best effort
		_, rpath := r.Path(id)
		if err := os.Link(rpath, path); err != nil {
			return err
		}
	}
	return nil
}

// Vacuum moves all objects from the given repository to this one.
func (r *Repository) Vacuum(ctx context.Context, repo *Repository) error {
	var w walker
	w.Init(repo)
	for w.Scan() {
		if err := r.InstallDigest(w.Digest(), w.Path()); err != nil {
			return err
		}
	}
	repo.Collect(ctx, nil) // ignore errors
	return w.Err()
}

// CollectWithThreshold removes from this repository any objects not in the
// Liveset and whose creation times are not more recent than the
// threshold time.
func (r *Repository) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, threshold time.Time, dryRun bool) error {
	return errors.E("collectwiththreshold", errors.NotSupported)
}

// Collect removes any objects in the repository that are not also in
// the live set.
func (r *Repository) Collect(ctx context.Context, live liveset.Liveset) error {
	var w walker
	w.Init(r)
	var (
		n    int
		size int64
	)
	for w.Scan() {
		if live != nil && live.Contains(w.Digest()) {
			continue
		}
		size += w.Info().Size()
		if err := os.Remove(w.Path()); err != nil {
			r.Log.Errorf("remove %q: %v", w.Path(), err)
		}
		// Clean up object subdirectories. (Ignores failure when nonempty.)
		os.Remove(filepath.Dir(w.Path()))
		n++
	}
	if live != nil {
		r.Log.Printf("collected %v objects (%s)", n, data.Size(size))
	}
	return w.Err()
}

// TempFile creates and returns a new temporary file adjacent to the
// repository. Files created by TempFile can be efficiently ingested
// by Repository.Install. The caller is responsible for cleaning up
// temporary files.
func (r *Repository) TempFile(prefix string) (*os.File, error) {
	dir := filepath.Join(r.Root, "tmp")
	os.MkdirAll(dir, 0777)
	return ioutil.TempFile(dir, prefix)
}
