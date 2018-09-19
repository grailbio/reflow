// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/pool"
)

var (
	errZombieAlloc = errors.New("zombie alloc")
	errZombieExec  = errors.New("zombie exec")
)

// Zombies are reanimated allocs. They are used to inspect allocs (and their objects)
// after they have died.
type zombie struct {
	manager *Pool
	dir     string
	id      string
}

func (z *zombie) path(id digest.Digest, elem ...string) string {
	elem = append([]string{z.dir, execsDir, id.Hex()}, elem...)
	return filepath.Join(elem...)
}

func (z *zombie) manifest(id digest.Digest) (Manifest, error) {
	file, err := os.Open(z.path(id, manifestPath))
	if err != nil {
		return Manifest{}, err
	}
	defer file.Close()
	var manifest Manifest
	err = json.NewDecoder(file).Decode(&manifest)
	return manifest, err
}

func (z *zombie) Pool() pool.Pool             { return z.manager }
func (z *zombie) ID() string                  { return z.id }
func (z *zombie) URI() string                 { return z.id }
func (z *zombie) Resources() reflow.Resources { return reflow.Resources{} }
func (z *zombie) Keepalive(ctx context.Context, iv time.Duration) (time.Duration, error) {
	return time.Duration(0), errors.E("keepalive", z.id, fmt.Sprint(iv), errors.NotSupported, errZombieAlloc)
}
func (z *zombie) Free(ctx context.Context) error {
	// TODO: this may make sense eventually, to clear up logs, etc.
	return errors.E("free", z.id, errors.NotSupported, errZombieAlloc)
}
func (z *zombie) Repository() reflow.Repository { return nil }

func (z *zombie) Put(ctx context.Context, id digest.Digest, obj reflow.ExecConfig) (reflow.Exec, error) {
	return nil, errors.E("put", z.id, id, fmt.Sprint(obj), errors.NotSupported, errZombieAlloc)
}

func (z *zombie) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	_, err := os.Stat(z.path(id))
	if err != nil {
		return nil, errors.E("get", z.id, id, err)
	}
	return &zombieExec{z, id}, nil
}

func (z *zombie) Remove(ctx context.Context, id digest.Digest) error {
	return errors.E("remove", z.id, id, errors.NotSupported, errZombieExec)
}

func (z *zombie) Open(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	return nil, errors.E("open", z.id, id, errors.NotSupported, errZombieExec)
}

func (z *zombie) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	path := filepath.Join(z.dir, metaPath)
	f, err := os.Open(path)
	if err != nil {
		return pool.AllocInspect{}, errors.E("inspect", z.id, err)
	}
	inspect := pool.AllocInspect{ID: z.id}
	if err := json.NewDecoder(f).Decode(&inspect.Meta); err != nil {
		return pool.AllocInspect{}, errors.E("inspect", z.id, err)
	}
	return inspect, nil
}

func (z *zombie) Execs(ctx context.Context) ([]reflow.Exec, error) {
	dir := filepath.Join(z.dir, execsDir)
	file, err := os.Open(dir)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, errors.E("execs", z.id, err)
	}
	infos, err := file.Readdir(-1)
	if err != nil {
		return nil, errors.E("execs", z.id, err)
	}
	var execs []reflow.Exec
	for _, info := range infos {
		id, err := reflow.Digester.Parse(info.Name())
		if err != nil {
			continue
		}
		execs = append(execs, &zombieExec{z, id})
	}
	return execs, nil
}

func (z *zombie) Load(ctx context.Context, fs reflow.Fileset) (reflow.Fileset, error) {
	return reflow.Fileset{}, errors.E("load", errors.NotSupported, errZombieExec)
}

type zombieExec struct {
	*zombie
	id digest.Digest
}

func (z *zombieExec) objectPath(elems ...string) string {
	return z.zombie.path(z.id, elems...)
}

func (z *zombieExec) ID() digest.Digest { return z.id }

func (z *zombieExec) URI() string { return z.zombie.URI() + "/" + z.id.Hex() }

func (z *zombieExec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
	manifest, err := z.manifest(z.id)
	if err != nil {
		return reflow.ExecInspect{}, errors.E("inspect", z.URI(), err)
	}
	inspect := reflow.ExecInspect{
		Config:  manifest.Config,
		Docker:  manifest.Docker,
		State:   "zombie",
		Status:  "zombie",
		Profile: manifest.Stats.Profile(),
	}
	// S3 execs don't have Docker manifests.
	if manifest.Docker.ContainerJSONBase == nil {
		return inspect, nil
	}
	if state := manifest.Docker.State; state != nil && state.ExitCode != 0 {
		inspect.Error = errors.Recover(errors.E("exec", z.URI(), errors.Errorf("process exited with status %d", state.ExitCode)))
	}
	return inspect, nil
}

func (z *zombieExec) Logs(ctx context.Context, stdout, stderr, follow bool) (io.ReadCloser, error) {
	if !stdout && !stderr {
		return nil, errors.E("logs",
			z.URI(), fmt.Sprint(stdout), fmt.Sprint(stderr),
			errors.New("one of stdout, stderr must be defined"))
	}
	var files []*os.File
	if stdout {
		file, err := os.Open(z.objectPath("stdout"))
		if err != nil {
			return nil, errors.E("logs", z.URI(), fmt.Sprint(stdout), fmt.Sprint(stderr), err)
		}
		files = append(files, file)
	}
	if stderr {
		file, err := os.Open(z.objectPath("stderr"))
		if err != nil {
			return nil, errors.E("logs", z.URI(), fmt.Sprint(stdout), fmt.Sprint(stderr), err)
		}
		files = append(files, file)
	}
	readers := make([]io.Reader, len(files))
	closers := make([]io.Closer, len(files))
	for i, f := range files {
		readers[i] = f
		closers[i] = f
	}
	return newAllCloser(io.MultiReader(readers...), closers...), nil
}

func (z *zombieExec) Shell(ctx context.Context) (io.ReadWriteCloser, error) {
	return nil, errors.New("cannot shell into a zombie exec")
}

func (z *zombieExec) Wait(ctx context.Context) error {
	return errors.E("wait", z.URI(), errors.NotSupported, errZombieExec)
}

func (z *zombieExec) Result(ctx context.Context) (reflow.Result, error) {
	manifest, err := z.manifest(z.id)
	if err != nil {
		return reflow.Result{}, err
	}
	return manifest.Result, nil
}

func (z *zombieExec) Promote(ctx context.Context) error {
	return errors.E("promote", z.URI(), errors.NotSupported, errZombieExec)
}
