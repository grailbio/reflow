// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"docker.io/go-docker"
	"docker.io/go-docker/api/types"
	"docker.io/go-docker/api/types/container"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/internal/walker"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/filerepo"
	"golang.org/x/sync/errgroup"
)

// Disk layout:
// 	Prefix/Dir/objects/<digest>
//	Prefix/Dir/execs/<digest>/...
const (
	objectsDir = "objects"
	execsDir   = "execs"
)

const (
	defaultDigestLimit   = 60
	defaultDownloadLimit = 60
)

// TODO(marius): configure this from profiles
const defaultRegion = "us-west-2"

var errDead = errors.New("executor is dead")

// Executor is a small management layer on top of exec. It implements
// reflow.Executor. Executor assumes that it has local access to the
// file system (perhaps with a prefix).
//
// Executor stores its state to disk and, when recovered, re-instantiates
// all execs (which in turn recover).
type Executor struct {
	// RunID of the run - <username>@grailbio.com/<hash>
	RunID string
	// ID is the ID of the executor. It is the URI of the executor and also
	// the prefix used in any Docker containers whose exec's are
	// children of this executor.
	ID string
	// Prefix is the filesystem prefix used to access paths on disk. This is
	// defined so that the executor can run inside of a Docker container
	// (which has the host's filesystem exported at this prefix).
	Prefix string
	// Dir is the root directory of this executor. All of its state is contained
	// within it.
	Dir string
	// Client is the Docker client used by this executor.
	Client *docker.Client
	// Authenticator is used to pull images that are stored on Amazon's ECR
	// service.
	Authenticator ecrauth.Interface
	// AWSImage is a Docker image that contains the 'aws' tool.
	// This is used to implement S3 interns and externs.
	AWSImage string
	// AWSCreds is an AWS credentials provider, used for S3 operations
	// and "$aws" passthroughs.
	AWSCreds *credentials.Credentials
	// Log is this executor's logger where operational status is printed.
	Log *log.Logger

	// ExternalS3 defines whether to use external processes (AWS CLI tool
	// running in docker) for S3 operations. At the moment, this flag only
	// works for interns.
	ExternalS3 bool

	// FileRepository is the (file-based) object repository used by this
	// Executor. It may be provided by the user, or else it is set to a
	// default implementation when (*Executor).Start is called.
	FileRepository *filerepo.Repository

	// HardMemLimit restricts an exec's memory limit to the exec's resource requirements
	HardMemLimit bool

	Blob blob.Mux

	// remoteStream is the client used to write logs to a remote cloud
	// stream.
	remoteStream remoteStream

	resources reflow.Resources

	// The executor's context. This is used to propagate
	// cancellation to execs.
	cancel context.CancelFunc
	ctx    context.Context

	mu         sync.Mutex
	dead       bool                   // tells whether the executor is dead
	execs      map[digest.Digest]exec // the set of execs managed by this executor.
	oomTracker *oomTracker

	// reference count of the objects in the executor repository.
	refCountsMu   sync.Mutex
	refCounts     map[digest.Digest]refCount
	refCountsCond *sync.Cond
	deadObjects   map[digest.Digest]bool
	gcing         chan struct{}
}

type refCount struct {
	count          int64
	lastAccessTime time.Time
}

// incr increments the reference count of the specified object while
// ensuring that it waits for an GC in progress on that object.
func (e *Executor) incr(d digest.Digest) {
	e.refCountsMu.Lock()
	for e.deadObjects[d] {
		e.refCountsCond.Wait()
	}
	r := e.refCounts[d]
	e.refCounts[d] = refCount{count: r.count + 1, lastAccessTime: time.Now()}
	e.refCountsMu.Unlock()
}

func (e *Executor) decr(id digest.Digest) {
	e.refCountsMu.Lock()
	saved := e.refCounts[id]
	if e.deadObjects[id] {
		panic(fmt.Sprintf("gc: decrement while gc is in progress: %v", id))
	}
	e.refCounts[id] = refCount{saved.count - 1, saved.lastAccessTime}
	e.refCountsMu.Unlock()
}

// Start initializes the executor and recovers previously stored
// state. It re-initializes all stored execs.
func (e *Executor) Start() error {
	e.refCountsCond = sync.NewCond(&e.refCountsMu)
	e.deadObjects = make(map[digest.Digest]bool)
	e.execs = map[digest.Digest]exec{}
	e.refCounts = make(map[digest.Digest]refCount)
	e.ctx, e.cancel = context.WithCancel(context.Background())
	// Monitor /dev/kmsg for OOMs.
	e.oomTracker = newOOMTracker()
	go e.oomTracker.Monitor(e.ctx, e.Log)

	if e.FileRepository == nil {
		e.FileRepository = &filerepo.Repository{Root: filepath.Join(e.Prefix, e.Dir, objectsDir)}
	}
	os.MkdirAll(e.FileRepository.Root, 0777)
	tempdir := filepath.Join(e.Prefix, e.Dir, "download")
	if err := os.MkdirAll(tempdir, 0777); err != nil {
		return err
	}

	execdir := filepath.Join(e.Prefix, e.Dir, execsDir)
	file, err := os.Open(execdir)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	infos, err := file.Readdir(-1)
	if err != nil {
		return err
	}
	for _, info := range infos {
		id, err := reflow.Digester.Parse(info.Name())
		if err != nil {
			e.Log.Errorf("skipping path %s: %v", info.Name(), err)
			continue
		}
		// Try to restore the exec's state.
		path := e.execPath(id, manifestPath)
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			e.Log.Errorf("no manifest for exec %s", id)
			continue
		} else if err != nil {
			e.Log.Errorf("open %v: %v", path, err)
			continue
		}
		var m Manifest
		if err := json.NewDecoder(f).Decode(&m); err != nil {
			e.Log.Errorf("decode %v: %v", path, err)
			continue
		}
		var x exec
		switch m.Type {
		case execDocker:
			stdout, stderr := e.getRemoteStreams(id, true, true)
			dx := newDockerExec(id, e, reflow.ExecConfig{},
				log.New(stdout, log.InfoLevel), log.New(stderr, log.InfoLevel))
			dx.Manifest = m
			x = dx
		case execBlob:
			_, stderr := e.getRemoteStreams(id, false, true)
			blobx := &blobExec{
				ExecID:       id,
				transferType: m.Config.Type,
				log:          e.Log.Tee(stderr, ""),
				x:            e,
			}
			blobx.Init(e)
			blobx.Manifest = m
			x = blobx
		default:
			e.Log.Errorf("unknown exec type %v", m.Type)
			continue
		}
		e.execs[id] = x
		go x.Go(e.ctx)
	}
	return nil
}

// ensureImage returns nil when the image is known to be present
// at the local Docker client.
// TODO(marius): image pulling may be(?) better off as part of the executor interface
func (e *Executor) ensureImage(ctx context.Context, ref string) error {
	return ensureImage(ctx, e.Client, e.Authenticator, ref, e.Log)
}

// execPath constructs a path for the exec with the given id.
func (e *Executor) execPath(id digest.Digest, elem ...string) string {
	elem = append([]string{e.Prefix, e.Dir, execsDir, id.Hex()}, elem...)
	return filepath.Join(elem...)
}

// execHostPath constructs a host path for the exec with the given id.
func (e *Executor) execHostPath(id digest.Digest, elem ...string) string {
	elem = append([]string{e.Dir, execsDir, id.Hex()}, elem...)
	return filepath.Join(elem...)
}

// URI returns the executor's ID.
func (e *Executor) URI() string { return e.ID }

func (e *Executor) getRemoteStreams(id digest.Digest, wantStdout, wantStderr bool) (so, se log.Outputter) {
	if e.remoteStream == nil {
		return
	}
	var err error
	instanceID := strings.Join([]string{e.RunID, e.URI(), id.Hex()}, "/")
	if wantStdout {
		so, err = e.remoteStream.NewStream(instanceID, stdout)
		if err != nil {
			e.Log.Errorf("creating remote logger stream: %v", err)
		}
	}
	if wantStderr {
		se, err = e.remoteStream.NewStream(instanceID, stderr)
		if err != nil {
			e.Log.Errorf("creating remote logger stream: %v", err)
		}
	}
	return
}

// Put idempotently defines a new exec with a given ID and config.
// The exec may be (deterministically) rewritten.
func (e *Executor) Put(ctx context.Context, id digest.Digest, cfg reflow.ExecConfig) (reflow.Exec, error) {
	if err := e.rewriteConfig(&cfg); err != nil {
		return nil, errors.E("put", id, fmt.Sprint(cfg), err)
	}
	e.mu.Lock()
	if e.dead {
		e.mu.Unlock()
		return nil, errors.E("put", id, errors.NotExist)
	}

	var x exec
	if obj := e.execs[id]; obj != nil {
		res, err := obj.Result(ctx)
		// Will return an existing obj only if either
		// - there was no error during its execution and the result wasn't an error
		// - or the only error we got back signifies that the obj isn't complete yet.
		if err == nil && res.Err == nil {
			x = obj
		} else if err != nil && strings.Contains(err.Error(), errExecNotComplete) {
			x = obj
		} else {
			e.Log.Debugf("put %s overwriting existing exec: %s", id.Short(), obj.URI())
			if err := obj.Kill(ctx); err != nil {
				e.Log.Debugf("kill existing %s: %v", id, err)
			}
			delete(e.execs, id)
		}
	}
	if x != nil {
		e.mu.Unlock()
		return x, nil
	}

	switch cfg.Type {
	case intern, extern:
		u, err := url.Parse(cfg.URL)
		if err != nil {
			e.mu.Unlock()
			return nil, err
		}
		switch u.Scheme {
		case "localfile":
			x = newLocalfileExec(id, e, cfg)
		default:
			_, stderr := e.getRemoteStreams(id, false, true)
			blob := &blobExec{
				ExecID:       id,
				transferType: cfg.Type,
				log:          e.Log.Tee(stderr, ""),
				x:            e,
			}
			blob.Config = cfg
			blob.Init(e)
			x = blob
		}
	default:
		stdout, stderr := e.getRemoteStreams(id, true, true)
		x = newDockerExec(id, e, cfg, log.New(stdout, log.InfoLevel), log.New(stderr, log.InfoLevel))
	}
	e.execs[id] = x
	e.mu.Unlock()
	go x.Go(e.ctx)
	return x, x.WaitUntil(execInit)
}

// Get returns the exec named ID, or an errors.NotExist if the exec
// does not exist.
func (e *Executor) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.dead {
		return nil, errors.E("get", id, errors.NotExist, errDead)
	}
	exec := e.execs[id]
	if exec == nil {
		return nil, errors.E("get", id, errors.NotExist)
	}
	return exec, exec.WaitUntil(execInit)
}

// Remove removes the exec named id.
func (e *Executor) Remove(ctx context.Context, id digest.Digest) error {
	if e.dead {
		e.mu.Unlock()
		return nil
	}
	e.mu.Lock()
	x := e.execs[id]
	e.mu.Unlock()
	if x == nil {
		// it's an idempotent operation
		return nil
	}
	if err := x.Kill(ctx); err != nil {
		return err
	}
	e.mu.Lock()
	delete(e.execs, id)
	e.mu.Unlock()
	return nil
}

// Unload decrements the reference count of the fileset objects. If any object's reference
// count is 0, then unload marks it for deletion. A GC goroutine separately collects these
// marked objects. The returned channel is closed when the GC is complete.
func (e *Executor) unload(ctx context.Context, fs reflow.Fileset) (done <-chan struct{}, err error) {
	files := fs.Files()
	e.refCountsMu.Lock()
	for _, f := range files {
		d := f.Digest()
		r := e.refCounts[d]
		e.refCounts[d] = refCount{count: r.count - 1, lastAccessTime: r.lastAccessTime}
		if e.refCounts[d].count < 0 {
			panic(fmt.Sprintf("unload: negative ref count: %v", f.Digest()))
		}
		if e.refCounts[d].count == 0 {
			e.deadObjects[d] = true
		}
	}
	if e.gcing != nil {
		done = e.gcing
		e.refCountsMu.Unlock()
		return
	}
	e.gcing = make(chan struct{})
	done = e.gcing
	e.refCountsMu.Unlock()
	go func() {
		e.refCountsMu.Lock()
		defer e.refCountsMu.Unlock()
		for len(e.deadObjects) > 0 {
			for id := range e.deadObjects {
				e.refCountsMu.Unlock()
				if err := e.FileRepository.Remove(id); err != nil {
					e.Log.Errorf("gc: unload dead collect: %v", err)
				}
				e.refCountsMu.Lock()
				delete(e.deadObjects, id)
				if e.refCounts[id].count > 0 {
					panic(fmt.Sprintf("gc: refcount %v not 0: %v", id.Short(), e.refCounts[id].count))
				}
				delete(e.refCounts, id)
				e.refCountsCond.Broadcast()
			}
		}
		close(e.gcing)
		e.gcing = nil
	}()
	return
}

// Unload unloads the fileset from the executor repository. When the fileset's reference count drops to zero,
// the executor may choose to remove the fileset from its repository.
func (e *Executor) Unload(ctx context.Context, fs reflow.Fileset) error {
	_, err := e.unload(ctx, fs)
	return err
}

// VerifyIntegrity verifies the integrity of the given set of files
func (e *Executor) VerifyIntegrity(ctx context.Context, fs reflow.Fileset) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	files := fs.Files()
	err := traverse.Limit(runtime.NumCPU()).Each(len(files), func(i int) error {
		file := files[i]
		if file.IsRef() {
			return errors.E(fmt.Sprintf("unresolved file %s", file), errors.Invalid)
		}
		if _, err := e.FileRepository.Stat(ctx, file.ID); err != nil {
			return errors.E(errors.NotExist, err)
		}
		rc, err := e.FileRepository.Get(ctx, file.ID)
		if err != nil {
			return err
		}
		defer func() { _ = rc.Close() }()
		w := reflow.Digester.NewWriter()
		if _, err := io.Copy(w, rc); err != nil {
			return err
		}
		d := w.Digest()
		if file.ID != d {
			return errors.E(fmt.Sprintf("digest %s mismatches ID %s", d.Short(), file.ID.Short()), file.ID, errors.Integrity)
		}
		return nil
	})
	if err != nil {
		return errors.E("verifyintegrity", err)
	}
	return nil
}

func (e *Executor) refCount(fs reflow.Fileset) {
	for _, f := range fs.Files() {
		e.incr(f.Digest())
	}
}

// Load loads the fileset into the executor repository. If the fileset is resolved, it is loaded from the
// specified backing repository. Else the file is loaded from its source.
func (e *Executor) Load(ctx context.Context, repo *url.URL, fs reflow.Fileset) (reflow.Fileset, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		resolved = make(map[digest.Digest]reflow.File)
		files    = fs.Files()
		tempRepo filerepo.Repository
		err      error
	)
	tempRepo.Root, err = ioutil.TempDir(e.FileRepository.Root, "temp-load")
	defer os.RemoveAll(tempRepo.Root)
	if err != nil {
		return reflow.Fileset{}, err
	}
	err = traverse.Each(len(files), func(i int) error {
		file := files[i]
		if !file.IsRef() {
			d := file.Digest()
			e.incr(d)
			// TODO(pgopal): change ReadFrom to return (reflow.File, error).
			rerr := e.FileRepository.ReadFrom(ctx, d, repo)
			if rerr != nil {
				e.decr(d)
				return rerr
			}
			var res reflow.File
			if res, rerr = e.FileRepository.Stat(ctx, d); rerr != nil {
				return rerr
			}
			mu.Lock()
			resolved[d] = res
			mu.Unlock()
			return nil
		}
		var (
			incr bool
			res  reflow.File
		)
		if !file.ContentHash.IsZero() {
			incr = true
			e.incr(file.ContentHash)
			res, err = fileFromRepo(ctx, e.FileRepository, file)
		}
		if file.ContentHash.IsZero() || err != nil {
			bucket, key, err := e.Blob.Bucket(ctx, file.Source)
			if err != nil {
				return err
			}
			dl := download{
				Bucket: bucket,
				Key:    key,
				File:   file,
				Log:    e.Log,
			}
			res, err = dl.Do(ctx, &tempRepo)
			if err != nil {
				return err
			}
			if !incr {
				e.incr(res.Digest())
			}
		}
		mu.Lock()
		resolved[file.Digest()] = res
		mu.Unlock()
		return nil
	})
	if err != nil {
		return reflow.Fileset{}, err
	}
	if err := e.FileRepository.Vacuum(ctx, &tempRepo); err != nil {
		return reflow.Fileset{}, err
	}
	x, ok := fs.Subst(resolved)
	if !ok {
		return reflow.Fileset{}, errors.E(errors.Invalid, "load", fmt.Sprint(fs), errors.New("fileset not resolved"))
	}
	return x, nil
}

// Repository returns the repository attached to this executor.
func (e *Executor) Repository() reflow.Repository { return e.FileRepository }

// SetResources sets the resources reported by Resources() to r.
func (e *Executor) SetResources(r reflow.Resources) {
	e.resources = r
}

// Resources reports the total capacity of this executor.
func (e *Executor) Resources() reflow.Resources {
	return e.resources
}

// Execs returns all execs managed by this executor.
func (e *Executor) Execs(ctx context.Context) ([]reflow.Exec, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	execs := make([]reflow.Exec, len(e.execs))
	i := 0
	for _, e := range e.execs {
		execs[i] = e
		i++
	}
	return execs, nil
}

func (e *Executor) promote(ctx context.Context, res reflow.Fileset, repo *filerepo.Repository) error {
	e.refCount(res)
	return e.FileRepository.Vacuum(ctx, repo)
}

// Kill disposes of the executors and all of its execs. It also sets
// the executor's "dead" flag, so that all future operations on the
// executor returns an error.
func (e *Executor) Kill(ctx context.Context) error {
	e.mu.Lock()
	if e.dead {
		e.mu.Unlock()
		return errors.E("kill", e.ID, errors.NotExist, errDead)
	}
	e.dead = true
	e.mu.Unlock()
	e.cancel()
	// After this point, we have exclusive access to e.execs.
	for _, x := range e.execs {
		x.Wait(ctx)
	}
	// Now try to collect any vestigial containers.
	cs, err := e.Client.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil {
		return errors.E("kill", e.ID, err)
	}
	for _, c := range cs {
		if len(c.Names) != 1 {
			continue
		}
		if !strings.HasPrefix(c.Names[0], "/reflow-"+e.ID) {
			continue
		}
		e.Client.ContainerKill(ctx, c.ID, "KILL")
		respc, errc := e.Client.ContainerWait(ctx, c.ID, container.WaitConditionNotRunning)
		select {
		case err := <-errc:
			if docker.IsErrNotFound(err) {
				continue
			}
		case <-respc:
		}
		e.Client.ContainerRemove(ctx, c.ID, types.ContainerRemoveOptions{Force: true})
	}
	// Finally remove collect the repository.
	// TODO: this could instead be handed off to a repository in the pool
	// which can be collected separately.
	return e.FileRepository.Collect(ctx, nil)
}

// rewriteConfig possibly rewrites the exec config cfg. In
// particular, it rewrites interns and externs (which are not
// intrinsic) to execs implementing those operations.
func (e *Executor) rewriteConfig(cfg *reflow.ExecConfig) error {
	if cfg.Type != intern && cfg.Type != extern {
		return nil
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "localfile":
		return nil
	case "s3", "s3f":
		if !e.ExternalS3 {
			return nil
		}
	default:
		return errors.E(errors.NotSupported, errors.Errorf("unsupported scheme %q", u.Scheme))
	}
	creds, err := e.AWSCreds.Get()
	if err != nil {
		return err
	}
	cfg.Image = e.AWSImage
	// This is reported to aid in the "MaxRetries" errors [1]. We introduce this
	// here as a temporary measure to improve S3 reliability until we introduce
	// intrinsic S3 support.
	//
	// [1] e.g., see https://github.com/aws/aws-cli/issues/2401
	const awsCLIFlags = `--cli-read-timeout 1200 --cli-connect-timeout 1200`
	switch cfg.Type {
	case intern:
		switch u.Scheme {
		case "s3":
			cfg.Cmd = fmt.Sprintf(`
			aws configure set default.s3.max_concurrent_requests 20
			aws configure set default.s3.max_queue_size 1000
			aws configure set default.s3.multipart_threshold 100MB
			aws configure set default.s3.multipart_chunksize 100MB
			aws configure set default.region us-west-2
			n=0
			until [ $n -ge 5 ]
			do
				env AWS_ACCESS_KEY_ID=%q AWS_SECRET_ACCESS_KEY=%q AWS_SESSION_TOKEN=%q \
					aws %s s3 sync %s $out --exclude '*.jpg' --exclude '*.jpg.zprof' && exit 0
				n=$[$n+1]
				sleep 10
			done
			exit 1`, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken, awsCLIFlags, u.String())
		case "s3f":
			uu, err := url.Parse(u.String())
			if err != nil {
				return err
			}
			uu.Scheme = "s3"
			cfg.Cmd = fmt.Sprintf(`
			export AWS_ACCESS_KEY_ID=%q
			export AWS_SECRET_ACCESS_KEY=%q
			export AWS_SESSION_TOKEN=%q 
			aws configure set default.s3.max_concurrent_requests 20
			aws configure set default.s3.max_queue_size 1000
			aws configure set default.s3.multipart_threshold 100MB
			aws configure set default.s3.multipart_chunksize 100MB
			aws configure set default.region us-west-2
			n=0
			until [ $n -ge 5 ]
			do
				aws %s s3 cp %s $out && exit 0
				n=$[$n+1]
				sleep 10
			done
			exit 1`, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken, awsCLIFlags, uu.String())
		}
	case extern:
		cfg.Cmd = fmt.Sprintf(`
			aws configure set default.region us-west-2
			export AWS_ACCESS_KEY_ID=%q
			export AWS_SECRET_ACCESS_KEY=%q
			export AWS_SESSION_TOKEN=%q
			d=%%s
			n=0
			until [ $n -ge 5 ]
			do
				if test -d $d
				then
					aws %s s3 sync $d %s && exit 0
				else
					aws %s s3 cp $d %s && exit 0
				fi
				n=$[$n+1]
				sleep 10
			done
			exit 1`, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken, awsCLIFlags, u.String(), awsCLIFlags, u.String())
	}
	cfg.Type = "exec"
	return nil
}

// install installs a directory tree into a repository and
// returns a value representing the tree. If replace is true, the
// original files are replaced with a symlink pointing to a textual
// representation of the file's digest.
func (e *Executor) install(ctx context.Context, path string, replace bool, repo *filerepo.Repository) (reflow.Fileset, error) {
	w := new(walker.Walker)
	w.Init(path)
	g, ctx := errgroup.WithContext(ctx)
	var (
		mu  sync.Mutex
		val = reflow.Fileset{Map: map[string]reflow.File{}}
	)
	for w.Scan() {
		if w.Info().IsDir() {
			continue
		}
		path, relpath, size := w.Path(), w.Relpath(), w.Info().Size()
		g.Go(func() error {
			file, err := repo.Install(path)
			if err != nil {
				return err
			}
			mu.Lock()
			val.Map[relpath] = reflow.File{ID: file.ID, Size: size}
			mu.Unlock()
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return reflow.Fileset{}, err
	}
	if err := w.Err(); err != nil {
		return reflow.Fileset{}, err
	}

	// We remove files only after digesting has completed; if the
	// directory tree has any symlinks, we may otherwise end up removing
	// a file before it is (re)digested.
	if replace {
		for k, v := range val.Map {
			objPath := filepath.Join(path, k)
			os.Remove(objPath)
			os.Symlink(v.ID.String(), objPath)
		}
	}
	return val, nil
}
