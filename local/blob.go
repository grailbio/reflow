// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/filerepo"
	"golang.org/x/sync/errgroup"
)

var (
	fetchingFiles    = expvar.NewInt("blobfetching")
	downloadingFiles = expvar.NewInt("blobdownloading")
	digestingFiles   = expvar.NewInt("blobdigesting")
	uploadingFiles   = expvar.NewInt("blobuploading")
	internRate       = expvar.NewInt("blobinternrate")
	externRate       = expvar.NewInt("blobexternrate")
)

// rateExporter measures progress in bytes and updates exported var.
// It does so by keeping track of total bytes
type rateExporter struct {
	begin time.Time
	exp   *expvar.Int

	mu       sync.Mutex
	bytes    int64
	prevRate int64
	done     bool
}

// newRateExporter returns a new rateExporter which updates the given exporter var.
func newRateExporter(expvar *expvar.Int) *rateExporter {
	return &rateExporter{begin: time.Now(), exp: expvar}
}

// rate returns the current rate defined as the ratio of
// total bytes by the time since the beginning.
func (b *rateExporter) rate() int64 {
	dur := time.Since(b.begin)
	dur -= dur % time.Second
	if dur < time.Second {
		dur = time.Second
	}
	return b.bytes / int64(dur.Seconds())
}

// Add adds the given bytes to the rateExporter and causes a recomputation of the rate.
// Then the difference between the current rate and the previous rate is added to the exported var.
func (b *rateExporter) Add(bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return
	}
	b.bytes += bytes
	rate := b.rate()
	b.exp.Add(rate - b.prevRate)
	b.prevRate = rate
}

// Done signals that this rateExporter is done and will no longer update the exporter var.
func (b *rateExporter) Done() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.done = true
	b.exp.Add(-b.prevRate)
}

// a canceler rendezvous a cancellation function with a
// cancellation request.
type canceler struct {
	mu       sync.Mutex
	cancel   func()
	canceled bool
}

// Set sets the canceler's cancel func. If the canceler has already
// been canceled, the cancel func is invoked immediately.
func (c *canceler) Set(cancel func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancel = cancel
	if c.canceled {
		cancel()
	}
}

// Cancel cancels this canceler. If there is a registered cancel
// func, it is invoked; if not, it's invoked upon registration.
func (c *canceler) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.canceled = true
	if c.cancel != nil {
		c.cancel()
	}
}

// bytewatch is a stopwatch for measuring progress in time and bytes.
type bytewatch struct {
	begin time.Time
}

// Reset starts the bytewatch anew.
func (b *bytewatch) Reset() {
	b.begin = time.Now()
}

// Lap measures progress, returning the elapsed duration
// and the current (average) BPS.
func (b *bytewatch) Lap(bytes int64) (dur time.Duration, bps int64) {
	dur = time.Since(b.begin)
	dur -= dur % time.Second
	if dur < time.Second {
		dur = time.Second
	}
	return dur, bytes / int64(dur.Seconds())
}

// BlobExec defines an exec that performs local (in-process) blob
// operations. BlobExec follow the standard on-disk layout, and can
// thus be restored on crashes/restarts, and as zombie execs for
// post-mortem inspection.
//
// TODO(marius): add stall detection
type blobExec struct {
	Blob blob.Mux

	// Root is the root directory where the exec's state is stored.
	Root string
	// Repository is the repository into which downloaded files are installed.
	Repository *filerepo.Repository

	// ExecID is returned by ID.
	ExecID digest.Digest
	// ExecURI is returned by URI.
	ExecURI string

	// transferType stores the type of transfer (ie "intern" or "extern")
	transferType string
	// transferredSize stores the total amount of data either downloaded and installed or uploaded.
	transferredSize uint64

	canceler canceler

	staging filerepo.Repository

	mu      sync.Mutex
	cond    *sync.Cond
	logfile *os.File
	log     *log.Logger

	// Manifest stores the serializable state of the exec.
	// This way, blob execs can be restored from crashes or
	// restarts; they can also be restored as zombies.
	Manifest
	err error

	x           *Executor
	promoteOnce once.Task
}

const (
	intern = "intern"
	extern = "extern"
)

// transferTypeStr returns a human-readable string for the transfer type.
func (e *blobExec) transferTypeStr() string {
	switch e.transferType {
	case intern:
		return "download"
	case extern:
		return "upload"
	}
	return "unknown"
}

// Init initializes an blobExec from (optionally) an executor.
func (e *blobExec) Init(x *Executor) {
	if x != nil {
		e.Blob = x.Blob
		e.Root = x.execPath(e.ID())
		e.Repository = x.FileRepository
		e.ExecURI = x.URI() + "/" + e.ID().Hex()
		if e.transferType == intern {
			e.staging.Root = x.execPath(e.ID(), objectsDir)
			e.staging.Log = x.Log
		}
	}
	e.Manifest.Created = time.Now()
	e.Manifest.Type = execBlob
	e.cond = sync.NewCond(&e.mu)
}

func (e *blobExec) save(state execState) error {
	if err := os.MkdirAll(e.path(), 0777); err != nil {
		return err
	}
	path := e.path(manifestPath)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	manifest := e.Manifest
	manifest.State = state
	if err := json.NewEncoder(f).Encode(manifest); err != nil {
		os.Remove(path)
		f.Close()
		return err
	}
	f.Close()
	return nil
}

// Go starts the exec state machine.
func (e *blobExec) Go(ctx context.Context) {
	for state, err := e.getState(); err == nil && state != execComplete; e.setState(state, err) {
		switch state {
		case execUnstarted:
			state = execInit
		case execInit:
			state, err = e.init(ctx)
		case execCreated:
			state = execRunning
		case execRunning:
			if e.transferType == intern {
				err = e.doIntern(ctx)
			} else {
				err = e.doExtern(ctx)
			}
			if err == nil {
				state = execComplete
				break
			}
			if err == context.DeadlineExceeded || err == context.Canceled {
				state = execInit
				break
			}
			state = execComplete
			e.Manifest.Result.Err = errors.Recover(errors.E(e.transferType, fmt.Sprint(e.Config.URL), err))
			err = nil
		default:
			panic("bug")
		}
		if err == nil {
			err = e.save(state)
		}
	}
	e.log = nil
	if e.logfile != nil {
		e.logfile.Close()
	}
}

// path constructs a path in the exec's directory.
func (e *blobExec) path(elems ...string) string {
	elems = append([]string{e.Root}, elems...)
	return filepath.Join(elems...)
}

// setState sets the current state and error. It broadcasts
// on the exec's condition variable to wake up all waiters.
func (e *blobExec) setState(state execState, err error) {
	e.mu.Lock()
	e.State = state
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
}

// getState returns the current state of the exec.
func (e *blobExec) getState() (execState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.State, e.err
}

func (e *blobExec) init(ctx context.Context) (execState, error) {
	if err := os.MkdirAll(e.path("download"), 0777); err != nil {
		return execInit, err
	}
	var err error
	e.logfile, err = os.OpenFile(e.path("stderr"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		return execInit, err
	}
	e.log = e.log.Tee(golog.New(e.logfile, "", golog.LstdFlags), "")

	return execCreated, nil
}

func (e *blobExec) doIntern(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.canceler.Set(cancel)
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.Config.Type != intern {
		return errors.E("exec", e.ID(), errors.NotSupported, errors.Errorf("unsupported exec type %v", e.Config.Type))
	}
	bucket, prefix, err := e.Blob.Bucket(ctx, e.Config.URL)
	if err != nil {
		return err
	}

	// Define the error group under which we will perform all of our fetches.
	g, ctx := errgroup.WithContext(ctx)

	e.mu.Lock()
	e.Manifest.Result.Fileset.Map = map[string]reflow.File{}
	e.mu.Unlock()
	nprefix := len(prefix)

	if !strings.HasSuffix(prefix, "/") {
		file, err := bucket.File(ctx, prefix)
		if found, err := fileFromRepo(ctx, e.Repository, file); err == nil {
			file = found
		} else {
			dl := download{
				Bucket: bucket,
				Key:    prefix,
				File:   file,
				Log:    e.log,
			}
			file, err = dl.Do(ctx, &e.staging)
		}
		if err != nil {
			return err
		}
		atomic.AddUint64(&e.transferredSize, uint64(file.Size))
		e.mu.Lock()
		e.Manifest.Result.Fileset.Map["."] = file
		e.mu.Unlock()
		return nil
	}

	rw := newRateExporter(internRate)
	defer rw.Done()
	scan := bucket.Scan(prefix)
	for scan.Scan(ctx) {
		key, file := scan.Key(), scan.File()
		if len(key) < nprefix {
			e.log.Errorf("invalid key %q; skipping", key)
			continue
		}
		// Skip "directories".
		if strings.HasSuffix(key, "/") {
			continue
		}
		g.Go(func() error {
			if found, err := fileFromRepo(ctx, e.Repository, file); err == nil {
				file = found
			} else {
				dl := download{
					Bucket: bucket,
					Key:    key,
					File:   file,
					Log:    e.log,
				}
				file, err = dl.Do(ctx, &e.staging)
			}
			if err != nil {
				return err
			}
			atomic.AddUint64(&e.transferredSize, uint64(file.Size))
			e.mu.Lock()
			e.Manifest.Result.Fileset.Map[key[nprefix:]] = file
			e.mu.Unlock()
			rw.Add(file.Size)
			return nil
		})
	}
	// Always wait for work to complete regardless of error.
	// If there is an error, the context will be cancelled and
	// waiting will be quick.
	if err := g.Wait(); err != nil {
		return err
	}
	return scan.Err()
}

func (e *blobExec) doExtern(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.canceler.Set(cancel)
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.Config.Type != extern {
		return errors.E("exec", e.ID(), errors.NotSupported, errors.Errorf("unsupported exec type %v", e.Config.Type))
	}
	bucket, prefix, err := e.Blob.Bucket(ctx, e.Config.URL)
	if err != nil {
		return err
	}

	if len(e.Config.Args) != 1 {
		return errors.E(errors.Precondition,
			errors.Errorf("unexpected args (must be 1, but was %d): %v", len(e.Config.Args), e.Config.Args))
	}
	fileset := e.Config.Args[0].Fileset.Pullup()

	// Define the error group under which we will perform all of our fetches.
	g, ctx := errgroup.WithContext(ctx)

	e.mu.Lock()
	e.Manifest.Result.Fileset.Map = map[string]reflow.File{}
	e.mu.Unlock()

	rw := newRateExporter(externRate)
	defer rw.Done()
	for k, v := range fileset.Map {
		fn, f := k, v
		g.Go(func() error {
			key := path.Join(prefix, fn)
			ul := upload{
				Repository: e.Repository,
				Bucket:     bucket,
				Key:        key,
				ID:         f.ID,
				Size:       f.Size,
				Log:        e.log,
			}
			err = ul.Do(ctx)
			if err != nil {
				return err
			}
			atomic.AddUint64(&e.transferredSize, uint64(f.Size))
			e.mu.Lock()
			e.Manifest.Result.Fileset.Map[fn] = f
			e.mu.Unlock()
			rw.Add(f.Size)
			return nil
		})
	}
	// Always wait for work to complete regardless of error.
	// If there is an error, the context will be cancelled and
	// waiting will be quick.
	return g.Wait()
}

func (e *blobExec) Kill(ctx context.Context) error {
	e.canceler.Cancel()
	return e.Wait(ctx)
}

func (e *blobExec) WaitUntil(min execState) error {
	e.mu.Lock()
	for e.State < min && e.err == nil {
		e.cond.Wait()
	}
	e.mu.Unlock()
	return e.err
}

func (e *blobExec) ID() digest.Digest {
	return e.ExecID
}

// URI returns a URI For this exec based on its executor's URI.
func (e *blobExec) URI() string { return e.ExecURI }

// Value returns the interned value when the exec is complete.
func (e *blobExec) Result(ctx context.Context) (reflow.Result, error) {
	state, err := e.getState()
	if err != nil {
		return reflow.Result{}, err
	}
	if state != execComplete {
		return reflow.Result{}, errors.Errorf("result %v: exec not complete", e.ExecID)
	}
	return e.Manifest.Result, nil
}

// Promote promotes the blob exec data to the executor repository.
func (e *blobExec) Promote(ctx context.Context) error {
	// Promotion moves the objects in the staging repository to the executor's repository.
	// The first call to Promote moves these objects and ref counts them. Later calls are
	// a no-op.
	// TODO(pgopal): Move promote to the exec state machine.
	return e.promoteOnce.Do(func() error {
		if e.transferType == intern {
			res, err := e.Result(ctx)
			if err != nil {
				return err
			}
			return e.x.promote(ctx, res.Fileset, &e.staging)
		}
		return nil
	})
}

// Inspect returns exec metadata.
func (e *blobExec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
	inspect := reflow.ExecInspect{
		Config:  e.Config,
		Created: e.Manifest.Created,
	}
	state, err := e.getState()
	if err != nil {
		inspect.Error = errors.Recover(err)
	}
	switch state {
	case execUnstarted, execInit, execCreated:
		inspect.State = "initializing"
		inspect.Status = fmt.Sprintf("%s has not yet started", e.transferTypeStr())
	case execRunning:
		if e.transferType == intern {
			inspect.Gauges = make(reflow.Gauges)
			// These gauges values are racy: we can observe an outdated disk size
			// with respect to tmp.
			inspect.Gauges["disk"] = float64(atomic.LoadUint64(&e.transferredSize))
			path := e.path("download")
			n, err := du(path)
			if err != nil {
				e.log.Errorf("du %s: %v", path, err)
			} else {
				inspect.Gauges["tmp"] = float64(n)
			}
		}
		inspect.State = "running"
		inspect.Status = fmt.Sprintf("%sing from/to bucket", e.transferTypeStr())
	case execComplete:
		inspect.State = "complete"
		inspect.Status = fmt.Sprintf("%s complete", e.transferTypeStr())
	}
	return inspect, nil
}

// Wait returns when the exec is complete.
func (e *blobExec) Wait(ctx context.Context) error {
	return e.WaitUntil(execComplete)
}

// Logs returns logs for this exec. Only stderr logs are emitted by blob execs.
func (e *blobExec) Logs(ctx context.Context, stdout bool, stderr bool, follow bool) (io.ReadCloser, error) {
	if stderr {
		return os.Open(e.path("stderr"))
	}
	return ioutil.NopCloser(bytes.NewReader(nil)), nil
}

func (e *blobExec) Shell(ctx context.Context) (io.ReadWriteCloser, error) {
	return nil, errors.New("cannot shell into a file intern/extern")
}

// fileFromRepo gets the given file from the given repo.
// Returns an error if the file does not contain ContentHash or it isn't found in repo.
func fileFromRepo(ctx context.Context, repo *filerepo.Repository, f reflow.File) (file reflow.File, err error) {
	if !f.ContentHash.IsZero() {
		if file, err = repo.Stat(ctx, f.ContentHash); err == nil {
			file.Source, file.ETag, file.LastModified = f.Source, f.ETag, f.LastModified
			file.Assertions = blob.Assertions(file)
		}
	} else {
		file, err = reflow.File{}, errors.New("No ContentHash")
	}
	return
}

type download struct {
	Bucket blob.Bucket
	Key    string
	File   reflow.File
	Log    *log.Logger
}

func (d *download) Do(ctx context.Context, repo *filerepo.Repository) (reflow.File, error) {
	fetchingFiles.Add(1)
	defer fetchingFiles.Add(-1)
	filename, err := d.download(ctx, repo)
	if err != nil {
		return reflow.File{}, err
	}
	defer func() {
		if filename != "" {
			if err := os.Remove(filename); err != nil {
				d.Log.Errorf("remove %s: %v", filename, err)
			}
		}
	}()
	var w bytewatch
	w.Reset()
	digestingFiles.Add(1)
	file, err := repo.Install(filename)
	digestingFiles.Add(-1)
	if err == nil && file.Size != d.File.Size {
		err = errors.E(errors.Integrity,
			errors.Errorf("expected size %d does not match actual size %d", d.File.Size, file.Size))
	}
	if err != nil {
		d.Log.Errorf("install %s%s: %v", d.Bucket.Location(), d.Key, err)
	} else {
		file.Source, file.ETag, file.LastModified = d.File.Source, d.File.ETag, d.File.LastModified
		file.Assertions = blob.Assertions(file)
		dur, bps := w.Lap(d.File.Size)
		d.Log.Printf("installed %s%s to %v in %s (%s/s)", d.Bucket.Location(), d.Key, filename, dur, data.Size(bps))
	}
	return file, err
}

func (d *download) download(ctx context.Context, repo *filerepo.Repository) (string, error) {
	f, err := repo.TempFile("download")
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			d.Log.Errorf("close %s: %v", f.Name(), err)
		}
	}()
	var w bytewatch
	w.Reset()
	d.Log.Printf("download %s%s (%s) to %s", d.Bucket.Location(), d.Key, data.Size(d.File.Size), f.Name())
	downloadingFiles.Add(1)
	_, err = d.Bucket.Download(ctx, d.Key, d.File.ETag, d.File.Size, f)
	downloadingFiles.Add(-1)
	if err != nil {
		d.Log.Printf("download %s%s: %v", d.Bucket.Location(), d.Key, err)
		return "", err
	}
	dur, bps := w.Lap(d.File.Size)
	d.Log.Printf("done %s%s in %s (%s/s)", d.Bucket.Location(), d.Key, dur, data.Size(bps))
	return f.Name(), err
}

type upload struct {
	Repository *filerepo.Repository
	Bucket     blob.Bucket
	Key        string
	ID         digest.Digest
	Size       int64
	Log        *log.Logger
}

func (u *upload) Do(ctx context.Context) error {
	f, err := u.Repository.Get(ctx, u.ID)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			_, path := u.Repository.Path(u.ID)
			u.Log.Errorf("close %s: %v", path, err)
		}
	}()
	var w bytewatch
	w.Reset()
	u.Log.Printf("upload %s (%s) to %s%s", u.Key, data.Size(u.Size), u.Bucket.Location(), u.Key)
	uploadingFiles.Add(1)
	err = u.Bucket.Put(ctx, u.Key, u.Size, f, u.ID.Hex())
	uploadingFiles.Add(-1)
	if err != nil {
		u.Log.Printf("upload %s/%s: %v", u.Bucket.Location(), u.Key, err)
		return err
	}
	dur, bps := w.Lap(u.Size)
	u.Log.Printf("done %s/%s in %s (%s/s)", u.Bucket.Location(), u.Key, dur, data.Size(bps))
	return nil
}
