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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/file"
	"golang.org/x/sync/errgroup"
	"grail.com/lib/data"
	"grail.com/lib/digest"
	"grail.com/lib/limiter"
)

// DefaultS3Region is the region used for s3 requests if a bucket's
// region is undiscoverable (e.g., lacking permissions for the
// GetBucketLocation API call.)
//
// Amazon generally defaults to us-east-1 when regions are unspecified
// (or undiscoverable), but this can be overridden if a different default is
// desired.
var DefaultS3Region = "us-east-1"

var (
	waitingFiles     = expvar.NewInt("s3waiting")
	fetchingFiles    = expvar.NewInt("s3fetching")
	downloadingFiles = expvar.NewInt("s3downloading")
)

// s3Client defines an abstract constructor for S3 clients.
type s3Client interface {
	// New creates a new S3 client with a config.
	New(*aws.Config) s3iface.S3API
}

// configS3client defines an s3Client that constructs clients
// from a base config together with the one supplied by the
// user's call to New.
type configS3client struct {
	Config *aws.Config
}

// New constructs a new S3 client with a configuration based
// on the merged config of the one supplied by the user and
// a base config.
func (c *configS3client) New(user *aws.Config) s3iface.S3API {
	config := new(aws.Config)
	config.MergeIn(c.Config, user)
	return s3.New(session.New(config))
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

// S3Exec defines an exec that performs local (in-process) S3
// operations. At the moment, s3Exec supports only interns from S3.
// S3Execs follow the standard on-disk layout, and can thus be
// restored on crashes/restarts, and as zombie execs for post-mortem
// inspection.
//
// TODO(marius): add stall detection
type s3Exec struct {
	// S3Client is used to construct new S3 clients for S3 operations.
	S3Client s3Client
	// Root is the root directory where the exec's state is stored.
	Root string
	// Repository is the repository into which downloaded files are installed.
	Repository *file.Repository

	// ExecID is returned by ID.
	ExecID digest.Digest
	// ExecURI is returned by URI.
	ExecURI string

	// FileLimiter limits the number of files that may be concurrently
	// downloaded.
	FileLimiter *limiter.Limiter

	// downloadedSize stores the total amount of downloaded and installed
	// data.
	downloadedSize uint64

	canceler canceler

	staging file.Repository

	mu      sync.Mutex
	cond    *sync.Cond
	state   execState
	logfile *os.File
	log     *log.Logger

	// Manifest stores the serializable state of the exec.
	// This way, s3 execs can be restored from crashes or
	// restarts; they can also be restored as zombies.
	Manifest
	err error
}

// Init initializes an s3Exec from (optionally) an executor.
func (e *s3Exec) Init(x *Executor) {
	if x != nil {
		e.S3Client = x.s3client
		e.Root = x.execPath(e.ID())
		e.Repository = x.FileRepository
		e.ExecURI = x.URI() + "/" + e.ID().Hex()
		e.staging.Root = x.execPath(e.ID(), objectsDir)
		e.staging.Log = x.Log
	}
	e.Manifest.Created = time.Now()
	e.Manifest.Type = execS3
	e.cond = sync.NewCond(&e.mu)
}

func (e *s3Exec) save(state execState) error {
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
func (e *s3Exec) Go(ctx context.Context) {
	for state, err := e.getState(); err == nil && state != execComplete; e.setState(state, err) {
		switch state {
		case execUnstarted:
			state = execInit
		case execInit:
			state, err = e.init(ctx)
		case execCreated:
			state = execRunning
		case execRunning:
			err = e.do(ctx)
			if err == nil {
				state = execComplete
				break
			}
			if err == context.DeadlineExceeded || err == context.Canceled {
				state = execInit
				break
			}
			// Application error: try to interpret it.
			var kind errors.Kind
			if aerr, ok := err.(awserr.Error); ok {
				// The underyling error was an S3 error. Try to classify it.
				// Best guess based on Amazon's descriptions:
				switch aerr.Code() {
				// Code NotFound is not documented, but it's what the API actually returns.
				case "NoSuchBucket", "NoSuchKey", "NoSuchVersion", "NotFound":
					kind = errors.NotExist
				case "AccessDenied":
					kind = errors.NotAllowed
				case "InvalidRequest", "InvalidArgument", "EntityTooSmall", "EntityTooLarge", "KeyTooLong", "MethodNotAllowed":
					kind = errors.Fatal
				case "ExpiredToken", "AccountProblem", "ServiceUnavailable", "SlowDown", "TokenRefreshRequired", "OperationAborted":
					kind = errors.Unavailable
				}
			}
			state = execComplete
			e.Manifest.Result.Err = errors.Recover(errors.E("intern", fmt.Sprint(e.Config.URL), kind, err))
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
func (e *s3Exec) path(elems ...string) string {
	elems = append([]string{e.Root}, elems...)
	return filepath.Join(elems...)
}

// setState sets the current state and error. It broadcasts
// on the exec's condition variable to wake up all waiters.
func (e *s3Exec) setState(state execState, err error) {
	e.mu.Lock()
	e.state = state
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
}

// getState returns the current state of the exec.
func (e *s3Exec) getState() (execState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, e.err
}

func (e *s3Exec) init(ctx context.Context) (execState, error) {
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

func (e *s3Exec) do(ctx context.Context) error {
	const (
		// The default file concurrency is 100.
		// 10*100 = 1000 maximum TCP streams, for the pool.
		// This isn't a global limit, and isn't ideal for downloads
		// that have a large number of small files.
		//
		// TODO(marius): use the repository limiter here, with an
		// additional restriction to limit TCP streams so that we
		// don't run out of file descriptors.
		chunkConcurrency = 20

		partSize = 100 << 20
	)
	ctx, cancel := context.WithCancel(ctx)
	e.canceler.Set(cancel)
	if err := ctx.Err(); err != nil {
		return err
	}
	u, err := url.Parse(e.Config.URL)
	if err != nil {
		return errors.E("exec", e.ID(), err)
	}
	if u.Scheme != "s3" {
		return errors.E("exec", e.ID(), errors.NotSupported, errors.Errorf("unsupported scheme %v", u.Scheme))
	}
	if e.Config.Type != "intern" {
		return errors.E("exec", e.ID(), errors.NotSupported, errors.Errorf("unsupported exec type %v", e.Config.Type))
	}
	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")
	config := &aws.Config{
		MaxRetries: aws.Int(10),
		Region:     aws.String("us-west-2"),
	}
	client := e.S3Client.New(config)

	// First discover the bucket's region so we can construct an appropriate
	// AWS session for it.
	req, rep := client.GetBucketLocationRequest(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
	if err := req.Send(); err == nil {
		region := aws.StringValue(rep.LocationConstraint)
		if region == "" {
			// This is a bit of an AWS wart: if the region is empty,
			// it means us-east-1; however, the API does not accept
			// an empty region.
			region = "us-east-1"
		}
		e.log.Printf("discovered region %s for %s", region, bucket)
		config.Region = aws.String(region)
	} else {
		e.log.Errorf("could not discover region for bucket %s: %v", bucket, err)
		config.Region = aws.String(DefaultS3Region)
	}

	// Define the error group under which we will perform all of our fetches.
	// We thread the common context through an http round tripper that will
	// terminate all pending requests when that context is cancelled.
	g, ctx := errgroup.WithContext(ctx)
	config.HTTPClient = &http.Client{Transport: contextTransport{ctx}}
	client = e.S3Client.New(config)
	dl := s3manager.NewDownloaderWithClient(client, func(d *s3manager.Downloader) {
		d.Concurrency = chunkConcurrency
		d.PartSize = partSize
	})

	e.mu.Lock()
	e.Manifest.Result.Fileset.Map = map[string]reflow.File{}
	e.mu.Unlock()
	nprefix := len(prefix)

	if !strings.HasSuffix(prefix, "/") {
		// Handle single-file downloads specially.
		req, resp := client.HeadObjectRequest(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(prefix),
		})
		req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
		if err := req.Send(); err != nil {
			return err
		}
		size := aws.Int64Value(resp.ContentLength)
		if err := e.FileLimiter.Acquire(ctx, 1); err != nil {
			return err
		}
		file, err := e.download(ctx, dl, bucket, prefix, size)
		e.FileLimiter.Release(1)
		if err != nil {
			return err
		}
		e.mu.Lock()
		e.Manifest.Result.Fileset.Map["."] = file
		e.mu.Unlock()
		return nil
	}

	w := &s3Walker{S3: client, Bucket: bucket, Prefix: prefix}
	for w.Scan(ctx) {
		key, size := aws.StringValue(w.Object().Key), aws.Int64Value(w.Object().Size)
		if len(key) < nprefix {
			e.log.Errorf("invalid key %q; skipping", key)
			continue
		}
		// Skip "directories".
		if strings.HasSuffix(key, "/") {
			continue
		}
		waitingFiles.Add(1)
		if err := e.FileLimiter.Acquire(ctx, 1); err != nil {
			return err
		}
		waitingFiles.Add(-1)
		g.Go(func() error {
			file, err := e.download(ctx, dl, bucket, key, size)
			e.FileLimiter.Release(1)
			if err != nil {
				return err
			}
			e.mu.Lock()
			e.Manifest.Result.Fileset.Map[key[nprefix:]] = file
			e.mu.Unlock()
			return nil
		})

	}
	// Always wait for work to complete regardless of error.
	// If there is an error, the context will be cancelled and
	// waiting will be quick.
	if err := g.Wait(); err != nil {
		return err
	}
	if err := w.Err(); err != nil {
		return err
	}
	return nil
}

func (e *s3Exec) download(ctx context.Context, dl *s3manager.Downloader, bucket, key string, size int64) (reflow.File, error) {
	fetchingFiles.Add(1)
	defer fetchingFiles.Add(-1)
	f, err := ioutil.TempFile(e.path("download"), "")
	if err != nil {
		return reflow.File{}, err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			e.log.Errorf("failed to remove file %q: %v", f.Name(), err)
		}
		f.Close()
	}()
	var w bytewatch
	w.Reset()
	e.log.Printf("download s3://%s/%s (%s) to %s", bucket, key, data.Size(size), f.Name())
	downloadingFiles.Add(1)
	dwa := reflow.Digester.NewWriterAt(f)
	_, err = dl.Download(dwa, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	downloadingFiles.Add(-1)
	if err != nil {
		e.log.Printf("download s3://%s/%s: %v", bucket, key, err)
		return reflow.File{}, err
	}
	dur, bps := w.Lap(size)
	e.log.Printf("done s3://%s/%s in %s (%s/s)", bucket, key, dur, data.Size(bps))
	file := reflow.File{
		ID:   dwa.Digest(),
		Size: size,
	}
	err = e.staging.InstallDigest(file.ID, f.Name())
	atomic.AddUint64(&e.downloadedSize, uint64(size))
	if err != nil {
		e.log.Errorf("install s3://%s/%s: %v", bucket, key, err)
	} else {
		e.log.Printf("installed s3://%s/%s to %v", bucket, key, file)
	}
	return file, err
}

func (e *s3Exec) Kill(ctx context.Context) error {
	e.canceler.Cancel()
	return e.Wait(ctx)
}

func (e *s3Exec) WaitUntil(min execState) error {
	e.mu.Lock()
	for e.state < min && e.err == nil {
		e.cond.Wait()
	}
	e.mu.Unlock()
	return e.err
}

func (e *s3Exec) ID() digest.Digest {
	return e.ExecID
}

// URI returns a URI For this exec based on its executor's URI.
func (e *s3Exec) URI() string { return e.ExecURI }

// Value returns the interned value when the exec is complete.
func (e *s3Exec) Result(ctx context.Context) (reflow.Result, error) {
	state, err := e.getState()
	if err != nil {
		return reflow.Result{}, err
	}
	if state != execComplete {
		return reflow.Result{}, errors.Errorf("result %v: exec not complete", e.ExecID)
	}
	return e.Manifest.Result, nil
}

func (e *s3Exec) Promote(ctx context.Context) error {
	return e.Repository.Vacuum(ctx, &e.staging)
}

// Inspect returns exec metadata.
func (e *s3Exec) Inspect(ctx context.Context) (reflow.ExecInspect, error) {
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
		inspect.Status = "download has not yet started"
	case execRunning:
		inspect.Gauges = make(reflow.Gauges)
		// These gauges values are racy: we can observe an outdated disk size
		// with respect to tmp.
		inspect.Gauges["disk"] = float64(atomic.LoadUint64(&e.downloadedSize))
		path := e.path("download")
		n, err := du(path)
		if err != nil {
			e.log.Errorf("du %s: %v", path, err)
		} else {
			inspect.Gauges["tmp"] = float64(n)
		}
		inspect.State = "running"
		inspect.Status = "downloading from s3"
	case execComplete:
		inspect.State = "complete"
		inspect.Status = "download complete"
	}
	return inspect, nil
}

// Wait returns when the exec is complete.
func (e *s3Exec) Wait(ctx context.Context) error {
	return e.WaitUntil(execComplete)
}

// Logs returns logs for this exec. Only stderr logs are emitted by s3 execs.
func (e *s3Exec) Logs(ctx context.Context, stdout bool, stderr bool) (io.ReadCloser, error) {
	if stderr {
		return os.Open(e.path("stderr"))
	}
	return ioutil.NopCloser(bytes.NewReader(nil)), nil
}

// contextTransport is an http.RoundTripper that injects a context
// into every request. It is used to inject contexts into HTTP
// requests made by the S3 downloader.
type contextTransport struct{ context.Context }

func (c contextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.WithContext(c.Context)
	return http.DefaultTransport.RoundTrip(req)
}
