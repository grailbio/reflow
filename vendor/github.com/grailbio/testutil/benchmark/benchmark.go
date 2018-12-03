// Package benchmark provides functions for benchmarking applications.
//
// It is designed for long-running applications. For microbenchmarks, use the
// standard testing.B instead. Most people will want to use
// testutil/cmd/grail-benchmark instead of this package directly.
package benchmark

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/stat"
)

const maxParallelism = 100

// For rate-limiting parallel S3 downloads.
type rateLimiter struct {
	cond   sync.Cond
	active map[string]bool
}

func newRateLimiter() *rateLimiter {
	mu := &sync.Mutex{}
	return &rateLimiter{cond: sync.Cond{L: mu}, active: map[string]bool{}}
}

// To be called before downloading a given file.
func (r *rateLimiter) enter(path string) {
	r.cond.L.Lock()
	for len(r.active) > maxParallelism || r.active[path] {
		r.cond.Wait()
	}
	r.active[path] = true
	r.cond.L.Unlock()
}

// To be called after handling the given file. It must be called exactly once
// even on error.
func (r *rateLimiter) exit(path string) {
	r.cond.L.Lock()
	delete(r.active, path)
	r.cond.Signal()
	r.cond.L.Unlock()
}

var limiter = newRateLimiter()

func cacheFile(ctx context.Context, srcPath string, dstPath string) (err error) {
	limiter.enter(srcPath)
	defer limiter.exit(srcPath)
	in, err := file.Open(ctx, srcPath)
	if err != nil {
		return err
	}
	defer file.CloseAndReport(ctx, in, &err)
	srcInfo, err := in.Stat(ctx)
	if err != nil {
		return err
	}
	dstInfo, err := file.Stat(ctx, dstPath)
	if err == nil {
		diff := dstInfo.ModTime().Sub(srcInfo.ModTime())
		if (diff >= -5*time.Second) && (diff <= 5*time.Second) {
			return nil
		}
	}

	out, err := file.Create(ctx, dstPath)
	if err != nil {
		return err
	}
	defer file.CloseAndReport(ctx, out, &err)
	if _, err := io.Copy(out.Writer(ctx), in.Reader(ctx)); err != nil {
		return err
	}
	return os.Chtimes(dstPath, srcInfo.ModTime(), srcInfo.ModTime())
}

// CacheDir recursively copies all files under srcDir into dstDir/hash, where
// "hash" is a determinsitically random string generated from the source
// file. If files have been already cached, this function becomes a noop.
func CacheDir(ctx context.Context, srcDir string, dstDir string) (string, error) {
	dstDir = fmt.Sprintf("%s/%x%s", dstDir, sha256.Sum256([]byte(srcDir)), filepath.Ext(srcDir))

	eg := errgroup.Group{}
	lister := file.List(ctx, srcDir, true /*recursive*/)
	for lister.Scan() {
		path := lister.Path()
		eg.Go(func() error {
			return cacheFile(ctx, path, dstDir+path[len(srcDir):])
		})
	}
	err := eg.Wait()
	if e := lister.Err(); e != nil && err == nil {
		err = e
	}
	return dstDir, err
}

// CacheFile copies srcPath to dstPath/hash.ext, where "hash" is a
// determinsitically random string generated from the source file, and ext is
// the extension of srcPath. If the file has been already cached, this function
// becomes a noop.
func CacheFile(ctx context.Context, srcPath string, dstDir string) (string, error) {
	dstPath := fmt.Sprintf("%s/%x%s", dstDir, sha256.Sum256([]byte(srcPath)), filepath.Ext(srcPath))
	return dstPath, cacheFile(ctx, srcPath, dstPath)
}

const (
	// DefaultMinIteration is the default value for Opts.MinIteration.
	DefaultMinIteration = 5
	// DefaultMaxIteration is the default value for Opts.MaxIteration.
	DefaultMaxIteration = 100
	// DefaultMinDuration is the default value for Opts.MinDuration.
	DefaultMinDuration = 10 * time.Second
	// DefaultMaxDuration is the default value for Opts.MaxDuration.
	DefaultMaxDuration = 10 * time.Minute
)

// Opts defines how long and how many times the benchmark apps are run.
//
// - When Run() is passed multiple apps, they are always run the same number of
// times.
//
// - When each app is run >= MaxIteration times, or MaxDuration elapses since
// the start of Run(), the Run() finishes even if their performance has not
// stabilized.
//
// - When each app is run >= MinIteration, or >= MinDuration elapses since the
//  start of Run(), and the performance of all the app performance has
//  stabilized, Run finishes.
type Opts struct {
	// MinIteration is the minimum number of times to run one application.
	MinIteration int
	// MaxIteration is the max number of times to run one application.
	MaxIteration int
	// MinDuration is the minimum duration of benchmark runs, across all the applications.
	MinDuration time.Duration
	// MaxDuration is the maximum duration of benchmark runs, across all the
	// applications.
	MaxDuration time.Duration
}

// Target defines one target application to run.
type Target struct {
	// Name is the name of the application. It will be copied to Result.Name
	Name string
	// Callback is the function that runs the application.
	Callback func() error
}

// RunResult is the result of one run of an application.
type RunResult struct {
	Start    time.Time
	Duration time.Duration
	Err      error
}

// Result is the result of runs of an applications.
type Result struct {
	// Name is copied from Target.Name
	Name string
	// Runs is the list of all runs.
	Runs []RunResult
}

func (r Result) isDone() bool {
	// Sort the runs in best-to-worst order. We assume failed runs to have taken
	// ∞.  Then remove the worst 1/3 of the results.
	temp := make([]RunResult, len(r.Runs))
	copy(temp, r.Runs)
	sort.SliceStable(temp, func(i, j int) bool {
		sortKey := func(r RunResult) time.Duration {
			if r.Err != nil {
				return time.Duration(math.MaxInt64)
			}
			return r.Duration
		}
		return sortKey(temp[i]) < sortKey(temp[j])
	})
	temp = temp[:len(temp)*2/3]

	// We are done if the sample stderr < 5% of the mean.  The 5% threshold is
	// picked arbitrarily.
	var durations []float64
	for _, r := range temp {
		if r.Err != nil {
			return false
		}
		durations = append(durations, float64(r.Duration))
	}
	if len(durations) <= 2 {
		// >= 2 samples are needed to compute stder.
		return false
	}
	mean := stat.Mean(durations, nil)
	if mean <= 0 {
		return false
	}
	stdev := stat.StdDev(durations, nil)
	stder := stat.StdErr(stdev, float64(len(durations)))
	relStderr := stder / mean
	log.Printf("%s: iter %d: stderr %f(s), mean %f(s) (%.2f%%)",
		r.Name, len(r.Runs), stder/1e9, mean/1e9, relStderr*100)
	return relStderr < 0.05
}

func validateOpts(opts *Opts) {
	if opts.MinIteration <= 0 {
		opts.MinIteration = DefaultMinIteration
	}
	if opts.MaxIteration <= 0 {
		opts.MaxIteration = DefaultMaxIteration
	}
	if opts.MinDuration <= 0 {
		opts.MinDuration = DefaultMinDuration
	}
	if opts.MaxDuration <= 0 {
		opts.MaxDuration = DefaultMaxDuration
	}
}

// Run runs the list of target applications repeatedly until their performance
// stabilizes.  The targets are run alternatingly to cancel out effects of
// low-frequency noises such as background cron jobs.
func Run(targets []Target, opts Opts) []Result {
	results := make([]Result, len(targets))
	startTime := time.Now()
	for i := range targets {
		results[i].Name = targets[i].Name
	}
	validateOpts(&opts)
	iter := 0
	for {
		ok := true
		for i := range targets {
			r := &results[i]
			runStart := time.Now()
			log.Debug.Printf("Start %s (%d)", targets[i].Name, iter)
			err := targets[i].Callback()
			runEnd := time.Now()
			r.Runs = append(r.Runs, RunResult{
				Start:    runStart,
				Duration: runEnd.Sub(runStart),
				Err:      err,
			})
			if !r.isDone() {
				ok = false
			}
		}
		elapsed := time.Since(startTime)
		iter++
		if ok && iter >= opts.MinIteration && elapsed >= opts.MinDuration {
			break
		}
		if iter >= opts.MaxIteration || elapsed >= opts.MaxDuration {
			break
		}
	}
	return results
}
