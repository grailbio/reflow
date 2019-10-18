// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package local

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/testblob"
	"github.com/grailbio/reflow/internal/walker"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/filerepo"
	"github.com/grailbio/testutil"
)

const (
	dockerSocket = "/var/run/docker.sock"
	bashImage    = "yikaus/alpine-bash" // the default alpine image doesn't have Bash.
	// We put this in /tmp because it's one of the default locations
	// that are bindable from Docker for Mac.
	tmpDir = "/tmp"
)

func newTestExecutorOrSkip(t *testing.T, creds *credentials.Credentials) (*Executor, func()) {
	dir, cleanup := testutil.TempDir(t, tmpDir, "reflowtest")
	x := &Executor{
		Client:   newDockerClientOrSkip(t),
		Dir:      dir,
		AWSImage: "619867110810.dkr.ecr.us-west-2.amazonaws.com/awstool",
		AWSCreds: creds,
	}
	x.SetResources(reflow.Resources{
		"mem":  1 << 30,
		"cpu":  2,
		"disk": 1e10,
	})
	if err := x.Start(); err != nil {
		cleanup()
		t.Fatal(err)
	}
	cleanup = func() {}
	return x, cleanup
}

func TestExec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	ctx := context.Background()
	id := reflow.Digester.FromString("hello world!")
	exec, err := x.Put(ctx, id, reflow.ExecConfig{
		Type:  "exec",
		Image: bashImage,
		Cmd:   "echo foobar > $tmp/x; cat $tmp/x > $out",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Give it some time to fetch the image, etc.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = exec.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res2 := reflow.Result{Fileset: reflow.Fileset{
		Map: map[string]reflow.File{".": {ID: reflow.Digester.FromString("foobar\n"), Size: 7}},
	}}
	if got, want := res, res2; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Get gauges and profile
	i, err := exec.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gauges := i.Gauges
	profile := i.Profile

	// Disk and tmp must be nonzero because they are always profiled at least once
	if got, zero := gauges["disk"], 0.0; got <= zero {
		t.Fatalf("disk gauge: %v !> %v", got, zero)
	}
	if got, zero := gauges["tmp"], 0.0; got <= zero {
		t.Fatalf("tmp gauge: %v !> %v", got, zero)
	}
	if got, zero := profile["disk"].Mean, 0.0; got <= zero {
		t.Fatalf("disk mean: %v !> %v", got, zero)
	}
	if got, zero := profile["tmp"].Mean, 0.0; got <= zero {
		t.Fatalf("tmp mean: %v !> %v", got, zero)
	}
	if got, zero := profile["disk"].Max, 0.0; got <= zero {
		t.Fatalf("disk max: %v !> %v", got, zero)
	}
	if got, zero := profile["tmp"].Max, 0.0; got <= zero {
		t.Fatalf("tmp max: %v !> %v", got, zero)
	}
	if got, zero := profile["disk"].N, int64(0); got <= zero {
		t.Fatalf("disk N: %v !> %v", got, zero)
	}
	if got, zero := profile["tmp"].N, int64(0); got <= zero {
		t.Fatalf("tmp N: %v !> %v", got, zero)
	}

	// Disk and tmp variance must be 0 because disk and tmp can only be profiled once in 45 seconds
	if got, want := profile["disk"].Var, 0.0; got != want {
		t.Fatalf("disk variance: %v != %v", got, want)
	}
	if got, want := profile["tmp"].Var, 0.0; got != want {
		t.Fatalf("tmp variance: %v != %v", got, want)
	}

	// Disk and tmp profiles must have nonzero First and Last times
	if profile["disk"].First.IsZero() || profile["disk"].Last.IsZero() {
		t.Fatalf("disk First and Last times must not be zero if profiling has occurred.")
	}
	if profile["tmp"].First.IsZero() || profile["tmp"].Last.IsZero() {
		t.Fatalf("tmp First and Last times must not be zero if profiling has occurred.")
	}
}

func TestProfileContextTimeOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	ctx := context.Background()
	id := reflow.Digester.FromString("hello world!")

	// execslow sleeps for 45 seconds, which, so ctx (with a 30-second timeout) will time out before
	// execslow finishes.
	execslow, err := x.Put(ctx, id, reflow.ExecConfig{
		Type:  "exec",
		Image: bashImage,
		Cmd:   "sleep 45; echo foobar > $tmp/x; cat $tmp/x > $out",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Give it some time to fetch the image, etc.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = execslow.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	i, err := execslow.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gauges := i.Gauges
	profile := i.Profile

	// Disk and tmp must be nonzero because they are always profiled at least once
	if got, zero := gauges["disk"], 0.0; got <= zero {
		t.Fatalf("disk gauge: %v !> %v", got, zero)
	}
	if got, zero := gauges["tmp"], 0.0; got <= zero {
		t.Fatalf("tmp gauge: %v !> %v", got, zero)
	}
	if got, zero := profile["disk"].Mean, 0.0; got <= zero {
		t.Fatalf("disk mean: %v !> %v", got, zero)
	}
	if got, zero := profile["tmp"].Mean, 0.0; got <= zero {
		t.Fatalf("tmp mean: %v !> %v", got, zero)
	}
	if got, zero := profile["disk"].Max, 0.0; got <= zero {
		t.Fatalf("disk max: %v !> %v", got, zero)
	}
	if got, zero := profile["tmp"].Max, 0.0; got <= zero {
		t.Fatalf("tmp max: %v !> %v", got, zero)
	}
	if got, zero := profile["disk"].N, int64(0); got <= zero {
		t.Fatalf("disk N: %v !> %v", got, zero)
	}
	if got, zero := profile["tmp"].N, int64(0); got <= zero {
		t.Fatalf("tmp N: %v !> %v", got, zero)
	}

	// Disk and tmp variance must be 0 because disk and tmp can only be profiled once in 45 seconds
	if got, want := profile["disk"].Var, 0.0; got != want {
		t.Fatalf("disk variance: %v != %v", got, want)
	}
	if got, want := profile["tmp"].Var, 0.0; got != want {
		t.Fatalf("tmp variance: %v != %v", got, want)
	}

	// Disk and tmp profiles must have nonzero First and Last times
	if profile["disk"].First.IsZero() || profile["disk"].Last.IsZero() {
		t.Fatalf("disk First and Last times must not be zero if profiling has occurred.")
	}
	if profile["tmp"].First.IsZero() || profile["tmp"].Last.IsZero() {
		t.Fatalf("tmp First and Last times must not be zero if profiling has occurred.")
	}
}

func TestLocalfile(t *testing.T) {
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	dir, cleanupDir := testutil.TempDir(t, tmpDir, "files")
	defer cleanupDir()
	testutil.CreateDirectoryTree(t, dir, 3, 2, 2)
	id := reflow.Digester.FromString("TestLocalFile")

	ctx := context.Background()
	exec, err := x.Put(ctx, id, reflow.ExecConfig{
		Type: "intern",
		URL:  "localfile://" + dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = exec.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var w walker.Walker
	w.Init(dir)
	for w.Scan() {
		if w.Info().IsDir() {
			continue
		}
		p := w.Relpath()
		_, ok := res.Fileset.Map[p]
		if !ok {
			t.Errorf("missing file %q", p)
		}
		delete(res.Fileset.Map, p)
	}
	for p := range res.Fileset.Map {
		t.Errorf("extraneous file %q", p)
	}
}

// TestExecRestore simulates an executor crash & exec restore.
func TestExecRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	ctx := context.Background()
	id := reflow.Digester.FromString("sleepy")
	exec, err := x.Put(ctx, id, reflow.ExecConfig{
		Type:  "exec",
		Image: bashImage,
		Cmd:   "sleep 2",
	})
	if err != nil {
		t.Fatal(err)
	}
	x.cancel()
	err = exec.Wait(ctx)
	if err == nil {
		t.Fatal("did not get error")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error %v is not a context cancellation error", err)
	}

	// This resets the executor's state, as if it had started anew.
	if err := x.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exec, err = x.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Err != nil {
		t.Fatal(res.Err)
	}
}

func randomFileset(repo reflow.Repository) reflow.Fileset {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := r.Intn(10) + 1
	var fs reflow.Fileset
	fs.Map = make(map[string]reflow.File, n)
	for i := 0; i < n; i++ {
		p := make([]byte, r.Intn(1024)+1)
		if _, err := r.Read(p); err != nil {
			panic(err)
		}
		d, err := repo.Put(context.TODO(), bytes.NewReader(p))
		if err != nil {
			panic(err)
		}
		path := fmt.Sprintf("file%d", i)
		fs.Map[path] = reflow.File{ID: d, Size: int64(len(p)), Source: repo.URL().String() + "/" + d.String()}
	}
	return fs
}

func randomBlobStore(scheme, bucketName string, count int) (blob.Store, reflow.Fileset, error) {
	ctx := context.Background()
	store := testblob.New(scheme)
	bucket, err := store.Bucket(ctx, bucketName)
	if err != nil {
		return nil, reflow.Fileset{}, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := count
	if n == 0 {
		n = r.Intn(10) + 1
	}
	var fs reflow.Fileset
	fs.Map = make(map[string]reflow.File, n)
	for i := 0; i < n; i++ {
		p := make([]byte, r.Intn(1024)+1)
		if _, err := r.Read(p); err != nil {
			panic(err)
		}
		d := reflow.Digester.FromBytes(p)
		key := d.Hex()
		err := bucket.Put(ctx, key, int64(len(p)), bytes.NewReader(p), "")
		if err != nil {
			return nil, reflow.Fileset{}, err
		}
		path := fmt.Sprintf("unresolvedfile%d", i)
		fs.Map[path] = reflow.File{ContentHash: d, Size: int64(len(p)), Source: fmt.Sprintf("%s://%s/%s", scheme, bucketName, key)}
	}
	return store, fs, nil
}

func expectExists(t *testing.T, repo reflow.Repository, fs reflow.Fileset) {
	t.Helper()
	missing, err := repository.Missing(context.TODO(), repo, fs.Files()...)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) > 0 {
		t.Errorf("missing files: %v", missing)
	}
}

func subsetFileset(fs reflow.Fileset) reflow.Fileset {
	rfs := reflow.Fileset{Map: make(map[string]reflow.File)}
	size := len(fs.Map)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	newSize := int(r.Uint32()) % size
	if newSize < 1 {
		newSize = size
	}
	for k, v := range fs.Map {
		rfs.Map[k] = v
		newSize--
		if newSize == 0 {
			break
		}
	}
	return rfs
}

func resolvedRepoFileset(t *testing.T, name, dir string) (filerepo.Repository, reflow.Fileset, func()) {
	t.Helper()
	repoUrl, err := url.Parse(fmt.Sprintf("%s://%s", name, name))
	if err != nil {
		t.Fatal(err)
	}
	repo := filerepo.Repository{Root: dir, RepoURL: repoUrl}
	repository.RegisterScheme(name, func(u *url.URL) (reflow.Repository, error) { return &repo, nil })
	cleanup := func() { repository.UnregisterScheme(name) }
	fs := randomFileset(&repo)
	return repo, fs, cleanup
}

func fileRepoScan(t *testing.T, repo *filerepo.Repository) []digest.Digest {
	t.Helper()
	var digests []digest.Digest
	err := repo.Scan(context.Background(), func(d digest.Digest) error {
		digests = append(digests, d)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return digests
}

func TestExecLoadUnload(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, tmpDir, "test")
	defer cleanup()
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	ctx := context.Background()
	repo, resolvedfs, cleanup := resolvedRepoFileset(t, "test", dir)
	defer cleanup()

	blobScheme, blobBucket := "unresolved", "testbucket"
	store, unresolvedfs, err := randomBlobStore(blobScheme, blobBucket, 0)
	if err != nil {
		t.Fatal(err)
	}
	x.Blob = blob.Mux{blobScheme: store}
	for count := 0; count < 10; count++ {
		var (
			dir     string
			cleanup func()
		)
		unresolved := subsetFileset(unresolvedfs)
		resolved := subsetFileset(resolvedfs)
		var input reflow.Fileset
		input.List = append(input.List, unresolved, resolved)
		output, err := x.Load(ctx, repo.URL(), input)
		if err != nil {
			t.Fatal(err)
		}
		expectExists(t, x.FileRepository, output)

		digests := fileRepoScan(t, x.FileRepository)
		if got, want := len(digests), len(output.Files()); got != want {
			t.Errorf("expected %v, got %v in executor repository", got, want)
		}

		dir, cleanup = testutil.TempDir(t, tmpDir, "promoted")
		promotedRepo, promotedfs, pcleanup := resolvedRepoFileset(t, "promoted", dir)
		x.Promote(ctx, &promotedRepo)
		cleanup()

		digests = fileRepoScan(t, &promotedRepo)
		if len(digests) != 0 {
			t.Errorf("expected empty promoted repo. got %v", len(digests))
		}

		done, err := x.unload(ctx, reflow.Fileset{List: []reflow.Fileset{output, promotedfs}})
		if err != nil {
			t.Fatal(err)
		}
		<-done

		digests = fileRepoScan(t, x.FileRepository)
		if len(digests) != 0 {
			t.Errorf("expected empty digests. got %v", len(digests))
		}
		pcleanup()
	}
}

func TestExecLoadUnloadDeadObjectRace(t *testing.T) {
	x, cleanup := newTestExecutorOrSkip(t, nil)
	x.Log = log.Std
	defer cleanup()
	ctx := context.Background()

	blobScheme, blobBucket := "unresolved", "testbucket"
	store, unresolvedfs, err := randomBlobStore(blobScheme, blobBucket, 10)
	if err != nil {
		t.Fatal(err)
	}
	x.Blob = blob.Mux{blobScheme: store}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			output, err := x.Load(ctx, nil, unresolvedfs)
			if err != nil {
				t.Fatal(err)
			}
			expectExists(t, x.FileRepository, output)
			err = x.Unload(ctx, output)
			if err != nil {
				t.Fatal(err)
			}
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 10; i++ {
			output, err := x.Load(ctx, nil, unresolvedfs)
			if err != nil {
				t.Fatal(err)
			}
			expectExists(t, x.FileRepository, output)
			err = x.Unload(ctx, output)
			if err != nil {
				t.Fatal(err)
			}
		}
		done <- struct{}{}
	}()
	<-done
	<-done
	{
		output, err := x.Load(ctx, nil, unresolvedfs)
		if err != nil {
			t.Fatal(err)
		}
		expectExists(t, x.FileRepository, output)
		done, err := x.unload(ctx, output)
		if err != nil {
			t.Fatal(err)
		}
		<-done
	}
	digests := fileRepoScan(t, x.FileRepository)
	if len(digests) != 0 {
		t.Errorf("expected empty executor repo. got %v", len(digests))
	}
	for k, v := range x.refCounts {
		if v.count != 0 {
			log.Errorf("file %v has %v refcount. expected 0", k.Short(), v.count)
		}
	}
}
