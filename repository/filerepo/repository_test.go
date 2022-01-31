// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package filerepo

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/grailbio/reflow/errors"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/liveset/bloomlive"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/test/testutil"
	grailtest "github.com/grailbio/testutil"
	"github.com/willf/bloom"
)

func mustInstall(t *testing.T, r *Repository, contents string) digest.Digest {
	id, err := r.Put(context.Background(), bytes.NewReader([]byte(contents)))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newTestRepository(t *testing.T) (*Repository, func()) {
	objects, cleanup := grailtest.TempDir(t, "", "test-")
	return &Repository{Root: objects}, cleanup
}

func TestInstall(t *testing.T) {
	r, cleanup := newTestRepository(t)
	defer cleanup()
	a := mustInstall(t, r, "foo")
	b := mustInstall(t, r, "bar")
	c := mustInstall(t, r, "foo")
	if a == b || a != c {
		t.Fatalf("bad digest")
	}
}

// TestMaterialize tests that a local repository may be materialized
// to a directory structure according to a set of bindings. We also
// test that this operation is idempotent.
func TestMaterialize(t *testing.T) {
	r, repoCleanup := newTestRepository(t)
	defer repoCleanup()
	a := mustInstall(t, r, "a")
	b := mustInstall(t, r, "b")
	c := mustInstall(t, r, "c")
	d := mustInstall(t, r, "d")
	root, cleanupRoot := grailtest.TempDir(t, "", "materialize-")
	defer cleanupRoot()
	binds := map[string]digest.Digest{
		"a/b/c":   a,
		"foo/bar": b,
		"a/b/d":   c,
		"blah":    d,
	}
	for i := 0; i < 2; i++ { // Do it twice for idempotency
		if err := r.Materialize(root, binds); err != nil {
			t.Fatal(err)
		}
		dirs, files := grailtest.ListRecursively(t, root)
		for i := range files {
			files[i], _ = filepath.Rel(root, files[i])
		}
		for i := range dirs {
			dirs[i], _ = filepath.Rel(root, dirs[i])
		}
		sort.Strings(dirs)
		sort.Strings(files)
		if got, want := dirs, []string{".", "a", "a/b", "foo"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
		if got, want := files, []string{"a/b/c", "a/b/d", "blah", "foo/bar"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
		for _, f := range files {
			b, err := ioutil.ReadFile(filepath.Join(root, f))
			if err != nil {
				t.Fatal(err)
			}
			if got, want := reflow.Digester.FromString(string(b)), binds[f]; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
		}
	}
}

func TestReadFrom(t *testing.T) {
	ctx := context.Background()
	r, cleanup := newTestRepository(t)
	defer cleanup()
	src := testutil.NewExpectRepository(t, "src://foobar")
	repository.RegisterScheme("src", func(u *url.URL) (reflow.Repository, error) { return src, nil })
	defer repository.UnregisterScheme("src")

	const body = "hello world"
	id := reflow.Digester.FromString(body)
	src.Expect(testutil.RepositoryCall{
		Kind:            testutil.RepositoryGet,
		ArgID:           id,
		ReplyReadCloser: ioutil.NopCloser(bytes.NewReader([]byte(body))),
	})
	if err := r.ReadFrom(ctx, id, src.URL()); err != nil {
		t.Fatal(err)
	}

	rc, err := r.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	body1, err := ioutil.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(body1), body; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := src.Complete(); err != nil {
		t.Error(err)
	}
}

func TestReadFromDigestMismatch(t *testing.T) {
	ctx := context.Background()
	r, cleanup := newTestRepository(t)
	defer cleanup()
	er := testutil.NewExpectRepository(t, "src://foobar")
	src := &testutil.ExpectGetFilerRepository{ExpectRepository: er}
	repository.RegisterScheme("src", func(u *url.URL) (reflow.Repository, error) { return src, nil })
	defer repository.UnregisterScheme("src")

	const body = "hello world"
	const body2 = "hello corrupt world"
	id := reflow.Digester.FromString(body)
	src.Expect(testutil.RepositoryCall{
		Kind:   testutil.RepositoryGetFile,
		ArgID:  id,
		ReplyN: int64(len(body2)),
	})
	err := r.ReadFrom(ctx, id, src.URL())
	if err == nil {
		t.Fatal("got no error, want integrity error")
	}
	if !errors.Is(errors.Integrity, err) {
		t.Fatalf("got %v error, want integrity error", err)
	}
	if err := src.Complete(); err != nil {
		t.Error(err)
	}
}

func TestWriteTo(t *testing.T) {
	ctx := context.Background()
	r, cleanup := newTestRepository(t)
	defer cleanup()
	// directly, and also just test transferring between two file repositories.
	dst := testutil.NewExpectRepository(t, "dst://foobar")
	repository.RegisterScheme("dst", func(u *url.URL) (reflow.Repository, error) { return dst, nil })
	defer repository.UnregisterScheme("dst")

	const body = "hello world"
	id, err := r.Put(ctx, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}

	dst.Expect(testutil.RepositoryCall{
		Kind:     testutil.RepositoryPut,
		ArgBytes: []byte(body),
		ReplyID:  id,
	})
	if err := r.WriteTo(ctx, id, dst.URL()); err != nil {
		t.Fatal(err)
	}
	if err := dst.Complete(); err != nil {
		t.Error(err)
	}
}

func TestWalker(t *testing.T) {
	r, cleanup := newTestRepository(t)
	defer cleanup()
	digests := map[digest.Digest]bool{
		mustInstall(t, r, "foo"):  true,
		mustInstall(t, r, "bar"):  true,
		mustInstall(t, r, "baz"):  true,
		mustInstall(t, r, "blah"): true,
	}
	var w walker
	w.Init(r)
	for w.Scan() {
		if !digests[w.Digest()] {
			t.Errorf("missing object %v", w.Digest())
		}
		if _, err := os.Stat(w.Path()); err != nil {
			t.Errorf("stat %q: %v", w.Path(), err)
		}
		delete(digests, w.Digest())
	}
	if n := len(digests); n != 0 {
		t.Errorf("missing %d objects", n)
	}
}

func objects(r *Repository) map[digest.Digest]bool {
	var w walker
	w.Init(r)
	m := map[digest.Digest]bool{}
	for w.Scan() {
		m[w.Digest()] = true
	}
	return m
}

func TestCollect(t *testing.T) {
	r, cleanup := newTestRepository(t)
	defer cleanup()
	mustInstall(t, r, "foo")
	mustInstall(t, r, "bar")
	digests := map[digest.Digest]bool{
		mustInstall(t, r, "baz"):  true,
		mustInstall(t, r, "blah"): true,
	}

	live := bloom.New(64, 2)
	for d := range digests {
		live.Add(d.Bytes())
	}
	if err := r.Collect(context.Background(), bloomlive.New(live)); err != nil {
		t.Fatal(err)
	}
	if got, want := objects(r), digests; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestVacuum(t *testing.T) {
	parent, cleanupParent := newTestRepository(t)
	defer cleanupParent()
	child, cleanupChild := newTestRepository(t)
	defer cleanupChild()
	digests := map[digest.Digest]bool{
		mustInstall(t, parent, "baz"):         true,
		mustInstall(t, child, "baz"):          true,
		mustInstall(t, parent, "blah"):        true,
		mustInstall(t, child, "blahblahblah"): true,
	}

	if err := parent.Vacuum(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if got, want := objects(parent), digests; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := objects(child), (map[digest.Digest]bool{}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestScan(t *testing.T) {
	r, cleanup := newTestRepository(t)
	defer cleanup()
	digests := map[digest.Digest]bool{
		mustInstall(t, r, "baz"):  true,
		mustInstall(t, r, "blah"): true,
	}
	expected := make(map[digest.Digest]bool)
	err := r.Scan(context.Background(), func(d digest.Digest) error {
		expected[d] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for k := range digests {
		if _, ok := expected[k]; !ok {
			t.Errorf("expected %v in scanned result", k)
		}
	}
	for k := range expected {
		if _, ok := digests[k]; !ok {
			t.Errorf("unexpected %v in scanned result", k)
		}
	}
}
