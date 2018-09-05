// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package evaltest

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/repository/file"
	op "github.com/grailbio/reflow/test/flow"
	"github.com/grailbio/reflow/test/testutil"
	"github.com/grailbio/reflow/values"
	grailtest "github.com/grailbio/testutil"
)

var maxResources = reflow.Resources{
	"mem":  math.MaxFloat64,
	"cpu":  math.MaxFloat64,
	"disk": math.MaxFloat64,
}

func TestSimpleEval(t *testing.T) {
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(extern, flow.EvalConfig{Executor: &e})
	rc := testutil.EvalAsync(context.Background(), eval)
	e.Ok(intern, testutil.Files("a/b/c", "a/b/d", "x/y/z"))
	e.Ok(exec, testutil.Files("execout"))
	e.Ok(extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
	}
}

func TestGroupbyMapCollect(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("^(.)/.*", intern)
	mapCollect := op.Map(func(f *flow.Flow) *flow.Flow {
		return op.Collect("^./(.*)", "$1", f)
	}, groupby)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(mapCollect, flow.EvalConfig{Executor: &e})
	rc := testutil.EvalAsync(context.Background(), eval)
	e.Ok(intern, testutil.Files("a/one:one", "a/two:two", "a/three:three", "b/1:four", "b/2:five", "c/xxx:six"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	expect := testutil.List(testutil.Files("one", "two", "three"), testutil.Files("1:four", "2:five"), testutil.Files("xxx:six"))
	if got, want := r.Val, expect; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExecRetry(t *testing.T) {
	exec := op.Exec("image", "command", testutil.Resources)
	e := testutil.Executor{Have: testutil.Resources}
	e.Init()

	eval := flow.NewEval(exec, flow.EvalConfig{Executor: &e})
	rc := testutil.EvalAsync(context.Background(), eval)
	e.Error(exec, errors.New("failed"))
	e.Ok(exec, testutil.Files("execout"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got, want := r.Val, testutil.Files("execout"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSteal(t *testing.T) {
	const N = 10
	var execs [N]*flow.Flow
	for i := range execs {
		execs[i] = op.Exec(fmt.Sprintf("cmd%d", i), "image", testutil.Resources)
	}
	merge := op.Merge(execs[:]...)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(merge, flow.EvalConfig{Executor: &e})
	rc := testutil.EvalAsync(context.Background(), eval)
	for i := 0; i < N; i++ {
		e.Wait(execs[i])
		s := eval.Stealer()
		stolen := make([]*flow.Flow, N-i-1)
		for j := range stolen {
			stolen[j] = <-s.Admit(maxResources)
		}
		select {
		case f := <-s.Admit(maxResources):
			t.Errorf("stole too much %d: %v", i, f)
		default:
		}
		e.Ok(execs[i], reflow.Fileset{})
		// Return the rest undone.
		for _, f := range stolen {
			s.Return(f)
		}
		s.Close()
	}
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
}

func TestCacheWrite(t *testing.T) {
	for _, bottomup := range []bool{false, true} {
		intern := op.Intern("internurl")
		exec := op.Exec("image", "command", testutil.Resources, intern)
		groupby := op.Groupby("(.*)", exec)
		pullup := op.Pullup(groupby)

		assoc := testutil.NewInmemoryAssoc()
		repo := testutil.NewInmemoryRepository()

		e := testutil.Executor{Have: testutil.Resources}
		e.Init()
		e.Repo = testutil.NewInmemoryRepository()
		eval := flow.NewEval(pullup, flow.EvalConfig{
			Executor:   &e,
			CacheMode:  flow.CacheRead | flow.CacheWrite,
			Assoc:      assoc,
			Transferer: testutil.Transferer,
			Repository: repo,
			BottomUp:   bottomup,
			//			Log:      log.New(golog.New(os.Stderr, "", golog.LstdFlags), log.InfoLevel),
		})
		rc := testutil.EvalAsync(context.Background(), eval)
		var (
			internValue = testutil.WriteFiles(e.Repo, "ignored")
			execValue   = testutil.WriteFiles(e.Repo, "a", "b", "c", "d")
		)
		e.Ok(intern, internValue)
		e.Ok(exec, execValue)
		r := <-rc
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		if got, want := r.Val, execValue; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := testutil.Exists(eval, intern.CacheKeys()...), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := testutil.Value(eval, exec.Digest()), execValue; !testutil.Exists(eval, exec.CacheKeys()...) || !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestCacheLookup(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  flow.CacheRead | flow.CacheWrite,
		Assoc:      testutil.NewInmemoryAssoc(),
		Repository: testutil.NewInmemoryRepository(),
		Transferer: testutil.Transferer,
	})

	testutil.WriteCache(eval, extern.Digest() /*empty*/)
	rc := testutil.EvalAsync(context.Background(), eval)
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv( /*no flows were executed*/ ) {
		t.Error("did not expect any flows to be executed")
	}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval = flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  flow.CacheRead | flow.CacheWrite,
		Assoc:      testutil.NewInmemoryAssoc(),
		Repository: testutil.NewInmemoryRepository(),
		Transferer: testutil.Transferer,
	})
	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	rc = testutil.EvalAsync(context.Background(), eval)
	for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
		v := v
		f := mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(f, v) // identity
	}

	e.Ok(extern, reflow.Fileset{})
	r = <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(flowFiles("a")), mapFunc(flowFiles("b"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomup(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  flow.CacheRead | flow.CacheWrite,
		Assoc:      testutil.NewInmemoryAssoc(),
		Repository: testutil.NewInmemoryRepository(),
		Transferer: testutil.Transferer,
		BottomUp:   true,
		// We set a small cache lookup timeout here to shorten test times.
		// TODO(marius): allow for tighter integration or observation
		// between the evaluator and its tests, e.g., so that we can wait
		// for physical digests to be available and not rely on cache
		// timeouts for progress. Perhaps this can be done by way of
		// traces, or a way of observing individual nodes. (Observers would
		// need to be shared across canonicalizations.)
		CacheLookupTimeout: 100 * time.Millisecond,
		//			Trace:    log.New(golog.New(os.Stderr, "", golog.LstdFlags), log.InfoLevel),
	})
	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	// "a" gets a cache hit, "b" a miss.
	testutil.WriteCache(eval, mapFunc(flowFiles("a")).Digest(), "a")
	rc := testutil.EvalAsync(context.Background(), eval)
	go e.Ok(mapFunc(flowFiles("b")), testutil.Files("b"))
	e.Ok(extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(flowFiles("b"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupMissing(t *testing.T) {
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	repo := testutil.NewInmemoryRepository()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()
	eval := flow.NewEval(exec, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          flow.CacheRead | flow.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		Repository:         repo,
		Transferer:         testutil.Transferer,
		BottomUp:           true,
		CacheLookupTimeout: 100 * time.Millisecond,
	})
	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	// Make sure the assoc and fileset exists, but not all of the objects.
	testutil.WriteCache(eval, exec.Digest(), "x", "y", "z")
	repo.Delete(context.Background(), reflow.Digester.FromString("x"))

	rc := testutil.EvalAsync(context.Background(), eval)
	e.Ok(exec, testutil.Files("x", "y", "z"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(exec) {
		t.Error("wrong set of expected flows")
	}
}

func TestNoCacheExtern(t *testing.T) {
	for _, bottomup := range []bool{false, true} {
		intern := op.Intern("internurl")
		groupby := op.Groupby("(.*)", intern)
		mapFunc := func(f *flow.Flow) *flow.Flow {
			return op.Exec("image", "command", testutil.Resources, f)
		}
		mapCollect := op.Map(mapFunc, groupby)
		pullup := op.Pullup(mapCollect)
		extern := op.Extern("externurl", pullup)

		e := testutil.Executor{Have: testutil.Resources}
		e.Init()
		e.Repo = testutil.NewInmemoryRepository()

		eval := flow.NewEval(extern, flow.EvalConfig{
			Executor:      &e,
			CacheMode:     flow.CacheRead | flow.CacheWrite,
			Assoc:         testutil.NewInmemoryAssoc(),
			Repository:    testutil.NewInmemoryRepository(),
			Transferer:    testutil.Transferer,
			BottomUp:      bottomup,
			NoCacheExtern: true,
		})
		testutil.WriteCache(eval, intern.Digest(), "a", "b")
		rc := testutil.EvalAsync(context.Background(), eval)

		for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
			f := &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
			go e.Ok(mapFunc(f), v)
		}

		e.Ok(extern, reflow.Fileset{})
		r := <-rc
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
}

func TestGC(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("^(.)/.*", intern)
	mapCollect := op.Map(func(f *flow.Flow) *flow.Flow {
		return op.Collect("^./(.*)", "$1", f)
	}, groupby)
	mapPullup := op.Map(func(f *flow.Flow) *flow.Flow {
		return op.Pullup(f, op.Collect("orphan", "anotherfile", intern))
	}, mapCollect)
	pullup := op.Pullup(mapPullup)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	objects, cleanup := grailtest.TempDir(t, "", "test-")
	defer cleanup()
	repo := file.Repository{Root: objects}
	e.Repo = &repo
	eval := flow.NewEval(pullup, flow.EvalConfig{Executor: &e, GC: true})
	rc := testutil.EvalAsync(context.Background(), eval)
	files := []string{
		"a/x:x", "a/y:y", "a/z:z", "b/1:1", "b/2:2", "c/xxx:xxx",
		"orphan:orphan", "unrooted:unrooted"}
	for _, file := range files {
		contents := strings.Split(file, ":")[1]
		_, err := repo.Put(context.Background(), bytes.NewReader([]byte(contents)))
		if err != nil {
			t.Fatal(err)
		}
	}
	e.Ok(intern, testutil.Files(files...))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	expect := testutil.Files("x:x", "y:y", "z:z", "1:1", "2:2", "xxx:xxx", "anotherfile:orphan")
	if got, want := r.Val, expect; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, file := range expect.Pullup().Map {
		ok, err := repo.Contains(file.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("missing file %s:%v", k, file)
		}
	}
	for _, file := range testutil.Files("unrooted:unrooted").Files() {
		ok, err := repo.Contains(file.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("failed to collect file %v", file)
		}
	}
}

func TestData(t *testing.T) {
	// Test that data are uploaded appropriately.
	hello := []byte("hello, world!")
	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval := flow.NewEval(op.Data(hello), flow.EvalConfig{Executor: &e})
	r := <-testutil.EvalAsync(context.Background(), eval)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	_, err := e.Repo.Stat(context.Background(), reflow.Digester.FromBytes(hello))
	if err != nil {
		t.Error(err)
	}
}

func flowFiles(files ...string) *flow.Flow {
	v := testutil.Files(files...)
	return &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
}
