// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository/file"
	repotest "github.com/grailbio/reflow/repository/testutil"
	"github.com/grailbio/reflow/test/flow"
	"grail.com/testutil"
)

func TestSimpleEval(t *testing.T) {
	intern := flow.Intern("internurl")
	exec := flow.Exec("image", "command", Resources, intern)
	extern := flow.Extern("externurl", exec)

	e := Executor{Have: Resources}
	e.Init()
	eval := reflow.NewEval(extern, reflow.EvalConfig{Executor: &e})
	rc := EvalAsync(context.Background(), eval)
	e.Ok(intern, Files("a/b/c", "a/b/d", "x/y/z"))
	e.Ok(exec, Files("execout"))
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
	intern := flow.Intern("internurl")
	groupby := flow.Groupby("^(.)/.*", intern)
	mapCollect := flow.Map(func(f *reflow.Flow) *reflow.Flow {
		return flow.Collect("^./(.*)", "$1", f)
	}, groupby)

	e := Executor{Have: Resources}
	e.Init()
	eval := reflow.NewEval(mapCollect, reflow.EvalConfig{Executor: &e})
	rc := EvalAsync(context.Background(), eval)
	e.Ok(intern, Files("a/one:one", "a/two:two", "a/three:three", "b/1:four", "b/2:five", "c/xxx:six"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	expect := List(Files("one", "two", "three"), Files("1:four", "2:five"), Files("xxx:six"))
	if got, want := r.Val, expect; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExecRetry(t *testing.T) {
	exec := flow.Exec("image", "command", Resources)
	e := Executor{Have: Resources}
	e.Init()

	eval := reflow.NewEval(exec, reflow.EvalConfig{Executor: &e})
	rc := EvalAsync(context.Background(), eval)
	e.Error(exec, errors.New("failed"))
	e.Ok(exec, Files("execout"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got, want := r.Val, Files("execout"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSteal(t *testing.T) {
	const N = 10
	var execs [N]*reflow.Flow
	for i := range execs {
		execs[i] = flow.Exec(fmt.Sprintf("cmd%d", i), "image", Resources)
	}
	merge := flow.Merge(execs[:]...)

	e := Executor{Have: Resources}
	e.Init()
	eval := reflow.NewEval(merge, reflow.EvalConfig{Executor: &e})
	rc := EvalAsync(context.Background(), eval)
	for i := 0; i < N; i++ {
		e.Wait(execs[i])
		s := eval.Stealer()
		stolen := make([]*reflow.Flow, N-i-1)
		for j := range stolen {
			stolen[j] = <-s.Admit(reflow.MaxResources)
		}
		select {
		case f := <-s.Admit(reflow.MaxResources):
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
		intern := flow.Intern("internurl")
		exec := flow.Exec("image", "command", Resources, intern)
		groupby := flow.Groupby("(.*)", exec)
		pullup := flow.Pullup(groupby)

		assoc := NewInmemoryAssoc()
		repo := repotest.NewInmemory()

		e := Executor{Have: Resources}
		e.Init()
		e.Repo = repotest.NewInmemory()
		eval := reflow.NewEval(pullup, reflow.EvalConfig{
			Executor:   &e,
			CacheMode:  reflow.CacheRead | reflow.CacheWrite,
			Assoc:      assoc,
			Transferer: repotest.Transferer,
			Repository: repo,
			BottomUp:   bottomup,
			//			Log:      log.New(golog.New(os.Stderr, "", golog.LstdFlags), log.InfoLevel),
		})
		rc := EvalAsync(context.Background(), eval)
		var (
			internValue = WriteFiles(e.Repo, "ignored")
			execValue   = WriteFiles(e.Repo, "a", "b", "c", "d")
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
		if got, want := Exists(eval, intern.CacheKeys()...), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := Value(eval, exec.Digest()), execValue; !Exists(eval, exec.CacheKeys()...) || !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := Value(eval, pullup.Digest()), execValue; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestCacheLookup(t *testing.T) {
	intern := flow.Intern("internurl")
	groupby := flow.Groupby("(.*)", intern)
	mapFunc := func(f *reflow.Flow) *reflow.Flow {
		return flow.Exec("image", "command", Resources, f)
	}
	mapCollect := flow.Map(mapFunc, groupby)
	pullup := flow.Pullup(mapCollect)
	extern := flow.Extern("externurl", pullup)

	e := Executor{Have: Resources}
	e.Init()
	e.Repo = repotest.NewInmemory()
	eval := reflow.NewEval(extern, reflow.EvalConfig{
		Executor:   &e,
		CacheMode:  reflow.CacheRead | reflow.CacheWrite,
		Assoc:      NewInmemoryAssoc(),
		Repository: repotest.NewInmemory(),
		Transferer: repotest.Transferer,
	})

	WriteCache(eval, extern.Digest() /*empty*/)
	rc := EvalAsync(context.Background(), eval)
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv( /*no flows were executed*/ ) {
		t.Error("did not expect any flows to be executed")
	}

	e.Init()
	e.Repo = repotest.NewInmemory()
	eval = reflow.NewEval(extern, reflow.EvalConfig{
		Executor:   &e,
		CacheMode:  reflow.CacheRead | reflow.CacheWrite,
		Assoc:      NewInmemoryAssoc(),
		Repository: repotest.NewInmemory(),
		Transferer: repotest.Transferer,
	})
	WriteCache(eval, intern.Digest(), "a", "b")
	rc = EvalAsync(context.Background(), eval)
	for _, v := range []reflow.Fileset{Files("a"), Files("b")} {
		v := v
		f := mapFunc(v.Flow())
		go e.Ok(f, v) // identity
	}

	e.Ok(extern, reflow.Fileset{})
	r = <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(Files("a").Flow()), mapFunc(Files("b").Flow())) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomup(t *testing.T) {
	intern := flow.Intern("internurl")
	groupby := flow.Groupby("(.*)", intern)
	mapFunc := func(f *reflow.Flow) *reflow.Flow {
		return flow.Exec("image", "command", Resources, f)
	}
	mapCollect := flow.Map(mapFunc, groupby)
	pullup := flow.Pullup(mapCollect)
	extern := flow.Extern("externurl", pullup)

	e := Executor{Have: Resources}
	e.Init()
	e.Repo = repotest.NewInmemory()
	var cache WaitCache
	cache.Init()
	eval := reflow.NewEval(extern, reflow.EvalConfig{
		Executor:   &e,
		CacheMode:  reflow.CacheRead | reflow.CacheWrite,
		Assoc:      NewInmemoryAssoc(),
		Repository: repotest.NewInmemory(),
		Transferer: repotest.Transferer,
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
	WriteCache(eval, intern.Digest(), "a", "b")
	// "a" gets a cache hit, "b" a miss.
	WriteCache(eval, mapFunc(Files("a").Flow()).Digest(), "a")
	rc := EvalAsync(context.Background(), eval)
	go e.Ok(mapFunc(Files("b").Flow()), Files("b"))
	e.Ok(extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(Files("b").Flow())) {
		t.Error("wrong set of expected flows")
	}
}

func TestNoCacheExtern(t *testing.T) {
	for _, bottomup := range []bool{false, true} {
		intern := flow.Intern("internurl")
		groupby := flow.Groupby("(.*)", intern)
		mapFunc := func(f *reflow.Flow) *reflow.Flow {
			return flow.Exec("image", "command", Resources, f)
		}
		mapCollect := flow.Map(mapFunc, groupby)
		pullup := flow.Pullup(mapCollect)
		extern := flow.Extern("externurl", pullup)

		e := Executor{Have: Resources}
		e.Init()
		e.Repo = repotest.NewInmemory()

		eval := reflow.NewEval(extern, reflow.EvalConfig{
			Executor:      &e,
			CacheMode:     reflow.CacheRead | reflow.CacheWrite,
			Assoc:         NewInmemoryAssoc(),
			Repository:    repotest.NewInmemory(),
			Transferer:    repotest.Transferer,
			BottomUp:      bottomup,
			NoCacheExtern: true,
		})
		WriteCache(eval, intern.Digest(), "a", "b")
		rc := EvalAsync(context.Background(), eval)

		for _, v := range []reflow.Fileset{Files("a"), Files("b")} {
			go e.Ok(mapFunc(v.Flow()), v)
		}

		e.Ok(extern, reflow.Fileset{})
		r := <-rc
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
}

func TestGC(t *testing.T) {
	intern := flow.Intern("internurl")
	groupby := flow.Groupby("^(.)/.*", intern)
	mapCollect := flow.Map(func(f *reflow.Flow) *reflow.Flow {
		return flow.Collect("^./(.*)", "$1", f)
	}, groupby)
	mapPullup := flow.Map(func(f *reflow.Flow) *reflow.Flow {
		return flow.Pullup(f, flow.Collect("orphan", "anotherfile", intern))
	}, mapCollect)
	pullup := flow.Pullup(mapPullup)

	e := Executor{Have: Resources}
	e.Init()
	objects, cleanup := testutil.TempDir(t, "", "test-")
	defer cleanup()
	repo := file.Repository{Root: objects}
	e.Repo = &repo
	eval := reflow.NewEval(pullup, reflow.EvalConfig{Executor: &e, GC: true})
	rc := EvalAsync(context.Background(), eval)
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
	e.Ok(intern, Files(files...))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	expect := Files("x:x", "y:y", "z:z", "1:1", "2:2", "xxx:xxx", "anotherfile:orphan")
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
	for _, file := range Files("unrooted:unrooted").Files() {
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
	e := Executor{Have: Resources}
	e.Init()
	e.Repo = repotest.NewInmemory()
	eval := reflow.NewEval(flow.Data(hello), reflow.EvalConfig{Executor: &e})
	r := <-EvalAsync(context.Background(), eval)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	_, err := e.Repo.Stat(context.Background(), reflow.Digester.FromBytes(hello))
	if err != nil {
		t.Error(err)
	}
}
