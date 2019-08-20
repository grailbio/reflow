// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	golog "log"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository/filerepo"
	"github.com/grailbio/reflow/sched"
	op "github.com/grailbio/reflow/test/flow"
	"github.com/grailbio/reflow/test/testutil"
	"github.com/grailbio/reflow/values"
	grailtest "github.com/grailbio/testutil"
)

var (
	debug       = flag.Bool("flow.trace", false, "log verbose flow scheduler traces")
	logOnce     sync.Once
	debugLogger *log.Logger
)

func logger() *log.Logger {
	if !*debug {
		return nil
	}
	logOnce.Do(func() {
		debugLogger = log.New(golog.New(os.Stderr, "trace: ", 0), log.DebugLevel)
	})
	return debugLogger
}

var maxResources = reflow.Resources{
	"mem":  math.MaxFloat64,
	"cpu":  math.MaxFloat64,
	"disk": math.MaxFloat64,
}

var errUnresolved = errors.New("unresolved fileset")

type testGenerator struct{ valuesBySubject map[string]string }

func (t testGenerator) Generate(ctx context.Context, key reflow.GeneratorKey) (*reflow.Assertions, error) {
	if key.Namespace == "error" {
		return nil, fmt.Errorf("error")
	}
	return reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{key.Namespace, key.Subject, "tag"}: t.valuesBySubject[key.Subject]}), nil
}

func TestSimpleEval(t *testing.T) {
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecId(nil, intern, exec, extern)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})
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
	testutil.AssignExecId(nil, intern, groupby, mapCollect)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(mapCollect, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})
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
	testutil.AssignExecId(nil, exec)
	e := testutil.Executor{Have: testutil.Resources}
	e.Init()

	eval := flow.NewEval(exec, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})
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
		testutil.AssignExecId(nil, execs[i])
	}
	merge := op.Merge(execs[:]...)
	testutil.AssignExecId(nil, merge)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(merge, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})
	rc := testutil.EvalAsync(context.Background(), eval)
	stolen := []*flow.Flow{execs[N-1]}
	for i := 0; i < N; i++ {
		exec := stolen[0]
		e.Wait(exec)
		s := eval.Stealer()
		stolen = make([]*flow.Flow, N-i-1)
		for j := 0; j < N-i-1; j++ {
			stolen[j] = <-s.Admit(maxResources)
		}
		select {
		case f := <-s.Admit(maxResources):
			t.Errorf("stole too much %d: %v", i, f)
		default:
		}
		e.Ok(exec, reflow.Fileset{})
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
		testutil.AssignExecId(nil, intern, exec, groupby, pullup)

		assoc := testutil.NewInmemoryAssoc()
		repo := testutil.NewInmemoryRepository()

		e := testutil.Executor{Have: testutil.Resources}
		e.Init()
		e.Repo = testutil.NewInmemoryRepository()
		eval := flow.NewEval(pullup, flow.EvalConfig{
			Executor:   &e,
			CacheMode:  infra.CacheRead | infra.CacheWrite,
			Assoc:      assoc,
			Transferer: testutil.Transferer,
			Repository: repo,
			BottomUp:   bottomup,
			Log:        logger(),
			Trace:      logger(),
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
		exec := op.Exec("image", "command", testutil.Resources, f)
		testutil.AssignExecId(nil, exec)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup, extern)

	e := testutil.Executor{Have: testutil.Resources}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  infra.CacheRead | infra.CacheWrite,
		Assoc:      testutil.NewInmemoryAssoc(),
		Repository: testutil.NewInmemoryRepository(),
		Transferer: testutil.Transferer,
		Log:        logger(),
		Trace:      logger(),
	})

	testutil.WriteCache(eval, extern.Digest())
	rc := testutil.EvalAsync(context.Background(), eval)
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv() { //no flows to be executed
		t.Error("did not expect any flows to be executed")
	}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval = flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  infra.CacheRead | infra.CacheWrite,
		Assoc:      testutil.NewInmemoryAssoc(),
		Repository: testutil.NewInmemoryRepository(),
		Transferer: testutil.Transferer,
		Log:        logger(),
		Trace:      logger(),
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

func TestCacheLookupWithAssertions(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		exec := op.Exec("image", "command", testutil.Resources, f)
		testutil.AssignExecId(nil, exec)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup, extern)

	e := testutil.Executor{Have: testutil.Resources}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"c": "v1"}},
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	// Write a cached result with same value returned by the generator.
	fs := testutil.WriteFiles(eval.Repository, "c")
	fs.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "c", "tag"}: "v1"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	rc := testutil.EvalAsync(context.Background(), eval)
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv() { //no flows to be executed
		t.Error("did not expect any flows to be executed")
	}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval = flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"c": "v1"}},
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b")

	// Write a cached result with different value returned by the generator.
	fs = testutil.WriteFiles(eval.Repository, "c")
	fs.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "c", "tag"}: "v2"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	rc = testutil.EvalAsync(context.Background(), eval)
	for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
		v := v
		f := mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(f, v) // identity
	}

	e.Ok(extern, fs)
	r = <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(flowFiles("a")), mapFunc(flowFiles("b"))) {
		t.Error("wrong set of expected flows")
	}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval = flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"c": "v1"}},
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b")

	// Write a cached result with an assertion for which the generator will return an error.
	fs = testutil.WriteFiles(eval.Repository, "c")
	fs.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"error", "c", "tag"}: "v"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	rc = testutil.EvalAsync(context.Background(), eval)
	for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
		v := v
		f := mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(f, v) // identity
	}

	e.Ok(extern, fs)
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
		exec := op.Exec("image", "command", testutil.Resources, f)
		testutil.AssignExecId(nil, exec)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup, extern)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  infra.CacheRead | infra.CacheWrite,
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
		Log:                logger(),
		Trace:              logger(),
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

func TestCacheLookupBottomupPhysical(t *testing.T) {
	// intern from two different locations but the same contents
	internA, internB := op.Intern("internurlA"), op.Intern("internurlB")
	// execA will compute and execB should use the former's cached results
	execA := op.Exec("image", "command1", testutil.Resources, internA)
	execB := op.Exec("image", "command1", testutil.Resources, internB)
	execA.Ident, execB.Ident = "execA", "execB"
	merge := op.Merge(execA, execB)
	extern := op.Extern("externurl", merge)
	testutil.AssignExecId(nil, internA, internB, execA, execB, merge, extern)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:   &e,
		CacheMode:  infra.CacheRead | infra.CacheWrite,
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
		Log:                logger(),
		Trace:              logger(),
	})
	internFiles, execFiles := "a:same_contents", "same_exec_result"
	testutil.WriteCache(eval, internA.Digest(), internFiles)
	testutil.WriteFile(e.Repo, execFiles)
	testutil.WriteCache(eval, extern.Digest(), "extern_result")

	rc := testutil.EvalAsync(context.Background(), eval)

	// define execA's result and wait for it to finish writing to cache.
	e.Ok(execA, testutil.Files(execFiles))
	if err := e.Exec(execA).Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	// TODO(marius/swami): allow for tighter integration or observation
	// Hack to wait for the exec's results to be written to the cache.
	// Calling eval.CacheWrite() directly won't work either since we
	// have to wait for the flow's state mutations anyway.
	time.Sleep(100 * time.Millisecond)
	// Now define internB's result (same as internA)
	e.Ok(internB, testutil.Files(internFiles))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(execA, internB) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomupWithAssertions(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		exec := op.Exec("image", "command", testutil.Resources, f)
		testutil.AssignExecId(nil, exec)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()

	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"a": "v1", "b": "v1", "c": "v1"}},
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		BottomUp:           true,
		// We set a small cache lookup timeout here to shorten test times.
		// TODO(marius): allow for tighter integration or observation
		// between the evaluator and its tests, e.g., so that we can wait
		// for physical digests to be available and not rely on cache
		// timeouts for progress. Perhaps this can be done by way of
		// traces, or a way of observing individual nodes. (Observers would
		// need to be shared across canonicalizations.)
		CacheLookupTimeout: 100 * time.Millisecond,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b", "c")

	fsA, fsB, fsC := testutil.Files("a"), testutil.Files("b"), testutil.Files("c")
	// "a" has "v1" and will get "v1" from the generator, so the cache hit will be accepted.
	fsA.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "a", "tag"}: "v1"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	// "b" has "v2" but will get "v1" from the generator, so the cache hit will be rejected.
	fsB.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "b", "tag"}: "v2"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	// "c" has "error" in the namespace so the generator will error out, so the cache hit will be rejected.
	fsC.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"error", "c", "tag"}: "v"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	// Cache-hits contribute towards exec ID computation.
	testutil.AssignExecId(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "a", "tag"}: "v1",
	}), extern)

	rc := testutil.EvalAsync(context.Background(), eval)
	go e.Ok(mapFunc(flowFiles("b")), testutil.Files("b"))
	go e.Ok(mapFunc(flowFiles("c")), testutil.Files("c"))
	e.Ok(extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(flowFiles("b")), mapFunc(flowFiles("c"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomupWithAssertExact(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		exec := op.Exec("image", "command", testutil.Resources, f)
		testutil.AssignExecId(nil, exec)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()

	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"a": "va", "b": "vb", "c": "vc"}},
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		BottomUp:           true,
		// We set a small cache lookup timeout here to shorten test times.
		// TODO(marius): allow for tighter integration or observation
		// between the evaluator and its tests, e.g., so that we can wait
		// for physical digests to be available and not rely on cache
		// timeouts for progress. Perhaps this can be done by way of
		// traces, or a way of observing individual nodes. (Observers would
		// need to be shared across canonicalizations.)
		CacheLookupTimeout: 100 * time.Millisecond,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b", "c")

	fsA, fsB, fsC := testutil.Files("a"), testutil.Files("b"), testutil.Files("c")
	// All three will be cache-hits.
	fsA.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "a", "tag"}: "va"}))
	fsB.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "b", "tag"}: "vb"}))
	fsC.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{reflow.AssertionKey{"namespace", "c", "tag"}: "vc"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	// Cache-hits contribute towards exec ID computation.
	testutil.AssignExecId(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "a", "tag"}: "va",
		reflow.AssertionKey{"namespace", "b", "tag"}: "vb",
		reflow.AssertionKey{"namespace", "c", "tag"}: "vc",
	}), extern)

	rc := testutil.EvalAsync(context.Background(), eval)
	e.Ok(extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomupWithAssertNever(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		exec := op.Exec("image", "command", testutil.Resources, f)
		testutil.AssignExecId(nil, exec)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup, extern)

	e := testutil.Executor{Have: testutil.Resources}

	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	eval := flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"e": "v1"}},
		Assert:             reflow.AssertNever,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	fs := testutil.Files("e")
	fs.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "e", "tag"}: "invalid"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	testutil.AssignExecId(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "e", "tag"}: "v1",
	}), extern)

	testutil.WriteCache(eval, extern.Digest())
	rc := testutil.EvalAsync(context.Background(), eval)
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv() { //no flows to be executed
		t.Error("did not expect any flows to be executed")
	}

	e = testutil.Executor{Have: testutil.Resources}
	e.Init()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()

	eval = flow.NewEval(extern, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		AssertionGenerator: testGenerator{map[string]string{"a": "va", "b": "vb", "c": "vc"}},
		Assert:             reflow.AssertNever,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		BottomUp:           true,
		// We set a small cache lookup timeout here to shorten test times.
		// TODO(marius): allow for tighter integration or observation
		// between the evaluator and its tests, e.g., so that we can wait
		// for physical digests to be available and not rely on cache
		// timeouts for progress. Perhaps this can be done by way of
		// traces, or a way of observing individual nodes. (Observers would
		// need to be shared across canonicalizations.)
		CacheLookupTimeout: 100 * time.Millisecond,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b", "c")

	fsA, fsB, fsC := testutil.Files("a"), testutil.Files("b"), testutil.Files("c")
	// All three will be cache-hits.
	fsA.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "a", "tag"}: "invalid"}))
	fsB.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "b", "tag"}: "invalid"}))
	fsC.AddAssertions(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "c", "tag"}: "invalid"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	// Cache-hits contribute towards exec ID computation.
	// Values used for computing Exec ID are based on the testGenerator
	// and not the cached values for those keys.
	testutil.AssignExecId(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
		reflow.AssertionKey{"namespace", "a", "tag"}: "va",
		reflow.AssertionKey{"namespace", "b", "tag"}: "vb",
		reflow.AssertionKey{"namespace", "c", "tag"}: "vc",
	}), extern)

	// extern also has a cached result.
	testutil.WriteCache(eval, extern.Digest())
	rc = testutil.EvalAsync(context.Background(), eval)
	r = <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv() { //no flows to be executed
		t.Error("did not expect any flows to be executed")
	}
}

func TestCacheLookupMissing(t *testing.T) {
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	testutil.AssignExecId(nil, intern, exec)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	repo := testutil.NewInmemoryRepository()
	e.Repo = testutil.NewInmemoryRepository()
	var cache testutil.WaitCache
	cache.Init()
	eval := flow.NewEval(exec, flow.EvalConfig{
		Executor:           &e,
		CacheMode:          infra.CacheRead | infra.CacheWrite,
		Assoc:              testutil.NewInmemoryAssoc(),
		Repository:         repo,
		Transferer:         testutil.Transferer,
		BottomUp:           true,
		CacheLookupTimeout: 100 * time.Millisecond,
		Log:                logger(),
		Trace:              logger(),
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
			exec := op.Exec("image", "command", testutil.Resources, f)
			testutil.AssignExecId(nil, exec)
			return exec
		}
		mapCollect := op.Map(mapFunc, groupby)
		pullup := op.Pullup(mapCollect)
		extern := op.Extern("externurl", pullup)
		testutil.AssignExecId(nil, intern, groupby, mapCollect, pullup, extern)

		e := testutil.Executor{Have: testutil.Resources}
		e.Init()
		e.Repo = testutil.NewInmemoryRepository()

		eval := flow.NewEval(extern, flow.EvalConfig{
			Executor:      &e,
			CacheMode:     infra.CacheRead | infra.CacheWrite,
			Assoc:         testutil.NewInmemoryAssoc(),
			Repository:    testutil.NewInmemoryRepository(),
			Transferer:    testutil.Transferer,
			BottomUp:      bottomup,
			NoCacheExtern: true,
			Log:           logger(),
			Trace:         logger(),
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
		c := op.Collect("^./(.*)", "$1", f)
		testutil.AssignExecId(nil, c)
		return c
	}, groupby)
	mapPullup := op.Map(func(f *flow.Flow) *flow.Flow {
		p := op.Pullup(f, op.Collect("orphan", "anotherfile", intern))
		testutil.AssignExecId(nil, p)
		return p
	}, mapCollect)
	pullup := op.Pullup(mapPullup)
	testutil.AssignExecId(nil, intern, groupby, mapCollect, mapPullup, pullup)

	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	objects, cleanup := grailtest.TempDir(t, "", "test-")
	defer cleanup()
	repo := filerepo.Repository{Root: objects}
	e.Repo = &repo
	eval := flow.NewEval(pullup, flow.EvalConfig{
		Executor: &e,
		GC:       true,
		Log:      logger(),
		Trace:    logger(),
	})
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
	eval := flow.NewEval(op.Data(hello), flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})
	r := <-testutil.EvalAsync(context.Background(), eval)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	_, err := e.Repo.Stat(context.Background(), reflow.Digester.FromBytes(hello))
	if err != nil {
		t.Error(err)
	}
}

func TestPropagateAssertions(t *testing.T) {
	fuzz := testutil.NewFuzz(nil)
	internNoFs := op.Intern("url")
	intern, iFs := op.Intern("url"), fuzz.Fileset(true, true)
	intern.Value = iFs
	internA, _ := getAssertions(iFs)

	ec, eFs := op.Exec("image", "cmd1", reflow.Resources{"mem": 10, "cpu": 1, "disk": 110}, intern), fuzz.Fileset(true, true)
	ec.Value = eFs
	eA, _ := getAssertions(eFs)
	ex, exFs := op.Extern("externurl", ec), fuzz.Fileset(true, true)
	ex.Value = exFs
	exA, _ := getAssertions(exFs)

	merged := op.Merge(intern, ec)

	e := testutil.Executor{Have: testutil.Resources}
	eval := flow.NewEval(ex, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})

	ieA := new(reflow.Assertions)
	ieA.AddFrom(internA)
	ieA.AddFrom(eA)

	fullA := new(reflow.Assertions)
	fullA.AddFrom(internA)
	fullA.AddFrom(eA)
	fullA.AddFrom(exA)

	tests := []struct {
		f    *flow.Flow
		want *reflow.Assertions
	}{
		{merged, nil}, {internNoFs, nil},
		{intern, internA},
		{ec, ieA}, {ex, fullA},
	}
	for _, tt := range tests {
		eval.Mutate(tt.f, flow.Propagate)
		if tt.want == nil {
			continue
		}

		got, err := getAssertions(tt.f.Value.(reflow.Fileset))
		if err != nil {
			t.Errorf("unexpected: %v", err)
		}
		if !got.Equal(tt.want) {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func getAssertions(fs reflow.Fileset) (*reflow.Assertions, error) {
	a := new(reflow.Assertions)
	err := fs.WriteAssertions(a)
	return a, err
}

// TestAlloc is used in scheduler tests. As well as implementing
// alloc, it implements sched.Cluster, handing itself out.
type testAlloc struct {
	testutil.Executor
	// Repo is the alloc's repository.
	Repo reflow.Repository
	// Sub is the substitution map used for reference loading.
	Sub map[digest.Digest]reflow.File

	mu        sync.Mutex
	allocated bool
}

func (a *testAlloc) Repository() reflow.Repository {
	return a.Repo
}

func (a *testAlloc) Remove(ctx context.Context, id digest.Digest) error {
	panic("not implemented")
}

func (a *testAlloc) Pool() pool.Pool {
	panic("not implemented")
}

func (a *testAlloc) ID() string {
	return fmt.Sprintf("%p", a)
}

func (a *testAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	panic("not implemented")
}

func (a *testAlloc) Free(ctx context.Context) error {
	panic("not implemented")
}

func (a *testAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	return interval, ctx.Err()
}

func (a *testAlloc) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.allocated {
		return nil, errors.E(errors.ResourcesExhausted)
	}
	a.allocated = true
	return a, ctx.Err()
}

func (a *testAlloc) Load(ctx context.Context, fs reflow.Fileset) (reflow.Fileset, error) {
	fs, ok := fs.Subst(a.Sub)
	if !ok {
		return reflow.Fileset{}, errUnresolved
	}
	return fs, nil
}

// NewTestScheduler starts up a new scheduler intended for testing,
// and returns the alloc to be scrutinized under this setup. The returned
// config can be used to configure evaluation.
func newTestScheduler() (alloc *testAlloc, config flow.EvalConfig, done func()) {
	alloc = new(testAlloc)
	alloc.Have.Scale(testutil.Resources, 2.0)
	alloc.Repo = testutil.NewInmemoryRepository()
	alloc.Init()

	sched := sched.New()
	sched.Transferer = testutil.Transferer
	sched.Repository = alloc.Repo
	sched.Cluster = alloc
	sched.MinAlloc = reflow.Resources{}
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	done = func() {
		cancel()
		wg.Wait()
	}
	go func() {
		sched.Do(ctx)
		wg.Done()
	}()

	var out *log.Logger // = log.New(golog.New(os.Stderr, "", golog.LstdFlags), log.DebugLevel)
	config = flow.EvalConfig{
		Repository: sched.Repository,
		Scheduler:  sched,
		Log:        out,
		Trace:      out,
	}
	return
}

type snapshotter map[string]reflow.Fileset

func (s snapshotter) Snapshot(ctx context.Context, url string) (reflow.Fileset, error) {
	fs, ok := s[url]
	if !ok {
		return reflow.Fileset{}, errors.E("snapshot", url, errors.NotExist)
	}
	return fs, nil
}

func TestScheduler(t *testing.T) {
	e, config, done := newTestScheduler()
	defer done()

	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecId(nil, intern, exec, extern)

	eval := flow.NewEval(extern, config)
	rc := testutil.EvalAsync(context.Background(), eval)
	e.Ok(intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	e.Ok(exec, testutil.WriteFiles(e.Repo, "execout"))
	e.Ok(extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
	}
}

func TestSnapshotter(t *testing.T) {
	e, config, done := newTestScheduler()
	defer done()
	snapshotter := make(snapshotter)
	config.Snapshotter = snapshotter

	snapshotter["s3://bucket/prefix"] = reflow.Fileset{
		Map: map[string]reflow.File{
			"x": reflow.File{Source: "s3://bucket/prefix/x", ETag: "x", Size: 1},
			"y": reflow.File{Source: "s3://bucket/prefix/y", ETag: "y", Size: 2},
			"z": reflow.File{Source: "s3://bucket/prefix/z", ETag: "z", Size: 3},
		},
	}
	// Populate the substitution map for all known files.
	e.Sub = make(map[digest.Digest]reflow.File)
	for _, fs := range snapshotter {
		for _, file := range fs.Files() {
			e.Sub[file.Digest()] = testutil.WriteFile(e.Repo, file.Source)
		}
	}

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecId(nil, intern, exec, extern)

	eval := flow.NewEval(extern, config)
	rc := testutil.EvalAsync(context.Background(), eval)
	// We never see an intern op. Instead we see the resolved + loaded fileset.
	// Make sure the config is correct.
	cfg := e.Exec(exec).Config()
	if got, want := len(cfg.Args), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	resolved, _ := snapshotter["s3://bucket/prefix"].Subst(e.Sub)
	if got, want := *cfg.Args[0].Fileset, resolved; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	e.Ok(exec, testutil.WriteFiles(e.Repo, "execout"))
	e.Ok(extern, reflow.Fileset{})

	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
	}
}

func TestResolverFail(t *testing.T) {
	e, config, done := newTestScheduler()
	defer done()
	config.Snapshotter = make(snapshotter)

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecId(nil, intern, exec, extern)

	eval := flow.NewEval(extern, config)
	rc := testutil.EvalAsync(context.Background(), eval)
	// Now we do see an intern op: it was forced by the failing resolve.
	e.Ok(intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	e.Ok(exec, testutil.WriteFiles(e.Repo, "execout"))
	e.Ok(extern, reflow.Fileset{})

	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
	}
}

func TestLoadFail(t *testing.T) {
	_, config, done := newTestScheduler()
	defer done()
	snapshotter := make(snapshotter)
	config.Snapshotter = snapshotter

	snapshotter["s3://bucket/prefix"] = reflow.Fileset{
		Map: map[string]reflow.File{
			".": reflow.File{Source: "s3://bucket/prefix", ETag: "xyz", Size: 1},
		},
	}

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecId(nil, intern, exec, extern)

	eval := flow.NewEval(extern, config)
	rc := testutil.EvalAsync(context.Background(), eval)
	// Here we see no ops at all, since the load fails the flow.
	r := <-rc
	if got, want := r.Err, errUnresolved; errors.Match(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSchedulerSubmit(t *testing.T) {
	e, config, done := newTestScheduler()
	defer done()

	intern := op.Intern("internurl")
	exec1 := op.Exec("image", "command", testutil.Resources, intern)
	exec2 := op.Exec("image", "command2", testutil.Resources, intern)
	merged := op.Pullup(exec1, exec2)
	extern := op.Extern("externurl", merged)
	testutil.AssignExecId(nil, intern, exec1, exec2, merged, extern)

	wa := newWaitAssoc()
	config.Assoc = wa
	config.Repository = testutil.NewInmemoryRepository()
	config.CacheMode = infra.CacheRead
	config.Transferer = testutil.Transferer
	config.BottomUp = true
	config.Log = logger()
	config.Trace = logger()
	eval := flow.NewEval(extern, config)
	_ = testutil.EvalAsync(context.Background(), eval)

	wa.Tick()
	e.Ok(intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	for i := 0; i < 4; i++ {
		if e.Pending(exec1) || e.Pending(exec2) {
			t.Fatal("prematurely pending exec")
		}
		wa.Tick()
	}
	// These should now both available.
	_ = e.Exec(exec1)
	_ = e.Exec(exec2)
}

func flowFiles(files ...string) *flow.Flow {
	v := testutil.Files(files...)
	return &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
}
