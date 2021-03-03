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
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/traverse"
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
	"github.com/grailbio/reflow/types"
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

type testGenerator struct {
	valuesBySubject map[string]string
	mu              sync.Mutex
	countsByKey     map[reflow.AssertionKey]int
}

func newTestGenerator(valuesBySubject map[string]string) *testGenerator {
	return &testGenerator{valuesBySubject: valuesBySubject, countsByKey: make(map[reflow.AssertionKey]int)}
}

func (t *testGenerator) Generate(ctx context.Context, key reflow.AssertionKey) (*reflow.Assertions, error) {
	if key.Namespace == "error" {
		return nil, fmt.Errorf("error")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.countsByKey[key] = t.countsByKey[key] + 1
	return reflow.AssertionsFromEntry(
		reflow.AssertionKey{key.Subject, key.Namespace},
		map[string]string{"tag": t.valuesBySubject[key.Subject]}), nil
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, intern, testutil.Files("a/b/c", "a/b/d", "x/y/z"))
	e.Ok(ctx, exec, testutil.Files("execout"))
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
	}
}

func TestSimpleK(t *testing.T) {
	runTestKWithN(t, 4, false)
	runTestKWithN(t, 4, true)
}

func TestComplexK(t *testing.T) {
	runTestKWithN(t, 100, false)
	runTestKWithN(t, 100, true)
}

func runTestKWithN(t *testing.T, n int, bugT41260 bool) {
	interns, execs, eOuts := make([]*flow.Flow, n), make([]*flow.Flow, n), make([]reflow.Fileset, n)
	for i := 0; i < n; i++ {
		interns[i] = op.Intern(fmt.Sprintf("internurl%d", i))
		execs[i] = op.Exec(fmt.Sprintf("image%d", i), fmt.Sprintf("command%d", i), testutil.Resources, interns[i])
		path := fmt.Sprintf("execout%d", i)
		fs := testutil.Files(path)
		fs.Map["."] = fs.Map[path]
		eOuts[i] = fs
	}
	if bugT41260 {
		// Randomly assign some intern or exec to be affected by ExecDepIncorrectCacheKeyBug
		r := rand.Intn(n)
		switch rand.Intn(2) {
		case 0:
			interns[r].ExecDepIncorrectCacheKeyBug = true
		case 1:
			execs[r].ExecDepIncorrectCacheKeyBug = true
		}
	}
	assertKEval(t, interns, execs, eOuts, bugT41260)
}

func assertKEval(t *testing.T, interns, execs []*flow.Flow, eOuts []reflow.Fileset, bugT41260 bool) {
	if ni := len(interns); ni%2 != 0 {
		panic(fmt.Sprintf("requires even number: %d", ni))
	}
	n, nk := len(interns), len(interns)/2
	if ni, ne := len(interns), len(execs); ni != ne {
		panic(fmt.Sprintf("#interns %d != #execs %d", ni, ne))
	}
	if ne, no := len(execs), len(eOuts); ne != no {
		panic(fmt.Sprintf("#execs %d != #execouts %d", ne, no))
	}
	kfn := func(vs []values.T) *flow.Flow {
		fs := reflow.Fileset{Map: map[string]reflow.File{}}
		for i, v := range vs {
			file, _ := v.(reflow.Fileset).File()
			fs.Map[fmt.Sprintf("path_%d", i)] = file
		}
		return &flow.Flow{Op: flow.Val, Value: fs, FlowDigest: values.Digest(fs, types.Fileset)}
	}
	ks := make([]*flow.Flow, nk)
	wantFsEntries := make([]reflow.Fileset, nk)
	for i := 0; i < nk; i++ {
		ks[i] = op.K(fmt.Sprintf("%s_%d", t.Name(), i), kfn, execs[i*2], execs[i*2+1])
		f0, _ := eOuts[i*2].File()
		f1, _ := eOuts[i*2+1].File()
		wantFsEntries[i] = reflow.Fileset{Map: map[string]reflow.File{"path_0": f0, "path_1": f1}}
	}
	finalk := op.K(t.Name(), func(vs []values.T) *flow.Flow {
		fs := reflow.Fileset{List: make([]reflow.Fileset, len(vs))}
		for i, v := range vs {
			fs.List[i] = v.(reflow.Fileset)
		}
		return &flow.Flow{Op: flow.Val, Value: fs, FlowDigest: values.Digest(fs, types.Fileset)}
	}, ks...)

	testutil.AssignExecId(nil, interns...)
	testutil.AssignExecId(nil, execs...)
	e := testutil.Executor{Have: testutil.Resources}
	e.Init()
	eval := flow.NewEval(finalk, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	_ = traverse.Each(n, func(i int) error {
		e.Ok(ctx, interns[i], testutil.Files(fmt.Sprintf("a/b/c/%d", i)))
		e.Ok(ctx, execs[i], eOuts[i])
		return nil
	})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got, want := r.Val, testutil.List(wantFsEntries...); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if bugT41260 {
		finalKCopy := eval.FindFlowCopy(finalk)
		if finalKCopy == nil {
			t.Fatalf("cannot find equivalent for flow: %v", finalk)
		}
		if !finalKCopy.ExecDepIncorrectCacheKeyBug {
			t.Errorf("root node %v: not tagged with ExecDepIncorrectCacheKeyBug when expected", finalKCopy)
		}
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, intern, testutil.Files("a/one:one", "a/two:two", "a/three:three", "b/1:four", "b/2:five", "c/xxx:six"))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Error(ctx, exec, errors.New("failed"))
	e.Ok(ctx, exec, testutil.Files("execout"))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	stolen := []*flow.Flow{execs[N-1]}
	for i := 0; i < N; i++ {
		exec := stolen[0]
		e.Wait(ctx, exec)
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
		e.Ok(ctx, exec, reflow.Fileset{})
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		rc := testutil.EvalAsync(ctx, eval)
		var (
			internValue = testutil.WriteFiles(e.Repo, "ignored")
			execValue   = testutil.WriteFiles(e.Repo, "a", "b", "c", "d")
		)
		e.Ok(ctx, intern, internValue)
		e.Ok(ctx, exec, execValue)
		r := <-rc
		cancel()
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rc := testutil.EvalAsync(ctx, eval)
	r := <-rc
	cancel()
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
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc = testutil.EvalAsync(ctx, eval)
	for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
		v := v
		f := mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(ctx, f, v) // identity
	}

	e.Ok(ctx, extern, reflow.Fileset{})
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
		AssertionGenerator: newTestGenerator(map[string]string{"c": "v1"}),
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	// Write a cached result with same value returned by the generator.
	fs := testutil.WriteFiles(eval.Repository, "c")
	_ = fs.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"c", "namespace"}, map[string]string{"tag": "v1"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rc := testutil.EvalAsync(ctx, eval)
	r := <-rc
	cancel()
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
		AssertionGenerator: newTestGenerator(map[string]string{"c": "v1"}),
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b")

	// Write a cached result with different value returned by the generator.
	fs = testutil.WriteFiles(eval.Repository, "c")
	_ = fs.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"c", "namespace"}, map[string]string{"tag": "v2"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	rc = testutil.EvalAsync(ctx, eval)
	for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
		v := v
		f := mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(ctx, f, v) // identity
	}

	e.Ok(ctx, extern, fs)
	r = <-rc
	cancel()
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
		AssertionGenerator: newTestGenerator(map[string]string{"c": "v1"}),
		Assert:             reflow.AssertExact,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	testutil.WriteCache(eval, intern.Digest(), "a", "b")

	// Write a cached result with an assertion for which the generator will return an error.
	fs = testutil.WriteFiles(eval.Repository, "c")
	_ = fs.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"c", "error"}, map[string]string{"tag": "v"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc = testutil.EvalAsync(ctx, eval)
	for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
		v := v
		f := mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(ctx, f, v) // identity
	}

	e.Ok(ctx, extern, fs)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	go e.Ok(ctx, mapFunc(flowFiles("b")), testutil.Files("b"))
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, mapFunc(flowFiles("b"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheOffBottomup(t *testing.T) {
	testCacheOff(t, true)
}

func TestCacheOffTopdown(t *testing.T) {
	testCacheOff(t, false)
}

func testCacheOff(t *testing.T, bottomup bool) {
	t.Helper()
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
		BottomUp: bottomup,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, intern, testutil.Files("a/b/c", "a/b/d", "x/y/z"))
	e.Ok(ctx, exec, testutil.Files("execout"))
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)

	// define execA's result and wait for it to finish writing to cache.
	e.Ok(ctx, execA, testutil.Files(execFiles))
	if err := e.Exec(ctx, execA).Wait(ctx); err != nil {
		t.Fatal(err)
	}
	// TODO(marius/swami): allow for tighter integration or observation
	// Hack to wait for the exec's results to be written to the cache.
	// Calling eval.CacheWrite() directly won't work either since we
	// have to wait for the flow's state mutations anyway.
	time.Sleep(100 * time.Millisecond)
	// Now define internB's result (same as internA)
	e.Ok(ctx, internB, testutil.Files(internFiles))
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
		AssertionGenerator: newTestGenerator(map[string]string{"a": "v1", "b": "v1", "c": "v1"}),
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
	_ = fsA.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"a", "namespace"}, map[string]string{"tag": "v1"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	// "b" has "v2" but will get "v1" from the generator, so the cache hit will be rejected.
	_ = fsB.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"b", "namespace"}, map[string]string{"tag": "v2"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	// "c" has "error" in the namespace so the generator will error out, so the cache hit will be rejected.
	_ = fsC.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"c", "error"}, map[string]string{"tag": "v"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	// Cache-hits contribute towards exec ID computation.
	testutil.AssignExecId(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"a", "namespace"}, map[string]string{"tag": "v1"}), extern)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	go e.Ok(ctx, mapFunc(flowFiles("b")), testutil.Files("b"))
	go e.Ok(ctx, mapFunc(flowFiles("c")), testutil.Files("c"))
	e.Ok(ctx, extern, reflow.Fileset{})
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
		AssertionGenerator: newTestGenerator(map[string]string{"a": "va", "b": "vb", "c": "vc"}),
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
	_ = fsA.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"a", "namespace"}, map[string]string{"tag": "va"}))
	_ = fsB.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"b", "namespace"}, map[string]string{"tag": "vb"}))
	_ = fsC.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"c", "namespace"}, map[string]string{"tag": "vc"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	// Cache-hits contribute towards exec ID computation.
	testutil.AssignExecId(reflow.AssertionsFromMap(
		map[reflow.AssertionKey]map[string]string{
			{"a", "namespace"}: {"tag": "va"},
			{"b", "namespace"}: {"tag": "vb"},
			{"c", "namespace"}: {"tag": "vc"},
		}), extern)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, extern, reflow.Fileset{})
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
		AssertionGenerator: newTestGenerator(map[string]string{"c": "v1"}),
		Assert:             reflow.AssertNever,
		Repository:         testutil.NewInmemoryRepository(),
		Transferer:         testutil.Transferer,
		Log:                logger(),
		Trace:              logger(),
	})

	fs := testutil.Files("e")
	_ = fs.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"e", "namespace"}, map[string]string{"tag": "invalid"}))
	testutil.WriteCacheFileset(eval, extern.Digest(), fs)

	testutil.AssignExecId(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"e", "namespace"}, map[string]string{"tag": "v1"}), extern)

	testutil.WriteCache(eval, extern.Digest())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rc := testutil.EvalAsync(ctx, eval)
	r := <-rc
	cancel()
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
		AssertionGenerator: newTestGenerator(map[string]string{"a": "va", "b": "vb", "c": "vc"}),
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
	_ = fsA.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"a", "namespace"}, map[string]string{"tag": "invalid"}))
	_ = fsB.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"b", "namespace"}, map[string]string{"tag": "invalid"}))
	_ = fsC.AddAssertions(reflow.AssertionsFromEntry(
		reflow.AssertionKey{"c", "namespace"}, map[string]string{"tag": "invalid"}))
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	// Cache-hits contribute towards exec ID computation.
	// Values used for computing Exec ID are based on the testGenerator
	// and not the cached values for those keys.
	testutil.AssignExecId(reflow.AssertionsFromMap(
		map[reflow.AssertionKey]map[string]string{
			{"a", "namespace"}: {"tag": "va"},
			{"b", "namespace"}: {"tag": "vb"},
			{"c", "namespace"}: {"tag": "vc"},
		}), extern)

	// extern also has a cached result.
	testutil.WriteCache(eval, extern.Digest())
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc = testutil.EvalAsync(ctx, eval)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, exec, testutil.Files("x", "y", "z"))
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rc := testutil.EvalAsync(ctx, eval)
		for _, v := range []reflow.Fileset{testutil.Files("a"), testutil.Files("b")} {
			f := &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
			go e.Ok(ctx, mapFunc(f), v)
		}

		e.Ok(ctx, extern, reflow.Fileset{})
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
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
	e.Ok(ctx, intern, testutil.Files(files...))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := <-testutil.EvalAsync(ctx, eval)
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
	internA := iFs.Assertions()

	ec, eFs := op.Exec("image", "cmd1", reflow.Resources{"mem": 10, "cpu": 1, "disk": 110}, intern), fuzz.Fileset(true, true)
	ec.Value = eFs
	eA := eFs.Assertions()
	ex, exFs := op.Extern("externurl", ec), fuzz.Fileset(true, true)
	ex.Value = exFs

	merged := op.Merge(intern, ec)

	e := testutil.Executor{Have: testutil.Resources}
	eval := flow.NewEval(ex, flow.EvalConfig{
		Executor: &e,
		Log:      logger(),
		Trace:    logger(),
	})

	ieA, _ := reflow.MergeAssertions(internA, eA)

	tests := []struct {
		f    *flow.Flow
		want *reflow.Assertions
	}{
		{merged, nil}, {internNoFs, nil},
		{intern, internA},
		{ec, ieA}, {ex, nil},
	}
	for _, tt := range tests {
		eval.Mutate(tt.f, flow.Propagate)
		if tt.want == nil {
			continue
		}

		got := tt.f.Value.(reflow.Fileset).Assertions()
		if !got.Equal(tt.want) {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
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

func (a *testAlloc) CanAllocate(r reflow.Resources) (bool, error) {
	return true, nil
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

func (a *testAlloc) Load(ctx context.Context, repo *url.URL, fs reflow.Fileset) (reflow.Fileset, error) {
	fs, ok := fs.Subst(a.Sub)
	if !ok {
		return reflow.Fileset{}, errUnresolved
	}
	return fs, nil
}

func (a *testAlloc) VerifyIntegrity(ctx context.Context, fs reflow.Fileset) error {
	return nil
}

func (a *testAlloc) Unload(ctx context.Context, fs reflow.Fileset) error {
	return nil
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
	testutil.AssignExecIdRandom(intern, exec, extern)

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	e.Ok(ctx, exec, testutil.WriteFiles(e.Repo, "execout"))
	e.Ok(ctx, extern, reflow.Fileset{})
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
	ss := make(snapshotter)
	config.Snapshotter = ss

	ss["s3://bucket/prefix"] = reflow.Fileset{
		Map: map[string]reflow.File{
			"x": reflow.File{Source: "s3://bucket/prefix/x", ETag: "x", Size: 1},
			"y": reflow.File{Source: "s3://bucket/prefix/y", ETag: "y", Size: 2},
			"z": reflow.File{Source: "s3://bucket/prefix/z", ETag: "z", Size: 3},
		},
	}
	// Populate the substitution map for all known files.
	e.Sub = make(map[digest.Digest]reflow.File)
	for _, fs := range ss {
		for _, file := range fs.Files() {
			e.Sub[file.Digest()] = testutil.WriteFile(e.Repo, file.Source)
		}
	}

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecIdRandom(intern, exec, extern)

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	// We never see an intern op. Instead we see the resolved + loaded fileset.
	// Make sure the config is correct.
	cfg := e.Exec(ctx, exec).Config()
	if got, want := len(cfg.Args), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	resolved, _ := ss["s3://bucket/prefix"].Subst(e.Sub)
	if got, want := *cfg.Args[0].Fileset, resolved; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	e.Ok(ctx, exec, testutil.WriteFiles(e.Repo, "execout"))
	e.Ok(ctx, extern, reflow.Fileset{})

	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := r.Val; !got.Empty() {
		t.Fatalf("got %v, want <empty>", got)
	}
}

func TestSnapshotterMustIntern(t *testing.T) {
	e, config, done := newTestScheduler()
	defer done()
	ss := make(snapshotter)
	config.Snapshotter = ss

	ss["s3://bucket/prefix"] = reflow.Fileset{
		Map: map[string]reflow.File{
			"x": reflow.File{Source: "s3://bucket/prefix/x", ETag: "x", Size: 1},
			"y": reflow.File{Source: "s3://bucket/prefix/y", ETag: "y", Size: 2},
			"z": reflow.File{Source: "s3://bucket/prefix/z", ETag: "z", Size: 3},
		},
	}
	// Populate the substitution map for all known files.
	e.Sub = make(map[digest.Digest]reflow.File)
	for _, fs := range ss {
		for _, file := range fs.Files() {
			e.Sub[file.Digest()] = testutil.WriteFile(e.Repo, file.Source)
		}
	}

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	internMust := op.Intern("s3://bucket/prefix")
	internMust.MustIntern = true
	out := op.Merge(exec, internMust)
	testutil.AssignExecIdRandom(intern, exec, internMust, out)

	eval := flow.NewEval(out, config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalFlowAsync(ctx, eval)
	// We never see an intern op. Instead we see the resolved + loaded fileset.
	// Make sure the config is correct.
	cfg := e.Exec(ctx, exec).Config()
	if got, want := len(cfg.Args), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	resolved, _ := ss["s3://bucket/prefix"].Subst(e.Sub)
	if got, want := *cfg.Args[0].Fileset, resolved; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	e.Ok(ctx, internMust, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	e.Ok(ctx, exec, testutil.WriteFiles(e.Repo, "execout"))

	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	expected := reflow.Fileset{List:[]reflow.Fileset{testutil.Files("execout"), testutil.Files("a/b/c", "a/b/d", "x/y/z")}}
	if got := r.Val; !values.Equal(got, expected) {
		t.Fatalf("got %v, want %v", got, expected)
	}
}

func TestResolverFail(t *testing.T) {
	e, config, done := newTestScheduler()
	defer done()
	config.Snapshotter = make(snapshotter)

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecIdRandom(intern, exec, extern)

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	// Now we do see an intern op: it was forced by the failing resolve.
	e.Ok(ctx, intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	e.Ok(ctx, exec, testutil.WriteFiles(e.Repo, "execout"))
	e.Ok(ctx, extern, reflow.Fileset{})

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
	ss := make(snapshotter)
	config.Snapshotter = ss

	ss["s3://bucket/prefix"] = reflow.Fileset{
		Map: map[string]reflow.File{
			".": reflow.File{Source: "s3://bucket/prefix", ETag: "xyz", Size: 1},
		},
	}

	intern := op.Intern("s3://bucket/prefix")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)
	testutil.AssignExecIdRandom(intern, exec, extern)

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
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
	testutil.AssignExecIdRandom(intern, exec1, exec2, merged, extern)

	wa := newWaitAssoc()
	config.Assoc = wa
	config.Repository = testutil.NewInmemoryRepository()
	config.CacheMode = infra.CacheRead
	config.Transferer = testutil.Transferer
	config.BottomUp = true
	config.Log = logger()
	config.Trace = logger()
	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = testutil.EvalAsync(ctx, eval)

	wa.Tick()
	e.Ok(ctx, intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	for i := 0; i < 4; i++ {
		if e.Pending(exec1) || e.Pending(exec2) {
			t.Fatal("prematurely pending exec")
		}
		wa.Tick()
	}
	// These should now both available.
	_ = e.Exec(ctx, exec1)
	_ = e.Exec(ctx, exec2)
}

func TestRefreshAssertionBatchCache(t *testing.T) {
	torefresh := make([]*reflow.Assertions, 100)
	for i := 0; i < len(torefresh); i++ {
		torefresh[i] = reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{
			{"a", "test"}: {"tag": fmt.Sprintf("vaold%d", i)},
			{"b", "test"}: {"tag": fmt.Sprintf("vbold%d", i)},
			{"c", "test"}: {"tag": fmt.Sprintf("vcold%d", i)},
		})
	}
	want := []*reflow.Assertions{reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{
		{"a", "test"}: {"tag": "vanew"},
		{"b", "test"}: {"tag": "vbnew"},
		{"c", "test"}: {"tag": "vcnew"},
	})}

	tests := []struct {
		g     *testGenerator
		cache bool
		want  int
	}{
		{newTestGenerator(map[string]string{"a": "vanew", "b": "vbnew", "c": "vcnew"}), false, len(torefresh)},
		{newTestGenerator(map[string]string{"a": "vanew", "b": "vbnew", "c": "vcnew"}), true, 1},
	}
	for _, tt := range tests {
		intern := op.Intern("internurl")
		eval := flow.NewEval(intern, flow.EvalConfig{AssertionGenerator: tt.g})
		var cache *flow.AssertionsBatchCache
		if tt.cache {
			cache = flow.NewAssertionsBatchCache(eval)
		}
		err := traverse.Each(len(torefresh), func(i int) error {
			got, err := flow.RefreshAssertions(context.Background(), eval, []*reflow.Assertions{torefresh[i]}, cache)
			if err != nil {
				return err
			}
			if !reflow.AssertExact(context.Background(), got, want) {
				return fmt.Errorf("assertions mismatch: %v\ngot %v\nwant %v", reflow.PrettyDiff(want, got), got, want)
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		emptyA := reflow.NewRWAssertions(reflow.NewAssertions())
		_, keys := emptyA.Filter(want[0])
		for _, k := range keys {
			if got, want := tt.g.countsByKey[reflow.AssertionKey{k.Subject, k.Namespace}], tt.want; got != want {
				t.Errorf("got %d, want %d", got, want)
			}
		}
	}
}

func TestOomAdjust(t *testing.T) {
	for _, tt := range []struct {
		specified, used, want reflow.Resources
	}{
		// Specified mem > used mem.
		{reflow.Resources{"cpu": 5, "mem": 10, "disk": 7}, reflow.Resources{"cpu": 4, "mem": 9, "disk": 6}, reflow.Resources{"cpu": 4, "mem": 10, "disk": 6}},
		// Specified = used mem.
		{reflow.Resources{"cpu": 5, "mem": 10, "disk": 7}, reflow.Resources{"cpu": 4, "mem": 10, "disk": 6}, reflow.Resources{"cpu": 4, "mem": 15, "disk": 6}},
		// Specified < used mem.
		{reflow.Resources{"cpu": 5, "mem": 10, "disk": 7}, reflow.Resources{"cpu": 4, "mem": 11, "disk": 6}, reflow.Resources{"cpu": 4, "mem": 11 * flow.MemMultiplier, "disk": 6}},
		{reflow.Resources{"mem": 4 << 30}, reflow.Resources{"mem": 2 << 30}, reflow.Resources{"mem": 4 << 30}},
		{reflow.Resources{"mem": 4 << 30}, reflow.Resources{"mem": 4 << 30}, reflow.Resources{"mem": 4 << 30 * flow.MemMultiplier}},
		{reflow.Resources{"mem": 4 << 30}, reflow.Resources{"mem": 7 << 30}, reflow.Resources{"mem": 7 << 30 * flow.MemMultiplier}},
		{reflow.Resources{"mem": 700 << 30}, reflow.Resources{"mem": 600 << 30}, reflow.Resources{"mem": 700 << 30}},
		{reflow.Resources{"mem": 500 << 30}, reflow.Resources{"mem": 600 << 30}, reflow.Resources{"mem": float64(flow.OomRetryMaxExecMemory)}},
		{reflow.Resources{"mem": 900 << 30}, reflow.Resources{"mem": 600 << 30}, reflow.Resources{"mem": 900 << 30}},
		{reflow.Resources{"mem": 900 << 30}, reflow.Resources{"mem": 800 << 30}, reflow.Resources{"mem": 900 << 30}},
		{reflow.Resources{"mem": 900 << 30}, reflow.Resources{"mem": 1200 << 30}, reflow.Resources{"mem": 900 << 30}},
		{reflow.Resources{"mem": float64(flow.OomRetryMaxExecMemory - 10<<30)}, reflow.Resources{"mem": float64(flow.OomRetryMaxExecMemory - 10<<30)}, reflow.Resources{"mem": float64(flow.OomRetryMaxExecMemory)}},
	} {
		if got, want := flow.OomAdjust(tt.specified, tt.used), tt.want; !got.Equal(want) {
			t.Errorf("cpu got %v, want %v", got, want)
		}
	}
}

func flowFiles(files ...string) *flow.Flow {
	v := testutil.Files(files...)
	return &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
}
