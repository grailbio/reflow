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
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/sched"
	op "github.com/grailbio/reflow/test/flow"
	"github.com/grailbio/reflow/test/testutil"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

var (
	timeout     = 5 * time.Second
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
		reflow.AssertionKey{Subject: key.Subject, Namespace: key.Namespace},
		map[string]string{"etag": t.valuesBySubject[key.Subject]}), nil
}

func TestSimpleEval(t *testing.T) {
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)

	e, config, done := newTestScheduler()
	defer done()
	eval := flow.NewEval(extern, config)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

func TestSimpleK(t *testing.T) {
	runTestKWithN(t, 4, false)
	runTestKWithN(t, 4, true)
}

func TestComplexK(t *testing.T) {
	runTestKWithN(t, 100, false)
	runTestKWithN(t, 100, true)
}

func runTestKWithN(t *testing.T, n int, bugT41260 bool) {
	e, config, done := newTestScheduler()
	defer done()

	interns, execs, eOuts := make([]*flow.Flow, n), make([]*flow.Flow, n), make([]reflow.Fileset, n)
	eOutPaths := make([]string, n)
	for i := 0; i < n; i++ {
		interns[i] = op.Intern(fmt.Sprintf("internurl%d", i))
		execs[i] = op.Exec(fmt.Sprintf("image%d", i), fmt.Sprintf("command%d", i), testutil.Resources, interns[i])
		path := fmt.Sprintf("execout%d", i)
		eOutPaths[i] = path
		fs := testutil.WriteFiles(e.Repo, path)
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
	assertKEval(t, e, config, interns, execs, eOuts, bugT41260)
}

func assertKEval(t *testing.T, e *testAlloc, config flow.EvalConfig, interns, execs []*flow.Flow, eOuts []reflow.Fileset, bugT41260 bool) {
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

	eval := flow.NewEval(finalk, config)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	_ = traverse.Each(n, func(i int) error {
		e.Ok(ctx, interns[i], testutil.WriteFiles(e.Repo, fmt.Sprintf("a/b/c/%d", i)))
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

	e, config, done := newTestScheduler()
	defer done()

	eval := flow.NewEval(mapCollect, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)

	e.Ok(ctx, intern, testutil.WriteFiles(e.Repo, "a/one:one", "a/two:two", "a/three:three", "b/1:four", "b/2:five", "c/xxx:six"))
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

	e, config, done := newTestScheduler()
	defer done()
	eval := flow.NewEval(exec, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Error(ctx, exec, errors.New("failed"))
	e.Ok(ctx, exec, testutil.WriteFiles(e.Repo, "execout"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got, want := r.Val, testutil.Files("execout"); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCacheWrite(t *testing.T) {
	for _, bottomup := range []bool{false, true} {
		// The following is done in a func to defer context cancellations.
		func() {
			e, config, done := newTestScheduler()
			defer done()
			config.CacheMode = infra.CacheRead | infra.CacheWrite
			config.BottomUp = bottomup

			intern := op.Intern("internurl")
			exec := op.Exec("image", "command", testutil.Resources, intern)
			groupby := op.Groupby("(.*)", exec)
			pullup := op.Pullup(groupby)

			eval := flow.NewEval(pullup, config)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		}()
	}
}

func TestCacheLookupFilesetMigration(t *testing.T) {
	*debug = true
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)
	extern := op.Extern("externurl", exec)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	eval := flow.NewEval(extern, config)

	// TODO(smahadevan): remove once fileset migration is complete
	fs := testutil.WriteFiles(eval.Repository, "foo", "bar")
	buf := new(bytes.Buffer)
	wErr := fs.Write(buf, assoc.Fileset, true, true)
	if wErr != nil {
		t.Fatal(wErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	v1Digest, pErr := eval.Repository.Put(ctx, buf)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if err := eval.Assoc.Store(ctx, assoc.Fileset, exec.Digest(), v1Digest); err != nil {
		t.Fatal(err)
	}
	cancel()

	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	cancel()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern) { // only extern is executed, eval will read v1 fileset format
		t.Error("wrong set of expected flows")
	}

	// expect eval to also writeback v2 format
	fsWriteback := testutil.Value(eval, exec.Digest())
	if diff, nomatch := fsWriteback.Diff(fs); nomatch {
		t.Errorf("expected writeback fs to match input fs but found following diff: %s", diff)
	}
}

func TestCacheLookup(t *testing.T) {
	// TopDown evaluation with cache entries for intern and execs.
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	m := newMapper(func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	})
	mapCollect := op.Map(m.mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	eval := flow.NewEval(extern, config)

	// Write cache entries for intern and execs.
	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	testutil.WriteCache(eval, m.mapFunc(flowFiles("a")).Digest(), "c")
	testutil.WriteCache(eval, m.mapFunc(flowFiles("b")).Digest(), "d")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	rc := testutil.EvalAsync(ctx, eval)
	// Prevent scheduler submission because externs are not cached.
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	cancel()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern) { // only the extern is executed.
		t.Error("wrong set of expected flows")
	}

	// TopDown evaluation with cache entry for intern.
	e, config, done = newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	eval = flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc = testutil.EvalAsync(ctx, eval)
	for _, v := range []reflow.Fileset{
		testutil.WriteFiles(e.Repo, "a"),
		testutil.WriteFiles(e.Repo, "b"),
	} {
		v := v
		f := m.mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(ctx, f, v) // identity
	}

	e.Ok(ctx, extern, reflow.Fileset{})
	r = <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, m.mapFunc(flowFiles("a")), m.mapFunc(flowFiles("b"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupWithAssertions(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	m := newMapper(func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	})
	mapCollect := op.Map(m.mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"c1": "v1"})
	config.Assert = reflow.AssertExact
	eval := flow.NewEval(extern, config)

	// Write cached results with the same values returned by the generator.
	testutil.WriteCache(eval, intern.Digest(), "a1", "b1")
	fs := testutil.WriteFiles(eval.Repository, "c1")
	assertion := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "c1", Namespace: "blob"}, map[string]string{"etag": "v1"})
	if err := fs.AddAssertions(assertion); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("a1")).Digest(), fs)
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("b1")).Digest(), fs)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	cancel()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern) {
		t.Error("wrong set of expected flows")
	}

	fmt.Println("finished first async eval")

	e, config, done = newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"c2": "v1"})
	config.Assert = reflow.AssertExact
	eval = flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a2", "b2")

	// Write a cached result with different value returned by the generator.
	fs = testutil.WriteFiles(eval.Repository, "c2")
	assertion = reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "c2", Namespace: "blob"}, map[string]string{"etag": "v2"})
	if err := fs.AddAssertions(assertion); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("a2")).Digest(), fs)
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("b2")).Digest(), fs)
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	rc = testutil.EvalAsync(ctx, eval)
	for _, v := range []reflow.Fileset{
		testutil.WriteFiles(e.Repo, "a2"),
		testutil.WriteFiles(e.Repo, "b2"),
	} {
		v := v
		f := m.mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(ctx, f, v) // identity
	}

	testutil.WriteFiles(e.Repo, "c2")
	e.Ok(ctx, extern, reflow.Fileset{})
	r = <-rc
	cancel()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, m.mapFunc(flowFiles("a2")), m.mapFunc(flowFiles("b2"))) {
		t.Error("wrong set of expected flows")
	}

	e, config, done = newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"c3": "v1"})
	config.Assert = reflow.AssertExact
	eval = flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a3", "b3")

	// Write a cached result with an assertion for which the generator will return an error.
	fs = testutil.WriteFiles(eval.Repository, "c3")
	assertion = reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "c3", Namespace: "error"}, map[string]string{"etag": "v"})
	if err := fs.AddAssertions(assertion); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("a3")).Digest(), fs)
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("b3")).Digest(), fs)

	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc = testutil.EvalAsync(ctx, eval)
	for _, v := range []reflow.Fileset{
		testutil.WriteFiles(e.Repo, "a3"),
		testutil.WriteFiles(e.Repo, "b3"),
	} {
		v := v
		f := m.mapFunc(&flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done})
		go e.Ok(ctx, f, v) // identity
	}

	testutil.WriteFiles(e.Repo, "c3")
	e.Ok(ctx, extern, fs)
	r = <-rc

	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, m.mapFunc(flowFiles("a3")), m.mapFunc(flowFiles("b3"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomup(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	m := newMapper(func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	})
	mapCollect := op.Map(m.mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.BottomUp = true
	// We set a small cache lookup timeout here to shorten test times.
	// TODO(marius): allow for tighter integration or observation
	// between the evaluator and its tests, e.g., so that we can wait
	// for physical digests to be available and not rely on cache
	// timeouts for progress. Perhaps this can be done by way of
	// traces, or a way of observing individual nodes. (Observers would
	// need to be shared across canonicalizations.)
	config.CacheLookupTimeout = 100 * time.Millisecond
	eval := flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	// "a" gets a cache hit, "b" a miss.
	testutil.WriteCache(eval, m.mapFunc(flowFiles("a")).Digest(), "a")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	go e.Ok(ctx, m.mapFunc(flowFiles("b")), testutil.WriteFiles(e.Repo, "b"))
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, m.mapFunc(flowFiles("b"))) {
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

	e, config, done := newTestScheduler()
	config.BottomUp = bottomup
	defer done()
	eval := flow.NewEval(extern, config)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

func TestCacheLookupBottomupPhysical(t *testing.T) {
	// intern from two different locations but the same contents
	internA, internB := op.Intern("internurlA"), op.Intern("internurlB")
	// execA will compute and execB should use the former's cached results
	execA := op.Exec("image", "command1", testutil.Resources, internA)
	execB := op.Exec("image", "command1", testutil.Resources, internB)
	execA.Ident, execB.Ident = "execA", "execB"
	merge := op.Merge(execA, execB)
	extern := op.Extern("externurl", merge)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.BottomUp = true
	// We set a small cache lookup timeout here to shorten test times.
	// TODO(marius): allow for tighter integration or observation
	// between the evaluator and its tests, e.g., so that we can wait
	// for physical digests to be available and not rely on cache
	// timeouts for progress. Perhaps this can be done by way of
	// traces, or a way of observing individual nodes. (Observers would
	// need to be shared across canonicalizations.)
	config.CacheLookupTimeout = 100 * time.Millisecond

	eval := flow.NewEval(extern, config)

	internFiles, execFiles := "a:same_contents", "same_exec_result"
	testutil.WriteCache(eval, internA.Digest(), internFiles)
	testutil.WriteFile(e.Repo, execFiles)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)

	// define execA's result and wait for it to finish writing to cache.
	e.Ok(ctx, execA, testutil.WriteFiles(e.Repo, execFiles))
	if err := e.Exec(ctx, execA).Wait(ctx); err != nil {
		t.Fatal(err)
	}
	// TODO(marius/swami): allow for tighter integration or observation
	// Hack to wait for the exec's results to be written to the cache.
	// Calling eval.CacheWrite() directly won't work either since we
	// have to wait for the flow's state mutations anyway.
	time.Sleep(200 * time.Millisecond)
	// Now define internB's result (same as internA)
	e.Ok(ctx, internB, testutil.WriteFiles(e.Repo, internFiles))
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, execA, internB) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomupWithAssertions(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	m := newMapper(func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	})
	mapCollect := op.Map(m.mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.BottomUp = true
	// We set a small cache lookup timeout here to shorten test times.
	// TODO(marius): allow for tighter integration or observation
	// between the evaluator and its tests, e.g., so that we can wait
	// for physical digests to be available and not rely on cache
	// timeouts for progress. Perhaps this can be done by way of
	// traces, or a way of observing individual nodes. (Observers would
	// need to be shared across canonicalizations.)
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"a": "v1", "b": "v1", "c": "v1"})
	config.Assert = reflow.AssertExact
	eval := flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b", "c")

	fsA, fsB, fsC := testutil.Files("a"), testutil.Files("b"), testutil.Files("c")
	// "a" has "v1" and will get "v1" from the generator, so the cache hit will be accepted.
	assertionA := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "a", Namespace: "blob"}, map[string]string{"etag": "v1"})
	if err := fsA.AddAssertions(assertionA); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("a")).Digest(), fsA)
	// "b" has "v2" but will get "v1" from the generator, so the cache hit will be rejected.
	assertionB := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "b", Namespace: "blob"}, map[string]string{"etag": "v2"})
	if err := fsB.AddAssertions(assertionB); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("b")).Digest(), fsB)
	// "c" has "error" in the namespace so the generator will error out, so the cache hit will be rejected.
	assertionC := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "c", Namespace: "error"}, map[string]string{"etag": "v"})
	if err := fsC.AddAssertions(assertionC); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("c")).Digest(), fsC)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	go e.Ok(ctx, m.mapFunc(flowFiles("b")), testutil.WriteFiles(e.Repo, "b"))
	go e.Ok(ctx, m.mapFunc(flowFiles("c")), testutil.WriteFiles(e.Repo, "c"))
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern, m.mapFunc(flowFiles("b")), m.mapFunc(flowFiles("c"))) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupBottomupWithAssertExact(t *testing.T) {
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	mapFunc := func(f *flow.Flow) *flow.Flow {
		exec := op.Exec("image", "command", testutil.Resources, f)
		return exec
	}
	mapCollect := op.Map(mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.BottomUp = true
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"a": "va", "b": "vb", "c": "vc"})
	config.Assert = reflow.AssertExact
	eval := flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b", "c")

	fsA, fsB, fsC := testutil.Files("a"), testutil.Files("b"), testutil.Files("c")
	// All three will be cache-hits.
	assertionA := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "a", Namespace: "blob"}, map[string]string{"etag": "va"})
	if err := fsA.AddAssertions(assertionA); err != nil {
		t.Fatal(err)
	}
	assertionB := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "b", Namespace: "blob"}, map[string]string{"etag": "vb"})
	if err := fsB.AddAssertions(assertionB); err != nil {
		t.Fatal(err)
	}
	assertionC := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "c", Namespace: "blob"}, map[string]string{"etag": "vc"})
	if err := fsC.AddAssertions(assertionC); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("a")).Digest(), fsA)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("b")).Digest(), fsB)
	testutil.WriteCacheFileset(eval, mapFunc(flowFiles("c")).Digest(), fsC)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
	// TopDown evaluation with filesets containing invalid assertions.
	intern := op.Intern("internurl")
	groupby := op.Groupby("(.*)", intern)
	m := newMapper(func(f *flow.Flow) *flow.Flow {
		return op.Exec("image", "command", testutil.Resources, f)
	})
	mapCollect := op.Map(m.mapFunc, groupby)
	pullup := op.Pullup(mapCollect)
	extern := op.Extern("externurl", pullup)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"c": "v1"})
	config.Assert = reflow.AssertNever
	eval := flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	fsA := testutil.Files("a")
	assertionA := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "a", Namespace: "blob"}, map[string]string{"etag": "invalid"})
	if err := fsA.AddAssertions(assertionA); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("a")).Digest(), fsA)
	fsB := testutil.Files("b")
	assertionB := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "b", Namespace: "blob"}, map[string]string{"etag": "invalid"})
	if err := fsB.AddAssertions(assertionB); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("b")).Digest(), fsB)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, extern, reflow.Fileset{})
	r := <-rc
	cancel()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern) {
		t.Error("wrong set of expected flows")
	}

	// BottomUp evaluation with filesets containing invalid assertions.
	e, config, done = newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.BottomUp = true
	// We set a small cache lookup timeout here to shorten test times.
	// TODO(marius): allow for tighter integration or observation
	// between the evaluator and its tests, e.g., so that we can wait
	// for physical digests to be available and not rely on cache
	// timeouts for progress. Perhaps this can be done by way of
	// traces, or a way of observing individual nodes. (Observers would
	// need to be shared across canonicalizations.)
	config.CacheLookupTimeout = 100 * time.Millisecond
	config.AssertionGenerator = newTestGenerator(map[string]string{"a": "va", "b": "vb", "c": "vc"})
	config.Assert = reflow.AssertNever
	eval = flow.NewEval(extern, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b", "c")

	fsA, fsB, fsC := testutil.Files("a"), testutil.Files("b"), testutil.Files("c")
	// All three will be cache-hits.
	assertionA = reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "a", Namespace: "blob"}, map[string]string{"etag": "invalid"})
	if err := fsA.AddAssertions(assertionA); err != nil {
		t.Fatal(err)
	}
	assertionB = reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "b", Namespace: "blob"}, map[string]string{"etag": "invalid"})
	if err := fsB.AddAssertions(assertionB); err != nil {
		t.Fatal(err)
	}
	assertionC := reflow.AssertionsFromEntry(
		reflow.AssertionKey{Subject: "c", Namespace: "blob"}, map[string]string{"etag": "invalid"})
	if err := fsC.AddAssertions(assertionC); err != nil {
		t.Fatal(err)
	}
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("a")).Digest(), fsA)
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("b")).Digest(), fsB)
	testutil.WriteCacheFileset(eval, m.mapFunc(flowFiles("c")).Digest(), fsC)

	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc = testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, extern, reflow.Fileset{})
	r = <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(extern) {
		t.Error("wrong set of expected flows")
	}
}

func TestCacheLookupMissing(t *testing.T) {
	intern := op.Intern("internurl")
	exec := op.Exec("image", "command", testutil.Resources, intern)

	e, config, done := newTestScheduler()
	defer done()
	config.CacheMode = infra.CacheRead | infra.CacheWrite
	config.BottomUp = true
	config.CacheLookupTimeout = 100 * time.Millisecond
	eval := flow.NewEval(exec, config)

	testutil.WriteCache(eval, intern.Digest(), "a", "b")
	// Make sure the assoc and fileset exists, but not all of the objects.
	testutil.WriteCache(eval, exec.Digest(), "x", "y", "z")
	eval.Repository.(*testutil.InmemoryRepository).Delete(context.Background(), reflow.Digester.FromString("x"))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rc := testutil.EvalAsync(ctx, eval)
	e.Ok(ctx, exec, testutil.WriteFiles(e.Repo, "x", "y", "z"))
	r := <-rc
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !e.Equiv(exec) {
		t.Error("wrong set of expected flows")
	}
}

func TestExtern(t *testing.T) {
	for _, bottomup := range []bool{false, true} {
		// The following is done in a func to defer context cancellations.
		func() {
			intern := op.Intern("internurl")
			groupby := op.Groupby("(.*)", intern)
			m := newMapper(func(f *flow.Flow) *flow.Flow {
				return op.Exec("image", "command", testutil.Resources, f)
			})
			mapCollect := op.Map(m.mapFunc, groupby)
			pullup := op.Pullup(mapCollect)
			extern := op.Extern("externurl", pullup)

			e, config, done := newTestScheduler()
			defer done()
			config.CacheMode = infra.CacheRead | infra.CacheWrite
			config.BottomUp = bottomup
			eval := flow.NewEval(extern, config)

			testutil.WriteCache(eval, intern.Digest(), "a", "b")
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			rc := testutil.EvalAsync(ctx, eval)
			for _, v := range []reflow.Fileset{
				testutil.WriteFiles(e.Repo, "a"),
				testutil.WriteFiles(e.Repo, "b"),
			} {
				f := &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
				go e.Ok(ctx, m.mapFunc(f), v)
			}

			e.Ok(ctx, extern, reflow.Fileset{})
			r := <-rc
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			if !e.Equiv(extern, m.mapFunc(flowFiles("a")), m.mapFunc(flowFiles("b"))) {
				t.Error("wrong set of expected flows")
			}
			// Assert that we don't write cache entries for externs.
			want := errors.E(errors.NotExist)
			if _, _, err := eval.Assoc.Get(ctx, assoc.Fileset, extern.Digest()); !errors.Match(want, err) {
				t.Fatal("did not expect to write a cache entry for extern")
			}
			if _, _, err := eval.Assoc.Get(ctx, assoc.FilesetV2, extern.Digest()); !errors.Match(want, err) {
				t.Fatal("did not expect to write a cache entry for extern")
			}
		}()
	}
}

func TestNoCacheExternDeprecation(t *testing.T) {
	// Assert that extern cache entries written before deprecating the
	// nocacheextern flag are not used in new evaluations.
	for _, bottomup := range []bool{false, true} {
		// The following is done in a func to defer context cancellations.
		func() {
			intern := op.Intern("internurl")
			exec := op.Exec("image", "command", testutil.Resources, intern)
			extern := op.Extern("externurl", exec)

			e, config, done := newTestScheduler()
			defer done()
			config.CacheMode = infra.CacheRead | infra.CacheWrite
			config.BottomUp = bottomup
			eval := flow.NewEval(extern, config)

			testutil.WriteCache(eval, intern.Digest(), "a")
			testutil.WriteCache(eval, exec.Digest())
			testutil.WriteCache(eval, extern.Digest()) // existing cache entry should not be used.

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		}()
	}
}

func TestData(t *testing.T) {
	// Test that data are uploaded appropriately.
	hello := []byte("hello, world!")

	_, config, done := newTestScheduler()
	defer done()
	eval := flow.NewEval(op.Data(hello), config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	r := <-testutil.EvalAsync(ctx, eval)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	_, err := eval.Repository.Stat(ctx, reflow.Digester.FromBytes(hello))
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

	_, config, done := newTestScheduler()
	defer done()
	eval := flow.NewEval(ex, config)

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
	return pool.AllocInspect{ID: a.ID()}, nil
}

func (a *testAlloc) Free(ctx context.Context) error {
	return nil
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
	alloc.Init()

	sched := sched.New()
	sched.Transferer = testutil.Transferer
	sched.TaskDB = testutil.NewNopTaskDB(testutil.NewInmemoryRepository("eval_test"))
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

	config = flow.EvalConfig{
		Scheduler:   sched,
		Snapshotter: snapshotter{},
		Assoc:       testutil.NewInmemoryAssoc(),
		Repository:  testutil.NewInmemoryRepository(""),
		Log:         logger(),
		Trace:       logger(),
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

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

	eval := flow.NewEval(out, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

	wa := newWaitAssoc()
	config.Assoc = wa
	config.Repository = testutil.NewInmemoryRepository("")
	config.CacheMode = infra.CacheRead
	config.BottomUp = true
	config.Log = logger()
	config.Trace = logger()
	eval := flow.NewEval(extern, config)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = testutil.EvalAsync(ctx, eval)

	// tick twice since we now lookup fileset and filesetV2
	wa.Tick()
	wa.Tick()
	e.Ok(ctx, intern, testutil.WriteFiles(e.Repo, "a/b/c", "a/b/d", "x/y/z"))
	for i := 0; i < 4; i++ {
		if e.Pending(exec1) || e.Pending(exec2) {
			t.Fatal("prematurely pending exec")
		}
		// tick twice since we now lookup fileset and filesetV2
		wa.Tick()
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
			{Subject: "a", Namespace: "blob"}: {"etag": fmt.Sprintf("vaold%d", i)},
			{Subject: "b", Namespace: "blob"}: {"etag": fmt.Sprintf("vbold%d", i)},
			{Subject: "c", Namespace: "blob"}: {"etag": fmt.Sprintf("vcold%d", i)},
		})
	}
	want := []*reflow.Assertions{reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{
		{Subject: "a", Namespace: "blob"}: {"etag": "vanew"},
		{Subject: "b", Namespace: "blob"}: {"etag": "vbnew"},
		{Subject: "c", Namespace: "blob"}: {"etag": "vcnew"},
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
			if got, want := tt.g.countsByKey[reflow.AssertionKey{Subject: k.Subject, Namespace: k.Namespace}], tt.want; got != want {
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

func TestCapMemory(t *testing.T) {
	e := flow.EvalConfig{MaxResources: reflow.Resources{"cpu": 16, "mem": 64 << 30}}
	eNoMax := flow.EvalConfig{}
	for _, tt := range []struct {
		specified, want reflow.Resources
		wantCapped      bool
		wantErr         error
	}{
		{reflow.Resources{"cpu": 5, "mem": 10 << 30}, reflow.Resources{"cpu": 5, "mem": 10 << 30}, false, nil},
		{reflow.Resources{"cpu": 5, "mem": 64 << 30}, reflow.Resources{"cpu": 5, "mem": 64 << 30}, false, nil},
		{reflow.Resources{"cpu": 16, "mem": 64 << 30}, reflow.Resources{"cpu": 16, "mem": 64 << 30}, false, nil},
		{reflow.Resources{"cpu": 10, "mem": 66 << 30}, reflow.Resources{"cpu": 10, "mem": 64 << 30}, true, nil},
		{reflow.Resources{"cpu": 24, "mem": 64 << 30}, reflow.Resources{"cpu": 24, "mem": 64 << 30}, false, nil},
		{reflow.Resources{"cpu": 17, "mem": 32 << 30}, reflow.Resources{"cpu": 17, "mem": 32 << 30}, false, nil},
		{reflow.Resources{"cpu": 17, "mem": 65 << 30}, reflow.Resources{"cpu": 17, "mem": 64 << 30}, true, nil},
		{reflow.Resources{"cpu": 10, "mem": 71 << 30}, nil, false, fmt.Errorf("resources {mem:71.0GiB cpu:10 disk:0B} are way higher than max %s", e.MaxResources)},
	} {
		got, gotCapped, gotErr := eNoMax.CapMemory(tt.specified)
		if gotErr != nil {
			t.Errorf("got %v, want nil", gotErr)
			continue
		}
		if !got.Equal(tt.specified) || gotCapped {
			t.Errorf("got %v, want %v (capped %v)", got, tt.specified, gotCapped)
		}

		got, gotCapped, gotErr = e.CapMemory(tt.specified)
		switch {
		case (gotErr == nil) != (tt.wantErr == nil):
			t.Errorf("got %v, want %v", gotErr, tt.wantErr)
		case gotErr != nil && tt.wantErr != nil:
			if tt.wantErr.Error() != gotErr.Error() {
				t.Errorf("got %v, want %v", gotErr, tt.wantErr)
			}
		}
		if gotErr != nil {
			continue
		}
		if gotCapped != tt.wantCapped {
			t.Errorf("got %v, want %v", gotCapped, tt.wantCapped)
		}
		if !got.Equal(tt.want) {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func flowFiles(files ...string) *flow.Flow {
	v := testutil.Files(files...)
	return &flow.Flow{Op: flow.Val, Value: values.T(v), State: flow.Done}
}

// onceMapper maps flows to other flows only once and returns the mapped result subsequently.
// This is particularly necessary for 'Map' flows which need to map a flow to another flow.
// Since we set a random value to 'ExecConfig.Ident', the mapped flows should be cached.
type onceMapper struct {
	once      once.Map
	mapped    sync.Map
	doMapFunc func(f *flow.Flow) *flow.Flow
}

func newMapper(mapFunc func(f *flow.Flow) *flow.Flow) *onceMapper {
	return &onceMapper{doMapFunc: mapFunc}
}

func (m *onceMapper) mapFunc(f *flow.Flow) *flow.Flow {
	_ = m.once.Do(f.Digest(), func() error {
		m.mapped.Store(f.Digest(), m.doMapFunc(f))
		return nil
	})
	v, _ := m.mapped.Load(f.Digest())
	return v.(*flow.Flow)
}
