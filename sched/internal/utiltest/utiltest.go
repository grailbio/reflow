// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package utiltest

import (
	"bytes"
	"context"
	"crypto"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	golog "log"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/test/testutil"
)

var logTasks = flag.Bool("logtasks", false, "log task output to stderr")

type counter uint64

func (c *counter) Next() uint64 {
	return atomic.AddUint64((*uint64)(c), 1)
}

func (c *counter) NextID() digest.Digest {
	n := crypto.Hash(reflow.Digester).Size()
	b := make([]byte, n)
	binary.LittleEndian.PutUint64(b, c.Next())
	return reflow.Digester.New(b)
}

var nalloc counter

func NewTask(cpu, mem float64, priority int) *sched.Task {
	task := sched.NewTask()
	task.Priority = priority
	task.Config.Resources = reflow.Resources{"cpu": cpu, "mem": mem}
	task.ID = taskdb.NewTaskID()
	task.FlowID = reflow.Digester.FromString("test-flow-id")
	task.PostUseChecksum = true
	if *logTasks {
		SetLogger(task)
	}
	return task
}

func SetLogger(task *sched.Task) {
	out := golog.New(os.Stderr, fmt.Sprintf("task %s (%s): ", task.ID.IDShort(), task.Config.Resources), golog.LstdFlags)
	task.Log = log.New(out, log.DebugLevel)
}

func NewRequirements(cpu, mem float64, width int) reflow.Requirements {
	return reflow.Requirements{
		Min:   reflow.Resources{"cpu": cpu, "mem": mem},
		Width: width,
	}
}

func RandomFileset(repo reflow.Repository) reflow.Fileset {
	fuzz := testutil.NewFuzz(nil)
	n := rand.Intn(100) + 1
	var fs reflow.Fileset
	fs.Map = make(map[string]reflow.File, n)
	for i := 0; i < n; i++ {
		p := make([]byte, rand.Intn(1024)+1)
		if _, err := rand.Read(p); err != nil {
			panic(err)
		}
		d, err := repo.Put(context.TODO(), bytes.NewReader(p))
		if err != nil {
			panic(err)
		}
		path := fmt.Sprintf("file%d", i)
		f := fuzz.File(false, true)
		f.ID = d
		f.Size = int64(len(p))
		fs.Map[path] = f
	}
	return fs
}

func RandomRepoFileset(repo reflow.Repository) reflow.Fileset {
	n := rand.Intn(100) + 1
	var fs reflow.Fileset
	fs.Map = make(map[string]reflow.File, n)
	for i := 0; i < n; i++ {
		p := make([]byte, rand.Intn(1024)+1)
		if _, err := rand.Read(p); err != nil {
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

type TestClusterAllocReply struct {
	Alloc pool.Alloc
	Err   error
}

type testClusterAllocReq struct {
	reflow.Requirements
	Labels pool.Labels
	Reply  chan<- TestClusterAllocReply
}

type TestCluster struct {
	max  reflow.Resources
	reqs chan testClusterAllocReq
}

func NewTestCluster() *TestCluster {
	return &TestCluster{
		max:  reflow.Resources{"cpu": 64, "mem": 256 << 30},
		reqs: make(chan testClusterAllocReq),
	}
}

func (c *TestCluster) Req() <-chan testClusterAllocReq {
	return c.reqs
}

func (c *TestCluster) CanAllocate(r reflow.Resources) (bool, error) {
	if c.max.Available(r) {
		return true, nil
	}
	return false, fmt.Errorf("resources %s too big (max: %s)", r, c.max)
}

func (c *TestCluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (pool.Alloc, error) {
	replyc := make(chan TestClusterAllocReply)
	select {
	case c.reqs <- testClusterAllocReq{
		Requirements: req,
		Labels:       labels,
		Reply:        replyc,
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case reply := <-replyc:
		return reply.Alloc, reply.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type testExec struct {
	reflow.Exec
	Config reflow.ExecConfig

	id digest.Digest

	mu   sync.Mutex
	cond *ctxsync.Cond

	done   bool
	result reflow.Result
	err    error
}

func newTestExec(id digest.Digest, config reflow.ExecConfig) *testExec {
	exec := &testExec{id: id, Config: config}
	exec.cond = ctxsync.NewCond(&exec.mu)
	return exec
}

func (e *testExec) ID() digest.Digest {
	return e.id
}

func (e *testExec) URI() string {
	return fmt.Sprintf("uri_%s", e.id)
}

func (e *testExec) Result(ctx context.Context) (reflow.Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.done {
		panic("Result called but not done")
	}
	return e.result, e.err
}

func (e *testExec) Inspect(ctx context.Context, repo *url.URL) (resp reflow.InspectResponse, err error) {
	resp.Inspect = &reflow.ExecInspect{}
	resp.RunInfo = &reflow.ExecRunInfo{}
	return resp, err
}

func (e *testExec) Promote(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (e *testExec) Wait(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for !e.done {
		if err := e.cond.Wait(ctx); err != nil {
			return err
		}
	}
	return e.err
}

func (e *testExec) Complete(res reflow.Result, err error) {
	e.mu.Lock()
	e.done = true
	e.result = res
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
}

type TestPool struct {
	pool.ResourcePool
	name string
}

func NewTestPool(name string, r reflow.Resources) *TestPool {
	p := &TestPool{name: name}
	p.ResourcePool = pool.NewResourcePool(p, log.Std)
	p.Init(r, nil)
	return p
}

func (p *TestPool) Name() string {
	return p.name
}

func (p *TestPool) New(ctx context.Context, id string, meta pool.AllocMeta, keepalive time.Duration, existing []pool.Alloc) (pool.Alloc, error) {
	alloc := NewTestAllocWithId(id, meta.Want)
	if _, err := alloc.Keepalive(ctx, keepalive); err != nil {
		return nil, err
	}
	return alloc, nil
}

func (p *TestPool) Kill(a pool.Alloc) error {
	log.Printf("kill alloc %s, %s", a.ID(), a.Resources())
	return nil
}

type TestAlloc struct {
	pool.Alloc
	id         string
	tdbAllocId reflow.StringDigest
	repository *testutil.InmemoryRepository
	resources  reflow.Resources

	created       time.Time
	lastkeepalive time.Time
	expires       time.Time

	mu         sync.Mutex
	cond       *sync.Cond
	execs      map[digest.Digest]*testExec
	err        error
	hung       bool
	refCountMu sync.Mutex
	refCount   map[digest.Digest]int64
}

func NewTestAlloc(resources reflow.Resources) *TestAlloc {
	return NewTestAllocWithId(fmt.Sprintf("test-%d", nalloc.Next()), resources)
}

func NewTestAllocWithId(id string, resources reflow.Resources) *TestAlloc {
	alloc := &TestAlloc{
		repository: testutil.NewInmemoryRepository(""),
		execs:      make(map[digest.Digest]*testExec),
		resources:  resources,
		created:    time.Now(),
		id:         id,
		tdbAllocId: reflow.NewStringDigest(fmt.Sprintf("tdb_alloc_id_%s", id)),
		refCount:   make(map[digest.Digest]int64),
	}
	alloc.cond = sync.NewCond(&alloc.mu)
	return alloc
}

func (a *TestAlloc) ID() string {
	return a.id
}

func (a *TestAlloc) Resources() reflow.Resources {
	return a.resources
}

func (a *TestAlloc) Repository() reflow.Repository {
	return a.repository
}

func (a *TestAlloc) Load(ctx context.Context, url *url.URL, fs reflow.Fileset) (reflow.Fileset, error) {
	a.refCountMu.Lock()
	defer a.refCountMu.Unlock()
	var (
		resolved = make(map[digest.Digest]reflow.File)
		id       digest.Digest
		res      reflow.File
	)
	for _, file := range fs.Files() {
		if file.IsRef() {
			u, err := url.Parse(file.Source)
			if err != nil {
				return reflow.Fileset{}, err
			}
			repo := testutil.GetInMemoryRepository(u)
			id, err = reflow.Digester.Parse(strings.Trim(u.Path, "/"))
			if err != nil {
				return reflow.Fileset{}, err
			}
			var rc io.ReadCloser
			if rc, err = repo.Get(ctx, id); err != nil {
				return reflow.Fileset{}, err
			}
			if _, err = a.repository.Put(ctx, rc); err != nil {
				return reflow.Fileset{}, err
			}
			if err = rc.Close(); err != nil {
				return reflow.Fileset{}, err
			}
			res, err = a.repository.Stat(ctx, id)
			if err != nil {
				return reflow.Fileset{}, err
			}
		} else {
			id = file.ID
			repo := testutil.GetInMemoryRepository(url)
			r, err := repo.Get(ctx, file.ID)
			if err != nil {
				return reflow.Fileset{}, err
			}
			id, err = a.repository.Put(ctx, r)
			if err != nil {
				return reflow.Fileset{}, err
			}
			res, err = a.repository.Stat(ctx, id)
			if err != nil {
				return reflow.Fileset{}, err
			}
		}
		a.refCount[res.ID]++
		resolved[file.Digest()] = res
	}
	out, ok := fs.Subst(resolved)
	if !ok {
		return reflow.Fileset{}, errors.New("unresolved files")
	}
	return out, nil
}

// VerifyIntegrity verifies the integrity of the given set of files
func (a *TestAlloc) VerifyIntegrity(ctx context.Context, fs reflow.Fileset) error {
	a.refCountMu.Lock()
	defer a.refCountMu.Unlock()
	for _, file := range fs.Files() {
		if file.IsRef() {
			return errors.New("unexpected file reference")
		}
		rc, err := a.repository.Get(ctx, file.ID)
		if err != nil {
			return err
		}
		w := reflow.Digester.NewWriter()
		if _, err = io.Copy(w, rc); err == nil {
			d := w.Digest()
			if file.ID != d {
				err = fmt.Errorf("digest %s mismatches ID %s", d.Short(), file.ID.Short())
			}
		}
		_ = rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *TestAlloc) Unload(ctx context.Context, fs reflow.Fileset) error {
	a.refCountMu.Lock()
	defer a.refCountMu.Unlock()
	for _, file := range fs.Files() {
		if file.IsRef() {
			return errors.New("unexpected file reference")
		}
		a.refCount[file.ID]--
		if a.refCount[file.ID] == 0 {
			a.repository.Delete(ctx, file.ID)
		}
		if a.refCount[file.ID] < 0 {
			golog.Panicf("unload: %v has negative ref count", file.ID)
		}
	}
	return nil
}

func (a *TestAlloc) Put(ctx context.Context, id digest.Digest, config reflow.ExecConfig) (reflow.Exec, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.execs[id]; !ok {
		a.execs[id] = newTestExec(id, config)
		a.cond.Broadcast()
	}
	return a.execs[id], nil
}

func (a *TestAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	a.mu.Lock()
	hung, err := a.hung, a.err
	if hung {
		a.mu.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}
	defer a.mu.Unlock()
	if err == nil {
		err = ctx.Err()
	}
	interval = 50 * time.Millisecond
	a.lastkeepalive = time.Now()
	a.expires = a.lastkeepalive.Add(interval)
	return interval, err
}

func (a *TestAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return pool.AllocInspect{
		ID:            a.id,
		TaskDBAllocID: a.tdbAllocId.Digest(),
		Resources:     a.resources,
		Created:       a.created,
		LastKeepalive: a.lastkeepalive,
		Expires:       a.expires,
	}, nil
}

func (a *TestAlloc) Free(ctx context.Context) error {
	_, err := a.Keepalive(ctx, 0)
	return err
}

func (a *TestAlloc) RefCount() map[digest.Digest]int64 {
	a.refCountMu.Lock()
	defer a.refCountMu.Unlock()
	rc := make(map[digest.Digest]int64, len(a.refCount))
	for k, v := range a.refCount {
		rc[k] = v
	}
	return rc
}

func (a *TestAlloc) RefCountInc(id digest.Digest) {
	a.refCountMu.Lock()
	defer a.refCountMu.Unlock()
	a.refCount[id]++
}

func (a *TestAlloc) Exec(id digest.Digest) *testExec {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.execs[id] == nil {
		a.cond.Wait()
	}
	return a.execs[id]
}

func (a *TestAlloc) NExecs() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.execs)
}

func (a *TestAlloc) Error(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.err = err
}

// CompleteAll completes all execs put in this alloc after a minimum duration has elapsed.
// The duration for each exec is expected to be set in `exec.Config.Ident`.
func (a *TestAlloc) CompleteAll(ctx context.Context) {
	var (
		done           bool
		tick           = time.NewTicker(20 * time.Millisecond)
		execStartTimes = make(map[digest.Digest]time.Time)
	)
	defer tick.Stop()
	for !done {
		a.mu.Lock()
		for id, exec := range a.execs {
			exec.mu.Lock()
			skip := exec.done
			exec.mu.Unlock()
			if skip {
				continue
			}
			delay, err := time.ParseDuration(exec.Config.Ident)
			if err != nil {
				panic(fmt.Sprintf("not a duration: %s", exec.Config.Ident))
			}
			if start, ok := execStartTimes[id]; ok {
				if time.Since(start) > delay {
					exec.Complete(reflow.Result{}, nil)
				}
			} else {
				execStartTimes[id] = time.Now()
			}
		}
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			done = true
		case <-tick.C:
		}
	}
}

func (a *TestAlloc) hang() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hung = true
}

type MockTaskDB struct {
	taskdb.TaskDB

	mu       sync.Mutex
	numCalls map[string]int
	tasks    map[taskdb.TaskID]taskdb.Task
}

func NewMockTaskDB() *MockTaskDB {
	return &MockTaskDB{
		numCalls: make(map[string]int),
		tasks:    make(map[taskdb.TaskID]taskdb.Task),
	}
}

func (t *MockTaskDB) Tasks(ctx context.Context, taskQuery taskdb.TaskQuery) ([]taskdb.Task, error) {
	tasks := make([]taskdb.Task, 0, len(t.tasks))
	for _, tsk := range t.tasks {
		tasks = append(tasks, tsk)
	}
	return tasks, nil
}

// CreateTask creates a new task in the taskdb with the provided task.
func (t *MockTaskDB) CreateTask(ctx context.Context, task taskdb.Task) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[task.ID] = task
	t.numCalls["CreateTask"] = t.numCalls["CreateTask"] + 1
	return nil
}

// SetTaskResult sets the result of the task post completion.
func (t *MockTaskDB) SetTaskResult(ctx context.Context, id taskdb.TaskID, result digest.Digest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if tsk, ok := t.tasks[id]; ok {
		tsk.ResultID = result
		t.tasks[id] = tsk
	}
	t.numCalls["SetTaskResult"] = t.numCalls["SetTaskResult"] + 1
	return nil
}

// SetTaskUri updates the task URI.
func (t *MockTaskDB) SetTaskUri(ctx context.Context, id taskdb.TaskID, uri string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if tsk, ok := t.tasks[id]; ok {
		tsk.URI = uri
		t.tasks[id] = tsk
	}
	t.numCalls["SetTaskUri"] = t.numCalls["SetTaskUri"] + 1
	return nil
}

func (t *MockTaskDB) SetTaskAttrs(ctx context.Context, id taskdb.TaskID, stdout, stderr, inspect digest.Digest) error {
	return nil
}

// SetTaskComplete mark the task as completed as of the given end time with the error (if any)
func (t *MockTaskDB) SetTaskComplete(ctx context.Context, id taskdb.TaskID, err error, end time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if tsk, ok := t.tasks[id]; ok {
		tsk.End = end
		t.tasks[id] = tsk
	}
	t.numCalls["SetTaskComplete"] = t.numCalls["SetTaskComplete"] + 1
	return nil
}

func (t *MockTaskDB) KeepTaskAlive(ctx context.Context, id taskdb.TaskID, keepalive time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if tsk, ok := t.tasks[id]; ok {
		tsk.Keepalive = keepalive
		t.tasks[id] = tsk
	}
	t.numCalls["KeepTaskAlive"] = t.numCalls["KeepTaskAlive"] + 1
	return nil
}
