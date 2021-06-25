// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package predictor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
)

const (
	defaultMinData       = 20
	defaultMaxInspect    = 50
	defaultMemPercentile = 95
	numTasks             = 20
)

type mockdb struct {
	taskdb.TaskDB
	tasks map[string][]taskdb.Task
	mu    sync.Mutex
}

func (m *mockdb) Add(key string, task taskdb.Task) {
	m.mu.Lock()
	m.tasks[key] = append(m.tasks[key], task)
	m.mu.Unlock()
}

func (m *mockdb) Tasks(_ context.Context, taskQuery taskdb.TaskQuery) ([]taskdb.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if taskQuery.ImgCmdID.IsValid() {
		if tasks, ok := m.tasks[taskQuery.ImgCmdID.ID()]; ok {
			return tasks, nil
		}
	}
	if taskQuery.Ident != "" {
		if tasks, ok := m.tasks[taskQuery.Ident]; ok {
			return tasks, nil
		}
	}
	return []taskdb.Task{}, fmt.Errorf("no tasks found")
}

func newMockdb() *mockdb {
	return &mockdb{
		tasks: make(map[string][]taskdb.Task),
	}
}

type mockrepo struct {
	reflow.Repository
	files map[digest.Digest][]byte
	mu    sync.Mutex
}

func newMockRepo() *mockrepo {
	return &mockrepo{
		files: make(map[digest.Digest][]byte),
	}
}

func (m *mockrepo) Get(_ context.Context, d digest.Digest) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.files[d]
	if !ok {
		return nil, fmt.Errorf("file %s not found", d)
	}
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

func (m *mockrepo) Put(_ context.Context, r io.Reader) (digest.Digest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return digest.Digest{}, err
	}
	d := reflow.Digester.FromBytes(b)
	m.files[d] = b
	return d, nil
}

// generateTasks returns a taskdb task, a sched task, and their corresponding ExecInspect.
func generateTasks(image, cmd, ident string, usedResource float64) (taskdb.Task, *sched.Task, reflow.ExecInspect) {
	config := reflow.ExecConfig{
		Ident: ident,
		Image: image,
		Cmd:   cmd,
		Resources: reflow.Resources{
			"cpu": usedResource,
			"mem": 20,
		},
		Type: "exec",
	}
	execInspect := reflow.ExecInspect{
		Created: time.Now(),
		Config:  config,
		Profile: reflow.Profile{
			"mem": {
				Max: usedResource,
			},
		},
	}
	id := taskdb.NewTaskID()
	b, _ := json.Marshal(execInspect)
	taskTaskdb := taskdb.Task{
		ID:       id,
		ImgCmdID: taskdb.NewImgCmdID(config.Image, config.Cmd),
		Ident:    config.Ident,
		Inspect:  reflow.Digester.FromBytes(b),
	}
	taskSched := sched.Task{
		ID:     id,
		Config: config,
	}
	return taskTaskdb, &taskSched, execInspect
}

// generateData generates data for testing resource prediction. It generates 20 scheduler tasks.
// The tasks are populated with mock profiles. The Max "mem" of the profiles is the sequence
// 1, 2, 3, ..., 20 bytes of memory. This sequence can be used to test the resource models. All tasks have
// the same taskGroup for the given level. Valid levels are 0 for imgCmdGroups and 1 for identGroups. Any other
// level will result in a panic. All taskGroups at a different valid level other than the specified level will be
// unique for each task.
func generateData(t *testing.T, ctx context.Context, repo reflow.Repository, tdb *mockdb, level int) ([]*sched.Task, taskGroup) {
	tasks := make([]*sched.Task, numTasks)
	for i := 0; i < numTasks; i++ {
		var (
			tdbTask   taskdb.Task
			schedTask *sched.Task
			inspect   reflow.ExecInspect
		)
		// In order to ensure that the selected taskGroup type is being used for querying and model building,
		// make all taskGroups of different types unique.
		switch level {
		case 0:
			tdbTask, schedTask, inspect = generateTasks("img", "cmd", "ident"+strconv.Itoa(i), float64(i+1))
		case 1:
			tdbTask, schedTask, inspect = generateTasks("img", "cmd"+strconv.Itoa(i), "ident", float64(i+1))
		default:
			panic("invalid level")
		}
		tdb.Add(tdbTask.ImgCmdID.ID(), tdbTask)
		tdb.Add(tdbTask.Ident, tdbTask)
		tasks[i] = schedTask
		b, err := json.Marshal(inspect)
		if err != nil {
			t.Fatal(err)
		}
		d, err := repo.Put(ctx, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		if tdbTask.Inspect != d {
			t.Fatalf("task and repository inspect digests do not match.")
		}
	}
	return tasks, getTaskGroups(tasks[0])[level]
}

func TestNewPred(t *testing.T) {
	var (
		logger = log.New(nil, 1)
	)

	// If either repo or tdb are nil, NewPred() must panic because a
	// Predictor requires both a Repository and a TaskDB to function.
	r := func(repo reflow.Repository, tdb taskdb.TaskDB, minData, maxInspect int, memPercentile float64) {
		if r := recover(); r == nil {
			t.Errorf("expected panic for repo: %v, taskdb: %v, minData %d, maxInspect %d, memPercentile %v", repo, tdb, minData, maxInspect, memPercentile)
		}
	}
	for _, tt := range []struct {
		repo                reflow.Repository
		tdb                 taskdb.TaskDB
		log                 *log.Logger
		minData, maxInspect int
		memPercentile       float64
	}{
		{newMockRepo(), nil, logger, 1, 2, 3},
		{nil, newMockdb(), logger, 1, 2, 3},
		{nil, nil, logger, 1, 2, 3},
		{newMockRepo(), newMockdb(), logger, 0, 2, 3},
		{newMockRepo(), newMockdb(), logger, 2, 1, 3},
		{newMockRepo(), newMockdb(), logger, 1, 2, -1},
	} {
		defer r(tt.repo, tt.tdb, tt.minData, tt.maxInspect, tt.memPercentile)
		_ = New(tt.repo, tt.tdb, tt.log, tt.minData, tt.maxInspect, tt.memPercentile)
	}
}

func TestPredictImgCmdID(t *testing.T) {
	var (
		repo = newMockRepo()
		tdb  = newMockdb()
		ctx  = context.Background()
	)
	tasks, _ := generateData(t, ctx, repo, tdb, 0)

	pred := New(repo, tdb, nil, defaultMinData, defaultMaxInspect, defaultMemPercentile)

	for i := 0; i < numTasks; i++ {
		if got, want := tasks[i].Config.Resources["mem"], float64(20); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	predictions := pred.Predict(ctx, tasks...)
	if len(predictions) != numTasks {
		t.Fatalf("predict did not return 1 Resource per predicted task")
	}
	// 95th percentile of 1, 2, 3, ..., 20 is 19.
	for _, p := range predictions {
		if got, want := p.Resources["mem"], float64(19); got != want {
			t.Errorf("mem: got %v, want %v", got, want)
		}
	}
}

func TestPredictIdent(t *testing.T) {
	var (
		repo = newMockRepo()
		tdb  = newMockdb()
		ctx  = context.Background()
	)
	tasks, _ := generateData(t, ctx, repo, tdb, 1)

	pred := New(repo, tdb, nil, defaultMinData, defaultMaxInspect, defaultMemPercentile)

	for i := 0; i < numTasks; i++ {
		if got, want := tasks[i].Config.Resources["mem"], float64(20); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	predictions := pred.Predict(ctx, tasks...)
	if len(predictions) != numTasks {
		t.Fatalf("predict did not return 1 Resource per predicted task")
	}
	// 95th percentile of 1, 2, 3, ..., 20 is 19.
	for _, p := range predictions {
		if got, want := p.Resources["mem"], float64(19); got != want {
			t.Errorf("mem: got %v, want %v", got, want)
		}
	}
}

func TestPredictMultiGroup(t *testing.T) {
	var (
		repo = newMockRepo()
		tdb  = newMockdb()
		ctx  = context.Background()
	)
	tasks, _ := generateData(t, ctx, repo, tdb, 1)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Config.Resources["mem"] < tasks[j].Config.Resources["mem"]
	})
	// Append a task whose resources cannot be predicted with the cached profiling data.
	// The total number of tasks will now be numTasks + 1.
	tasks = append(tasks, &sched.Task{
		ID: taskdb.NewTaskID(),
		Config: reflow.ExecConfig{
			Ident: "badident",
			Image: "badimage",
			Cmd:   "badcmd",
			Type:  "exec",
		},
	})

	// Since minData is set to 1 and each task has a unique imgCmdID, there will be 20
	// imgCmdIDs and 20 unique predictions (1, 2, 3..., 20).
	pred := New(repo, tdb, nil, 1, defaultMaxInspect, defaultMemPercentile)

	for i := 0; i < numTasks; i++ {
		if got, want := tasks[i].Config.Resources["mem"], float64(20); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	predictions := pred.Predict(ctx, tasks...)
	if len(predictions) == numTasks+1 {
		t.Fatalf("predict returned resources for a task whose resources were not predicted")
	}
	if len(predictions) != numTasks {
		t.Fatalf("predict did not return 1 Resource per predicted task")
	}
	var (
		resourceSlice = make([]reflow.Resources, len(predictions))
		i             int
	)
	for _, p := range predictions {
		resourceSlice[i] = p.Resources
		i++
	}
	sort.Slice(resourceSlice, func(i, j int) bool {
		return resourceSlice[i]["mem"] < resourceSlice[j]["mem"]
	})
	for i := 0; i < numTasks; i++ {
		if got, want := resourceSlice[i]["mem"], float64(i+1); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// Tasks which are not "execs" should not have their resources predicted.
	tasks = tasks[0:1]
	tasks[0].Config.Type = "intern"
	tasks[0].Config.Type = "extern"
	if got, want := len(pred.Predict(ctx, tasks...)), 0; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func generateProfiles(memVals []float64) []reflow.Profile {
	profiles := make([]reflow.Profile, numTasks)
	for i := 0; i < len(memVals); i++ {
		profiles[i] = reflow.Profile{"mem": {Max: memVals[i]}}
	}
	return profiles
}

func TestMemUsage(t *testing.T) {
	for _, tc := range []struct {
		minData, memPct int
		profiles        []reflow.Profile
		want            float64
		wantErr         bool
	}{
		{defaultMinData, defaultMemPercentile, generateProfiles([]float64{}), 0.0, true},
		// 95th percentile of 1, 2, 3, ..., 20 is 19.
		{defaultMinData, defaultMemPercentile, generateProfiles([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), 19.0, false},
		{5, 50, generateProfiles([]float64{1, 2, 3, 4}), 0.0, true},
		{5, 50, generateProfiles([]float64{1, 2, 3, 4, 5}), 3.0, false},
	} {
		pred := New(newMockRepo(), newMockdb(), nil, tc.minData, defaultMaxInspect, float64(tc.memPct))
		mem, err := pred.memUsage(tc.profiles)
		if (err != nil) != tc.wantErr {
			t.Errorf("got %v, want error %v", err, tc.wantErr)
		}
		if tc.wantErr {
			continue
		}
		if got, want := mem, tc.want; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestGetProfilesInspectErrors(t *testing.T) {
	var (
		repo = newMockRepo()
		tdb  = newMockdb()
		ctx  = context.Background()
	)

	pred := New(repo, tdb, nil, defaultMinData, defaultMaxInspect, defaultMemPercentile)

	tasks, group := generateData(t, ctx, repo, tdb, 0)
	for i := 0; i < numTasks; i++ {
		if got, want := tasks[i].Config.Resources["mem"], float64(20); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	// Give each ExecInspect an Error or an ExecError.
	var i int
	for d, b := range repo.files {
		var ei reflow.ExecInspect
		if err := json.Unmarshal(b, &ei); err != nil {
			t.Fatal(err)
		}

		switch i % 3 {
		case 0:
			ei.Error = errors.Recover(errors.E(errors.NotExist, "filler error"))
		case 1:
			ei.ExecError = errors.Recover(errors.E(errors.NotExist, "filler error"))
		default:
		}
		b, err := json.Marshal(ei)
		if err != nil {
			t.Fatal(err)
		}
		repo.files[d] = b
		i++
	}
	profiles, _ := pred.getProfiles(ctx, group)
	if got, want := len(profiles), numTasks/3; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGroupByLevel(t *testing.T) {
	var (
		repo     = newMockRepo()
		tdb      = newMockdb()
		ctx      = context.Background()
		tasks, _ = generateData(t, ctx, repo, tdb, 0)
	)

	// level 0 (imgCmdGroup): 1 taskGroup with 20 tasks.
	groupMap := groupByLevel(tasks, 0)
	groupSlice := make([]taskGroup, 0, len(groupMap))
	for group := range groupMap {
		groupSlice = append(groupSlice, group)
	}
	if got, want := len(groupSlice), 1; got != want {
		t.Errorf("taskgroupset slice: got %d unique taskGroups, want %d", got, want)
	}
	if got, want := len(groupMap), 1; got != want {
		t.Errorf("taskgroupset map: got %d unique taskGroups, want %d", got, want)
	}
	if got, want := len(groupMap[groupSlice[0]]), numTasks; got != want {
		t.Errorf("taskgroupset map: got %d tasks, want %d", got, want)
	}

	// level 1 (identGroup): 20 taskGroups with 1 task each.
	groupMap = groupByLevel(tasks, 1)
	groupSlice = make([]taskGroup, 0, len(groupMap))
	for group := range groupMap {
		groupSlice = append(groupSlice, group)
	}
	if got, want := len(groupSlice), numTasks; got != want {
		t.Errorf("taskgroupset slice: got %d unique taskGroups, want %d", got, want)
	}
	if got, want := len(groupMap), numTasks; got != want {
		t.Errorf("taskgroupset map: got %d unique taskGroups, want %d", got, want)
	}
	for k, v := range groupMap {
		if got, want := len(v), 1; got != want {
			t.Errorf("taskgroupset map taskgroup %s: got %d tasks, want %d", k.Name(), got, want)
		}
	}
}

const nanosInSecs = float64(time.Second)

func TestValuePercentile(t *testing.T) {
	const numData, numNegData = 100, 10
	// testdata is a the sequence of Max "mem" profile values 1, 2, 3, ..., 20
	testData := make([]reflow.Profile, numData+numNegData)
	for i := 0; i < numData; i++ {
		testData[i] = reflow.Profile{"mem": {Max: float64(i + 1), First: time.Unix(0, 0), Last: time.Unix(int64(i+1)*100, 0)}}
	}
	for i := numData; i < numData+numNegData; i++ {
		testData[i] = reflow.Profile{"mem": {Max: -float64(i + 1)}}
	}

	// 0th percentile of testdata is 1.
	p, n := valuePercentile(testData, 0, memMaxGetter)
	if got, want := p, float64(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := n, numData; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// ith percentile is i for i in 1, 2, 3, ..., 100.
	for i := 1; i <= 100; i++ {
		p, n = valuePercentile(testData, float64(i), memMaxGetter)
		if got, want := p, float64(i); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, numData; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		p, n = valuePercentile(testData, float64(i), maxDurationGetter)
		if got, want := p, float64(i)*100*nanosInSecs; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, numData; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

	}

	// i-0.1th percentile is i for i in 1, 2, 3, ..., 100.
	for i := 1; i <= 99; i++ {
		p, n = valuePercentile(testData, float64(i)-0.1, memMaxGetter)
		if got, want := p, float64(i); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, numData; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		p, n = valuePercentile(testData, float64(i), maxDurationGetter)
		if got, want := p, float64(i)*100*nanosInSecs; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, numData; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// i+0.1th percentile is i+1 for i in 1, 2, 3, ..., 99.
	for i := 1; i <= 99; i++ {
		p, n = valuePercentile(testData, float64(i)+0.1, memMaxGetter)
		if got, want := p, float64(i+1); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, numData; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// Remove "mem" from a couple of profiles.
	delete(testData[0], "mem")
	delete(testData[1], "mem")
	p, n = valuePercentile(testData, 0, memMaxGetter)
	if got, want := p, float64(3); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := n, numData-2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	p, n = valuePercentile(testData, 0, maxDurationGetter)
	if got, want := p, 300.0*nanosInSecs; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := n, numData-2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Test panics for percentiles < 0 and > 100
	r := func(p float64) {
		if r := recover(); r == nil {
			t.Errorf("expected panic for percentile %v", p)
		}
	}
	for _, p := range []float64{-1, 101} {
		defer r(p)
		_, _ = valuePercentile(testData, p, memMaxGetter)
	}
}
