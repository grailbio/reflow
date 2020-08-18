// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package predictor implements an exec resource prediction
// system for reflow.
//
// The predictor takes a group of scheduler Tasks and attempts
// to predict the resource usage of each Task's exec based on
// previous runs of the exec. The predictor queries taskdb for
// specific taskGroups--groupings of Tasks which have the same
// underlying exec. Next, the predictor fetches profiling data
// from each exec's cached ExecInspect. If the predictor fails
// to build a model for a specific taskGroup, the predictor
// will generate a new taskGroup for the Task submitted to the
// predictor and will retry. If no more taskGroups can be tried,
// the predictor will not return any predicted resources
// for the Task.

package predictor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/sched"
	"github.com/grailbio/reflow/taskdb"
)

// Predictor predicts tasks' resource usage. All predictions
// are performed online using cached profiling data.
type Predictor struct {
	// taskDB is the task reporting db.
	taskDB taskdb.TaskDB
	// repository is the repository from which ExecInspects are
	// downloaded.
	repository reflow.Repository
	// log is used to log.
	log *log.Logger
	// minData is the minimum number of data points (Profiles)
	// required to predict the resource usage of a taskGroup.
	minData int
	// maxInspect is the maximum number of ExecInspects which
	// will be unmarshalled to get obtain profiling data for a
	// particular taskGroup.
	maxInspect int
	// memPercentile is the percentile that will
	// be used to predict memory usage for all tasks.
	memPercentile float64
	// inspectLimiter limits the number of concurrent
	// ExecInspect Profile unmarshal operations.
	inspectLimiter *limiter.Limiter
}

// Prediction consists of various predicted attributes.
type Prediction struct {
	// Resources is the predicted amount of resources a task is expected to require.
	Resources reflow.Resources
	// Duration is the predicted duration of a task.  Currently this is populated with
	// the duration of the task's `exec` and does not include load/unload times, etc.
	// TODO(swami): Predict and use total task duration instead.
	Duration time.Duration
}

// New returns a new Predictor instance. New will panic if either repo or tdb is nil because
// a Predictor requires both a taskdb and a repository to function. NewPred will also panic if
// minData <= 0 because a prediction requires at least one data point.
func New(repo reflow.Repository, tdb taskdb.TaskDB, log *log.Logger, minData, maxInspect int, memPercentile float64) *Predictor {
	if tdb == nil || repo == nil {
		panic("predictor requires both a repository and a taskdb to function")
	}
	if minData <= 0 {
		panic("minData must be greater than zero")
	}
	if maxInspect < minData {
		panic("maxInspect must be greater than or equal to minData")
	}

	// ExecInspect unmarshaling is CPU-intensive
	// because ExecInspects JSONs can be very large
	// (on the order of 10s of MiB). Because of this,
	// inspectLimiter is set to the number of available CPUs.
	inspectLimiter := limiter.New()
	inspectLimiter.Release(runtime.NumCPU())

	return &Predictor{
		taskDB:         tdb,
		repository:     repo,
		log:            log,
		minData:        minData,
		maxInspect:     maxInspect,
		memPercentile:  memPercentile,
		inspectLimiter: inspectLimiter,
	}
}

// Predict returns the predicted Resources of submitted tasks. If Predict fails
// to predict the Resources of a particular task, it will return no resources
// for the task.
func (p *Predictor) Predict(ctx context.Context, tasks ...*sched.Task) map[*sched.Task]Prediction {
	// Only predict the resources of tasks of type "exec" because
	// "exec" tasks are the only tasks with configured resources and
	// profiling data.
	predictableTasks := make([]*sched.Task, 0, len(tasks))
	for _, task := range tasks {
		if task.Config.Type == "exec" {
			predictableTasks = append(predictableTasks, task)
		}
	}

	var (
		// Since all tasks have the same list of taskGroups,
		// all taskGroups will have the same taskLevel.
		maxLevel = len(getTaskGroups(tasks[0]))

		mu      sync.Mutex
		predMap = make(map[*sched.Task]Prediction)
		// todo keeps track of which tasks do not yet have their resources predicted.
		// Since all tasks have the same list of taskGroups, all the tasks in todo
		// can be grouped by their taskGroup's level.
		todo = sched.NewTaskSet(predictableTasks...)
	)

	for level := 0; level < maxLevel && todo.Len() > 0; level++ {
		// Find all unique taskGroups and map all tasks to their respective
		// taskGroup.
		groupMap := groupByLevel(todo.Slice(), level)
		groups := make([]taskGroup, 0, todo.Len())
		for group := range groupMap {
			groups = append(groups, group)
		}
		// Model each taskGroup's resource usage concurrently. All predicted Resources
		// are mapped to their respective task.
		_ = traverse.Each(len(groups), func(i int) error {
			profiles, err := p.getProfiles(ctx, groups[i])
			if err != nil {
				p.log.Debugf("getting profiles for group %s: %v", groups[i], err)
				return nil
			}
			durNanos, err := p.durationNanos(profiles)
			if err != nil {
				p.log.Debugf("predicting duration (from %d profiles): %v", len(profiles), err)
			}
			mem, err := p.memUsage(profiles)
			if err != nil {
				p.log.Debugf("predicting memory (from %d profiles): %v", len(profiles), err)
				return nil
			}
			p.log.Debugf("successfully modeled memory usage: %s", groups[i].Name())
			doneTasks := groupMap[groups[i]]
			for j := 0; j < len(doneTasks); j++ {
				predictedResources := make(reflow.Resources)
				predictedResources["mem"] = mem

				mu.Lock()
				todo.RemoveAll(doneTasks[j])
				predMap[doneTasks[j]] = Prediction{
					Resources: predictedResources,
					// TODO(swami): Predict and use total task duration instead of just the time
					// taken to run the exec  (ie, we should include object load/unload times, etc).
					Duration: time.Duration(int64(durNanos)),
				}
				mu.Unlock()
			}
			return nil
		})
		p.log.Debugf("successfully modeled memory usage for %d/%d tasks", len(tasks)-todo.Len(), len(tasks))
	}

	return predMap
}

// getProfiles returns a list of profiles the predicted memory usage of a task in group.
func (p *Predictor) getProfiles(ctx context.Context, group taskGroup) ([]reflow.Profile, error) {
	// Query taskdb for all tasks in the taskGroup.
	tasks, err := p.taskDB.Tasks(ctx, group.Query())
	if err != nil {
		return nil, errors.E(group.Name(), "taskdb query", err)
	}
	if len(tasks) < p.minData {
		return nil, errors.E(group.Name(), fmt.Errorf("insufficient tasks (%d < %d)", len(tasks), p.minData))
	}

	var inspectDigests = make([]digest.Digest, len(tasks))
	for i, task := range tasks {
		inspectDigests[i] = task.Inspect
	}

	// In the event that there are over maxInspect inspects,
	// randomly select maxInspect inspects to download and
	// unmarshal.
	if len(inspectDigests) > p.maxInspect {
		src := rand.NewSource(time.Now().UnixNano())
		rand.New(src).Shuffle(len(inspectDigests), func(i, j int) {
			inspectDigests[i], inspectDigests[j] = inspectDigests[j], inspectDigests[i]
		})
		inspectDigests = inspectDigests[:p.maxInspect]
	}

	// Get all profiles for all tasks in the taskGroup.
	var (
		mu       sync.Mutex
		profiles = make([]reflow.Profile, 0, len(inspectDigests))
	)
	_ = traverse.Each(len(inspectDigests), func(i int) error {
		if inspectDigests[i].IsZero() {
			return nil
		}
		rc, err := p.repository.Get(ctx, inspectDigests[i])
		if err != nil {
			return nil
		}
		defer rc.Close()

		if err := p.inspectLimiter.Acquire(ctx, 1); err != nil {
			return nil
		}
		var si smallInspect
		if err := json.NewDecoder(rc).Decode(&si); err != nil {
			return nil
		}
		p.inspectLimiter.Release(1)

		// Only use profiles with memory data
		// to make memory predictions.
		mu.Lock()
		if _, ok := si.Profile["mem"]; ok && si.Error == nil && si.ExecError == nil {
			profiles = append(profiles, si.Profile)
		}
		mu.Unlock()
		return nil
	})
	return profiles, nil
}

var (
	// memMaxGetter gets the max value of "mem" resource from the given profile.
	memMaxGetter = func(rp reflow.Profile) (float64, bool) {
		if v, ok := rp["mem"]; !ok {
			return 0.0, false
		} else {
			return v.Max, true
		}
	}

	// maxDurationGetter gets the max duration (in nanoseconds) across all resources from the given profile.
	maxDurationGetter = func(rp reflow.Profile) (float64, bool) {
		var (
			dur   time.Duration
			valid bool
		)
		for _, v := range rp {
			if v.First.IsZero() || v.Last.IsZero() {
				continue
			}
			d := v.Last.Sub(v.First)
			if d > dur {
				dur = d
				valid = true
			}
		}
		return float64(dur.Nanoseconds()), valid
	}
)

// durationNanos returns the predicted duration from the given profiles.
func (p *Predictor) durationNanos(profiles []reflow.Profile) (float64, error) {
	// Predict the memory usage of the taskGroup.
	pv, n := valuePercentile(profiles, 100, maxDurationGetter)
	if n < p.minData {
		return 0, fmt.Errorf("insufficient profiles (%d < %d)", n, p.minData)
	}
	return pv, nil
}

// memUsage returns the predicted memory usage (in bytes) from the given profiles.
func (p *Predictor) memUsage(profiles []reflow.Profile) (float64, error) {
	// Predict the memory usage of the taskGroup.
	pv, n := valuePercentile(profiles, p.memPercentile, memMaxGetter)
	if n < p.minData {
		return 0, fmt.Errorf("insufficient profiles (%d < %d)", n, p.minData)
	}
	return pv, nil
}

// groupByLevel maps all tasks by their respective taskGroups at the specified level.
// groupByLevel assumes all tasks have the same number of taskGroups.
func groupByLevel(tasks []*sched.Task, level int) map[taskGroup][]*sched.Task {
	groupSetMap := make(map[taskGroup][]*sched.Task)
	for _, task := range tasks {
		group := getTaskGroups(task)[level]
		if _, ok := groupSetMap[group]; !ok {
			groupSetMap[group] = make([]*sched.Task, 0)
		}
		groupSetMap[group] = append(groupSetMap[group], task)
	}
	return groupSetMap
}

// valuePercentile computes the 'p'th percentile of the value extracted across all the given profiles
// by the given extractFunc.  If the extractFunc returns false, the value is ignored.
// A valid percentile is in the range [0, 100].  Any percentile outside of this range will result in a panic.
func valuePercentile(profiles []reflow.Profile, p float64, extractFunc func(reflow.Profile) (float64, bool)) (float64, int) {
	if p < 0 || p > 100 {
		panic(fmt.Sprintf("percentile %v is outside of range [0, 100].", p))
	}
	maxVals := make([]float64, 0, len(profiles))
	for _, profile := range profiles {
		v, ok := extractFunc(profile)
		if !ok {
			continue
		}
		maxVals = append(maxVals, v)
	}
	sort.Float64s(maxVals)
	n := len(maxVals)
	switch {
	case n == 0:
		return 0, 0
	case p == 0:
		return maxVals[0], n
	case p == 100:
		return maxVals[len(maxVals)-1], n
	default:
		idx := int(math.Ceil((float64(n) * p / 100) - 1))
		return maxVals[idx], n
	}
}

// smallInspect is used to
// exclusively unmarshal Profile
// from an ExecInspect.
type smallInspect struct {
	Profile   reflow.Profile
	Error     *errors.Error
	ExecError *errors.Error
}
