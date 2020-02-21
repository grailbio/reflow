package sched

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
	"github.com/grailbio/reflow/taskdb"
)

const (
	// memPercentile is the percentile that will
	// be used to predict memory usage for all tasks.
	memPercentile float64 = 95
	// maxInspect is the maximum number of ExecInspects which
	// will be unmarshalled to get obtain profiling data for a
	// particular taskGroup.
	maxInspect = 50
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
	// inspectLimiter limits the number of concurrent
	// ExecInspect Profile unmarshal operations.
	inspectLimiter *limiter.Limiter
}

// NewPred returns a new Predictor instance. NewPred will panic if either repo or tdb is nil because
// a Predictor requires both a taskdb and a repository to function. NewPred will also panic if
// minData <= 0 because a prediction requires at least one data point.
func NewPred(repo reflow.Repository, tdb taskdb.TaskDB, log *log.Logger, minData int) *Predictor {
	if tdb == nil || repo == nil {
		panic("predictor requires both a repository and a taskdb to function")
	}
	if minData <= 0 {
		panic("minData must be greater than zero")
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
		inspectLimiter: inspectLimiter,
	}
}

// Predict returns the predicted Resources of submitted tasks. If Predict fails
// to predict the Resources of a particular task, it will return no resources
// for the task.
func (p *Predictor) Predict(ctx context.Context, tasks ...*Task) map[*Task]reflow.Resources {
	// Only predict the resources of tasks of type "exec" because
	// "exec" tasks are the only tasks with configured resources and
	// profiling data.
	predictableTasks := make([]*Task, 0, len(tasks))
	for _, task := range tasks {
		if task.Config.Type == "exec" {
			predictableTasks = append(predictableTasks, task)
		}
	}

	var (
		// Since all tasks have the same list of taskGroups,
		// all taskGroups will have the same taskLevel.
		maxLevel = len(getTaskGroups(tasks[0]))

		mu     sync.Mutex
		resMap = make(map[*Task]reflow.Resources)
		// todo keeps track of which tasks do not yet have their resources predicted.
		// Since all tasks have the same list of taskGroups, all the tasks in todo
		// can be grouped by their taskGroup's level.
		todo = newTaskSet(predictableTasks...)
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
			if mem, err := p.memUsage(ctx, groups[i]); err != nil {
				p.log.Debugf("error while predicting memory: %s", err)
			} else {
				p.log.Debugf("successfully modeled memory usage: %s", groups[i].Name())
				doneTasks := groupMap[groups[i]]
				for j := 0; j < len(doneTasks); j++ {
					predictedResources := make(reflow.Resources)
					predictedResources["mem"] = mem

					mu.Lock()
					todo.RemoveAll(doneTasks[j])
					resMap[doneTasks[j]] = predictedResources
					mu.Unlock()
				}
			}
			return nil
		})
		p.log.Debugf("successfully modeled memory usage for %d/%d tasks", len(tasks)-todo.Len(), len(tasks))
	}

	return resMap
}

// memUsage returns the predicted memory usage of a task in group.
func (p *Predictor) memUsage(ctx context.Context, group taskGroup) (float64, error) {
	// Query taskdb for all tasks in the taskGroup.
	tasks, err := p.taskDB.Tasks(ctx, group.Query())
	if err != nil {
		return 0, errors.E(group.Name(), "taskdb query", err)
	}
	if len(tasks) < p.minData {
		return 0, errors.E(group.Name(), fmt.Errorf("insufficient tasks (%d < %d)", len(tasks), p.minData))
	}

	var inspectDigests = make([]digest.Digest, len(tasks))
	for i, task := range tasks {
		inspectDigests[i] = task.Inspect
	}

	// In the event that there are over maxInspect inspects,
	// randomly select maxInspect inspects to download and
	// unmarshal.
	if len(inspectDigests) > maxInspect {
		src := rand.NewSource(time.Now().UnixNano())
		rand.New(src).Shuffle(len(inspectDigests), func(i, j int) {
			inspectDigests[i], inspectDigests[j] = inspectDigests[j], inspectDigests[i]
		})
		inspectDigests = inspectDigests[:maxInspect]
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
		if _, ok := si.Profile["mem"]; ok {
			profiles = append(profiles, si.Profile)
		}
		mu.Unlock()
		return nil
	})
	if len(profiles) < p.minData {
		return 0, errors.E(group.Name(), fmt.Errorf("insufficient profiles (%d < %d)", len(profiles), p.minData))
	}

	// Predict the memory usage of the taskGroup.
	return maxValuePercentile(profiles, "mem", memPercentile), nil
}

// taskSet is a set of tasks.
type taskSet map[*Task]bool

// newTaskSet returns a set of tasks.
func newTaskSet(tasks ...*Task) taskSet {
	set := make(taskSet)
	for _, task := range tasks {
		set[task] = true
	}
	return set
}

// RemoveAll removes tasks from the taskSet.
func (s taskSet) RemoveAll(tasks ...*Task) {
	for _, task := range tasks {
		delete(s, task)
	}
}

// Slice returns a slice containing the tasks in the taskSet.
func (s taskSet) Slice() []*Task {
	var tasks = make([]*Task, 0, len(s))
	for task := range s {
		tasks = append(tasks, task)
	}
	return tasks
}

// Len returns the number of tasks in the taskSet.
func (s taskSet) Len() int {
	return len(s)
}

// groupByLevel maps all tasks by their respective taskGroups at the specified level.
// groupByLevel assumes all tasks have the same number of taskGroups.
func groupByLevel(tasks []*Task, level int) map[taskGroup][]*Task {
	groupSetMap := make(map[taskGroup][]*Task)
	for _, task := range tasks {
		group := getTaskGroups(task)[level]
		if _, ok := groupSetMap[group]; !ok {
			groupSetMap[group] = make([]*Task, 0)
		}
		groupSetMap[group] = append(groupSetMap[group], task)
	}
	return groupSetMap
}

// maxValuePercentile computes the 'p'th percentile of the max values for the given resource
// across all the given profiles. A valid percentile is in the range [0, 100]. Any
// percentile outside of this range will result in a panic.
func maxValuePercentile(profiles []reflow.Profile, resource string, p float64) float64 {
	if p < 0 || p > 100 {
		panic(fmt.Sprintf("percentile %v is outside of range [0, 100].", p))
	}
	maxMem := make([]float64, len(profiles))
	for i, profile := range profiles {
		maxMem[i] = profile[resource].Max
	}
	sort.Float64s(maxMem)
	n := len(maxMem)
	if n == 0 {
		return 0
	}
	if p == 100 {
		return maxMem[len(maxMem)-1]
	} else if p == 0 {
		return maxMem[0]
	}
	idx := int(math.Ceil((float64(n) * p / 100) - 1))
	return maxMem[idx]
}

// smallInspect is used to
// exclusively unmarshal Profile
// from an ExecInspect.
type smallInspect struct {
	Profile reflow.Profile
}
