package sched

import (
	"expvar"
	"sync"

	"github.com/grailbio/reflow"
)

// ExpVarScheduler is the prefix of the scheduler stats exported name.
const ExpVarScheduler = "scheduler"

// OverallStats is the overall scheduler stats.
type OverallStats struct {
	// TotalAllocs is the total number of allocs in the system (live or dead).
	TotalAllocs int64
	// TotalTasks is the total number of tasks (pending, running or completed).
	TotalTasks int64
}

// AllocStatsData is the per alloc stats snapshot.
type AllocStatsData struct {
	// Resources is the currently available resources.
	reflow.Resources
	// Dead indicates if this alloc is dead.
	Dead bool
	// TaskIDs is the list of tasks running in this alloc.
	TaskIDs map[string]int
}

// AllocStats is the per alloc stats used to update stats.
type AllocStats struct {
	sync.Mutex `json:"-"`
	AllocStatsData
}

// AssignTask makes an alloc<->task association.
func (a *AllocStats) AssignTask(task *Task) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	a.Resources.Sub(a.Resources, task.Config.Resources)
	a.TaskIDs[GetTaskStatsId(task)] = 1
}

// RemoveTask removes the alloc<->task association.
func (a *AllocStats) RemoveTask(task *Task) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	a.Resources.Add(a.Resources, task.Config.Resources)
	delete(a.TaskIDs, GetTaskStatsId(task))
}

// MarkDead marks an alloc dead.
func (a *AllocStats) MarkDead() {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	a.Dead = true
}

// Copy returns an immutable snapshot of AllocStats.
func (a *AllocStats) Copy() AllocStatsData {
	var copy AllocStatsData
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	copy.Resources.Set(a.Resources)
	copy.Dead = a.Dead
	copy.TaskIDs = make(map[string]int, len(a.TaskIDs))
	for k, v := range a.TaskIDs {
		copy.TaskIDs[k] = v
	}
	return copy
}

// TaskStatsData is a snapshot of the task stats.
type TaskStatsData struct {
	// Ident is the exec identifier of this task.
	Ident string
	// Type is the type of exec.
	Type string
	// State is the current state of the task.
	State int
	// Error if not nil, is the task error.
	Error error
	// RunID is the run the task belongs to.
	RunID string
	// FlowID is the flow corresponding to this task.
	FlowID string
}

// TaskStats is the per task info and stats used to update stats.
type TaskStats struct {
	// Mutex protects TaskStatsData.
	sync.Mutex `json:"-"`
	// TaskStatsData are the task stats.
	TaskStatsData
}

// Update updates task state, error, if any.
func (t *TaskStats) Update(task *Task) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.State = int(task.state)
	if task.Err != nil {
		t.Error = task.Err
	}
}

// Copy returns a immutable snapshot of TaskStats.
func (t *TaskStats) Copy() TaskStatsData {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.TaskStatsData
}

// NewStats returns an new Stats object.
func newStats() *Stats {
	return &Stats{
		Allocs: make(map[string]*AllocStats),
		Tasks:  make(map[string]*TaskStats),
	}
}

// StatsData is a immutable snapshot of Stats, usually obtained by calling Stats.GetStats().
type StatsData struct {
	// OverallStats has the overall scheduler stats.
	OverallStats
	// Allocs has all the alloc stats, including dead ones.
	Allocs map[string]AllocStatsData
	// Tasks has all the task state and stats, including completed/error tasks.
	Tasks map[string]TaskStatsData
}

// Stats has all the scheduler stats, including alloc/task states and stats.
// It is thread safe and can be used to update stats.
type Stats struct {
	// Mutex protects all the data members.
	sync.Mutex `json:"-"`
	// OverallStats has the overall scheduler stats.
	OverallStats
	// Allocs has all the alloc stats, including dead ones.
	Allocs map[string]*AllocStats
	// Tasks has all the task state and stats, including completed/error tasks.
	Tasks map[string]*TaskStats
}

// Publish publishes the stats as a go expvar.
func (s *Stats) Publish() {
	s.PublishPrefix(ExpVarScheduler)
}

// Publish publishes the stats as a go expvar with the given prefix.
func (s *Stats) PublishPrefix(prefix string) {
	if prefix == "" {
		panic("prefix not provided for publishing stats")
	}
	if v := expvar.Get(prefix); v != nil {
		return
	}
	expvar.Publish(prefix, expvar.Func(func() interface{} { return s.GetStats() }))
}

// AddTasks adds the tasks to the stats.
func (s *Stats) AddTasks(tasks []*Task) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.TotalTasks += int64(len(tasks))
	for _, t := range tasks {
		s.Tasks[GetTaskStatsId(t)] = &TaskStats{TaskStatsData: TaskStatsData{Ident: t.Config.Ident, Type: t.Config.Type, RunID: t.RunID.ID(), FlowID: t.FlowID.String()}}
		t.stats = s.Tasks[GetTaskStatsId(t)]
	}
}

// ReturnTask removes a task from the stats before returning it.
func (s *Stats) ReturnTask(task *Task, alloc *alloc) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	t := s.Tasks[GetTaskStatsId(task)]
	t.Update(task)
	a := s.Allocs[alloc.id]
	a.RemoveTask(task)
}

// AssignTask assigns a task to an alloc.
func (s *Stats) AssignTask(task *Task, alloc *alloc) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	t := s.Tasks[GetTaskStatsId(task)]
	t.Update(task)
	a := s.Allocs[alloc.id]
	a.AssignTask(task)
}

// AddAlloc adds an alloc to the stats.
func (s *Stats) AddAlloc(alloc *alloc) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.TotalAllocs += 1
	resources := make(reflow.Resources, len(alloc.Resources()))
	for k, v := range alloc.Resources() {
		resources[k] = v
	}
	s.Allocs[alloc.id] = &AllocStats{AllocStatsData: AllocStatsData{TaskIDs: make(map[string]int), Resources: resources}}
}

// MarkAllocDead marks an alloc dead.
func (s *Stats) MarkAllocDead(alloc *alloc) {
	s.Allocs[alloc.id].MarkDead()
}

// GetStats returns a snapshot of the scheduler stats.
func (s *Stats) GetStats() StatsData {
	var copy StatsData
	s.Mutex.Lock()
	copy.OverallStats = s.OverallStats
	copy.Allocs = make(map[string]AllocStatsData)
	for k, v := range s.Allocs {
		copy.Allocs[k] = v.Copy()
	}
	copy.Tasks = make(map[string]TaskStatsData)
	for k, v := range s.Tasks {
		copy.Tasks[k] = v.Copy()
	}
	s.Mutex.Unlock()
	return copy
}

func GetTaskStatsId(task *Task) string {
	return task.FlowID.String()
}
