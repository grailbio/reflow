package sched

import (
	"expvar"
	"sync"

	"github.com/grailbio/reflow"
)

// ExpVarScheduler is the name of the expvar scheduler stats.
const ExpVarScheduler = "scheduler"

// OverallStats is the overall scheduler stats.
type OverallStats struct {
	// TotalAllocs is the total number of allocs in the system (live or dead).
	TotalAllocs int64
	// TotalTasks is the total number of tasks (pending, running or completed).
	TotalTasks int64
}

// AllocStats is the per alloc stats.
type AllocStats struct {
	sync.Mutex `json:"-"`
	// Resources is the currently available resources.
	reflow.Resources
	// Dead indicates if this alloc is dead.
	Dead bool
	// TaskIDs is the list of tasks running in this alloc.
	TaskIDs map[string]int
}

// AssignTask makes an alloc<->task association.
func (a *AllocStats) AssignTask(task *Task) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	a.Resources.Sub(a.Resources, task.Config.Resources)
	a.TaskIDs[task.ID.String()] = 1
}

// RemoveTask removes the alloc<->task association.
func (a *AllocStats) RemoveTask(task *Task) {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	a.Resources.Add(a.Resources, task.Config.Resources)
	delete(a.TaskIDs, task.ID.String())
}

// MarkDead marks an alloc dead.
func (a *AllocStats) MarkDead() {
	a.Mutex.Lock()
	defer a.Mutex.Unlock()
	a.Dead = true
}

// Copy returns a copy of AllocStats.
func (a *AllocStats) Copy() AllocStats {
	var copy AllocStats
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

// TaskStatFields is the set of the task stats.
type TaskStatsFields struct {
	// Ident is the exec identifier of this task.
	Ident string
	// Type is the type of exec.
	Type string
	// State is the current state of the task.
	State int
	// Error if not nil, is the task error.
	Error error
}

// TaskStats is the per task info and stats.
type TaskStats struct {
	// Mutex protects TaskStatsFields.
	sync.Mutex `json:"-"`
	// TaskStatsFields are the task stats.
	TaskStatsFields
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

// Copy returns a copy of TaskStats
func (t *TaskStats) Copy() TaskStats {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return TaskStats{TaskStatsFields: t.TaskStatsFields}
}

// NewStats returns an new Stats object.
func newStats() *Stats {
	return &Stats{
		Allocs: make(map[string]*AllocStats),
		Tasks:  make(map[string]*TaskStats),
	}
}

// Stats has all the scheduler stats, including alloc/task states and stats.
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
func (s *Stats) publish() {
	expvar.Publish(ExpVarScheduler, expvar.Func(func() interface{} { return s.GetStats() }))
}

// AddTasks adds the tasks to the stats.
func (s *Stats) AddTasks(tasks []*Task) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.TotalTasks += int64(len(tasks))
	for _, t := range tasks {
		s.Tasks[t.ID.String()] = &TaskStats{TaskStatsFields: TaskStatsFields{Ident: t.Config.Ident, Type: t.Config.Type}}
		t.stats = s.Tasks[t.ID.String()]
	}
}

// ReturnTask removes a task from the stats before returning it.
func (s *Stats) ReturnTask(task *Task, alloc *alloc) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	t := s.Tasks[task.ID.String()]
	t.Update(task)
	a := s.Allocs[alloc.id]
	a.RemoveTask(task)
}

// AssignTask assigns a task to an alloc.
func (s *Stats) AssignTask(task *Task, alloc *alloc) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	t := s.Tasks[task.ID.String()]
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
	s.Allocs[alloc.id] = &AllocStats{TaskIDs: make(map[string]int), Resources: resources}
}

// MarkAllocDead marks an alloc dead.
func (s *Stats) MarkAllocDead(alloc *alloc) {
	s.Allocs[alloc.id].MarkDead()
}

// GetStats returns a copy of the scheduler stats.
func (s *Stats) GetStats() Stats {
	var copy Stats
	s.Mutex.Lock()
	copy.OverallStats = s.OverallStats
	copy.Allocs = make(map[string]*AllocStats)
	for k, v := range s.Allocs {
		copy.Allocs[k] = v
	}
	copy.Tasks = make(map[string]*TaskStats)
	for k, v := range s.Tasks {
		copy.Tasks[k] = v
	}
	s.Mutex.Unlock()
	for k, v := range copy.Allocs {
		c := v.Copy()
		copy.Allocs[k] = &c
	}
	for k, v := range copy.Tasks {
		c := v.Copy()
		copy.Tasks[k] = &c
	}
	return copy
}
