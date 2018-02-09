// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package status provides facilities for reporting statuses from a
// number of tasks working towards a common goal. The toplevel Status
// represents the status for the whole job; it in turn comprises a
// number of groups; each group has 0 or more tasks.
//
// Tasks (and groups) may be updated via their Print[f] functions;
// reporters receive notifications when updates have been made.
//
// Package status also includes a standard console reporter that
// formats nice status screens when the output is a terminal, or else
// issues periodic status updates.
package status

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const expiry = 10 * time.Second

// Value is a task or group status at a point in time, it includes a
// title, status, as well as its start and stop times (undefined for
// groups).
type Value struct {
	Title, Status string
	Begin, End    time.Time
	LastBegin     time.Time
	Count         int
}

// A Task is a single unit of work. It has a title, a beginning and
// an end time, and may receive zero or more status updates
// throughout its lifetime.
type Task struct {
	group *Group
	// next is managed by the task's group.
	next *Task

	mu    sync.Mutex
	value Value
}

// Print formats a message as fmt.Sprint and updates the task's
// status.
func (t *Task) Print(v ...interface{}) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.value.Status = fmt.Sprint(v...)
	t.mu.Unlock()
	t.group.notify()
}

// Printf formats a message as fmt.Sprintf and updates the task's
// status.
func (t *Task) Printf(format string, args ...interface{}) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.value.Status = fmt.Sprintf(format, args...)
	t.mu.Unlock()
	t.group.notify()
}

// Title formats a title as fmt.Sprint, and updates the task's title.
func (t *Task) Title(v ...interface{}) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.value.Title = fmt.Sprint(v...)
	t.mu.Unlock()
	t.group.notify()
}

// Titlef formats a title as fmt.Sprintf, and updates the task's title.
func (t *Task) Titlef(format string, args ...interface{}) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.value.Title = fmt.Sprintf(format, args...)
	t.mu.Unlock()
	t.group.notify()
}

// Done sets the completion time of the task to the current time.
// Tasks should not be updated after a call to Done; they will be
// discarded by the group after a timeout.
func (t *Task) Done() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.value.End = time.Now()
	t.mu.Unlock()
	t.group.notify()
}

// Value returns this tasks's current value.
func (t *Task) Value() Value {
	t.mu.Lock()
	v := t.value
	t.mu.Unlock()
	return v
}

// A Group is a collection of tasks, working toward a common goal.
// Groups are persistent: they have no beginning or end; they have a
// "toplevel" status that can be updated.
type Group struct {
	status *Status

	mu    sync.Mutex
	value Value
	task  *Task
}

// Print formats a status as fmt.Sprint and sets it as the group's status.
func (g *Group) Print(v ...interface{}) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.value.Status = fmt.Sprint(v...)
	g.mu.Unlock()
	g.notify()
}

// Printf formats a status as fmt.Sprintf and sets it as the group's status.
func (g *Group) Printf(format string, args ...interface{}) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.value.Status = fmt.Sprintf(format, args...)
	g.mu.Unlock()
	g.notify()
}

// Start creates a new task associated with this group and returns it.
// The task's initial title is formatted from the provided arguments as
// fmt.Sprint.
func (g *Group) Start(v ...interface{}) *Task {
	if g == nil {
		return nil
	}
	task := new(Task)
	g.mu.Lock()
	task.value.Begin = time.Now()
	task.group = g
	p := &g.task
	for *p != nil {
		p = &(*p).next
	}
	*p = task
	g.mu.Unlock()
	task.Title(v...) // this will also notify
	return task
}

// Startf creates a new task associated with tihs group and returns it.
// The task's initial title is formatted from the provided arguments as
// fmt.Sprintf.
func (g *Group) Startf(format string, args ...interface{}) *Task {
	return g.Start(fmt.Sprintf(format, args...))
}

// Tasks returns a snapshot of the group's currently active tasks.
// Expired tasks are garbage collected on calls to Tasks. Tasks are
// returned in the order of creation: the oldest is always first.
func (g *Group) Tasks() []*Task {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	var tasks []*Task
	for p := &g.task; *p != nil; {
		value := (*p).Value()
		if !value.End.IsZero() && now.Sub(value.End) > expiry {
			*p = (*p).next
		} else {
			tasks = append(tasks, *p)
			p = &(*p).next
		}
	}
	return tasks
}

// Value returns the group's current value.
func (g *Group) Value() Value {
	g.mu.Lock()
	v := g.value
	g.mu.Unlock()
	return v
}

func (g *Group) notify() {
	g.status.notify()
}

type waiter struct {
	version int
	c       chan int
}

// Status represents a toplevel status object. A status comprises a
// number of groups which in turn comprise a number of sub-tasks.
type Status struct {
	mu      sync.Mutex
	groups  map[string]*Group
	version int
	order   []string
	waiters []waiter
}

// Group creates and returns a new group named by the provided
// arguments as formatted by fmt.Sprint. If the group already exists,
// it is returned.
func (s *Status) Group(v ...interface{}) *Group {
	name := fmt.Sprint(v...)
	s.mu.Lock()
	if s.groups == nil {
		s.groups = make(map[string]*Group)
	}
	if s.groups[name] == nil {
		s.groups[name] = &Group{status: s, value: Value{Title: name}}
	}
	g := s.groups[name]
	s.mu.Unlock()
	s.notify()
	return g
}

// Groupf creates and returns a new group named by the provided
// arguments as formatted by fmt.Sprintf. If the group already exists,
// it is returned.
func (s *Status) Groupf(format string, args ...interface{}) *Group {
	return s.Group(fmt.Sprintf(format, args...))
}

// Wait returns a channel that is blocked until the version of
// status data is greater than the provided version. When the
// status version exceeds v, it is written to the channel and
// then closed.
//
// This allows status observers to implement a simple loop
// that coalesces updates:
//
//	v := -1
//	for {
//		v = <-status.Wait(v)
//		groups := status.Groups()
//		// ... process groups
//	}
func (s *Status) Wait(v int) <-chan int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := make(chan int, 1)
	if v < s.version {
		c <- s.version
		return c
	}
	i := sort.Search(len(s.waiters), func(i int) bool {
		return s.waiters[i].version > v
	})
	s.waiters = append(s.waiters[:i], append([]waiter{{v, c}}, s.waiters[i:]...)...)
	return c
}

// Groups returns a snapshot of the status groups. Groups maintains a
// consistent order of returned groups: when a group cohort first
// appears, it is returned in arbitrary order; each cohort is
// appended to the last, and the order of all groups is remembered
// across invocations.
func (s *Status) Groups() []*Group {
	s.mu.Lock()
	seen := make(map[string]bool)
	for _, name := range s.order {
		seen[name] = true
	}
	for name := range s.groups {
		if !seen[name] {
			s.order = append(s.order, name)
		}
	}
	var groups []*Group
	for _, name := range s.order {
		if s.groups[name] != nil {
			groups = append(groups, s.groups[name])
		}
	}
	s.mu.Unlock()
	return groups
}

func (s *Status) notify() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	for len(s.waiters) > 0 && s.waiters[0].version < s.version {
		s.waiters[0].c <- s.version
		s.waiters = s.waiters[1:]
	}
}
