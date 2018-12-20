// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package infra

type state int

const (
	todo state = iota
	visiting
	visited
)

// TopoSorter implements a simple topological sort
// based on provider instances.
type topoSorter map[*instance][]*instance

// Add adds an edge to the graph s.
func (s topoSorter) Add(from, to *instance) {
	s[from] = append(s[from], to)
}

// Sort returns a topological sort of the graph s.
func (s topoSorter) Sort() []*instance {
	var (
		states = make(map[*instance]state)
		order  []*instance
	)
	for key := range s {
		order = s.visit(key, states, order)
	}
	return order
}

func (s topoSorter) Cycle() []*instance {
	states := make(map[*instance]state)
	for key := range s {
		if trail := s.cycles(key, nil, states); trail != nil {
			return trail
		}
	}
	return nil
}

func (s topoSorter) cycles(key *instance, trail []*instance, states map[*instance]state) []*instance {
	switch states[key] {
	case todo:
		states[key] = visiting
		trail = append(trail, key)
		for _, child := range s[key] {
			if cycle := s.cycles(child, trail, states); cycle != nil {
				return cycle
			}
		}
		states[key] = visited
		return nil
	case visiting:
		// Find the previous instance of key in the trail.
		for i := range trail {
			if trail[i] == key {
				trail = trail[i:]
				break
			}
		}
		return append(trail, key)
	case visited:
		return nil
	default:
		panic("invalid state")
	}
}

func (s topoSorter) visit(key *instance, states map[*instance]state, order []*instance) []*instance {
	switch states[key] {
	case todo:
		states[key] = visiting
		for _, child := range s[key] {
			order = s.visit(child, states, order)
		}
		states[key] = visited
		return append(order, key)
	case visiting:
		panic("cycle in graph involving node " + key.Impl())
	case visited:
		return order
	default:
		panic("invalid state")
	}
}
