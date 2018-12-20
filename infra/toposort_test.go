// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package infra

import "testing"

func TestTopoSorter(t *testing.T) {
	var (
		credentials = &instance{name: "credentials"}
		repository  = &instance{name: "repository"}
		cluster     = &instance{name: "cluster"}
		database    = &instance{name: "database"}
	)
	graph := make(topoSorter)
	graph.Add(database, credentials)
	graph.Add(database, repository)
	graph.Add(cluster, credentials)
	graph.Add(repository, credentials)

	order := graph.Sort()
	for from, tos := range graph {
		i := index(from, order)
		for _, to := range tos {
			j := index(to, order)
			if i <= j {
				t.Errorf("invalid order: %d <= %d (%v)", i, j, order)
			}
		}
	}
}

func index(needle *instance, haystack []*instance) int {
	for i, inst := range haystack {
		if needle == inst {
			return i
		}
	}
	panic("not found")
}
