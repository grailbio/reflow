// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package metrics

import (
	"context"
)

var (
	Counters = map[string]counterOpts{
		"tasks_started_count": {
			Help: "Count of started tasks.",
		},
		"tasks_finished_count": {
			Help: "Count of finished tasks.",
		},
	}
	Gauges     = map[string]gaugeOpts{}
	Histograms = map[string]histogramOpts{
		"assoc_op_duration_seconds": {
			Help:    "Duration of queries against dydbassoc.",
			Labels:  []string{"operation"},
			Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
		},
	}
)

// GetTasksStartedCountCounter returns a Counter to set metric tasks_started_count (count of started tasks).
func GetTasksStartedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_started_count", nil)
}

// GetTasksFinishedCountCounter returns a Counter to set metric tasks_finished_count (count of finished tasks).
func GetTasksFinishedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_finished_count", nil)
}

// GetAssocOpDurationSecondsHistogram returns a Histogram to set metric assoc_op_duration_seconds (duration of queries against dydbassoc).
func GetAssocOpDurationSecondsHistogram(ctx context.Context, operation string) Histogram {
	return getHistogram(ctx, "assoc_op_duration_seconds", map[string]string{"operation": operation})
}
