// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// THIS FILE WAS AUTOMATICALLY GENERATED (@generated). DO NOT EDIT.

package metrics

import (
	"context"
)

var (
	Counters = map[string]counterOpts{
		"allocs_completed_count": {
			Help: "Count of completed allocs.",
		},
		"allocs_completed_size": {
			Help: "Size of completed allocs.",
		},
		"allocs_started_count": {
			Help: "Count of started allocs.",
		},
		"allocs_started_size": {
			Help: "Size of started allocs.",
		},
		"tasks_completed_count": {
			Help: "Count of completed tasks.",
		},
		"tasks_completed_size": {
			Help: "Size of completed tasks.",
		},
		"tasks_started_count": {
			Help: "Count of started tasks.",
		},
		"tasks_started_size": {
			Help: "Size of started tasks.",
		},
		"tasks_submitted_count": {
			Help: "Count of submitted tasks.",
		},
		"tasks_submitted_size": {
			Help: "Size of submitted tasks.",
		},
	}
	Gauges = map[string]gaugeOpts{
		"memstats_heap_inuse_bytes": {
			Help: "Bytes of memory used by in use heap spans.",
		},
		"memstats_heap_objects": {
			Help: "Current number of allocated heap objects.",
		},
		"memstats_heap_sys_bytes": {
			Help: "Bytes of heap memory obtained from the OS.",
		},
		"memstats_stack_inuse_bytes": {
			Help: "Bytes of memory used for stack spans.",
		},
		"memstats_stack_sys_bytes": {
			Help: "Bytes of stack memory obtained from the OS.",
		},
	}
	Histograms = map[string]histogramOpts{
		"dydbassoc_op_latency_seconds": {
			Help:    "Dydbassoc operation latency in seconds.",
			Labels:  []string{"operation"},
			Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
		},
	}
)

// GetAllocsCompletedCountCounter returns a Counter to set metric allocs_completed_count (count of completed allocs).
func GetAllocsCompletedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "allocs_completed_count", nil)
}

// GetAllocsCompletedSizeCounter returns a Counter to set metric allocs_completed_size (size of completed allocs).
func GetAllocsCompletedSizeCounter(ctx context.Context) Counter {
	return getCounter(ctx, "allocs_completed_size", nil)
}

// GetAllocsStartedCountCounter returns a Counter to set metric allocs_started_count (count of started allocs).
func GetAllocsStartedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "allocs_started_count", nil)
}

// GetAllocsStartedSizeCounter returns a Counter to set metric allocs_started_size (size of started allocs).
func GetAllocsStartedSizeCounter(ctx context.Context) Counter {
	return getCounter(ctx, "allocs_started_size", nil)
}

// GetTasksCompletedCountCounter returns a Counter to set metric tasks_completed_count (count of completed tasks).
func GetTasksCompletedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_completed_count", nil)
}

// GetTasksCompletedSizeCounter returns a Counter to set metric tasks_completed_size (size of completed tasks).
func GetTasksCompletedSizeCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_completed_size", nil)
}

// GetTasksStartedCountCounter returns a Counter to set metric tasks_started_count (count of started tasks).
func GetTasksStartedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_started_count", nil)
}

// GetTasksStartedSizeCounter returns a Counter to set metric tasks_started_size (size of started tasks).
func GetTasksStartedSizeCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_started_size", nil)
}

// GetTasksSubmittedCountCounter returns a Counter to set metric tasks_submitted_count (count of submitted tasks).
func GetTasksSubmittedCountCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_submitted_count", nil)
}

// GetTasksSubmittedSizeCounter returns a Counter to set metric tasks_submitted_size (size of submitted tasks).
func GetTasksSubmittedSizeCounter(ctx context.Context) Counter {
	return getCounter(ctx, "tasks_submitted_size", nil)
}

// GetMemstatsHeapInuseBytesGauge returns a Gauge to set metric memstats_heap_inuse_bytes (bytes of memory used by in use heap spans).
func GetMemstatsHeapInuseBytesGauge(ctx context.Context) Gauge {
	return getGauge(ctx, "memstats_heap_inuse_bytes", nil)
}

// GetMemstatsHeapObjectsGauge returns a Gauge to set metric memstats_heap_objects (current number of allocated heap objects).
func GetMemstatsHeapObjectsGauge(ctx context.Context) Gauge {
	return getGauge(ctx, "memstats_heap_objects", nil)
}

// GetMemstatsHeapSysBytesGauge returns a Gauge to set metric memstats_heap_sys_bytes (bytes of heap memory obtained from the OS).
func GetMemstatsHeapSysBytesGauge(ctx context.Context) Gauge {
	return getGauge(ctx, "memstats_heap_sys_bytes", nil)
}

// GetMemstatsStackInuseBytesGauge returns a Gauge to set metric memstats_stack_inuse_bytes (bytes of memory used for stack spans).
func GetMemstatsStackInuseBytesGauge(ctx context.Context) Gauge {
	return getGauge(ctx, "memstats_stack_inuse_bytes", nil)
}

// GetMemstatsStackSysBytesGauge returns a Gauge to set metric memstats_stack_sys_bytes (bytes of stack memory obtained from the OS).
func GetMemstatsStackSysBytesGauge(ctx context.Context) Gauge {
	return getGauge(ctx, "memstats_stack_sys_bytes", nil)
}

// GetDydbassocOpLatencySecondsHistogram returns a Histogram to set metric dydbassoc_op_latency_seconds (dydbassoc operation latency in seconds).
func GetDydbassocOpLatencySecondsHistogram(ctx context.Context, operation string) Histogram {
	return getHistogram(ctx, "dydbassoc_op_latency_seconds", map[string]string{"operation": operation})
}
