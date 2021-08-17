// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package monitoring

const (
	pressureCpuWaiting = "node_pressure_cpu_waiting_seconds_total"
	pressureIoStalled  = "node_pressure_io_stalled_seconds_total"
	pressureIoWaiting  = "node_pressure_io_waiting_seconds_total"
	pressureMemStalled = "node_pressure_memory_stalled_seconds_total"
	pressureMemWaiting = "node_pressure_memory_waiting_seconds_total"

	memAvailableBytes = "node_memory_MemAvailable_bytes"
	memFreeBytes      = "node_memory_MemFree_bytes"
	memTotalBytes     = "node_memory_MemTotal_bytes"
)

var NodeMetricsNames = map[string]bool{
	"node_disk_read_bytes_total":       true, // Per Device: The total number of bytes read successfully.
	"node_disk_reads_completed_total":  true, // Per Device: The total number of reads completed successfully.
	"node_disk_writes_completed_total": true, // Per Device: The total number of writes completed successfully.
	"node_disk_written_bytes_total":    true, // Per Device: The total number of bytes written successfully.

	"node_filefd_allocated": true, // File descriptor statistics: allocated.
	"node_filefd_maximum":   true, // File descriptor statistics: maximum.

	"node_load1":  true, // CPU: 1m load average.
	"node_load5":  true, // CPU: 5m load average.
	"node_load15": true, // CPU: 15m load average.

	memAvailableBytes: true, // Memory information field MemAvailable_bytes.
	memFreeBytes:      true, // Memory information field MemFree_bytes.
	memTotalBytes:     true, // Memory information field MemTotal_bytes.

	pressureCpuWaiting: true, // Total time in seconds that processes have waited for CPU time
	pressureIoStalled:  true, // Total time in seconds no process could make progress due to IO congestion
	pressureIoWaiting:  true, // Total time in seconds that processes have waited due to IO congestion
	pressureMemStalled: true, // Total time in seconds no process could make progress due to memory congestion
	pressureMemWaiting: true, // Total time in seconds that processes have waited for memory

	"node_vmstat_oom_kill": true,
}
