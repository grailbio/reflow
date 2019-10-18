package local

import (
	"bufio"
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/reflow/log"
)

var bootTime = getLastBootTime()

type oomTracker struct {
	mu      sync.Mutex
	lastOOM map[int]time.Time
}

// newOOMTracker initializes a new oomTracker.
func newOOMTracker() *oomTracker {
	return &oomTracker{lastOOM: make(map[int]time.Time)}
}

// Monitor continuously reads system logs and records
// all OOMs it finds. It returns after the provided
// context has been canceled or if it encounters an error.
// It has the option of passing a Logger to log non-os.ErrClosed
// errors that occured while monitoring.
func (o *oomTracker) Monitor(ctx context.Context, log *log.Logger) {
	o.monitor(ctx, log, "/dev/kmsg", bootTime)
}

// LastOOMKill returns the most recent timestamp at which the provided pid was OOM killed.
// A zero-valued time indicates that the pid was never OOM Killed.
func (o *oomTracker) LastOOMKill(pid int) (time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	t, ok := o.lastOOM[pid]
	return t, ok
}

func (o *oomTracker) monitor(ctx context.Context, log *log.Logger, logPath string, bootTime time.Time) {
	kmsg, err := os.Open(logPath)
	if err != nil {
		return
	}
	// Close kmsg when ctx is cancelled. This will result in
	// the scanner encountering an os.ErrClosed, which will
	// cause monitoring to stop.
	closeCtx, closeCancel := context.WithCancel(ctx)
	defer closeCancel()
	go func() {
		<-closeCtx.Done()
		kmsg.Close()
	}()

	scan := bufio.NewScanner(kmsg)
	for scan.Scan() {
		o.maybeRecordOOM(scan.Text(), bootTime, log)
	}
	if scan.Err() != os.ErrClosed && log != nil {
		log.Errorf("error encountered while scanning %v for OOMs: %v", logPath, scan.Err())
	}
}

// maybeRecordOOM checks to see if a row contains an OOM message from
// the OOM Killer or the cgroup. If it finds an oom, it records the corresponding
// PID and the OOM time based on the machine's boot time.
func (o *oomTracker) maybeRecordOOM(row string, bootTime time.Time, log *log.Logger) {
	// The two possible formats for an OOM entry are:
	// <int>,<int>,<time since boot in us>,-;Out of memory: Kill process <pid>...
	// <int>,<int>,<time since boot in us>,-;memory cgroup out of memory: Kill process <pid>...
	const (
		lookOOMKiller = "Out of memory: Kill process"
		lookCGroup    = "Memory cgroup out of memory: Kill process"
	)

	if !strings.Contains(row, lookOOMKiller) && !strings.Contains(row, lookCGroup) {
		return
	}
	// Extract time since boot.
	logTime, err := strconv.ParseInt(strings.Split(row, ",")[2], 10, 64)
	if err != nil && log != nil {
		log.Errorf("unrecognizable OOM entry format: %v", row)
		return
	}
	durSinceBoot := time.Duration(logTime) * time.Microsecond
	oomTime := bootTime.Add(durSinceBoot)
	// Extract PID.
	msg := strings.Split(row, ";")[1]
	msg = strings.Split(msg, ":")[1]
	msg = strings.Trim(msg, " ")
	pid, err := strconv.Atoi(strings.Split(msg, " ")[2])
	if err != nil && log != nil {
		log.Errorf("unrecognizable OOM entry format: %v", row)
		return
	}
	o.mu.Lock()
	o.lastOOM[pid] = oomTime
	o.mu.Unlock()
}

// getLastBootTime gets the system's boot time. It is only valid on linux systems.
func getLastBootTime() time.Time {
	p, err := ioutil.ReadFile("/proc/uptime")
	if err != nil {
		return time.Time{}
	}
	field := bytes.Fields(p)[0]
	// /proc/uptime returns the uptime in seconds, recorded to 0.01s.
	uptime, err := strconv.ParseFloat(string(field), 64)
	if err != nil {
		return time.Time{}
	}
	// Convert uptime to an integer number of milliseconds.
	uptimeMs := time.Duration(int64(uptime*1000)) * time.Millisecond
	return time.Now().Add(-uptimeMs)
}
