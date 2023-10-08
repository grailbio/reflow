package local

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/reflow/log"
)

const devKmsgPath = "/dev/kmsg"

var bootTime = getLastBootTime()

// OomDetector detects if an OOM has occurred.
type OomDetector interface {
	// Oom returns whether an OOM occurred for the given process ID within the given time range,
	// and a string with an explanation of why (if true) an OOM occurrence determination was made.
	// If pid is unspecified (ie, -1), then implementations can make a "possible OOM" determination.
	Oom(pid int, start, end time.Time) (bool, string)
}

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
// errors that occurred while monitoring.
func (o *oomTracker) Monitor(ctx context.Context, log *log.Logger) {
	o.monitor(ctx, log, devKmsgPath, bootTime)
}

// Oom implements OomDetector.
func (o *oomTracker) Oom(pid int, start, end time.Time) (bool, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if t, ok := o.lastOOM[pid]; ok && t.After(start) && !end.Before(t) {
		return true, fmt.Sprintf("pid %d OOM killed (based on %s logs) at %s, within (%s, %s]",
			pid, devKmsgPath, t.Format(time.RFC3339), start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return false, ""
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
	if !errors.Is(scan.Err(), os.ErrClosed) {
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
