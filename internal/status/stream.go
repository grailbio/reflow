// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package status

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"
)

const (
	refreshPeriod = 10 * time.Second
	// the smallest interval between reports in "simple" mode
	minSimpleReportingPeriod = time.Minute
)

var maxTime = time.Unix(1<<63-1, 0)

type result struct {
	n   int
	err error
}

type kind int

const (
	noop kind = iota
	write
	stop
)

type req struct {
	kind kind
	p    []byte
	w    io.Writer
	rc   chan result
}

type writer struct {
	r Reporter
	w io.Writer
}

func (w *writer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		// To check EOF?
		return w.w.Write(p)
	}
	c := make(chan result, 1)
	w.r <- req{write, p, w.w, c}
	r := <-c
	return r.n, r.err
}

// Reporter displays regular updates of a Status. When updates are
// displayed on a terminal, each update replaces the previous, so
// only one update remains visible at a time. Otherwise update
// snapshots are written periodically.
type Reporter chan req

// Wrap returns a writer whose writes are serviced by the reporter
// and written to the underlying writer w. Wrap is used to allow an
// application to write to the same set of file descriptors as are
// used to render status updates. This permits the reporter's
// terminal handling code to properly write log messages while also
// rendering regular status updates.
func (r Reporter) Wrap(w io.Writer) io.Writer {
	return &writer{r: r, w: w}
}

// Go starts the Reporter's service routine, and will write regular
// updates to the provided writer.
func (r Reporter) Go(w io.Writer, status *Status) {
	if term, err := openTerm(w); err == nil {
		r.displayTerm(w, term, status)
	} else {
		r.displaySimple(w, status)
	}
}

// Stop halts rendering of status updates; writes to writers
// returned by Wrap are still serviced.
func (r Reporter) Stop() {
	c := make(chan result, 1)
	r <- req{kind: stop, rc: c}
	<-c
}

// displayTerm updates a status on the terminal w, with terminal
// capabilities as described by term. displayTerm draws the status on
// each update of status, and also at a regular refresh period to
// update task elapsed times. Writes wrapped by the reporter are
// serviced after first clearing the screen. This ensures that these
// writes appear consistently on the screen above a persistent status
// display. displayTerm handles window resize events.
func (r Reporter) displayTerm(w io.Writer, term *term, status *Status) {
	var nlines int
	// TODO(marius): limit the maximum number of subtasks displayed
	// Cursor is always at the end.
	var (
		tick    = time.NewTicker(refreshPeriod)
		stopped bool
		v       = -1
		winch   = make(chan os.Signal, 1)
	)
	signal.Notify(winch, syscall.SIGWINCH)
	defer tick.Stop()
	defer signal.Stop(winch)
	width, height := term.Dim()
	for {
		var req req
		select {
		case v = <-status.Wait(v):
		case req = <-r:
		case <-tick.C:
		case <-winch:
			width, height = term.Dim()
		}
		if nlines > height {
			nlines = height
		}
		for i := 0; i < nlines; i++ {
			term.Move(w, -1)
			term.Clear(w)
		}
		nlines = 0
		switch req.kind {
		case noop:
		case write:
			n, err := req.w.Write(req.p)
			req.rc <- result{n, err}
		case stop:
			// We stop reporting status but keep servicing writes.
			stopped = true
			close(req.rc)
		}
		if stopped {
			continue
		}
		groups := status.Groups()
		if len(groups) == 0 {
			continue
		}
		// Take a snapshot of all the values to be rendered. The 0th value
		// in each group is the group toplevel status. We then accomodate
		// for our height budget by trimming task statuses (oldest first).
		var snapshot [][]Value
		for _, g := range groups {
			v := g.Value()
			tasks := g.Tasks()
			if v.Status == "" && len(tasks) == 0 {
				continue
			}
			values := []Value{v}
			for _, task := range tasks {
				values = append(values, task.Value())
			}
			snapshot = append(snapshot, values)
		}
		var n int
		for _, g := range snapshot {
			n += len(g) - 1
		}
		// Always make room for the toplevel status.
		// We also need one extra line for the last newline.
		for n > height-len(snapshot)-1 {
			var (
				mini = -1
				min  time.Time
			)
			for i, g := range snapshot {
				if len(g) > 1 && (mini < 0 || g[1].Begin.Before(min)) {
					min = g[1].Begin
					mini = i
				}
			}
			if mini < 0 {
				// Nothing we can do.
				break
			}
			snapshot[mini] = append(snapshot[mini][:1], snapshot[mini][2:]...)
			n--
		}
		now := time.Now()
		for _, group := range snapshot {
			v, tasks := group[0], group[1:]
			top := fmt.Sprintf("%s: %s", v.Title, v.Status)
			if len(top) > width {
				top = top[:width]
			}
			fmt.Fprintln(w, top)
			nlines++
			tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
			type row struct{ title, value, elapsed string }
			rows := make([]row, len(tasks))
			var maxtitle, maxvalue, maxtime int
			for i, v := range tasks {
				elapsed := now.Sub(v.Begin)
				elapsed -= elapsed % time.Second
				rows[i] = row{
					title:   v.Title,
					value:   v.Status,
					elapsed: elapsed.String(),
				}
				maxtitle = max(maxtitle, len(rows[i].title))
				maxvalue = max(maxvalue, len(rows[i].value))
				maxtime = max(maxtime, len(rows[i].elapsed))
			}
			if trim := 2 + maxtitle + 3 + maxvalue + 2 + maxtime - width; trim > 0 {
				if trim > maxvalue {
					trim -= maxvalue
					maxvalue = 0
				} else {
					maxvalue -= trim
					trim = 0
				}
				if trim > 0 && maxtitle > 10 {
					n := maxtitle - trim
					if n < 10 {
						n = 10
					}
					maxtitle = n
				}
			}
			for _, row := range rows {
				fmt.Fprintf(tw, "\t%s:\t%s\t%s\n",
					trim(row.title, maxtitle),
					trim(row.value, maxvalue),
					trim(row.elapsed, maxtime),
				)
				nlines++
			}
			tw.Flush()
		}
	}
}

// displaySimple writes the provided status to writer w whenever
// the status is updated, but at a minimum interval defined by
// minSimpleReportingPeriod. Writes wrapped by the reporter
// are serviced directly by displaySimple.
func (r Reporter) displaySimple(w io.Writer, status *Status) {
	var (
		stopped    bool
		v          = -1
		lastReport time.Time
		nextReport <-chan time.Time
	)
	for {
		var req req
		select {
		case v = <-status.Wait(v):
		case req = <-r:
		case <-nextReport:
			nextReport = nil
		}
		switch req.kind {
		case noop:
		case write:
			n, err := req.w.Write(req.p)
			req.rc <- result{n, err}
			continue
		case stop:
			stopped = true
			close(req.rc)
			continue
		}
		if stopped {
			continue
		}
		// In this case we're already waiting to report.
		if nextReport != nil {
			continue
		}
		if elapsed := time.Since(lastReport); elapsed < minSimpleReportingPeriod {
			nextReport = time.After(minSimpleReportingPeriod - elapsed)
			continue
		}
		now := time.Now()
		for _, group := range status.Groups() {
			v := group.Value()
			tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
			fmt.Fprintf(tw, "%s: %s\n", v.Title, v.Status)
			for _, task := range group.Tasks() {
				v := task.Value()
				elapsed := now.Sub(v.Begin)
				elapsed -= elapsed % time.Second
				fmt.Fprintf(tw, "\t%s:\t%s\t%s\n", v.Title, v.Status, elapsed)
			}
			tw.Flush()
		}
		lastReport = time.Now()
	}
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func trim(s string, n int) string {
	if len(s) < n {
		return s
	}
	return s[:n]
}
