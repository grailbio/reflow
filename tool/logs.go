// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/taskdb/noptaskdb"
)

const (
	// rFC3339Millis is a custom format of timestamp useful for logging.
	rFC3339Millis = "[2006-01-02T15:04:05.000Z07:00]: "
	divider       = "------------------------------------------------------------"
)

func (c *Cmd) logs(ctx context.Context, args ...string) {
	var (
		flags         = flag.NewFlagSet("logs", flag.ExitOnError)
		stdoutFlag    = flags.Bool("stdout", false, "for exec logs, display stdout instead of stderr")
		followFlag    = flags.Bool("f", false, "for exec logs, follows until exec completion (full exec URI must be specified e.g. ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030/9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49)")
		reflowletFlag = flags.Bool("reflowlet", false, "if true, then streams the logs of the reflowlet")
		runLevelFlag  = flags.String("level", "info", "for a run log, this is the minimum log level for logs returned from the runlog")
		startFlag     = flags.String("start", "", "start of the time range (inclusive) for reflowlet logs (format YYYY-MM-DD UTC or time.Duration)")
		endFlag       = flags.String("end", "", "end of the time range (exclusive) for reflowlet logs (format YYYY-MM-DD UTC or time.Duration)")
		help          = `Logs displays logs from reflow runs, execs, or reflowlets.

The valid arguments are:
  RunId
	Fetches a reflow run log filtered to a certain logging level. First checks local reflow run logs, then taskdb.
		  Options:
			  -level=info
				   one of (debug,info,error), this specifies the minimum log level for logs returned

  TaskId / Exec URI
	Fetches a log produced by a reflow exec. Argument may be a TaskId or Exec URI.
		  Options:
			  -stdout
				   if set, will retrieve exec's stdout log instead of stderr.
			  -f
				   if the argument provided is an exec URI, follow the exec log until the exec completes.
			  -reflowlet
				   stream reflowlet logs instead of exec logs.

  Alloc URI / Hostname
	Fetches a log produced by a reflowlet. Requires -reflowlet to be set.
		  Options:
			 -start=[YYYY-MM-DD | time.Duration]
				   exclude logs produced before a UTC date or X [hours|minutes|seconds] ago.
			 -end=[YYYY-MM-DD | time.Duration]
				   exclude logs produced on or after a UTC date or X [hours|minutes|seconds] ago.

Examples:
  $ reflow logs 9909853c                                                                            (RunId or TaskId, requires taskdb)
  $ reflow logs 9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49                    (RunId or TaskId, requires taskdb)
  $ reflow logs sha256:9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49             (RunId or TaskId, requires taskdb)
  $ reflow logs -level=debug 5c902b1c                                                               (RunId, requires taskdb)
  $ reflow logs -stdout 71444f1a                                                                    (TaskId, requires taskdb)
  $ reflow logs -f ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030/71444f1a8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49
  $ reflow logs -reflowlet ec2-35-165-199-174.us-west-2.compute.amazonaws.com                       (requires -reflowlet to be set)
  $ reflow logs -reflowlet ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030 (requires -reflowlet to be set)
  $ reflow logs -reflowlet -start=24h -end=12h ec2-35-165-199-174.us-west-2.compute.amazonaws.com   (requires -reflowlet to be set)
`
	)
	c.Parse(flags, args, help, "logs [OPTIONS] ARG")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	arg := flags.Arg(0)

	var st, et time.Time
	n, err := parseName(arg)
	if err != nil {
		c.Printf("invalid argument '%s'\n%s", arg, help)
		flags.Usage()
		return
	}
	switch n.Kind {
	default:
		c.Fatalf("argument has invalid kind %s. An exec URI or task ID is required", n.Kind)
	case execName:
		if *reflowletFlag {
			c.reflowletLogsCw(ctx, n.Hostname, st, et, nil)
		} else {
			if err = c.execLogsFromAlloc(ctx, n, *stdoutFlag, *followFlag); err != nil {
				c.execLogsCloudWatch(ctx, execPath(n), *stdoutFlag)
			}
		}
	case idName:
		if c.tryLocalRunLog(n, log.LevelFromString(*runLevelFlag)) {
			return
		}
		var tdb taskdb.TaskDB
		err = c.Config.Instance(&tdb)
		if _, nop := tdb.(noptaskdb.NopTaskDB); nop || err != nil {
			c.Printf("invalid argument '%s' (requires taskdb)\n%s", arg, help)
			flags.Usage()
		}
		runq := taskdb.RunQuery{ID: taskdb.RunID(n.ID)}
		runs, rerr := tdb.Runs(ctx, runq)
		if rerr != nil {
			c.Fatal(rerr)
		}
		taskq := taskdb.TaskQuery{ID: taskdb.TaskID(n.ID)}
		tasks, terr := tdb.Tasks(ctx, taskq)
		if terr != nil {
			c.Fatal(terr)
		}
		switch len(tasks) + len(runs) {
		case 0:
			c.Fatalf("unable to resolve id %s", arg)
		case 1:
			if len(runs) == 1 {
				c.runLogAtLevel(ctx, runs[0], log.LevelFromString(*runLevelFlag))
			} else {
				if *reflowletFlag {
					c.reflowletLogsForTask(ctx, tasks[0])
				} else {
					c.execLogsForTaskId(ctx, tdb, tasks[0], *stdoutFlag)
				}
			}
		default:
			var matches []string
			for _, t := range tasks {
				matches = append(matches, fmt.Sprintf("TaskId %s", t.ID.ID()))
			}
			for _, r := range runs {
				matches = append(matches, fmt.Sprintf("RunId %s", r.ID.ID()))
			}
			c.Fatalf("Found multiple matches for the given ID. Matches are [%s]", strings.Join(matches, ", "))
		}
	case hostName, allocName:
		if !*reflowletFlag {
			c.Fatal("alloc URI and hostnames require -reflowlet to be set")
		}
		if dateStr := *startFlag; dateStr != "" {
			if st, err = parseDateStr(dateStr); err != nil {
				c.Fatalf("invalid -start %s: %v", dateStr, err)
			}
		}
		if dateStr := *endFlag; dateStr != "" {
			if et, err = parseDateStr(dateStr); err != nil {
				c.Fatalf("invalid -end %s: %v", dateStr, err)
			}
		}
		c.reflowletLogsCw(ctx, n.Hostname, st, et, nil)
	}
}

func (c *Cmd) execLogsForTaskId(ctx context.Context, tdb taskdb.TaskDB, task taskdb.Task, stdout bool) {
	d := task.Stderr
	if stdout {
		d = task.Stdout
	}
	if !d.IsZero() {
		rlogs := c.remoteLogsFromTaskRepo(ctx, tdb.Repository(), d)
		if rlogs.Type != reflow.RemoteLogsTypeCloudwatch {
			c.Printf("log location: %s\n", rlogs)
			return
		}
		var st, et time.Time
		c.must(c.writeCwLogs(c.Stdout, rlogs.LogGroupName, rlogs.LogStreamName, st, et, func(w io.Writer, event *cloudwatchlogs.OutputLogEvent) {
			_, _ = fmt.Fprintln(w, *event.Message)
		}))
		return
	}
	if n, err := parseName(task.URI); err != nil {
		c.Fatalf("missing reference to logs for task '%s' and and unable to parse task URI '%s': %v", task.ID.IDShort(), task.URI, err)
	} else {
		c.execLogsCloudWatch(ctx, execPath(n), stdout)
	}
	return
}

func (c *Cmd) reflowletLogsForTask(ctx context.Context, task taskdb.Task) {
	n, err := parseName(task.URI)
	if err != nil {
		c.Fatalf("unable to parse task URI '%s': %v\n%s", task.URI, err, help)
	}
	taskEnd := task.End
	if taskEnd.IsZero() {
		taskEnd = task.Keepalive
	}
	inlines := map[time.Time]string{
		task.Start: fmt.Sprintf("TASK %s START", task.ID.IDShort()),
		taskEnd:    fmt.Sprintf("TASK %s END", task.ID.IDShort()),
	}
	c.reflowletLogsCw(ctx, n.Hostname, task.Start.Add(-2*time.Minute), taskEnd.Add(2*time.Minute), inlines)
	return
}

func (c *Cmd) reflowletLogsCw(ctx context.Context, host string, st, et time.Time, inlines map[time.Time]string) {
	past := make(map[time.Time]bool)
	for t := range inlines {
		past[t] = false
	}
	mw := func(w io.Writer, event *cloudwatchlogs.OutputLogEvent) {
		tMillis := aws.Int64Value(event.Timestamp)
		sec, msec := tMillis/1000, tMillis%1000
		t := time.Unix(sec, msec*int64(time.Millisecond)).UTC()
		for k, pastk := range past {
			if pastk {
				continue
			}
			if t.After(k) {
				past[k] = true
				_, _ = fmt.Fprintf(w, "%s\n%s%s\n%s\n", divider, k.Format(rFC3339Millis), inlines[k], divider)
			}
		}
		// Reflowlet cloudwatch log event messages are themselves a JSON with a `message` field.
		var js map[string]interface{}
		msg := *event.Message
		if derr := json.Unmarshal([]byte(msg), &js); derr != nil {
			c.Log.Debugf("failed to decode '%s': %v", msg, derr)
			return
		}
		_, _ = fmt.Fprintln(w, t.Format(rFC3339Millis), js["message"])
	}
	c.must(c.writeCwLogs(c.Stdout, "reflow/reflowlet", host, st, et, mw))
}

func (c *Cmd) execLogsCloudWatch(ctx context.Context, p string, stdout bool) {
	var st, et time.Time
	streamName := p + "/stderr"
	if stdout {
		streamName = p + "/stdout"
	}
	c.must(c.writeCwLogs(c.Stdout, "reflow", streamName, st, et, func(w io.Writer, event *cloudwatchlogs.OutputLogEvent) {
		_, _ = fmt.Fprintln(w, *event.Message)
	}))
}

func (c *Cmd) execLogsFromAlloc(ctx context.Context, n name, stdout, follow bool) error {
	cluster := c.CurrentPool(ctx)
	alloc, err := cluster.Alloc(ctx, n.AllocID)
	if err != nil {
		return errors.E("execLogsFromAlloc", n.AllocID, err)
	}
	exec, err := alloc.Get(ctx, n.ID)
	if err != nil {
		return errors.E("execLogsFromAlloc", n.ID, err)
	}
	rc, err := exec.Logs(ctx, stdout, !stdout, follow)
	if err != nil {
		return errors.E("execLogsFromAlloc", exec.URI(), err)
	}
	defer func() { _ = rc.Close() }()
	_, err = io.Copy(c.Stdout, rc)
	c.must(err)
	return nil
}

func (c *Cmd) taskFromArg(ctx context.Context, tdb taskdb.TaskDB, d digest.Digest) (*taskdb.Task, error) {
	q := taskdb.TaskQuery{ID: taskdb.TaskID(d)}
	tasks, err := tdb.Tasks(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	if len(tasks) > 1 {
		return nil, fmt.Errorf("more than one task matched id: %v", d)
	}
	return &tasks[0], nil
}

func (c *Cmd) remoteLogsFromTaskRepo(ctx context.Context, repo reflow.Repository, d digest.Digest) (rl reflow.RemoteLogs) {
	rc, err := repo.Get(ctx, d)
	if err != nil {
		c.Fatalf("repository get %s: %v", d.Short(), err)
	}
	defer rc.Close()
	if derr := json.NewDecoder(rc).Decode(&rl); derr != nil {
		c.Fatalf("failed to decode remote logs location: %v", derr)
	}
	return
}

// runLogAtLevel prints the runlog for runId d, filtered for logs at or above the provided level
func (c *Cmd) runLogAtLevel(ctx context.Context, run taskdb.Run, level log.Level) {
	var (
		rc   io.ReadCloser
		repo reflow.Repository
		err  error
	)
	if run.RunLog.IsZero() {
		c.Fatalf("run log digest for run %s is empty", run.ID.IDShort())
	}
	c.must(c.Config.Instance(&repo))
	if rc, err = repo.Get(ctx, run.RunLog); err != nil {
		c.Fatalf("get %s: %v", run.RunLog, err)
	}
	defer rc.Close()
	c.printRunLog(rc, level)
}

// printRunLog prints the runlog from rc, filtered for logs at or above the provided level
func (c *Cmd) printRunLog(rc io.ReadCloser, level log.Level) {
	r, _ := regexp.Compile(`(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} )\[([^\]]*)\] (.*)`)
	scanner := bufio.NewScanner(rc)
	levelAllowed := false
	for scanner.Scan() {
		line := scanner.Text()
		matches := r.FindStringSubmatch(line)
		if len(matches) == 4 {
			currentLevel := log.LevelFromString(matches[2])
			if currentLevel <= level {
				levelAllowed = true
				c.Println(matches[1] + matches[3])
			} else {
				levelAllowed = false
			}
		} else if levelAllowed {
			// Assume logs with no level are continuations from the previous line
			c.Println(line)
		}
	}
}

// tryLocalRunLog checks for local runlogs which match the given ID, then prints. Returns true if a file was found and printed.
func (c *Cmd) tryLocalRunLog(n name, level log.Level) bool {
	var logName string
	if n.ID.IsShort() {
		logName = fmt.Sprintf("%s*.runlog", n.ID.Short())
	} else {
		logName = fmt.Sprintf("%s.runlog", n.ID.Hex())
	}
	localMatches, gerr := filepath.Glob(path.Join(c.rundir(), logName))
	if gerr != nil {
		return false
	}
	switch len(localMatches) {
	case 0:
		return false
	case 1:
		f, err := os.Open(localMatches[0])
		if f == nil || err != nil {
			return false
		}
		defer f.Close()
		c.Printf("Using local runlog file %s\n", localMatches[0])
		c.printRunLog(f, level)
		return true
	default:
		return false
	}
}

// messageWriter writes the given event to the writer
type messageWriter func(w io.Writer, event *cloudwatchlogs.OutputLogEvent)

func (c *Cmd) writeCwLogs(w io.Writer, groupName, streamName string, st, et time.Time, mw messageWriter) error {
	var awsSession *session.Session
	if err := c.Config.Instance(&awsSession); err != nil {
		return err
	}
	api := cloudwatchlogs.New(awsSession)
	req := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
		StartFromHead: aws.Bool(true),
	}
	if !st.IsZero() {
		stNanos := st.UTC().Truncate(time.Millisecond).UnixNano()
		req.StartTime = aws.Int64(stNanos / int64(time.Millisecond))
	}
	if !et.IsZero() {
		etNanos := et.UTC().Round(time.Millisecond).UnixNano()
		req.EndTime = aws.Int64(etNanos / int64(time.Millisecond))
	}
	for {
		out, err := api.GetLogEvents(req)
		if err != nil {
			return errors.E("GetLogEvents", req, err)
		}
		// AWS indicates that we have reached the end of the stream by returning the same
		// token that was passed in.
		if aws.StringValue(req.NextToken) == aws.StringValue(out.NextForwardToken) {
			return nil
		}
		req.NextToken = out.NextForwardToken
		for _, m := range out.Events {
			mw(w, m)
		}
	}
}
