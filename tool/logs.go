// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/grailbio/reflow/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/taskdb"
)

const (
	// rFC3339Millis is a custom format of timestamp useful for logging.
	rFC3339Millis = "[2006-01-02T15:04:05.000Z07:00]: "
	divider       = "------------------------------------------------------------"
)

func (c *Cmd) logs(ctx context.Context, args ...string) {
	var (
		flags         = flag.NewFlagSet("logs", flag.ExitOnError)
		stdoutFlag    = flags.Bool("stdout", false, "display stdout instead of stderr")
		followFlag    = flags.Bool("f", false, "follows the logs until exec completion (full exec URI must be specified e.g. ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030/9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49)")
		reflowletFlag = flags.Bool("reflowlet", false, "if true, then streams the logs of the reflowlet")
		help          = `Logs displays logs from execs or the alloc (ie, reflowlet).

Acceptable argument types are:
` + objNameExamples + `

If the passed argument is an exec URI:
- the exec logs will be retrieved from the executor (assuming it's still accessible)
- in this case, the -f flag can also be provided to follow the exec logs until the exec completes.
- if the executor is not available, then the exec logs are streamed from cloudwatch.

If the passed argument is a hostname or alloc URI 
- if -reflowlet is set, then the logs of the reflowlet are streamed from cloudwatch.
- otherwise, it is an error

If the passed argument argument is a digest (short or long):
- the location of the exec logs is determined (using taskdb)
- then the exec logs are streamed from cloudwatch (if available, otherwise, the location of the logs is printed)

If the passed argument is an exec URI or a digest (short or long) and -reflowlet is set:
- the start/end times of the relevant exec are determined (using taskdb)
- the task URI is determined from taskdb  
- the relevant reflowlet logs (ie, around the task's execution time) are streamed from cloudwatch.
- the task's start and end time is marked (chronologically) within the streamed logs.

When printing exec logs, if -stdout is set, then the standard output of the exec is returned instead of standard error (default)
`
	)
	c.Parse(flags, args, help, "logs [-f] [-stdout] [-reflowlet] arg")
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
	switch {
	case n.Kind == execName && !*reflowletFlag:
		if err = c.execLogsFromAlloc(ctx, n, *stdoutFlag, *followFlag); err != nil {
			c.execLogsCloudWatch(ctx, execPath(n), *stdoutFlag)
		}
		return
	case (n.Kind == hostName || n.Kind == allocName):
		if *reflowletFlag {
			c.reflowletLogsCw(ctx, n.Hostname, st, et, nil)
			return
		}
		c.Printf("invalid argument '%s' (must set -reflowlet)\n%s", arg, help)
		flags.Usage()
	}
	var tdb taskdb.TaskDB
	err = c.Config.Instance(&tdb)
	if err != nil {
		c.Printf("invalid argument '%s' (requires taskdb)\n%s", arg, help)
		flags.Usage()
	}
	var task *taskdb.Task
	if task, err = c.taskFromArg(ctx, tdb, n.ID); err != nil {
		c.Fatal(err)
	}

	if !*reflowletFlag {
		d := task.Stderr
		if *stdoutFlag {
			d = task.Stdout
		}
		if !d.IsZero() {
			rlogs := c.remoteLogsFromRepo(ctx, d)
			if rlogs.Type != reflow.RemoteLogsTypeCloudwatch {
				c.Printf("log location: %s\n", rlogs)
				return
			}
			c.must(c.writeCwLogs(c.Stdout, rlogs.LogGroupName, rlogs.LogStreamName, st, et, func(w io.Writer, event *cloudwatchlogs.OutputLogEvent) {
				_, _ = fmt.Fprintln(w, *event.Message)
			}))
			return
		}
		if n, err = parseName(task.URI); err != nil {
			c.Fatalf("missing reference to logs for task '%s' and and unable to parse task URI '%s': %v", task.ID.IDShort(), task.URI, err)
		} else {
			c.execLogsCloudWatch(ctx, execPath(n), *stdoutFlag)
			return
		}
	}
	if n.Hostname == "" {
		if n, err = parseName(task.URI); err != nil {
			c.Fatalf("invalid argument '%s' (need hostname with -reflowlet) and unable to parse task URI '%s': %v\n%s", arg, task.URI, err, help)
		}
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
	cluster := c.Cluster(nil)
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
		return nil, fmt.Errorf("no tasks matched id: %v", d)
	}
	if len(tasks) > 1 {
		return nil, fmt.Errorf("more than one task matched id: %v", d)
	}
	return &tasks[0], nil
}

func (c *Cmd) remoteLogsFromRepo(ctx context.Context, d digest.Digest) (rl reflow.RemoteLogs) {
	var repo reflow.Repository
	c.must(c.Config.Instance(&repo))
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
