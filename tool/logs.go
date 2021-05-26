// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"encoding/json"
	"flag"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/taskdb"
)

func (c *Cmd) logs(ctx context.Context, args ...string) {
	var (
		flags      = flag.NewFlagSet("logs", flag.ExitOnError)
		stdoutFlag = flags.Bool("stdout", false, "display stdout instead of stderr")
		followFlag = flags.Bool("f", false, "follows the logs until exec completion (full exec URI must be specified e.g. ec2-35-165-199-174.us-west-2.compute.amazonaws.com:9000/bb97e35db4101030/9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49)")
		streamFlag = flags.Bool("s", false, "if true, streams the logs from cloudwatch, otherwise just prints the location (default)")
		help       = `Logs displays logs from execs.

If the passed argument is an exec URI, then the logs will be retrieved from the executor (assuming it's still accessible). 
In this case, the -f flag can also be provided to follow the logs until the exec completes.

Otherwise, if the argument is an exec ID, then the location of logs is retrieved from taskdb.
In this case, the -s flag can be provided to stream the logs from cloudwatch.
`
	)
	c.Parse(flags, args, help, "logs [-f] [-stdout] exec")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	arg := flags.Arg(0)
	var tdb taskdb.TaskDB
	err := c.Config.Instance(&tdb)
	if err != nil || tdb == nil {
		n, err := parseName(arg)
		c.must(err)
		if n.Kind != execName {
			c.Fatalf("%s: not an exec URI", arg)
		}
		cluster := c.Cluster(nil)
		alloc, err := cluster.Alloc(ctx, n.AllocID)
		if err != nil {
			c.Fatalf("alloc %s: %s", n.AllocID, err)
		}
		exec, err := alloc.Get(ctx, n.ID)
		if err != nil {
			c.Fatalf("%s: %s", n.ID, err)
		}
		rc, err := exec.Logs(ctx, *stdoutFlag, !*stdoutFlag, *followFlag)
		if err != nil {
			c.Fatalf("logs %s: %s", exec.URI(), err)
		}
		_, err = io.Copy(c.Stdout, rc)
		rc.Close()
		c.must(err)
		return
	}

	d, err := reflow.Digester.Parse(arg)
	if err == nil {
		q := taskdb.TaskQuery{ID: taskdb.TaskID(d)}
		tasks, err := tdb.Tasks(ctx, q)
		c.must(err)
		if len(tasks) == 0 {
			c.Fatalf("no tasks matched id: %v", d.String())
		}
		if len(tasks) > 1 {
			c.Fatalf("more than one task matched id: %v", d.String())
		}
		var repo reflow.Repository
		err = c.Config.Instance(&repo)
		if err != nil {
			log.Fatal("repository: ", err)
		}
		var d digest.Digest
		if *stdoutFlag {
			d = tasks[0].Stdout
		} else {
			d = tasks[0].Stderr
		}
		var rc io.ReadCloser
		rc, err = repo.Get(ctx, d)
		if err != nil {
			log.Fatalf("repository get %s: %v", d.Short(), err)
		}
		var rlogs reflow.RemoteLogs
		if derr := json.NewDecoder(rc).Decode(&rlogs); derr != nil {
			c.Fatal("failed to decode remote logs location: ", derr)
		}
		if *streamFlag && rlogs.Type == reflow.RemoteLogsTypeCloudwatch {
			c.pullCwLogs(rlogs)
		} else {
			c.Printf("log location: %s\n", rlogs)
			if rlogs.Type == reflow.RemoteLogsTypeCloudwatch {
				c.Println("to view log contents, re-run with the '-s' flag (see 'reflow logs -help') or use the AWS CLI's get-log-events tool")
			}
		}
		rc.Close()
		c.must(err)
		return
	}
	n, err := parseName(arg)
	c.must(err)
	if n.Kind != execName {
		c.Fatalf("%s: not an exec URI", arg)
	}
	rc, err := c.execLogs(ctx, *stdoutFlag, *followFlag, n)
	if err != nil {
		c.Fatalf("logs %s: %s", arg, err)
	}
	_, err = io.Copy(c.Stdout, rc)
	rc.Close()
	c.must(err)
}

func (c *Cmd) pullCwLogs(rlogs reflow.RemoteLogs) {
	log.Printf("streaming logs from: %s", rlogs.String())
	var awsSession *session.Session
	c.must(c.Config.Instance(&awsSession))
	cl := cloudwatchlogs.New(awsSession)
	var nextToken *string // intentionally nil for the first call
	for {
		out, err := cl.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(rlogs.LogGroupName),
			LogStreamName: aws.String(rlogs.LogStreamName),
			StartFromHead: aws.Bool(true),
			NextToken:     nextToken,
		})
		if err != nil {
			c.Fatal("failed to get log events from cloudwatch: ", err)
		}
		// AWS indicates that we have reached the end of the stream by returning the same
		// token that was passed in.
		if aws.StringValue(nextToken) == aws.StringValue(out.NextForwardToken) {
			return
		}
		nextToken = out.NextForwardToken
		for _, m := range out.Events {
			c.Println(*m.Message)
		}
	}
}
