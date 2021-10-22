// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"errors"
	"io"
	"time"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

// TODO(pgopal) - Put this code into a separate package and inject the
// implementation via Config.

var errDropped = errors.New("dropped log message: buffer full")

type streamType string

const (
	stdout streamType = "stdout"
	stderr            = "stderr"
)

// remoteStream is an interface to log to cloud but must be closed once all
// the streams are done. Closing before all the streams are done writing
// can cause panic.
type remoteStream interface {
	io.Closer
	// NewStream creates a new stream for logging. Creating two remote
	// loggers that write to the same output stream is undefined.
	NewStream(string, streamType) (remoteLogsOutputter, error)
}

type logEntry struct {
	stream string
	msg    string
}

// cloudWatchLogs implements a client that can stream logs to Amazon CloudWatch
// Logs.
type cloudWatchLogs struct {
	client cloudwatchlogsiface.CloudWatchLogsAPI
	group  string
	buffer chan logEntry
}

// newCloudWatchLogs creates a new remote logger client for AWS CloudWatchLogs.
// The remoteLogger client can be used to create a new stream and log to them.
// newCloudWatchLogs expects that the given cloudwatch logs group name already exists.
func newCloudWatchLogs(client cloudwatchlogsiface.CloudWatchLogsAPI, group string) (remoteStream, error) {
	cwl := &cloudWatchLogs{client: client, group: group}
	cwl.buffer = make(chan logEntry, 1024)
	cwl.loop()
	return cwl, nil
}

// NewStream creates new stream with the given stream prefix and type.
func (c *cloudWatchLogs) NewStream(prefix string, sType streamType) (remoteLogsOutputter, error) {
	stream := &cloudWatchLogsStream{client: c, name: prefix + "/" + string(sType)}
	return stream, nil
}

func (c *cloudWatchLogs) createStream(name string) error {
	out, err := c.client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(c.group),
		LogStreamNamePrefix: aws.String(name),
	})
	if err == nil {
		var found bool
		for _, s := range out.LogStreams {
			if found = aws.StringValue(s.LogStreamName) == name; found {
				break
			}
		}
		if found {
			return nil
		}
	}
	_, err = c.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(c.group),
		LogStreamName: aws.String(name),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
			err = nil
		}
	}
	return err
}

func (c *cloudWatchLogs) Close() error {
	close(c.buffer)
	return nil
}

func (c *cloudWatchLogs) loop() {
	go func() {
		sequenceToken := make(map[string]*string)
		for entry := range c.buffer {
			// If this is the first log for this stream, create the stream in cloudwatch first
			if _, ok := sequenceToken[entry.stream]; !ok {
				err := c.createStream(entry.stream)
				if err != nil {
					log.Errorf("Failed to create log stream: %v", err)
				}
			}
			event := []*cloudwatchlogs.InputLogEvent{{
				Message:   aws.String(entry.msg),
				Timestamp: aws.Int64(time.Now().UnixNano() / 1000000),
			}}
			response, err := c.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
				LogEvents:     event,
				LogGroupName:  aws.String(c.group),
				LogStreamName: aws.String(entry.stream),
				SequenceToken: sequenceToken[entry.stream],
			})
			if err != nil {
				log.Errorf("cloudWatchLogs.PutLogEvent: %v", err)
			} else {
				sequenceToken[entry.stream] = response.NextSequenceToken
			}
		}
	}()
}

type remoteLogsOutputter interface {
	log.Outputter

	// RemoteLogs returns the remote log description.
	RemoteLogs() reflow.RemoteLogs
}

type cloudWatchLogsStream struct {
	client *cloudWatchLogs
	name   string
}

// Output writes the contents of s to cloudwatchlogs via a buffer.
// If the buffer is full, logs are dropped on the floor.
func (s *cloudWatchLogsStream) Output(calldepth int, msg string) error {
	select {
	case s.client.buffer <- logEntry{s.name, msg}:
		return nil
	default:
		return errDropped
	}
}

func (s *cloudWatchLogsStream) RemoteLogs() reflow.RemoteLogs {
	return reflow.RemoteLogs{
		Type:          reflow.RemoteLogsTypeCloudwatch,
		LogGroupName:  s.client.group,
		LogStreamName: s.name,
	}
}
