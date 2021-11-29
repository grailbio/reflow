// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"golang.org/x/time/rate"
)

// TODO(pgopal) - Put this code into a separate package and inject the
// implementation via Config.

type streamType string

const (
	stdout streamType = "stdout"
	stderr            = "stderr"
)

// remoteStream is an interface to log to cloud. It must be closed once all the streams are done.
// Calls to Output on a stream after this is closed will return an error.
type remoteStream interface {
	io.Closer
	// NewStream creates a new stream for logging. Creating two remote
	// loggers that write to the same output stream is undefined.
	NewStream(string, streamType) remoteLogsOutputter
}

type logEntry struct {
	msg         string
	timestampMs int64
}

// cloudWatchLogs implements a client that can stream logs to Amazon CloudWatch
// Logs.
type cloudWatchLogs struct {
	client cloudwatchlogsiface.CloudWatchLogsAPI
	group  string

	streamsMu sync.Mutex
	streams   []*cloudWatchLogsStream
	closed    bool
}

// newCloudWatchLogs creates a new remote logger client for AWS CloudWatchLogs.
// The remoteLogger client can be used to create a new stream and log to them.
// newCloudWatchLogs expects that the given cloudwatch logs group name already exists.
func newCloudWatchLogs(client cloudwatchlogsiface.CloudWatchLogsAPI, group string) *cloudWatchLogs {
	return &cloudWatchLogs{client: client, group: group}
}

// createStream creates a LogStream in cloudwatch if it does not yet exist.
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

// NewStream creates new stream with the given stream prefix and type.
func (c *cloudWatchLogs) NewStream(prefix string, sType streamType) remoteLogsOutputter {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	if c.closed {
		panic("calling NewStream after closing")
	}
	stream := newLogStream(c, prefix+"/"+string(sType))
	c.streams = append(c.streams, stream)
	return stream
}

func (c *cloudWatchLogs) Close() error {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	_ = traverse.Each(len(c.streams), func(i int) error {
		c.streams[i].Close()
		return nil
	})
	c.closed = true
	return nil
}

type remoteLogsOutputter interface {
	log.Outputter

	// RemoteLogs returns the remote log description.
	RemoteLogs() reflow.RemoteLogs
	Close()
}

type cloudWatchLogsStream struct {
	cwl *cloudWatchLogs

	// buffer for storing logEntries for later PutLogEvents requests
	buffer *cloudWatchBuffer

	// rateLimiter is applied to PutLogEvents requests
	rateLimiter *rate.Limiter

	quit int32
	wg   sync.WaitGroup

	created  bool
	closed   bool
	attempts int
	name     string
	token    *string
}

// newLogStream creates the underlying object which puts incoming logs to cloudwatch, respecting the rate limit.
func newLogStream(cwl *cloudWatchLogs, name string) *cloudWatchLogsStream {
	stream := cloudWatchLogsStream{
		cwl:         cwl,
		name:        name,
		rateLimiter: rate.NewLimiter(rate.Every(time.Second), 4), // 4qps
		buffer:      newCloudWatchBuffer(),
	}
	stream.wg.Add(1)
	go stream.loop()
	return &stream
}

// putLogs sends the provided logs to cloudwatch via a PutLogEvents request
func (s *cloudWatchLogsStream) putLogs(logs []logEntry) (err error) {
	events := make([]*cloudwatchlogs.InputLogEvent, len(logs))
	for i, entry := range logs {
		events[i] = &cloudwatchlogs.InputLogEvent{
			Message:   aws.String(entry.msg),
			Timestamp: aws.Int64(entry.timestampMs),
		}
	}
	if !s.created {
		if err = s.cwl.createStream(s.name); err != nil {
			return s.handleAwsErr("CreateLogStream", err)
		}
		s.created = true
	}
	response, err := s.cwl.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(s.cwl.group),
		LogStreamName: aws.String(s.name),
		SequenceToken: s.token,
	})
	if err != nil {
		return s.handleAwsErr("PutLogEvents", err)
	} else {
		s.token = response.NextSequenceToken
	}
	return
}

// handleAwsErr returns a reflow representation of the provided AWS error. If the error is
// InvalidSequenceTokenException, this method also sets this stream's token accordingly.
func (s *cloudWatchLogsStream) handleAwsErr(op string, err error) error {
	var kind errors.Kind
	if terr, ok := err.(*cloudwatchlogs.InvalidSequenceTokenException); ok {
		s.token = terr.ExpectedSequenceToken
		kind = errors.Temporary
	} else if _, ok := err.(*cloudwatchlogs.ServiceUnavailableException); ok {
		kind = errors.Unavailable
	}
	return errors.E(op, err, kind)
}

// sendBufferToCw clears logs from the buffer and sends them to CloudWatch. If the request fails with a retryable error
// or a token error, logs are returned to the buffer for future attempts.
func (s *cloudWatchLogsStream) sendBufferToCw() {
	if logs, szB := s.buffer.Clear(); len(logs) > 0 {
		if err := s.putLogs(logs); err != nil {
			s.attempts++
			// If we have a retryable error, add the logs to the front of the buffer. Limit of 3 retries.
			if errors.Transient(err) && s.attempts < 3 {
				log.Errorf("Re-adding logs to buffer after retryable error: %v", err)
				s.buffer.Add(true, szB, logs...)
				return
			} else if s.attempts == 3 {
				log.Errorf("Dropping %d logs after 3 attempts: %v", len(logs), err)
			} else {
				log.Errorf("Dropping %d logs after unrecoverable error: %v", len(logs), err)
			}
		}
		// Attempts count is reset after successful putLogs, or after dropping logs.
		s.attempts = 0
	}
}

// loop loops forever and uploads logs from the buffer to cloudwatch.
func (s *cloudWatchLogsStream) loop() {
	defer s.wg.Done()
	for 0 == atomic.LoadInt32(&s.quit) {
		if s.rateLimiter.Allow() {
			s.sendBufferToCw()
		}
	}
	// On close we attempt one last clear. Logs may be dropped if we cannot send all remaining logs within cloudwatch limits
	if logs, _ := s.buffer.Clear(); len(logs) > 0 {
		if err := s.putLogs(logs); err != nil {
			log.Error(err)
		}
	}
}

// Output writes the contents of msg to cloudwatch via a buffer.
func (s *cloudWatchLogsStream) Output(calldepth int, msg string) error {
	if atomic.LoadInt32(&s.quit) == 1 {
		return errors.New("cannot call Output on a closed stream")
	}
	msgBytes := len(msg) + cwLogEntryOverheadBytes
	if msgBytes > cwRequestMaxBytes {
		return errors.New("log is too large to send to cloudwatch")
	}
	s.buffer.AddLogEntry(msg)
	return nil
}

func (s *cloudWatchLogsStream) RemoteLogs() reflow.RemoteLogs {
	return reflow.RemoteLogs{
		Type:          reflow.RemoteLogsTypeCloudwatch,
		LogGroupName:  s.cwl.group,
		LogStreamName: s.name,
	}
}

func (s *cloudWatchLogsStream) Close() {
	if atomic.CompareAndSwapInt32(&s.quit, 0, 1) {
		s.wg.Wait()
	}
}

const (
	// These constants are based on cloudwatch request limits.
	// See https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html

	// The maximum batch size is 1,048,576 bytes. This size is calculated as the sum of all event messages in UTF-8,
	// plus 26 bytes for each log event.
	cwLogEntryOverheadBytes int = 26
	cwRequestMaxBytes       int = 1_048_576
	// Maximum number of log entries per batch
	cwRequestMaxEntries      int = 10000
	logBufferDefaultCapacity int = 1024
)

// cloudWatchBuffer provides operations on the underlying buffer while tracking the buffer's total size in bytes
type cloudWatchBuffer struct {
	mu        sync.Mutex
	buffer    []logEntry
	sizeBytes int
}

func newCloudWatchBuffer() *cloudWatchBuffer {
	return &cloudWatchBuffer{
		buffer: make([]logEntry, 0, logBufferDefaultCapacity),
	}
}

// AddLogEntry creates a new log entry and adds it to the buffer. Timestamp must be set after the buffer mutex is
// acquired to preserve chronological order.
func (b *cloudWatchBuffer) AddLogEntry(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	newEntry := logEntry{msg, time.Now().UnixNano() / 1000000}
	b.add(false, cwLogEntryOverheadBytes+len(msg), newEntry)
}

// add either appends or prepends log entries to the buffer, updating the buffer size.
// Must be called while `b.mu` is held.
func (b *cloudWatchBuffer) add(prepend bool, szB int, logs ...logEntry) {
	if prepend {
		b.buffer = append(logs, b.buffer...)
	} else {
		b.buffer = append(b.buffer, logs...)
	}
	b.sizeBytes += szB
}

// Add adds existing log entries in bulk to the buffer
func (b *cloudWatchBuffer) Add(prepend bool, szB int, logs ...logEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.add(prepend, szB, logs...)
}

// Clear empties logs from the buffer for a PutLogEvents request. If the buffer is too large for a single request,
// this will empty logs up to Cloudwatch's single request limit. Returns cleared logs along with their buffer size.
func (b *cloudWatchBuffer) Clear() ([]logEntry, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) == 0 {
		return nil, 0
	}
	// Clear as many logs as we can, respecting the cloudwatch limits on number of logs and request size
	nBuffer := len(b.buffer)
	if nBuffer > cwRequestMaxEntries {
		nBuffer = cwRequestMaxEntries
	}
	// Count how many logs we can send under the cloudwatch request bytes limit
	var requestBytes int
	for i := 0; i < nBuffer; i++ {
		reqBytes := requestBytes + len(b.buffer[i].msg) + cwLogEntryOverheadBytes
		if reqBytes > cwRequestMaxBytes {
			nBuffer = i
			break
		}
		requestBytes = reqBytes
	}
	bLen := len(b.buffer)
	logs := make([]logEntry, nBuffer)
	copy(logs, b.buffer[:nBuffer])
	copy(b.buffer, b.buffer[nBuffer:])
	b.buffer = b.buffer[:(bLen - nBuffer)]
	b.sizeBytes -= requestBytes
	return logs, requestBytes
}
