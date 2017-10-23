package local

import (
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/grailbio/reflow/log"
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
	NewStream(string, streamType) (log.Outputter, error)
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
	stream string
	buffer chan logEntry
}

// NewCloudWatchLogs creates a new remote logger client for Amazon
// CloudWatchLogs. The remoteLogger client can be used to create a new stream
// and log to them.
func newCloudWatchLogs(client cloudwatchlogsiface.CloudWatchLogsAPI, group string) (remoteStream, error) {
	cwl := &cloudWatchLogs{client: client, group: group}
	_, err := cwl.client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(group)})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || ok && aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
			return nil, err
		}
	}
	cwl.buffer = make(chan logEntry, 1024)
	cwl.loop()
	return cwl, nil
}

// NewStream creates new stream with the given stream prefix and type.
func (c *cloudWatchLogs) NewStream(prefix string, sType streamType) (log.Outputter, error) {
	stream := &cloudWatchLogsStream{
		client: c,
		name:   prefix + "/" + string(sType),
	}
	_, err := c.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(stream.client.group),
		LogStreamName: aws.String(stream.name),
	})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
			return nil, err
		}
	}
	return stream, nil
}

func (c *cloudWatchLogs) Close() error {
	close(c.buffer)
	return nil
}

func (c *cloudWatchLogs) loop() {
	go func() {
		sequenceToken := make(map[string]*string)
		for entry := range c.buffer {
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
			}
			sequenceToken[entry.stream] = response.NextSequenceToken
		}
	}()
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
