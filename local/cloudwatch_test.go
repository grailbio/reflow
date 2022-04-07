package local

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/grailbio/base/traverse"
	"golang.org/x/time/rate"
)

type MockCloudWatchClient struct {
	cloudwatchlogsiface.CloudWatchLogsAPI
	PutLogInputs      []*cloudwatchlogs.PutLogEventsInput
	PutLogTimes       []time.Time
	CreateStreamTimes []time.Time
	PutErr            error
	CreateErr         error

	mu      sync.Mutex
	Streams map[string]cloudwatchlogs.DescribeLogStreamsOutput

	sequenceToken int
}

func NewMockCloudWatch() MockCloudWatchClient {
	return MockCloudWatchClient{
		PutLogInputs:  make([]*cloudwatchlogs.PutLogEventsInput, 0),
		Streams:       make(map[string]cloudwatchlogs.DescribeLogStreamsOutput),
		sequenceToken: 0,
	}
}

func (mc *MockCloudWatchClient) CreateLogStream(input *cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	if mc.CreateErr != nil {
		return nil, mc.CreateErr
	}
	mc.CreateStreamTimes = append(mc.CreateStreamTimes, time.Now())
	describeOutput := cloudwatchlogs.DescribeLogStreamsOutput{
		LogStreams: []*cloudwatchlogs.LogStream{{LogStreamName: input.LogStreamName}},
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.Streams[*input.LogStreamName] = describeOutput
	return nil, nil
}

func (mc *MockCloudWatchClient) DescribeLogStreams(input *cloudwatchlogs.DescribeLogStreamsInput) (*cloudwatchlogs.DescribeLogStreamsOutput, error) {
	output, ok := mc.Streams[*input.LogStreamNamePrefix]
	if ok {
		return &output, nil
	} else {
		return &cloudwatchlogs.DescribeLogStreamsOutput{}, nil
	}
}

func (mc *MockCloudWatchClient) PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error) {
	if mc.PutErr != nil {
		return nil, mc.PutErr
	}
	expectedToken := fmt.Sprint(mc.sequenceToken)
	if input.SequenceToken != nil && *(input.SequenceToken) != expectedToken {
		return nil, &cloudwatchlogs.InvalidSequenceTokenException{ExpectedSequenceToken: &expectedToken}
	}
	mc.PutLogInputs = append(mc.PutLogInputs, input)
	mc.PutLogTimes = append(mc.PutLogTimes, time.Now())
	mc.sequenceToken = mc.sequenceToken + len(input.LogEvents)
	nextToken := fmt.Sprint(mc.sequenceToken)
	output := cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken: &nextToken,
	}
	return &output, nil
}

func TestCloudWatchConcurrent(t *testing.T) {
	// Number of concurrent goroutines submitting logs
	numCallers := 10
	// Number of logs submitted per goroutine
	numLogs := 1000
	client := NewMockCloudWatch()
	cwlogs := cloudWatchLogs{client: &client, group: "test"}
	stdoutLog := cwlogs.NewStream("testprefix", stdout)
	// Test concurrent stream creation
	_ = traverse.Each(numCallers, func(i int) error {
		cwlogs.NewStream(fmt.Sprint(i), stdout)
		return nil
	})
	if got, want := len(cwlogs.streams), numCallers+1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Use a single stream to test concurrent additions to the same buffer
	err := traverse.Each(numCallers, func(i int) error {
		for j := 0; j < numLogs; j++ {
			err := stdoutLog.Output(0, "test log")
			if err != nil {
				return err
			}
			// very small sleep in between logs so that Output calls are mixed well between different callers
			time.Sleep(10 * time.Microsecond)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	_ = cwlogs.Close()

	allLogEvents := make([]*cloudwatchlogs.InputLogEvent, 0, 10000)
	for _, input := range client.PutLogInputs {
		allLogEvents = append(allLogEvents, input.LogEvents...)
	}

	if !sort.SliceIsSorted(allLogEvents, func(i, j int) bool {
		return *allLogEvents[i].Timestamp < *allLogEvents[j].Timestamp
	}) {
		t.Error("Timestamps are out of order")
	}

	// Check PutLogEvents is properly rate limited
	requestsDuration := client.PutLogTimes[len(client.PutLogTimes)-1].Sub(client.PutLogTimes[0])
	qps := float64(len(client.PutLogTimes)-1) / requestsDuration.Seconds()
	if qps > 5.0 {
		t.Error("PutLogEvents queries are not properly rate limited")
	}

	if got, want := len(allLogEvents), numCallers*numLogs; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCloudWatchCreateRateLimit(t *testing.T) {
	client := NewMockCloudWatch()
	numStreams := 5
	for i := 0; i < numStreams; i++ {
		cwlogs := newCloudWatchLogs(&client, "test")
		stdoutLog := cwlogs.NewStream(fmt.Sprintf("testprefix%d", i), stdout)
		err := stdoutLog.Output(0, "test log")
		if err != nil {
			t.Error(err)
		}
		err = cwlogs.Close()
		if err != nil {
			t.Error(err)
		}
	}
	if got, want := len(client.CreateStreamTimes), numStreams; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	requestsDuration := client.CreateStreamTimes[len(client.CreateStreamTimes)-1].Sub(client.CreateStreamTimes[0])
	qps := float64(len(client.CreateStreamTimes)-1) / requestsDuration.Seconds()
	if qps > 2.0 {
		t.Error("CreateLogStream queries are not properly rate limited")
	}
}

func TestCloudWatchLimits(t *testing.T) {
	client := NewMockCloudWatch()
	cwlogs := newCloudWatchLogs(&client, "test")
	countLog := newLogStream(cwlogs, "count")
	countLog.rateLimiter = &rate.Limiter{}
	byteLog := newLogStream(cwlogs, "byte")
	byteLog.rateLimiter = &rate.Limiter{}
	entries := make([]logEntry, 15000)
	for i := 0; i < len(entries); i++ {
		entries[i] = logEntry{"a", 0}
	}
	countLog.buffer.buffer = entries

	countLog.sendBufferToCw()
	// Two requests should have been sent
	if got, want := len(client.PutLogInputs), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(client.PutLogInputs[0].LogEvents), 10000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	countLog.sendBufferToCw()
	// Two requests should have been sent
	if got, want := len(client.PutLogInputs), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(client.PutLogInputs[1].LogEvents), 5000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	err := byteLog.Output(0, strings.Repeat("a", cwRequestMaxBytes))
	if err == nil {
		t.Errorf("exceeding byte limit with a singe log should fail")
	}

	// Two logs should be send in separate requests
	_ = byteLog.Output(0, strings.Repeat("a", cwRequestMaxBytes-26))
	_ = byteLog.Output(0, "a")
	byteLog.sendBufferToCw()
	byteLog.sendBufferToCw()
	if got, want := len(client.PutLogInputs), 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(client.PutLogInputs[2].LogEvents), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(client.PutLogInputs[3].LogEvents), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	_ = cwlogs.Close()
}

func TestCloudWatchStreamHandling(t *testing.T) {
	client := NewMockCloudWatch()
	cwlogs := newCloudWatchLogs(&client, "test")
	stdoutLog := cwlogs.NewStream("testprefix", stdout).(*cloudWatchLogsStream)
	stderrLog := cwlogs.NewStream("testprefix", stderr).(*cloudWatchLogsStream)
	stdoutMsgs := []string{"stdout 1", "stdout 2", "stdout 3", "stdout 4"}
	stderrMsgs := []string{"stdout 1", "stdout 2", "stdout 3", "stdout 4"}

	// Lazy creation, before calls to output no streams should be created yet
	if got, want := len(client.Streams), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Submit mixed stdout/stderr logs
	_ = stdoutLog.Output(0, stdoutMsgs[0])
	_ = stderrLog.Output(0, stderrMsgs[0])
	_ = stdoutLog.Output(0, stdoutMsgs[1])
	_ = stdoutLog.Output(0, stdoutMsgs[2])
	_ = stderrLog.Output(0, stderrMsgs[1])
	_ = stderrLog.Output(0, stderrMsgs[2])
	_ = stdoutLog.Output(0, stdoutMsgs[3])
	_ = stderrLog.Output(0, stderrMsgs[3])

	_ = cwlogs.Close()

	// After logging, streams should exist
	if got, want := len(client.Streams), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := client.sequenceToken, 8; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	var stdoutEvents []*cloudwatchlogs.InputLogEvent
	var stderrEvents []*cloudwatchlogs.InputLogEvent
	for _, call := range client.PutLogInputs {
		switch *call.LogStreamName {
		case "testprefix/stdout":
			stdoutEvents = append(stdoutEvents, call.LogEvents...)
		case "testprefix/stderr":
			stderrEvents = append(stderrEvents, call.LogEvents...)
		default:
			t.Fatal("Put call was made to incorrect LogStream: ", *call.LogStreamName)
		}
	}
	if got, want := cap(stderrLog.buffer.buffer), logBufferDefaultCapacity; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stdoutEvents), 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(stderrEvents), 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, event := range stdoutEvents {
		if got, want := *event.Message, stdoutMsgs[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	for i, event := range stderrEvents {
		if got, want := *event.Message, stderrMsgs[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestCloudWatchErrorHandling(t *testing.T) {
	client := NewMockCloudWatch()
	cwlogs := newCloudWatchLogs(&client, "test")
	stdoutLog := cwlogs.NewStream("err_test", stdout).(*cloudWatchLogsStream)
	stdoutLog.rateLimiter = &rate.Limiter{}
	logsDone := make(chan struct{})
	_ = stdoutLog.Output(0, strings.Repeat("a", cwRequestMaxBytes-26))
	_ = stdoutLog.Output(0, strings.Repeat("a", cwRequestMaxBytes-26))
	go func() {
		for i := 0; i < 15000; i++ {
			_ = stdoutLog.Output(0, "a")
		}
		close(logsDone)
	}()
	// Lazy creation, before calls to output no streams should be created yet
	if got, want := len(client.Streams), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	client.CreateErr = &cloudwatchlogs.ServiceUnavailableException{}
	stdoutLog.sendBufferToCw()
	if got, want := len(client.PutLogInputs), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	client.CreateErr = nil
	<-logsDone
	// Current log buffer dropped after non-retryable error
	client.PutErr = &cloudwatchlogs.InvalidParameterException{}
	stdoutLog.sendBufferToCw()
	if got, want := len(client.PutLogInputs), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	client.PutErr = &cloudwatchlogs.ServiceUnavailableException{}
	// Re-add to buffer for 2 retries
	for i := 0; i < 2; i++ {
		stdoutLog.sendBufferToCw()
		if got, want := len(stdoutLog.buffer.buffer), 15001; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	// Then finally drop after 3th attempt
	stdoutLog.sendBufferToCw()

	if got, want := len(stdoutLog.buffer.buffer), 15000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// First success
	client.PutErr = nil
	stdoutLog.sendBufferToCw()
	// After successful log, stream should exist
	if got, want := len(client.Streams), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := stdoutLog.buffer.sizeBytes, 5000*(cwLogEntryOverheadBytes+1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Invalid Sequence token. Should recover with expected token.
	badToken := "1"
	stdoutLog.token = &badToken
	stdoutLog.sendBufferToCw()
	_ = cwlogs.Close()
	if got, want := len(client.PutLogInputs), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	allLogEvents := make([]*cloudwatchlogs.InputLogEvent, 0, 15000)
	for _, input := range client.PutLogInputs {
		allLogEvents = append(allLogEvents, input.LogEvents...)
	}
	if got, want := len(allLogEvents), 15000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !sort.SliceIsSorted(allLogEvents, func(i, j int) bool {
		return *allLogEvents[i].Timestamp < *allLogEvents[j].Timestamp
	}) {
		t.Error("Timestamps are out of order")
	}
	// Test panic occurs if you try to create a stream after closing
	defer func() {
		if r := recover(); r == nil {
			t.Error("should panic when creating new stream after close")
		}
	}()
	cwlogs.NewStream("testprefix", stderr)
}
