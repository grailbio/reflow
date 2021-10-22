package local

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

type MockCloudWatchClient struct {
	cloudwatchlogsiface.CloudWatchLogsAPI
	PutLogInputs  []*cloudwatchlogs.PutLogEventsInput
	Streams       map[string]cloudwatchlogs.DescribeLogStreamsOutput
	sequenceToken int
	wg            *sync.WaitGroup
}

func NewMockCloudWatch() MockCloudWatchClient {
	return MockCloudWatchClient{
		PutLogInputs:  make([]*cloudwatchlogs.PutLogEventsInput, 0),
		Streams:       make(map[string]cloudwatchlogs.DescribeLogStreamsOutput),
		sequenceToken: 0,
		wg:            nil,
	}
}

func (mc *MockCloudWatchClient) CreateLogStream(input *cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	describeOutput := cloudwatchlogs.DescribeLogStreamsOutput{
		LogStreams: []*cloudwatchlogs.LogStream{{LogStreamName: input.LogStreamName}},
	}
	mc.Streams[*input.LogStreamName] = describeOutput
	return nil, nil
}

func (mc MockCloudWatchClient) DescribeLogStreams(input *cloudwatchlogs.DescribeLogStreamsInput) (*cloudwatchlogs.DescribeLogStreamsOutput, error) {
	output, ok := mc.Streams[*input.LogStreamNamePrefix]
	if ok {
		return &output, nil
	} else {
		return &cloudwatchlogs.DescribeLogStreamsOutput{}, nil
	}
}

func (mc *MockCloudWatchClient) PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error) {
	mc.PutLogInputs = append(mc.PutLogInputs, input)
	mc.sequenceToken = mc.sequenceToken + len(input.LogEvents)
	nextToken := fmt.Sprint(mc.sequenceToken)
	output := cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken: &nextToken,
	}
	if mc.wg != nil {
		mc.wg.Done()
	}
	return &output, nil
}

func TestCloudWatchStreamHandling(t *testing.T) {
	client := NewMockCloudWatch()
	group := "test"
	cwlogs, err := newCloudWatchLogs(&client, group)
	if err != nil {
		t.Error(err)
	}
	stdoutLog, err := cwlogs.NewStream("testprefix", stdout)
	if err != nil {
		t.Error(err)
	}
	stderrLog, err := cwlogs.NewStream("testprefix", stderr)
	if err != nil {
		t.Error(err)
	}

	stdoutMsgs := []string{"stdout 1", "stdout 2", "stdout 3", "stdout 4"}
	stderrMsgs := []string{"stdout 1", "stdout 2", "stdout 3", "stdout 4"}

	// Lazy creation, before calls to output no streams should be created yet
	if got, want := len(client.Streams), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	var wg sync.WaitGroup
	wg.Add(8)
	client.wg = &wg

	// Submit mixed stdout/stderr logs
	for i := 0; i < 4; i++ {
		_ = stdoutLog.Output(0, stdoutMsgs[i])
		_ = stderrLog.Output(0, stderrMsgs[i])
	}

	wg.Wait()

	// After logging, streams should exist
	if got, want := len(client.Streams), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	cwlogs.Close()

	// Should be 8 calls to PutLogEvents
	if got, want := len(client.PutLogInputs), 8; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := client.sequenceToken, 8; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
