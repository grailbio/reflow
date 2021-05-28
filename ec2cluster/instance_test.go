package ec2cluster

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/retry"
	rlog "github.com/grailbio/reflow/log"
	"golang.org/x/time/rate"
)

func TestConfigureEBS(t *testing.T) {
	type ebsInfo struct {
		ebsType string
		ebsSize uint64
		nebs    int
	}
	for _, tc := range []struct {
		e, want ebsInfo
	}{
		{ebsInfo{"st1", 0, 0}, ebsInfo{"st1", 500, 1}},
		{ebsInfo{"st1", 100, 2}, ebsInfo{"st1", 500, 1}},
		{ebsInfo{"st1", 501, 2}, ebsInfo{"st1", 1000, 2}},
		{ebsInfo{"st1", 1300, 2}, ebsInfo{"st1", 1300, 2}},
		{ebsInfo{"gp2", 0, 0}, ebsInfo{"gp2", 1, 1}},
		{ebsInfo{"gp2", 10, 5}, ebsInfo{"gp2", 10, 5}},
		{ebsInfo{"gp2", 500, 4}, ebsInfo{"gp2", 500, 4}},
		{ebsInfo{"gp2", 1200, 5}, ebsInfo{"gp2", 1336, 4}},
		{ebsInfo{"gp2", 2200, 10}, ebsInfo{"gp2", 2338, 7}},
	} {
		i := &instance{EBSType: tc.e.ebsType, EBSSize: tc.e.ebsSize, NEBS: tc.e.nebs}
		i.configureEBS()
		got := ebsInfo{i.EBSType, i.EBSSize, i.NEBS}
		if got != tc.want {
			t.Errorf("given %v: got %v, want %v", tc.e, got, tc.want)
		}
	}
}

type counter struct {
	nextId int
}

func (c *counter) next(prefix string) string {
	id := fmt.Sprintf("%s-%d", prefix, c.nextId)
	c.nextId += 1
	return id
}

func TestCancelSpot(t *testing.T) {
	spotReqStatusPolicy = retry.MaxRetries(retry.Backoff(10*time.Millisecond, 100*time.Millisecond, 1.2), 5)
	var c counter
	for _, state := range []string{
		ec2.CancelSpotInstanceRequestStateActive,
		ec2.CancelSpotInstanceRequestStateCancelled,
		ec2.CancelSpotInstanceRequestStateClosed,
		ec2.CancelSpotInstanceRequestStateCompleted,
		ec2.CancelSpotInstanceRequestStateOpen,
	} {
		// Add spot instance request with every possible instance state
		for _, instState := range []string{
			"", // this case will take a long time to run due to retries in ec2SpotRequestStatus
			ec2.InstanceStateNamePending,
			ec2.InstanceStateNameRunning,
			ec2.InstanceStateNameShuttingDown,
			ec2.InstanceStateNameStopped,
			ec2.InstanceStateNameStopping,
			ec2.InstanceStateNameTerminated,
		} {
			state := state
			instState := instState
			t.Run("spot:\""+state+"\",instance:\""+instState+"\"", func(t *testing.T) {
				t.Parallel()
				tcMsg := fmt.Sprintf("sir state: %s, instanceState: %s", state, instState)
				sirId := c.next("sir")
				client := &mockSirClient{sirId: sirId, state: state}
				if instState != "" {
					client.instanceId = c.next("instance")
					client.instState = instState
				}
				testLogger := rlog.New(log.New(os.Stderr, "", log.LstdFlags), rlog.DebugLevel)
				l := limiter.NewBatchLimiter(&descSpotBatchApi{api: client, log: testLogger}, rate.NewLimiter(rate.Every(time.Millisecond), 10))
				ec2CleanupSpotRequest(context.Background(), l, client, sirId, testLogger)
				if got, want := client.state, ec2.SpotInstanceStateCancelled; got != want {
					t.Errorf("got %v, want %v for %s", got, want, tcMsg)
				}
				if instState != "" {
					if got, want := client.instState, ec2.InstanceStateNameTerminated; got != want {
						t.Errorf("got %v, want %v for %s", got, want, tcMsg)
					}
				}
			})
		}
	}
}
