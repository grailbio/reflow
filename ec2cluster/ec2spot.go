// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

var notFoundRegex = regexp.MustCompile(`The spot instance request ID '(.*)' does not exist`)

// descSpotBatchApi implements limiter.BatchApi for AWS API `DescribeSpotInstanceRequests`.
type descSpotBatchApi struct {
	maxPerBatch int
	api         ec2iface.EC2API
	log         *log.Logger
}

// MaxPerBatch implements limiter.BatchApi
func (a *descSpotBatchApi) MaxPerBatch() int { return a.maxPerBatch }

// Do implements limiter.BatchApi
func (a *descSpotBatchApi) Do(m map[limiter.ID]*limiter.Result) {
	ids := make([]string, 0, len(m))
	spotIdsToID := make(map[string]limiter.ID)
	for id := range m {
		spotId := id.(string)
		ids = append(ids, spotId)
		spotIdsToID[spotId] = id
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := a.api.DescribeSpotInstanceRequestsWithContext(ctx, &ec2.DescribeSpotInstanceRequestsInput{
		SpotInstanceRequestIds: aws.StringSlice(ids)})
	if err == nil {
		for _, sir := range resp.SpotInstanceRequests {
			spotId := aws.StringValue(sir.SpotInstanceRequestId)
			id := spotIdsToID[spotId]
			m[id].Set(sir, nil)
		}
		return
	}
	switch {
	case awsErrorCode(err) == "InvalidSpotInstanceRequestID.NotFound":
		if matches := notFoundRegex.FindStringSubmatch(err.Error()); len(matches) == 2 {
			badId := matches[1]
			a.log.Errorf("DescribeSpotInstanceRequests %s: %v", badId, err)
			id := spotIdsToID[badId]
			m[id].Set(nil, err)
		}
	default:
		a.log.Errorf("DescribeSpotInstanceRequests (%d IDs): %v", len(ids), err)
		for _, r := range m {
			r.Set(nil, err)
		}
	}
	return
}

// waitUntilFulfilled waits until the given spot instance request is fulfilled.
// It is expected that this is a newly initiated spot instance request and hence
// it starts with an initial delay and uses a very liberal retry policy and a long timeout.
// The retry policy used is mostly similar to what's used in EC2API.WaitUntilSpotInstanceRequestFulfilled, except
// it starts with an initial wait of 10s (compared to 15s) and uses backoff and a small jitter.
func waitUntilFulfilled(ctx context.Context, l *limiter.BatchLimiter, spotId string, log *log.Logger) (*ec2.SpotInstanceRequest, error) {
	// A single spot instance request hitting consistency errors results in an error for the whole batch.
	// Since we just created the spot instance, we'll sleep for a bit to allow for the SpotID to propagate within AWS.
	// Due to the eventual consistency model, this helps us reduce the likelihood of hitting consistency errors.
	// See: https://docs.aws.amazon.com/AWSEC2/latest/APIReference/query-api-troubleshooting.html#eventual-consistency
	time.Sleep(2 * time.Second)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var policy = retry.MaxRetries(retry.Jitter(retry.Backoff(10*time.Second, 30*time.Second, 1.2), 0.2), 40)
	sir, err := pollSpotRequest(ctx, l, policy, spotId, log, func(sir *ec2.SpotInstanceRequest) bool {
		terminal, _ := interpretStatusCode(sir)
		return terminal
	})
	if err != nil {
		return nil, errors.E(fmt.Sprintf("waitUntilFulfilled %s", spotId), err)
	}
	if _, fulfilled := interpretStatusCode(sir); !fulfilled {
		return nil, errors.E(fmt.Sprintf("waitUntilFulfilled %s failed: status %s", spotId, *sir.Status.Code))
	}
	return sir, nil
}

var spotReqStatusPolicy = retry.MaxRetries(retry.Backoff(2*time.Second, 5*time.Second, 1.5), 5)

// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-request-status.html
func spotReqStatus(ctx context.Context, l *limiter.BatchLimiter, spotID string, log *log.Logger) (id, state string, err error) {
	sir, err := pollSpotRequest(ctx, l, spotReqStatusPolicy, spotID, log, func(sir *ec2.SpotInstanceRequest) bool {
		if state := aws.StringValue(sir.State); state == ec2.SpotInstanceStateCancelled || state == ec2.SpotInstanceStateClosed {
			return true
		}
		if iid := aws.StringValue(sir.InstanceId); iid != "" {
			return true
		}
		return false
	})
	if err != nil {
		return "", "", err
	}
	return getInstanceIdStateFromSir(sir)
}

func getInstanceIdStateFromSir(sir *ec2.SpotInstanceRequest) (string, string, error) {
	iid, state := aws.StringValue(sir.InstanceId), aws.StringValue(sir.State)
	switch {
	case state == ec2.SpotInstanceStateCancelled:
		// Cancelled state returns error (could be due to expiry)
		return iid, state, fmt.Errorf("expired or canceled before fulfillment")
	case state == ec2.SpotInstanceStateClosed:
		// Closed state returns error
		return iid, state, fmt.Errorf("closed, could indicate an aws system error")
	}
	return iid, state, nil
}

func pollSpotRequest(ctx context.Context, l *limiter.BatchLimiter, policy retry.Policy, spotId string, log *log.Logger, accept func(*ec2.SpotInstanceRequest) bool) (*ec2.SpotInstanceRequest, error) {
	var (
		sir               *ec2.SpotInstanceRequest
		statusCode, state string
	)
	for retries := 0; ; retries++ {
		r, err := l.Do(ctx, spotId)
		if err == nil {
			sir = r.(*ec2.SpotInstanceRequest)
			state = aws.StringValue(sir.State)
			if sir.Status != nil {
				statusCode = aws.StringValue(sir.Status.Code)
			}
		}
		switch {
		case err != nil && err == limiter.ErrNoResult:
			log.Debugf("batch limiter (attempt %d): %v", retries, err)
		case err != nil && request.IsErrorRetryable(err):
			// Upon retryable errors, we will retry, so nothing to do here.
			log.Debugf("retryable (attempt %d): %v", retries, err)
		case err != nil && request.IsErrorThrottle(err):
			// Upon throttling errors, we will retry, so nothing to do here.
			log.Debugf("throttled (attempt %d): %v", retries, err)
		case awsErrorCode(err) == "InvalidSpotInstanceRequestID.NotFound":
		case err != nil:
			// Upon any other error, we return the error
			return nil, err
		case accept(sir) == true:
			return sir, nil
		}
		log.Debugf("pollSpotRequest state: %s status.Code: %s (attempt %d)", state, statusCode, retries)
		if err = retry.Wait(ctx, policy, retries); err != nil {
			return nil, errors.E(spotId, fmt.Sprintf("missing instance ID (after %d retries)", retries))
		}
	}
}

// interpretStatusCode interprets the status code (if any) from the given spot instance request.
// This interpretation is based on AWS EC2 documentation:
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-request-status.html
func interpretStatusCode(sir *ec2.SpotInstanceRequest) (terminal, fulfilled bool) {
	if sir.Status == nil {
		return
	}
	switch *sir.Status.Code {
	case "fulfilled", "request-canceled-and-instance-running":
		terminal, fulfilled = true, true
	case "canceled-before-fulfillment", "schedule-expired", "bad-parameters", "system-error":
		terminal = true
	}
	return
}

// awsErrorCode is the AWS error code if the given error is an AWS error.
func awsErrorCode(err error) (code string) {
	if aerr, ok := err.(awserr.Error); ok {
		code = aerr.Code()
	}
	return
}
