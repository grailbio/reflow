// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
)

// descInstBatchApi implements limiter.BatchApi for AWS API `DescribeInstances`.
type descInstBatchApi struct {
	maxPerBatch int
	api         ec2iface.EC2API
	log         *log.Logger
}

// MaxPerBatch implements limiter.BatchApi
func (a *descInstBatchApi) MaxPerBatch() int { return a.maxPerBatch }

// Do implements limiter.BatchApi
func (a *descInstBatchApi) Do(m map[limiter.ID]*limiter.Result) {
	ids := make([]string, 0, len(m))
	idsToID := make(map[string]limiter.ID)
	for id := range m {
		iid := id.(string)
		ids = append(ids, iid)
		idsToID[iid] = id
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := a.api.DescribeInstancesWithContext(ctx, &ec2.DescribeInstancesInput{InstanceIds: aws.StringSlice(ids)})
	if err != nil {
		a.log.Errorf("DescribeInstances (%d ids): %v", len(ids), err)
		for _, r := range m {
			r.Set(nil, err)
		}
		return
	}
	for _, res := range resp.Reservations {
		for _, inst := range res.Instances {
			iid := aws.StringValue(inst.InstanceId)
			id := idsToID[iid]
			m[id].Set(inst, nil)
		}
	}
	return
}

func describeInstance(ctx context.Context, l *limiter.BatchLimiter, id string, log *log.Logger) (*ec2.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var policy = retry.MaxRetries(retry.Jitter(retry.Backoff(time.Second, 2*time.Second, 1.2), 0.7), 5)
	return pollinstance(ctx, l, policy, id, log)
}

// waitUntilRunning waits until an instance is in the running state.  It is expected that this is newly
// started instance and hence this uses a very liberal retry policy and a long timeout.
// The retry policy used is mostly similar to what's used in EC2API.WaitUntilInstanceRunning, except
// it starts with an initial wait of 10s (compared to 15s) and uses backoff and a small jitter.
func waitUntilRunning(ctx context.Context, l *limiter.BatchLimiter, id string, log *log.Logger) (*ec2.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var policy = retry.MaxRetries(retry.Jitter(retry.Backoff(10*time.Second, 30*time.Second, 1.2), 0.2), 40)
	return pollinstance(ctx, l, policy, id, log)
}

// stillRunning determines if an instance previously known to be in a running state is still running.
// The distinction is key because this uses a conservative retry policy and timeout.
func stillRunning(ctx context.Context, l *limiter.BatchLimiter, id string, log *log.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var policy = retry.MaxRetries(retry.Jitter(retry.Backoff(time.Second, 5*time.Second, 2), 0.7), 3)
	_, err := pollinstance(ctx, l, policy, id, log)
	return err
}

func pollinstance(ctx context.Context, l *limiter.BatchLimiter, policy retry.Policy, id string, log *log.Logger) (*ec2.Instance, error) {
	var (
		inst  *ec2.Instance
		state string
	)
	for retries := 0; ; retries++ {
		r, err := l.Do(ctx, id)
		if err == nil {
			inst = r.(*ec2.Instance)
			state = *inst.State.Name
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
		case awsErrorCode(err) == "InvalidInstanceID.NotFound":
		case err != nil:
			// Upon any other error, we return the error
			return inst, err
		case state == ec2.InstanceStateNameRunning:
			return inst, nil
		case state == ec2.InstanceStateNamePending:
		default:
			return inst, fmt.Errorf("waitUntilRunning: instance state %s (after %d retries)", state, retries)
		}
		if rerr := retry.Wait(ctx, policy, retries); rerr != nil {
			return inst, fmt.Errorf("waitUntilRunning: instance state %s (after %d retries)", state, retries)
		}
		log.Debugf("pollinstance state: %s (attempt %d)", state, retries)
	}
}
