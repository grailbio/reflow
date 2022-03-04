// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
)

type mockEC2Client struct {
	ec2iface.EC2API
	descInstOut   *ec2.DescribeInstancesOutput
	descSubnetOut *ec2.DescribeSubnetsOutput
}

// DescribeInstances returns e.descInstOut as DescribeInstancesOutput.
func (e *mockEC2Client) DescribeInstances(input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	o := e.descInstOut
	if o != nil {
		return o, nil
	}
	return nil, fmt.Errorf("must set return value before call to mockEC2Client.DescribeInstances")
}

// DescribeSubnets returns e.descSubnetOut as DescribeSubnetsOutput.
func (e *mockEC2Client) DescribeSubnets(input *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error) {
	o := e.descSubnetOut
	if o != nil {
		return o, nil
	}
	return nil, fmt.Errorf("must set return value before call to mockEC2Client.DescribeSubnets")
}

// DescribeInstances returns e.output as DescribeInstancesOutput.
func (e *mockEC2Client) DescribeInstancesWithContext(ctx aws.Context, input *ec2.DescribeInstancesInput, _ ...request.Option) (*ec2.DescribeInstancesOutput, error) {
	return e.DescribeInstances(input)
}

type mockSirClient struct {
	ec2iface.EC2API
	sirId, state          string
	instanceId, instState string
}

func (e *mockSirClient) DescribeSpotInstanceRequestsWithContext(ctx aws.Context, input *ec2.DescribeSpotInstanceRequestsInput, opts ...request.Option) (*ec2.DescribeSpotInstanceRequestsOutput, error) {
	return &ec2.DescribeSpotInstanceRequestsOutput{SpotInstanceRequests: []*ec2.SpotInstanceRequest{
		{SpotInstanceRequestId: aws.String(e.sirId), State: aws.String(e.state), InstanceId: aws.String(e.instanceId)},
	}}, nil
}

func (e *mockSirClient) CancelSpotInstanceRequestsWithContext(ctx aws.Context, input *ec2.CancelSpotInstanceRequestsInput, opts ...request.Option) (*ec2.CancelSpotInstanceRequestsOutput, error) {
	e.state = ec2.SpotInstanceStateCancelled
	return &ec2.CancelSpotInstanceRequestsOutput{CancelledSpotInstanceRequests: []*ec2.CancelledSpotInstanceRequest{
		{SpotInstanceRequestId: aws.String(e.sirId), State: aws.String(e.state)},
	}}, nil
}

func (e *mockSirClient) TerminateInstancesWithContext(ctx aws.Context, input *ec2.TerminateInstancesInput, opts ...request.Option) (*ec2.TerminateInstancesOutput, error) {
	prev := e.instState
	e.instState = ec2.InstanceStateNameTerminated
	return &ec2.TerminateInstancesOutput{TerminatingInstances: []*ec2.InstanceStateChange{
		{
			PreviousState: &ec2.InstanceState{Name: aws.String(prev)},
			CurrentState:  &ec2.InstanceState{Name: aws.String(e.instState)},
		},
	}}, nil
}
