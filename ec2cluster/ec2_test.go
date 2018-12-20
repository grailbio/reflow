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
	output *ec2.DescribeInstancesOutput
}

// DescribeInstances returns e.output as DescribeInstancesOutput.
func (e *mockEC2Client) DescribeInstances(input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	o := e.output
	if o != nil {
		return o, nil
	}
	return nil, fmt.Errorf("must set return value before call to mockEC2Client.DescribeInstances")
}

// DescribeInstances returns e.output as DescribeInstancesOutput.
func (e *mockEC2Client) DescribeInstancesWithContext(ctx aws.Context, input *ec2.DescribeInstancesInput, _ ...request.Option) (*ec2.DescribeInstancesOutput, error) {
	return e.DescribeInstances(input)
}
