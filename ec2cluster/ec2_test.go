// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
)

type mockEC2Client struct {
	ec2iface.EC2API
	DescribeInstancesInputs []*ec2.DescribeInstancesInput
}

// DescribeInstances sends the call on DescribeInstancesInputs
// and returns an empty result.
func (e *mockEC2Client) DescribeInstances(input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	e.DescribeInstancesInputs = append(e.DescribeInstancesInputs, input)
	return new(ec2.DescribeInstancesOutput), nil
}
