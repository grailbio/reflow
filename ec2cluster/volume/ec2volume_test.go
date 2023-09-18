// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
)

type mockEC2Client struct {
	ec2iface.EC2API
	descVolsFn     func(volIds []string) (*ec2.DescribeVolumesOutput, error)
	descVolsModsFn func(volIds []string) (*ec2.DescribeVolumesModificationsOutput, error)
	modVolsFn      func(volId string) (*ec2.ModifyVolumeOutput, error)

	nModVols int
}

func (e *mockEC2Client) DescribeVolumesWithContext(_ aws.Context, input *ec2.DescribeVolumesInput, _ ...request.Option) (*ec2.DescribeVolumesOutput, error) {
	ids := make([]string, 0, len(input.VolumeIds))
	for _, id := range input.VolumeIds {
		ids = append(ids, aws.StringValue(id))
	}
	return e.descVolsFn(ids)
}

func (e *mockEC2Client) DescribeVolumesModificationsWithContext(_ aws.Context, input *ec2.DescribeVolumesModificationsInput, _ ...request.Option) (*ec2.DescribeVolumesModificationsOutput, error) {
	ids := make([]string, 0, len(input.VolumeIds))
	for _, id := range input.VolumeIds {
		ids = append(ids, aws.StringValue(id))
	}
	return e.descVolsModsFn(ids)
}

func (e *mockEC2Client) ModifyVolumeWithContext(_ aws.Context, input *ec2.ModifyVolumeInput, _ ...request.Option) (*ec2.ModifyVolumeOutput, error) {
	e.nModVols++
	return e.modVolsFn(aws.StringValue(input.VolumeId))
}
