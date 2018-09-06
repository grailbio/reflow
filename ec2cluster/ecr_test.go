// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
)

type mockECRClient struct {
	ecriface.ECRAPI

	Err error // If set, calls to DescribeImages() will return this error.
}

// DescribeImages the call on DescribeInstancesInputs
// and returns an empty result.
func (e *mockECRClient) DescribeImages(input *ecr.DescribeImagesInput) (*ecr.DescribeImagesOutput, error) {
	if e.Err != nil {
		return nil, e.Err
	}
	return new(ecr.DescribeImagesOutput), nil
}
