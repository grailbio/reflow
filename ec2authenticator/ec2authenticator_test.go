// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2authenticator

import "testing"

func TestRegexp(t *testing.T) {
	for _, ok := range []string{
		"012345678910.dkr.ecr.us-west-2.amazonaws.com/ubuntu",
		"012345678910.dkr.ecr.us-west-2.amazonaws.com/amazonlinux",
		"012345678910.dkr.ecr.us-west-2.amazonaws.com/windows_sample_app",
		"012345678910.dkr.ecr.us-west-2.amazonaws.com/wgs:v2",
	} {
		if !ecrURI.MatchString(ok) {
			t.Errorf("expected match for %s", ok)
		}
	}

	for _, notok := range []string{
		"ubuntu",
		"alpine/linux",
		"monkey.org/docker/blah",
		"xyz012345678910.dkr.ecr.us-west-2.amazonaws.com/amazonlinux",
	} {
		if ecrURI.MatchString(notok) {
			t.Errorf("did not expect match for %s", notok)
		}
	}
}
