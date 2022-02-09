// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
// Generated from URL: https://stable.release.flatcar-linux.net/amd64-usr/2905.2.6/flatcar_production_ami_all.json
// FLATCAR_BUILD=2905
// FLATCAR_BRANCH=2
// FLATCAR_PATCH=6
// FLATCAR_VERSION=2905.2.6
// FLATCAR_VERSION_ID=2905.2.6
// FLATCAR_BUILD_ID="2021-10-23-0726"
// FLATCAR_SDK_VERSION=2905.0.0

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var flatcarAmiByRegion = map[string]string{
	"ap-east-1":      "ami-0f41787d8b62ba403",
	"ap-northeast-1": "ami-03f8275fe0dec6ad6",
	"ap-northeast-2": "ami-007ea8b4f6ed4f6f8",
	"ap-south-1":     "ami-023c88717ca949957",
	"ap-southeast-1": "ami-0851eb4571a8db038",
	"ap-southeast-2": "ami-072e1fb64e3fe3fb1",
	"ca-central-1":   "ami-0a5780ebc95e5f0fc",
	"eu-central-1":   "ami-0a2bcb24f878c3d73",
	"eu-north-1":     "ami-04599e15f90bea263",
	"eu-west-1":      "ami-0aa2688dc724257d5",
	"eu-west-2":      "ami-03a38399892d9810c",
	"eu-west-3":      "ami-06dd2218e61f555cb",
	"me-south-1":     "ami-06b9211f214f977cf",
	"sa-east-1":      "ami-0e1c8106843ffc977",
	"us-east-1":      "ami-0b131772a73859a4d",
	"us-east-2":      "ami-0adaa453e3c7f2503",
	"us-west-1":      "ami-07e03afbd6b7c4e44",
	"us-west-2":      "ami-00e290a89aa63b273",
}

// GetAMI gets the AMI ID for the AWS region derived from the given AWS session.
func GetAMI(sess *session.Session) (string, error) {
	region := *sess.Config.Region
	ami, ok := flatcarAmiByRegion[region]
	if !ok {
		return "", fmt.Errorf("no AMI defined for region (derived from AWS session): %s", region)
	}
	return ami, nil
}
