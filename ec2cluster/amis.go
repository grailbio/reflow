// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
// Generated from URL: https://stable.release.flatcar-linux.net/amd64-usr/3510.2.2/flatcar_production_ami_all.json
// FLATCAR_BUILD=3510
// FLATCAR_BRANCH=2
// FLATCAR_PATCH=2
// FLATCAR_VERSION=3510.2.2
// FLATCAR_VERSION_ID=3510.2.2
// FLATCAR_BUILD_ID="2023-05-29-0928"
// FLATCAR_SDK_VERSION=3510.0.0

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var flatcarAmiByRegion = map[string]string{
	"af-south-1":     "ami-01756cb8d01b26bfe",
	"ap-east-1":      "ami-054c1fe223f41098e",
	"ap-northeast-1": "ami-05a118992a9c8b247",
	"ap-northeast-2": "ami-0f9917a5a2e80fdc2",
	"ap-south-1":     "ami-0f56cf76947aaf484",
	"ap-southeast-1": "ami-0f8bd04f749c6c6e2",
	"ap-southeast-2": "ami-058c3d891b690523b",
	"ap-southeast-3": "ami-0404d57cbeb14f218",
	"ca-central-1":   "ami-054b18882f16583c5",
	"eu-central-1":   "ami-0725ab4f9ac1aefe2",
	"eu-north-1":     "ami-0ae6e87d1d9af8f80",
	"eu-south-1":     "ami-08acaf90e774b3fd2",
	"eu-west-1":      "ami-0443cbab581827b97",
	"eu-west-2":      "ami-091f97d850fcb03de",
	"eu-west-3":      "ami-0fe9d6507c8ce0265",
	"me-south-1":     "ami-0ccd234e07a72c2c6",
	"sa-east-1":      "ami-0ad7efab76469f626",
	"us-east-1":      "ami-07654ac6d13b1af1a",
	"us-east-2":      "ami-07e4ca87c0ad9a102",
	"us-west-1":      "ami-0ce58480bfda967ea",
	"us-west-2":      "ami-098a87cfe99ca95fa",
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
