// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
// Generated from URL: https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_ami_all.json
// FLATCAR_BUILD=3510
// FLATCAR_BRANCH=2
// FLATCAR_PATCH=6
// FLATCAR_VERSION=3510.2.6
// FLATCAR_VERSION_ID=3510.2.6
// FLATCAR_BUILD_ID="2023-08-07-1626"
// FLATCAR_SDK_VERSION=3510.0.0

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var flatcarAmiByRegion = map[string]string{
	"af-south-1":     "ami-0f65cba92b090d9bd",
	"ap-east-1":      "ami-05ae6f5ed5e180227",
	"ap-northeast-1": "ami-098fdc56ca0e74c4e",
	"ap-northeast-2": "ami-0059a10861a323893",
	"ap-south-1":     "ami-0129ec8fa007ccbc9",
	"ap-southeast-1": "ami-030e74a8aa78cea2a",
	"ap-southeast-2": "ami-0e870aed71e91b6aa",
	"ap-southeast-3": "ami-021ee199ab285a2d8",
	"ca-central-1":   "ami-05078bc9a3e5a87fc",
	"eu-central-1":   "ami-01e8d5553e3d74da1",
	"eu-north-1":     "ami-0caa27870781d0253",
	"eu-south-1":     "ami-07fe82f740deec21b",
	"eu-west-1":      "ami-0eb5c17b8ff82a90b",
	"eu-west-2":      "ami-0fa82a03ecfd4f048",
	"eu-west-3":      "ami-0bd47dc70046f0dd8",
	"me-south-1":     "ami-01ca41a5c57a95869",
	"sa-east-1":      "ami-077c11aaefc590fe2",
	"us-east-1":      "ami-0f22961936c6c0405",
	"us-east-2":      "ami-0aa7c49092f6ff964",
	"us-west-1":      "ami-0973cb1190384f029",
	"us-west-2":      "ami-0af1032ee364853f0",
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
