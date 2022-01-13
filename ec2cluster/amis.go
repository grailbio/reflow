// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
// Generated from URL: https://stable.release.flatcar-linux.net/amd64-usr/2983.2.1/flatcar_production_ami_all.json
// FLATCAR_BUILD=2983
// FLATCAR_BRANCH=2
// FLATCAR_PATCH=1
// FLATCAR_VERSION=2983.2.1
// FLATCAR_VERSION_ID=2983.2.1
// FLATCAR_BUILD_ID="2021-11-19-1945"
// FLATCAR_SDK_VERSION=2983.0.0

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var flatcarAmiByRegion = map[string]string{
	"ap-east-1":      "ami-0a48fa3bbf462f0ff",
	"ap-northeast-1": "ami-050816ccedaaeb8a2",
	"ap-northeast-2": "ami-03af5432243e40991",
	"ap-south-1":     "ami-08165d837cc8ef7f6",
	"ap-southeast-1": "ami-0b6edc73dc098ba7c",
	"ap-southeast-2": "ami-0995cac81d6af43b9",
	"ca-central-1":   "ami-08a4a5aad0d8df897",
	"eu-central-1":   "ami-0aa389f6b6ee6a92f",
	"eu-north-1":     "ami-06ee80f195a37cd91",
	"eu-west-1":      "ami-0f49bf244b066dcec",
	"eu-west-2":      "ami-016dfe6a2b22b09a4",
	"eu-west-3":      "ami-004e44a5e5874fb47",
	"me-south-1":     "ami-069603c7facb1ee7b",
	"sa-east-1":      "ami-059285bd1fce28373",
	"us-east-1":      "ami-05ca28cd9e1fa05fc",
	"us-east-2":      "ami-02cf9dd33c947653a",
	"us-west-1":      "ami-0de6eeaadfc2b3e19",
	"us-west-2":      "ami-02f37fad71fba6110",
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
