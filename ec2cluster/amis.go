// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
// Generated from URL: https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_ami_all.json
// FLATCAR_BUILD=3510
// FLATCAR_BRANCH=2
// FLATCAR_PATCH=5
// FLATCAR_VERSION=3510.2.5
// FLATCAR_VERSION_ID=3510.2.5
// FLATCAR_BUILD_ID="2023-07-14-1811"
// FLATCAR_SDK_VERSION=3510.0.0

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var flatcarAmiByRegion = map[string]string{
	"af-south-1":     "ami-01ded80cef2a8b702",
	"ap-east-1":      "ami-05eafa28a49149c88",
	"ap-northeast-1": "ami-0e27551679539c187",
	"ap-northeast-2": "ami-048558bf5c43223bf",
	"ap-south-1":     "ami-03173eb5a8a664f51",
	"ap-southeast-1": "ami-0d49cd7850cd4e306",
	"ap-southeast-2": "ami-043218f72cdd8c1ca",
	"ap-southeast-3": "ami-0b18cf0b853dd34a7",
	"ca-central-1":   "ami-0c75d660afb2ce0e5",
	"eu-central-1":   "ami-06a4b805512e7caa6",
	"eu-north-1":     "ami-0e141bde238df6f0f",
	"eu-south-1":     "ami-0ad3bbfae97cb8383",
	"eu-west-1":      "ami-034d01cf9569b17fc",
	"eu-west-2":      "ami-08e43fa9e4eff7967",
	"eu-west-3":      "ami-0593d47b422b91ece",
	"me-south-1":     "ami-0446f28e8b1a8ae13",
	"sa-east-1":      "ami-0dbd802e72d0b653d",
	"us-east-1":      "ami-02bdfedc6e535b409",
	"us-east-2":      "ami-0d9b9ed6da2b86087",
	"us-west-1":      "ami-0d7bfbd54e938866b",
	"us-west-2":      "ami-0286d11545f844f93",
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
