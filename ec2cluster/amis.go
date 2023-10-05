// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
// Generated from URL: https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_ami_all.json
// FLATCAR_BUILD=3510
// FLATCAR_BRANCH=2
// FLATCAR_PATCH=8
// FLATCAR_VERSION=3510.2.8
// FLATCAR_VERSION_ID=3510.2.8
// FLATCAR_BUILD_ID="2023-09-19-1914"
// FLATCAR_SDK_VERSION=3510.0.0

package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var flatcarAmiByRegion = map[string]string{
	"af-south-1":     "ami-018d9b9d7151ab6c8",
	"ap-east-1":      "ami-0a20232670e0772af",
	"ap-northeast-1": "ami-0233879a9dd92df11",
	"ap-northeast-2": "ami-01bff27d84a7a7700",
	"ap-south-1":     "ami-0cc2acda59cd3af78",
	"ap-southeast-1": "ami-0c42cc418ecd92604",
	"ap-southeast-2": "ami-01691d179b95f6f8c",
	"ap-southeast-3": "ami-0dfb599c8fc910601",
	"ca-central-1":   "ami-09dc356370f64513d",
	"eu-central-1":   "ami-068829f5bf9333501",
	"eu-north-1":     "ami-0a560c9c18e52fe3a",
	"eu-south-1":     "ami-0b069d1752bac2e31",
	"eu-west-1":      "ami-0e28f7527ce1dd0d1",
	"eu-west-2":      "ami-03f8f46c0d8a86c5c",
	"eu-west-3":      "ami-0328a3887119e6dd3",
	"me-south-1":     "ami-0f34884e3fbf09f49",
	"sa-east-1":      "ami-0151f43dfd95a3865",
	"us-east-1":      "ami-0604fd4b5a31a9e9f",
	"us-east-2":      "ami-0a804cb708f395c8a",
	"us-west-1":      "ami-0ba006ff9272db530",
	"us-west-2":      "ami-0c3488073a14ab0af",
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
