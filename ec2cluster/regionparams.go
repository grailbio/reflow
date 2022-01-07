package ec2cluster

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	// TODO(swami/pboyapalli): Automatically determine and use the latest Flatcar stable image instead.
	// For example, using an API call like this:
	//   cloudkey eng/dev aws ec2 describe-images --region us-west-2 --owners "075585003325" --filters "[{\"Name\":\"name\", \"Values\": [\"Flatcar-stable*\"] }, { \"Name\": \"virtualization-type\", \"Values\": [\"hvm\"]}, { \"Name\": \"architecture\", \"Values\": [\"x86_64\"]}]" --output json
	// AMI of "Flatcar-stable-2765.2.6-hvm" in each region.
	flatcarAmiByRegion = map[string]string{
		"us-east-1": "ami-0fd66875fa1ef8395",
		"us-west-2": "ami-019657181ea76e880",
	}
)

// GetAMI gets the AMI ID for the AWS region derived from the given AWS session.
func GetAMI(sess *session.Session) (string, error) {
	region := *sess.Config.Region
	ami, ok := flatcarAmiByRegion[region]
	if !ok {
		return "", fmt.Errorf("no AMI defined for region (derived from AWS session): %s", region)
	}
	return ami, nil
}
