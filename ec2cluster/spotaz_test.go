package ec2cluster

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func TestComputeAzSubnetMap(t *testing.T) {
	client := &mockEC2Client{descSubnetOut: &ec2.DescribeSubnetsOutput{
		Subnets: []*ec2.Subnet{
			{AvailabilityZone: aws.String("abc"), SubnetId: aws.String("abc-id1")},
			{AvailabilityZone: aws.String("def"), SubnetId: aws.String("def-id1")},
		},
	}}
	azNameToSubnetOnce.Reset()
	if err := computeAzSubnetMap(client, []string{ /* doesn't matter for test */ }, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := subnetForAZ("abc"), "abc-id1"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got, want := subnetForAZ("def"), "def-id1"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	client = &mockEC2Client{descSubnetOut: &ec2.DescribeSubnetsOutput{
		Subnets: []*ec2.Subnet{
			{AvailabilityZone: aws.String("abc"), SubnetId: aws.String("abc-id1")},
			{AvailabilityZone: aws.String("def"), SubnetId: aws.String("def-id1")},
			{AvailabilityZone: aws.String("abc"), SubnetId: aws.String("abc-id2")},
		},
	}}

	azNameToSubnetOnce.Reset()
	if err := computeAzSubnetMap(client, []string{ /* doesn't matter for test */ }, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := subnetForAZ("abc"), "abc-id2"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got, want := subnetForAZ("def"), "def-id1"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
