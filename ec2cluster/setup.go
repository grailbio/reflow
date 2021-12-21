package ec2cluster

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/reflow/ec2cluster/instances"
	"github.com/grailbio/reflow/errors"
)

// Setup sets defaults for any unset ec2 configuration values.
func (c *Cluster) Setup(sess *session.Session) error {
	if c.DiskType == "" {
		c.DiskType = ec2.VolumeTypeGp2
	}
	if c.DiskSpace == 0 {
		c.DiskSpace = 250
	}
	if c.AMI == "" {
		c.AMI = "ami-019657181ea76e880"
	}
	if c.MaxHourlyCostUSD == 0 {
		c.MaxHourlyCostUSD = defaultMaxHourlyCostUSD
	}
	if c.MaxPendingInstances == 0 {
		c.MaxPendingInstances = defaultMaxPendingInstances
	}
	if len(c.InstanceTypes) == 0 {
		for _, instance := range instances.Types {
			c.InstanceTypes = append(c.InstanceTypes, instance.Name)
		}
	}
	if c.KeyName == "" {
		c.Log.Debug("EC2 key pair not configured")
	}
	if c.SecurityGroup == "" {
		svc := ec2.New(sess)
		var err error
		c.SecurityGroup, err = setupEC2SecurityGroup(svc)
		if err != nil {
			return err
		}
	}
	return nil
}

// securityGroup is the name of reflow's security group.
const securityGroup = "reflow"

func setupEC2SecurityGroup(svc *ec2.EC2) (string, error) {
	// First try to find an existing reflow security group.
	describeResp, err := svc.DescribeSecurityGroups(&ec2.DescribeSecurityGroupsInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("group-name"),
				Values: []*string{aws.String(securityGroup)},
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("no security group configured, and unable to query existing security groups: %v", err)
	}
	if len(describeResp.SecurityGroups) > 0 {
		id := aws.StringValue(describeResp.SecurityGroups[0].GroupId)
		log.Printf("found existing reflow security group %s\n", id)
		return id, nil
	}
	log.Println("no existing reflow security group found; creating new")
	// We are going to be launching into the default VPC, so find it.
	vpcResp, err := svc.DescribeVpcs(&ec2.DescribeVpcsInput{
		Filters: []*ec2.Filter{{
			Name:   aws.String("isDefault"),
			Values: []*string{aws.String("true")},
		}},
	})
	if err != nil {
		return "", errors.Errorf("error retrieving default VPC while creating new security group:% v", err)
	}
	if len(vpcResp.Vpcs) == 0 {
		return "", errors.New("AWS account does not have a default VPC; needs manual setup")
	} else if len(vpcResp.Vpcs) > 1 {
		// I'm not sure this is possible. But keep it as a sanity check.
		return "", errors.New("AWS account has multiple default VPCs; needs manual setup")
	}
	vpc := vpcResp.Vpcs[0]
	log.Printf("found default VPC %s", aws.StringValue(vpc.VpcId))
	resp, err := svc.CreateSecurityGroup(&ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(securityGroup),
		Description: aws.String("security group automatically created by reflow"),
		VpcId:       vpc.VpcId,
	})

	id := aws.StringValue(resp.GroupId)
	log.Printf("authorizing ingress traffic for security group %s", id)
	_, err = svc.AuthorizeSecurityGroupIngress(&ec2.AuthorizeSecurityGroupIngressInput{
		GroupName: aws.String(securityGroup),
		IpPermissions: []*ec2.IpPermission{
			// Allow all internal traffic.
			{
				IpProtocol: aws.String("-1"),
				IpRanges:   []*ec2.IpRange{{CidrIp: vpc.CidrBlock}},
				FromPort:   aws.Int64(0),
				ToPort:     aws.Int64(0),
			},
			// Allow incoming SSH connections.
			{
				IpProtocol: aws.String("tcp"),
				IpRanges:   []*ec2.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
				FromPort:   aws.Int64(22),
				ToPort:     aws.Int64(22),
			},
			// Allow incoming reflow executor connections.
			{
				IpProtocol: aws.String("tcp"),
				IpRanges:   []*ec2.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
				FromPort:   aws.Int64(9000),
				ToPort:     aws.Int64(9000),
			},
		},
	})
	if err != nil {
		return "", errors.Errorf("failed to authorize security group %s for ingress traffic: %v", id, err)
	}
	// The default egress rules are to permit all outgoing traffic.
	log.Printf("tagging security group %s", id)
	_, err = svc.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{aws.String(id)},
		Tags: []*ec2.Tag{
			{Key: aws.String("reflow-sg"), Value: aws.String("true")},
			{Key: aws.String("Name"), Value: aws.String("reflow")},
		},
	})
	if err != nil {
		log.Printf("tag security group %s: %v", id, err)
	}
	log.Printf("created security group %v", id)
	return id, nil
}
