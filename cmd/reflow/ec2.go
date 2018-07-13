// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/tool"
	"github.com/pkg/errors"
)

// securityGroup is the name of reflow's security group.
const securityGroup = "reflow"

func setupEC2(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-ec2", flag.ExitOnError)
	sshkey := flags.String("sshkey", os.ExpandEnv("$HOME/.ssh/id_rsa.pub"), "install this public SSH key on EC2 nodes")
	help := `Setup-ec2 modifies Reflow's configuration to use Reflow's cluster
manager to compute on an EC2 cluster. 

Reflow is configured to launch new instances in the default VPC of 
the user's AWS account. A new security group named "reflow" is 
provisioned if necessary. The security group permits the following
ingress traffic:

	port 9000 source 0.0.0.0/0 9000
	port 22 source 0.0.0.0/0 9000
	
The former port is used for reflowlet RPC; the latter to permit users
to SSH into the EC2 instances for debugging.

The cluster is configured to install the user's SSH keys (flag
-sshkey, $HOME/.ssh/id_rsa.pub by default).

The resulting configuration can be examined with "reflow config".`
	c.Parse(flags, args, help, "setup-ec2")
	if flags.NArg() != 0 {
		flags.Usage()
	}

	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	base := make(config.Base)
	if err := config.Unmarshal(b, base.Keys()); err != nil {
		c.Fatal(err)
	}
	v, _ := base[config.Cluster].(string)
	if v == "" {
		base[config.Cluster] = "ec2cluster"
	} else if v != "ec2cluster" {
		c.Fatalf("cluster is already configured: %v", v)
	}
	// We also need to set up an HTTPS CA since the cluster is useless without it.
	// Default to a file-based CA.
	v, _ = base[config.HTTPS].(string)
	if v == "" {
		path := filepath.Join(filepath.Dir(c.ConfigFile), "reflow.pem")
		base[config.HTTPS] = fmt.Sprintf("httpsca,%s", path)
	}

	cfg := &ec2cluster.Config{Config: base}
	if err := cfg.Init(); err != nil {
		c.Fatal(err)
	}

	if cfg.DiskType == "" {
		cfg.DiskType = "gp2"
	}
	if cfg.DiskSpace == 0 {
		cfg.DiskSpace = 250
	}
	if cfg.AMI == "" {
		cfg.AMI = "ami-4296ec3a"
	}
	if cfg.MaxInstances == 0 {
		cfg.MaxInstances = 10
	}
	if len(cfg.InstanceTypes) == 0 {
		cfg.InstanceTypes = []string{
			"c1.medium",
			"c1.xlarge",
			"c3.2xlarge",
			"c3.4xlarge",
			"c3.8xlarge",
			"c3.large",
			"c3.xlarge",
			"c4.2xlarge",
			"c4.4xlarge",
			"c4.8xlarge",
			"c4.large",
			"c4.xlarge",
			"c5.large",
			"c5.xlarge",
			"c5.2xlarge",
			"c5.4xlarge",
			"c5.9xlarge",
			"c5.18xlarge",
			"cc2.8xlarge",
			"m1.large",
			"m1.medium",
			"m1.small",
			"m1.xlarge",
			"m2.2xlarge",
			"m2.4xlarge",
			"m2.xlarge",
			"m3.2xlarge",
			"m3.large",
			"m3.medium",
			"m3.xlarge",
			"m4.16xlarge",
			"m4.4xlarge",
			"m4.xlarge",
			"r4.xlarge",
			"t1.micro",
			"t2.large",
			"t2.medium",
			"t2.micro",
			"t2.nano",
			"t2.small",
		}
	}
	if cfg.KeyName == "" {
		c.Log.Debug("EC2 key pair not configured")
	}
	if cfg.Region == "" {
		cfg.Region = "us-west-2"
	}
	if cfg.SshKey == "" {
		b, err := ioutil.ReadFile(*sshkey)
		if err == nil {
			cfg.SshKey = string(b)
		} else {
			c.Log.Errorf("error reading SSH key: %v", err)
		}
	}

	if cfg.SecurityGroup == "" {
		sess, err := c.Config.AWS()
		if err != nil {
			c.Fatal(err)
		}
		svc := ec2.New(sess)
		cfg.SecurityGroup, err = setupEC2SecurityGroup(c, svc)
		if err != nil {
			c.Fatal(err)
		}
	}
	b, err = config.Marshal(cfg)
	if err != nil {
		c.Fatal(err)
	}

	configPath := filepath.Dir(c.ConfigFile)
	if err := os.MkdirAll(configPath, 0766); err != nil {
		c.Fatal(err)
	}

	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}

func setupEC2SecurityGroup(c *tool.Cmd, svc *ec2.EC2) (string, error) {
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
		c.Log.Debugf("found existing reflow security group %s\n", id)
		return id, nil
	}
	c.Errorln("no existing reflow security group found; creating new")
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
	c.Log.Debugf("found default VPC %s", aws.StringValue(vpc.VpcId))
	resp, err := svc.CreateSecurityGroup(&ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(securityGroup),
		Description: aws.String("security group automatically created by reflow"),
		VpcId:       vpc.VpcId,
	})

	id := aws.StringValue(resp.GroupId)
	c.Log.Debugf("authorizing ingress traffic for security group %s", id)
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
	c.Log.Debugf("tagging security group %s", id)
	_, err = svc.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{aws.String(id)},
		Tags: []*ec2.Tag{
			{Key: aws.String("reflow-sg"), Value: aws.String("true")},
			{Key: aws.String("Name"), Value: aws.String("reflow")},
		},
	})
	if err != nil {
		c.Log.Errorf("tag security group %s: %v", id, err)
	}
	c.Log.Printf("created security group %v", id)
	return id, nil
}
