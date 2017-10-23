package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/tool"
)

func setupS3Cache(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-s3-cache", flag.ExitOnError)
	var (
		writeCap = flags.Int("writecap", 10, "dynamodb provisioned write capacity")
		readCap  = flags.Int("readcap", 20, "dynamodb provisioned read capacity")
	)
	help := `Setup-s3 provisions resources on AWS in order to set up and configure
a distributed cache for Reflow. The modified configuration is written back to the
configuration file.

A bucket with the provided name is created (if it does not already
exist); a DynamoDB table with the provided name is also instantiated
(if it does not already exist). By default the DynamoDB table is
configured with a provisoned capacity of 10 writes/sec and 20
reads/sec. This can be overriden with the flags -writecap and
-readcap, or else modified through the AWS console after
configuration.

The resulting configuration can be examined with "reflow config"`
	c.Parse(flags, args, help, "setup-s3-cache s3bucket dynamoDBtable")
	if flags.NArg() != 2 {
		flags.Usage()
	}
	bucket, table := flags.Arg(0), flags.Arg(1)

	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	base := make(config.Base)
	if err := config.Unmarshal(b, base.Keys()); err != nil {
		c.Fatal(err)
	}
	v, _ := base[config.Cache].(string)
	if v != "" {
		c.Fatalf("cache already set up: %v", v)
	}
	sess, err := c.Config.AWS()
	if err != nil {
		c.Fatal(err)
	}
	region, err := c.Config.AWSRegion()
	if err != nil {
		c.Fatal(err)
	}
	c.Log.Printf("creating s3 bucket %s", bucket)
	_, err = s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				c.Fatalf("s3 bucket %s is already owned by someone else", bucket)
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				c.Log.Printf("s3 bucket %s already exists; not created", bucket)
			default:
				c.Fatal(err)
			}
		} else {
			c.Fatal(err)
		}
	} else {
		c.Log.Printf("created s3 bucket %s", bucket)
	}
	_, err = dynamodb.New(sess).CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(*readCap)),
			WriteCapacityUnits: aws.Int64(int64(*writeCap)),
		},
		TableName: aws.String(table),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != dynamodb.ErrCodeResourceInUseException {
			log.Fatal(err)
		}
		c.Log.Printf("dynamodb table %s already exists", table)
	}

	base[config.Cache] = fmt.Sprintf("s3,%s,%s", bucket, table)
	b, err = config.Marshal(base)
	if err != nil {
		c.Fatal(err)
	}
	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}
