// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/tool"
)

func setupDynamoDBAssoc(c *tool.Cmd, ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("setup-dynamodb-assoc", flag.ExitOnError)
	var (
		writeCap = flags.Int("writecap", 10, "dynamodb provisioned write capacity")
		readCap  = flags.Int("readcap", 20, "dynamodb provisioned read capacity")
	)
	help := `Setup-dynamodb-assoc provisions a table in AWS's DynamoDB service and
modifies Reflow's configuration to use this table as its assoc.

By default the DynamoDB table is configured with a provisoned
capacity of 10 writes/sec and 20 reads/sec. This can be overriden
with the flags -writecap and -readcap, or else modified through the
AWS console after configuration.

The resulting configuration can be examined with "reflow config"`
	c.Parse(flags, args, help, "setup-dynamodb-assoc tablename")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	table := flags.Arg(0)

	b, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		c.Fatal(err)
	}
	base := make(config.Base)
	if err := config.Unmarshal(b, base.Keys()); err != nil {
		c.Fatal(err)
	}
	v, _ := base[config.Assoc].(string)
	if v != "" {
		parts := strings.Split(v, ",")
		if len(parts) != 2 || parts[0] != "dynamodb" || parts[1] != table {
			c.Fatalf("assoc already set up: %v", v)
		}
		c.Log.Printf("assoc already set up; updating schemas")
	}
	sess, err := c.Config.AWS()
	if err != nil {
		c.Fatal(err)
	}

	c.Log.Printf("creating DynamoDB table %s", table)
	db := dynamodb.New(sess)
	_, err = db.CreateTable(&dynamodb.CreateTableInput{
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
	} else {
		c.Log.Printf("created DynamoDB table %s", table)
	}
	const indexName = "ID4-ID-index"
	var describe *dynamodb.DescribeTableOutput
	start := time.Now()
	for {
		describe, err = db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			c.Fatal(err)
		}
		status := *describe.Table.TableStatus
		if status == "ACTIVE" {
			break
		}

		if time.Since(start) > time.Minute {
			c.Fatal("waited for table to become active for too long; try again later")
		}
		c.Log.Printf("waiting for table to become active; current status: %v", status)
		time.Sleep(4 * time.Second)
	}
	var exists bool
	for _, index := range describe.Table.GlobalSecondaryIndexes {
		if *index.IndexName == indexName {
			exists = true
			break
		}
	}
	if exists {
		c.Log.Printf("dynamodb index %s already exists", indexName)
	} else {
		// Create a secondary index to look up keys by their ID4-prefix.
		_, err = db.UpdateTable(&dynamodb.UpdateTableInput{
			TableName: aws.String(table),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("ID"),
					AttributeType: aws.String("S"),
				},
				// DynamoDB has to know about the attribute type to index it
				{
					AttributeName: aws.String("ID4"),
					AttributeType: aws.String("S"),
				},
			},
			GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
				{
					Create: &dynamodb.CreateGlobalSecondaryIndexAction{
						IndexName: aws.String(indexName),
						KeySchema: []*dynamodb.KeySchemaElement{
							{
								KeyType:       aws.String("HASH"),
								AttributeName: aws.String("ID4"),
							},
							{
								KeyType:       aws.String("RANGE"),
								AttributeName: aws.String("ID"),
							},
						},
						Projection: &dynamodb.Projection{
							ProjectionType: aws.String("ALL"),
						},
						ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(int64(*readCap)),
							WriteCapacityUnits: aws.Int64(int64(*writeCap)),
						},
					},
				},
			},
		})
		if err != nil {
			c.Fatalf("error creating secondary index: %v", err)
		}
		c.Log.Printf("created secondary index %s", indexName)
	}

	base[config.Assoc] = fmt.Sprintf("dynamodb,%s", table)
	b, err = config.Marshal(base)
	if err != nil {
		c.Fatal(err)
	}
	if err := ioutil.WriteFile(c.ConfigFile, b, 0666); err != nil {
		c.Fatal(err)
	}
}
