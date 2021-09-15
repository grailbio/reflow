// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dynamodbtask

import (
	"strings"
	"time"

	"github.com/grailbio/reflow/repository/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

type indexdefs struct {
	attrdefs  []*dynamodb.AttributeDefinition
	keyschema []*dynamodb.KeySchemaElement
}

var indexes = map[string]*indexdefs{
	id4Index: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(colID),
				AttributeType: aws.String("S"),
			},
			// DynamoDB has to know about the attribute type to index it
			{
				AttributeName: aws.String(colID4),
				AttributeType: aws.String("S"),
			},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(colID4),
			},
			{
				KeyType:       aws.String("RANGE"),
				AttributeName: aws.String(colID),
			},
		},
	},
	dateKeepaliveIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(colDate),
				AttributeType: aws.String("S"),
			},
			// DynamoDB has to know about the attribute type to index it
			{
				AttributeName: aws.String(colKeepalive),
				AttributeType: aws.String("S"),
			},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(colDate),
			},
			{
				KeyType:       aws.String("RANGE"),
				AttributeName: aws.String(colKeepalive),
			},
		},
	},
	runIDIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(colRunID),
				AttributeType: aws.String("S"),
			},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(colRunID),
			},
		},
	},
	imgCmdIDIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(colImgCmdID),
				AttributeType: aws.String("S"),
			},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(colImgCmdID),
			},
		},
	},
	identIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(colIdent),
				AttributeType: aws.String("S"),
			},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(colIdent),
			},
		},
	},
}

// Setup implements infra.Provider.
func (t *TaskDB) Setup(sess *session.Session, log *log.Logger) error {
	tableName, bucketName := t.TableNameFlagsTrait.TableName, t.BucketNameFlagsTrait.BucketName
	log.Printf("attempting to create DynamoDB table %s", tableName)
	db := dynamodb.New(sess)
	if err := createTable(db, tableName, log); err != nil {
		return err
	}
	describe, err := waitForActiveTable(db, tableName, log)
	if err != nil {
		return err
	}
	indexExists := make(map[string]bool)
	for _, index := range describe.Table.GlobalSecondaryIndexes {
		if _, ok := indexes[*index.IndexName]; ok {
			indexExists[*index.IndexName] = true
		}
	}
	exists := true
	for name := range indexes {
		if !indexExists[name] {
			exists = false
		}
	}
	if exists {
		var keys []string
		for k := range indexes {
			keys = append(keys, k)
		}
		log.Printf("dynamodb indexes [%s] already exist", strings.Join(keys, ","))
		return nil
	}
	for index, config := range indexes {
		if indexExists[index] {
			continue
		}
		input := &dynamodb.UpdateTableInput{
			TableName:            aws.String(tableName),
			AttributeDefinitions: config.attrdefs,
			GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
				{
					Create: &dynamodb.CreateGlobalSecondaryIndexAction{
						IndexName: aws.String(index),
						KeySchema: config.keyschema,
						Projection: &dynamodb.Projection{
							ProjectionType: aws.String("ALL"),
						},
					},
				},
			},
		}
		_, err = db.UpdateTable(input)
		if err != nil {
			return errors.E("error creating secondary index: %v", err)
		}
		log.Printf("created secondary index %s", index)
		// dynamodb allows only one index creation at a time. We have to wait until the
		// table becomes active before we can create the next index.
		_, err := waitForActiveTable(db, tableName, log)
		if err != nil {
			return err
		}
	}
	return s3.CreateS3Bucket(sess, bucketName, log)
}

func createTable(db dynamodbiface.DynamoDBAPI, tableName string, log *log.Logger) error {
	_, err := db.CreateTable(&dynamodb.CreateTableInput{
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
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName:   aws.String(tableName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != dynamodb.ErrCodeResourceInUseException {
			return errors.E("TaskDB.Setup", err)
		}
		log.Printf("dynamodb table %s already exists", tableName)
	} else {
		log.Printf("created DynamoDB table %s", tableName)
	}
	return nil
}

func waitForActiveTable(db dynamodbiface.DynamoDBAPI, table string, log *log.Logger) (*dynamodb.DescribeTableOutput, error) {
	var describe *dynamodb.DescribeTableOutput
	start := time.Now()
	var err error
	for {
		describe, err = db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			return nil, err
		}
		status := *describe.Table.TableStatus
		if status != "ACTIVE" {
			log.Printf("waiting for table to become active; current status: %v", status)
		} else {
			active := true
			for _, i := range describe.Table.GlobalSecondaryIndexes {
				if *i.IndexStatus != "ACTIVE" {
					log.Printf("waiting for index %v to become active; current status: %v", *i.IndexName, *i.IndexStatus)
					active = false
				}
			}
			if active {
				break
			}
		}
		if time.Since(start) > 10*time.Minute {
			return nil, errors.New("waited for table/indexes to become active for too long; try again later")
		}
		time.Sleep(20 * time.Second)
	}
	return describe, nil
}
