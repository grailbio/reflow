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
	projection *dynamodb.Projection
}

// TODO(swami): Remove older indices once we've migrated fully to the new indices.
// This will be a two-step process:
// - First we release code which can create and use the new indices.
// - Once we've released and migrated all the indices, we'll have to uncomment the following
//   so that in a subsequent release, we can remove the older indices.
var indexesToRemove []string // []string{imgCmdIDIndex, identIndex}

var indexes = map[string]*indexdefs{
	idIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(colID),
				AttributeType: aws.String("S"),
			},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(colID),
			},
		},
		projection: &dynamodb.Projection{ProjectionType: aws.String("ALL")},
	},
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
		projection: &dynamodb.Projection{ProjectionType: aws.String("ALL")},
	},
	dateKeepaliveIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String(colDate), AttributeType: aws.String("S")},
			// DynamoDB has to know about the attribute type to index it
			{AttributeName: aws.String(colKeepalive), AttributeType: aws.String("S")},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{KeyType: aws.String("HASH"), AttributeName: aws.String(colDate)},
			{KeyType: aws.String("RANGE"), AttributeName: aws.String(colKeepalive)},
		},
		projection: &dynamodb.Projection{ProjectionType: aws.String("ALL")},
	},
	runIDIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String(colRunID), AttributeType: aws.String("S")},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{KeyType: aws.String("HASH"), AttributeName: aws.String(colRunID)},
		},
		projection: &dynamodb.Projection{ProjectionType: aws.String("ALL")},
	},
	// Deprecated:  The following indices are deprecated and no longer added to new tables.
	// But they remain here as a reference to their definitions.
	// TODO(swami): Remove these after migration is complete.
	//imgCmdIDIndex: {
	//	attrdefs: []*dynamodb.AttributeDefinition{
	//		{ AttributeName: aws.String(colImgCmdID), AttributeType: aws.String("S") },
	//	},
	//	keyschema: []*dynamodb.KeySchemaElement{
	//		{ KeyType:       aws.String("HASH"), AttributeName: aws.String(colImgCmdID) },
	//	},
	//},
	//identIndex: {
	//	attrdefs: []*dynamodb.AttributeDefinition{
	//		{ AttributeName: aws.String(colIdent), AttributeType: aws.String("S") },
	//	},
	//	keyschema: []*dynamodb.KeySchemaElement{
	//		{ KeyType:       aws.String("HASH"), AttributeName: aws.String(colIdent) },
	//	},
	//},
	imgCmdIDSortEndIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String(colImgCmdID), AttributeType: aws.String("S")},
			// DynamoDB has to know about the attribute type to index it
			{AttributeName: aws.String(colEndTime), AttributeType: aws.String("S")},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{KeyType: aws.String("HASH"), AttributeName: aws.String(colImgCmdID)},
			{KeyType: aws.String("RANGE"), AttributeName: aws.String(colEndTime)},
		},
		projection: &dynamodb.Projection{
			// We need these columns: "ID", "EndTime", "Inspect". Since "ID" is the partition key of the table,
			// it is automatically projected, so we specify the other fields we need to be projected.
			NonKeyAttributes: aws.StringSlice([]string{"EndTime", "Inspect"}),
			ProjectionType: aws.String("INCLUDE"),
		},
	},
	identSortEndIndex: {
		attrdefs: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String(colIdent), AttributeType: aws.String("S")},
			// DynamoDB has to know about the attribute type to index it
			{AttributeName: aws.String(colEndTime), AttributeType: aws.String("S")},
		},
		keyschema: []*dynamodb.KeySchemaElement{
			{KeyType: aws.String("HASH"), AttributeName: aws.String(colIdent)},
			{KeyType: aws.String("RANGE"), AttributeName: aws.String(colEndTime)},
		},
		projection: &dynamodb.Projection{
			// We need these columns: "ID", "EndTime", "Inspect". Since "ID" is the partition key of the table,
			// it is automatically projected, so we specify the other fields we need to be projected.
			NonKeyAttributes: aws.StringSlice([]string{"EndTime", "Inspect"}),
			ProjectionType: aws.String("INCLUDE"),
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
						Projection: config.projection,
					},
				},
			},
		}
		if _, err = db.UpdateTable(input); err != nil {
			return errors.E("error creating secondary index %s: %v", index, err)
		}
		log.Printf("created secondary index %s", index)
		// dynamodb allows only one index update at a time. We have to wait until the
		// table becomes active before we can update the next index.
		if _, err = waitForActiveTable(db, tableName, log); err != nil {
			return err
		}
	}
	for _, index := range indexesToRemove {
		if !indexExists[index] {
			continue
		}
		input := &dynamodb.UpdateTableInput{
			TableName:            aws.String(tableName),
			GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{{
				Delete: &dynamodb.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(index)}},
			},
		}
		if _, err = db.UpdateTable(input); err != nil {
			return errors.E("error deleting secondary index %s: %v", index, err)
		}
		log.Printf("deleted secondary index %s", index)
		// dynamodb allows only one index update at a time. We have to wait until the
		// table becomes active before we can update the next index.
		if _, err = waitForActiveTable(db, tableName, log); err != nil {
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
