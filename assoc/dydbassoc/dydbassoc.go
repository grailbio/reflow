// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dydbassoc implements an assoc.Assoc based on AWS's
// DynamoDB.
package dydbassoc

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

// Assoc implements a DynamoDB-backed Assoc for use in caches.
// Each association entry is represented by a DynamoDB
// item with the attributes "ID" and "Value".
//
// TODO(marius): support batch querying in this interface; it will be
// more efficient than relying on call concurrency.
type Assoc struct {
	DB        *dynamodb.DynamoDB
	Limiter   *limiter.Limiter
	TableName string
}

// Put associates the digest v with the key digest k in the dynamodb
// table. DynamoDB condtional expressions are used to implement
// compare-and-swap when expect is nonzero.
func (a *Assoc) Put(ctx context.Context, kind assoc.Kind, expect, k, v digest.Digest) error {
	if kind != assoc.Fileset {
		return errors.E(errors.NotSupported, errors.Errorf("mappings of kind %v are not supported", kind))
	}
	if err := a.Limiter.Acquire(ctx, 1); err != nil {
		return err
	}
	defer a.Limiter.Release(1)
	var (
		conditionExpression       *string
		expressionAttributeValues map[string]*dynamodb.AttributeValue
	)
	switch {
	case expect.IsZero() && !v.IsZero():
		conditionExpression = aws.String("attribute_not_exists(ID)")
	case !expect.IsZero():
		conditionExpression = aws.String("Value = :expect")
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":expect": {S: aws.String(expect.String())},
		}
	}
	if v.IsZero() {
		_, err := a.DB.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
			ConditionExpression:       conditionExpression,
			ExpressionAttributeValues: expressionAttributeValues,
			Key: map[string]*dynamodb.AttributeValue{
				"ID": {
					S: aws.String(k.String()),
				},
			},
			TableName: aws.String(a.TableName),
		})
		return err
	}
	k4 := k
	k4.Truncate(4)
	_, err := a.DB.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		ConditionExpression:       conditionExpression,
		ExpressionAttributeValues: expressionAttributeValues,
		Item: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(k.String()),
			},
			"Value": {
				S: aws.String(v.String()),
			},
			"LastAccessTime": {
				N: aws.String(fmt.Sprint(time.Now().Unix())),
			},
			"ID4": {
				S: aws.String(k4.HexN(4)),
			},
		},
		TableName: aws.String(a.TableName),
	})
	if err == nil {
		return nil
	}
	awserr, ok := err.(awserr.Error)
	if !ok {
		return err
	}
	switch awserr.Code() {
	case "ConditionalCheckFailedException":
		return errors.E(errors.Precondition, err)
	}
	return err
}

// Lookup returns the digest associated with key digest k. Lookup
// returns an error flagged errors.NotExist when no such mapping
// exists. Lookup also modifies the item's last-accessed time, which
// can be used for LRU object garbage collection.
//
// Get expands abbreviated keys by making use of a DynamoDB index.
func (a *Assoc) Get(ctx context.Context, kind assoc.Kind, k digest.Digest) (digest.Digest, digest.Digest, error) {
	var v digest.Digest
	if kind != assoc.Fileset {
		return k, v, errors.E(errors.NotSupported, errors.Errorf("mappings of kind %v are not supported", kind))
	}
	if err := a.Limiter.Acquire(ctx, 1); err != nil {
		return k, v, err
	}
	defer a.Limiter.Release(1)
	var item *dynamodb.AttributeValue
	if k.IsAbbrev() {
		resp, err := a.DB.Query(&dynamodb.QueryInput{
			TableName:              aws.String(a.TableName),
			IndexName:              aws.String("ID4-ID-index"),
			KeyConditionExpression: aws.String("ID4 = :id4"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":id4": {S: aws.String(k.HexN(4))},
			},
		})
		if err != nil {
			return k, v, err
		}
		expanded := make(map[digest.Digest]*dynamodb.AttributeValue)
		for _, it := range resp.Items {
			v := it["ID"]
			if v == nil || v.S == nil {
				continue
			}
			kit, err := reflow.Digester.Parse(*v.S)
			if err != nil {
				log.Debugf("invalid dynamodb entry %v", it)
				continue
			}
			if kit.Expands(k) {
				expanded[kit] = it["Value"]
			}
		}
		switch len(expanded) {
		case 0:
		case 1:
			for k1, v1 := range expanded {
				k = k1
				item = v1
			}
		default:
			return k, v, errors.E("lookup", k, errors.Invalid, errors.New("more than one key matched"))
		}
	} else {
		resp, err := a.DB.GetItemWithContext(ctx, &dynamodb.GetItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"ID": {
					S: aws.String(k.String()),
				},
			},
			TableName: aws.String(a.TableName),
		})
		if err != nil {
			return k, v, err
		}
		item = resp.Item["Value"]
	}
	if item == nil || item.S == nil {
		return k, v, errors.E("lookup", k, errors.NotExist)
	}
	var err error
	v, err = reflow.Digester.Parse(*item.S)
	if err != nil {
		return k, v, errors.E("lookup", k, err)
	}
	_, err = a.DB.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(k.String()),
			},
		},
		TableName:        aws.String(a.TableName),
		UpdateExpression: aws.String("SET LastAccessTime = :time ADD AccessCount :one"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":time": {N: aws.String(fmt.Sprint(time.Now().Unix()))},
			":one":  {N: aws.String("1")},
		},
	})
	if err != nil && err != ctx.Err() {
		awserr, ok := err.(awserr.Error)
		// The AWS SDK decides to override context cancellation
		// with its own non-standard error. Thanks Obama.
		if !ok || awserr.Code() != "RequestCanceled" {
			log.Errorf("dynamodb: update %v: %v", k, err)
		}
	}
	return k, v, nil
}
