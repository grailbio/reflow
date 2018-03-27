// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dydbassoc implements an assoc.Assoc based on AWS's
// DynamoDB.
package dydbassoc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset"
	"github.com/grailbio/reflow/log"
)

const (
	dbmaxdeleteobjects  = 25
	dbmaxdeleteattempts = 6
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
// table. DynamoDB conditional expressions are used to implement
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

// deleteItems removes the items from the dynamoDB table, with backoff and retry until they're all deleted
func (a *Assoc) deleteItems(ctx context.Context, writeRequests []*dynamodb.WriteRequest) error {
	var err error
	toWrite := map[string][]*dynamodb.WriteRequest{a.TableName: writeRequests}
	sleepTime := 2 * time.Second
	for attempts := 0; attempts < dbmaxdeleteattempts; attempts++ {
		if err := a.Limiter.Acquire(ctx, 1); err != nil {
			return err
		}
		out, err := a.DB.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: toWrite,
		})
		a.Limiter.Release(1)
		if err == nil && len(out.UnprocessedItems) > 0 {
			log.Debugf("Tried to delete %d items, %d left to go, attempt %d, sleeping %s",
				len(toWrite[a.TableName]), len(out.UnprocessedItems[a.TableName]), attempts, sleepTime)
			time.Sleep(sleepTime)
			toWrite = out.UnprocessedItems
			sleepTime *= 2
			continue
		}
		// We either hit an error or processed all our items
		return err
	}
	// We tried too many times and gave up
	return err
}

// CollectWithThreshold removes from this Assoc any objects whose keys are not in the
// liveset and have not been accessed more recently than the liveset's threshold
func (a *Assoc) CollectWithThreshold(ctx context.Context, live liveset.Liveset, threshold time.Time, dryRun bool) error {
	log.Debug("Collecting association")
	scanner := newScanner(a)

	var resultsLock sync.Mutex
	itemsCheckedCount := int64(0)
	liveItemsCount := int64(0)
	afterThresholdCount := int64(0)
	itemsCollectedCount := int64(0)
	start := time.Now()

	writeRequests := make([]*dynamodb.WriteRequest, 0, dbmaxdeleteobjects)

	err := scanner.Scan(ctx, ItemsHandlerFunc(func(items Items) error {
		for _, item := range items {
			itemAccessTime := int64(0)
			var err error
			if item["LastAccessTime"] != nil {
				itemAccessTime, err = strconv.ParseInt(*item["LastAccessTime"].N, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid dynamodb entry %v", item)
				}
			}
			d, err := reflow.Digester.Parse(*item["ID"].S)
			if err != nil {
				return fmt.Errorf("invalid dynamodb entry %v", item)
			}

			// Many threads can be calling this, get a lock before tallying results
			resultsLock.Lock()
			itemsCheckedCount++
			if itemsCheckedCount%10000 == 0 {
				// This can take a long time, we want to know it's doing something
				log.Debugf("Checking item %d in association", itemsCheckedCount)
			}
			if live.Contains(d) {
				liveItemsCount++
			} else if time.Unix(itemAccessTime, 0).After(threshold) {
				afterThresholdCount++
			} else {
				if !dryRun {
					// Add this item to the items to be deleted
					wr := &dynamodb.WriteRequest{DeleteRequest: &dynamodb.DeleteRequest{Key: map[string]*dynamodb.AttributeValue{"ID": {S: item["ID"].S}}}}
					writeRequests = append(writeRequests, wr)

					// See if we have enough to delete
					if len(writeRequests) == cap(writeRequests) {
						err := a.deleteItems(ctx, writeRequests)
						if err != nil {
							resultsLock.Unlock()
							return fmt.Errorf("error deleting dynamodb items %v (%s)", writeRequests, err)
						}
						writeRequests = writeRequests[:0]
					}
				}
				itemsCollectedCount++
			}
			resultsLock.Unlock()
		}
		return nil
	}))

	// Delete any items that may still be in the queue
	if !dryRun && len(writeRequests) > 0 {
		err := a.deleteItems(ctx, writeRequests)
		if err != nil {
			return fmt.Errorf("error deleting dynamodb items %v (%s)", writeRequests, err)
		}
	}

	// Print what happened
	log.Debugf("Time to collect %s: %s", a.TableName, time.Since(start))
	log.Debugf("Checked %d associations, %d were live, %d were after the threshold.",
		itemsCheckedCount, liveItemsCount, afterThresholdCount)
	action := "would have been"
	if !dryRun {
		action = "were"
	}
	log.Printf("%d of %d associations (%.2f%%) %s collected",
		itemsCollectedCount, itemsCheckedCount, float64(itemsCollectedCount)/float64(itemsCheckedCount)*100, action)

	return err
}

// Count returns an estimate of the number of associations in this mapping
func (a *Assoc) Count(ctx context.Context) (int64, error) {
	if err := a.Limiter.Acquire(ctx, 1); err != nil {
		return 0, err
	}
	defer a.Limiter.Release(1)

	resp, err := a.DB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(a.TableName),
	})
	if err != nil {
		return 0, err
	}
	return *resp.Table.ItemCount, nil
}

// Scan calls the handler function for every association in the mapping.
// Note that the handler function may be called asynchronously from multiple threads.
func (a *Assoc) Scan(ctx context.Context, mappingHandler assoc.MappingHandler) error {
	scanner := newScanner(a)
	return scanner.Scan(ctx, ItemsHandlerFunc(func(items Items) error {
		for _, item := range items {
			itemAccessTime := int64(0)
			var err error
			if item["LastAccessTime"] != nil {
				itemAccessTime, err = strconv.ParseInt(*item["LastAccessTime"].N, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid dynamodb entry %v", item)
				}
			}
			keyDigest, err := reflow.Digester.Parse(*item["ID"].S)
			if err != nil {
				return fmt.Errorf("invalid dynamodb entry %v", item)
			}
			valueDigest, err := reflow.Digester.Parse(*item["Value"].S)
			if err != nil {
				return fmt.Errorf("invalid dynamodb entry %v", item)
			}
			mappingHandler.HandleMapping(keyDigest, valueDigest, time.Unix(itemAccessTime, 0))
		}
		return nil
	}))
}
