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
	"sync/atomic"
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
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
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

// Store associates the digest v with the key digest k of the provided kind. If v is zero,
// k's association for (kind,v) will be removed.
func (a *Assoc) Store(ctx context.Context, kind assoc.Kind, k, v digest.Digest) error {
	switch kind {
	case assoc.Fileset, assoc.ExecInspect, assoc.Logs:
	default:
		return errors.E(errors.NotSupported, errors.Errorf("mappings of kind %v are not supported", kind))
	}

	if err := a.Limiter.Acquire(ctx, 1); err != nil {
		return err
	}
	defer a.Limiter.Release(1)
	updateExpr, attrValues, attrNames := getUpdateComponents(kind, k, v)
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {S: aws.String(k.String())},
		},
		UpdateExpression: &updateExpr,
		TableName:        aws.String(a.TableName),
	}
	if len(attrValues) > 0 {
		input.ExpressionAttributeValues = attrValues
	}
	// Expression attribute names have to be used if specified and cannot be empty.
	if len(attrNames) > 0 {
		input.ExpressionAttributeNames = attrNames
	}
	_, err := a.DB.UpdateItemWithContext(ctx, input)
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

func getUpdateComponents(kind assoc.Kind, k, v digest.Digest) (expr string, av map[string]*dynamodb.AttributeValue, an map[string]*string) {
	av = make(map[string]*dynamodb.AttributeValue)
	an = make(map[string]*string)
	switch kind {
	case assoc.Fileset:
		k4 := k
		k4.Truncate(4)
		switch {
		case v.IsZero():
			expr = "REMOVE #v"
		default:
			expr = "SET #v = :value, ID4 = :id4, LastAccessTime = :lastaccess"
			av[":value"] = &dynamodb.AttributeValue{S: aws.String(v.String())}
			av[":id4"] = &dynamodb.AttributeValue{S: aws.String(k4.HexN(4))}
			av[":lastaccess"] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprint(time.Now().Unix()))}
		}
		// Value is a reserved word. Use a placeholder.
		an["#v"] = aws.String("Value")
	case assoc.ExecInspect:
		switch {
		case v.IsZero():
			expr = "REMOVE ExecInspect"
		default:
			expr = "SET ExecInspect = list_append(:inspect, if_not_exists(ExecInspect, :empty_list))"
			av[":inspect"] = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{S: aws.String(v.String())}}}
			av[":empty_list"] = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}
		}
	case assoc.Logs:
		switch {
		case v.IsZero():
			expr = "REMOVE Logs"
		default:
			expr = "SET Logs = list_append(:logs, if_not_exists(Logs, :empty_list))"
			av[":logs"] = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{S: aws.String(v.String())}}}
			av[":empty_list"] = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}
		}
	}
	return
}

// Lookup returns the digest associated with key digest k. Lookup
// returns an error flagged errors.NotExist when no such mapping
// exists. Lookup also modifies the item's last-accessed time, which
// can be used for LRU object garbage collection.
//
// Get expands abbreviated keys by making use of a DynamoDB index.
func (a *Assoc) Get(ctx context.Context, kind assoc.Kind, k digest.Digest) (digest.Digest, digest.Digest, error) {
	var v digest.Digest
	switch kind {
	case assoc.Fileset, assoc.ExecInspect, assoc.Logs:
	default:
		return k, v, errors.E(errors.NotSupported, errors.Errorf("mappings of kind %v are not supported", kind))
	}
	var col string
	switch kind {
	case assoc.Fileset:
		col = "Value"
	case assoc.ExecInspect:
		col = "ExecInspect"
	case assoc.Logs:
		col = "Logs"
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
				expanded[kit] = it[col]
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
		item = resp.Item[col]
	}
	if item == nil || (kind == assoc.Fileset && item.S == nil) || (kind == assoc.ExecInspect && item.L == nil) || (kind == assoc.Logs && item.L == nil) {
		return k, v, errors.E("lookup", k, errors.NotExist)
	}
	if item.L != nil {
		item = item.L[0]
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

const updaterConcurrency = 10

// cell identifies a row,col we want to remove.
type cell struct {
	K    digest.Digest
	Kind assoc.Kind
}

type updater struct {
	cells chan *cell
	a     *Assoc
	rate  int64
}

func (u *updater) Go(ctx context.Context) error {
	rl := rate.NewLimiter(rate.Limit(u.rate), 1)
	g, ctx := errgroup.WithContext(ctx)
	retries := make(chan *cell, updaterConcurrency+1)
	for i := 0; i < updaterConcurrency; i++ {
		g.Go(func() error {
			for {
				var c *cell
				select {
				case c = <-retries:
				case c = <-u.cells:
					if c == nil {
						return nil
					}
				case <-ctx.Done():
					return ctx.Err()
				}
				err := rl.Wait(ctx)
				if err != nil {
					return err
				}
				err = u.a.Store(ctx, c.Kind, c.K, digest.Digest{})
				if awserr, ok := err.(awserr.Error); ok {
					switch awserr.Code() {
					case "ThrottlingException", "ProvisionedThroughputExceededException":
						time.Sleep(time.Second)
						// Writes to u.cells can block all threads and deadlock (since we have
						// an external writer). Write to a separate channel that only the
						// updater threads know about.
						retries <- c
					default:
						return err
					}
				}
				if err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
}

type counter struct {
	v int64
}

func (c *counter) Add(delta int64) {
	if c == nil {
		return
	}
	atomic.AddInt64(&c.v, delta)
}

func (c *counter) Get() int64 {
	if c == nil {
		return 0
	}
	return atomic.LoadInt64(&c.v)
}

// CollectWithThreshold removes from this Assoc any objects whose keys are not in the
// liveset and have not been accessed more recently than the liveset's threshold
func (a *Assoc) CollectWithThreshold(ctx context.Context, live liveset.Liveset, kind assoc.Kind, threshold time.Time, rate int64, dryRun bool) error {
	log.Debug("Collecting association")
	scanner := newScanner(a)

	var itemsCheckedCount, liveItemsCount, afterThresholdCount, itemsCollectedCount counter
	start := time.Now()

	updater := &updater{cells: make(chan *cell), a: a, rate: rate}
	errch := make(chan error)
	if !dryRun {
		go func() {
			errch <- updater.Go(ctx)
		}()
	}
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
			itemsCheckedCount.Add(1)
			if itemsCheckedCount.Get()%10000 == 0 {
				// This can take a long time, we want to know it's doing something
				log.Debugf("Checking item %d in association", itemsCheckedCount.Get())
			}
			if live.Contains(d) {
				liveItemsCount.Add(1)
			} else if time.Unix(itemAccessTime, 0).After(threshold) {
				afterThresholdCount.Add(1)
			} else {
				if !dryRun {
					updater.cells <- &cell{d, kind}
				}
				itemsCollectedCount.Add(1)
			}
		}
		return nil
	}))
	if !dryRun {
		close(updater.cells)
		<-errch
	}

	// Print what happened
	log.Debugf("Time to collect %s: %s", a.TableName, time.Since(start))
	log.Debugf("Checked %d associations, %d were live, %d were after the threshold.",
		itemsCheckedCount.Get(), liveItemsCount.Get(), afterThresholdCount.Get())
	action := "would have been"
	if !dryRun {
		action = "were"
	}
	log.Printf("%d of %d associations (%.2f%%) %s collected",
		itemsCollectedCount.Get(), itemsCheckedCount.Get(), float64(itemsCollectedCount.Get())/float64(itemsCheckedCount.Get())*100, action)

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
			k, err := reflow.Digester.Parse(*item["ID"].S)
			if err != nil {
				return fmt.Errorf("invalid dynamodb entry %v", item)
			}
			if item["Value"] != nil {
				v, err := reflow.Digester.Parse(*item["Value"].S)
				if err != nil {
					return fmt.Errorf("invalid dynamodb entry %v", item)
				}
				mappingHandler.HandleMapping(k, v, assoc.Fileset, time.Unix(itemAccessTime, 0))
			}

		}
		return nil
	}))
}
