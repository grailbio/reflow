// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dydbassoc implements an assoc.Assoc based on AWS's
// DynamoDB.
package dydbassoc

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/liveset"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

func init() {
	infra.Register("dynamodbassoc", new(Assoc))
}

const (
	// getTimeout is the timeout used for a single DynamoDB get request.
	getTimeout = 30 * time.Second

	// Default provisioned capacities for DynamoDB.
	writecap = 10
	readcap  = 20
)

// Assoc implements a DynamoDB-backed Assoc for use in caches.
// Each association entry is represented by a DynamoDB
// item with the attributes "ID" and "Value".
//
// TODO(marius): support batch querying in this interface; it will be
// more efficient than relying on call concurrency.
type Assoc struct {
	DB        dynamodbiface.DynamoDBAPI `yaml:"-"`
	Limiter   *limiter.Limiter          `yaml:"-"`
	TableName string                    `yaml:"-"`
	// Labels to assign to cache entries.
	Labels pool.Labels `yaml:"-"`

	labelsOnce sync.Once `yaml:"-"`
	labels     []*string `yaml:"-"`
}

// Help implements infra.Provider.
func (a *Assoc) Help() string {
	return "configure an assoc using the provided DynamoDB table name"
}

// Init implements infra.Provider.
func (a *Assoc) Init(sess *session.Session, labels pool.Labels) error {
	lim := limiter.New()
	lim.Release(32)
	a.DB = dynamodb.New(sess)
	a.Limiter = lim
	a.Labels = labels.Copy()
	return nil
}

// Setup implements infra.Provider.
func (a *Assoc) Setup(sess *session.Session, logger *log.Logger) error {
	log.Printf("creating DynamoDB table %s", a.TableName)
	db := dynamodb.New(sess)
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
		TableName:   aws.String(a.TableName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != dynamodb.ErrCodeResourceInUseException {
			log.Fatal(err)
		}
		log.Printf("dynamodb table %s already exists", a.TableName)
	} else {
		log.Printf("created DynamoDB table %s", a.TableName)
	}
	const indexName = "ID4-ID-index"
	var describe *dynamodb.DescribeTableOutput
	start := time.Now()
	for {
		describe, err = db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(a.TableName),
		})
		if err != nil {
			return err
		}
		status := *describe.Table.TableStatus
		if status == "ACTIVE" {
			break
		}

		if time.Since(start) > time.Minute {
			return errors.New("waited for table to become active for too long; try again later")
		}
		log.Printf("waiting for table to become active; current status: %v", status)
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
		log.Printf("dynamodb index %s already exists", indexName)
	} else {
		// Create a secondary index to look up keys by their ID4-prefix.
		_, err = db.UpdateTable(&dynamodb.UpdateTableInput{
			TableName: aws.String(a.TableName),
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
					},
				},
			},
		})
		if err != nil {
			return errors.E("error creating secondary index: %v", err)
		}
		log.Printf("created secondary index %s", indexName)
	}
	return nil
}

// Flags implements infra.Provider.
func (a *Assoc) Flags(flags *flag.FlagSet) {
	flags.StringVar(&a.TableName, "table", "", "name of the dynamodb table")
}

// Version implements infra.Provider.
func (a *Assoc) Version() int {
	return 1
}

// Store associates the digest v with the key digest k of the provided kind. If v is zero,
// k's association for (kind,v) will be removed.
func (a *Assoc) Store(ctx context.Context, kind assoc.Kind, k, v digest.Digest) error {
	switch kind {
	case assoc.Fileset, assoc.ExecInspect, assoc.Logs, assoc.Bundle:
	default:
		return errors.E(errors.NotSupported, errors.Errorf("mappings of kind %v are not supported", kind))
	}

	if err := a.Limiter.Acquire(ctx, 1); err != nil {
		return err
	}
	defer a.Limiter.Release(1)
	updateExpr, attrValues, attrNames := a.getUpdateComponents(kind, k, v)
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

func (a *Assoc) getUpdateComponents(kind assoc.Kind, k, v digest.Digest) (expr string, av map[string]*dynamodb.AttributeValue, an map[string]*string) {
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
	case assoc.Bundle:
		k4 := k
		k4.Truncate(4)
		switch {
		case v.IsZero():
			expr = "REMOVE Bundle"
		default:
			expr = "SET ID4 = :id4, Bundle= list_append(:bundle, if_not_exists(Bundle, :empty_list))"
			av[":bundle"] = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{S: aws.String(v.String())}}}
			av[":id4"] = &dynamodb.AttributeValue{S: aws.String(k4.HexN(4))}
			av[":empty_list"] = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}
		}

	}
	if !v.IsZero() && len(a.Labels) > 0 {
		a.labelsOnce.Do(func() {
			for k, v := range a.Labels {
				a.labels = append(a.labels, aws.String(fmt.Sprintf("%s=%s", k, v)))
			}
		})
		an["#l"] = aws.String("Labels")
		expr += " ADD #l :labels"
		av[":labels"] = &dynamodb.AttributeValue{SS: a.labels}
	}
	return
}

var (
	colmap = map[assoc.Kind]string{
		assoc.Fileset:     "Value",
		assoc.Logs:        "Logs",
		assoc.Bundle:      "Bundle",
		assoc.ExecInspect: "ExecInspect",
	}
	backOffPolicy = retry.MaxTries(retry.Backoff(2*time.Millisecond, time.Minute, 1), 10)
)

// Get returns the digest associated with key digest k. Lookup
// returns an error flagged errors.NotExist when no such mapping
// exists. Lookup also modifies the item's last-accessed time, which
// can be used for LRU object garbage collection.
// Get expands abbreviated keys by making use of a DynamoDB index.
func (a *Assoc) Get(ctx context.Context, kind assoc.Kind, k digest.Digest) (digest.Digest, digest.Digest, error) {
	var v digest.Digest
	switch kind {
	case assoc.Fileset, assoc.ExecInspect, assoc.Logs, assoc.Bundle:
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
	case assoc.Bundle:
		col = "Bundle"
	}
	if err := a.Limiter.Acquire(ctx, 1); err != nil {
		return k, v, err
	}
	defer a.Limiter.Release(1)
	ctx, cancel := context.WithTimeout(ctx, getTimeout)
	defer cancel()
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
	if item == nil || (kind == assoc.Fileset && item.S == nil) || (kind == assoc.ExecInspect && item.L == nil) || (kind == assoc.Logs && item.L == nil) || (kind == assoc.Bundle && item.L == nil) {
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

// BatchGet implements the assoc interface. BatchGet will return a result for each key in the batch.
// BatchGet could internally split the keys into several batches. Any global errors, like context
// cancellation, S3 API errors or a key parse error would be returned from BatchGet. Any value parse
// errors would be returned as part of the result for that key.
func (a *Assoc) BatchGet(ctx context.Context, batch assoc.Batch) error {
	unique := make(map[digest.Digest]map[assoc.Kind]bool)
	for k := range batch {
		if _, ok := unique[k.Digest]; !ok {
			unique[k.Digest] = make(map[assoc.Kind]bool)
		}
		unique[k.Digest][k.Kind] = true
	}
	// Dynamodb BatchGetItemWithContext has API limitation of 100 keys per batch request.
	const maxKeysPerBatch = 100
	numBatches := (len(unique) + maxKeysPerBatch - 1) / maxKeysPerBatch
	keys := make([]digest.Digest, 0, len(unique))
	for k := range unique {
		keys = append(keys, k)
	}

	batches := make([]map[assoc.Key]assoc.Result, numBatches)
	err := traverse.Each(numBatches, func(batch int) error {
		end := maxKeysPerBatch * (batch + 1)
		if end > len(keys) {
			end = len(keys)
		}
		keys := keys[maxKeysPerBatch*batch : end]
		batches[batch] = make(map[assoc.Key]assoc.Result)
		input := dynamodb.BatchGetItemInput{RequestItems: map[string]*dynamodb.KeysAndAttributes{
			a.TableName: &dynamodb.KeysAndAttributes{},
		}}
		for _, k := range keys {
			av := map[string]*dynamodb.AttributeValue{
				"ID": {
					S: aws.String(k.String()),
				},
			}
			input.RequestItems[a.TableName].Keys = append(input.RequestItems[a.TableName].Keys, av)
		}
		for retries := 0; ; {
			var (
				output *dynamodb.BatchGetItemOutput
				err    error
			)
			if output, err = a.DB.BatchGetItemWithContext(ctx, &input); err != nil {
				if !request.IsErrorThrottle(err) {
					return err
				}
				if err := retry.Wait(ctx, backOffPolicy, retries); err != nil {
					return err
				}
				retries++
				continue
			}
			for _, it := range output.Responses[a.TableName] {
				key := it["ID"].S
				k, err := reflow.Digester.Parse(*key)
				if err != nil {
					return err
				}
				kinds := unique[k]
				for kind := range kinds {
					if _, ok := it[colmap[kind]]; !ok {
						continue
					}
					// TODO(pgopal) - this assumes that the value is of type string. Today we store lists too.
					// But we don't call batchget for those types. We should ideally handle all types.
					value := it[colmap[kind]].S
					v, err := reflow.Digester.Parse(*value)
					if err != nil {
						batches[batch][assoc.Key{Digest: k, Kind: kind}] = assoc.Result{Error: err}
						continue
					}
					batches[batch][assoc.Key{Digest: k, Kind: kind}] = assoc.Result{Digest: v}
				}
			}
			input.RequestItems[a.TableName].Keys = input.RequestItems[a.TableName].Keys[:0]
			if _, ok := output.UnprocessedKeys[a.TableName]; ok {
				input.RequestItems[a.TableName].Keys = output.UnprocessedKeys[a.TableName].Keys
			}
			if len(input.RequestItems[a.TableName].Keys) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	var cacheKeys = make([]digest.Digest, 0, len(batches))
	for _, b := range batches {
		for k, v := range b {
			if v.Error == nil {
				cacheKeys = append(cacheKeys, k.Digest)
			}
			batch[k] = v
		}
	}
	if len(cacheKeys) <= 0 {
		return nil
	}

	// Asynchronously update LastAccessTime and AccessCount for each accessed key.
	updateCtx := flow.Background(ctx)
	go func() {
		_ = traverse.Each(len(cacheKeys), func(i int) error {
			if err := a.Limiter.Acquire(updateCtx, 1); err != nil {
				return nil
			}
			defer a.Limiter.Release(1)
			input := &dynamodb.UpdateItemInput{
				Key: map[string]*dynamodb.AttributeValue{
					"ID": {
						S: aws.String(cacheKeys[i].String()),
					},
				},
				TableName:        aws.String(a.TableName),
				UpdateExpression: aws.String("SET LastAccessTime = :time ADD AccessCount :one"),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":time": {N: aws.String(fmt.Sprint(time.Now().Unix()))},
					":one":  {N: aws.String("1")},
				},
			}
			_, err := a.DB.UpdateItemWithContext(updateCtx, input)
			if err != nil && err != updateCtx.Err() {
				awserr, ok := err.(awserr.Error)
				// The AWS SDK decides to override context cancellation
				// with its own non-standard error.
				if !ok || awserr.Code() != "RequestCanceled" {
					log.Errorf("dynamodb: update %v: %v", cacheKeys[i], err)
				}
			}
			return nil
		})
		updateCtx.Complete()
	}()
	return nil
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
func (a *Assoc) CollectWithThreshold(ctx context.Context, live liveset.Liveset, dead liveset.Liveset, kind assoc.Kind, threshold time.Time, rate int64, dryRun bool) error {
	log.Debug("Collecting association")
	scanner := newScanner(a)

	var itemsCheckedCount, liveItemsCount, afterThresholdCount, itemsCollectedCount, deadFilterCount counter
	start := time.Now()

	updater := &updater{cells: make(chan *cell), a: a, rate: rate}
	errch := make(chan error)
	if !dryRun {
		go func() {
			errch <- updater.Go(ctx)
		}()
	}
	var mu sync.Mutex
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

			itemsCheckedCount.Add(1)
			if itemsCheckedCount.Get()%10000 == 0 {
				// This can take a long time, we want to know it's doing something
				log.Debugf("Checking item %d in association", itemsCheckedCount.Get())
			}

			mu.Lock()
			contains := live.Contains(d)
			remove := dead.Contains(d)
			mu.Unlock()
			if contains {
				liveItemsCount.Add(1)
			} else if remove {
				if !dryRun {
					updater.cells <- &cell{d, kind}
				}
				itemsCollectedCount.Add(1)
				deadFilterCount.Add(1)
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
	log.Printf("%d of %d associations (%.2f%%) %s collected (%d associations matched the dead set)",
		itemsCollectedCount.Get(), itemsCheckedCount.Get(), float64(itemsCollectedCount.Get())/float64(itemsCheckedCount.Get())*100, action, deadFilterCount.Get())

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
func (a *Assoc) Scan(ctx context.Context, kind assoc.Kind, mappingHandler assoc.MappingHandler) error {
	scanner := newScanner(a)
	colname, ok := colmap[kind]
	if !ok {
		panic("invalid kind")
	}

	return scanner.Scan(ctx, ItemsHandlerFunc(func(items Items) error {
		for _, item := range items {
			itemAccessTime := int64(0)
			var err error
			if item["LastAccessTime"] != nil {
				itemAccessTime, err = strconv.ParseInt(*item["LastAccessTime"].N, 10, 64)
				if err != nil {
					log.Errorf("invalid dynamodb entry %v", item)
					continue
				}
			}
			k, err := reflow.Digester.Parse(*item["ID"].S)
			if err != nil {
				log.Errorf("invalid dynamodb entry %v", item)
				continue
			}

			if item[colname] != nil {
				var labels []string
				if item["Labels"] != nil {
					err := dynamodbattribute.Unmarshal(item["Labels"], &labels)
					if err != nil {
						log.Errorf("invalid label: %v", err)
						continue
					}
				}

				dbval := *item[colname]
				var v []digest.Digest
				switch kind {
				case assoc.Fileset:
					d, err := reflow.Digester.Parse(*dbval.S)
					if err != nil {
						log.Errorf("invalid digest of kind %v for dynamodb entry %v", kind, item)
						continue
					}
					v = []digest.Digest{d}
				case assoc.ExecInspect, assoc.Logs, assoc.Bundle:
					l := dbval.L
					for _, val := range l {
						d, err := reflow.Digester.Parse(*val.S)
						if err != nil {
							continue
						}
						v = append(v, d)
					}
					if v == nil {
						log.Errorf("no valid digests of kind %v for dynamodb entry %v", kind, item)
						continue
					}
				}
				mappingHandler.HandleMapping(k, v, kind, time.Unix(itemAccessTime, 0), labels)
			}
		}
		return nil
	}))
}
