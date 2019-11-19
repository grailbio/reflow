package dydbassoc

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/reflow/log"
	"golang.org/x/sync/errgroup"
)

// scanner lets us scan segments of a dyanamoDB table in parallel
type scanner struct {
	// Assoc that we're scanning
	Assoc *Assoc
	// SegmentCount is the number of segments to break the table into
	SegmentCount int
	// MaxAttempts controls how many times in a row a thread will allow transient dynamoDB errors
	// before giving up
	MaxAttempts int
}

// newScanner creates a new dynamoDB scanner.
func newScanner(a *Assoc) *scanner {
	return &scanner{
		Assoc:        a,
		SegmentCount: 50, // Picked a value that seems to give good performance, we may want to make this configurable
		MaxAttempts:  5,
	}
}

// Scan uses the Handler function to process Items for each of the segments.
func (s *scanner) Scan(ctx context.Context, h ItemsHandler) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for i := 0; i < s.SegmentCount; i++ {
		segment := i
		group.Go(func() error {
			var lastEvaluatedKey map[string]*dynamodb.AttributeValue

			attempts := 0
			for {
				// Limit the number of concurrent calls we make to the database
				if err := s.Assoc.Limiter.Acquire(groupCtx, 1); err != nil {
					return fmt.Errorf("cannot acquire token for segment %d (%s)", segment, err)
				}

				// Do the scan
				params := &dynamodb.ScanInput{
					TableName:                aws.String(s.Assoc.TableName),
					Segment:                  aws.Int64(int64(segment)),
					TotalSegments:            aws.Int64(int64(s.SegmentCount)),
					FilterExpression:         aws.String("attribute_exists(#v)"),
					ExpressionAttributeNames: map[string]*string{"#v": aws.String("Value")},
				}
				if lastEvaluatedKey != nil {
					params.ExclusiveStartKey = lastEvaluatedKey
				}
				resp, err := s.Assoc.DB.ScanWithContext(groupCtx, params)

				// Release the token before processing the Items
				s.Assoc.Limiter.Release(1)

				// If we hit an error sleep some number of times and try again,
				// we may have been rate limited
				if err != nil {
					log.Errorf("error calling dynamodb for segment %d, attempt %d (%s)", segment, attempts, err)
					attempts++
					if attempts > s.MaxAttempts {
						return fmt.Errorf("error calling db for segment %d (%s)", segment, err)
					}
					time.Sleep(time.Duration(attempts) * 10 * time.Second)
					continue
				}

				// Reset the attempt counter when we get a successful response
				attempts = 0

				// Call the Handler function with the Items
				err = h.HandleItems(resp.Items)
				log.Errorf("error handling items %v", err)

				// We're done if the last evaluated key is empty
				if resp.LastEvaluatedKey == nil {
					return nil
				}

				// Set last evaluated key
				lastEvaluatedKey = resp.LastEvaluatedKey
			}
		})
	}
	return group.Wait()
}

// ItemsHandler is an interface for handling Items from a segment scan.
type ItemsHandler interface {
	// HandleItems handles a set of scanned items.
	HandleItems(items Items) error
}

// Items is the response from a dynamoDb scan.
type Items []map[string]*dynamodb.AttributeValue

// ItemsHandlerFunc is a convenience type to avoid having to declare a struct
// to implement the ItemsHandler interface.
type ItemsHandlerFunc func(items Items) error

// HandleItems implements the ItemsHandler interface.
func (h ItemsHandlerFunc) HandleItems(items Items) error {
	return h(items)
}
