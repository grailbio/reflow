package dydbassoc

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/test/testutil"
	"golang.org/x/sync/errgroup"
)

var mockTable = "mockTable"

type mockdb struct {
	dynamodbiface.DynamoDBAPI
}

func (m *mockdb) BatchGetItemWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, options ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	var n = len(input.RequestItems[mockTable].Keys)
	if n == 0 {
		return &dynamodb.BatchGetItemOutput{Responses: make(map[string][]map[string]*dynamodb.AttributeValue)}, nil
	}
	if n > 100 {
		return nil, awserr.New("ValidationException", "Too many items requested for the BatchGetItem call", nil)
	}
	rand.Shuffle(len(input.RequestItems[mockTable].Keys), func(i, j int) {
		s := input.RequestItems[mockTable].Keys
		s[i], s[j] = s[j], s[i]
	})
	o := &dynamodb.BatchGetItemOutput{Responses: make(map[string][]map[string]*dynamodb.AttributeValue)}
	if input.RequestItems[mockTable] != nil {
		for i := 0; i < n; i++ {
			v := input.RequestItems[mockTable].Keys[i]
			if v["ID"] != nil {
				m := map[string]*dynamodb.AttributeValue{
					"ID": {
						S: aws.String(*v["ID"].S),
					},
					"Value": {
						S: aws.String(*v["ID"].S),
					},
					"Logs": {
						S: aws.String(*v["ID"].S),
					},
					"Bundle": {
						S: aws.String(*v["ID"].S),
					},
					"ExecInspect": {
						S: aws.String(*v["ID"].S),
					},
				}
				o.Responses[mockTable] = append(o.Responses[mockTable], m)
			}
		}
	}
	return o, nil
}

func (m *mockdb) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return nil, nil
}

var kinds = []assoc.Kind{assoc.Fileset, assoc.ExecInspect, assoc.Logs, assoc.Bundle}

func TestEmptyKeys(t *testing.T) {
	ctx := context.Background()
	ass := &Assoc{DB: &mockdb{}, TableName: mockTable}
	keys := make(assoc.Batch)
	err := ass.BatchGet(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(keys), 0; got != want {
		t.Errorf("expected %d values, got %v", want, got)
	}
}

func TestSimpleBatchGetItem(t *testing.T) {
	ctx := context.Background()
	ass := &Assoc{DB: &mockdb{}, TableName: mockTable}
	k := reflow.Digester.Rand(nil)
	key := assoc.Key{Kind: assoc.Fileset, Digest: k}
	batch := assoc.Batch{key: assoc.Result{}}
	err := ass.BatchGet(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(batch), 1; got != want {
		t.Errorf("expected %v responses, got %v", want, got)
	}
	if got, want := batch[key].Digest, k; batch.Found(key) && got != want {
		t.Errorf("want %v, got %v", got, want)
	}
}

func TestMultiKindBatchGetItem(t *testing.T) {
	ctx := context.Background()
	ass := &Assoc{DB: &mockdb{}, TableName: mockTable}
	k := reflow.Digester.Rand(nil)
	keys := []assoc.Key{{assoc.Fileset, k}, {assoc.ExecInspect, k}}
	batch := make(assoc.Batch)
	batch.Add(keys...)
	err := ass.BatchGet(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(batch), 2; got != want {
		t.Fatalf("expected %v responses, got %v", want, got)
	}
	if got, want := batch[keys[0]].Digest, k; batch.Found(keys[0]) && got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := batch[keys[1]].Digest, k; batch.Found(keys[0]) && got != want {
		t.Errorf("want %v, got %v", got, want)
	}
}

type mockdbunprocessed struct {
	dynamodbiface.DynamoDBAPI
	maxRetries int
	retries    int
}

func (m *mockdbunprocessed) BatchGetItemWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, options ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	m.retries = m.retries + 1
	var total = len(input.RequestItems[mockTable].Keys)
	if total == 0 {
		return &dynamodb.BatchGetItemOutput{Responses: make(map[string][]map[string]*dynamodb.AttributeValue)}, nil
	}
	if total > 100 {
		return nil, awserr.New("ValidationException", "Too many items requested for the BatchGetItem call", nil)
	}
	// process some keys and leave the remaining unprocessed.
	n := rand.Int() % total
	if n == 0 {
		n = 1
	}
	if m.retries >= m.maxRetries {
		n = total
	}
	rand.Shuffle(len(input.RequestItems[mockTable].Keys), func(i, j int) {
		s := input.RequestItems[mockTable].Keys
		s[i], s[j] = s[j], s[i]
	})
	o := &dynamodb.BatchGetItemOutput{Responses: make(map[string][]map[string]*dynamodb.AttributeValue)}
	if input.RequestItems[mockTable] != nil {
		for i := 0; i < total; i++ {
			v := input.RequestItems[mockTable].Keys[i]
			if v["ID"] != nil {
				m := map[string]*dynamodb.AttributeValue{
					"ID": {
						S: aws.String(*v["ID"].S),
					},
					"Value": {
						S: aws.String(*v["ID"].S),
					},
					"Logs": {
						S: aws.String(*v["ID"].S),
					},
					"Bundle": {
						S: aws.String(*v["ID"].S),
					},
					"ExecInspect": {
						S: aws.String(*v["ID"].S),
					},
				}
				o.Responses[mockTable] = append(o.Responses[mockTable], m)
			}
		}
		o.UnprocessedKeys = map[string]*dynamodb.KeysAndAttributes{mockTable: &dynamodb.KeysAndAttributes{}}
		for i := n; i < len(input.RequestItems[mockTable].Keys); i++ {
			v := input.RequestItems[mockTable].Keys[i]
			o.UnprocessedKeys[mockTable].Keys = append(o.UnprocessedKeys[mockTable].Keys, v)
		}
	}
	return o, nil
}

func TestParallelBatchGetItem(t *testing.T) {
	ctx := context.Background()
	ass := &Assoc{DB: &mockdbunprocessed{maxRetries: 10}, TableName: mockTable}
	count := 10 * 1024
	digests := make([]assoc.Key, count)

	for i := 0; i < count; i++ {
		digests[i] = assoc.Key{Kind: kinds[rand.Int()%4], Digest: reflow.Digester.Rand(nil)}
	}
	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan assoc.Key)
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			var err error
			batch := make(assoc.Batch)
			keys := make([]assoc.Key, 0)
			for c := range ch {
				keys = append(keys, c)
				batch[c] = assoc.Result{}
			}
			err = ass.BatchGet(ctx, batch)
			if err != nil {
				return err
			}
			checkKeys := make(map[assoc.Key]bool)
			for k, v := range batch {
				expected := k
				if got, want := v.Digest, expected.Digest; got != want && !v.Digest.IsZero() {
					t.Errorf("got %v, want %v", got, want)
					continue
				}
				if got, want := k.Kind, expected.Kind; got != want {
					t.Errorf("got %v, want %v", got, want)
					continue
				}
				checkKeys[k] = true
			}
			for _, k := range keys {
				if _, ok := checkKeys[k]; !ok {
					t.Errorf("Result missing key (%v)", k)
				}
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
	g.Go(func() error {
		for _, k := range digests {
			select {
			case ch <- k:
			case <-ctx.Done():
				close(ch)
				return nil
			}
		}
		close(ch)
		return nil
	})
	err := g.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

type mockdbInvalidDigest struct {
	dynamodbiface.DynamoDBAPI
	invalidDigestCol string
}

func (m *mockdbInvalidDigest) BatchGetItemWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, options ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	total := len(input.RequestItems[mockTable].Keys)
	if total == 0 {
		return &dynamodb.BatchGetItemOutput{Responses: make(map[string][]map[string]*dynamodb.AttributeValue)}, nil
	}
	o := &dynamodb.BatchGetItemOutput{Responses: make(map[string][]map[string]*dynamodb.AttributeValue)}
	if input.RequestItems[mockTable] != nil {
		for i := 0; i < total; i++ {
			v := input.RequestItems[mockTable].Keys[i]
			if v["ID"] != nil {
				ma := map[string]*dynamodb.AttributeValue{
					"ID": {
						S: aws.String(*v["ID"].S),
					},
					"Value": {
						S: aws.String(*v["ID"].S),
					},
					"Logs": {
						S: aws.String(*v["ID"].S),
					},
					"Bundle": {
						S: aws.String(*v["ID"].S),
					},
					"ExecInspect": {
						S: aws.String(*v["ID"].S),
					},
				}
				ma[m.invalidDigestCol] = &dynamodb.AttributeValue{S: aws.String("corrupted")}
				o.Responses[mockTable] = append(o.Responses[mockTable], ma)
			}
		}
	}
	return o, nil
}

func TestInvalidDigest(t *testing.T) {
	batch := make(assoc.Batch)
	for i := 0; i < 1000; i++ {
		batch[assoc.Key{Kind: kinds[i%4], Digest: reflow.Digester.Rand(nil)}] = assoc.Result{}
	}
	pat := `encoding/hex: invalid byte:.*`
	re, err := regexp.Compile(pat)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range kinds {
		ass := &Assoc{DB: &mockdbInvalidDigest{invalidDigestCol: colmap[kind]}, TableName: mockTable}
		err := ass.BatchGet(context.Background(), batch)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := len(batch), 1000; got != want {
			t.Errorf(fmt.Sprintf("expected %v result keys, got %v", want, got))
		}
		errCount := 0
		for k, v := range batch {
			if k.Kind == kind {
				ok := re.MatchString(v.Error.Error())
				errCount++
				if !ok {
					t.Errorf(fmt.Sprintf("error %s does not match %s", pat, v.Error.Error()))
				}
			}
		}
		if got, want := errCount, 250; got != want {
			t.Errorf(fmt.Sprintf("expected %v invalid digest keys, got %v", want, got))
		}
	}
}

func TestDydbassocInfra(t *testing.T) {
	const table = "reflow-unittest"
	testutil.SkipIfNoCreds(t)
	var schema = infra.Schema{
		"labels":  make(pool.Labels),
		"session": new(session.Session),
		"assoc":   new(assoc.Assoc),
		"logger":  new(log.Logger),
		"user":    new(infra2.User),
	}
	config, err := schema.Make(infra.Keys{
		"labels":  "kv",
		"session": "awssession",
		"assoc":   fmt.Sprintf("dynamodbassoc,table=%v", table),
		"logger":  "logger",
		"user":    "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	var a assoc.Assoc
	config.Must(&a)
	dydbassoc, ok := a.(*Assoc)
	if !ok {
		t.Fatalf("%v is not an dydbassoc", reflect.TypeOf(a))
	}
	if got, want := dydbassoc.TableName, table; got != want {
		t.Errorf("got %v, want %v", dydbassoc.TableName, table)
	}
}
