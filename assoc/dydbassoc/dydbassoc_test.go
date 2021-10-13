package dydbassoc

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/flow"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/test/testutil"
	"golang.org/x/sync/errgroup"
)

const mockTable = "mockTable"

type mockEntry struct {
	Attributes map[string]*dynamodb.AttributeValue
	Kind       assoc.Kind
}

type mockdb struct {
	dynamodbiface.DynamoDBAPI
	mockStore map[*mockEntry]bool
	dbscanned bool
	muScan    sync.Mutex

	muUpdate   sync.Mutex
	numUpdates int
}

// NumUpdates is the total of recorded calls to UpdateWithContext.
func (m *mockdb) NumUpdates() int {
	m.muUpdate.Lock()
	defer m.muUpdate.Unlock()
	return m.numUpdates
}

func (m *mockdb) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, options ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	key, ok := input.Key["ID"]
	if !ok {
		panic("key missing")
	}
	for k := range m.mockStore {
		entry, ok := k.Attributes["ID"]
		if !ok {
			continue
		}
		if *entry.S == *key.S {
			delete(m.mockStore, k)
			return &dynamodb.DeleteItemOutput{
				Attributes: input.Key,
			}, nil
		}
	}
	return &dynamodb.DeleteItemOutput{}, nil
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
				}
				o.Responses[mockTable] = append(o.Responses[mockTable], m)
			}
		}
	}
	return o, nil
}

func (m *mockdb) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.muUpdate.Lock()
	m.numUpdates++
	m.muUpdate.Unlock()
	return nil, nil
}

func (m *mockdb) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	m.muScan.Lock()
	defer m.muScan.Unlock()
	var output = &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{},
	}
	if m.dbscanned {
		return output, nil
	}
	for k := range m.mockStore {
		// Ignore all entries that do not show LastAccessTime.
		if _, ok := k.Attributes["LastAccessTime"]; !ok {
			continue
		}
		output.Items = append(output.Items, k.Attributes)
	}
	m.dbscanned = true
	count := int64(len(output.Items))
	output.Count = &count
	output.ScannedCount = &count
	return output, nil
}

var kinds = []assoc.Kind{assoc.Fileset}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	key := reflow.Digester.Rand(nil)
	dummyEntry := mockEntry{
		Attributes: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(key.String()),
			},
		},
		Kind: assoc.Fileset,
	}
	db := &mockdb{
		mockStore: map[*mockEntry]bool{&dummyEntry: true},
	}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable

	badkey := reflow.Digester.Rand(nil)
	if err := ass.Delete(ctx, badkey); err == nil {
		t.Fatal("cannot successfully delete key that does not exist")
	}
	if got, want := len(db.mockStore), 1; got != want {
		t.Errorf("got %d entries, want %d", got, want)
	}

	if err := ass.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got, want := len(db.mockStore), 0; got != want {
		t.Errorf("got %d entries, want %d", got, want)
	}
}

func TestEmptyKeys(t *testing.T) {
	var wg sync.WaitGroup
	ctx, _ := flow.WithBackground(context.Background(), &wg)
	db := &mockdb{}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable
	keys := make(assoc.Batch)
	err := ass.BatchGet(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(keys), 0; got != want {
		t.Errorf("expected %d values, got %v", want, got)
	}
	wg.Wait()
	if got, want := db.NumUpdates(), 0; got != want {
		t.Errorf("got %d updates, want %d", got, want)
	}
}

func TestSimpleBatchGetItem(t *testing.T) {
	var wg sync.WaitGroup
	ctx, _ := flow.WithBackground(context.Background(), &wg)
	db := &mockdb{}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable
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
	wg.Wait()
	if got, want := db.NumUpdates(), 1; got != want {
		t.Errorf("got %d updates, want %d", got, want)
	}
}

type mockdbunprocessed struct {
	dynamodbiface.DynamoDBAPI
	maxRetries int
	retries    int

	mu         sync.Mutex
	numUpdates int
}

// NumUpdates is the total of recorded calls to UpdateWithContext.
func (m *mockdbunprocessed) NumUpdates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numUpdates
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

func (m *mockdbunprocessed) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	m.numUpdates++
	m.mu.Unlock()
	return nil, nil
}

func TestParallelBatchGetItem(t *testing.T) {
	var wg sync.WaitGroup
	ctx, _ := flow.WithBackground(context.Background(), &wg)
	count := 10 * 1024
	db := &mockdbunprocessed{maxRetries: 10}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable
	digests := make([]assoc.Key, count)

	for i := 0; i < count; i++ {
		digests[i] = assoc.Key{Kind: kinds[rand.Int()%len(kinds)], Digest: reflow.Digester.Rand(nil)}
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
	wg.Wait()
	if got, want := db.NumUpdates(), count; got != want {
		t.Errorf("got %d updated entries, want %d", got, want)
	}
}

type mockdbInvalidDigest struct {
	dynamodbiface.DynamoDBAPI
	invalidDigestCol string

	mu         sync.Mutex
	numUpdates int
}

// NumUpdates is the total of recorded calls to UpdateWithContext.
func (m *mockdbInvalidDigest) NumUpdates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numUpdates
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
				}
				ma[m.invalidDigestCol] = &dynamodb.AttributeValue{S: aws.String("corrupted")}
				o.Responses[mockTable] = append(o.Responses[mockTable], ma)
			}
		}
	}
	return o, nil
}

func (m *mockdbInvalidDigest) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	m.numUpdates++
	m.mu.Unlock()
	return nil, nil
}

func TestInvalidDigest(t *testing.T) {
	batch := make(assoc.Batch)
	for i := 0; i < 1000; i++ {
		batch[assoc.Key{Kind: kinds[i%len(kinds)], Digest: reflow.Digester.Rand(nil)}] = assoc.Result{}
	}
	pat := `encoding/hex: invalid byte:.*`
	re, err := regexp.Compile(pat)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range kinds {
		db := &mockdbInvalidDigest{invalidDigestCol: colmap[kind]}
		ass := &Assoc{DB: db}
		ass.TableName = mockTable
		var wg sync.WaitGroup
		ctx, cancel := flow.WithBackground(context.Background(), &wg)
		err := ass.BatchGet(ctx, batch)
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
		if got, want := errCount, len(batch) / len(kinds); got != want {
			t.Errorf(fmt.Sprintf("expected %v invalid digest keys, got %v", want, got))
		}
		wg.Wait()
		if got, want := db.NumUpdates(), 1000-errCount; got != want {
			t.Errorf("got %d updates, want %d", got, want)
		}
		cancel()
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

func TestAssocScanValidEntries(t *testing.T) {
	var (
		ctx = context.Background()
		db  = &mockdb{mockStore: make(map[*mockEntry]bool)}
		ass = &Assoc{DB: db}
	)
	ass.TableName = mockTable
	for _, tt := range []struct {
		kind               assoc.Kind
		key                digest.Digest
		val                digest.Digest
		labels             []string
		lastAccessTimeUnix string
	}{
		{assoc.Fileset, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}, "1571573191"},
		{assoc.Fileset, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), nil, "1572455280"},
		{assoc.Fileset, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), nil, "1572448157"},
		{assoc.Fileset, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), []string{"grail:type=reflow", "grail:user=def@graiobio.com"}, "1568099519"},
		{assoc.Fileset, reflow.Digester.Rand(nil), digest.Digest{}, nil, "1568099519"},
	} {
		entry := mockEntry{
			Attributes: map[string]*dynamodb.AttributeValue{
				"ID": {S: aws.String(tt.key.String())},
			},
		}
		if !tt.val.IsZero() {
			val := dynamodb.AttributeValue{S: aws.String(tt.val.String())}
			switch tt.kind {
			case assoc.Fileset:
				entry.Attributes[colmap[tt.kind]] = &val
			}
		}
		if tt.labels != nil {
			var labelsEntry dynamodb.AttributeValue
			for _, v := range tt.labels {
				labelsEntry.SS = append(labelsEntry.SS, aws.String(v))
			}
			entry.Attributes["Labels"] = &labelsEntry
		}
		if tt.lastAccessTimeUnix != "" {
			entry.Attributes["LastAccessTime"] = &dynamodb.AttributeValue{N: aws.String(tt.lastAccessTimeUnix)}
		}
		db.mockStore[&entry] = true
	}

	var (
		numFileSets          = new(int)
		thresholdTime        = time.Unix(1572000000, 0)
		numPastThresholdTime = 0
	)
	for _, tt := range []struct {
		gotKind             *int
		wantKind, wantLabel int
		assocKind           assoc.Kind
		wantLabels          []string
	}{
		{numFileSets, 4, 1, assoc.Fileset, []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
	} {
		gotLabel := 0
		err := ass.Scan(ctx, tt.assocKind, assoc.MappingHandlerFunc(func(k digest.Digest, v []digest.Digest, mapkind assoc.Kind, lastAccessTime time.Time, labels []string) {
			if lastAccessTime.After(thresholdTime) {
				numPastThresholdTime++
			}
			switch mapkind {
			case assoc.Fileset:
				*numFileSets++
			default:
				return
			}
			if tt.wantLabels == nil {
				return
			} else if len(tt.wantLabels) != len(labels) {
				return
			}
			numMatch := 0
			for i := 0; i < len(labels); i++ {
				if labels[i] == tt.wantLabels[i] {
					numMatch++
				}
			}
			if numMatch == len(tt.wantLabels) {
				gotLabel++
			}
		}))
		// Reset db.dbscanned to false so that db can be scanned in the next unit test.
		db.dbscanned = false
		if err != nil {
			t.Fatal(err)
		}
		if got, want := *tt.gotKind, tt.wantKind; got != want {
			t.Errorf("kind %v: got %v, want %v", tt.assocKind, got, want)
		}
		if got, want := gotLabel, tt.wantLabel; got != want {
			t.Errorf("label: got %v, want %v", got, want)
		}
	}
	if got, want := numPastThresholdTime, 2; got != want {
		t.Errorf("last access time past threshold: got %v, want %v", got, want)
	}
}

func TestAssocScanInvalidEntries(t *testing.T) {
	var (
		ctx = context.Background()
		db  = &mockdb{mockStore: make(map[*mockEntry]bool)}
		ass = &Assoc{DB: db}
	)
	ass.TableName = mockTable
	for _, tt := range []struct {
		key                digest.Digest
		val                digest.Digest
		lastAccessTimeUnix string
	}{
		{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "1571573191"},
		{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "1572455280"},
		{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "1572448157"},
		{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "1568099519"},
		{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), ""},
		{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "1568099519"},
	} {
		entry := mockEntry{
			Attributes: map[string]*dynamodb.AttributeValue{
				"ID": {S: aws.String(tt.key.String())},
			},
		}
		if !tt.val.IsZero() {
			entry.Attributes[colmap[assoc.Fileset]] = &dynamodb.AttributeValue{S: aws.String(tt.val.String())}
		}
		if tt.lastAccessTimeUnix != "" {
			entry.Attributes["LastAccessTime"] = &dynamodb.AttributeValue{N: aws.String(tt.lastAccessTimeUnix)}
		}
		db.mockStore[&entry] = true
	}

	// Scan should only scan entries with a LastAccessTime column.
	var validEntries int
	err := ass.Scan(ctx, assoc.Fileset, assoc.MappingHandlerFunc(func(k digest.Digest, v []digest.Digest, mapkind assoc.Kind, lastAccessTime time.Time, labels []string) {
		validEntries++
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := validEntries, 5; got != want {
		t.Errorf("got %d valid entries, want %d", got, want)
	}
}

type testSet map[digest.Digest]struct{}

func (t testSet) Contains(k digest.Digest) bool {
	_, ok := t[k]
	return ok
}

func TestCollectWithThreshold(t *testing.T) {
	var (
		ctx              = context.Background()
		db               = &mockdb{mockStore: make(map[*mockEntry]bool)}
		ass              = &Assoc{DB: db}
		threshold        = time.Unix(1000000, 0)
		liveset, deadset = make(testSet), make(testSet)
		keepKeys         = []digest.Digest{reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), reflow.Digester.Rand(nil)}
		ignoreKey        = reflow.Digester.Rand(nil)
	)
	ass.TableName = mockTable

	for _, tt := range []struct {
		name               string
		key                digest.Digest
		lastAccessTimeUnix string
		liveset, deadset   bool
	}{
		{"livesetAfterThreshold", keepKeys[0], "1000001", true, false},
		{"livesetBeforeThreshold", keepKeys[1], "999999", true, false},
		{"deadsetBeforeThreshold", reflow.Digester.Rand(nil), "999998", false, true},
		{"deadsetAfterThreshold", reflow.Digester.Rand(nil), "1000002", false, true},
		{"noSetBeforeThreshold", reflow.Digester.Rand(nil), "999997", false, false},
		{"noSetAfterThreshold", keepKeys[2], "1000003", false, false},
		{"entryNoLastAccessTime", ignoreKey, "", false, true},
	} {
		entry := mockEntry{
			Attributes: map[string]*dynamodb.AttributeValue{
				"ID": {S: aws.String(tt.key.String())},
			},
		}
		if tt.lastAccessTimeUnix != "" {
			entry.Attributes["LastAccessTime"] = &dynamodb.AttributeValue{N: aws.String(tt.lastAccessTimeUnix)}
		}

		db.mockStore[&entry] = true
		if tt.liveset {
			liveset[tt.key] = struct{}{}
		}
		if tt.deadset {
			deadset[tt.key] = struct{}{}
		}
	}
	if got, want := len(db.mockStore), 7; got != want {
		t.Fatalf("got %d mock entries, want %d", got, want)
	}

	if err := ass.CollectWithThreshold(ctx, liveset, deadset, threshold, 300, true); err != nil {
		t.Fatal(err)
	}
	if got, want := len(db.mockStore), 7; got != want {
		t.Fatalf("got %d mock entries, want %d", got, want)
	}
	// Allow db to be scanned for a non-dry run.
	db.dbscanned = false

	if err := ass.CollectWithThreshold(ctx, liveset, deadset, threshold, 300, false); err != nil {
		t.Fatal(err)
	}
	if got, want := len(db.mockStore), 4; got != want {
		t.Fatalf("got %d mock entries, want %d", got, want)
	}
	var ignoreKeyFound bool
	for k := range db.mockStore {
		key, err := reflow.Digester.Parse(*k.Attributes["ID"].S)
		if err != nil {
			t.Fatal(err)
		}
		var keyValid bool
		for _, keepKey := range keepKeys {
			if keepKey == key {
				keyValid = true
				break
			}
		}
		if key == ignoreKey {
			ignoreKeyFound = true
		} else if !keyValid {
			t.Errorf("key %s not found in remaining keys", key)
		}
	}
	if !ignoreKeyFound {
		t.Errorf("key with no LastAccessTime deleted from assoc")
	}
}
