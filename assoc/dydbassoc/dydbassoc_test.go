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

func newMockEntry(kind assoc.Kind, d digest.Digest) *mockEntry {
	return &mockEntry{
		Attributes: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(d.String()),
			},
			colmap[kind]: {
				S: aws.String(d.String()),
			},
		},
		Kind: kind,
	}
}

type mockdb struct {
	dynamodbiface.DynamoDBAPI
	mockStore  map[digest.Digest]*mockEntry
	maxRetries int

	mu         sync.Mutex
	scanned    bool
	retries    int
	numUpdates int
}

// NumUpdates is the total of recorded calls to UpdateItemWithContext.
func (m *mockdb) NumUpdates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numUpdates
}

func (m *mockdb) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, options ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	key, ok := input.Key["ID"]
	if !ok {
		panic("key missing")
	}
	d, err := digest.Parse(aws.StringValue(key.S))
	if err != nil {
		return nil, err
	}

	if _, ok = m.mockStore[d]; ok {
		delete(m.mockStore, d)
		return &dynamodb.DeleteItemOutput{
			Attributes: input.Key,
		}, nil
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockdb) BatchGetItemWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, options ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	m.mu.Lock()
	m.retries = m.retries + 1
	m.mu.Unlock()
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
				d, err := digest.Parse(aws.StringValue(v["ID"].S))
				if err != nil {
					return nil, err
				}
				if res, ok := m.mockStore[d]; ok {
					o.Responses[mockTable] = append(o.Responses[mockTable], res.Attributes)
				}
			}
		}
		o.UnprocessedKeys = map[string]*dynamodb.KeysAndAttributes{mockTable: {}}
		for i := n; i < len(input.RequestItems[mockTable].Keys); i++ {
			v := input.RequestItems[mockTable].Keys[i]
			o.UnprocessedKeys[mockTable].Keys = append(o.UnprocessedKeys[mockTable].Keys, v)
		}
	}
	return o, nil
}

func (m *mockdb) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	m.numUpdates++
	m.mu.Unlock()
	return nil, nil
}

func (m *mockdb) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var output = &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{},
	}
	if m.scanned {
		return output, nil
	}
	for _, k := range m.mockStore {
		// Ignore all entries that do not show LastAccessTime.
		if _, ok := k.Attributes["LastAccessTime"]; !ok {
			continue
		}
		output.Items = append(output.Items, k.Attributes)
	}
	m.scanned = true
	count := int64(len(output.Items))
	output.Count = &count
	output.ScannedCount = &count
	return output, nil
}

// reset clears counters and transient state about interactions with the database,
// but does not clear the store or maxRetries.
func (m *mockdb) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries = 0
	m.scanned = false
	m.numUpdates = 0
}

var kinds = []assoc.Kind{assoc.Fileset, assoc.FilesetV2}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	key := reflow.Digester.Rand(nil)
	kind := assoc.FilesetV2
	db := &mockdb{
		mockStore: map[digest.Digest]*mockEntry{key: newMockEntry(kind, key)},
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
	k := reflow.Digester.Rand(nil)
	kind := assoc.FilesetV2
	key := assoc.Key{Kind: kind, Digest: k}
	batch := assoc.Batch{key: assoc.Result{}}

	db := &mockdb{mockStore: map[digest.Digest]*mockEntry{
		k: newMockEntry(kind, k),
	}}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable

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

func TestParallelBatchGetItem(t *testing.T) {
	var wg sync.WaitGroup
	ctx, _ := flow.WithBackground(context.Background(), &wg)
	count := 10 * 1024
	digests := make([]assoc.Key, count)
	store := make(map[digest.Digest]*mockEntry)
	for i := 0; i < count; i++ {
		kind, d := kinds[rand.Int()%len(kinds)], reflow.Digester.Rand(nil)
		digests[i] = assoc.Key{Kind: kind, Digest: d}
		entry := newMockEntry(kind, d)
		store[d] = entry
	}

	db := &mockdb{maxRetries: 10, mockStore: store}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable
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

func TestInvalidDigest(t *testing.T) {
	pat := `encoding/hex: invalid byte:.*`
	re, err := regexp.Compile(pat)
	if err != nil {
		t.Fatal(err)
	}
	batch := make(assoc.Batch)
	store := make(map[digest.Digest]*mockEntry)
	invalidCountByKind := make(map[assoc.Kind]int)
	var totalInvalidCount int
	for i := 0; i < 1000; i++ {
		entryKind, d := kinds[i%len(kinds)], reflow.Digester.Rand(nil)
		batch[assoc.Key{Kind: entryKind, Digest: d}] = assoc.Result{}
		entry := newMockEntry(entryKind, d)
		if rand.Int()%2 == 0 {
			delete(entry.Attributes, colmap[entryKind])
			entry.Attributes[colmap[entryKind]] = &dynamodb.AttributeValue{
				S: aws.String("corrupted"),
			}
			if _, ok := invalidCountByKind[entryKind]; !ok {
				invalidCountByKind[entryKind] = 0
			}
			invalidCountByKind[entryKind]++
			totalInvalidCount++
		}
		store[d] = entry
	}

	db := &mockdb{mockStore: store}
	ass := &Assoc{DB: db}
	ass.TableName = mockTable
	var wg sync.WaitGroup
	ctx, cancel := flow.WithBackground(context.Background(), &wg)
	err = ass.BatchGet(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(batch), 1000; got != want {
		t.Errorf(fmt.Sprintf("expected %v result keys, got %v", want, got))
	}

	for _, kind := range kinds {
		errCount := 0
		for k, v := range batch {
			if k.Kind == kind && v.Error != nil {
				ok := re.MatchString(v.Error.Error())
				errCount++
				if !ok {
					t.Errorf(fmt.Sprintf("error %s does not match %s", pat, v.Error.Error()))
				}
			}
		}
		if got, want := errCount, invalidCountByKind[kind]; got != want {
			t.Errorf(fmt.Sprintf("expected %v invalid digest keys, got %v", want, got))
		}
	}
	wg.Wait()
	if got, want := db.NumUpdates(), 1000-totalInvalidCount; got != want {
		t.Errorf("got %d updates, want %d", got, want)
	}
	cancel()
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
		db  = &mockdb{mockStore: make(map[digest.Digest]*mockEntry)}
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
		{assoc.FilesetV2, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}, "1571573191"},
		{assoc.FilesetV2, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), nil, "1572455280"},
		{assoc.FilesetV2, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), nil, "1572448157"},
		{assoc.FilesetV2, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), []string{"grail:type=reflow", "grail:user=def@graiobio.com"}, "1568099519"},
		{assoc.FilesetV2, reflow.Digester.Rand(nil), digest.Digest{}, nil, "1568099519"},
	} {
		entry := newMockEntry(tt.kind, tt.key)
		if !tt.val.IsZero() {
			val := dynamodb.AttributeValue{S: aws.String(tt.val.String())}
			entry.Attributes[colmap[tt.kind]] = &val
		} else {
			delete(entry.Attributes, colmap[tt.kind])
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
		db.mockStore[tt.key] = entry
	}

	var (
		thresholdTime = time.Unix(1572000000, 0)
	)
	for _, tt := range []struct {
		assocKinds                             []assoc.Kind
		wantKind, wantLabel, wantPastThreshold int
		wantLabels                             []string
	}{
		{[]assoc.Kind{assoc.Fileset}, 4, 1, 2, []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
		{[]assoc.Kind{assoc.FilesetV2}, 4, 1, 2, []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
		{[]assoc.Kind{assoc.Fileset, assoc.FilesetV2}, 8, 2, 4, []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
	} {
		gotLabel := 0
		gotKind := 0
		numPastThresholdTime := 0
		err := ass.Scan(ctx, tt.assocKinds, assoc.MappingHandlerFunc(func(k digest.Digest, v map[assoc.Kind]digest.Digest, lastAccessTime time.Time, labels []string) {
			var match bool
			for _, kind := range tt.assocKinds {
				if _, ok := v[kind]; ok {
					gotKind++
					match = true
					break
				}
			}
			if !match {
				return
			}
			if lastAccessTime.After(thresholdTime) {
				numPastThresholdTime++
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
		// Reset db.scanned to false so that db can be scanned in the next unit test.
		db.reset()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := gotKind, tt.wantKind; got != want {
			t.Errorf("kinds %v: got %v, want %v", tt.assocKinds, got, want)
		}
		if got, want := gotLabel, tt.wantLabel; got != want {
			t.Errorf("label: got %v, want %v", got, want)
		}
		if got, want := numPastThresholdTime, tt.wantPastThreshold; got != want {
			t.Errorf("last access time past threshold: got %v, want %v", got, want)
		}
	}
}

func TestAssocScanInvalidEntries(t *testing.T) {
	var (
		ctx = context.Background()
		db  = &mockdb{mockStore: make(map[digest.Digest]*mockEntry)}
		ass = &Assoc{DB: db}
	)
	ass.TableName = mockTable
	for _, tt := range []struct {
		key                digest.Digest
		kind               assoc.Kind
		lastAccessTimeUnix string
	}{
		{reflow.Digester.Rand(nil), assoc.Fileset, "1571573191"},
		{reflow.Digester.Rand(nil), assoc.Fileset, "1572455280"},
		{reflow.Digester.Rand(nil), assoc.Fileset, "1572448157"},
		{reflow.Digester.Rand(nil), assoc.Fileset, "1568099519"},
		{reflow.Digester.Rand(nil), assoc.Fileset, ""},
		{reflow.Digester.Rand(nil), assoc.Fileset, "1568099519"},
		{reflow.Digester.Rand(nil), assoc.FilesetV2, "1571573191"},
		{reflow.Digester.Rand(nil), assoc.FilesetV2, "1568099519"},
		{reflow.Digester.Rand(nil), assoc.FilesetV2, ""},
		{reflow.Digester.Rand(nil), assoc.FilesetV2, "1568099519"},
	} {
		entry := newMockEntry(tt.kind, tt.key)
		if tt.lastAccessTimeUnix != "" {
			entry.Attributes["LastAccessTime"] = &dynamodb.AttributeValue{N: aws.String(tt.lastAccessTimeUnix)}
		}
		db.mockStore[tt.key] = entry
	}

	// Scan should only scan entries with a LastAccessTime column.
	var validEntries int
	err := ass.Scan(ctx, []assoc.Kind{assoc.Fileset, assoc.FilesetV2}, assoc.MappingHandlerFunc(func(k digest.Digest, v map[assoc.Kind]digest.Digest, lastAccessTime time.Time, labels []string) {
		validEntries++
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := validEntries, 8; got != want {
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
		db               = &mockdb{mockStore: make(map[digest.Digest]*mockEntry)}
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

		db.mockStore[tt.key] = &entry
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
	db.reset()

	if err := ass.CollectWithThreshold(ctx, liveset, deadset, threshold, 300, false); err != nil {
		t.Fatal(err)
	}
	if got, want := len(db.mockStore), 4; got != want {
		t.Fatalf("got %d mock entries, want %d", got, want)
	}
	var ignoreKeyFound bool
	for _, k := range db.mockStore {
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
