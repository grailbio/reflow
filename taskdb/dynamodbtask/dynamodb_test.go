package dynamodbtask

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/reflow/assoc"

	"github.com/grailbio/reflow/test/testutil"

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
	_ "github.com/grailbio/reflow/assoc/dydbassoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
)

const (
	mockTableName = "mockdynamodb"
)

type mockDynamodbPut struct {
	dynamodbiface.DynamoDBAPI
	pinput dynamodb.PutItemInput
	err    error
}

func (m *mockDynamodbPut) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	m.pinput = *input
	return &dynamodb.PutItemOutput{}, m.err
}

func TestRunCreate(t *testing.T) {
	var (
		mockdb = mockDynamodbPut{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id     = reflow.Digester.Rand(rand.New(rand.NewSource(1)))
		labels = pool.Labels{"test": "label"}
		user   = "reflow"
	)
	err := taskb.CreateRun(context.Background(), id, labels, user)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.pinput.TableName, "mockdynamodb"},
		{*mockdb.pinput.Item[colID].S, id.String()},
		{*mockdb.pinput.Item[colID4].S, id.HexN(4)},
		{*mockdb.pinput.Item[colUser].S, user},
		{*mockdb.pinput.Item[colType].S, "run"},
		{*mockdb.pinput.Item[colLabels].SS[0], "test=label"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTaskCreate(t *testing.T) {
	var (
		mockdb = mockDynamodbPut{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id     = reflow.Digester.Rand(nil)
		runid  = reflow.Digester.Rand(nil)
		flowid = reflow.Digester.Rand(nil)
		uri    = "machineUri"
	)
	err := taskb.CreateTask(context.Background(), id, runid, flowid, uri)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.pinput.TableName, "mockdynamodb"},
		{*mockdb.pinput.Item[colID].S, id.String()},
		{*mockdb.pinput.Item[colID4].S, id.HexN(4)},
		{*mockdb.pinput.Item[colRunID].S, runid.String()},
		{*mockdb.pinput.Item[colRunID4].S, runid.HexN(4)},
		{*mockdb.pinput.Item[colFlowID].S, flowid.String()},
		{*mockdb.pinput.Item[colType].S, "task"},
		{*mockdb.pinput.Item[colType].S, "task"},
		{*mockdb.pinput.Item[colURI].S, "machineUri"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

type mockDynamoDBUpdate struct {
	dynamodbiface.DynamoDBAPI
	uInput dynamodb.UpdateItemInput
	err    error
}

func (m *mockDynamoDBUpdate) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.uInput = *input
	return &dynamodb.UpdateItemOutput{}, m.err
}

func TestSetTaskResult(t *testing.T) {
	var (
		mockdb = mockDynamoDBUpdate{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id     = reflow.Digester.Rand(nil)
		result = reflow.Digester.Rand(nil)
	)
	err := taskb.SetTaskResult(context.Background(), id, result)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, id.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":result"].S, result.String()},
		{*mockdb.uInput.UpdateExpression, "SET ResultID = :result"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestSetTaskAttrs(t *testing.T) {
	var (
		mockdb  = mockDynamoDBUpdate{}
		taskb   = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id      = reflow.Digester.Rand(nil)
		stderr  = reflow.Digester.Rand(nil)
		stdout  = reflow.Digester.Rand(nil)
		inspect = reflow.Digester.Rand(nil)
	)
	err := taskb.SetTaskAttrs(context.Background(), id, stdout, stderr, inspect)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, id.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":stderr"].S, stderr.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":stdout"].S, stdout.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":inspect"].S, inspect.String()},
		{*mockdb.uInput.UpdateExpression, "SET Stdout = :stdout, Stderr = :stderr, Inspect = :inspect"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestKeepalive(t *testing.T) {
	var (
		mockdb    = mockDynamoDBUpdate{}
		taskb     = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id        = reflow.Digester.Rand(nil)
		keepalive = time.Now().UTC()
	)
	err := taskb.Keepalive(context.Background(), id, keepalive)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.ExpressionAttributeValues[":ka"].S, keepalive.Format(timeLayout)},
		{*mockdb.uInput.ExpressionAttributeValues[":date"].S, date(keepalive).Format(dateLayout)},
		{*mockdb.uInput.UpdateExpression, "SET Keepalive = :ka, Date = :date"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

type mockDynamodbQueryRuns struct {
	dynamodbiface.DynamoDBAPI
	qinput    dynamodb.QueryInput
	user      string
	keepalive time.Time
	starttime time.Time
	testId    digest.Digest
	err       error
}

func (m *mockDynamodbQueryRuns) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	m.qinput = *input
	var id, id4 string
	if _, ok := input.ExpressionAttributeValues[":id"]; ok {
		id = *input.ExpressionAttributeValues[":id"].S
	} else {
		id = m.testId.String()
	}
	if _, ok := input.ExpressionAttributeValues[":id4"]; ok {
		id4 = *input.ExpressionAttributeValues[":id4"].S
	} else {
		id4 = m.testId.HexN(4)
	}
	return &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			map[string]*dynamodb.AttributeValue{
				colID:        &dynamodb.AttributeValue{S: aws.String(id)},
				colID4:       &dynamodb.AttributeValue{S: aws.String(id4)},
				colUser:      &dynamodb.AttributeValue{S: aws.String(m.user)},
				colLabels:    &dynamodb.AttributeValue{SS: []*string{aws.String("label=test")}},
				colKeepalive: &dynamodb.AttributeValue{S: aws.String(m.keepalive.Format(timeLayout))},
				colStartTime: &dynamodb.AttributeValue{S: aws.String(m.starttime.Format(timeLayout))},
			},
		},
	}, m.err
}

func getMockRunTaskDB() *mockDynamodbQueryRuns {
	return &mockDynamodbQueryRuns{starttime: time.Now(), keepalive: time.Now(), user: colUser}
}

func TestRunsIDQuery(t *testing.T) {
	var (
		id     = reflow.Digester.Rand(nil)
		mockdb = getMockRunTaskDB()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.Query{ID: id, User: colUser}
	)
	mockdb.testId = id
	runs, err := taskb.Runs(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(runs), 1; actual != expected {
		t.Fatalf("expected %v runs, got %v", expected, actual)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.qinput.TableName, "mockdynamodb"},
		{*mockdb.qinput.IndexName, idIndex},
		{*mockdb.qinput.ExpressionAttributeValues[":type"].S, "run"},
		{*mockdb.qinput.ExpressionAttributeValues[":testId"].S, id.String()},
		{*mockdb.qinput.KeyConditionExpression, colID + " = :testId"},
		{*mockdb.qinput.FilterExpression, colType + " = :type"},
		{runs[0].User, colUser},
		{runs[0].ID.String(), id.String()},
		{runs[0].Labels["label"], "test"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestRunsIDShortQuery(t *testing.T) {
	var (
		id     = reflow.Digester.Rand(nil)
		mockdb = getMockRunTaskDB()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
	)
	mockdb.testId = id

	sid := id
	sid.Truncate(4)
	query := taskdb.Query{ID: sid, User: colUser}
	runs, err := taskb.Runs(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 1; got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.qinput.TableName, "mockdynamodb"},
		{*mockdb.qinput.IndexName, id4Index},
		{*mockdb.qinput.ExpressionAttributeValues[":type"].S, "run"},
		{*mockdb.qinput.ExpressionAttributeValues[":id4"].S, sid.HexN(4)},
		{*mockdb.qinput.KeyConditionExpression, colID4 + " = :id4"},
		{*mockdb.qinput.FilterExpression, colType + " = :type"},
		{runs[0].User, colUser},
		{runs[0].ID.String(), mockdb.testId.String()},
		{runs[0].Labels["label"], "test"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}

	// Abbrev id that does not match the full id.
	str := id.ShortString(5)
	str = str[:len(str)-2] + fmt.Sprintf("%1x", str[len(str)-1]+1)
	sid, err = reflow.Digester.Parse(str)
	if err != nil {
		t.Fatal(err)
	}
	query = taskdb.Query{ID: sid, User: colUser}
	runs, err = taskb.Runs(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 0; got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestRunsAwsErrMissingIndex(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		expectederr = awserr.New("ValidationException", "The table does not have the specified index", nil)
		mockdb      = getMockRunTaskDB()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.Query{ID: id}
	)
	mockdb.testId = id
	mockdb.err = expectederr
	tasks, err := taskb.Runs(context.Background(), query)
	awserr, ok := err.(*errors.Error).Err.(awserr.Error)
	if !ok {
		t.Fatalf("expected awserr.Error, got %v", err)
	}
	if got, want := awserr.Message(), expectederr.Message(); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestRunsUnknownError(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		expectederr = errors.New("some unknown error")
		mockdb      = getMockRunTaskDB()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.Query{ID: id}
	)
	mockdb.testId = id
	mockdb.err = expectederr
	tasks, err := taskb.Runs(context.Background(), query)
	if got, want := err, expectederr; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

type mockDynamodbQueryTasks struct {
	dynamodbiface.DynamoDBAPI
	mu        sync.Mutex
	qinput    dynamodb.QueryInput
	qinputs   []dynamodb.QueryInput
	id        digest.Digest
	user      string
	keepalive time.Time
	starttime time.Time
	uri       string
	err       error
}

func (m *mockDynamodbQueryTasks) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	m.qinput = *input
	m.qinputs = append(m.qinputs, *input)
	m.mu.Unlock()
	var id, id4 string
	id = m.id.String()
	if !m.id.IsZero() {
		id4 = m.id.HexN(8)
	}
	return &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			map[string]*dynamodb.AttributeValue{
				colID:        &dynamodb.AttributeValue{S: aws.String(id)},
				colID4:       &dynamodb.AttributeValue{S: aws.String(id4)},
				colUser:      &dynamodb.AttributeValue{S: aws.String(m.user)},
				colLabels:    &dynamodb.AttributeValue{SS: []*string{aws.String("label=test")}},
				colKeepalive: &dynamodb.AttributeValue{S: aws.String(m.keepalive.Format(timeLayout))},
				colStartTime: &dynamodb.AttributeValue{S: aws.String(m.starttime.Format(timeLayout))},
				colFlowID:    &dynamodb.AttributeValue{S: aws.String(id)},
				colResultID:  &dynamodb.AttributeValue{S: aws.String(id)},
				colRunID:     &dynamodb.AttributeValue{S: aws.String(id)},
				colStdout:    &dynamodb.AttributeValue{S: aws.String(id)},
				colStderr:    &dynamodb.AttributeValue{S: aws.String(id)},
				colInspect:   &dynamodb.AttributeValue{S: aws.String(id)},
				colURI:       &dynamodb.AttributeValue{S: aws.String(m.uri)},
			},
		},
	}, m.err
}

func getmocktaskstaskdb() *mockDynamodbQueryTasks {
	return &mockDynamodbQueryTasks{starttime: time.Now(), keepalive: time.Now(), user: colUser}
}

func TestTasksAwsErrMissingIndex(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		expectederr = awserr.New("ValidationException", "The table does not have the specified index", nil)
		mockdb      = getmocktaskstaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.Query{RunID: id}
	)
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	awserr, ok := err.(*errors.Error).Err.(awserr.Error)
	if !ok {
		t.Fatal("expected awserr.Error")
	}
	if got, want := awserr.Message(), expectederr.Message(); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestTasksUnknownError(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		expectederr = errors.New("some unknown error")
		mockdb      = getmocktaskstaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.Query{ID: id}
	)
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	if got, want := err, expectederr; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func getmockquerytaskdb() *mockDynamodbQueryTasks {
	return &mockDynamodbQueryTasks{
		starttime: time.Now(),
		keepalive: time.Now(),
		user:      "testuser",
		uri:       "uri",
	}
}

func TestTasksRunIDQuery(t *testing.T) {
	var (
		id     = reflow.Digester.Rand(nil)
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.Query{RunID: id}
	)
	mockdb.id = id
	mockdb.uri = "execURI"
	tasks, err := taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := *mockdb.qinput.TableName, "mockdynamodb"; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for _, test := range []struct {
		name     string
		actual   string
		expected string
	}{
		{"table", *mockdb.qinput.TableName, "mockdynamodb"},
		{"index", *mockdb.qinput.IndexName, runIDIndex},
		{"value run id", *mockdb.qinput.ExpressionAttributeValues[":rid"].S, id.String()},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colRunID + " = :rid"},
		{"run id", tasks[0].RunID.String(), id.String()},
		{"id", tasks[0].ID.String(), id.String()},
		{"result id", tasks[0].ResultID.String(), id.String()},
		{"flow id", tasks[0].FlowID.String(), id.String()},
		{"stderr", tasks[0].Stderr.String(), id.String()},
		{"stdout", tasks[0].Stdout.String(), id.String()},
		{"inspect", tasks[0].Inspect.String(), id.String()},
		{"uri", tasks[0].URI, mockdb.uri},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksRunIDQueryAwsErrMissingIndex(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		expectederr = awserr.New("ValidationException", "The table does not have the specified index", nil)
		mockdb      = getmockquerytaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.Query{RunID: id}
	)
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	awserr, ok := err.(*errors.Error).Err.(awserr.Error)
	if !ok {
		t.Fatal("expected awserror.Err")
	}
	if got, want := awserr.Message(), expectederr.Message(); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestTasksRunIDQueryOtherError(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		expectederr = awserr.New("", "some error", nil)
		mockdb      = getmockquerytaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.Query{RunID: id}
	)
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	awserr, ok := err.(awserr.Error)
	if !ok {
		t.Fatal("expected awserror.Err")
	}
	if got, want := awserr.Message(), expectederr.Message(); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestTasksUserQueryTimeBucket(t *testing.T) {
	var (
		id     = reflow.Digester.Rand(nil)
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		since  = time.Now().UTC().Add(-time.Minute * 10)
		query  = taskdb.Query{User: "testuser", Since: since}
	)

	date := date(since).Format(dateLayout)
	mockdb.id = id
	tasks, err := taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := *mockdb.qinput.TableName, "mockdynamodb"; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 1; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(mockdb.qinput.ExpressionAttributeValues), 3; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	for _, test := range []struct {
		name     string
		actual   interface{}
		expected interface{}
	}{
		{"table", *mockdb.qinput.TableName, "mockdynamodb"},
		{"index", *mockdb.qinput.IndexName, dateKeepaliveIndex},
		{"len(attribute_values)", len(mockdb.qinput.ExpressionAttributeValues), 3},
		{"date", *mockdb.qinput.ExpressionAttributeValues[":date"].S, date},
		{"user", *mockdb.qinput.ExpressionAttributeValues[":user"].S, query.User},
		{"attribute name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colDate + " = :date and " + colKeepalive + " > :ka "},
		{"filter expression", *mockdb.qinput.FilterExpression, "#User = :user"},
	} {
		if test.expected != test.actual {
			t.Errorf("%v: expected %s, got %v", test.name, test.expected, test.actual)
		}
	}
	ka, err := time.Parse(timeLayout, *mockdb.qinput.ExpressionAttributeValues[":ka"].S)
	if err != nil {
		t.Fatal(err)
	}
	since, err = time.Parse(timeLayout, since.Format(timeLayout))
	if err != nil {
		t.Fatal(err)
	}
	if !ka.UTC().After(since) && !ka.UTC().Equal(since) {
		t.Errorf("expected %v after %v", ka.UTC(), since.UTC())
	}
}

func TestTasksQueryTimeBucketSince(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		since  = time.Now().UTC().Add(-time.Minute * 10)
		query  = taskdb.Query{Since: since}
	)
	date := date(since).Format(dateLayout)
	_, err := taskb.Runs(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		actual   string
		expected string
	}{
		{"table", *mockdb.qinput.TableName, "mockdynamodb"},
		{"index", *mockdb.qinput.IndexName, dateKeepaliveIndex},
		{"type", *mockdb.qinput.ExpressionAttributeValues[":type"].S, "run"},
		{"date", *mockdb.qinput.ExpressionAttributeValues[":date"].S, date},
		{"keepalive", *mockdb.qinput.ExpressionAttributeValues[":ka"].S, since.Format(timeLayout)},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colDate + " = :date and " + colKeepalive + " > :ka "},
		{"filter expression", *mockdb.qinput.FilterExpression, colType + " = :type"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksQueryTimeBucketUser(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		since  = time.Now().UTC()
		query  = taskdb.Query{User: "pgopal", Since: since}
	)
	date := date(since).Format(dateLayout)
	_, err := taskb.Runs(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		actual   string
		expected string
	}{
		{"table", *mockdb.qinput.TableName, "mockdynamodb"},
		{"index", *mockdb.qinput.IndexName, dateKeepaliveIndex},
		{"type", *mockdb.qinput.ExpressionAttributeValues[":type"].S, "run"},
		{"date", *mockdb.qinput.ExpressionAttributeValues[":date"].S, date},
		{"keepalive", *mockdb.qinput.ExpressionAttributeValues[":ka"].S, since.UTC().Format(timeLayout)},
		{"user", *mockdb.qinput.ExpressionAttributeValues[":user"].S, query.User},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colDate + " = :date and " + colKeepalive + " > :ka "},
		{"filter expression", *mockdb.qinput.FilterExpression, "#User = :user and " + colType + " = :type"},
		{"name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksQueryTimeBucketRun(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
	)
	queryTime := time.Now().UTC().Add(-time.Hour * 24)
	// Make sure we don't hit an hour boundary. This can result in one extra query.
	if queryTime.Truncate(time.Hour) == queryTime {
		queryTime = queryTime.Add(time.Minute)
	}
	query := taskdb.Query{User: "pgopal", Since: queryTime}
	_, err := taskb.Runs(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(mockdb.qinputs), 2; got != want {
		t.Fatalf("expected %v sub queries, got %v", want, got)
	}
	//	log.Printf("before: %v", mockdb.qinputs)
	sort.Slice(mockdb.qinputs, func(i, j int) bool {
		di, _ := time.Parse(dateLayout, *mockdb.qinputs[i].ExpressionAttributeValues[":date"].S)
		dj, _ := time.Parse(dateLayout, *mockdb.qinputs[j].ExpressionAttributeValues[":date"].S)
		if di.Before(dj) {
			return true
		}
		_, ok1 := mockdb.qinputs[i].ExpressionAttributeValues[":ka"]
		_, ok2 := mockdb.qinputs[j].ExpressionAttributeValues[":ka"]
		if ok1 && ok2 {
			hi, _ := time.Parse(timeLayout, *mockdb.qinputs[i].ExpressionAttributeValues[":ka"].S)
			hj, _ := time.Parse(timeLayout, *mockdb.qinputs[j].ExpressionAttributeValues[":ka"].S)
			return hi.Before(hj)
		}
		return i < j
	})
	// log.Printf("after: %v", mockdb.qinputs)
	ka := queryTime.Format(timeLayout)
	for _, qinput := range mockdb.qinputs {
		date := date(queryTime).Format(dateLayout)
		for _, test := range []struct {
			name     string
			actual   string
			expected string
		}{
			{"table", *qinput.TableName, "mockdynamodb"},
			{"index", *qinput.IndexName, dateKeepaliveIndex},
			{"type", *qinput.ExpressionAttributeValues[":type"].S, "run"},
			{"date", *qinput.ExpressionAttributeValues[":date"].S, date},
			{"keepalive", *qinput.ExpressionAttributeValues[":ka"].S, ka},
			{"user", *qinput.ExpressionAttributeValues[":user"].S, query.User},
			{"key condition", *qinput.KeyConditionExpression, "Date = :date and Keepalive > :ka "},
			{"filter expression", *qinput.FilterExpression, "#User = :user and " + colType + " = :type"},
			{"name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
		} {
			if test.expected != test.actual {
				t.Errorf("expected %s, got %v", test.expected, test.actual)
			}
		}
		queryTime = queryTime.Add(time.Hour * 24)
	}
}

func TestDydbTaskdbInfra(t *testing.T) {
	const table = "reflow-unittest"
	testutil.SkipIfNoCreds(t)
	var schema = infra.Schema{
		"session": new(session.Session),
		"assoc":   new(assoc.Assoc),
		"user":    new(reflow.User),
		"labels":  make(pool.Labels),
		"taskdb":  new(taskdb.TaskDB),
		"logger":  new(log.Logger),
	}
	config, err := schema.Make(infra.Keys{
		"session": "github.com/grailbio/infra/aws.Session",
		"user":    "github.com/grailbio/reflow.User,user=test",
		"taskdb":  "github.com/grailbio/reflow/taskdb/dynamodbtask.TaskDB",
		"assoc":   fmt.Sprintf("github.com/grailbio/reflow/assoc/dydbassoc.Assoc,table=%v", table),
		"logger":  "github.com/grailbio/reflow/log.Logger",
		"labels":  "github.com/grailbio/reflow/pool.Labels",
	})
	if err != nil {
		t.Fatal(err)
	}
	var tdb taskdb.TaskDB
	config.Must(&tdb)
	dynamotaskdb, ok := tdb.(*TaskDB)
	if !ok {
		t.Fatalf("%v is not an dynamodbtask", reflect.TypeOf(tdb))
	}
	if got, want := dynamotaskdb.TableName, table; got != want {
		t.Errorf("got %v, want %v", dynamotaskdb.TableName, table)
	}
}
