// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dynamodbtask

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
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
	_ "github.com/grailbio/reflow/assoc/dydbassoc"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/test/testutil"
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
		labels = []string{"test=label"}
		mockdb = mockDynamodbPut{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName, Labels: labels}
		runID  = taskdb.RunID(reflow.Digester.Rand(rand.New(rand.NewSource(1))))
		user   = "reflow"
	)
	err := taskb.CreateRun(context.Background(), runID, user)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.pinput.TableName, "mockdynamodb"},
		{*mockdb.pinput.Item[colID].S, runID.ID()},
		{*mockdb.pinput.Item[colID4].S, runID.IDShort()},
		{*mockdb.pinput.Item[colUser].S, user},
		{*mockdb.pinput.Item[colType].S, "run"},
		{*mockdb.pinput.Item[colLabels].SS[0], labels[0]},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestSetRunAttrs(t *testing.T) {
	var (
		mockdb = mockDynamoDBUpdate{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		runID  = taskdb.NewRunID()
		bundle = reflow.Digester.Rand(nil)
		args   = []string{"-a=1", "-b=2"}
	)
	err := taskb.SetRunAttrs(context.Background(), runID, bundle, args)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, runID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":bundle"].S, bundle.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":args"].SS[0], args[0]},
		{*mockdb.uInput.ExpressionAttributeValues[":args"].SS[1], args[1]},
		{*mockdb.uInput.UpdateExpression, fmt.Sprintf("SET %s = :bundle, %s = :args", colBundle, colArgs)},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
	// Verify correct behavior if no args are passed to SetRunAttrs
	taskb = &TaskDB{DB: &mockdb, TableName: mockTableName}
	err = taskb.SetRunAttrs(context.Background(), runID, bundle, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, runID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":bundle"].S, bundle.String()},
		{*mockdb.uInput.UpdateExpression, fmt.Sprintf("SET %s = :bundle", colBundle)},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}

	var emptyArgs []string
	taskb = &TaskDB{DB: &mockdb, TableName: mockTableName}
	err = taskb.SetRunAttrs(context.Background(), runID, bundle, emptyArgs)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, runID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":bundle"].S, bundle.String()},
		{*mockdb.uInput.UpdateExpression, fmt.Sprintf("SET %s = :bundle", colBundle)},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}

}

func TestTaskCreate(t *testing.T) {
	var (
		labels = []string{"test=label"}
		mockdb = mockDynamodbPut{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName, Labels: labels}
		taskID = taskdb.NewTaskID()
		runID  = taskdb.NewRunID()
		flowID = reflow.Digester.Rand(nil)
		uri    = "machineUri"
	)
	err := taskb.CreateTask(context.Background(), taskID, runID, flowID, uri)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.pinput.TableName, "mockdynamodb"},
		{*mockdb.pinput.Item[colID].S, taskID.ID()},
		{*mockdb.pinput.Item[colID4].S, taskID.IDShort()},
		{*mockdb.pinput.Item[colRunID].S, runID.ID()},
		{*mockdb.pinput.Item[colRunID4].S, runID.IDShort()},
		{*mockdb.pinput.Item[colFlowID].S, flowID.String()},
		{*mockdb.pinput.Item[colType].S, "task"},
		{*mockdb.pinput.Item[colType].S, "task"},
		{*mockdb.pinput.Item[colURI].S, "machineUri"},
		{*mockdb.pinput.Item[colLabels].SS[0], labels[0]},
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
		taskID = taskdb.NewTaskID()
		result = reflow.Digester.Rand(nil)
	)
	err := taskb.SetTaskResult(context.Background(), taskID, result)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, taskID.ID()},
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
		taskID  = taskdb.NewTaskID()
		stderr  = reflow.Digester.Rand(nil)
		stdout  = reflow.Digester.Rand(nil)
		inspect = reflow.Digester.Rand(nil)
	)
	err := taskb.SetTaskAttrs(context.Background(), taskID, stdout, stderr, inspect)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, taskID.ID()},
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
		runID     = taskdb.NewRunID()
		keepalive = time.Now().UTC()
	)
	err := taskb.keepalive(context.Background(), digest.Digest(runID), keepalive)
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
		{*mockdb.uInput.UpdateExpression, "SET Keepalive = :ka, #Date = :date"},
		{*mockdb.uInput.ExpressionAttributeNames["#Date"], colDate},
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
		runID  = taskdb.NewRunID()
		mockdb = getMockRunTaskDB()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.RunQuery{ID: runID, User: colUser}
	)
	mockdb.testId = digest.Digest(runID)
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
		{*mockdb.qinput.ExpressionAttributeValues[":testId"].S, runID.ID()},
		{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		{*mockdb.qinput.KeyConditionExpression, colID + " = :testId"},
		{*mockdb.qinput.FilterExpression, "#Type = :type"},
		{runs[0].User, colUser},
		{runs[0].ID.ID(), runID.ID()},
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

	runID := taskdb.RunID(sid)
	query := taskdb.RunQuery{ID: runID, User: colUser}
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
		{*mockdb.qinput.ExpressionAttributeValues[":id4"].S, runID.IDShort()},
		{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		{*mockdb.qinput.KeyConditionExpression, colID4 + " = :id4"},
		{*mockdb.qinput.FilterExpression, "#Type = :type"},
		{runs[0].User, colUser},
		{runs[0].ID.ID(), mockdb.testId.String()},
		{runs[0].Labels["label"], "test"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}

	// Abbrev id that does not match the full id.
	str := id.ShortString(5)
	str = str[:len(str)-2] + fmt.Sprintf("%1x", str[len(str)-1]+1)
	id, err = reflow.Digester.Parse(str)
	if err != nil {
		t.Fatal(err)
	}
	runID = taskdb.RunID(id)
	query = taskdb.RunQuery{ID: runID, User: colUser}
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
		runID       = taskdb.NewRunID()
		expectederr = awserr.New("ValidationException", "The table does not have the specified index", nil)
		mockdb      = getMockRunTaskDB()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.RunQuery{ID: runID}
	)
	mockdb.testId = digest.Digest(runID)
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
		runID       = taskdb.NewRunID()
		expectederr = errors.New("some unknown error")
		mockdb      = getMockRunTaskDB()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.RunQuery{ID: runID}
	)
	mockdb.testId = digest.Digest(runID)
	mockdb.err = expectederr
	tasks, err := taskb.Runs(context.Background(), query)
	if got, want := err, expectederr; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := len(tasks), 0; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

type mockEntry struct {
	Attributes map[string]*dynamodb.AttributeValue
	Kind       taskdb.Kind
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
		runID       = taskdb.NewRunID()
		expectederr = awserr.New("ValidationException", "The table does not have the specified index", nil)
		mockdb      = getmocktaskstaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.TaskQuery{RunID: runID}
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
		runID       = taskdb.NewRunID()
		badTaskID   = taskdb.TaskID(runID)
		expectederr = errors.New("some unknown error")
		mockdb      = getmocktaskstaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.TaskQuery{ID: badTaskID}
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
		runID  = taskdb.NewRunID()
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.TaskQuery{RunID: runID}
	)
	mockdb.id = digest.Digest(runID)
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
		{"value run id", *mockdb.qinput.ExpressionAttributeValues[":rid"].S, runID.ID()},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colRunID + " = :rid"},
		{"run id", tasks[0].RunID.ID(), runID.ID()},
		{"id", tasks[0].ID.ID(), runID.ID()},
		{"result id", tasks[0].ResultID.String(), runID.ID()},
		{"flow id", tasks[0].FlowID.String(), runID.ID()},
		{"stderr", tasks[0].Stderr.String(), runID.ID()},
		{"stdout", tasks[0].Stdout.String(), runID.ID()},
		{"inspect", tasks[0].Inspect.String(), runID.ID()},
		{"uri", tasks[0].URI, mockdb.uri},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksRunIDQueryAwsErrMissingIndex(t *testing.T) {
	var (
		runID       = taskdb.NewRunID()
		expectederr = awserr.New("ValidationException", "The table does not have the specified index", nil)
		mockdb      = getmockquerytaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.TaskQuery{RunID: runID}
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
		runID       = taskdb.NewRunID()
		expectederr = awserr.New("", "some error", nil)
		mockdb      = getmockquerytaskdb()
		taskb       = &TaskDB{DB: mockdb, TableName: mockTableName}
		query       = taskdb.TaskQuery{RunID: runID}
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
		runID  = taskdb.NewRunID()
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		since  = time.Now().UTC().Add(-time.Minute * 10)
		query  = taskdb.TaskQuery{User: "testuser", Since: since}
	)

	date := date(since).Format(dateLayout)
	mockdb.id = digest.Digest(runID)
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
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka "},
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
		query  = taskdb.RunQuery{Since: since}
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
		{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka "},
		{"filter expression", *mockdb.qinput.FilterExpression, "#Type = :type"},
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
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
		query  = taskdb.RunQuery{User: "reflow", Since: since}
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
		{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka "},
		{"filter expression", *mockdb.qinput.FilterExpression, "#User = :user and #Type = :type"},
		{"attribute name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
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
	query := taskdb.RunQuery{User: "reflow", Since: queryTime}
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
			{"key condition", *qinput.KeyConditionExpression, "#Date = :date and Keepalive > :ka "},
			{"filter expression", *qinput.FilterExpression, "#User = :user and #Type = :type"},
			{"attribute name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
			{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
			{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
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
		"user":    new(infra2.User),
		"labels":  make(pool.Labels),
		"taskdb":  new(taskdb.TaskDB),
		"logger":  new(log.Logger),
	}
	config, err := schema.Make(infra.Keys{
		"session": "awssession",
		"user":    "user,user=test",
		"taskdb":  "dynamodbtask",
		"assoc":   fmt.Sprintf("dynamodbassoc,table=%v", table),
		"logger":  "logger",
		"labels":  "kv",
	})
	if err != nil {
		t.Fatal(err)
	}
	var tdb taskdb.TaskDB
	config.Must(&tdb)

	var dynamotaskdb *TaskDB
	config.Must(&dynamotaskdb)

	if got, want := dynamotaskdb.TableName, table; got != want {
		t.Errorf("got %v, want %v", dynamotaskdb.TableName, table)
	}
}

type mockDynamodbScanTasks struct {
	dynamodbiface.DynamoDBAPI
	MockStore []mockEntry
	dbscanned bool
	muScan    sync.Mutex
}

func (m *mockDynamodbScanTasks) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	m.muScan.Lock()
	defer m.muScan.Unlock()
	var output = &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{},
	}
	if m.dbscanned {
		return output, nil
	}
	for i, v := range m.MockStore {
		output.Items = append(output.Items, v.Attributes)
		if i == len(m.MockStore)-1 {
			m.dbscanned = true
		}
	}
	count := int64(len(m.MockStore))
	output.Count = &count
	output.ScannedCount = &count
	return output, nil
}

func TestTaskDBScan(t *testing.T) {
	var (
		ctx    = context.Background()
		mockdb = &mockDynamodbScanTasks{}
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
	)
	for _, tt := range []struct {
		kind     taskdb.Kind
		key      digest.Digest
		val      digest.Digest
		taskType string
		labels   []string
	}{
		{ExecInspect, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "task", []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
		{ExecInspect, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "task", []string{"grail:type=reflow", "grail:user=def@graiobio.com"}},
		{ExecInspect, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "run", []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
		{Stdout, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "run", nil},
		{Stderr, reflow.Digester.Rand(nil), reflow.Digester.Rand(nil), "run", nil},
	} {
		entry := mockEntry{
			Attributes: map[string]*dynamodb.AttributeValue{
				colID:   {S: aws.String(tt.key.String())},
				colType: {S: aws.String(tt.taskType)},
			},
		}
		entry.Attributes[colmap[tt.kind]] = &dynamodb.AttributeValue{S: aws.String(tt.val.String())}
		if tt.labels != nil {
			var labelsEntry dynamodb.AttributeValue
			for _, v := range tt.labels {
				labelsEntry.SS = append(labelsEntry.SS, aws.String(v))
			}
			entry.Attributes[colLabels] = &labelsEntry
		}
		mockdb.MockStore = append(mockdb.MockStore, entry)
	}
	var (
		numExecInspect = new(int)
		numStdout      = new(int)
		numStderr      = new(int)
		numURI         = new(int)
	)
	for _, tt := range []struct {
		gotKind             *int
		wantKind, wantLabel int
		taskdbKind          taskdb.Kind
		wantType            string
		wantLabels          []string
	}{
		{numExecInspect, 2, 1, ExecInspect, "task", []string{"grail:type=reflow", "grail:user=abc@graiobio.com"}},
		{numStdout, 1, 0, Stdout, "run", nil},
		{numStderr, 1, 0, Stderr, "run", nil},
		{numURI, 0, 0, URI, "run", nil},
	} {
		gotLabel := 0
		err := taskb.Scan(ctx, tt.taskdbKind, taskdb.MappingHandlerFunc(func(k, v digest.Digest, mapkind taskdb.Kind, taskType string, labels []string) {
			if taskType != tt.wantType {
				return
			}
			switch mapkind {
			case ExecInspect:
				*numExecInspect++
			case Stdout:
				*numStdout++
			case Stderr:
				*numStderr++
			case URI:
				*numURI++
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
		mockdb.dbscanned = false
		if err != nil {
			t.Fatal(err)
		}
		if got, want := *tt.gotKind, tt.wantKind; got != want {
			t.Errorf("kind %v: got %v, want %v", colmap[tt.taskdbKind], got, want)
		}
		if got, want := gotLabel, tt.wantLabel; got != want {
			t.Errorf("label got %v, want %v", got, want)
		}
	}
}
