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
		labels   = []string{"test=label"}
		mockdb   = mockDynamodbPut{}
		taskb    = &TaskDB{DB: &mockdb, TableName: mockTableName, Labels: labels}
		taskID   = taskdb.NewTaskID()
		runID    = taskdb.NewRunID()
		allocID  = reflow.Digester.Rand(nil)
		flowID   = reflow.Digester.Rand(nil)
		uri      = "machineUri"
		ident    = "ident"
		attempt  = 2
		imgCmdID = taskdb.NewImgCmdID("image", "cmd")
		res      = reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024}
	)
	err := taskb.CreateTask(context.Background(), taskdb.Task{
		ID:        taskID,
		RunID:     runID,
		AllocID:   allocID,
		FlowID:    flowID,
		ImgCmdID:  imgCmdID,
		Ident:     ident,
		Attempt:   attempt,
		Resources: res,
		URI:       uri})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := *mockdb.pinput.Item[colAttempt].N, fmt.Sprintf("%d", attempt); got != want {
		t.Errorf("got %v, want %v", got, want)
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
		{*mockdb.pinput.Item[colAllocID].S, allocID.String()},
		{*mockdb.pinput.Item[colFlowID].S, flowID.String()},
		{*mockdb.pinput.Item[colImgCmdID].S, imgCmdID.ID()},
		{*mockdb.pinput.Item[colIdent].S, ident},
		{*mockdb.pinput.Item[colResources].S, "{\"cpu\":2,\"mem\":4294967296}"},
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

func TestSetTaskUri(t *testing.T) {
	var (
		mockdb = mockDynamoDBUpdate{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		taskID = taskdb.NewTaskID()
		uri    = "some_uri"
	)
	err := taskb.SetTaskUri(context.Background(), taskID, uri)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, taskID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":uri"].S, uri},
		{*mockdb.uInput.UpdateExpression, "SET URI = :uri"},
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

func TestSetRunComplete(t *testing.T) {
	var (
		mockdb    = mockDynamoDBUpdate{}
		taskb     = &TaskDB{DB: &mockdb, TableName: mockTableName}
		runID     = taskdb.NewRunID()
		execLog   = reflow.Digester.Rand(nil)
		sysLog    = reflow.Digester.Rand(nil)
		evalGraph = reflow.Digester.Rand(nil)
		trace     = reflow.Digester.Rand(nil)
		end       = time.Now()
	)
	err := taskb.SetRunComplete(context.Background(), runID, execLog, sysLog, evalGraph, trace, end)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		got, want string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, runID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":endtime"].S, end.UTC().Format(timeLayout)},
		{*mockdb.uInput.ExpressionAttributeValues[":execlog"].S, execLog.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":syslog"].S, sysLog.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":evalgraph"].S, evalGraph.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":trace"].S, trace.String()},
		{
			*mockdb.uInput.UpdateExpression,
			"SET EndTime = :endtime, ExecLog = :execlog, Syslog = :syslog, EvalGraph = :evalgraph, Trace = :trace",
		},
	} {
		if test.want != test.got {
			t.Errorf("got %v, want %v", test.got, test.want)
		}
	}
}

func TestSetTaskComplete(t *testing.T) {
	var (
		mockdb = mockDynamoDBUpdate{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		taskID = taskdb.NewTaskID()
		tdbErr error
		end    = time.Now()
	)
	err := taskb.SetTaskComplete(context.Background(), taskID, tdbErr, end)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		got, want string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, taskID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":endtime"].S, end.UTC().Format(timeLayout)},
		{*mockdb.uInput.UpdateExpression, "SET EndTime = :endtime"},
	} {
		if test.want != test.got {
			t.Errorf("got %v, want %v", test.got, test.want)
		}
	}
	tdbErr = errors.New("some error")
	end = time.Now()
	err = taskb.SetTaskComplete(context.Background(), taskID, tdbErr, end)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		got, want string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, taskID.ID()},
		{*mockdb.uInput.ExpressionAttributeValues[":endtime"].S, end.UTC().Format(timeLayout)},
		{*mockdb.uInput.ExpressionAttributeValues[":error"].S, tdbErr.Error()},
		{*mockdb.uInput.ExpressionAttributeNames["#Err"], "Error"},
		{*mockdb.uInput.UpdateExpression, "SET EndTime = :endtime, #Err = :error"},
	} {
		if test.want != test.got {
			t.Errorf("got %v, want %v", test.got, test.want)
		}
	}
}

func TestStartAlloc(t *testing.T) {
	var (
		mockdb  = mockDynamodbPut{}
		taskb   = &TaskDB{DB: &mockdb, TableName: mockTableName}
		allocID = reflow.NewStringDigest("allocid")
		poolID  = reflow.NewStringDigest("poolid")
		res     = reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024}
		start   = time.Now().Add(-time.Hour)
	)
	err := taskb.StartAlloc(context.Background(), allocID, poolID, res, start)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.pinput.TableName, "mockdynamodb"},
		{*mockdb.pinput.Item[colID].S, allocID.Digest().String()},
		{*mockdb.pinput.Item[colID4].S, allocID.Digest().Short()},
		{*mockdb.pinput.Item[colPoolID].S, poolID.Digest().String()},
		{*mockdb.pinput.Item[colAllocID].S, allocID.String()},
		{*mockdb.pinput.Item[colResources].S, "{\"cpu\":2,\"mem\":4294967296}"},
		{*mockdb.pinput.Item[colType].S, "alloc"},
		{*mockdb.pinput.Item[colStartTime].S, start.UTC().Format(timeLayout)},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestStartPool(t *testing.T) {
	var (
		mockdb = mockDynamodbPut{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		p      = taskdb.Pool{
			PoolID:        reflow.NewStringDigest("poolid"),
			PoolType:      "pool_type",
			Resources:     reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024},
			URI:           "http://some_url",
			Start:         time.Now().Add(-time.Hour),
			ClusterName:   "cluster_name",
			User:          "user@grailbio.com",
			ReflowVersion: "version_x",
		}
	)
	err := taskb.StartPool(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.pinput.TableName, "mockdynamodb"},
		{*mockdb.pinput.Item[colID].S, p.PoolID.Digest().String()},
		{*mockdb.pinput.Item[colID4].S, p.PoolID.Digest().Short()},
		{*mockdb.pinput.Item[colPoolID].S, p.PoolID.String()},
		{*mockdb.pinput.Item[colURI].S, p.URI},
		{*mockdb.pinput.Item[colPoolType].S, p.PoolType},
		{*mockdb.pinput.Item[colResources].S, "{\"cpu\":2,\"mem\":4294967296}"},
		{*mockdb.pinput.Item[colType].S, "pool"},
		{*mockdb.pinput.Item[colStartTime].S, p.Start.UTC().Format(timeLayout)},
		{*mockdb.pinput.Item[colClusterName].S, p.ClusterName},
		{*mockdb.pinput.Item[colUser].S, p.User},
		{*mockdb.pinput.Item[colReflowVersion].S, p.ReflowVersion},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestSetResources(t *testing.T) {
	var (
		mockdb = mockDynamoDBUpdate{}
		taskb  = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id     = reflow.Digester.Rand(nil)
		res    = reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024}
	)
	err := taskb.SetResources(context.Background(), id, res)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, id.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":resources"].S, "{\"cpu\":2,\"mem\":4294967296}"},
		{*mockdb.uInput.UpdateExpression, "SET Resources = :resources"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestKeepIDAlive(t *testing.T) {
	var (
		mockdb    = mockDynamoDBUpdate{}
		taskb     = &TaskDB{DB: &mockdb, TableName: mockTableName}
		runID     = taskdb.NewRunID()
		keepalive = time.Now().UTC()
	)
	err := taskb.KeepIDAlive(context.Background(), digest.Digest(runID), keepalive)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, runID.ID()},
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

func TestSetEndTime(t *testing.T) {
	var (
		mockdb  = mockDynamoDBUpdate{}
		taskb   = &TaskDB{DB: &mockdb, TableName: mockTableName}
		id      = reflow.Digester.Rand(nil)
		endtime = time.Now().UTC()
	)
	err := taskb.SetEndTime(context.Background(), id, endtime)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.uInput.TableName, "mockdynamodb"},
		{*mockdb.uInput.Key[colID].S, id.String()},
		{*mockdb.uInput.ExpressionAttributeValues[":endtime"].S, endtime.Format(timeLayout)},
		{*mockdb.uInput.UpdateExpression, "SET EndTime = :endtime"},
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
	id        digest.Digest
	err       error
}

func (m *mockDynamodbQueryRuns) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	m.qinput = *input
	var id, id4 string
	if _, ok := input.ExpressionAttributeValues[":id"]; ok {
		id = *input.ExpressionAttributeValues[":id"].S
	} else {
		id = m.id.String()
	}
	if _, ok := input.ExpressionAttributeValues[":id4"]; ok {
		id4 = *input.ExpressionAttributeValues[":id4"].S
	} else {
		id4 = m.id.HexN(4)
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
	mockdb.id = digest.Digest(runID)
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
		{*mockdb.qinput.ExpressionAttributeValues[":keyval"].S, runID.ID()},
		{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		{*mockdb.qinput.KeyConditionExpression, colID + " = :keyval"},
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
	mockdb.id = id

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
		{*mockdb.qinput.ExpressionAttributeValues[":keyval"].S, runID.IDShort()},
		{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		{*mockdb.qinput.KeyConditionExpression, colID4 + " = :keyval"},
		{*mockdb.qinput.FilterExpression, "#Type = :type"},
		{runs[0].User, colUser},
		{runs[0].ID.ID(), mockdb.id.String()},
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

func TestRunsSinceQuery(t *testing.T) {
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

func TestRunsTimeBucketQuery(t *testing.T) {
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
			{"filter expression", *qinput.FilterExpression, "#Type = :type and #User = :user"},
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

func TestRunsSinceUserQuery(t *testing.T) {
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
		{"filter expression", *mockdb.qinput.FilterExpression, "#Type = :type and #User = :user"},
		{"attribute name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

type mockEntry struct {
	Attributes map[string]*dynamodb.AttributeValue
	Kind       taskdb.Kind
}

type mockDynamodbQueryTasks struct {
	dynamodbiface.DynamoDBAPI
	mu          sync.Mutex
	qinput      dynamodb.QueryInput
	qinputs     []dynamodb.QueryInput
	id          digest.Digest
	user        string
	keepalive   time.Time
	starttime   time.Time
	uri         string
	ident       string
	err         error
	multiOutput bool
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
	output := &dynamodb.QueryOutput{
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
				colImgCmdID:  &dynamodb.AttributeValue{S: aws.String(id)},
				colIdent:     &dynamodb.AttributeValue{S: aws.String(m.ident)},
			},
		},
	}

	if m.multiOutput {
		output.LastEvaluatedKey = map[string]*dynamodb.AttributeValue{
			"key": &dynamodb.AttributeValue{S: aws.String("value")},
		}
	}

	return output, m.err
}

func getmocktaskstaskdb() *mockDynamodbQueryTasks {
	return &mockDynamodbQueryTasks{starttime: time.Now(), keepalive: time.Now(), user: colUser}
}

func getmockquerytaskdb() *mockDynamodbQueryTasks {
	return &mockDynamodbQueryTasks{
		starttime: time.Now(),
		keepalive: time.Now(),
		user:      "testuser",
		uri:       "uri",
	}
}

func TestTasksIDQuery(t *testing.T) {
	var (
		taskID = taskdb.NewTaskID()
		mockdb = getmocktaskstaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.TaskQuery{ID: taskID}
	)
	mockdb.id = digest.Digest(taskID)
	tasks, err := taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(tasks), 1; actual != expected {
		t.Fatalf("expected %v runs, got %v", expected, actual)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.qinput.TableName, "mockdynamodb"},
		{*mockdb.qinput.IndexName, idIndex},
		{*mockdb.qinput.ExpressionAttributeValues[":type"].S, "task"},
		{*mockdb.qinput.ExpressionAttributeValues[":keyval"].S, taskID.ID()},
		{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		{*mockdb.qinput.KeyConditionExpression, colID + " = :keyval"},
		{*mockdb.qinput.FilterExpression, "#Type = :type"},
		{tasks[0].ID.ID(), taskID.ID()},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksIDShortQuery(t *testing.T) {
	var (
		id     = reflow.Digester.Rand(nil)
		mockdb = getmocktaskstaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
	)
	mockdb.id = id

	sid := id
	sid.Truncate(4)

	taskID := taskdb.TaskID(sid)
	query := taskdb.TaskQuery{ID: taskID}
	tasks, err := taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for _, test := range []struct {
		actual   string
		expected string
	}{
		{*mockdb.qinput.TableName, "mockdynamodb"},
		{*mockdb.qinput.IndexName, id4Index},
		{*mockdb.qinput.ExpressionAttributeValues[":type"].S, "task"},
		{*mockdb.qinput.ExpressionAttributeValues[":keyval"].S, taskID.IDShort()},
		{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		{*mockdb.qinput.KeyConditionExpression, colID4 + " = :keyval"},
		{*mockdb.qinput.FilterExpression, "#Type = :type"},
		{tasks[0].ID.ID(), mockdb.id.String()},
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
	taskID = taskdb.TaskID(id)
	query = taskdb.TaskQuery{ID: taskID}
	tasks, err = taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tasks), 0; got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestTasksRunIDQuery(t *testing.T) {
	var (
		id     = taskdb.NewRunID()
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.TaskQuery{RunID: id}
	)
	mockdb.id = digest.Digest(id)
	mockdb.uri = "execURI"
	mockdb.ident = "testident"
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
		{"value run id", *mockdb.qinput.ExpressionAttributeValues[":keyval"].S, id.ID()},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colRunID + " = :keyval"},
		{"run id", tasks[0].RunID.ID(), id.ID()},
		{"id", tasks[0].ID.ID(), id.ID()},
		{"result id", tasks[0].ResultID.String(), id.ID()},
		{"flow id", tasks[0].FlowID.String(), id.ID()},
		{"stderr", tasks[0].Stderr.String(), id.ID()},
		{"stdout", tasks[0].Stdout.String(), id.ID()},
		{"inspect", tasks[0].Inspect.String(), id.ID()},
		{"uri", tasks[0].URI, mockdb.uri},
		{"exec id", tasks[0].ImgCmdID.ID(), id.ID()},
		{"ident", string(tasks[0].Ident), "testident"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksImgCmdIDQuery(t *testing.T) {
	var (
		image    = "img"
		cmd      = "cmd"
		imgCmdID = taskdb.NewImgCmdID(image, cmd)
		mockdb   = getmockquerytaskdb()
		taskb    = &TaskDB{DB: mockdb, TableName: mockTableName}
		query    = taskdb.TaskQuery{ImgCmdID: imgCmdID}
	)
	id, err := reflow.Digester.Parse(imgCmdID.ID())
	if err != nil {
		t.Fatal(err)
	}
	mockdb.id = id
	mockdb.uri = "execURI"
	mockdb.ident = "testident"
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
		{"index", *mockdb.qinput.IndexName, imgCmdIDIndex},
		{"value exec id", *mockdb.qinput.ExpressionAttributeValues[":keyval"].S, id.String()},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colImgCmdID + " = :keyval"},
		{"run id", tasks[0].RunID.ID(), id.String()},
		{"id", tasks[0].ID.ID(), id.String()},
		{"result id", tasks[0].ResultID.String(), id.String()},
		{"flow id", tasks[0].FlowID.String(), id.String()},
		{"stderr", tasks[0].Stderr.String(), id.String()},
		{"stdout", tasks[0].Stdout.String(), id.String()},
		{"inspect", tasks[0].Inspect.String(), id.String()},
		{"uri", tasks[0].URI, mockdb.uri},
		{"exec id", tasks[0].ImgCmdID.ID(), id.String()},
		{"ident", string(tasks[0].Ident), "testident"},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksIdentQuery(t *testing.T) {
	var (
		ident  = "testident"
		id     = reflow.Digester.Rand(nil)
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		query  = taskdb.TaskQuery{Ident: ident}
	)
	mockdb.id = id
	mockdb.uri = "execURI"
	mockdb.ident = ident
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
		{"index", *mockdb.qinput.IndexName, identIndex},
		{"value ident", *mockdb.qinput.ExpressionAttributeValues[":keyval"].S, ident},
		{"key condition", *mockdb.qinput.KeyConditionExpression, colIdent + " = :keyval"},
		{"run id", tasks[0].RunID.ID(), id.String()},
		{"id", tasks[0].ID.ID(), id.String()},
		{"result id", tasks[0].ResultID.String(), id.String()},
		{"flow id", tasks[0].FlowID.String(), id.String()},
		{"stderr", tasks[0].Stderr.String(), id.String()},
		{"stdout", tasks[0].Stdout.String(), id.String()},
		{"inspect", tasks[0].Inspect.String(), id.String()},
		{"uri", tasks[0].URI, mockdb.uri},
		{"behavior id", tasks[0].ImgCmdID.ID(), id.String()},
		{"ident", string(tasks[0].Ident), ident},
	} {
		if test.expected != test.actual {
			t.Errorf("expected %s, got %v", test.expected, test.actual)
		}
	}
}

func TestTasksSinceQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
		since  = time.Now().UTC().Add(-time.Minute * 10)
		query  = taskdb.TaskQuery{Since: since}
	)
	date := date(since).Format(dateLayout)
	_, err := taskb.Tasks(context.Background(), query)
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
		{"type", *mockdb.qinput.ExpressionAttributeValues[":type"].S, "task"},
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

func TestTasksTimeBucketQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, TableName: mockTableName}
	)
	queryTime := time.Now().UTC().Add(-time.Hour * 24)
	// Make sure we don't hit an hour boundary. This can result in one extra query.
	if queryTime.Truncate(time.Hour) == queryTime {
		queryTime = queryTime.Add(time.Minute)
	}
	query := taskdb.TaskQuery{Since: queryTime}
	_, err := taskb.Tasks(context.Background(), query)
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
			{"type", *qinput.ExpressionAttributeValues[":type"].S, "task"},
			{"date", *qinput.ExpressionAttributeValues[":date"].S, date},
			{"keepalive", *qinput.ExpressionAttributeValues[":ka"].S, ka},
			{"key condition", *qinput.KeyConditionExpression, "#Date = :date and Keepalive > :ka "},
			{"filter expression", *qinput.FilterExpression, "#Type = :type"},
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

func TestTasksLimitQuery(t *testing.T) {
	var (
		ident        = "testident"
		id           = reflow.Digester.Rand(nil)
		mockdb       = getmockquerytaskdb()
		taskb        = &TaskDB{DB: mockdb, TableName: mockTableName}
		limit  int64 = 5
		query        = taskdb.TaskQuery{Ident: ident, Limit: limit}
	)
	mockdb.id = id
	mockdb.uri = "execURI"
	mockdb.ident = ident
	mockdb.multiOutput = true
	tasks, err := taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tasks), int(limit); got != want {
		t.Errorf("got %d, want %d", got, want)
	}
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
		badTaskID   = taskdb.NewTaskID()
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
