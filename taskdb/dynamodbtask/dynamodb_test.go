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
	"github.com/grailbio/reflow/assoc/dydbassoc"
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
		taskb  = &TaskDB{DB: &mockdb, Labels: labels}
		runID  = taskdb.RunID(reflow.Digester.Rand(rand.New(rand.NewSource(1))))
		user   = "reflow"
	)
	taskb.TableName = mockTableName
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
		taskb  = &TaskDB{DB: &mockdb}
		runID  = taskdb.NewRunID()
		bundle = reflow.Digester.Rand(nil)
		args   = []string{"-a=1", "-b=2"}
	)
	taskb.TableName = mockTableName
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
	taskb = &TaskDB{DB: &mockdb}
	taskb.TableName = mockTableName
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
	taskb = &TaskDB{DB: &mockdb}
	taskb.TableName = mockTableName
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
		taskb    = &TaskDB{DB: &mockdb, Labels: labels}
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
	taskb.TableName = mockTableName
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
		taskb  = &TaskDB{DB: &mockdb}
		taskID = taskdb.NewTaskID()
		result = reflow.Digester.Rand(nil)
	)
	taskb.TableName = mockTableName
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
		taskb  = &TaskDB{DB: &mockdb}
		taskID = taskdb.NewTaskID()
		uri    = "some_uri"
	)
	taskb.TableName = mockTableName
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
		taskb   = &TaskDB{DB: &mockdb}
		taskID  = taskdb.NewTaskID()
		stderr  = reflow.Digester.Rand(nil)
		stdout  = reflow.Digester.Rand(nil)
		inspect = reflow.Digester.Rand(nil)
	)
	taskb.TableName = mockTableName
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
		taskb     = &TaskDB{DB: &mockdb}
		runID     = taskdb.NewRunID()
		execLog   = reflow.Digester.Rand(nil)
		sysLog    = reflow.Digester.Rand(nil)
		evalGraph = reflow.Digester.Rand(nil)
		trace     = reflow.Digester.Rand(nil)
		end       = time.Now()
	)
	taskb.TableName = mockTableName
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
		taskb  = &TaskDB{DB: &mockdb}
		taskID = taskdb.NewTaskID()
		tdbErr error
		end    = time.Now()
	)
	taskb.TableName = mockTableName
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
		taskb   = &TaskDB{DB: &mockdb}
		allocID = reflow.NewStringDigest("allocid")
		poolID  = reflow.Digester.Rand(nil)
		res     = reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024}
		start   = time.Now().Add(-time.Hour)
	)
	taskb.TableName = mockTableName
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
		{*mockdb.pinput.Item[colPoolID].S, poolID.String()},
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
		taskb  = &TaskDB{DB: &mockdb}
		p      = taskdb.Pool{
			PoolID:    reflow.NewStringDigest("poolid"),
			PoolType:  "pool_type",
			Resources: reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024},
			URI:       "http://some_url",
		}
	)
	taskb.TableName = mockTableName
	p.Start = time.Now().Add(-time.Hour)
	p.ClusterName = "cluster_name"
	p.User = "user@grailbio.com"
	p.ReflowVersion = "version_x"
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
		taskb  = &TaskDB{DB: &mockdb}
		id     = reflow.Digester.Rand(nil)
		res    = reflow.Resources{"cpu": 2, "mem": 4 * 1024 * 1024 * 1024}
	)
	taskb.TableName = mockTableName
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
		taskb     = &TaskDB{DB: &mockdb}
		runID     = taskdb.NewRunID()
		keepalive = time.Now().UTC()
	)
	taskb.TableName = mockTableName
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
		taskb   = &TaskDB{DB: &mockdb}
		id      = reflow.Digester.Rand(nil)
		endtime = time.Now().UTC()
	)
	taskb.TableName = mockTableName
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
			{
				colID:        {S: aws.String(id)},
				colID4:       {S: aws.String(id4)},
				colUser:      {S: aws.String(m.user)},
				colLabels:    {SS: []*string{aws.String("label=test")}},
				colKeepalive: {S: aws.String(m.keepalive.Format(timeLayout))},
				colStartTime: {S: aws.String(m.starttime.Format(timeLayout))},
			},
		},
	}, m.err
}

func getMockRunTaskDB() *mockDynamodbQueryRuns {
	return &mockDynamodbQueryRuns{starttime: time.Now(), keepalive: time.Now(), user: colUser}
}

func truncated(d digest.Digest) digest.Digest {
	d.Truncate(4)
	return d
}

func TestRunsIDQueries(t *testing.T) {
	var (
		id         = reflow.Digester.Rand(nil)
		runID      = taskdb.RunID(id)
		truncRunID = taskdb.RunID(truncated(id))
		mockdb     = getMockRunTaskDB()
		taskb      = &TaskDB{DB: mockdb}
	)
	taskb.TableName = mockTableName
	mockdb.id = id
	for _, tt := range []struct {
		q                     taskdb.RunQuery
		index, keycol, keyval string
	}{
		{taskdb.RunQuery{ID: runID, User: colUser}, idIndex, colID, runID.ID()},
		{taskdb.RunQuery{ID: truncRunID, User: colUser}, id4Index, colID4, runID.IDShort()},
	} {
		runs, err := taskb.Runs(context.Background(), tt.q)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := len(runs), 1; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		for _, test := range []struct {
			actual   string
			expected string
		}{
			{*mockdb.qinput.TableName, "mockdynamodb"},
			{*mockdb.qinput.IndexName, tt.index},
			{*mockdb.qinput.ExpressionAttributeValues[":type"].S, "run"},
			{*mockdb.qinput.ExpressionAttributeValues[":keyval"].S, tt.keyval},
			{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
			{*mockdb.qinput.KeyConditionExpression, tt.keycol + " = :keyval"},
			{*mockdb.qinput.FilterExpression, "#Type = :type"},
			{runs[0].User, colUser},
			{runs[0].ID.ID(), mockdb.id.String()},
			{runs[0].Labels["label"], "test"},
		} {
			if test.expected != test.actual {
				t.Errorf("expected %s, got %v", test.expected, test.actual)
			}
		}
	}

	// Abbrev id that does not match the full id.
	str := id.ShortString(5)
	str = str[:len(str)-2] + fmt.Sprintf("%1x", str[len(str)-1]+1)
	id, err := reflow.Digester.Parse(str)
	if err != nil {
		t.Fatal(err)
	}
	runID = taskdb.RunID(id)
	runs, err := taskb.Runs(context.Background(), taskdb.RunQuery{ID: runID, User: colUser})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(runs), 0; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestRunsSinceQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb}
		until  = time.Now().UTC()
		since  = until.Add(-time.Minute * 10)
	)
	taskb.TableName = mockTableName
	// Ensure since has the current UTC date so we generate only one query
	if date(until) != date(since) {
		since = until.UTC().Truncate(time.Hour * 24)
	}
	date := date(until).Format(dateLayout)
	_, err := taskb.Runs(context.Background(), taskdb.RunQuery{Since: since, Until: time.Now()})
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
		{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka"},
		{"filter expression", *mockdb.qinput.FilterExpression, "#Type = :type"},
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
	} {
		if test.expected != test.actual {
			t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
		}
	}
}

func TestRunsTimeBucketQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb}
		until  = time.Now().UTC()
		since  = until.UTC().Add(-time.Hour * 24)
	)
	taskb.TableName = mockTableName
	// Make sure we don't hit an hour boundary. This can result in one extra query.
	if since.Truncate(time.Hour) == since {
		since = since.Add(time.Minute)
	}
	query := taskdb.RunQuery{User: "reflow", Since: since, Until: until}
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
	dt := date(since).Format(dateLayout)
	for _, qinput := range mockdb.qinputs {
		for _, test := range []struct {
			name     string
			actual   string
			expected string
		}{
			{"table", *qinput.TableName, "mockdynamodb"},
			{"index", *qinput.IndexName, dateKeepaliveIndex},
			{"type", *qinput.ExpressionAttributeValues[":type"].S, "run"},
			{"date", *qinput.ExpressionAttributeValues[":date"].S, dt},
			{"keepalive", *mockdb.qinput.ExpressionAttributeValues[":ka"].S, since.Format(timeLayout)},
			{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka"},
			{"filter expression", *qinput.FilterExpression, "#Type = :type and #User = :user"},
			{"attribute name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
			{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
			{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		} {
			if test.expected != test.actual {
				t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
			}
		}
		dt = date(since.Add(time.Hour * 24)).Format(dateLayout)
	}
}

func TestRunsSinceUserQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb}
		since  = time.Now().UTC()
		query  = taskdb.RunQuery{User: "reflow", Since: since}
	)
	taskb.TableName = mockTableName
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
		{"keepalive_since", *mockdb.qinput.ExpressionAttributeValues[":ka"].S, since.Format(timeLayout)},
		// We don't check keepalive_until since we haven't set it, and it defaults to `time.Now()`, but we don't know the value.
		{"user", *mockdb.qinput.ExpressionAttributeValues[":user"].S, query.User},
		{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka"},
		{"filter expression", *mockdb.qinput.FilterExpression, "#Type = :type and #User = :user"},
		{"attribute name user", *mockdb.qinput.ExpressionAttributeNames["#User"], colUser},
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
	} {
		if test.expected != test.actual {
			t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
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
			{
				colID:        {S: aws.String(id)},
				colID4:       {S: aws.String(id4)},
				colUser:      {S: aws.String(m.user)},
				colLabels:    {SS: []*string{aws.String("label=test")}},
				colKeepalive: {S: aws.String(m.keepalive.Format(timeLayout))},
				colStartTime: {S: aws.String(m.starttime.Format(timeLayout))},
				colFlowID:    {S: aws.String(id)},
				colResultID:  {S: aws.String(id)},
				colRunID:     {S: aws.String(id)},
				colStdout:    {S: aws.String(id)},
				colStderr:    {S: aws.String(id)},
				colInspect:   {S: aws.String(id)},
				colURI:       {S: aws.String(m.uri)},
				colImgCmdID:  {S: aws.String(id)},
				colIdent:     {S: aws.String(m.ident)},
			},
		},
	}

	if m.multiOutput {
		output.LastEvaluatedKey = map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("value")},
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

func TestTasksIDQueries(t *testing.T) {
	var (
		id          = reflow.Digester.Rand(nil)
		taskID      = taskdb.TaskID(id)
		truncTaskID = taskdb.TaskID(truncated(id))
		mockdb      = getmocktaskstaskdb()
		taskb       = &TaskDB{DB: mockdb}
	)
	taskb.TableName = mockTableName
	mockdb.id = id
	for _, tt := range []struct {
		q                     taskdb.TaskQuery
		index, keycol, keyval string
	}{
		{taskdb.TaskQuery{ID: taskID}, idIndex, colID, taskID.ID()},
		{taskdb.TaskQuery{ID: truncTaskID}, id4Index, colID4, taskID.IDShort()},
	} {
		tasks, err := taskb.Tasks(context.Background(), tt.q)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := len(tasks), 1; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		for _, test := range []struct {
			actual   string
			expected string
		}{
			{*mockdb.qinput.TableName, "mockdynamodb"},
			{*mockdb.qinput.IndexName, tt.index},
			{*mockdb.qinput.ExpressionAttributeValues[":type"].S, "task"},
			{*mockdb.qinput.ExpressionAttributeValues[":keyval"].S, tt.keyval},
			{*mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
			{*mockdb.qinput.KeyConditionExpression, tt.keycol + " = :keyval"},
			{*mockdb.qinput.FilterExpression, "#Type = :type"},
			{tasks[0].ID.ID(), mockdb.id.String()},
		} {
			if test.expected != test.actual {
				t.Errorf("expected %s, got %v", test.expected, test.actual)
			}
		}
	}
	// Abbrev id that does not match the full id.
	str := id.ShortString(5)
	str = str[:len(str)-2] + fmt.Sprintf("%1x", str[len(str)-1]+1)
	id, err := reflow.Digester.Parse(str)
	if err != nil {
		t.Fatal(err)
	}
	taskID = taskdb.TaskID(id)
	tasks, err := taskb.Tasks(context.Background(), taskdb.TaskQuery{ID: taskID})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tasks), 0; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestTasksRunIDQuery(t *testing.T) {
	var (
		id     = taskdb.NewRunID()
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb}
		query  = taskdb.TaskQuery{RunID: id}
	)
	taskb.TableName = mockTableName
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
			t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
		}
	}
}

func TestTasksImgCmdIDQuery(t *testing.T) {
	var (
		image    = "img"
		cmd      = "cmd"
		imgCmdID = taskdb.NewImgCmdID(image, cmd)
		mockdb   = getmockquerytaskdb()
		taskb    = &TaskDB{DB: mockdb, activeImgCmdIDIndexName: imgCmdIDIndex, activeIdentIndexName: identIndex}
		query    = taskdb.TaskQuery{ImgCmdID: imgCmdID}
	)
	taskb.TableName = mockTableName
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
			t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
		}
	}
}

func TestTasksIdentQuery(t *testing.T) {
	var (
		ident  = "testident"
		id     = reflow.Digester.Rand(nil)
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb, activeImgCmdIDIndexName: imgCmdIDIndex, activeIdentIndexName: identIndex}
		query  = taskdb.TaskQuery{Ident: ident}
	)
	taskb.TableName = mockTableName
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
			t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
		}
	}
}

func TestTasksSinceQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb}
		until  = time.Now().UTC()
		since  = until.Add(-time.Minute * 10)
	)
	taskb.TableName = mockTableName
	// Ensure since has the current UTC date so we generate only one query
	if date(until) != date(since) {
		since = until.UTC().Truncate(time.Hour * 24)
	}
	date := date(until).Format(dateLayout)
	_, err := taskb.Tasks(context.Background(), taskdb.TaskQuery{Since: since, Until: time.Now()})
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
		{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka"},
		{"filter expression", *mockdb.qinput.FilterExpression, "#Type = :type"},
		{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
		{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
	} {
		if test.expected != test.actual {
			t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
		}
	}
}

func TestTasksTimeBucketQuery(t *testing.T) {
	var (
		mockdb = getmockquerytaskdb()
		taskb  = &TaskDB{DB: mockdb}
		until  = time.Now().UTC()
		since  = until.UTC().Add(-time.Hour * 24)
	)
	taskb.TableName = mockTableName
	// Make sure we don't hit an hour boundary. This can result in one extra query.
	if since.Truncate(time.Hour) == since {
		since = since.Add(time.Minute)
	}
	query := taskdb.TaskQuery{Since: since, Until: until}
	_, err := taskb.Tasks(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(mockdb.qinputs), 2; got != want {
		t.Fatalf("expected %v sub queries, got %v", want, got)
	}
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
	dt := date(since).Format(dateLayout)
	for _, qinput := range mockdb.qinputs {
		for _, test := range []struct {
			name     string
			actual   string
			expected string
		}{
			{"table", *qinput.TableName, "mockdynamodb"},
			{"index", *qinput.IndexName, dateKeepaliveIndex},
			{"type", *qinput.ExpressionAttributeValues[":type"].S, "task"},
			{"date", *qinput.ExpressionAttributeValues[":date"].S, dt},
			{"keepalive", *mockdb.qinput.ExpressionAttributeValues[":ka"].S, since.Format(timeLayout)},
			{"key condition", *mockdb.qinput.KeyConditionExpression, "#Date = :date and " + colKeepalive + " > :ka"},
			{"filter expression", *qinput.FilterExpression, "#Type = :type"},
			{"attribute name date", *mockdb.qinput.ExpressionAttributeNames["#Date"], colDate},
			{"attribute name type", *mockdb.qinput.ExpressionAttributeNames["#Type"], colType},
		} {
			if test.expected != test.actual {
				t.Errorf("%s: expected %s, got %v", test.name, test.expected, test.actual)
			}
		}
		dt = date(since.Add(time.Hour * 24)).Format(dateLayout)
	}
}

func TestTasksLimitQuery(t *testing.T) {
	var (
		ident        = "testident"
		id           = reflow.Digester.Rand(nil)
		mockdb       = getmockquerytaskdb()
		taskb        = &TaskDB{DB: mockdb, activeImgCmdIDIndexName: imgCmdIDIndex, activeIdentIndexName: identIndex}
		limit  int64 = 5
		query        = taskdb.TaskQuery{Ident: ident, Limit: limit}
	)
	taskb.TableName = mockTableName
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
		taskb       = &TaskDB{DB: mockdb}
		query       = taskdb.TaskQuery{RunID: runID}
	)
	taskb.TableName = mockTableName
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	awserr, ok := err.(*errors.Error).Err.(*errors.Error).Err.(awserr.Error)
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
		taskb       = &TaskDB{DB: mockdb}
		query       = taskdb.TaskQuery{ID: badTaskID}
	)
	taskb.TableName = mockTableName
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	if err == nil {
		t.Fatal("expected err, got nil")
	}
	err = err.(*errors.Error).Err
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
		taskb       = &TaskDB{DB: mockdb}
		query       = taskdb.TaskQuery{RunID: runID}
	)
	taskb.TableName = mockTableName
	mockdb.err = expectederr
	tasks, err := taskb.Tasks(context.Background(), query)
	awserr, ok := err.(*errors.Error).Err.(*errors.Error).Err.(awserr.Error)
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
		taskb       = &TaskDB{DB: mockdb}
		query       = taskdb.TaskQuery{RunID: runID}
	)
	taskb.TableName = mockTableName
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

func TestDydbTaskdbInfra(t *testing.T) {
	const (
		bucket = "reflow-unittest"
		table  = "reflow-unittest"
	)
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
		"taskdb":  fmt.Sprintf("%s,table=%v,bucket=%s", ProviderName, table, bucket),
		"assoc":   fmt.Sprintf("%s,table=%v", dydbassoc.ProviderName, table),
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
		taskb  = &TaskDB{DB: mockdb}
	)
	taskb.TableName = mockTableName
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

func TestParseAttr(t *testing.T) {
	id := reflow.Digester.Rand(nil)
	ts := time.Now()
	tsParsed, _ := time.Parse(timeLayout, ts.Format(timeLayout))
	for _, tt := range []struct {
		it    map[string]*dynamodb.AttributeValue
		k     taskdb.Kind
		f     func(s string) (interface{}, error)
		want  interface{}
		wantE error
	}{
		{it: map[string]*dynamodb.AttributeValue{}, k: User, want: ""},
		{it: map[string]*dynamodb.AttributeValue{}, k: ID, f: parseDigestFunc, want: nil},
		{it: map[string]*dynamodb.AttributeValue{colUser: {S: aws.String("user")}}, k: User, want: "user"},
		{it: map[string]*dynamodb.AttributeValue{colID: {S: aws.String(id.String())}}, k: ID, f: parseDigestFunc, want: id},
		{it: map[string]*dynamodb.AttributeValue{colID: {S: aws.String("hello")}}, k: ID, f: parseDigestFunc, wantE: fmt.Errorf("parse ID hello: %v", digest.ErrInvalidDigest)},
		{it: map[string]*dynamodb.AttributeValue{colStartTime: {S: aws.String(ts.Format(timeLayout))}}, k: StartTime, f: parseTimeFunc, want: tsParsed},
		{it: map[string]*dynamodb.AttributeValue{colStartTime: {S: aws.String("hello")}}, k: StartTime, f: parseTimeFunc, wantE: fmt.Errorf("parse StartTime hello: parsing time \"hello\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"hello\" as \"2006\"")},
	} {
		var m errors.Multi
		v := parseAttr(tt.it, tt.k, tt.f, &m)
		if tt.wantE != nil {
			if got, want := m.Combined(), tt.wantE; got == nil || got.Error() != want.Error() {
				t.Errorf("got %v, want %v", got, want)
			}
			continue
		}
		if got, want := v, tt.want; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
