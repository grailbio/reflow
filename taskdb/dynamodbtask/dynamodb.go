// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dynamodbtask implements the taskdb.TaskDB interface for AWS dynamodb backend.
// Every run or task is stored in a row with their attributes which includes labels, user,
// keepalive and start times. Tasks have a runid column to identify which run it belongs to.
// Tasks also store the flowId of the reflow flow, resultId, exec uri and stdout, stderr
// and inspect log ids.
// To make common queries like recently run/tasks, runs/tasks have day time
// buckets stored. "Date-Keepalive-index" index allows querying runs/tasks based on time
// buckets. Dynamodbtask also uses a bunch of secondary indices to help with run/task querying.
// Schema:
// run:  {ID, ID4, Labels, Bundle, Args, Date, Keepalive, StartTime, Type="run", User}
// task: {ID, ID4, Labels, Date, Keepalive, StartTime, Type="task", FlowID, Inspect, ResultID, RunID, RunID4, ImgCmdID, Ident, Stderr, Stdout, URI}
// Indexes:
// 1. Date-Keepalive-index - for time-based queries.
// 2. RunID-index - for finding all tasks that belong to a run.
// 3. ID-index and ID4-ID-index - for queries looking for specific runs or tasks.
// 4. ImgCmdID-index and Ident-index - for queries looking for specific execs.
package dynamodbtask

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc/dydbassoc"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
)

const (
	ID taskdb.Kind = iota
	ID4
	RunID
	RunID4
	FlowID
	ResultID
	ImgCmdID
	Ident
	KeepAlive
	StartTime
	Stdout
	Stderr
	ExecInspect
	URI
	Labels
	User
	Type
	Date
	Bundle
	Args
)

func init() {
	infra.Register("dynamodbtask", new(TaskDB))
}

type objType string

const (
	run  objType = "run"
	task objType = "task"
)

const (
	// TimeLayout is the time layout used to serialize time to dynamodb attributes.
	timeLayout = time.RFC3339
	// DateLayout is the layout used to serialize date.
	dateLayout = "2006-01-02"

	// Default provisioned capacities for DynamoDB.
	writecap = 10
	readcap  = 20
)

// Column names used in dynamodb table.
const (
	colID        = "ID"
	colID4       = "ID4"
	colRunID     = "RunID"
	colRunID4    = "RunID4"
	colFlowID    = "FlowID"
	colResultID  = "ResultID"
	colImgCmdID  = "ImgCmdID"
	colIdent     = "Ident"
	colKeepalive = "Keepalive"
	colStartTime = "StartTime"
	colStdout    = "Stdout"
	colStderr    = "Stderr"
	colInspect   = "Inspect"
	colURI       = "URI"
	colLabels    = "Labels"
	colUser      = "User"
	colType      = "Type"
	colDate      = "Date"
	colBundle    = "Bundle"
	colArgs      = "Args"
)

var colmap = map[taskdb.Kind]string{
	ID:          colID,
	ID4:         colID4,
	RunID:       colRunID,
	RunID4:      colRunID4,
	FlowID:      colFlowID,
	ImgCmdID:    colImgCmdID,
	Ident:       colIdent,
	ResultID:    colResultID,
	KeepAlive:   colKeepalive,
	StartTime:   colStartTime,
	Stdout:      colStdout,
	Stderr:      colStderr,
	ExecInspect: colInspect,
	URI:         colURI,
	Labels:      colLabels,
	User:        colUser,
	Type:        colType,
	Date:        colDate,
	Bundle:      colBundle,
	Args:        colArgs,
}

// Index names used in dynamodb table.
const (
	idIndex            = "ID-index"
	id4Index           = "ID4-ID-index"
	runIDIndex         = "RunID-index"
	imgCmdIDIndex      = "ImgCmdID-index"
	identIndex         = "Ident-index"
	dateKeepaliveIndex = "Date-Keepalive-index"
)

// TaskDB implements the dynamodb backed taskdb.TaskDB interface to
// store run/task state and metadata.
// Each association is either:
// a) RunID and its associated metadata (run labels, user info, and leases)
// b) TaskID and its associated metadata (RunID that spawned this task, FlowID of the node, and leases)
type TaskDB struct {
	// DB is the dynamodb.
	DB dynamodbiface.DynamoDBAPI
	// TableName is the table to write the run/task info to.
	TableName string
	// Labels on the run.
	Labels []string
	// User who initiated this run.
	User string
	// Limiter limits number of concurrent operations.
	limiter *limiter.Limiter
}

// Help implements infra.Provider.
func (TaskDB) Help() string {
	return "configure a dynamodb table to store run/task information"
}

// Init implements infra.Provider.
func (t *TaskDB) Init(sess *session.Session, assoc *dydbassoc.Assoc, user *infra2.User, labels pool.Labels) error {
	t.limiter = limiter.New()
	t.limiter.Release(32)
	t.DB = dynamodb.New(sess)
	t.Labels = make([]string, 0, len(labels))
	for k, v := range labels {
		t.Labels = append(t.Labels, fmt.Sprintf("%s=%s", k, v))
	}
	t.User = string(*user)
	t.TableName = assoc.TableName
	return nil
}

// Version implements infra.Provider.
func (t *TaskDB) Version() int {
	return 1
}

// CreateRun sets a new run in the taskdb with the given id, labels and user.
func (t *TaskDB) CreateRun(ctx context.Context, id taskdb.RunID, user string) error {
	input := &dynamodb.PutItemInput{
		TableName: aws.String(t.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
			colID4: {
				S: aws.String(id.IDShort()),
			},
			colLabels: {
				SS: aws.StringSlice(t.Labels),
			},
			colUser: {
				S: aws.String(user),
			},
			colType: {
				S: aws.String(string(run)),
			},
			colStartTime: {
				S: aws.String(time.Now().UTC().Format(timeLayout)),
			},
		},
	}
	_, err := t.DB.PutItemWithContext(ctx, input)
	return err
}

// SetRunAttrs sets the reflow bundle and corresponding args for this run.
func (t *TaskDB) SetRunAttrs(ctx context.Context, id taskdb.RunID, bundle digest.Digest, args []string) error {
	updateExpression := aws.String(fmt.Sprintf("SET %s = :bundle", colBundle))
	values := map[string]*dynamodb.AttributeValue{
		":bundle": {
			S: aws.String(bundle.String()),
		},
	}
	if args != nil && len(args) > 0 {
		*updateExpression += fmt.Sprintf(", %s = :args", colArgs)
		values[":args"] = &dynamodb.AttributeValue{SS: aws.StringSlice(args)}
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
		},
		UpdateExpression:          updateExpression,
		ExpressionAttributeValues: values,
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err
}

// CreateTask creates a new task in the taskdb with the provided taskID, runID and flowID, imgCmdID, ident, and uri.
func (t *TaskDB) CreateTask(ctx context.Context, id taskdb.TaskID, runID taskdb.RunID, flowID digest.Digest, imgCmdID taskdb.ImgCmdID, ident, uri string) error {
	now := time.Now().UTC()
	input := &dynamodb.PutItemInput{
		TableName: aws.String(t.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
			colID4: {
				S: aws.String(id.IDShort()),
			},
			colRunID: {
				S: aws.String(runID.ID()),
			},
			colRunID4: {
				S: aws.String(runID.IDShort()),
			},
			colFlowID: {
				S: aws.String(flowID.String()),
			},
			colImgCmdID: {
				S: aws.String(imgCmdID.ID()),
			},
			colIdent: {
				S: aws.String(ident),
			},
			colType: {
				S: aws.String(string(task)),
			},
			colStartTime: {
				S: aws.String(time.Now().UTC().Format(timeLayout)),
			},
			colURI: {
				S: aws.String(uri),
			},
			colLabels: {
				SS: aws.StringSlice(t.Labels),
			},
			colDate: {
				S: aws.String(now.Format(dateLayout)),
			},
			colKeepalive: {
				S: aws.String(now.Format(timeLayout)),
			},
		},
	}
	_, err := t.DB.PutItemWithContext(ctx, input)
	return err
}

// SetTaskResult sets the task result id.
func (t *TaskDB) SetTaskResult(ctx context.Context, id taskdb.TaskID, result digest.Digest) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :result", colResultID)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":result": {S: aws.String(result.String())},
		},
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err
}

// SetTaskAttrs sets the stdout, stderr and inspect ids for the task.
func (t *TaskDB) SetTaskAttrs(ctx context.Context, id taskdb.TaskID, stdout, stderr, inspect digest.Digest) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :stdout, %s = :stderr, %s = :inspect", colStdout, colStderr, colInspect)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":stdout":  {S: aws.String(stdout.String())},
			":stderr":  {S: aws.String(stderr.String())},
			":inspect": {S: aws.String(inspect.String())},
		},
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err
}

func date(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func dates(beg, end time.Time) (dates []time.Time) {
	for beg, end = date(beg), date(end); !end.Before(beg); beg = beg.AddDate(0, 0, 1) {
		dates = append(dates, beg)
	}
	return
}

// KeepRunAlive sets the keepalive for run id to keepalive.
func (t *TaskDB) KeepRunAlive(ctx context.Context, id taskdb.RunID, keepalive time.Time) error {
	return t.keepalive(ctx, digest.Digest(id), keepalive)
}

// KeepTaskAlive sets the keepalive for task id to keepalive.
func (t *TaskDB) KeepTaskAlive(ctx context.Context, id taskdb.TaskID, keepalive time.Time) error {
	return t.keepalive(ctx, digest.Digest(id), keepalive)
}

// keepalive sets the keepalive for the specified id to keepalive.
func (t *TaskDB) keepalive(ctx context.Context, id digest.Digest, keepalive time.Time) error {
	keepalive = keepalive.UTC()
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.String()),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :ka, #Date = :date", colKeepalive)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ka":   {S: aws.String(keepalive.Format(timeLayout))},
			":date": {S: aws.String(keepalive.Format(dateLayout))},
		},
		ExpressionAttributeNames: map[string]*string{
			"#Date": aws.String(colDate),
		},
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err
}

// query is the generic query struct for the TaskDB querying interface. All fields, with
// the exception of Typ, are optional. If a user filter is
// specified, all queries are restricted to runs/tasks created by the user.
// If id is specified, runs/tasks with id is looked up. If Since is specified,
// runs/tasks whose keepalive is within that time frame are looked up. The only valid Typs are
// "run" and "task".
type query struct {
	// ID is the task/run id being queried.
	ID digest.Digest
	// Since queries for runs/tasks that were active past this time.
	Since time.Time
	// User looks up the runs/tasks that are created by the user. If empty, the user filter is dropped.
	User string
	// Typ is the type (either run or task).
	Typ objType
}

// buildIndexQuery returns a dynamodb QueryInput for the specified key on the specified index.
func (t *TaskDB) buildIndexQuery(kind taskdb.Kind, indexName, partKey string, typ objType) []*dynamodb.QueryInput {
	colname, colnameOk := colmap[kind]
	if !colnameOk {
		panic("invalid kind")
	}
	keyExpression := fmt.Sprintf("%s = :keyval", colname)
	attributeValues := map[string]*dynamodb.AttributeValue{
		":keyval": {S: aws.String(partKey)},
		":type":   {S: aws.String(string(typ))},
	}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(t.TableName),
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyExpression),
		ExpressionAttributeValues: attributeValues,
		FilterExpression:          aws.String("#Type = :type"),
		ExpressionAttributeNames: map[string]*string{
			"#Type": aws.String(colType),
		},
	}
	return []*dynamodb.QueryInput{input}
}

// buildSinceUserQueries builds Since and User-based queries. All query fields are optional.
func (t *TaskDB) buildSinceUserQueries(q query) []*dynamodb.QueryInput {
	// Build time bucket based queries.
	type part struct {
		keyExpression string
		attrValues    map[string]*dynamodb.AttributeValue
		attrNames     map[string]*string
	}
	var (
		keyExpression    string
		timeBuckets      []part
		attributeValues  = make(map[string]*dynamodb.AttributeValue)
		attributeNames   = make(map[string]*string)
		filterExpression []string
		now              = time.Now().UTC()
	)
	if q.Since.IsZero() {
		panic("taskdb invalid query: missing since")
	}
	since := q.Since.UTC()
	for _, d := range dates(since, now) {
		part := part{
			keyExpression: "#Date = :date and " + colKeepalive + " > :ka ",
			attrValues: map[string]*dynamodb.AttributeValue{
				":date": &dynamodb.AttributeValue{S: aws.String(d.Format(dateLayout))},
				":ka":   &dynamodb.AttributeValue{S: aws.String(since.Format(timeLayout))},
			},
			attrNames: map[string]*string{
				"#Date": aws.String(colDate),
			},
		}
		timeBuckets = append(timeBuckets, part)
	}

	filterExpression = append(filterExpression, "#Type = :type")
	attributeValues[":type"] = &dynamodb.AttributeValue{S: aws.String(string(q.Typ))}
	attributeNames["#Type"] = aws.String(colType)

	if q.User != "" && q.Typ == run {
		filterExpression = append(filterExpression, "#User = :user")
		attributeValues[":user"] = &dynamodb.AttributeValue{S: aws.String(q.User)}
		attributeNames["#User"] = aws.String(colUser)
	}

	if len(timeBuckets) > 0 {
		var queries []*dynamodb.QueryInput
		for _, ti := range timeBuckets {
			for k, v := range attributeValues {
				ti.attrValues[k] = v
			}
			for k, v := range attributeNames {
				ti.attrNames[k] = v
			}
			query := &dynamodb.QueryInput{
				TableName:                 aws.String(t.TableName),
				IndexName:                 aws.String(dateKeepaliveIndex),
				KeyConditionExpression:    aws.String(ti.keyExpression),
				ExpressionAttributeValues: ti.attrValues,
			}
			if len(ti.attrNames) > 0 {
				query.ExpressionAttributeNames = ti.attrNames
			}
			if len(filterExpression) > 0 {
				query.FilterExpression = aws.String(strings.Join(filterExpression, " and "))
			}
			queries = append(queries, query)
		}
		return queries
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(t.TableName),
		IndexName:                 aws.String(dateKeepaliveIndex),
		KeyConditionExpression:    aws.String(keyExpression),
		ExpressionAttributeValues: attributeValues,
	}
	if len(attributeNames) > 0 {
		input.ExpressionAttributeNames = attributeNames
	}
	if len(filterExpression) > 0 {
		input.FilterExpression = aws.String(strings.Join(filterExpression, " and "))
	}
	return []*dynamodb.QueryInput{input}
}

// Tasks returns tasks that matches the query.
func (t *TaskDB) Tasks(ctx context.Context, taskQuery taskdb.TaskQuery) ([]taskdb.Task, error) {
	var queries []*dynamodb.QueryInput
	switch {
	case taskQuery.ID.IsValid() && digest.Digest(taskQuery.ID).IsAbbrev():
		queries = t.buildIndexQuery(ID4, id4Index, taskQuery.ID.IDShort(), task)
	case taskQuery.ID.IsValid():
		queries = t.buildIndexQuery(ID, idIndex, taskQuery.ID.ID(), task)
	case taskQuery.RunID.IsValid():
		queries = t.buildIndexQuery(RunID, runIDIndex, taskQuery.RunID.ID(), task)
	case taskQuery.ImgCmdID.IsValid():
		queries = t.buildIndexQuery(ImgCmdID, imgCmdIDIndex, taskQuery.ImgCmdID.ID(), task)
	case taskQuery.Ident != "":
		queries = t.buildIndexQuery(Ident, identIndex, taskQuery.Ident, task)
	default:
		q := query{
			Since: taskQuery.Since,
			Typ:   task,
		}
		queries = t.buildSinceUserQueries(q)
	}
	var (
		responses  = make([][]map[string]*dynamodb.AttributeValue, len(queries))
		err        error
		errs       []error
		totalTasks int64
	)
	err = traverse.Each(len(queries), func(i int) error {
		var (
			query   = queries[i]
			lastKey map[string]*dynamodb.AttributeValue
		)
		for {
			query.ExclusiveStartKey = lastKey
			resp, err := t.DB.QueryWithContext(ctx, query)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case "ValidationException":
						if strings.Contains(aerr.Message(),
							"The table does not have the specified index") {
							return errors.E(`index missing: run "reflow migrate"`, err)
						}
					}
				}
				return err
			}

			atomic.AddInt64(&totalTasks, int64(len(resp.Items)))
			responses[i] = append(responses[i], resp.Items...)
			lastKey = resp.LastEvaluatedKey
			if lastKey == nil || (taskQuery.Limit > 0 && atomic.LoadInt64(&totalTasks) >= taskQuery.Limit) {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var items []map[string]*dynamodb.AttributeValue
	for i := range responses {
		items = append(items, responses[i]...)
	}
	tasks := make([]taskdb.Task, 0, len(items))
	for _, it := range items {
		var (
			id, fid, runid, result, stderr, stdout, inspect, imgCmdID digest.Digest
			keepalive                                                 time.Time
			ident, uri                                                string
		)

		id, err = digest.Parse(*it[colID].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse id %v: %v", *it[colID], err))
		}
		if d := digest.Digest(taskQuery.ID); taskQuery.ID.IsValid() && d.IsAbbrev() {
			if !id.Expands(d) {
				continue
			}
		}
		fid, err := reflow.Digester.Parse(*it[colFlowID].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse flowid %v: %v", *it[colFlowID].S, err))
		}
		runid, err = digest.Parse(*it[colRunID].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse runid %v: %v", *it[colRunID].S, err))
		}
		if resultID, ok := it[colResultID]; ok {
			result, err = digest.Parse(*resultID.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse resultid %v: %v", *resultID.S, err))
			}
		}
		if _, ok := it[colKeepalive]; ok {
			keepalive, err = time.Parse(timeLayout, *it[colKeepalive].S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse keepalive %v: %v", *it[colKeepalive].S, err))
			}
		}
		st, err := time.Parse(timeLayout, *it[colStartTime].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse starttime %v: %v", *it[colStartTime].S, err))
		}
		if v, ok := it[colStdout]; ok {
			stdout, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse stdout %v: %v", *it[colStdout].S, err))
			}
		}
		if v, ok := it[colStderr]; ok {
			stderr, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse stderr %v: %v", *it[colStderr].S, err))
			}
		}
		if v, ok := it[colInspect]; ok {
			inspect, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse inspect %v: %v", *it[colInspect].S, err))
			}
		}
		if v, ok := it[colImgCmdID]; ok {
			imgCmdID, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse imagecmdid %v: %v", *it[colImgCmdID].S, err))
			}
		}
		if v, ok := it[colIdent]; ok {
			ident = *v.S
		}
		if v, ok := it[colURI]; ok {
			uri = *v.S
		}
		tasks = append(tasks, taskdb.Task{
			ID:        taskdb.TaskID(id),
			RunID:     taskdb.RunID(runid),
			FlowID:    fid,
			ResultID:  result,
			ImgCmdID:  taskdb.ImgCmdID(imgCmdID),
			Ident:     ident,
			URI:       uri,
			Keepalive: keepalive,
			Start:     st,
			Stdout:    stdout,
			Stderr:    stderr,
			Inspect:   inspect,
		})
	}
	if len(errs) == 0 {
		return tasks, nil
	}
	var b strings.Builder
	for i, err := range errs {
		b.WriteString(err.Error())
		if i != len(errs)-1 {
			b.WriteString(", ")
		}
	}
	return nil, errors.New(b.String())
}

// Runs returns runs that matches the query.
func (t *TaskDB) Runs(ctx context.Context, runQuery taskdb.RunQuery) ([]taskdb.Run, error) {
	var queries []*dynamodb.QueryInput
	switch {
	case runQuery.ID.IsValid() && digest.Digest(runQuery.ID).IsAbbrev():
		queries = t.buildIndexQuery(ID4, id4Index, runQuery.ID.IDShort(), run)
	case runQuery.ID.IsValid():
		queries = t.buildIndexQuery(ID, idIndex, runQuery.ID.ID(), run)
	default:
		q := query{
			ID:    digest.Digest(runQuery.ID),
			Since: runQuery.Since,
			User:  runQuery.User,
			Typ:   run,
		}
		queries = t.buildSinceUserQueries(q)
	}
	var (
		responses = make([][]map[string]*dynamodb.AttributeValue, len(queries))
		errs      []error
		err       error
	)
	err = traverse.Each(len(queries), func(i int) error {
		var (
			query   = queries[i]
			lastKey map[string]*dynamodb.AttributeValue
		)
		for {
			query.ExclusiveStartKey = lastKey
			resp, err := t.DB.QueryWithContext(ctx, query)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case "ValidationException":
						if strings.Contains(aerr.Message(),
							"The table does not have the specified index") {
							return errors.E(`index missing: run "reflow migrate"`, err)
						}
					}
				}
				return err
			}
			responses[i] = append(responses[i], resp.Items...)
			lastKey = resp.LastEvaluatedKey
			if lastKey == nil {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var items []map[string]*dynamodb.AttributeValue
	for i := range responses {
		items = append(items, responses[i]...)
	}
	runs := make([]taskdb.Run, 0, len(items))
	for _, it := range items {
		id, err := reflow.Digester.Parse(*it[colID].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse id %v: %v", *it[colID].S, err))
		}
		if runQuery.ID.IsValid() && digest.Digest(runQuery.ID).IsAbbrev() {
			if !id.Expands(digest.Digest(runQuery.ID)) {
				continue
			}
		}
		l := make(pool.Labels)
		for _, va := range it[colLabels].SS {
			vals := strings.Split(*va, "=")
			if len(vals) != 2 {
				errs = append(errs, fmt.Errorf("label not well formed: %v", *va))
				continue
			}
			l[vals[0]] = vals[1]
		}
		keepalive, err := time.Parse(timeLayout, *it[colKeepalive].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse keepalive %v: %v", *it[colKeepalive].S, err))
		}
		st, err := time.Parse(timeLayout, *it[colStartTime].S)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse starttime %v: %v", *it[colStartTime].S, err))
		}
		runs = append(runs, taskdb.Run{
			ID:        taskdb.RunID(id),
			Labels:    l,
			User:      *it["User"].S,
			Keepalive: keepalive,
			Start:     st})
	}
	if len(errs) == 0 {
		return runs, nil
	}
	var b strings.Builder
	for i, err := range errs {
		b.WriteString(err.Error())
		if i != len(errs)-1 {
			b.WriteString(", ")
		}
	}
	return nil, fmt.Errorf("%s", b.String())
}

// Scan calls the handler function for every association in the mapping.
// Note that the handler function may be called asynchronously from multiple threads.
func (t *TaskDB) Scan(ctx context.Context, kind taskdb.Kind, mappingHandler taskdb.MappingHandler) error {
	colname, ok := colmap[kind]
	if !ok {
		panic("invalid kind")
	}
	scanner := newScanner(t)
	return scanner.Scan(ctx, ItemsHandlerFunc(func(items Items) error {
		for _, item := range items {
			var err error
			k, err := reflow.Digester.Parse(*item[colID].S)
			if err != nil {
				log.Errorf("invalid taskdb entry %v", item)
				continue
			}
			if item[colType] == nil {
				log.Errorf("invalid taskdb entry %v", item)
				continue
			}
			taskType := *item[colType].S
			if item[colname] != nil {
				v, err := reflow.Digester.Parse(*item[colname].S)
				if err != nil {
					log.Errorf("invalid taskdb entry %v", item)
					continue
				}
				var labels []string
				if item[colLabels] != nil {
					err := dynamodbattribute.Unmarshal(item[colLabels], &labels)
					if err != nil {
						log.Errorf("invalid label: %v", err)
						continue
					}
				}
				mappingHandler.HandleMapping(k, v, kind, taskType, labels)
			}
		}
		return nil
	}))
}
