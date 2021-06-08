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
// run:  {ID, ID4, Labels, Bundle, Args, Date, Keepalive, StartTime, EndTime, Type="run", User}
// task: {ID, ID4, Labels, Date, Attempt, Keepalive, StartTime, EndTime, Type="task", FlowID, Inspect, Error, ResultID, RunID, RunID4, AllocID, ImgCmdID, Ident, Stderr, Stdout, URI}
// alloc: {ID, ID4, PoolID, AllocID, Resources, Keepalive, StartTime, EndTime, Type="alloc"}
// pool: {ID, ID4, PoolID, Type, Resources, Keepalive, StartTime, EndTime, Type="pool"}
// Note:
// PoolID: While rows of type "pool" are expected to store the implementation-specific identifier of a pool,
// rows of type "alloc" will contain the digest of PoolID in this field (of the pool they belong to).
// AllocID: Similarly, While rows of type "alloc" are expected to store the value Alloc.ID(),
// rows of type "task" will contain the digest of Alloc.ID() (of the alloc where they are attempted).
// Indexes:
// 1. Date-Keepalive-index - for time-based queries.
// 2. RunID-index - for finding all tasks that belong to a run.
// 3. ID-index and ID4-ID-index - for queries looking for specific runs or tasks.
// 4. ImgCmdID-index and Ident-index - for queries looking for specific execs.
package dynamodbtask

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	AllocID
	PoolID
	ResultID
	ImgCmdID
	Ident
	Attempt
	KeepAlive
	StartTime
	Stdout
	Stderr
	ExecInspect
	Error
	URI
	Labels
	User
	Type
	Date
	Bundle
	Args
	EndTime
	ExecLog
	SysLog
	EvalGraph
	Trace
	Resources
	PoolType
	ClusterName
	ReflowVersion
)

func init() {
	infra.Register("dynamodbtask", new(TaskDB))
}

type objType string

const (
	runObj   objType = "run"
	taskObj  objType = "task"
	allocObj objType = "alloc"
	poolObj  objType = "pool"
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
	colID            = "ID"
	colID4           = "ID4"
	colRunID         = "RunID"
	colRunID4        = "RunID4"
	colFlowID        = "FlowID"
	colAllocID       = "AllocID"
	colPoolID        = "PoolID"
	colPoolType      = "PoolType"
	colResultID      = "ResultID"
	colImgCmdID      = "ImgCmdID"
	colIdent         = "Ident"
	colAttempt       = "Attempt"
	colKeepalive     = "Keepalive"
	colStartTime     = "StartTime"
	colEndTime       = "EndTime"
	colStdout        = "Stdout"
	colStderr        = "Stderr"
	colInspect       = "Inspect"
	colError         = "Error"
	colURI           = "URI"
	colLabels        = "Labels"
	colUser          = "User"
	colType          = "Type"
	colDate          = "Date"
	colBundle        = "Bundle"
	colArgs          = "Args"
	colExecLog       = "ExecLog"
	colSysLog        = "Syslog"
	colEvalGraph     = "EvalGraph"
	colTrace         = "Trace"
	colResources     = "Resources"
	colClusterName   = "ClusterName"
	colReflowVersion = "ReflowVersion"
)

var colmap = map[taskdb.Kind]string{
	ID:            colID,
	ID4:           colID4,
	RunID:         colRunID,
	RunID4:        colRunID4,
	FlowID:        colFlowID,
	AllocID:       colAllocID,
	PoolID:        colPoolID,
	PoolType:      colPoolType,
	ImgCmdID:      colImgCmdID,
	Ident:         colIdent,
	Attempt:       colAttempt,
	ResultID:      colResultID,
	KeepAlive:     colKeepalive,
	StartTime:     colStartTime,
	EndTime:       colEndTime,
	Stdout:        colStdout,
	Stderr:        colStderr,
	ExecInspect:   colInspect,
	Error:         colError,
	URI:           colURI,
	Labels:        colLabels,
	User:          colUser,
	Type:          colType,
	Date:          colDate,
	Bundle:        colBundle,
	Args:          colArgs,
	ExecLog:       colExecLog,
	SysLog:        colSysLog,
	EvalGraph:     colEvalGraph,
	Trace:         colTrace,
	Resources:     colResources,
	ClusterName:   colClusterName,
	ReflowVersion: colReflowVersion,
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

func (t *TaskDB) String() string {
	return fmt.Sprintf("%T,TableName=%s,Labels=%s", t, t.TableName, strings.Join(t.Labels, ","))
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
				S: aws.String(string(runObj)),
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

// SetRunComplete sets the result of the run post completion.
func (t *TaskDB) SetRunComplete(ctx context.Context, id taskdb.RunID, execLog, sysLog, evalGraph, trace digest.Digest, end time.Time) error {
	if end.IsZero() {
		end = time.Now()
	}
	var (
		updates = []string{fmt.Sprintf("%s = :endtime", colEndTime)}
		values  = map[string]*dynamodb.AttributeValue{
			":endtime": {S: aws.String(end.UTC().Format(timeLayout))},
		}
	)
	if !execLog.IsZero() {
		updates = append(updates, fmt.Sprintf("%s = :execlog", colExecLog))
		values[":execlog"] = &dynamodb.AttributeValue{S: aws.String(execLog.String())}
	}
	if !sysLog.IsZero() {
		updates = append(updates, fmt.Sprintf("%s = :syslog", colSysLog))
		values[":syslog"] = &dynamodb.AttributeValue{S: aws.String(sysLog.String())}
	}
	if !evalGraph.IsZero() {
		updates = append(updates, fmt.Sprintf("%s = :evalgraph", colEvalGraph))
		values[":evalgraph"] = &dynamodb.AttributeValue{S: aws.String(evalGraph.String())}
	}
	if !trace.IsZero() {
		updates = append(updates, fmt.Sprintf("%s = :trace", colTrace))
		values[":trace"] = &dynamodb.AttributeValue{S: aws.String(trace.String())}
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
		},
		UpdateExpression:          aws.String("SET " + strings.Join(updates, ", ")),
		ExpressionAttributeValues: values,
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err
}

// CreateTask creates a new task in the taskdb with the provided task.
func (t *TaskDB) CreateTask(ctx context.Context, task taskdb.Task) error {
	var (
		now = time.Now().UTC()
		res string
	)
	if r := task.Resources; !r.Equal(nil) {
		if b, err := json.Marshal(r); err == nil {
			res = string(b)
		}
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(t.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(task.ID.ID()),
			},
			colID4: {
				S: aws.String(task.ID.IDShort()),
			},
			colRunID: {
				S: aws.String(task.RunID.ID()),
			},
			colRunID4: {
				S: aws.String(task.RunID.IDShort()),
			},
			colAllocID: {
				S: aws.String(task.AllocID.String()),
			},
			colFlowID: {
				S: aws.String(task.FlowID.String()),
			},
			colImgCmdID: {
				S: aws.String(task.ImgCmdID.ID()),
			},
			colIdent: {
				S: aws.String(task.Ident),
			},
			colAttempt: {
				N: aws.String(strconv.Itoa(task.Attempt)),
			},
			colResources: {
				S: aws.String(res),
			},
			colType: {
				S: aws.String(string(taskObj)),
			},
			colStartTime: {
				S: aws.String(time.Now().UTC().Format(timeLayout)),
			},
			colURI: {
				S: aws.String(task.URI),
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

// SetTaskUri updates the task URI.
func (t *TaskDB) SetTaskUri(ctx context.Context, id taskdb.TaskID, uri string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :uri", colURI)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":uri": {S: aws.String(uri)},
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

// SetTaskComplete mark the task as completed as of the given end time.
func (t *TaskDB) SetTaskComplete(ctx context.Context, id taskdb.TaskID, err error, end time.Time) error {
	if end.IsZero() {
		end = time.Now()
	}
	var (
		update = aws.String(fmt.Sprintf("SET %s = :endtime", colEndTime))
		values = map[string]*dynamodb.AttributeValue{
			":endtime": {S: aws.String(end.UTC().Format(timeLayout))},
		}
		keys map[string]*string
	)
	if err != nil {
		update = aws.String(fmt.Sprintf("SET %s = :endtime, #Err = :error", colEndTime))
		values = map[string]*dynamodb.AttributeValue{
			":endtime": {S: aws.String(end.UTC().Format(timeLayout))},
			":error":   {S: aws.String(err.Error())},
		}
		keys = map[string]*string{"#Err": aws.String(colError)}
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.ID()),
			},
		},
		UpdateExpression:          update,
		ExpressionAttributeValues: values,
		ExpressionAttributeNames:  keys,
	}
	_, uerr := t.DB.UpdateItemWithContext(ctx, input)
	return uerr
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
	return t.KeepIDAlive(ctx, digest.Digest(id), keepalive)
}

// KeepTaskAlive sets the keepalive for task id to keepalive.
func (t *TaskDB) KeepTaskAlive(ctx context.Context, id taskdb.TaskID, keepalive time.Time) error {
	return t.KeepIDAlive(ctx, digest.Digest(id), keepalive)
}

// StartAlloc creates a new alloc in the taskdb with the provided parameters.
func (t *TaskDB) StartAlloc(ctx context.Context, allocID, poolID reflow.StringDigest, resources reflow.Resources, start time.Time) error {
	var (
		now = time.Now().UTC()
		res string
		id  = allocID.Digest()
	)
	if start.IsZero() {
		start = now
	}
	if r := resources; !r.Equal(nil) {
		if b, err := json.Marshal(r); err == nil {
			res = string(b)
		}
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(t.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.String()),
			},
			colID4: {
				S: aws.String(id.Short()),
			},
			colPoolID: {
				S: aws.String(poolID.Digest().String()),
			},
			colAllocID: {
				S: aws.String(allocID.String()),
			},
			colResources: {
				S: aws.String(res),
			},
			colType: {
				S: aws.String(string(allocObj)),
			},
			colStartTime: {
				S: aws.String(start.UTC().Format(timeLayout)),
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

// StartPool creates a new pool in the taskdb with the provided parameters.
func (t *TaskDB) StartPool(ctx context.Context, pool taskdb.Pool) error {
	var (
		now       = time.Now().UTC()
		id        = pool.PoolID.Digest()
		start     = pool.Start
		resources = pool.Resources
		res       string
	)
	if start.IsZero() {
		start = now
	}
	if r := resources; !r.Equal(nil) {
		if b, err := json.Marshal(r); err == nil {
			res = string(b)
		}
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(t.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.String()),
			},
			colID4: {
				S: aws.String(id.Short()),
			},
			colPoolID: {
				S: aws.String(pool.PoolID.String()),
			},
			colURI: {
				S: aws.String(pool.URI),
			},
			colPoolType: {
				S: aws.String(pool.PoolType),
			},
			colResources: {
				S: aws.String(res),
			},
			colClusterName: {
				S: aws.String(pool.ClusterName),
			},
			colUser: {
				S: aws.String(pool.User),
			},
			colReflowVersion: {
				S: aws.String(pool.ReflowVersion),
			},
			colType: {
				S: aws.String(string(poolObj)),
			},
			colStartTime: {
				S: aws.String(start.UTC().Format(timeLayout)),
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

// SetResources sets the resources field in the taskdb for the row with the given id.
func (t *TaskDB) SetResources(ctx context.Context, id digest.Digest, resources reflow.Resources) error {
	var res string
	if r := resources; !r.Equal(nil) {
		if b, err := json.Marshal(r); err == nil {
			res = string(b)
		}
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.String()),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :resources", colResources)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":resources": {S: aws.String(res)},
		},
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err

}

// SetEndTime sets the end time for the given id.
func (t *TaskDB) SetEndTime(ctx context.Context, id digest.Digest, end time.Time) error {
	if end.IsZero() {
		end = time.Now()
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			colID: {
				S: aws.String(id.String()),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :endtime", colEndTime)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":endtime": {S: aws.String(end.UTC().Format(timeLayout))},
		},
	}
	_, err := t.DB.UpdateItemWithContext(ctx, input)
	return err
}

// keepalive sets the keepalive for the specified id to keepalive.
func (t *TaskDB) KeepIDAlive(ctx context.Context, id digest.Digest, keepalive time.Time) error {
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

	if q.User != "" && q.Typ == runObj {
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
		queries = t.buildIndexQuery(ID4, id4Index, taskQuery.ID.IDShort(), taskObj)
	case taskQuery.ID.IsValid():
		queries = t.buildIndexQuery(ID, idIndex, taskQuery.ID.ID(), taskObj)
	case taskQuery.RunID.IsValid():
		queries = t.buildIndexQuery(RunID, runIDIndex, taskQuery.RunID.ID(), taskObj)
	case taskQuery.ImgCmdID.IsValid():
		queries = t.buildIndexQuery(ImgCmdID, imgCmdIDIndex, taskQuery.ImgCmdID.ID(), taskObj)
	case taskQuery.Ident != "":
		queries = t.buildIndexQuery(Ident, identIndex, taskQuery.Ident, taskObj)
	default:
		q := query{
			Since: taskQuery.Since,
			Typ:   taskObj,
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
			id, fid, runid, allocID, imgCmdID digest.Digest
			result, stderr, stdout, inspect   digest.Digest
			keepalive, et                     time.Time
			ident, uri                        string
			attempt                           int
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
		if allocIDValue, ok := it[colAllocID]; ok {
			allocID, err = digest.Parse(*allocIDValue.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse allocID %v: %v", *allocIDValue.S, err))
			}
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
		if v, ok := it[colEndTime]; ok {
			et, err = time.Parse(timeLayout, *v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse endtime %v: %v", *it[colEndTime].S, err))
			}
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
		if v, ok := it[colAttempt]; ok {
			attempt, err = strconv.Atoi(*v.N)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse attempt %v: %v", *it[colAttempt].N, err))
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
			AllocID:   allocID,
			ResultID:  result,
			ImgCmdID:  taskdb.ImgCmdID(imgCmdID),
			Ident:     ident,
			URI:       uri,
			Keepalive: keepalive,
			Start:     st,
			End:       et,
			Stdout:    stdout,
			Stderr:    stderr,
			Inspect:   inspect,
			Attempt:   attempt,
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
		queries = t.buildIndexQuery(ID4, id4Index, runQuery.ID.IDShort(), runObj)
	case runQuery.ID.IsValid():
		queries = t.buildIndexQuery(ID, idIndex, runQuery.ID.ID(), runObj)
	default:
		q := query{
			ID:    digest.Digest(runQuery.ID),
			Since: runQuery.Since,
			User:  runQuery.User,
			Typ:   runObj,
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
		var et time.Time
		if v, ok := it[colEndTime]; ok {
			et, err = time.Parse(timeLayout, *v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse endtime %v: %v", *it[colEndTime].S, err))
			}
		}
		var execLog, sysLog, evalGraph, trace digest.Digest
		if v, ok := it[colExecLog]; ok {
			execLog, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse execLog %v: %v", *v.S, err))
			}
		}
		if v, ok := it[colSysLog]; ok {
			sysLog, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse sysLog %v: %v", *v.S, err))
			}
		}
		if v, ok := it[colEvalGraph]; ok {
			evalGraph, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse evalGraph %v: %v", *v.S, err))
			}
		}
		if v, ok := it[colTrace]; ok {
			trace, err = digest.Parse(*v.S)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse trace %v: %v", *v.S, err))
			}
		}
		runs = append(runs, taskdb.Run{
			ID:        taskdb.RunID(id),
			Labels:    l,
			User:      *it["User"].S,
			Keepalive: keepalive,
			Start:     st,
			End:       et,
			ExecLog:   execLog,
			SysLog:    sysLog,
			EvalGraph: evalGraph,
			Trace:     trace,
		})
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
