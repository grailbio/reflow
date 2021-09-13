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
// run:  {ID, ID4, Type="run", Labels, Bundle, Args, Date, Keepalive, StartTime, EndTime, User}
// task: {ID, ID4, Type="task", Labels, Date, Attempt, Keepalive, StartTime, EndTime, FlowID, Inspect, Error, ResultID, RunID, RunID4, AllocID, ImgCmdID, Ident, Stderr, Stdout, URI}
// alloc: {ID, ID4, Type="alloc", PoolID, AllocID, Resources, URI, Keepalive, StartTime, EndTime}
// pool: {ID, ID4, Type="pool", PoolID, PoolType, ClusterID.*, Resources, URI, Keepalive, StartTime, EndTime}
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
	"flag"
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
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/repository/blobrepo"
	"github.com/grailbio/reflow/repository/s3"
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
	infra.Register(ProviderName, new(TaskDB))
}

type objType string

const (
	runObj   objType = "run"
	taskObj  objType = "task"
	allocObj objType = "alloc"
	poolObj  objType = "pool"
)

const (
	// ProviderName is the name of this TaskDB's infra config provider.
	ProviderName = "dynamodbtask"

	// TimeLayout is the time layout used to serialize time to dynamodb attributes.
	timeLayout = time.RFC3339
	// DateLayout is the layout used to serialize date.
	dateLayout = "2006-01-02"

	// perQueryItemsLimit is the number of items to retrieve per dynamodb query.
	perQueryItemsLimit = 100
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
	dateKeepaliveIndex = "Date-Keepalive-index"

	// The following indices are applicable for the Predictor use-case.
	// The *SortEndIndex are the (newer) versions which support sorting by "EndTime" column.
	imgCmdIDSortEndIndex = "ImgCmdID-SortEnd-index"
	identSortEndIndex    = "Ident-SortEnd-index"
	// TODO(swami): Remove the following indices once we've fully migrated to the newer ones.
	// Note: No external reflow versions were released with support for TaskDB using these older indices.
	imgCmdIDIndex        = "ImgCmdID-index"
	identIndex           = "Ident-index"
)

// TaskDB implements the dynamodb backed taskdb.TaskDB interface to
// store run/task state and metadata.
// Each association is either:
// a) RunID and its associated metadata (run labels, user info, and leases)
// b) TaskID and its associated metadata (RunID that spawned this task, FlowID of the node, and leases)
type TaskDB struct {
	infra2.TableNameFlagsTrait
	infra2.BucketNameFlagsTrait
	// DB is the dynamodb.
	DB dynamodbiface.DynamoDBAPI
	// Labels on the run.
	Labels []string
	// User who initiated this run.
	User string
	// Limiter limits number of concurrent operations.
	limiter *limiter.Limiter

	// Repo is the repository to store large objects referenced from this TaskDB.
	Repo *blobrepo.Repository

	// The currently active index names for ImgCmdId and Ident columns.
	activeImgCmdIDIndexName, activeIdentIndexName string
}

func (t *TaskDB) String() string {
	return fmt.Sprintf("%T,TableName=%s,BucketName=%s,Labels=%s", t, t.TableName, t.BucketName, strings.Join(t.Labels, ","))
}

// Help implements infra.Provider.
func (TaskDB) Help() string {
	return "configure a dynamodb table to store run/task information"
}

// Init implements infra.Provider.
func (t *TaskDB) Init(sess *session.Session, user *infra2.User, labels pool.Labels) (err error) {
	if t.TableName == "" {
		return fmt.Errorf("TaskDB table name cannot be empty")
	}
	switch {
	case t.Repo != nil:
	case t.BucketName == "":
		return fmt.Errorf("TaskDB bucket name cannot be empty")
	default:
		if t.Repo, err = s3.InitRepo(sess, t.BucketName); err != nil {
			return
		}
	}
	t.limiter = limiter.New()
	t.limiter.Release(32)
	t.DB = dynamodb.New(sess)
	t.Labels = make([]string, 0, len(labels))
	for k, v := range labels {
		t.Labels = append(t.Labels, fmt.Sprintf("%s=%s", k, v))
	}
	t.User = string(*user)
	return
}

// Flags implements infra.Provider.
func (t *TaskDB) Flags(flags *flag.FlagSet) {
	t.TableNameFlagsTrait.Flags(flags)
	t.BucketNameFlagsTrait.Flags(flags)
}

// Version implements infra.Provider.
func (t *TaskDB) Version() int {
	return 2
}

// determineIndices determines which indices are available for querying `ImgCmdID` and `Ident` columns,
// and sets those as the "active" ones, preferring the (newer) sorted indices over the older ones.
func (t *TaskDB) determineIndices() error {
	// Skip if already determined.
	if t.activeIdentIndexName != "" && t.activeImgCmdIDIndexName != "" {
		return nil
	}
	describe, err := waitForActiveTable(t.DB, t.TableName, nil)
	if err != nil {
		return errors.E("determineIndices", err)
	}
	activeIndices := make(map[string]bool)
	for _, index := range describe.Table.GlobalSecondaryIndexes {
		if *index.IndexStatus != dynamodb.IndexStatusActive {
			continue
		}
		activeIndices[*index.IndexName] = true
	}
	if _, ok := activeIndices[imgCmdIDSortEndIndex]; ok {
		t.activeImgCmdIDIndexName = imgCmdIDSortEndIndex
	} else if _, ok = activeIndices[imgCmdIDIndex]; ok {
		t.activeImgCmdIDIndexName = imgCmdIDIndex
	} else {
		indexNames := make([]string, 0, len(activeIndices))
		for k := range activeIndices {
			indexNames = append(indexNames, k)
		}
		return fmt.Errorf("no usable index found for activeImgCmdIDIndexName (available indices: %s)", strings.Join(indexNames, ","))
	}
	if _, ok := activeIndices[identSortEndIndex]; ok {
		t.activeIdentIndexName = identSortEndIndex
	} else if _, ok = activeIndices[identIndex]; ok {
		t.activeIdentIndexName = identIndex
	} else {
		indexNames := make([]string, 0, len(activeIndices))
		for k := range activeIndices {
			indexNames = append(indexNames, k)
		}
		return fmt.Errorf("no usable index found for activeIdentIndexName (available indices: %s)", strings.Join(indexNames, ","))
	}
	return nil
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
func (t *TaskDB) StartAlloc(ctx context.Context, allocID reflow.StringDigest, poolID digest.Digest, resources reflow.Resources, start time.Time) error {
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
				S: aws.String(poolID.String()),
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

// buildIndexQuery returns a dynamodb QueryInput for the specified key on the specified index where:
// - indexName is the name of the index
// - kind is the index column name
// - partKey is the value of the index column to lookup
// - typ is the type of objects to return (used as a filter)
// - cols (if set) is the column/attribute values to return (if unset, all columns are returned)
func (t *TaskDB) buildIndexQuery(kind taskdb.Kind, indexName, partKey string, typ objType, cols ...taskdb.Kind) *dynamodb.QueryInput {
	colname, colnameOk := colmap[kind]
	if !colnameOk {
		panic("invalid kind")
	}
	keyExpression := fmt.Sprintf("%s = :keyval", colname)
	attributeValues := map[string]*dynamodb.AttributeValue{
		":keyval": {S: aws.String(partKey)},
		":type":   {S: aws.String(string(typ))},
	}
	exprAttrNames := map[string]string{"#Type": colType}
	filters := []string{"#Type = :type"}
	projCols := make([]string, len(cols))
	if len(cols) > 0 {
		for i, col := range cols {
			colname, ok := colmap[col]
			if !ok {
				panic("invalid kind")
			}
			deref := fmt.Sprintf("#%s", colname)
			exprAttrNames[deref] = colname
			projCols[i] = deref
			filters = append(filters, fmt.Sprintf("attribute_exists(%s)", deref))
		}
	}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(t.TableName),
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyExpression),
		ExpressionAttributeValues: attributeValues,
		FilterExpression:          aws.String(strings.Join(filters, " AND ")),
		ExpressionAttributeNames:  aws.StringMap(exprAttrNames),
	}
	// ProjectionExpression must only be set if it is non-empty.
	if len(projCols) > 0 {
		input.ProjectionExpression = aws.String(strings.Join(projCols, ", "))
	}
	return input
}


// buildSortEndIndexQuery returns a dynamodb QueryInput for the specified key on the specified index where:
// - indexName is the name of the index and must be one of the "SortEnd" indices.
// - kind is the index column name
// - partKey is the value of the index column to lookup
func (t *TaskDB) buildSortEndIndexQuery(kind taskdb.Kind, indexName, partKey string) *dynamodb.QueryInput {
	colname, colnameOk := colmap[kind]
	if !colnameOk {
		panic("invalid kind")
	}
	if indexName != imgCmdIDSortEndIndex && indexName != identSortEndIndex {
		panic(fmt.Sprintf("unrecognized 'SortEnd' index: %s", indexName))
	}
	keyExpression := fmt.Sprintf("%s = :keyval", colname)
	attributeValues := map[string]*dynamodb.AttributeValue{
		":keyval": {S: aws.String(partKey)},
	}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(t.TableName),
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyExpression),
		ExpressionAttributeValues: attributeValues,
		ScanIndexForward:          aws.Bool(false),
	}
	return input
}

// buildSinceQueries builds queries that return taskdb rows with Type set to `typ`,
// and whose keepalive column value is from `since` and now.
// If filters is specified, the queries will return only taskdb rows with column values matching
// the column kind to value specified in the map
func (t *TaskDB) buildSinceQueries(typ objType, since time.Time, filters map[taskdb.Kind]string) []*dynamodb.QueryInput {
	if since.IsZero() {
		panic("taskdb invalid query: missing since")
	}
	switch typ {
	case runObj, taskObj, poolObj, allocObj:
	default:
		panic(fmt.Sprintf("taskdb invalid query: unrecognized row type %s", typ))
	}
	since = since.UTC()
	// Build time bucket based queries.
	type part struct {
		keyExpression string
		attrValues    map[string]*dynamodb.AttributeValue
		attrNames     map[string]*string
	}
	var (
		keyExpression   string
		timeBuckets     []part
		filterExprs     = []string{"#Type = :type"}
		attributeValues = map[string]*dynamodb.AttributeValue{":type": {S: aws.String(string(typ))}}
		attributeNames  = map[string]*string{"#Type": aws.String(colType)}
		now             = time.Now().UTC()
	)
	for _, d := range dates(since, now) {
		part := part{
			keyExpression: "#Date = :date and " + colKeepalive + " > :ka ",
			attrValues: map[string]*dynamodb.AttributeValue{
				":date": {S: aws.String(d.Format(dateLayout))},
				":ka":   {S: aws.String(since.Format(timeLayout))},
			},
			attrNames: map[string]*string{"#Date": aws.String(colDate)},
		}
		timeBuckets = append(timeBuckets, part)
	}

	for k, v := range filters {
		key, ok := colmap[k]
		if !ok {
			panic(fmt.Sprintf("invalid kind %v", k))
		}
		if v == "" {
			panic(fmt.Sprintf("empty value for kind %v", k))
		}
		derefK, subK := "#"+key, ":"+strings.ToLower(key)
		filterExprs = append(filterExprs, fmt.Sprintf("%s = %s", derefK, subK))
		attributeValues[subK] = &dynamodb.AttributeValue{S: aws.String(v)}
		attributeNames[derefK] = aws.String(key)
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
			if len(filterExprs) > 0 {
				query.FilterExpression = aws.String(strings.Join(filterExprs, " and "))
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
	if len(filterExprs) > 0 {
		input.FilterExpression = aws.String(strings.Join(filterExprs, " and "))
	}
	return []*dynamodb.QueryInput{input}
}

// Tasks returns tasks that matches the query.
func (t *TaskDB) Tasks(ctx context.Context, q taskdb.TaskQuery) ([]taskdb.Task, error) {
	var (
		queries   []*dynamodb.QueryInput
		withAlloc bool
	)
	switch {
	case q.ID.IsValid() && digest.Digest(q.ID).IsAbbrev():
		withAlloc = q.WithAlloc
		queries = append(queries, t.buildIndexQuery(ID4, id4Index, q.ID.IDShort(), taskObj))
	case q.ID.IsValid():
		withAlloc = q.WithAlloc
		queries = append(queries, t.buildIndexQuery(ID, idIndex, q.ID.ID(), taskObj))
	case q.RunID.IsValid():
		withAlloc = q.WithAlloc
		queries = append(queries, t.buildIndexQuery(RunID, runIDIndex, q.RunID.ID(), taskObj))
	case q.ImgCmdID.IsValid():
		// Determine which index to use for `ImgCmdId` column.
		if err := t.determineIndices(); err != nil {
			return nil, err
		}
		var query *dynamodb.QueryInput
		switch t.activeImgCmdIDIndexName {
		case imgCmdIDSortEndIndex:
			query = t.buildSortEndIndexQuery(ImgCmdID, imgCmdIDSortEndIndex, q.ImgCmdID.ID())
			// Since this is the predictor use-case, we know that by default the limit is 1000.
			// (See `queryLimit` in `predictor/taskgroup.go`)
			// Reduce the total number of rows fetched, when using the sorted index.
			// TODO(swami): Remove this adjustment once we've fully migrated to the new indices.
			q.Limit /= 4
		case imgCmdIDIndex:
			// ImgCmdID is set only for the predictor use-case, so we limit the columns to only the ones we know it needs.
			// Ideally, the columns should also be set in the query, but they are an implementation detail
			// and not exposed through the TaskDB interface.
			query = t.buildIndexQuery(ImgCmdID, imgCmdIDIndex, q.ImgCmdID.ID(), taskObj, ID, EndTime, ExecInspect)
		}
		if query != nil {
			queries = append(queries, query)
		}
	case q.Ident != "":
		// Determine which index to use for `Ident` column.
		if err := t.determineIndices(); err != nil {
			return nil, err
		}
		var query *dynamodb.QueryInput
		switch t.activeIdentIndexName {
		case identSortEndIndex:
			query = t.buildSortEndIndexQuery(Ident, identSortEndIndex, q.Ident)
			// Since this is the predictor use-case, we know that by default the limit is 1000.
			// (See `queryLimit` in `predictor/taskgroup.go`)
			// Reduce the total number of rows fetched, when using the sorted index.
			// TODO(swami): Remove this adjustment once we've fully migrated to the new indices.
			q.Limit /= 4
		case identIndex:
			// ImgCmdID is set only for the predictor use-case, so we limit the columns to only the ones we know it needs.
			// Ideally, the columns should also be set in the query, but they are an implementation detail
			// and not exposed through the TaskDB interface.
			query = t.buildIndexQuery(Ident, identIndex, q.Ident, taskObj, ID, EndTime, ExecInspect)
		}
		if query != nil {
			queries = append(queries, query)
		}
	default:
		queries = t.buildSinceQueries(taskObj, q.Since, nil)
	}
	stream := t.streamItems(ctx, queries, q.Limit)
	var (
		tasks = make([]taskdb.Task, 0, q.Limit)
		errs  errors.Multi
		err   error
	)
	for si := range stream {
		if q.Limit > 0 && len(tasks) == int(q.Limit) {
			break
		}
		if si.err != nil {
			errs.Add(errors.E("query", si.query.GoString(), si.err))
			continue
		}
		var (
			it = si.item
			t  taskdb.Task
		)
		if v := parseAttr(it, ID, parseDigestFunc, &errs); v != nil {
			t.ID = taskdb.TaskID(v.(digest.Digest))
		}
		if id, d := digest.Digest(t.ID), digest.Digest(q.ID); !d.IsZero() && d.IsAbbrev() {
			if !id.Expands(d) {
				continue
			}
		}
		if v := parseAttr(it, FlowID, parseDigestFunc, &errs); v != nil {
			t.FlowID = v.(digest.Digest)
		}
		if v := parseAttr(it, RunID, parseDigestFunc, &errs); v != nil {
			t.RunID = taskdb.RunID(v.(digest.Digest))
		}
		if v := parseAttr(it, AllocID, parseDigestFunc, &errs); v != nil {
			t.AllocID = v.(digest.Digest)
		}
		if v := parseAttr(it, ResultID, parseDigestFunc, &errs); v != nil {
			t.ResultID = v.(digest.Digest)
		}
		setTimeFields(it, &t.TimeFields, &errs)

		if v := parseAttr(it, Stdout, parseDigestFunc, &errs); v != nil {
			t.Stdout = v.(digest.Digest)
		}
		if v := parseAttr(it, Stderr, parseDigestFunc, &errs); v != nil {
			t.Stderr = v.(digest.Digest)
		}
		if v := parseAttr(it, ExecInspect, parseDigestFunc, &errs); v != nil {
			t.Inspect = v.(digest.Digest)
		}
		if v := parseAttr(it, ImgCmdID, parseDigestFunc, &errs); v != nil {
			t.ImgCmdID = taskdb.ImgCmdID(v.(digest.Digest))
		}
		if v := parseAttr(it, Resources, parseResourcesFunc, &errs); v != nil {
			t.Resources = v.(reflow.Resources)
		}
		t.Ident = parseAttr(it, Ident, nil, &errs).(string)
		t.URI = parseAttr(it, URI, nil, &errs).(string)
		if v, ok := it[colAttempt]; ok {
			t.Attempt, err = strconv.Atoi(*v.N)
			if err != nil {
				errs.Add(fmt.Errorf("parse attempt %v: %v", *v.N, err))
			}
		}
		tasks = append(tasks, t)
	}

	if withAlloc {
		allocsById := make(map[digest.Digest]*taskdb.Alloc)
		var aids []digest.Digest
		for _, t := range tasks {
			if _, ok := allocsById[t.AllocID]; !ok {
				aids = append(aids, t.AllocID)
			}
			allocsById[t.AllocID] = nil
		}
		if len(aids) > 0 {
			allocs, aerr := t.Allocs(ctx, taskdb.AllocQuery{IDs: aids})
			if aerr != nil {
				log.Errorf("taskdb.Allocs for tasks: %v", aerr)
			} else {
				for _, a := range allocs {
					alloc := a
					allocsById[a.ID] = &alloc
				}
				b := tasks[:0]
				for _, t := range tasks {
					if aid := t.AllocID; !aid.IsZero() {
						t.Alloc = allocsById[aid]
					}
					b = append(b, t)
				}
				tasks = b
			}
		}
	}
	if err = errs.Combined(); err != nil && len(tasks) > 0 {
		log.Errorf("taskdb.Tasks: %v", err)
		err = nil
	}
	return tasks, err
}

// Runs returns runs that matches the query.
func (t *TaskDB) Runs(ctx context.Context, runQuery taskdb.RunQuery) ([]taskdb.Run, error) {
	var queries []*dynamodb.QueryInput
	switch {
	case runQuery.ID.IsValid() && digest.Digest(runQuery.ID).IsAbbrev():
		queries = append(queries, t.buildIndexQuery(ID4, id4Index, runQuery.ID.IDShort(), runObj))
	case runQuery.ID.IsValid():
		queries = append(queries, t.buildIndexQuery(ID, idIndex, runQuery.ID.ID(), runObj))
	case runQuery.User != "":
		queries = t.buildSinceQueries(runObj, runQuery.Since, map[taskdb.Kind]string{User: runQuery.User})
	default:
		queries = t.buildSinceQueries(runObj, runQuery.Since, nil)
	}
	stream := t.streamItems(ctx, queries, 0)
	var (
		runs []taskdb.Run
		errs errors.Multi
	)
	for si := range stream {
		if si.err != nil {
			errs.Add(errors.E("query", si.query.GoString(), si.err))
			continue
		}
		var (
			it = si.item
			r  taskdb.Run
		)
		if v := parseAttr(it, ID, parseDigestFunc, &errs); v != nil {
			r.ID = taskdb.RunID(v.(digest.Digest))
		}
		if id, d := digest.Digest(r.ID), digest.Digest(runQuery.ID); !d.IsZero() && d.IsAbbrev() {
			if !id.Expands(d) {
				continue
			}
		}
		r.Labels = make(pool.Labels)
		for _, va := range it[colLabels].SS {
			vals := strings.Split(*va, "=")
			if len(vals) != 2 {
				errs.Add(fmt.Errorf("label not well formed: %v", *va))
				continue
			}
			r.Labels[vals[0]] = vals[1]
		}
		setTimeFields(it, &r.TimeFields, &errs)

		if v := parseAttr(it, ExecLog, parseDigestFunc, &errs); v != nil {
			r.ExecLog = v.(digest.Digest)
		}
		if v := parseAttr(it, SysLog, parseDigestFunc, &errs); v != nil {
			r.SysLog = v.(digest.Digest)
		}
		if v := parseAttr(it, EvalGraph, parseDigestFunc, &errs); v != nil {
			r.EvalGraph = v.(digest.Digest)
		}
		if v := parseAttr(it, Trace, parseDigestFunc, &errs); v != nil {
			r.Trace = v.(digest.Digest)
		}
		r.User = parseAttr(it, User, nil, &errs).(string)
		runs = append(runs, r)
	}
	if err := errs.Combined(); err != nil && len(runs) > 0 {
		log.Errorf("taskdb.Runs: %v", err)
	}
	return runs, nil
}

// Allocs returns allocs (with their pools) that matches the query.
func (t *TaskDB) Allocs(ctx context.Context, q taskdb.AllocQuery) ([]taskdb.Alloc, error) {
	var queries []*dynamodb.QueryInput
	switch {
	case len(q.IDs) == 0:
		return nil, fmt.Errorf("invalid AllocQuery (missing IDs): %v", q)
	default:
		for _, id := range q.IDs {
			switch {
			case id.IsZero(): // Skip
			case id.IsAbbrev():
				queries = append(queries, t.buildIndexQuery(ID4, id4Index, id.Short(), allocObj))
			default:
				queries = append(queries, t.buildIndexQuery(ID, idIndex, id.String(), allocObj))
			}
		}
	}
	stream := t.streamItems(ctx, queries, 0)
	var (
		allocs []taskdb.Alloc
		errs   errors.Multi
	)
	for si := range stream {
		if si.err != nil {
			errs.Add(errors.E("query", si.query.GoString(), si.err))
			continue
		}
		var (
			it = si.item
			a  taskdb.Alloc
		)
		if v := parseAttr(it, ID, parseDigestFunc, &errs); v != nil {
			a.ID = v.(digest.Digest)
		}
		setTimeFields(it, &a.TimeFields, &errs)
		if v := parseAttr(it, Resources, parseResourcesFunc, &errs); v != nil {
			a.Resources = v.(reflow.Resources)
		}
		a.URI = parseAttr(it, URI, nil, &errs).(string)
		if v := parseAttr(it, PoolID, parseDigestFunc, &errs); v != nil {
			a.PoolID = v.(digest.Digest)
		}
		allocs = append(allocs, a)
	}
	pools := make(map[digest.Digest]*taskdb.PoolRow)
	var pids []digest.Digest
	for _, a := range allocs {
		if _, ok := pools[a.PoolID]; !ok {
			pids = append(pids, a.PoolID)
		}
		pools[a.PoolID] = nil
	}
	if len(pids) > 0 {
		prs, perr := t.Pools(ctx, taskdb.PoolQuery{IDs: pids})
		if perr != nil {
			log.Errorf("taskdb.Pools for allocs: %v", perr)
		} else {
			for _, pr := range prs {
				p := pr
				pools[pr.ID] = &p
			}
			b := allocs[:0]
			for _, a := range allocs {
				if pid := a.PoolID; !pid.IsZero() {
					a.Pool = pools[pid]
				}
				b = append(b, a)
			}
			allocs = b
		}
	}
	if err := errs.Combined(); err != nil && len(allocs) > 0 {
		log.Errorf("taskdb.Allocs: %v", err)
	}
	return allocs, nil
}

// Pools returns pools that matches the query.
func (t *TaskDB) Pools(ctx context.Context, q taskdb.PoolQuery) ([]taskdb.PoolRow, error) {
	var queries []*dynamodb.QueryInput
	switch {
	case len(q.IDs) > 0:
		for _, id := range q.IDs {
			switch {
			case id.IsZero(): // Skip
			case id.IsAbbrev():
				queries = append(queries, t.buildIndexQuery(ID4, id4Index, id.Short(), poolObj))
			default:
				queries = append(queries, t.buildIndexQuery(ID, idIndex, id.String(), poolObj))
			}
		}
	case q.Since.IsZero():
		return nil, fmt.Errorf("invalid PoolQuery (missing either IDs OR Since): %v", q)
	default:
		filters := make(map[taskdb.Kind]string)
		if v := q.Cluster.ClusterName; v != "" {
			filters[ClusterName] = v
		}
		if v := q.Cluster.User; v != "" {
			filters[User] = v
		}
		if v := q.Cluster.ReflowVersion; v != "" {
			filters[ReflowVersion] = v
		}
		queries = t.buildSinceQueries(poolObj, q.Since, filters)
	}
	stream := t.streamItems(ctx, queries, 0)
	var (
		pools []taskdb.PoolRow
		errs  errors.Multi
	)
	for si := range stream {
		if si.err != nil {
			errs.Add(errors.E("query", si.query.GoString(), si.err))
			continue
		}
		var (
			it = si.item
			pr taskdb.PoolRow
		)
		if v := parseAttr(it, ID, parseDigestFunc, &errs); v != nil {
			pr.ID = v.(digest.Digest)
		}
		setTimeFields(it, &pr.TimeFields, &errs)
		pr.ClusterName = parseAttr(it, ClusterName, nil, &errs).(string)
		pr.User = parseAttr(it, User, nil, &errs).(string)
		pr.ReflowVersion = parseAttr(it, ReflowVersion, nil, &errs).(string)
		if pid := parseAttr(it, PoolID, nil, &errs).(string); pid != "" {
			pr.PoolID = reflow.NewStringDigest(pid)
		}
		if v := parseAttr(it, Resources, parseResourcesFunc, &errs); v != nil {
			pr.Resources = v.(reflow.Resources)
		}
		pr.PoolType = parseAttr(it, PoolType, nil, &errs).(string)
		pr.URI = parseAttr(it, URI, nil, &errs).(string)
		pools = append(pools, pr)
	}
	if err := errs.Combined(); err != nil && len(pools) > 0 {
		log.Errorf("taskdb.Pools: %v", err)
	}
	return pools, nil
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

// Repository implements taskdb.TaskDB.
func (t *TaskDB) Repository() reflow.Repository {
	return t.Repo
}

type streamedItem struct {
	query *dynamodb.QueryInput
	item  map[string]*dynamodb.AttributeValue
	err   error
}

// streamItems streams the items (QueryOutput.Items) for all the given queries.
// streamItems makes best-effort attempt to limit the results to limit (if it is > 0).
func (t *TaskDB) streamItems(ctx context.Context, queries []*dynamodb.QueryInput, limit int64) <-chan streamedItem {
	s := make(chan streamedItem)
	go func() {
		defer close(s)
		var total int64
		_ = traverse.Each(len(queries), func(i int) error {
			var (
				query   = queries[i]
				lastKey map[string]*dynamodb.AttributeValue
				done    bool
			)
			query.Limit = aws.Int64(perQueryItemsLimit)
			for !done {
				query.ExclusiveStartKey = lastKey
				if resp, qerr := t.DB.QueryWithContext(ctx, query); qerr != nil {
					done = true
					if aerr, ok := qerr.(awserr.Error); ok {
						switch aerr.Code() {
						case "ValidationException":
							if strings.Contains(aerr.Message(),
								"The table does not have the specified index") {
								qerr = errors.E(`index missing: run "reflow migrate"`, qerr)
							}
						}
					}
					s <- streamedItem{query: query, err: qerr}
				} else {
					for _, item := range resp.Items {
						s <- streamedItem{query: query, item: item}
						atomic.AddInt64(&total, 1)
						if done = limit > 0 && atomic.LoadInt64(&total) >= limit; done {
							break
						}
					}
					lastKey = resp.LastEvaluatedKey
					done = done || lastKey == nil
				}
			}
			return nil
		})
	}()
	return s
}

// parseAttr gets the AttributeValue corresponding to the given taskdb.Kind from map 'it'
// and parses the 'S' field of the AttributeValue using the given func 'f' (nil f acts as identity)
func parseAttr(it map[string]*dynamodb.AttributeValue, k taskdb.Kind, f func(s string) (interface{}, error), errs *errors.Multi) interface{} {
	key, ok := colmap[k]
	if !ok {
		panic(fmt.Sprintf("invalid kind %v", k))
	}
	av, ok := it[key]
	switch {
	case !ok && f == nil:
		return ""
	case !ok:
		return nil
	case f == nil:
		return *av.S
	}
	v, err := f(*av.S)
	if err != nil {
		errs.Add(fmt.Errorf("parse %s %v: %v", key, *av.S, err))
	}
	return v
}

var (
	parseTimeFunc      = func(s string) (interface{}, error) { return time.Parse(timeLayout, s) }
	parseDigestFunc    = func(s string) (interface{}, error) { return digest.Parse(s) }
	parseResourcesFunc = func(s string) (interface{}, error) {
		if len(s) == 0 {
			return nil, nil
		}
		var r reflow.Resources
		err := json.Unmarshal([]byte(s), &r)
		return r, err
	}
)

func setTimeFields(it map[string]*dynamodb.AttributeValue, dst *taskdb.TimeFields, errs *errors.Multi) {
	if v := parseAttr(it, StartTime, parseTimeFunc, errs); v != nil {
		dst.Start = v.(time.Time)
	}
	if v := parseAttr(it, KeepAlive, parseTimeFunc, errs); v != nil {
		dst.Keepalive = v.(time.Time)
	}
	if v := parseAttr(it, EndTime, parseTimeFunc, errs); v != nil {
		dst.End = v.(time.Time)
	}
}
