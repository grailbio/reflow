// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package s3client

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/grailbio/reflow/errors"
)

// ErrKind interprets an S3 API error into a Reflow error kind.
func ErrKind(err error) errors.Kind {
	aerr, ok := err.(awserr.Error)
	if !ok {
		return errors.Other
	}
	// The underlying error was an S3 error. Try to classify it.
	// Best guess based on Amazon's descriptions:
	switch aerr.Code() {
	// Code NotFound is not documented, but it's what the API actually returns.
	case "NoSuchBucket", "NoSuchKey", "NoSuchVersion", "NotFound":
		return errors.NotExist
	case "AccessDenied":
		return errors.NotAllowed
	case "InvalidRequest", "InvalidArgument", "EntityTooSmall", "EntityTooLarge", "KeyTooLong", "MethodNotAllowed":
		return errors.Fatal
	case "ExpiredToken", "AccountProblem", "ServiceUnavailable", "SlowDown", "TokenRefreshRequired", "OperationAborted":
		return errors.Unavailable
	}
	return errors.Other
}
