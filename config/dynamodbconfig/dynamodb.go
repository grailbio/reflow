// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dynamodbconfig

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/assoc/dydbassoc"
	"github.com/grailbio/reflow/config"
)

func init() {
	config.Register(config.Assoc, "dynamodb", "table", "configure an assoc using the provided DynamoDB table name",
		func(cfg config.Config, arg string) (config.Config, error) {
			if arg == "" {
				return nil, errors.New("table name not provided")
			}
			return &Assoc{cfg, arg}, nil
		},
	)
}

// Assoc is a dynamodb-based assoc configuration provider.
type Assoc struct {
	config.Config
	Table string
}

// Assoc returns a new dynamodb-backed assoc, as configured.
func (a *Assoc) Assoc() (assoc.Assoc, error) {
	sess, err := a.AWS()
	if err != nil {
		return nil, err
	}
	return &dydbassoc.Assoc{
		DB:        dynamodb.New(sess),
		TableName: a.Table,
	}, nil
}
