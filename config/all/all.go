// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package all imports all standard configuration providers
// in Reflow.
package all

import (
	_ "github.com/grailbio/reflow/config/awsenvconfig"
	_ "github.com/grailbio/reflow/config/dockerconfig"
	_ "github.com/grailbio/reflow/config/dynamodbconfig"
	_ "github.com/grailbio/reflow/config/ec2metadataconfig"
	_ "github.com/grailbio/reflow/config/httpscaconfig"
	_ "github.com/grailbio/reflow/config/httpsconfig"
	_ "github.com/grailbio/reflow/config/s3config"
)
