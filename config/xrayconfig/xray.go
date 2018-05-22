// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package xrayconfig

import (
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/trace/xraytrace"
)

func init() {
	config.Register("tracer", "xray", "", "use AWS xray for tracing. Xray daemon should be running locally on port 2000. Follow instructions to run the daemon locally: https://docs.aws.amazon.com/xray/latest/devguide/xray-daemon-local.html ",
		func(cfg config.Config, arg string) (config.Config, error) {
			c := &Tracer{Config: cfg}
			return c, nil
		})
}

// Tracer is the AWS Xray tracer configuration provider.
type Tracer struct {
	config.Config
}

// Tracer returns a Xray tracer.
func (t *Tracer) Tracer() (trace.Tracer, error) {
	return xraytrace.Xray, nil
}
