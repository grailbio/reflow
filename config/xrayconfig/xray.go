// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package xrayconfig

import (
	"github.com/aws/aws-xray-sdk-go/strategy/sampling"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/trace"
	"github.com/grailbio/reflow/trace/xraytrace"
)

func init() {
	config.Register("tracer", "xray", "address", "use AWS xray for tracing. Xray daemon should be running on specified address(if unspecified defaults to 127.0.0.1:2000). Follow instructions to run the daemon locally: https://docs.aws.amazon.com/xray/latest/devguide/xray-daemon-local.html ",
		func(cfg config.Config, arg string) (config.Config, error) {
			c := &Tracer{Config: cfg, addr: arg}
			c.init()
			return c, nil
		})
}

// Tracer is the AWS Xray tracer configuration provider.
type Tracer struct {
	config.Config
	addr   string
	tracer trace.Tracer
}

func (t *Tracer) init() {
	b := []byte(`{
		"version": 1,
		"default": {
			"fixed_target": 1,
			"rate": 1.0
		},
		"rules": [ ]
	}`)
	l, err := sampling.NewLocalizedStrategyFromJSONBytes(b)
	if err != nil {
		log.Printf("parse xray strategy: %s", err)
		l, _ = sampling.NewLocalizedStrategy()
	}
	addr := "127.0.0.1:2000"
	if t.addr != "address" {
		addr = t.addr
	}
	err = xray.Configure(xray.Config{
		DaemonAddr:       addr,
		LogLevel:         "error",
		SamplingStrategy: l,
	})
	if err != nil {
		log.Print("xray: ", err)
		t.tracer = trace.NopTracer
	}
	t.tracer = xraytrace.Xray
}

// Tracer returns a Xray tracer.
func (t *Tracer) Tracer() (trace.Tracer, error) {
	return t.tracer, nil
}
