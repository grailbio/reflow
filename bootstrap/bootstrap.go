// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bootstrap

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/grailbio/reflow/bootstrap/common"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/rest"
)

// A BootstrapServer exports a simple service which allows clients to execute any image or binary referenced
// by an S3 path with a given set of arguments
type BootstrapServer struct {
	// AwsSession is the AWS session instance this server will use to make AWS (specifically S3) calls
	AwsSession *session.Session

	// ServerConfig is the tls config specified in the reflow config
	ServerConfig *tls.Config

	// Addr is the address on which to listen.
	Addr string

	// Insecure listens on HTTP, not HTTPS.
	Insecure bool
}

// ListenAndServe serves the bootstrap server on the configured address.
func (s *BootstrapServer) ListenAndServe() error {
	bl := &blob.Mux{"s3": s3blob.New(s.AwsSession)}

	statNode := newStatusNode()
	http.Handle("/v1/status", rest.DoFuncHandler(statNode, nil))

	const expiry = 10 * time.Minute
	ticker := time.NewTicker(expiry)

	go func() {
		<-ticker.C
		log.Fatalf("no bootstrap image installed after %s; shutting down", expiry)
	}()

	http.Handle("/v1/execimage", rest.DoFuncHandler(newExecImageNode(bl), nil))

	server := &http.Server{Addr: s.Addr}
	if s.Insecure {
		return server.ListenAndServe()
	}
	server.TLSConfig = s.ServerConfig

	return server.ListenAndServeTLS("", "")
}

func newExecImageNode(m *blob.Mux) rest.DoFunc {
	return func(ctx context.Context, call *rest.Call) {
		if !call.Allow("POST") {
			return
		}
		var image common.Image
		if err := call.Unmarshal(&image); err != nil {
			return
		}
		rc, _, err := m.Get(ctx, image.Path, "")
		if err != nil {
			call.Error(fmt.Errorf("bootstrap execimage POST %v: %v", image, err))
			return
		}

		if err := common.InstallImage(rc, image.Args, image.Name); err != nil {
			call.Error(fmt.Errorf("bootstrap execimage POST %v: %v", image, err))
			return
		}

		panic("should never reach")
	}
}

func newStatusNode() rest.DoFunc {
	return func(ctx context.Context, call *rest.Call) {
		if !call.Allow("GET") {
			return
		}
		call.Reply(http.StatusOK, struct{}{})
	}
}
