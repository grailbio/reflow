// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bootstrap

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws"
	_ "github.com/grailbio/infra/ec2metadata"
	infratls "github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/bootstrap/common"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/rest"
	"gopkg.in/yaml.v2"
)

var (
	configFile string
	addr       string
	insecure   bool
	expiry     time.Duration

	flags    *flag.FlagSet
	cfgFlags map[string]*string

	// DefaultSchema defines the default schema for the Bootstrap server.
	DefaultSchema = infra.Schema{
		infra2.AWSCreds: new(credentials.Credentials),
		infra2.Log:      new(log.Logger),
		infra2.Session:  new(session.Session),
		infra2.SSHKey:   new(infra2.SshKey),
		infra2.TLS:      new(infratls.Certs),
	}
	// DefaultSchemaKeys defines the default infra keys for the Bootstrap server.
	DefaultSchemaKeys = infra.Keys{
		infra2.AWSCreds: "awscreds",
		infra2.Log:      "logger",
		infra2.Session:  "awssession",
		infra2.SSHKey:   "key",
		infra2.TLS:      "tls,file=/tmp/ca.reflow",
	}
)

func Flags(schemaKeys infra.Keys) *flag.FlagSet {
	if flags == nil {
		flags = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		flags.StringVar(&configFile, "config", os.ExpandEnv("$HOME/.reflow/config.yaml"), "the Reflow configuration file")
		flags.StringVar(&addr, "addr", ":9000", "HTTPS server address")
		flags.BoolVar(&insecure, "insecure", false, "listen on HTTP, not HTTPS")
		flags.DurationVar(&expiry, "expiry", 10*time.Minute, "Time to expiry (if no image is installed)")
		// Add flags to override configuration.
		cfgFlags = make(map[string]*string)
		for key := range schemaKeys {
			cfgFlags[key] = flags.String(key, "", fmt.Sprintf("override %s from config", key))
		}
	}
	return flags
}

// RunServer runs the bootstrap server.
func RunServer(schema infra.Schema, schemaKeys infra.Keys) {
	if configFile != "" {
		b, err := ioutil.ReadFile(configFile)
		if err != nil {
			log.Fatal(err)
		}
		keys := make(infra.Keys)
		if err := yaml.Unmarshal(b, keys); err != nil {
			log.Fatalf("config %v: %v", configFile, err)
		}
		for k, v := range keys {
			schemaKeys[k] = v
		}
	}
	for k, v := range cfgFlags {
		if *v == "" {
			continue
		}
		schemaKeys[k] = *v
	}
	var err error
	config, err := schema.Make(schemaKeys)
	if err != nil {
		log.Fatal(err)
	}

	var sess *session.Session
	if err = config.Instance(&sess); err != nil {
		log.Fatal(err)
	}
	var tlsa infratls.Certs
	if err = config.Instance(&tlsa); err != nil {
		log.Fatal(err)
	}
	_, serverConfig, err := tlsa.HTTPS()
	if err != nil {
		log.Fatal(err)
	}
	serverConfig.ClientAuth = tls.RequireAndVerifyClientCert

	serv := server{
		addr:     addr,
		insecure: insecure,
		sess:     sess,
		config:   serverConfig,
		expiry:   expiry,
	}

	log.Fatal(serv.listenAndServe())
}

// A server exports a simple service which allows clients to execute any image or binary referenced
// by an S3 path with a given set of arguments
type server struct {
	// sess is the AWS session instance this server will use to make AWS (specifically S3) calls
	sess *session.Session

	// config is the tls config specified in the reflow config
	config *tls.Config

	// addr is the address on which to listen.
	addr string

	// insecure listens on HTTP, not HTTPS.
	insecure bool

	// expiry is the duration after which the server will stop running (if no image is installed)
	expiry time.Duration
}

// listenAndServe serves the bootstrap server on the configured address.
func (s *server) listenAndServe() error {
	start := time.Now()
	bl := &blob.Mux{"s3": s3blob.New(s.sess)}

	http.Handle("/v1/execimage", rest.DoFuncHandler(newExecImageNode(bl), nil))
	http.Handle("/v1/status", rest.DoFuncHandler(
		func(ctx context.Context, call *rest.Call) {
			if !call.Allow("GET") {
				return
			}
			call.Reply(http.StatusOK, struct{}{})
		}, nil))

	log.Printf("bootstrap server running, waiting (%s) for image...", s.expiry)
	time.AfterFunc(s.expiry, func() {
		log.Fatalf("no bootstrap image installed after %s (> expiry %s); shutting down", time.Since(start), s.expiry)
	})

	server := &http.Server{Addr: s.addr}
	if s.insecure {
		return server.ListenAndServe()
	}
	server.TLSConfig = s.config

	return server.ListenAndServeTLS("", "")
}

func newExecImageNode(m *blob.Mux) rest.DoFunc {
	return func(ctx context.Context, call *rest.Call) {
		if !call.Allow("POST") {
			return
		}
		var image common.Image
		log.Debugf("execing image...")
		if err := call.Unmarshal(&image); err != nil {
			call.Error(fmt.Errorf("unmarshal execimage %v", err))
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
