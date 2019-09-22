// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"crypto/tls"
	"flag"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws"
	_ "github.com/grailbio/infra/ec2metadata"
	infratls "github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow/bootstrap"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"gopkg.in/yaml.v2"
)

var (
	configFile = flag.String("config", os.ExpandEnv("$HOME/.reflow/config.yaml"), "the Reflow configuration file")
	addr       = flag.String("addr", ":9000", "HTTPS server address")
	insecure   = flag.Bool("insecure", false, "listen on HTTP, not HTTPS")
)

func main() {
	flag.Parse()

	schema := infra.Schema{
		infra2.AWSCreds: new(credentials.Credentials),
		infra2.Log:      new(log.Logger),
		infra2.Session:  new(session.Session),
		infra2.SSHKey:   new(infra2.SshKey),
		infra2.TLS:      new(infratls.Authority),
	}
	schemaKeys := infra.Keys{
		infra2.AWSCreds: "awscreds",
		infra2.Log:      "logger",
		infra2.Session:  "awssession",
		infra2.SSHKey:   "key",
		infra2.TLS:      "tls,file=/tmp/ca.reflow",
	}

	configFlag := *configFile
	if configFlag != "" {
		b, err := ioutil.ReadFile(configFlag)
		if err != nil {
			log.Fatal(err)
		}
		keys := make(infra.Keys)
		if err := yaml.Unmarshal(b, keys); err != nil {
			log.Fatal("config %v: %v", configFlag, err)
		}
		for k, v := range keys {
			schemaKeys[k] = v
		}
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
	var tlsa *infratls.Authority
	if err = config.Instance(&tlsa); err != nil {
		log.Fatal(err)
	}
	_, serverConfig, err := tlsa.HTTPS()
	if err != nil {
		log.Fatal(err)
	}
	serverConfig.ClientAuth = tls.RequireAndVerifyClientCert

	serv := bootstrap.BootstrapServer{
		Addr:         *addr,
		Insecure:     *insecure,
		AwsSession:   sess,
		ServerConfig: serverConfig,
	}

	log.Fatal(serv.ListenAndServe())
}
