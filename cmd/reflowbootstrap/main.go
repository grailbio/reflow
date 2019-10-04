// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"

	"github.com/grailbio/reflow/bootstrap"
)

var (
	configFile = flag.String("config", os.ExpandEnv("$HOME/.reflow/config.yaml"), "the Reflow configuration file")
	addr       = flag.String("addr", ":9000", "HTTPS server address")
	insecure   = flag.Bool("insecure", false, "listen on HTTP, not HTTPS")
)

func main() {
	flag.Parse()
	bootstrap.RunServer(bootstrap.DefaultSchema, bootstrap.DefaultSchemaKeys, *configFile, *addr, *insecure)
}
