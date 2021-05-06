// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"os"

	"github.com/grailbio/reflow/bootstrap"
)

func main() {
	if err := bootstrap.Flags(bootstrap.DefaultSchemaKeys).Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	bootstrap.RunServer(bootstrap.DefaultSchema, bootstrap.DefaultSchemaKeys)
}
