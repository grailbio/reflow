// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"

	"github.com/grailbio/reflow/reflowlet"
)

func (c *Cmd) serveCmd(ctx context.Context, args ...string) {
	var (
		flags = flag.NewFlagSet("serve", flag.ExitOnError)
		help  = `Runs the reflow process in 'reflowlet' which is an agent process.
It exposes a Reflow pool through a REST API. A single Reflowlet can
serve multiple Reflow invocations at any given time.

In a typical configuration, Reflowlets are automatically launched
through Reflow's ec2cluster mechanism, but they may also be launched
manually if one wishes to outsource cluster management.

Flag -config defines a configuration filename from which the Reflowlet
restores its configuration. When run in an automatic cluster configuration,
the configuration is typically sealed, containing both configuration information
as well as credentials to access various services.
`
	)
	server := reflowlet.NewServer(c.Version, c.Config)
	server.AddFlags(flags)
	c.Parse(flags, args, help, "serve [-ec2cluster]")
	go reflowlet.IgnoreSigpipe()
	// Shutdown the server if the context is done.
	go func() {
		<-ctx.Done()
		server.Shutdown()
	}()
	c.Fatal(server.ListenAndServe())
}
