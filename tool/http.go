// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"
)

func (c *Cmd) http(ctx context.Context, args ...string) {
	var (
		flags = flag.NewFlagSet("http", flag.ExitOnError)
		help  = `Command http can be used to do an HTTP GET on any URL. For example:
  reflow http https://<ec2instance-url>:9000/debug/vars - will dump the vars from the reflowlet
  reflow http https://<ec2instance-url>:9000/debug/pprof - to see profiling data`
	)
	c.Parse(flags, args, help, "http url")
	if flags.NArg() != 1 {
		flags.Usage()
	}
	arg := flags.Arg(0)

	httpClient, err := c.httpClient()
	c.must(err)
	resp, err := httpClient.Get(arg)
	c.must(err)
	if resp.Body == nil {
		return
	}
	defer resp.Body.Close()
	if _, err := io.Copy(c.Stdout, resp.Body); err != nil {
		c.Fatal(err)
	}
}
