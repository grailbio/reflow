// Reflowlet is the agent process that is run on nodes in a Reflow
// cluster. Reflowlet instantiates a local reflow pool and exposes it
// via the standard REST API. Reflowlet receives a profile token
// through a flag; it uses this to: (1) mutually authenticate with
// reflow evaluator processes; (2) other Reflowlet instances for
// direct file transfers; and (3) to S3 buckets used for caching.
package main

import (
	_ "expvar"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"

	"github.com/grailbio/reflow/config"
	_ "github.com/grailbio/reflow/config/awsenvconfig"
	_ "github.com/grailbio/reflow/config/dockerconfig"
	_ "github.com/grailbio/reflow/config/ec2metadataconfig"
	_ "github.com/grailbio/reflow/config/httpscaconfig"
	_ "github.com/grailbio/reflow/config/httpsconfig"
	_ "github.com/grailbio/reflow/config/s3config"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/reflowlet"
)

// maxConcurrentStreams is the number of concurrent http/2 streams we
// support.
const maxConcurrentStreams = 20000

func usage() {
	fmt.Fprintf(os.Stderr, `usage: reflowlet [flags]

Reflowlet is the agent process for Reflow. It exposes a Reflow pool
through a REST API. A single Reflowlet can serve multiple Reflow
invocations at any given time.

In a typical configuration, Reflowlets are automatically launched
through Reflow's ec2cluster mechanism, but they may also be launched
manually if one wishes to outsource cluster management.
`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	// Make sure that we always shut down with a non-zero exit code,
	// so that systemd considers the process failed.
	defer os.Exit(1)
	var server reflowlet.Server
	server.AddFlags(flag.CommandLine)
	flag.Usage = usage
	flag.Parse()
	server.Config = make(config.Base)
	go reflowlet.IgnoreSigpipe()
	log.Fatal(server.ListenAndServe())

}
