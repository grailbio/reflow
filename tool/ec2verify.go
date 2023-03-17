// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/ec2cluster/instances"
	"github.com/grailbio/reflow/runtime"
)

func (c *Cmd) ec2verify(ctx context.Context, args ...string) {
	var (
		flags = flag.NewFlagSet("ec2verify [--probe] [--retry] [--package <gopath>]", flag.ExitOnError)
		help  = `ec2verify verifies reflowlet start-up on all previously unverified EC2 instance types
and outputs a config with the verified instance types only.
Previously attempted but failed instance types can be tried again using the flag --retry.
Optionally, the -package will write out the verified results to a Go file verified.go in the given package

Example usage:
> reflow ec2verify --retry --probe --package=$GRAIL/go/src/github.com/grailbio/reflow/ec2cluster/instances
`
		probeFlag = flags.Bool("probe", false, "whether to actually probe and verify instance types or just do a dry run")
		limitFlag = flags.Int("limit", 10, "number of instance types to probe concurrently (default 10)")
		maxFlag   = flags.Int("max", -1, "max number of instance types of those that need to be verified to actually verify (ignored if <=0)")
		retry     = flags.Bool("retry", false, "whether to retry previously attempted but unverified instance types")
		pkgPath   = flags.String("package", "", "if specified, the result of verification will be saved in a file verified.go in this Go package")
	)
	c.Parse(flags, args, help, "ec2verify")

	cluster, err := runtime.ClusterInstance(c.Config)
	c.must(err)
	var (
		ec *ec2cluster.Cluster
		ok bool
	)
	if ec, ok = cluster.(*ec2cluster.Cluster); ok {
		ec.Status = c.Status.Group("ec2verify")
		var wg sync.WaitGroup
		ec.Start(ctx, &wg)
	} else {
		c.Fatalf("not an ec2cluster - %s %T", cluster.GetName(), cluster)
	}

	// Find instance types that need to be verified.
	verified := instances.VerifiedByRegion[ec.Region()]
	toverify := instancesToVerify(instances.Types, verified, *retry)
	if len(toverify) == 0 {
		if _, err := c.Stdout.Write([]byte("no instance types to be verified\n")); err != nil {
			c.Fatal(err)
		}
		c.Exit(0)
	}
	if max := *maxFlag; max > 0 {
		if len(toverify) < max {
			max = len(toverify)
		}
		toverify = toverify[:max]
	}
	c.Log.Printf("instance types to be verified [%d]: %s\n", len(toverify), strings.Join(toverify, ", "))

	// Verify.
	if *probeFlag {
		results := probe(ctx, ec, toverify, *limitFlag)
		for _, r := range results {
			verified[r.ec2Type] = instances.VerifiedStatus{Attempted: true, Verified: r.err == nil, ApproxETASeconds: int64(r.duration.Seconds()), MemoryBytes: r.memBytes}
			if r.err == nil {
				c.Log.Printf("Successfully verified instance type: %s (took %s)\n", r.ec2Type, r.duration)
			}
		}
		// Log failed results separately.
		for _, r := range results {
			if r.err != nil {
				c.Log.Printf("Failed to verify instance type: %s (took %s) - %v\n", r.ec2Type, r.duration, r.err)
			}
		}
	}

	// Write to verified.go.
	if *pkgPath != "" {
		dir := *pkgPath
		vgen := instances.VerifiedSrcGenerator{Package: filepath.Base(dir), VerifiedByRegion: instances.VerifiedByRegion}
		src, err := vgen.Source()
		c.must(err)
		c.must(os.MkdirAll(dir, 0777))
		path := filepath.Join(dir, "verified.go")
		c.must(ioutil.WriteFile(path, src, 0644))
	}
}

type probeResult struct {
	ec2Type  string
	memBytes int64
	duration time.Duration
	err      error
}

// probe is a helper function to probe many instance types concurrently.
func probe(ctx context.Context, cluster *ec2cluster.Cluster, instanceTypes []string, limit int) []probeResult {
	results := make([]probeResult, len(instanceTypes))
	_ = traverse.Limit(limit).Each(len(instanceTypes), func(i int) error {
		r, d, err := cluster.Probe(ctx, instanceTypes[i])
		var memBytes int64
		if r != nil {
			memBytes = int64(r["mem"])
		}
		results[i] = probeResult{instanceTypes[i], memBytes, d, err}
		return nil
	})
	return results
}

// instancesToVerify returns a list of instance types that need to be verified given a list of
// instance types and an existing mapping of instance types to verification status.
// If retry is set, already attempted (but unverified) instance types are also included.
func instancesToVerify(instanceTypes []instances.Type, existing map[string]instances.VerifiedStatus, retry bool) (toverify []string) {
	for _, typ := range instanceTypes {
		vs, ok := existing[typ.Name]
		if !ok {
			// Attempt to verify instance type if verification status is unknown.
			toverify = append(toverify, typ.Name)
		} else if !vs.Verified && (!vs.Attempted || retry) {
			// Attempt to verify unverified instance type if not previously attempted or if retry is true.
			toverify = append(toverify, typ.Name)
		}
	}
	sort.Strings(toverify)
	return
}
