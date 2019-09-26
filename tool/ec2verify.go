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
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/ec2cluster/instances"
)

func (c *Cmd) ec2verify(ctx context.Context, args ...string) {
	var (
		flags = flag.NewFlagSet("ec2verify [--probe] [--retry] [--package <gopath>]", flag.ExitOnError)
		help  = `ec2verify verifies reflowlet start-up on all previously unverified EC2 instance types
and outputs a config with the verified instance types only.
Previously attempted but failed instance types can be tried again using the flag --retry.
Optionally, the -package will write out the verified results to a Go file verified.go in the given package`
		probeFlag = flags.Bool("probe", false, "whether to actually probe and verify instance types or just do a dry run")
		limitFlag = flags.Int("limit", 10, "number of instance types to probe concurrently (default 10)")
		retry     = flags.Bool("retry", false, "whether to retry previously attempted but unverified instance types")
		pkgPath   = flags.String("package", "", "if specified, the result of verification will be saved in a file verified.go in this Go package")
	)
	c.Parse(flags, args, help, "ec2verify")

	var ec *ec2cluster.Cluster
	var ok bool
	cluster := c.Cluster(c.Status.Group("ec2verify"))
	if ec, ok = cluster.(*ec2cluster.Cluster); !ok {
		c.Fatalf("not a ec2cluster! - %T", cluster)
	}
	existing := instances.VerifiedByRegion[ec.Region]
	verified, toverify := instancesToVerify(ec.InstanceTypes, existing, *retry)
	if len(toverify) == 0 {
		c.Stdout.Write([]byte("no instance types to be verified\n"))
		c.Exit(0)
	}
	sort.Strings(toverify)
	c.Log.Printf("instance types to be verified: %s\n", strings.Join(toverify, ", "))

	final := make(map[string]instances.VerifiedStatus)
	for _, it := range ec.InstanceTypes {
		vs, ok := existing[it]
		if !ok {
			vs = instances.VerifiedStatus{false, false, -1}
		}
		final[it] = vs
	}
	if *probeFlag {
		results := probe(ctx, ec, toverify, *limitFlag)
		for _, r := range results {
			final[r.ec2Type] = instances.VerifiedStatus{true, r.err == nil, int64(r.duration.Seconds())}
			if r.err == nil {
				c.Log.Printf("Successfully verified instance type: %s (took %s)\n", r.ec2Type, r.duration)
				verified = append(verified, r.ec2Type)
			}
		}
		// Log failed results separately.
		for _, r := range results {
			if r.err != nil {
				c.Log.Printf("Failed to verify instance type: %s (took %s) - %v\n", r.ec2Type, r.duration, r.err)
			}
		}
	}
	sort.Strings(verified)
	ec.InstanceTypes = verified

	data, err := c.Config.Marshal(true)
	if err != nil {
		c.Fatal(err)
	}
	c.Stdout.Write(data)
	c.Println()

	if *pkgPath != "" {
		dir := *pkgPath
		instances.VerifiedByRegion[ec.Region] = final
		vgen := instances.VerifiedSrcGenerator{filepath.Base(dir), instances.VerifiedByRegion}
		src, err := vgen.Source()
		if err != nil {
			c.Fatal(err)
		}
		os.MkdirAll(dir, 0777)
		path := filepath.Join(dir, "verified.go")
		if err := ioutil.WriteFile(path, src, 0644); err != nil {
			c.Fatal(err)
		}
	}
}

type probeResult struct {
	ec2Type  string
	duration time.Duration
	err      error
}

// probe is a helper function to probe many instance types concurrently.
func probe(ctx context.Context, cluster *ec2cluster.Cluster, instanceTypes []string, limit int) []probeResult {
	results := make([]probeResult, len(instanceTypes))
	_ = traverse.Limit(limit).Each(len(instanceTypes), func(i int) error {
		d, err := cluster.Probe(ctx, instanceTypes[i])
		results[i] = probeResult{instanceTypes[i], d, err}
		return nil
	})
	return results
}

// instancesToVerify returns a list each of verified and toverify instance types, given
// a list of instance types and an existing mapping of instance types to verification status.
// If retry is set, already attempted (but unverified) instance types are also included in toverify
func instancesToVerify(instanceTypes []string, existing map[string]instances.VerifiedStatus, retry bool) (verified, toverify []string) {
	set := make(map[string]bool)
	for _, it := range instanceTypes {
		set[it] = true
	}
	for it := range existing {
		set[it] = true
	}
	for it := range set {
		vs := existing[it]
		if !vs.Verified && (!vs.Attempted || retry) {
			toverify = append(toverify, it)
		}
		if vs.Verified {
			verified = append(verified, it)
		}
	}
	sort.Strings(verified)
	sort.Strings(toverify)
	return
}
