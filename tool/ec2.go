// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/grailbio/base/cloud/spotadvisor"
	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/ec2cluster/instances"
)

var sa *spotadvisor.SpotAdvisor

func (c *Cmd) ec2instances(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("ec2instances", flag.ExitOnError)
	help := `Ec2instances lists EC2 instance types known by Reflow.

The columns displayed by the instance listing are:

	type    the name of the instance type
	mem     the amount of instance memory (GiB)
	cpu     the number of instance VCPUs
    ebs_max	the max EBS throughput available for an EBS instance	
	price   the hourly on-demand price of the instance in the selected region
	cpu features
		a set of CPU features supported by this instance type
	flags   a set of flags:
	            ebs    when the instance supports EBS optimization
	            old    when the instance is not of the current generation
	 interrupt prob
		the probability that a spot instance of this type will be interrupted`
	regionFlag := flags.String("region", "us-west-2", "region for which to show prices")
	sortFlag := flags.String("sort", "type", "sorting field (type, cpu, mem, price)")
	minCpuFlag := flags.Int("mincpu", 0, "mininum CPU (will filter out smaller instance types)")
	minMemFlag := flags.Int("minmem", 0, "mininum Memory GiB (will filter out smaller instance types)")
	c.Parse(flags, args, help, "ec2instances")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	var types []instances.Type
	for _, t := range instances.Types {
		if int(t.VCPU) < *minCpuFlag || int(t.Memory) < *minMemFlag {
			continue
		}
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool {
		switch *sortFlag {
		case "type":
			return types[i].Name < types[j].Name
		case "cpu":
			return types[i].VCPU < types[j].VCPU
		case "mem":
			return types[i].Memory < types[j].Memory
		case "ebs_max":
			return types[i].EBSThroughput < types[j].EBSThroughput
		case "price":
			return types[i].Price[*regionFlag] < types[j].Price[*regionFlag]
		default:
			flags.Usage()
			panic("notreached")
		}
	})

	// TODO(swami): Get region from config
	// Unfortunately, this is not trivial.
	region := "us-west-2"

	// best effort to include spot advisor data; if error, "N/A" will be shown in printed output
	sa, _ = spotadvisor.NewSpotAdvisor(c.Log, ctx.Done())
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	fmt.Fprint(&tw, "type\t\tusable mem\tinstance mem\tcpu\tebs_max\tprice\tinterrupt prob\tcpu features\tflags\n")
	for _, typ := range types {
		var flags []string
		if typ.EBSOptimized {
			flags = append(flags, "ebs")
		}
		if typ.Generation != "current" {
			flags = append(flags, "old")
		}
		var features []string
		for feature, ok := range typ.CPUFeatures {
			if !ok {
				continue
			}
			features = append(features, feature)
		}
		sort.Strings(features)

		verifiedStatus := instances.VerifiedByRegion[region][typ.Name]
		if !verifiedStatus.Verified {
			continue
		}
		usableMem := data.Size(verifiedStatus.ExpectedMemoryBytes())
		fmt.Fprintf(&tw, "%s\t\t%s\t%s\t%d\t%.2f\t%.2f\t%s\t{%s}\t{%s}\n",
			typ.Name, usableMem, data.Size(typ.Memory) * data.GiB,
			typ.VCPU, typ.EBSThroughput, typ.Price[*regionFlag],
			getSpotInterruptRange(*regionFlag, typ.Name),
			strings.Join(features, ","),
			strings.Join(flags, ","))
	}
}

func getSpotInterruptRange(region string, name string) string {
	if sa != nil {
		if ir, err := sa.GetInterruptRange(spotadvisor.Linux, spotadvisor.AwsRegion(region), spotadvisor.InstanceType(name)); err == nil {
			return ir.String()
		}
	}
	return "N/A"
}
