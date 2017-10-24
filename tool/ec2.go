// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/grailbio/reflow/ec2cluster/instances"
)

func (c *Cmd) ec2instances(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("ec2instances", flag.ExitOnError)
	help := `Ec2instances lists EC2 instance types known by Reflow.

The columns displayed by the instance listing are:

	type    the name of the instance type
	mem     the amount of instance memory (GiB)
	cpu     the number of instance VCPUs
	price   the hourly on-demand price of the instance in the selected region
	flags   a set of flags:
	            ebs    when the instance supports EBS optimization
	            old    when the instance is not of the current generation`
	regionFlag := flags.String("region", "us-west-2", "region for which to show prices")
	sortFlag := flags.String("sort", "type", "sorting field (type, cpu, mem, price)")
	c.Parse(flags, args, help, "ec2instances")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	types := make([]instances.Type, len(instances.Types))
	copy(types, instances.Types)
	sort.Slice(types, func(i, j int) bool {
		switch *sortFlag {
		case "type":
			return types[i].Name < types[j].Name
		case "cpu":
			return types[i].VCPU < types[j].VCPU
		case "mem":
			return types[i].Memory < types[j].Memory
		case "price":
			return types[i].Price[*regionFlag] < types[j].Price[*regionFlag]
		default:
			flags.Usage()
			panic("notreached")
		}
	})
	var tw tabwriter.Writer
	tw.Init(os.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	for _, typ := range types {
		var flags []string
		if typ.EBSOptimized {
			flags = append(flags, "ebs")
		}
		if typ.Generation != "current" {
			flags = append(flags, "old")
		}
		fmt.Fprintf(&tw, "%s\t\t%.2f\t%d\t%.2f\t{%s}\n",
			typ.Name, typ.Memory,
			typ.VCPU, typ.Price[*regionFlag],
			strings.Join(flags, ","))
	}
}
