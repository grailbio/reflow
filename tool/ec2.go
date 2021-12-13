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
	"sync"
	"text/tabwriter"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/cloud/spotadvisor"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/ec2cluster"
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
	interrupt prob
		the probability that a spot instance of this type will be interrupted
	spot placement scores
		spot placement scores (by AZ), ie, the likelihood of a spot fulfillment as of now.
		Low scores indicate less likelihood (and scores are based on a target capacity of 10).
	cpu features
		a set of CPU features supported by this instance type
	flags   a set of flags:
	            ebs    when the instance supports EBS optimization
	            old    when the instance is not of the current generation`
	regionFlag := flags.String("region", "us-west-2", "region for which to show prices")
	sortFlag := flags.String("sort", "type", "sorting field (type, cpu, mem, price)")
	minCpuFlag := flags.Int("mincpu", 0, "mininum CPU (will filter out smaller instance types)")
	maxCpuFlag := flags.Int("maxcpu", 1000, "maximum CPU (will filter out larger instance types)")
	minMemFlag := flags.Int("minmem", 0, "mininum Memory GiB (will filter out smaller instance types)")
	maxMemFlag := flags.Int("maxmem", 10000, "maximum Memory GiB (will filter out smaller instance types)")
	minProbFlag := flags.Int("minprob", int(spotadvisor.Any), fmt.Sprintf("mininum interrupt probability (int in the range [%d, %d])", int(spotadvisor.LessThanFivePct), int(spotadvisor.Any)))
	filterTypesFlag := flags.String("filtertypes", "", "filter instance types to show only the given comma-separated ones (if any)")
	spotScoresFlag := flags.Bool("spotscores", false, "whether to fetch spot placement scores for each instance type")
	c.Parse(flags, args, help, "ec2instances")
	if flags.NArg() != 0 {
		flags.Usage()
	}
	var filterTypes map[string]bool
	if typesCsv := *filterTypesFlag; typesCsv != "" {
		parts := strings.Split(typesCsv, ",")
		filterTypes = make(map[string]bool, len(parts))
		for _, p := range parts {
			filterTypes[strings.TrimSpace(p)] = true
		}
	}
	// best effort to include spot advisor data; if error, "N/A" will be shown in printed output
	sa, _ = spotadvisor.NewSpotAdvisor(c.Log, ctx.Done())
	var types []instances.Type
	for _, t := range instances.Types {
		if int(t.VCPU) < *minCpuFlag || int(t.VCPU) > *maxCpuFlag {
			continue
		}
		if int(t.Memory) < *minMemFlag || int(t.Memory) > *maxMemFlag {
			continue
		}
		if len(filterTypes) > 0 && !filterTypes[t.Name] {
			continue
		}
		if sa != nil {
			tProb, err := sa.GetMaxInterruptProbability(spotadvisor.Linux, spotadvisor.AwsRegion(*regionFlag), spotadvisor.InstanceType(t.Name))
			if err == nil && int(tProb) > *minProbFlag {
				continue
			}
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

	spotScores, _ := c.spotScores(ctx, *spotScoresFlag, *regionFlag, types)
	var tw tabwriter.Writer
	tw.Init(c.Stdout, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	_, _ = fmt.Fprint(&tw, "type\t\tusable mem\tinstance mem\tcpu\tebs_max\tprice\tinterrupt prob\tspot placement scores\tcpu features\tflags\n")
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

		var vs instances.VerifiedStatus
		if forRegion := instances.VerifiedByRegion[*regionFlag]; len(forRegion) > 0 {
			if vs = instances.VerifiedByRegion[*regionFlag][typ.Name]; !vs.Verified {
				continue
			}
		}
		usableMem := "UNKNOWN"
		if m := data.Size(vs.ExpectedMemoryBytes()); m > 0 {
			usableMem = m.String()
		}
		_, _ = fmt.Fprintf(&tw, "%s\t\t%s\t%s\t%d\t%.2f\t%.2f\t%s\t%v\t{%s}\t{%s}\n",
			typ.Name, usableMem, data.Size(typ.Memory) * data.GiB,
			typ.VCPU, typ.EBSThroughput, typ.Price[*regionFlag],
			getSpotInterruptRange(*regionFlag, typ.Name),
			spotScores[typ.Name],
			strings.Join(features, ","),
			strings.Join(flags, ","))
	}
}

func (c *Cmd) spotScores(ctx context.Context, fetch bool, region string, instanceTypes []instances.Type) (results map[string]map[string]int, err error) {
	var resultsMu sync.Mutex
	results = make(map[string]map[string]int)
	for _, it := range instanceTypes {
		results[it.Name] = make(map[string]int)
	}
	if !fetch {
		return
	}
	var sess *session.Session
	if err = c.Config.Instance(&sess); err != nil {
		return
	}
	api := ec2.New(sess)
	_ = traverse.Each(len(instanceTypes), func(i int) error {
		instanceType := instanceTypes[i].Name
		if scores, err := ec2cluster.GetSpotPlacementScores(ctx, api, region, instanceType); err != nil {
			c.Log.Debug(err)
		} else {
			resultsMu.Lock()
			results[instanceType] = scores
			resultsMu.Unlock()
		}
		return nil
	})
	return
}

func getSpotInterruptRange(region string, name string) string {
	if sa != nil {
		if ir, err := sa.GetInterruptRange(spotadvisor.Linux, spotadvisor.AwsRegion(region), spotadvisor.InstanceType(name)); err == nil {
			return ir.String()
		}
	}
	return "N/A"
}
