// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/grailbio/reflow/ec2cluster/instances"
)

var (
	url      = flag.String("url", "http://www.ec2instances.info/instances.json", "the URL from which to fetch instances.json")
	stdout   = flag.Bool("stdout", false, "print the package to stdout instead of materializing it")
	verified = flag.Bool("verified", false, "whether to generate verified.go")
)

// nitroBasedInstanceTypes contain a list of instance type classes that are nitro-based
// (and hence expose the EBS volumes as NVMe) as per:
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html#ec2-nitro-instances
// Note: www.ec2instances.info doesn't capture this correctly.
var nitroInstanceTypePrefixes = []string{"A1", "C5", "C5a", "C5d", "C5n", "G4", "I3en", "Inf1", "M5", "M5a", "M5ad", "M5d", "M5dn", "M5n", "M6g", "p3dn.24xlarge", "R5", "R5a", "R5ad", "R5d", "R5dn", "R5n", "T3", "T3a", "z1d"}

var avx512InstanceTypePrefixes = []string{"m5", "c5", "r5"}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: ec2instances dir

ec2instances generates a Go package with EC2 instance metadata
by pulling data from http://ec2instances.info/.
It includes only x86_64 instances with Linux HVM support.
`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
	}
	dir := flag.Arg(0)

	for i, t := range nitroInstanceTypePrefixes {
		nitroInstanceTypePrefixes[i] = strings.ToLower(t)
	}
	var body io.Reader
	if strings.HasPrefix(*url, "file://") {
		path := strings.TrimPrefix(*url, "file://")
		f, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		body = f
	} else {
		resp, err := http.Get(*url)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		body = resp.Body
	}
	dec := json.NewDecoder(body)
	var entries []entry
	if err := dec.Decode(&entries); err != nil {
		log.Fatal(err)
	}
	var g generator
	g.Printf("// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.\n")
	g.Printf("\n")
	g.Printf("package %s\n", filepath.Base(dir))
	g.Printf("\n")
	g.Printf("// Type describes an EC2 instance type.\n")
	g.Printf("type Type struct {\n")
	g.Printf("	// Name is the API name of this EC2 instance type.\n")
	g.Printf("	Name string\n")
	g.Printf("	// EBSOptimized is set to true if the instance type permits EBS optimization.\n")
	g.Printf("	EBSOptimized bool\n")
	g.Printf("	// EBSThroughput is the max throughput for the EBS optimized instance.\n")
	g.Printf("	EBSThroughput float64\n")
	g.Printf("	// VCPU stores the number of VCPUs provided by this instance type.\n")
	g.Printf("	VCPU uint\n")
	g.Printf("	// Memory stores the number of (fractional) GiB of memory provided by this instance type.\n")
	g.Printf("	Memory float64\n")
	g.Printf("	// Price stores the on-demand price per region for this instance type.\n")
	g.Printf("	Price map[string]float64\n")
	g.Printf("	// Generation stores the generation name for this instance (\"current\" or \"previous\").\n")
	g.Printf("	Generation string\n")
	g.Printf("	// Virt stores the virtualization type used by this instance type.\n")
	g.Printf("	Virt string\n")
	g.Printf("	// NVMe specifies whether EBS block devices are exposed as NVMe volumes.\n")
	g.Printf("	NVMe bool\n")
	g.Printf("	// CPUFeatures defines the available CPU features on this instance type\n")
	g.Printf("	CPUFeatures map[string]bool\n")
	g.Printf("}\n")

	g.Printf("// Types stores known EC2 instance types.\n")
	g.Printf("var Types = []Type{\n")

	var acceptedTypes []string
	for _, e := range entries {
		var ok bool
		for _, arch := range e.Arch {
			if arch == "x86_64" {
				ok = true
				break
			}
		}
		if !ok {
			log.Printf("excluding instance type %s because it does not support arch x86_64", e.Type)
			continue
		}
		if strings.HasSuffix(e.Type, ".metal") {
			log.Printf("excluding bare-metal instance type %s", e.Type)
			continue
		}
		if strings.Contains(e.Network, "Low") {
			log.Printf("excluding instance type %s because its network performance can be Low", e.Type)
			continue
		}
		if strings.HasPrefix(e.Type, "a1.") {
			log.Printf("excluding instance type %s because it uses ARM (would need a different AMI)", e.Type)
			continue
		}
		parts := strings.Split(e.Type, ".")
		if strings.HasSuffix(parts[0], "gd") || strings.HasSuffix(parts[0], "g") {
			log.Printf("excluding instance type %s because it uses AWS Graviton (would need a different AMI)", e.Type)
			continue
		}
		ok = false
		// TODO(marius): should we prefer a particular virtualization type?
		var virt string
		// ec2instances doesn't seem to correctly classify the virtualization type of c5,
		// so we override this for now.
		if strings.HasPrefix(e.Type, "c5.") {
			virt = "HVM"
			ok = true
		} else if len(e.LinuxVirtType) == 0 {
			virt = "HVM"
			ok = true
		} else {
			for _, virt = range e.LinuxVirtType {
				if virt == "HVM" {
					ok = true
					break
				}
			}
		}
		if !ok {
			log.Printf("excluding instance type %s because it does not support Linux HVM (supported: %s)", e.Type, strings.Join(e.LinuxVirtType, ", "))
			continue
		}
		acceptedTypes = append(acceptedTypes, e.Type)
		// All current generation instances are EBS optimized by default as per:
		// https://aws.amazon.com/ec2/pricing/on-demand/
		// "For Current Generation Instance types, EBS-optimization is enabled by default at no additional cost."
		// However, http://ec2instances.info/ seems to have EBSOptimized set to false for all instances.
		ebsOptimized := e.EBSOptimized || e.Generation == "current"
		g.Printf("{\n")
		g.Printf("	Name: %q,\n", e.Type)
		g.Printf("	EBSOptimized: %v,\n", ebsOptimized)
		g.Printf("	EBSThroughput: %f,\n", e.EBSThroughput)
		g.Printf("	VCPU: %v,\n", e.VCPU)
		g.Printf("	Memory: %f,\n", e.Memory)
		g.Printf("	Price: map[string]float64{\n")
		var regions []string
		for region := range e.Pricing {
			regions = append(regions, region)
		}
		sort.Strings(regions)
		for _, region := range regions {
			linux := e.Pricing[region]["linux"]
			if linux == nil {
				continue
			}
			price, ok := linux.(map[string]interface{})["ondemand"].(string)
			if !ok {
				continue
			}

			g.Printf("		%q: %s,\n", region, price)
		}

		g.Printf("	},\n")
		g.Printf("	Generation: %q,\n", e.Generation)
		g.Printf("	Virt: %q,\n", virt)
		nvme := false
		for _, prefix := range nitroInstanceTypePrefixes {
			if strings.HasPrefix(e.Type, prefix) {
				nvme = true
				break
			}
		}
		g.Printf("	NVMe: %v,\n", nvme)
		g.Printf("	CPUFeatures: map[string]bool{\n")
		if e.IntelAVX {
			// TODO: This seems wrong (false negative) for many instances.
			g.Printf("		%q: true,\n", "intel_avx")
		}
		if e.IntelAVX2 {
			g.Printf("		%q: true,\n", "intel_avx2")
		}
		// AVX512 isn't yet exported by the data provided by AWS/ec2instances.info.
		for _, prefix := range avx512InstanceTypePrefixes {
			if strings.HasPrefix(e.Type, prefix) {
				g.Printf("		%q: true,\n", "intel_avx512")
				break
			}
		}
		g.Printf("	},\n")
		g.Printf("},\n")
	}
	g.Printf("}\n")
	src := g.Gofmt()

	if *stdout {
		os.Stdout.Write(src)
	} else {
		os.MkdirAll(dir, 0777)
		path := filepath.Join(dir, "instances.go")
		if err := ioutil.WriteFile(path, src, 0644); err != nil {
			log.Fatal(err)
		}
		if *verified {
			vgen := instances.VerifiedSrcGenerator{filepath.Base(dir), instances.VerifiedByRegion}
			vsrc, err := vgen.AddTypes(acceptedTypes).Source()
			if err != nil {
				log.Fatal(err)
			}
			vpath := filepath.Join(dir, "verified.go")
			if err := ioutil.WriteFile(vpath, vsrc, 0644); err != nil {
				log.Fatal(err)
			}
		}
	}
}

type entry struct {
	Arch          []string `json:"arch"`
	Type          string   `json:"instance_type"`
	EBSOptimized  bool     `json:"ebs_optimized"`
	EBSThroughput float64  `json:"ebs_throughput"`
	Memory        float64  `json:"memory"`
	// VCPU must be an abstract, because "N/A" is returned
	// for the "i3.metal" instance type.
	VCPU          interface{}                       `json:"vCPU"`
	Pricing       map[string]map[string]interface{} `json:"pricing"`
	Network       string                            `json:"network_performance"`
	Generation    string                            `json:"generation"`
	LinuxVirtType []string                          `json:"linux_virtualization_types"`
	IntelAVX      bool                              `json:"intel_avx"`
	IntelAVX2     bool                              `json:"intel_avx2"`
}

type generator struct {
	buf bytes.Buffer
}

func (g *generator) Printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

func (g *generator) Gofmt() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		log.Println(g.buf.String())
		log.Fatalf("generated code is invalid: %s", err)
	}
	return src
}
