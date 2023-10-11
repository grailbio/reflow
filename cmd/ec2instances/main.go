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
	instancesUrl = flag.String("instancesUrl", "https://instances.vantage.sh/instances.json", "the URL from which to fetch instances.json")
	amisUrl      = flag.String("amisUrl", "https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_ami_all.json", "the URL from which to fetch flatcar_production_ami_all.json")
	amisDir      = flag.String("amisDir", "", "if given, will create/update amis.go in amisDir with latest Flatcar stable AMIs")
	stdout       = flag.Bool("stdout", false, "print the package to stdout instead of materializing it")
	verified     = flag.Bool("verified", false, "whether to generate verified.go")
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: ec2instances dir

ec2instances generates a Go package with EC2 instance metadata
by pulling data from http://ec2instances.info/ and fetches AMIs 
for the latest stable Flatcar release. It includes only x86_64 
instances with Linux HVM support.
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

	generateInstances(dir)
	if len(*amisDir) > 0 {
		generateAmis(*amisDir)
	}
}

// generateInstances produces an instances.go file in the given dir, containing
// instance type information.
func generateInstances(dir string) {
	var body io.Reader
	if strings.HasPrefix(*instancesUrl, "file://") {
		path := strings.TrimPrefix(*instancesUrl, "file://")
		f, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		body = f
	} else {
		resp, err := http.Get(*instancesUrl)
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
	g.Printf("	// StorageDevices stores the number of instance storage devices available for this instance type.\n")
	g.Printf("	StorageDevices int\n")
	g.Printf("	// StorageSize stores the size of each instance storage device (GiB), so total storage is (StorageDevices*StorageSize).\n")
	g.Printf("	StorageSize int\n")
	g.Printf("	// StorageType stores the type of instance storage devices available for this instance type.\n")
	g.Printf("	StorageType StorageType\n")
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

	g.Printf("// StorageType specifies the type of instance storage.\n")
	g.Printf("type StorageType int\n")
	g.Printf("const (\n")
	g.Printf("	StorageTypeNone = iota\n")
	g.Printf("	StorageTypeHDD\n")
	g.Printf("	StorageTypeSSD\n")
	g.Printf("	StorageTypeSSDNVMe\n")
	g.Printf(")\n")

	g.Printf("// Types stores known EC2 instance types.\n")
	g.Printf("var Types = []Type{\n")

	var allowedFamilies = map[string]struct{}{
		"Compute optimized":               {},
		"General purpose":                 {},
		"GPU instance":                    {},
		"Machine Learning ASIC Instances": {},
		"Memory optimized":                {},
		"Storage optimized":               {},
	}
	var acceptedTypes []string
	for _, e := range entries {
		if _, ok := allowedFamilies[e.Family]; !ok {
			log.Printf("excluding instance type %s of family %s", e.Type, e.Family)
			continue
		}
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
		if strings.Contains(e.Type, ".metal") {
			log.Printf("excluding bare-metal instance type %s", e.Type)
			continue
		}
		if strings.Contains(e.Network, "Low") {
			log.Printf("excluding instance type %s because its network performance can be Low", e.Type)
			continue
		}
		if strings.HasPrefix(e.Type, "u-") {
			log.Printf("excluding High Memory instance type %s (purpose-built for large in-memory Databases)", e.Type)
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
		storageType := "StorageTypeNone"
		if e.Storage.Devices > 0 {
			storageType = "StorageTypeHDD"
			switch {
			case e.Storage.SSD && e.Storage.NVMeSSD:
				storageType = "StorageTypeSSDNVMe"
			case e.Storage.SSD:
				storageType = "StorageTypeSSD"
			case e.Storage.NVMeSSD:
				// https://instances.vantage.sh/instances.json returns nvme_ssd=True and ssd=False for d3 instance types
				// because their storage type is NVMe HDD but their scraper incorrectly assumes that all NVMe must be
				// SSD (https://github.com/vantage-sh/ec2instances.info/blob/944a3451715b53a572b38b2ae54973c05a66c0ce/scrape.py#L519)
				// TODO(pfialho): revert to log.Fatal once the above is fixed.
				log.Printf("skipping %s: inconsistent instance storage type: marked NVMe SSD but not SSD", e.Type)
				continue
			default:
				storageType = "StorageTypeHDD"
			}
		}
		g.Printf("{\n")
		g.Printf("	Name: %q,\n", e.Type)
		g.Printf("	EBSOptimized: %v,\n", ebsOptimized)
		g.Printf("	EBSThroughput: %f,\n", e.EBSThroughput)
		g.Printf("	VCPU: %v,\n", e.VCPU)
		g.Printf("	Memory: %f,\n", e.Memory)
		g.Printf("	StorageDevices: %d,\n", e.Storage.Devices)
		g.Printf("	StorageSize: %d,\n", e.Storage.Size)
		g.Printf("	StorageType: %s,\n", storageType)
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
		g.Printf("	NVMe: %v,\n", e.EbsAsNvme)
		g.Printf("	CPUFeatures: map[string]bool{\n")
		if e.IntelAVX {
			// TODO: This seems wrong (false negative) for many instances.
			g.Printf("		%q: true,\n", "intel_avx")
		}
		if e.IntelAVX2 {
			g.Printf("		%q: true,\n", "intel_avx2")
		}
		if e.IntelAVX512 {
			g.Printf("		%q: true,\n", "intel_avx512")
		}
		if e.IntelTurbo {
			g.Printf("		%q: true,\n", "intel_turbo")
		}
		g.Printf("	},\n")
		g.Printf("},\n")
	}
	g.Printf("}\n")
	src := g.Gofmt()

	if *stdout {
		if _, err := os.Stdout.Write(src); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := os.MkdirAll(dir, 0777); err != nil {
			log.Fatal(err)
		}
		path := filepath.Join(dir, "instances.go")
		if err := ioutil.WriteFile(path, src, 0644); err != nil {
			log.Fatal(err)
		}
		if *verified {
			vgen := instances.VerifiedSrcGenerator{Package: filepath.Base(dir), VerifiedByRegion: instances.VerifiedByRegion}
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

// generateAmis produces an amis.go file in the given dir, containing a mapping
// of AWS regions to Flatcar Stable AMIs and a getter method.
func generateAmis(dir string) {
	resp, err := http.Get(*amisUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body := resp.Body
	dec := json.NewDecoder(body)
	var entries amiEntries
	if err := dec.Decode(&entries); err != nil {
		log.Fatal(err)
	}

	vb := strings.Builder{}
	versionUrl := strings.Split(*amisUrl, "flatcar_production_ami_all.json")[0] + "/version.txt"
	vresp, verr := http.Get(versionUrl)
	if verr != nil {
		log.Fatal(verr)
	}
	defer vresp.Body.Close()
	b, rerr := ioutil.ReadAll(vresp.Body)
	if rerr != nil {
		log.Fatal(rerr)
	}
	for _, s := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		vb.WriteString(fmt.Sprintf("// %s\n", s))
	}

	var g generator
	g.Printf(`// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.
	// Generated from URL: %s
	%s

	package %s

	import (
	   "fmt"

	   "github.com/aws/aws-sdk-go/aws/session"
	)

	var flatcarAmiByRegion = map[string]string{
	`, *amisUrl, vb.String(), filepath.Base(dir))

	for _, entry := range entries["amis"] {
		g.Printf("\t\"%s\": \"%s\",\n", entry.Region, entry.Ami)
	}

	g.Println(`}

	// GetAMI gets the AMI ID for the AWS region derived from the given AWS session.
	func GetAMI(sess *session.Session) (string, error) {
		region := *sess.Config.Region
		ami, ok := flatcarAmiByRegion[region]
		if !ok {
		   return "", fmt.Errorf("no AMI defined for region (derived from AWS session): %s", region)
		   }
		return ami, nil
	}`)

	src := g.Gofmt()
	if *stdout {
		if _, err := os.Stdout.Write(src); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := os.MkdirAll(dir, 0777); err != nil {
			log.Fatal(err)
		}
		path := filepath.Join(dir, "amis.go")
		if err := ioutil.WriteFile(path, src, 0644); err != nil {
			log.Fatal(err)
		}
	}
}

type amiEntries map[string][]ami

type ami struct {
	Region string `json:"name"`
	Ami    string `json:"hvm"`
}

type entry struct {
	Arch          []string `json:"arch"`
	Type          string   `json:"instance_type"`
	Family        string   `json:"family"`
	EBSOptimized  bool     `json:"ebs_optimized"`
	EBSThroughput float64  `json:"ebs_throughput"`
	Memory        float64  `json:"memory"`
	// VCPU must be an abstract, because "N/A" is returned
	// for the "i3.metal" instance type.
	VCPU          interface{}                       `json:"vCPU"`
	Pricing       map[string]map[string]interface{} `json:"pricing"`
	Network       string                            `json:"network_performance"`
	Storage       storage                           `json:"storage"`
	Generation    string                            `json:"generation"`
	LinuxVirtType []string                          `json:"linux_virtualization_types"`
	EbsAsNvme     bool                              `json:"ebs_as_nvme"`
	IntelAVX      bool                              `json:"intel_avx"`
	IntelAVX2     bool                              `json:"intel_avx2"`
	IntelAVX512   bool                              `json:"intel_avx512"`
	IntelTurbo    bool                              `json:"intel_turbo"`
}

type storage struct {
	Devices int  `json:"devices"`
	Size    int  `json:"size"`
	NVMeSSD bool `json:"nvme_ssd"`
	SSD     bool `json:"ssd"`
}

type generator struct {
	buf bytes.Buffer
}

func (g *generator) Printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

func (g *generator) Println(s string) {
	fmt.Fprintln(&g.buf, s)
}

func (g *generator) Gofmt() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		log.Println(g.buf.String())
		log.Fatalf("generated code is invalid: %s", err)
	}
	return src
}
