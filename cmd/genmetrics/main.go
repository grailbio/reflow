// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

func convertToPascalCase(id string) string {
	result := strings.ToUpper(string(id[0]))
	makeNextUpper := false
	for i := range id[1:] {
		substr := id[i+1 : i+2]
		if substr == "_" {
			makeNextUpper = true
		} else {
			if makeNextUpper {
				substr = strings.ToUpper(substr)
				makeNextUpper = false
			}
			result += substr
		}
	}
	return result
}

func convertHelpToCommentFmt(help string) string {
	help = strings.TrimSpace(help)
	return strings.ToLower(help[0:1]) + help[1:len(help)-1]
}

var idRe = regexp.MustCompile(`[a-z][a-z_]*[a-z]`)
var helpRe = regexp.MustCompile(`[A-Z][a-zA-Z0-9 ]*\.`)

type metricDef struct {
	Name    string
	Type    string
	Help    string
	Labels  []string
	Buckets []float64
}

// validate returns if the metric definition is well formed, otherwise exits with a fatal error.
func (m metricDef) validate() {
	if !idRe.MatchString(m.Name) {
		log.Fatalf("invalid metric name %s, must match id regex: %s", m.Name, idRe.String())
	}

	switch m.Type {
	case "counter":
	case "gauge":
	case "histogram":
	default:
		log.Fatalf("unknown metric type %s on %s, must be one of {counter,gauge,histogram}", m.Type, m.Name)
	}

	if !helpRe.MatchString(m.Help) {
		log.Fatalf("invalid help text for metric %s, %s", m.Name, m.Help)
	}

	for _, l := range m.Labels {
		if !idRe.MatchString(l) {
			log.Fatalf("label %s on metric %s is incorrectly formatted", m.Name, l)
		}
	}

	if m.Buckets != nil && m.Type != "histogram" {
		log.Fatalf("metric %s has type %s and buckets %v, but buckets are only allowed for histogram type",
			m.Name, m.Type, m.Buckets)
	}
}

// printVarDefToGen prints a {metricType}Opt definition to the generator. These definitions
// are used by clients to initialize metric backing stores.
func (m metricDef) printVarDefToGen(gen *generator) {
	gen.Printf("		\"%s\": {\n", m.Name)

	if m.Help != "" {
		gen.Printf("			Help: \"%s\",\n", m.Help)
	}

	if len(m.Labels) != 0 {
		ls := make([]string, len(m.Labels))
		for i, l := range m.Labels {
			ls[i] = fmt.Sprintf("\"%s\"", l)
		}
		gen.Printf("			Labels: []string{%s},\n", strings.Join(ls, ","))
	}

	if len(m.Buckets) != 0 {
		bs := make([]string, len(m.Buckets))
		for i, b := range m.Buckets {
			bs[i] = strconv.FormatFloat(b, 'f', -1, 64)
		}
		gen.Printf("			Buckets: []float64{%s},\n", strings.Join(bs, ","))
	}

	gen.Printf("		},\n")

}

// printGetterToGen prints a function definition for metricDef to the generator. This function
// definition is used by clients and guarantees correctness of metric and label names.
func (m metricDef) printGetterToGen(gen *generator) {
	fnName := fmt.Sprintf("Get%s%s", convertToPascalCase(m.Name), convertToPascalCase(m.Type))
	gen.Printf("// %s returns a %s to set metric %s", fnName, convertToPascalCase(m.Type), m.Name)
	if m.Help != "" {
		gen.Printf(" (%s)", convertHelpToCommentFmt(m.Help))
	}
	gen.Printf(".\n")

	gen.Printf("func %s(ctx context.Context", fnName)
	if len(m.Labels) != 0 {
		for _, l := range m.Labels {
			gen.Printf(", %s", l)
		}
		gen.Printf(" string")
	}
	gen.Printf(") %s {\n", convertToPascalCase(m.Type))

	labelDict := "nil"
	if len(m.Labels) != 0 {
		assns := make([]string, len(m.Labels))
		for i, l := range m.Labels {
			assns[i] = fmt.Sprintf("\"%s\":%s", l, l)
		}
		labelDict = fmt.Sprintf("map[string]string{%s}", strings.Join(assns, ","))
	}
	gen.Printf("	return get%s(ctx, \"%s\", %s)\n", convertToPascalCase(m.Type), m.Name, labelDict)
	gen.Printf("}\n\n")
}

var (
	stdout = flag.Bool("stdout", false, "print the package to stdout instead of materializing it")
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: genmetrics defpath dstpackage

genmetrics reads a metrics definition file at defpath and generates a Go file
at dstpath that provides typing for metrics used by reflow.
`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
	}

	defpath, dstpackage := flag.Arg(0), flag.Arg(1)
	f, err := os.Open(defpath)
	if err != nil {
		log.Fatal(err)
	}

	var defs []metricDef
	d := json.NewDecoder(f)
	if err := d.Decode(&defs); err != nil {
		log.Fatal(err)
	}

	var counters, gauges, histograms []metricDef
	for _, def := range defs {
		def.validate()
		switch def.Type {
		case "counter":
			counters = append(counters, def)
		case "gauge":
			gauges = append(gauges, def)
		case "histogram":
			histograms = append(histograms, def)
		}
	}

	gen := &generator{}
	gen.Printf("// Copyright 2021 GRAIL, Inc. All rights reserved.\n")
	gen.Printf("// Use of this source code is governed by the Apache 2.0\n")
	gen.Printf("// license that can be found in the LICENSE file.\n")
	gen.Printf("\n")
	gen.Printf("// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.\n")
	gen.Printf("\n")
	gen.Printf("package %s\n", filepath.Base(dstpackage))
	gen.Printf("\n")
	gen.Printf("import (\n")
	gen.Printf("	\"context\"")
	gen.Printf(")\n")

	// define global variables used by clients during initialization.
	gen.Printf("var (\n")

	// counters
	gen.Printf("	Counters = map[string]counterOpts{\n")
	for _, c := range counters {
		c.printVarDefToGen(gen)
	}
	gen.Printf("	}\n")

	// gauges
	gen.Printf("	Gauges = map[string]gaugeOpts{\n")
	for _, g := range gauges {
		g.printVarDefToGen(gen)
	}
	gen.Printf("	}\n")

	// histograms
	gen.Printf("	Histograms = map[string]histogramOpts{\n")
	for _, h := range histograms {
		h.printVarDefToGen(gen)
	}
	gen.Printf("	}\n")

	gen.Printf(")\n")

	// define getters to access specific metrics from client embedded in context.
	// counters
	for _, c := range counters {
		c.printGetterToGen(gen)
	}
	// gauges
	for _, g := range gauges {
		g.printGetterToGen(gen)
	}
	// histograms
	for _, h := range histograms {
		h.printGetterToGen(gen)
	}

	src := gen.Gofmt()

	if *stdout {
		os.Stdout.Write(src)
	} else {
		path := filepath.Join(dstpackage, "metrics.go")
		if err := ioutil.WriteFile(path, src, 0644); err != nil {
			log.Fatal(err)
		}
	}
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
