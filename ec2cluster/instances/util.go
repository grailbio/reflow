// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package instances

import (
	"bytes"
	"go/format"
	"log"
	"text/template"
)

var verifiedTmpl = template.Must(template.New("verified").Parse(`
// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT DIRECTLY UNLESS
// when making changes to the format of the file, in which case, one would
// need to edit it first, and then update the template accordingly.

package {{.Package}}

// VerifiedStatus captures the verification status for each Instance type.
type VerifiedStatus struct {
	// Attempted denotes whether a verification attempt has been made.
	Attempted bool
	// Verified denotes whether the instance type is verified to work for Reflow.
	Verified bool
	// ApproxETASeconds is the approximate ETA (in seconds) for Reflow to become available on this instance type.
	ApproxETASeconds int64
	// MemoryBytes is memory bytes reported as available on this Instance type.
	MemoryBytes int64
}

// ExpectedMemoryBytes is the amount of memory we can expect to be available based on verification.
func (v VerifiedStatus) ExpectedMemoryBytes() int64 {
	// samplingErrorDiscount is used to discount the amount of memory to account for sampling variation.
	// Since we are modeling the expected available memory on an instance type based on
	// one sample (collected during verification), this provides a buffer.
	const samplingErrorDiscount = 0.02 // 2 percent

	return int64(float64(v.MemoryBytes) * (1 - samplingErrorDiscount))
}

// VerifiedByRegion stores mapping of instance types to VerifiedStatus by AWS Region.
var VerifiedByRegion = make(map[string]map[string]VerifiedStatus)

func init() {
    {{range $key, $value := .VerifiedByRegion}} VerifiedByRegion["{{$key}}"] = map[string]VerifiedStatus{
	    {{range $k, $v := $value}} {{printf "%q" $k}}:    { {{printf "%t" $v.Attempted}}, {{printf "%t" $v.Verified}}, {{printf "%d" $v.ApproxETASeconds}}, {{printf "%d" $v.MemoryBytes}} },
        {{end}}
    }
	{{end}}
}
`))

// VerifiedSrcGenerator generates Go source code of the form found in verified.go (in this package)
// given a target Go package and a mapping of instance types to VerifiedStatus by AWS Region.
type VerifiedSrcGenerator struct {
	// Package is the target Go package for to generate the Go source code for.
	Package string
	// VerifiedByRegion maps AWS region to a mapping of instance types to VerifiedStatus.
	VerifiedByRegion map[string]map[string]VerifiedStatus
}

// Source returns the Go source code representing the contents of this generator.
func (v *VerifiedSrcGenerator) Source() ([]byte, error) {
	var buf bytes.Buffer
	if err := verifiedTmpl.Execute(&buf, *v); err != nil {
		return nil, err
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		log.Println(buf.String())
		log.Fatalf("generated code is invalid: %s", err)
		return nil, err
	}
	return src, nil
}

// AddTypes adds the given instance types and returns the same generator (for convenience)
func (v *VerifiedSrcGenerator) AddTypes(types []string) *VerifiedSrcGenerator {
	for region := range v.VerifiedByRegion {
		for _, k := range types {
			if _, ok := v.VerifiedByRegion[region][k]; !ok {
				v.VerifiedByRegion[region][k] = VerifiedStatus{false, false, -1, 0}
			}
		}
	}
	return v
}
