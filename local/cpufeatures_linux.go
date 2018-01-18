// +build linux

// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

func cpuFeatures() ([]string, error) {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	// Example features line:
	//	flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves ida arat pku ospke
	for s.Scan() && !strings.HasPrefix(s.Text(), "flags") {
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	parts := strings.SplitN(s.Text(), ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("bad cpu flags")
	}
	flags := strings.Split(parts[1], " ")
	var features []string
	for _, flag := range flags {
		switch flag {
		case "avx":
			features = append(features, "intel_avx")
		case "avx2":
			features = append(features, "intel_avx2")
		case "avx512f": // AVX-512 foundation
			features = append(features, "intel_avx512")
		}
	}
	return features, nil
}
