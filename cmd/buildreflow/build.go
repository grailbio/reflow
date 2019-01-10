// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// TODO(marius): allow users to pass other build flags, too

func usage() {
	fmt.Fprintln(os.Stderr, `usage: buildreflow [-o output] [-version version] [-v]

Buildreflow builds a reflow binary that's usable for distributed
execution.`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var (
		output      = flag.String("o", "reflow", "reflow binary output path")
		packagePath = flag.String("p", "github.com/grailbio/reflow/cmd/reflow", "reflow main package path")
		v           = flag.Bool("v", false, "verbose output")
		version     = flag.String("version", "", "version with which to stamp the binary; computed from git if empty")
	)
	log.SetFlags(0)
	log.SetPrefix("")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 0 {
		flag.Usage()
	}
	// TODO(marius): embed Git version, or some other meaningful version stamp?

	if *version == "" {
		*version = packageVersion(*packagePath)
	}

	var (
		goos   = goenv("", "GOOS")
		goarch = goenv("", "GOARCH")
	)

	// Whatever version is hardcoded is probably wrong.
	ldflags := "-X main.version=" + *version
	cmd := exec.Command("go", "build", "-ldflags", ldflags, "-o", *output, *packagePath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if *v {
		log.Println(*packagePath, *version, goos, goarch)
	}
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s %s: %v", cmd.Path, strings.Join(cmd.Args, " "), err)
	}

	// We use our own env lookup instead of runtime.Goos since
	// the user might override the build environment, which is also
	// propagated to the underlying command invocations.
	if goos == "linux" && goarch == "amd64" {
		return
	}

	// If we're not on Linux, then build and attach a Linux binary to
	// create a "fat" binary. This Linux binary is used by reflow to
	// upload to remote reflowlets.
	linuxPath := *output + ".linux"
	cmd = exec.Command("go", "build", "-ldflags", ldflags, "-o", linuxPath, *packagePath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
	log.Println(*packagePath, *version, "linux", "amd64")
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s %s: %v", cmd.Path, strings.Join(cmd.Args, " "), err)
	}
	defer os.Remove(linuxPath)
	fatalf := func(format string, v ...interface{}) {
		os.Remove(*output)
		os.Remove(linuxPath)
		log.Fatalf(format, v...)
	}

	dst, err := os.OpenFile(*output, os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		fatalf("open %s: %v", *output, err)
	}
	src, err := os.Open(linuxPath)
	if err != nil {
		fatalf("open %s: %v", linuxPath, err)
	}
	defer src.Close()
	if _, err := io.Copy(dst, src); err != nil {
		fatalf("embed %s %s: %v", *output, linuxPath, err)
	}
	if err := dst.Close(); err != nil {
		fatalf("embed %s %s: %v", *output, linuxPath, err)
	}
}

func packageVersion(packagePath string) string {
	pkgs, err := packages.Load(nil, packagePath)
	if err != nil {
		log.Fatalf("failed to load package %s: %v", packagePath)
	}
	if len(pkgs) == 0 {
		log.Fatalf("package %s not found", packagePath)
	}
	if len(pkgs) != 1 {
		panic("non-unique package")
	}
	pkg := pkgs[0]
	if len(pkg.CompiledGoFiles) == 0 {
		log.Fatalf("package %s is empty", packagePath)
	}
	pkgDir := filepath.Dir(pkg.CompiledGoFiles[0])
	gomod := goenv(pkgDir, "GOMOD")
	if gomod == "" {
		log.Fatalf("package %s does not belong to a Go module", packagePath)
	}
	root := filepath.Dir(gomod)

	// Try to find the version of reflow.
	gitSHA := git(root, "rev-parse", "HEAD")
	if gitSHA == "" {
		log.Fatalf("invalid git sha")
	}
	version := gitSHA[:12]
	if git(root, "status", "--short") != "" {
		version += "-dirty"
	}
	return version
}

func goenv(dir, which string) string {
	var out strings.Builder
	cmd := exec.Command("go", "env", which)
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		log.Fatalf("failed to get Go environment %s: %v", which, err)
	}
	return strings.TrimSpace(out.String())
}

func git(dir string, args ...string) string {
	var out strings.Builder
	cmd := exec.Command("git", args...)
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		log.Fatalf("git %s: %v", strings.Join(args, " "))
	}
	return strings.TrimSpace(out.String())
}
