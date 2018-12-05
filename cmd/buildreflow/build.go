// Copyright 2017 GRAIL, Inc. All rights reserved.
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

	"golang.org/x/sync/errgroup"
)

var (
	vflag  = flag.Bool("v", false, "print subcommand output")
	reflow = flag.String("reflow", "github.com/grailbio/reflow/cmd/reflow", "reflow go get path")
	output = flag.String("o", "reflow", "output binary path")
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: buildreflow

Buildreflow builds a new cross-platform reflow binary.
Specifically, it builds a Mac (darwin) binary and embeds a linux version.
`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	lxPath, dwPath := *output, *output+"darwin"
	var g errgroup.Group
	for _, config := range []struct{ goos, path string }{{"linux", lxPath}, {"darwin", dwPath}} {
		goos, path := config.goos, config.path
		g.Go(func() error {
			cmd := command("go", "build", "-o", path, *reflow)
			cmd.Env = os.Environ()
			cmd.Env = append(cmd.Env, "GOOS="+goos)
			cmd.Env = append(cmd.Env, "GOARCH=amd64")
			log.Printf("building reflow %s", goos)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("build reflow %s: %v", goos, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
	log.Print("embedding linux reflow into darwin reflow")
	dst, err := os.OpenFile(dwPath, os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		log.Fatalf("reflow embedding: %v", err)
	}
	defer func() {
		if err := dst.Close(); err != nil {
			log.Printf("close %s: %v", dwPath, err)
		}
	}()
	src, err := os.Open(lxPath)
	if err != nil {
		log.Fatalf("reflow embedding: %v", err)
	}
	defer func() {
		if err := src.Close(); err != nil {
			log.Printf("close %s: %v", lxPath, err)
		}
	}()
	if _, err := io.Copy(dst, src); err != nil {
		log.Fatalf("reflow embedding: %v", err)
	}
}

func command(name string, arg ...string) *exec.Cmd {
	cmd := exec.Command(name, arg...)
	if *vflag {
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
	}
	return cmd
}
