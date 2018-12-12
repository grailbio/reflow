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
	"path/filepath"
	"runtime"

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

// ospath is a simple struct that holds the GOOS and path to binary.
type ospath struct{ goos, path string }

func main() {
	flag.Usage = usage
	flag.Parse()
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("getwd: %v", err)
	}
	tmpPath, binPath := filepath.Join(dir, "reflow.tmp"), filepath.Join(dir, *output)
	configs := []ospath{{runtime.GOOS, tmpPath}}
	var lxTmpPath string
	if runtime.GOOS != "linux" {
		lxTmpPath = filepath.Join(dir, "reflowlinux.tmp")
		configs = append(configs, ospath{"linux", lxTmpPath})
	}
	var g errgroup.Group
	for _, config := range configs {
		goos, path := config.goos, config.path
		g.Go(func() error {
			cmd := command("go", "build", "-o", path, *reflow)
			cmd.Env = os.Environ()
			cmd.Env = append(cmd.Env, "GOOS="+goos)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("build reflow %s: %v", goos, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
	if lxTmpPath != "" {
		defer os.Remove(lxTmpPath)
		dst, err := os.OpenFile(tmpPath, os.O_APPEND|os.O_WRONLY, 0755)
		if err != nil {
			log.Fatal(err)
		}
		src, err := os.Open(lxTmpPath)
		if err != nil {
			log.Fatal(err)
		}
		defer src.Close()
		if _, err = io.Copy(dst, src); err != nil {
			log.Fatal(err)
		}
		if err := dst.Close(); err != nil {
			log.Fatal(err)
		}
	}
	if err := os.Rename(tmpPath, binPath); err != nil {
		log.Fatal(err)
	}
	path, err := filepath.Abs(binPath)
	if err != nil {
		path = binPath
	}
	log.Print(path)
}

func command(name string, arg ...string) *exec.Cmd {
	cmd := exec.Command(name, arg...)
	if *vflag {
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
	}
	return cmd
}
