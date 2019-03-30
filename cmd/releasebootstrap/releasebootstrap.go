// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

var (
	vflag  = flag.Bool("v", false, "print subcommand output")
	reflow = flag.String("reflow", "github.com/grailbio/reflow/cmd/reflow", "reflow go get path")
	repo   = flag.String("repo", "grailbio/reflowlet", "docker repository")
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: releasebootstrap [-repo repo] [-reflow package] [-v]

Releasebootstrap builds a new reflowlet bootstrap binary and pushes it to the reflowlet Docker repository.
`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	dir, err := ioutil.TempDir("", "reflow")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "reflow")
	ldflags := "-X main.version=bootstrap"
	cmd := command("go", "build", "-ldflags", ldflags, "-o", path, *reflow)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GOOS=linux")
	cmd.Env = append(cmd.Env, "GOARCH=amd64")
	log.Println("building reflowlet (bootstrap binary)")
	if err := cmd.Run(); err != nil {
		log.Fatalf("build: %v", err)
	}

	const dockerfile = `FROM frolvlad/alpine-glibc
COPY reflow /reflow
ENTRYPOINT ["/reflow"]
`
	dockerfilePath := filepath.Join(dir, "Dockerfile")
	if err := ioutil.WriteFile(dockerfilePath, []byte(dockerfile), 0666); err != nil {
		log.Fatal(err)
	}
	// For some reason, Docker fails to find the binary if we
	// give it an absolute path. Instead, change into the temporary
	// directory and build from there. (This happens on a Mac.)
	if err := os.Chdir(dir); err != nil {
		log.Fatal(err)
	}

	image := fmt.Sprintf("%s:bootstrap", *repo)
	cmd = command("docker", "build", "-t", image, dir)
	log.Printf("building reflowlet docker image")
	if err := cmd.Run(); err != nil {
		log.Fatalf("build image: %v", err)
	}
	cmd = command("docker", "push", image)
	log.Printf("pushing image to %s", image)
	if err := cmd.Run(); err != nil {
		log.Fatalf("push image: %v", err)
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
