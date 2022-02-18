// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"flag"
	"fmt"
	"os"
	"runtime"
)

// Parse parses the provided FlagSet from the provided arguments. It
// adds a -help flag to the flagset, and prints the help and usage
// string when the command is called with -help. On usage error, it
// prints the flag defaults and exits with code 2.
func (c *Cmd) Parse(fs *flag.FlagSet, args []string, help, usage string) {
	helpFlag := fs.Bool("help", false, "display subcommand help")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: reflow "+usage)
		fmt.Fprintln(os.Stderr, "Flags:")
		fs.PrintDefaults()
		c.Exit(2)
	}

	if err := fs.Parse(args); err != nil {
		c.Fatal(err)
	}
	if *helpFlag {
		fmt.Fprintln(os.Stderr, "usage: reflow "+usage)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, help)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Flags:")
		fs.PrintDefaults()
		c.Exit(0)
	}
}

func (c Cmd) must(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		c.Fatalf("%s:%d: %v", file, line, err)
	}
}
