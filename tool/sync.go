// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"golang.org/x/sync/errgroup"
)

func (c *Cmd) sync(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	help := `Sync synchronizes a Reflow fileset with a local directory.

Sync skips downloading files that already exist and have the correct
Sync size and checksum. does not delete files in the target path that
are not also in the fileset.`
	sizeOnly := flags.Bool("sizeonly", false, "validate files based on size alone")
	index := flags.Int("index", 0, "fileset list index")
	c.Parse(flags, args, help, "sync [-sizeonly] [-index n] fileset path")
	if flags.NArg() != 2 {
		flags.Usage()
	}
	fsID, base := flags.Arg(0), flags.Arg(1)
	id, err := reflow.Digester.Parse(fsID)
	if err != nil {
		c.Fatalf("parse %s: %v", fsID, err)
	}
	cache, err := c.Config.Cache()
	if err != nil {
		c.Fatal(err)
	}
	fs, err := cache.Lookup(ctx, id)
	if err != nil {
		c.Fatal(err)
	}
	if fs.N() == 0 {
		c.Fatal("fileset is empty")
	}
	if n := len(fs.List); n > 0 {
		if *index >= n {
			c.Fatalf("index %d out of bounds: list is size %d", *index, n)
		}
		fs = fs.List[*index]
	}
	if _, ok := fs.Map["."]; ok {
		c.Fatal("fileset is singular")
	}
	g, ctx := errgroup.WithContext(ctx)
	for k, f := range fs.Map {
		k, f := k, f
		g.Go(func() error {
			path := filepath.Join(base, k)
			info, err := os.Stat(path)
			switch {
			case err == nil:
				ok := info.Size() == f.Size
				if ok && !*sizeOnly {
					d, err := digestFile(path)
					if err != nil {
						return err
					}
					ok = d == f.ID
				}
				if ok {
					c.Log.Printf("%s: up to date", path)
					return nil
				}
			case os.IsNotExist(err):
			default:
				return err
			}
			os.MkdirAll(filepath.Dir(path), 0777)
			w, err := os.Create(path)
			if err != nil {
				return err
			}
			c.Log.Printf("copying %s %s", path, f.ID.Hex())
			rc, err := cache.Repository().Get(ctx, f.ID)
			if err != nil {
				return err
			}
			_, err = io.Copy(w, rc)
			rc.Close()
			return err
		})
	}
	if err := g.Wait(); err != nil {
		c.Fatal(err)
	}
}

func digestFile(path string) (digest.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return digest.Digest{}, err
	}
	defer f.Close()
	w := reflow.Digester.NewWriter()
	_, err = io.Copy(w, f)
	return w.Digest(), err
}
