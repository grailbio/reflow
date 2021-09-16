// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

type memorySourcer map[string][]byte

func (m memorySourcer) Source(path string) (p []byte, d digest.Digest, err error) {
	p = m[path]
	if p == nil {
		err = os.ErrNotExist
	} else {
		d = reflow.Digester.FromBytes(p)
	}
	return
}

func TestBundle(t *testing.T) {
	sess := NewSession(nil)
	m, err := sess.Open("testdata/bundle/main.rf")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.InjectArgs(sess, []string{"-f1=x"}); err != nil {
		t.Fatal(err)
	}

	check := func(sess *Session, m Module) {
		t.Helper()
		env := sess.Values.Push()
		v, err := m.Make(sess, env)
		if err != nil {
			t.Fatal(err)
		}
		v = v.(values.Module)["Main"]
		v = Force(v, types.String)
		if got, want := v.(string), "xworld"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	check(sess, m)

	// Package it and re-run it. Injected args should be persisted.
	bundle := sess.Bundle()
	var buf bytes.Buffer
	if err := bundle.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	sess = NewSession(memorySourcer{
		"foo.rfx": buf.Bytes(),
	})
	m, err = sess.Open("foo.rfx")
	if err != nil {
		t.Fatal(err)
	}
	check(sess, m)

	// Also make sure it can be read by other modules:
	sess = NewSession(memorySourcer{
		"main.rf": []byte(`val Main = make("./foo.rfx").Main`),
		"foo.rfx": buf.Bytes(),
	})
	m, err = sess.Open("main.rf")
	if err != nil {
		t.Fatal(err)
	}
	check(sess, m)

	// Then make sure we can bundle modules that contain other bundles.
	bundle = sess.Bundle()
	buf.Reset()
	if err = bundle.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	sess = NewSession(memorySourcer{
		"bundlebundle.rfx": buf.Bytes(),
	})
	m, err = sess.Open("bundlebundle.rfx")
	if err != nil {
		t.Fatal(err)
	}
	check(sess, m)
}

func TestOpenBundleModuleCaching(t *testing.T) {
	for _, tt := range []struct {
		bundleMainArgs []string
		args []string
		wantX int
		wantY string
	}{
		{nil, nil, 321, "hellook"},
		{nil, []string{"-p1=world"}, 321, "worldok"},
		{nil, []string{"-p3=456"}, 456, "hellook"},
		{nil, []string{"-p1=ok", "-p3=789"}, 789, "okok"},

		{[]string{"-p1=world"}, nil, 321, "worldok"},
		{[]string{"-p1=world"}, []string{"-p1=hello"}, 321, "hellook"},
		{[]string{"-p1=world"}, []string{"-p3=456"}, 456, "worldok"},
		{[]string{"-p1=world"}, []string{"-p1=ok", "-p3=789"}, 789, "okok"},

		{[]string{"-p1=go"}, nil, 321, "gook"},
		{[]string{"-p1=go"}, []string{"-p1=hello"}, 321, "hellook"},
		{[]string{"-p1=go"}, []string{"-p3=456"}, 456, "gook"},
		{[]string{"-p1=go"}, []string{"-p1=ok", "-p3=789"}, 789, "okok"},
	} {
		sess, m := getModuleWithBundle(t, tt.bundleMainArgs)
		v := makeModule(t, tt.args, sess, m)
		v = v.(values.Module)["Main"]
		v = Force(v, types.Tuple(&types.Field{T: types.Int}, &types.Field{T: types.String}))
		tup := v.(values.Tuple)
		if got, want := tup[0].(*big.Int), tt.wantX; got.Int64() != int64(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tup[1], tt.wantY; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	// Check that the cache only consists of the bundles we expect:
	// The following bundle digests must exist in the cache, since they should be added by TestOpenBundleModuleCaching.
	//2021/09/30 13:46:00 openBundle p.rfx (digest sha256:1f44bcf4870b4beafd119007905b9ef0ec582db74f371bd98c46491164d1a546, size 584):
	//2021/09/30 13:46:00 openBundle main.rfx (digest sha256:2f4cb76b53bdd406d11f6e68518836068418ff3d868e9522bc8e79ff790df0e2, size 1274):
	//2021/09/30 13:46:00 openBundle main.rfx (digest sha256:706fb894641ce3bc7db0bb99c39fa76c30cf38ef1d28d51c4cd65b908a21443d, size 1285):
	//2021/09/30 13:46:00 openBundle main.rfx (digest sha256:5fe38f45f77d83229f5470ab70fdb8d4a6979c0e16be79d08ff67c14e3ad54c9, size 1281):
	mustExistDigests := map[string]bool {
		"sha256:1f44bcf4870b4beafd119007905b9ef0ec582db74f371bd98c46491164d1a546": true,
		"sha256:2f4cb76b53bdd406d11f6e68518836068418ff3d868e9522bc8e79ff790df0e2": true,
		"sha256:706fb894641ce3bc7db0bb99c39fa76c30cf38ef1d28d51c4cd65b908a21443d": true,
		"sha256:5fe38f45f77d83229f5470ab70fdb8d4a6979c0e16be79d08ff67c14e3ad54c9": true,
	}
	// The following digests may exist (these are added by TestBundle) depending on the execution order of the tests.
	//2021/09/30 13:09:22 openBundle foo.rfx (digest sha256:4fed51c26e4e9776b474c90b7c6fd582a3150db612c2ad4b8ccd97908f9dc831, size 952):
	//2021/09/30 13:09:22 openBundle bundlebundle.rfx (digest sha256:f327c049df7f73a91132ff79f901cd7d8146d4fea01169c7df94b67604c64393, size 1449):
	mayExistDigests := map[string]bool {
		"sha256:4fed51c26e4e9776b474c90b7c6fd582a3150db612c2ad4b8ccd97908f9dc831": true,
		"sha256:f327c049df7f73a91132ff79f901cd7d8146d4fea01169c7df94b67604c64393": true,
	}
	// Get all the cached digests
	var nMustExist, nOther int
	bundleModCache.Range(func(k, _ interface{}) bool {
		d := k.(digest.Digest)
		switch {
		case mustExistDigests[d.String()]:
			nMustExist++
		case mayExistDigests[d.String()]:
		default:
			nOther++
		}
		return true
	})

	if got, want := nMustExist, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := nOther, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func makeModule(t *testing.T, args []string, sess *Session, m Module) values.T {
	t.Helper()
	if len(args) == 0 {
		env := sess.Values.Push()
		v, err := m.Make(sess, env)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	flags, err := m.Flags(sess, sess.Values)
	if err != nil {
		t.Fatal(err)
	}
	usage := func() error {
		var b bytes.Buffer
		saved := flags.Output()
		flags.SetOutput(&b)
		flags.PrintDefaults()
		flags.SetOutput(saved)
		return fmt.Errorf("usage of %s:\n%s", paramMain, b.String())
	}
	if err = flags.Parse(args); err != nil {
		t.Fatal(err)
	}
	if flags.NArg() > 0 {
		err = fmt.Errorf("unrecognized parameters: %s", strings.Join(flags.Args(), " "))
		t.Fatalf("%v: %s", err, usage())
	}
	env := sess.Values.Push()
	if err = m.FlagEnv(flags, env, types.NewEnv()); err != nil {
		t.Fatalf("%v: %s", err, usage())
	}
	v, err := m.Make(sess, env)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

const paramMain = "testdata/bundle/parammain.rf"

func getModuleWithBundle(t *testing.T, mainargs []string) (*Session, Module) {
	t.Helper()
	sess := NewSession(nil)
	m, err := sess.Open("testdata/bundle/param.rf")
	if err != nil {
		t.Fatal(err)
	}
	if err = m.InjectArgs(sess, []string{"-p3=456"}); err != nil {
		t.Fatal(err)
	}
	bundle := sess.Bundle()
	var buf bytes.Buffer
	if err = bundle.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	main, err := ioutil.ReadFile(paramMain)
	if err != nil {
		t.Fatal(err)
	}
	sess = NewSession(memorySourcer{
		"main.rf": main,
		"p.rfx":   buf.Bytes(),
	})
	m, err = sess.Open("main.rf")
	if err != nil {
		t.Fatal(err)
	}
	if len(mainargs) > 0 {
		if err = m.InjectArgs(sess, mainargs); err != nil {
			t.Fatal(err)
		}
	}
	bundle2 := sess.Bundle()
	var buf2 bytes.Buffer
	if err = bundle2.WriteTo(&buf2); err != nil {
		t.Fatal(err)
	}
	sess = NewSession(memorySourcer{
		"main.rfx": buf2.Bytes(),
	})
	m, err = sess.Open("main.rfx")
	if err != nil {
		t.Fatal(err)
	}
	return sess, m
}
