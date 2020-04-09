// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"io/ioutil"
	"math/big"
	"os"
	"testing"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

type memorySourcer map[string][]byte

func (m memorySourcer) Source(path string) ([]byte, error) {
	p := m[path]
	if p == nil {
		return nil, os.ErrNotExist
	}
	return p, nil
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

func TestBundleParam(t *testing.T) {
	sess := NewSession(nil)
	m, err := sess.Open("testdata/bundle/param.rf")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.InjectArgs(sess, []string{"-p3=321"}); err != nil {
		t.Fatal(err)
	}
	bundle := sess.Bundle()
	var buf bytes.Buffer
	if err = bundle.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	main, err := ioutil.ReadFile("testdata/bundle/parammain.rf")
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
	env := sess.Values.Push()
	v, err := m.Make(sess, env)
	if err != nil {
		t.Fatal(err)
	}
	v = v.(values.Module)["Main"]
	v = Force(v, types.Tuple(&types.Field{T: types.Int}, &types.Field{T: types.String}))
	tup := v.(values.Tuple)
	if got, want := tup[0].(*big.Int), 321; got.Int64() != int64(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tup[1], "hellook"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
