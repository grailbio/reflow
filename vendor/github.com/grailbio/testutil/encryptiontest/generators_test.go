// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryptiontest_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"path/filepath"
	"strconv"
	"time"

	"github.com/grailbio/testutil"
)

type generatorTestCase struct {
	name      string
	generator generatorFn
	fixed     bool
	random    bool
	flaky     bool
}

type generatorFn func(nbytes int) []byte

func readTestdata(name string) ([]byte, error) {
	n := filepath.Join("testdata", name)
	buf, err := ioutil.ReadFile(n)
	if err != nil {
		return nil, fmt.Errorf("failed to read %v, %v", buf, err)
	}
	return buf, nil
}

func zipFile(nbytes int) ([]byte, error) {
	b, err := readTestdata("dict.zip")
	if err != nil {
		return nil, err
	}
	if len(b) >= nbytes {
		return b[:nbytes], nil
	}
	return nil, fmt.Errorf("zip file too small: %v < %v", len(b), nbytes)
}

func divxFile(nbytes int) ([]byte, error) {
	b, err := readTestdata("divx-sample.mkv")
	if err != nil {
		return nil, err
	}
	if len(b) >= nbytes {
		return b[:nbytes], nil
	}
	return nil, fmt.Errorf("divx file too small: %v < %v", len(b), nbytes)
}

func zip(nbytes int) []byte {
	b, err := zipFile(nbytes)
	if err != nil {
		panic(fmt.Sprintf("%v: %v", testutil.Caller(1), err))
	}
	return b
}

func divx(nbytes int) []byte {
	b, err := divxFile(nbytes)
	if err != nil {
		panic(fmt.Sprintf("%v: %v", testutil.Caller(1), err))
	}
	return b
}

func ascendingBytes(nbytes int) []byte {
	b := make([]byte, nbytes, nbytes)
	v := byte(0)
	for i := 0; i < nbytes; i++ {
		b[i] = v
		v++
	}
	return b
}

func ascendingHex(nbytes int) []byte {
	return ascendingNumbers(nbytes, 16)
}

func ascendingDecimal(nbytes int) []byte {
	return ascendingNumbers(nbytes, 10)
}

func ascendingNumbers(nbytes, base int) []byte {
	buf := make([]byte, nbytes)
	n := 0
	for i := 0; i < nbytes; {
		tmp := strconv.FormatInt(int64(n), base)
		n++
		nlen := i + len(tmp)
		if nlen < nbytes {
			i += copy(buf[i:], tmp)
		} else {
			i += copy(buf[i:], tmp[:nlen-nbytes+1])
		}
	}
	return buf
}

func cryptorand(nbytes int) []byte {
	buf := make([]byte, nbytes)
	n, err := io.ReadFull(rand.Reader, buf)
	if err != nil || n != len(buf) {
		panic(err)
	}
	return buf
}

func pseudorand(nbytes int) []byte {
	rnd := mrand.New(mrand.NewSource(time.Now().Unix()))
	b := make([]byte, nbytes, nbytes)
	for i := 0; i < nbytes; i++ {
		b[i] = byte((rnd.Int() & 0xff00) >> 8)
	}
	return []byte(b)
}
