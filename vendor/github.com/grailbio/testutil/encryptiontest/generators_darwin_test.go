// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build darwin

package encryptiontest_test

var reliableGenerators = []generatorTestCase{
	{"ascendingDecimal", ascendingDecimal, true, false, false},
	{"ascendingHex", ascendingHex, true, false, false},
	{"ascendingBytes", ascendingBytes, true, false, false},
	{"pesudorand", pseudorand, false, true, false},
	// cryptorand on mac isn't suitable for generating large amounts of
	// random data. There is a theoretical exploit, but it's not clear
	// it's practical. For our purposes, using cryptorand to generate
	// small amounts of random data (<160bytes) is fine, but it's not well
	// suited to this test.
	//	{"cryptorand", cryptorand, false, true,true},
	{"divx", divx, true, false, false},
	{"zip", zip, true, false, false},
}
