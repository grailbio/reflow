// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build linux

package encryptiontest_test

var reliableGenerators = []generatorTestCase{
	{"ascendingDecimal", ascendingDecimal, true, false, false},
	{"ascendingHex", ascendingHex, true, false, false},
	{"ascendingBytes", ascendingBytes, true, false, false},
	{"pesudorand", pseudorand, false, true, false},
	{"cryptorand", cryptorand, false, true, true},
	{"divx", divx, true, false, false},
	{"zip", zip, true, false, false},
}
