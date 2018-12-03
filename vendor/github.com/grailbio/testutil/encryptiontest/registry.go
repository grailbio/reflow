// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryptiontest

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha512"
	"fmt"
	"hash"
	"strings"
)

// FakeAESRegistry represents a fake aes-128 (16-byte key) registry
// with hmac-sha512 for use with tests.
type FakeAESRegistry struct {
	Key []byte
}

var (
	// TestKey is the key used in tests.
	TestKey = []byte("0123456789abcdef")
	// TestID is the ID used in tests.
	TestID = []byte("0123456789abcdef")

	// BadID will generate an error if passed to NewBlock.
	BadID = []byte(strings.Repeat("0", 16))
	// FailGenKey will generate an error when GenerateKey is called.
	FailGenKey = []byte(strings.Repeat("1", 16))
	// FailNewGCMKey will generate an error when NewGCM is called.
	FailNewGCMKey = []byte(strings.Repeat("2", 16))
)

// GenerateKey implements encrypted.KeyRegistry.
func (fr *FakeAESRegistry) GenerateKey() (ID []byte, err error) {
	if bytes.Equal(fr.Key, FailGenKey) {
		return nil, fmt.Errorf("generate-key-failed")
	}
	return TestID, nil
}

// BlockSize implements encryted.KeyRegistry.
func (fr *FakeAESRegistry) BlockSize() int {
	return aes.BlockSize

}

// HMACSize implements encryted.KeyRegistry.
func (fr *FakeAESRegistry) HMACSize() int {
	return sha512.Size

}

// NewBlock implements encryted.KeyRegistry.
func (fr *FakeAESRegistry) NewBlock(ID []byte, opts ...interface{}) (hmacHash hash.Hash, block cipher.Block, err error) {
	if !bytes.Equal(ID, TestID) {
		return nil, nil, fmt.Errorf("new-block-failed")
	}
	h := hmac.New(sha512.New, fr.Key)
	c, err := aes.NewCipher(fr.Key)
	return h, c, err
}

// NewGCM implements encryted.KeyRegistry.
func (fr *FakeAESRegistry) NewGCM(block cipher.Block, opts ...interface{}) (aead cipher.AEAD, err error) {
	if bytes.Equal(fr.Key, FailNewGCMKey) {
		return nil, fmt.Errorf("new-gcm-block-failed")
	}
	return cipher.NewGCM(block)
}

// NewFakeAESRegistry returns a new FakeAESRegistry
func NewFakeAESRegistry() *FakeAESRegistry {
	return &FakeAESRegistry{Key: TestKey}
}
