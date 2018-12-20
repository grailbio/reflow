// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

// KeyConfig provides default values for keys that do not
// exist in the underlying configuration.
type KeyConfig struct {
	Config
	Key string
	Val interface{}
}

// Marshal adds the key-value pair carried by this struct
// into keys, if it is not added by Config.Marshal.
func (c *KeyConfig) Marshal(keys Keys) error {
	if err := c.Config.Marshal(keys); err != nil {
		return err
	}
	if _, ok := keys[c.Key]; !ok {
		keys[c.Key] = c.Val
	}
	return nil
}

// Value returns this KeyConfig's value for the config's
// key, if it is not returned by the underlying configuration's
// Value.
func (c *KeyConfig) Value(key string) interface{} {
	val := c.Config.Value(key)
	if key == c.Key && val == nil {
		return c.Val
	}
	return val
}

// StaticKeyConfig provides values for keys, but does not marshal them.
// Note that they Val provided for the Key overrides that (if any) provided by Config.
type StaticKeyConfig struct {
	Config
	Key string
	Val interface{}
}

// Value returns this StaticKeyConfig's value if the key matches Key.
func (c *StaticKeyConfig) Value(key string) interface{} {
	if key == c.Key {
		return c.Val
	}
	return c.Config.Value(key)
}
