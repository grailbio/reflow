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
