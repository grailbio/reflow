// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package infra provides cloud infrastructure management for Go
// programs. The package includes facilities for configuring,
// provisioning, and migrating cloud infrastructure that is used by a Go
// program. You can think of infra as a simple, embedded version of
// Terraform, combined with a self-contained dependency injection
// framework.
//
// Infrastructure managed by this package is exposed through a
// configuration. Configurations specify which providers should be used
// for which infrastructure component; configurations also store
// provider configuration and migration state. Users instantiate typed
// values directly from the configuration: the details of configuration
// and of managing dependencies between infrastructure components is
// handled by the config object itself. Configurations are marshaled and
// must be stored by the user.
//
// Infrastructure migration is handled by maintaining a set of versions
// for each provider; migrations perform side-effects and can modify the
// configuration accordingly (e.g., to store identifiers used by the
// cloud infrastructure provider).
package infra

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

// A Schema defines a mapping between configuration keys and the
// types of values provided by those configuration keys. For example,
// the key "cluster" may provide values of the type
// "cluster.Interface". Schemas themselves are represented by strings
// to zero values of the mapped type. Interface types should use
// a pointer to a zero value. The following schema defines a mapping
// between to two interface types and a value type.
//
//	type Cluster interface { ... }
//	type BlobStore interface { ... }
//	type User string
//
//	var schema = infra.Schema{
//		"cluster": new(Cluster),
//		"repository": new(BlobStore),
//		"user": User(""),
//	}
//
// Schemas must be bijective: multiple keys cannot map to the same
// type.
type Schema map[string]interface{}

// Make builds a new configuration based on the Schema s, with the
// provided configuration keys. Make ensures that the configuration
// is well-formed: that there are no dependency cycles and that all
// dependencies are satisfied. Make panics if the schema is not a
// bijection.
func (s Schema) Make(keys Keys) (Config, error) {
	if keys == nil {
		keys = make(Keys)
	}
	config := Config{
		Keys:      keys,
		schema:    s,
		types:     s.types(),
		versions:  make(map[string]int),
		instances: make(map[reflect.Type]*instance),
	}
	if v := keys["versions"]; v != nil {
		if err := remarshal(v, config.versions); err != nil {
			return Config{}, err
		}
	}
	if err := config.build(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Unmarshal unmarshals the configuration keys in the YAML-formatted
// byte buffer p. The configuration is then initialized with Make.
func (s Schema) Unmarshal(p []byte) (Config, error) {
	keys := make(Keys)
	if err := yaml.Unmarshal(p, keys); err != nil {
		return Config{}, err
	}
	return s.Make(keys)
}

func (s Schema) types() map[reflect.Type]string {
	types := make(map[reflect.Type]string)
	for k, zero := range s {
		typ := reflect.TypeOf(zero)
		if typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Interface {
			typ = typ.Elem()
		}
		if _, ok := types[typ]; ok {
			panic("infra.Schema: bindings not bijective")
		}
		types[typ] = k
	}
	return types
}

// A Config manages a concrete configuration of infrastructure
// providers. Configs are instantiated from a Schema, which also
// performs validation.
type Config struct {
	Keys
	schema Schema

	types     map[reflect.Type]string
	instances map[reflect.Type]*instance
	order     []*instance

	versions map[string]int
}

// Instance stores the configuration-managed instance into the
// provided pointer. Instance panics if ptr is not pointer-typed.
// Instance returns an error if no providers are configured for the
// requested type, or if the provider's initialization failed.
func (c Config) Instance(ptr interface{}) error {
	vptr := reflect.ValueOf(ptr)
	if vptr.Kind() != reflect.Ptr {
		panic("infra.Instance: non-pointer argument")
	}
	inst := c.instances[vptr.Type().Elem()]
	if inst == nil {
		return fmt.Errorf("no provider for type %s", vptr.Type().Elem())
	}
	// If we get an instance, it's guaranteed to have well-formed dependencies.
	if err := inst.Init(); err != nil {
		return err
	}
	vptr.Elem().Set(inst.Value())
	return nil
}

// Must stores the configuration-managed instance into the provider
// pointer, as in Instance. Must fails fatally if any errors occur.
func (c Config) Must(ptr interface{}) {
	if err := c.Instance(ptr); err != nil {
		log.Fatal(err)
	}
}

// Marshal marshals the configuration's using YAML and returns the
// marshaled content. The configuration can thus be persisted and
// restored with Schema.Unmarshal.
func (c Config) Marshal() ([]byte, error) {
	keys := c.Keys.Clone()
	keys["versions"] = c.versions
	return yaml.Marshal(keys)
}

// Setup performs any required provider setup actions implied by this
// configuration. The configuration may be marshaled in the process
// and the caller should (re-)marshal the configuration after setup
// completes.
func (c Config) Setup() error {
	for _, inst := range c.order {
		impl := inst.Impl()
		if c.versions[impl] >= inst.Version() {
			continue
		}
		if err := inst.Setup(); err != nil {
			return fmt.Errorf("setup %s: %v", inst.Impl(), err)
		}
		c.versions[impl] = inst.Version()
	}
	return nil
}

func (c Config) provider(key string) (p *provider, name string) {
	args, ok := c.Keys.String(key)
	if !ok {
		return nil, ""
	}
	argv := strings.SplitN(args, ",", 2)
	return lookup(argv[0]), argv[0]
}

func (c Config) args(key string) []string {
	args, ok := c.Keys.String(key)
	if !ok || args == "" {
		return nil
	}
	return strings.Split(args, ",")[1:]
}

func (c *Config) build() error {
	graph := make(topoSorter)
	for typ, key := range c.types {
		p, impl := c.provider(key)
		if p == nil {
			if impl != "" {
				return fmt.Errorf("%s: no provider named %s", key, impl)
			}
			// Ignore missing providers. They only matter if they're
			// going to be used when instantiating values later on.
			continue
		}
		if !p.Type().AssignableTo(typ) {
			return fmt.Errorf(
				"provider %s implements type %s, which is incompatible to the bound type %s for key %s",
				impl, p.Type(), typ, key)
		}
		inst := p.New(*c)
		flags := inst.Flags()
		for _, arg := range c.args(key) {
			var (
				kv  = strings.SplitN(arg, "=", 2)
				err error
			)
			switch len(kv) {
			case 1:
				err = flags.Set(kv[0], "") // ok for booleans
			case 2:
				err = flags.Set(kv[0], kv[1])
			}
			if err != nil {
				return fmt.Errorf("provider %s flag %s: %v", impl, kv[0], err)
			}
		}
		if src, dst := c.Value(impl), inst.Config(); src != nil && dst != nil {
			if err := remarshal(src, dst); err != nil {
				return err
			}
		}
		if config := inst.Config(); config != nil {
			c.Keys[impl] = inst.Config()
		}
		c.instances[typ] = inst
	}

	for _, src := range c.instances {
		for _, typ := range src.RequiresInit() {
			dst := c.instances[typ]
			if dst == nil {
				return fmt.Errorf("no provider for type %s required to initialize provider %s", typ, src.Impl())
			}
			graph.Add(src, dst)
		}
		for _, typ := range src.RequiresSetup() {
			dst := c.instances[typ]
			if dst == nil {
				return fmt.Errorf("no provider for type %s required to setup provider %s", typ, src.Impl())
			}
			graph.Add(src, dst)
		}
	}
	if cycle := graph.Cycle(); cycle != nil {
		strs := make([]string, len(cycle))
		for i := range strs {
			strs[i] = cycle[i].Impl()
		}
		return fmt.Errorf("dependency cycle: %s", strings.Join(strs, "<-"))
	}
	c.order = graph.Sort()
	return nil
}

// Keys holds the toplevel configuration keys as managed
// by a Keys. Each config instance defines a provider for this
// type to be used by other providers that may need to access
// the raw configuration (e.g., common config values).
type Keys map[string]interface{}

var typeOfKeys = reflect.TypeOf(Keys{})

// Value returns the value associated with the provided key.
func (k Keys) Value(key string) interface{} {
	return k[key]
}

// String returns the string value of key k.
func (k Keys) String(key string) (string, bool) {
	v, ok := k[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// String returns the integer value of key k.
func (k Keys) Int(key string) (int, bool) {
	v, ok := k[key]
	if !ok {
		return 0, false
	}
	s, ok := v.(int)
	return s, ok
}

// Clone returns a deeply-copied version of keys.
func (k Keys) Clone() Keys {
	return deepcopy(k).(Keys)
}

func remarshal(src, dst interface{}) error {
	b, err := yaml.Marshal(src)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, dst)
}

func deepcopy(v interface{}) interface{} {
	switch w := v.(type) {
	case Keys:
		copy := make(Keys)
		for k, v := range w {
			copy[k] = deepcopy(v)
		}
		return copy
	case map[string]interface{}:
		copy := make(map[string]interface{})
		for k, v := range w {
			copy[k] = deepcopy(v)
		}
		return copy
	case map[interface{}]interface{}:
		copy := make(map[interface{}]interface{})
		for k, v := range w {
			copy[k] = deepcopy(v)
		}
		return copy
	default:
		return v
	}
}
