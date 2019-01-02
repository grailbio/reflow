// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package infra

import (
	"flag"
	"fmt"
	"reflect"
	"sync"

	"github.com/grailbio/base/sync/once"
)

var (
	typeOfError      = reflect.TypeOf((*error)(nil)).Elem()
	typeOfInt        = reflect.TypeOf(int(0))
	typeOfFlagSetPtr = reflect.TypeOf(new(flag.FlagSet))

	reservedKeys = map[string]bool{
		"versions":  true,
		"instances": true,
		"infra":     true,
	}

	mu        sync.Mutex
	providers = map[string]*provider{}
)

// Register registers the named provider with the infra package.
// Names must be unique, and can not be a reserved key ("versions",
// "instances", "infra").
//
// Register uses only the type of the provided object (which should be
// zero-valued): every config creates a new instance for exclusive use
// by that configuration. If iface is a pointer, then each config allocates
// a fresh instance. The registered value is the value managed by this
// provider. Management functions are provided by methods implemented
// directly on the value; they are one or more of the following:
//
//	// Init initializes the value using the provided requirements, which are
//	// themselves provisioned by the current configuration.
//	Init(req1 type1, req2 type2, ...) error
//
//	// Flags registers provider (instance) parameters in the provided
//	// FlagSet. Flag values are set before the value is used, initialized,
//	// or setup.
//	Flags(flags *flag.FlagSet)
//
//	// Setup performs infrastructure setup using the given requirements
//	// which are themselves provisioned by the config that manages the
//	// value.
//	Setup(req1 type1, req2 type2, ...) error
//
//	// Version returns the provider's version. Managed infrastructure
//	// is considered out of date if the currently configured version
//	// is less than the returned version. (Configured versions start
//	// at 0.) If Version is missing, then the version is taken to be
//	// 0.
//	Version() int
//
//	// Config returns the provider's configuration. The configuration
//	// is marshaled from and unmarshaled into the returned value.
//	// Configurations are restored before calls to Init or Setup.
//	Config() interface{}
//
//	// InstanceConfig returns the provider's instance configuration.
//	// This is used to marshal and unmarshal the specific instance (as
//	// initialized by Init) so that it may be restored later.
//	InstanceConfig() interface{}
func Register(name string, iface interface{}) {
	if reservedKeys[name] {
		panic("infra.Register: key " + name + " is reserved")
	}
	p := &provider{name: name, typ: reflect.TypeOf(iface)}
	if err := p.Typecheck(); err != nil {
		panic("infra.Register: invalid type " + p.typ.String() + " for provider named " + name + ": " + err.Error())
	}
	mu.Lock()
	defer mu.Unlock()
	if providers[name] != nil {
		panic("infra.Register: provider named " + name + " is already registered")
	}
	providers[name] = p
}

type provider struct {
	name string
	typ  reflect.Type
}

func lookup(key string) *provider {
	mu.Lock()
	p := providers[key]
	mu.Unlock()
	return p
}

// Typecheck performs typechecking of the provider. Specifically,
// methods Init, Setup, and Version must match their expected
// signatures as documented in Register.
func (p *provider) Typecheck() error {
	if m, ok := p.typ.MethodByName("Init"); ok {
		typ := m.Type
		if typ.NumOut() != 1 || typ.Out(0) != typeOfError {
			return fmt.Errorf("method Init: got %s, expected func(...) (type, error)", typ)
		}
	}
	if m, ok := p.typ.MethodByName("Setup"); ok {
		typ := m.Type
		if typ.NumOut() != 1 || typ.Out(0) != typeOfError {
			return fmt.Errorf("method Setup: got %s, expected func(...) error", typ)
		}
	}
	if m, ok := p.typ.MethodByName("Version"); ok {
		typ := m.Type
		if typ.NumOut() != 1 || typ.Out(0) != typeOfInt {
			return fmt.Errorf("method Version: got %s, expected func() int", typ)
		}
	}
	for _, name := range []string{"Config", "InstanceConfig"} {
		if m, ok := p.typ.MethodByName(name); ok {
			typ := m.Type
			if typ.NumOut() != 1 || typ.NumIn() != 1 {
				return fmt.Errorf("method %s: got %s, expected func() configType", name, typ)
			}
		}
	}
	return nil
}

// Type returns the type of values managed by this provider.
func (p *provider) Type() reflect.Type {
	return p.typ
}

// An instance holds an instance of a provider, as managed by a
// Config.
type instance struct {
	typ reflect.Type
	val reflect.Value

	config   Config
	name     string
	initOnce once.Task
	flags    flag.FlagSet
	flagOnce sync.Once
}

// New returns a new instance for the given Config.
func (p *provider) New(c Config) *instance {
	inst := &instance{typ: p.typ, name: p.name, config: c}
	if p.typ.Kind() == reflect.Ptr {
		inst.val = reflect.New(p.typ.Elem())
	} else {
		inst.val = reflect.Zero(p.typ)
	}
	return inst
}

// Impl returns the implementation name for this
// instance.
func (inst *instance) Impl() string { return inst.name }

// Value returns the value managed by this provider
// instance.
func (inst *instance) Value() reflect.Value {
	return inst.val
}

// Init performs value initialization, if required. Init uses the
// instance's configuration to look up dependent values; thus the
// dependency graph between instances must be well formed.
func (inst *instance) Init() error {
	if _, ok := inst.typ.MethodByName("Init"); !ok {
		return nil
	}
	init := inst.val.MethodByName("Init")
	return inst.initOnce.Do(func() error {
		types := inst.RequiresInit()
		args := make([]reflect.Value, len(types))
		for i, typ := range types {
			arg := inst.config.instances[typ]
			if err := arg.Init(); err != nil {
				return err
			}
			args[i] = arg.Value()
		}
		err := init.Call(args)[0].Interface()
		if err != nil {
			return err.(error)
		}
		return nil
	})
}

// Setup performs provider setup for the instance. Setup
// uses the configuration to instantiate required values;
// thus the instance dependency graph must be well formed.
func (inst *instance) Setup() error {
	if _, ok := inst.typ.MethodByName("Setup"); !ok {
		return nil
	}
	var (
		setup = inst.val.MethodByName("Setup")
		types = inst.RequiresSetup()
		args  = make([]reflect.Value, len(types))
	)
	for i, typ := range types {
		arg := inst.config.instances[typ]
		if err := arg.Init(); err != nil {
			return err
		}
		args[i] = arg.Value()
	}
	err := setup.Call(args)[0].Interface()
	if err != nil {
		return err.(error)
	}
	return nil
}

// Config returns the instance's config.
func (inst *instance) Config() interface{} {
	if _, ok := inst.typ.MethodByName("Config"); !ok {
		return nil
	}
	config := inst.val.MethodByName("Config")
	return config.Call(nil)[0].Interface()
}

// HasInstanceConfig returns whether this instance provides an
// instance configuration.
func (inst *instance) HasInstanceConfig() bool {
	_, ok := inst.typ.MethodByName("InstanceConfig")
	return ok
}

// InstanceConfig returns the instance's instance configuration.
// When marshaling an instance configuration, InstanceConfig
// must be called after initialization.
func (inst *instance) InstanceConfig() interface{} {
	if !inst.HasInstanceConfig() {
		return nil
	}
	m := inst.val.MethodByName("InstanceConfig")
	return m.Call(nil)[0].Interface()
}

// Flags returns the instance's FlagSet.
func (inst *instance) Flags() *flag.FlagSet {
	inst.flagOnce.Do(func() {
		m, ok := inst.typ.MethodByName("Flags")
		if !ok {
			return
		}
		if m.Type.NumIn() != 2 {
			return
		}
		if m.Type.In(1) != typeOfFlagSetPtr {
			return
		}
		mv := inst.val.MethodByName("Flags")
		mv.Call([]reflect.Value{reflect.ValueOf(&inst.flags)})
	})
	return &inst.flags
}

// Version returns the provider version for this instance.
func (inst *instance) Version() int {
	if _, ok := inst.typ.MethodByName("Version"); !ok {
		return 0
	}
	return inst.val.MethodByName("Version").Call(nil)[0].Interface().(int)
}

// RequiresInit returns the set of types required by this instance's
// Init method.
func (inst *instance) RequiresInit() []reflect.Type {
	m, ok := inst.typ.MethodByName("Init")
	if !ok {
		return nil
	}
	types := make([]reflect.Type, m.Type.NumIn()-1)
	for i := range types {
		types[i] = m.Type.In(i + 1)
	}
	return types
}

// RequiresSetup returns the set of types required by this instance's
// Setup method.
func (inst *instance) RequiresSetup() []reflect.Type {
	m, ok := inst.typ.MethodByName("Setup")
	if !ok {
		return nil
	}
	types := make([]reflect.Type, m.Type.NumIn()-1)
	for i := range types {
		types[i] = m.Type.In(i + 1)
	}
	return types
}

// CanMarshal returns whether the instance can be marshaled.
func (inst *instance) CanMarshal() bool {
	_, ok := inst.typ.MethodByName("MarshaledInstance")
	return ok
}
