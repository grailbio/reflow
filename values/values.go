// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package values defines data structures for representing (runtime) values
// in Reflow. Any valid reflow type has representable values (see grail.com/reflow/types)
// and the structures in this package mirror those in the type system.
//
// Values are represented by values.T, defined as
//
//	type T = interface{}
//
// which is done to clarify code that uses reflow values.
package values

import (
	"crypto" // The SHA-256 implementation is required for this package's
	// Digester.
	_ "crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sort"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/types"
)

// Digester is the digester used to compute value digests.
var Digester = digest.Digester(crypto.SHA256)

// T is the type of value. It is just an alias to interface{},
// but is used throughout code for clarity.
type T interface{}

// NewInt returns a new integer value.
func NewInt(i int64) T {
	return big.NewInt(i)
}

// NewFloat returns a new floating point value.
func NewFloat(f float64) T {
	return big.NewFloat(f)
}

// Tuple is the type of tuple values.
type Tuple []T

// List is the type of list values.
type List []T

type mapEntry struct {
	Key   T
	Value T
	Next  *mapEntry
}

// Map is the type of map values. It uses a Go map as a hash table
// based on the key's digest, which in turn stores a list of entries
// that share the same hash bucket.
type Map struct {
	n   int
	tab map[digest.Digest]**mapEntry
}

// Lookup looks up the provided key in map m. The caller must provide
// the key's digest which is used as a hash.
func (m Map) Lookup(d digest.Digest, key T) T {
	if _, ok := m.tab[d]; !ok {
		return nil
	}
	entry := *m.tab[d]
	for entry != nil && Less(entry.Key, key) {
		entry = entry.Next
	}
	if entry == nil || !Equal(entry.Key, key) {
		return nil
	}
	return entry.Value
}

// Insert inserts the provided key-value pair into the map,
// overriding any previous definiton of the key. The caller
// must provide the digest which is used as a hash.
func (m *Map) Insert(d digest.Digest, key, value T) {
	if m.tab[d] == nil {
		entry := &mapEntry{Key: key, Value: value}
		if m.tab == nil {
			m.tab = make(map[digest.Digest]**mapEntry)
		}
		m.n++
		m.tab[d] = &entry
		return
	}
	entryp := m.tab[d]
	for *entryp != nil && Less((*entryp).Key, key) {
		entryp = &(*entryp).Next
	}
	if *entryp == nil || !Equal((*entryp).Key, key) {
		*entryp = &mapEntry{Key: key, Value: value, Next: *entryp}
		m.n++
	} else {
		(*entryp).Value = value
	}
}

// Len returns the total number of entries in the map.
func (m Map) Len() int {
	return m.n
}

// Each enumerates all key-value pairs in map m in deterministic order.
//
// TODO(marius): we really ought to use a representation that's
// more amenable to such (common) operations.
func (m Map) Each(fn func(k, v T)) {
	digests := make([]digest.Digest, 0, len(m.tab))
	for d := range m.tab {
		digests = append(digests, d)
	}
	sort.Slice(digests, func(i, j int) bool { return digests[i].Less(digests[j]) })
	for _, d := range digests {
		for entry := *m.tab[d]; entry != nil; entry = entry.Next {
			fn(entry.Key, entry.Value)
		}
	}
}

// MakeMap is a convenient way to construct a from a set of key-value pairs.
func MakeMap(kt *types.T, kvs ...T) *Map {
	if len(kvs)%2 != 0 {
		panic("uneven makemap")
	}
	m := new(Map)
	for i := 0; i < len(kvs); i += 2 {
		m.Insert(Digest(kvs[i], kt), kvs[i], kvs[i+1])
	}
	return m
}

// Struct is the type of struct values.
type Struct map[string]T

// Module is the type of module values.
type Module map[string]T

type MutableDir struct {
	contents map[string]reflow.File
}

// Set sets the mutable directory's entry for the provided path.
// Set overwrites any previous file set at path.
func (d *MutableDir) Set(path string, file reflow.File) {
	if d.contents == nil {
		d.contents = make(map[string]reflow.File)
	}
	d.contents[path] = file
}

// Dir returns an immutable version of this dir.
func (d *MutableDir) Dir() Dir {
	contents := make(map[string]reflow.File, len(d.contents))
	for k, v := range d.contents {
		contents[k] = v
	}
	dir := Dir{}
	dir.AddContents(contents)
	return dir
}

// SumDir returns a dir which behaves as the sum of the given dirs.
// ie, SumDir behaves like a map containing all the key-value mappings that exist in Dirs d and e.
// If there is a duplicate key in d and e, the value of e will be effective - just as what would happen
// if we created a new map and added all the mappings from d first and then from e.
func SumDir(d Dir, e Dir) Dir {
	dir := Dir{}
	for _, c := range d.contentsList {
		dir.AddContents(c)
	}
	for _, c := range e.contentsList {
		dir.AddContents(c)
	}
	return dir
}

// Dir is an immutable type of directory values. Directory values are opaque
// and may only be accessed through its methods. This is to ensure
// proper usage, and that the directory is always accessed in the
// same order to provide determinism. The zero dir is a valid,
// empty directory.
// Dir is immutable.
type Dir struct {
	sortedKeys   []string
	contentsList []map[string]reflow.File
}

func (d *Dir) AddContents(contents map[string]reflow.File) {
	keySet := make(map[string]bool)
	// Add the existing sorted keys
	for _, k := range d.sortedKeys {
		keySet[k] = true
	}
	d.contentsList = append(d.contentsList, contents)
	// Add the keys from the new contents
	for k := range contents {
		keySet[k] = true
	}
	// Create a new list of sorted keys
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	d.sortedKeys = keys
}

// Len returns the number of entries in the directory.
func (d Dir) Len() int {
	return len(d.sortedKeys)
}

// Lookup returns the entry associated with the provided path and a boolean
// indicating whether the entry was found.
func (d Dir) Lookup(path string) (file reflow.File, ok bool) {
	// We iterate over the list of contents in reverse because we are simulating the
	// existence of a combined map to which key-value mappings were added from the list
	// of maps in d.contentsList in that order.
	for i := len(d.contentsList) - 1; i >= 0; i-- {
		if file, ok = d.contentsList[i][path]; ok {
			return file, ok
		}
	}
	return reflow.File{}, false
}

// SortedKeys returns a list of keys from this dir in sorted order.
func (d Dir) SortedKeys() []string {
	return d.sortedKeys
}

// A DirScanner is a stateful scan of a directory. DirScanners should
// be instantiated by Dir.Scan.
type DirScanner struct {
	path string
	todo []string
	dir  Dir
}

// Scan advances the scanner to the next entry (the first entry for a
// fresh scanner). It returns false when the scan stops with no more
// entries.
func (s *DirScanner) Scan() bool {
	if len(s.todo) == 0 {
		return false
	}
	s.path = s.todo[0]
	s.todo = s.todo[1:]
	return true
}

// Path returns the path of the currently scanned entry.
func (s *DirScanner) Path() string {
	return s.path
}

// File returns the file of the currently scanned entry.
func (s *DirScanner) File() reflow.File {
	file, _ := s.dir.Lookup(s.path)
	return file
}

// Scan returns a new scanner that traverses the directory in
// path-sorted order.
//
//	for scan := dir.Scan(); scan.Scan(); {
//		fmt.Println(scan.Path(), scan.File())
//	}
func (d Dir) Scan() DirScanner {
	return DirScanner{todo: d.SortedKeys(), dir: d}
}

// Equal compares the file names and digests in the directory.
func (d Dir) Equal(e Dir) bool {
	if d.Len() != e.Len() {
		return false
	}
	for _, lk := range d.SortedKeys() {
		lv, _ := d.Lookup(lk)
		if rv, ok := e.Lookup(lk); !ok || !rv.Equal(lv) {
			return false
		}
	}
	return true
}

// Variant holds a tagged value. It is the value type that occupies sum types.
type Variant struct {
	// Tag is the tag of this variant value.
	Tag string
	// Elem is the element of this variant. If this is a variant with no
	// element, this will be nil.
	Elem T
}

// Unit is the unit value.
var Unit = struct{}{}

// Equal tells whether values v and w are structurally equal.
func Equal(v, w T) bool {
	switch v := v.(type) {
	case reflow.File:
		l, r := v, w.(reflow.File)
		return l.Equal(r)
	default:
	}
	return reflect.DeepEqual(v, w)
}

// Less tells whether value v is (structurally) less than w.
func Less(v, w T) bool {
	if v == Unit {
		return false
	}
	switch v := v.(type) {
	case *big.Int:
		return v.Cmp(w.(*big.Int)) < 0
	case *big.Float:
		return v.Cmp(w.(*big.Float)) < 0
	case string:
		return v < w.(string)
	case bool:
		return !v && w.(bool)
	case reflow.File:
		w := w.(reflow.File)
		if v.IsRef() != w.IsRef() {
			return v.IsRef()
		}
		if !v.IsRef() {
			return v.ID.Less(w.ID)
		} else if v.Source != w.Source {
			return v.Source < w.Source
		} else {
			return v.ETag < w.ETag
		}
	case Dir:
		w := w.(Dir)
		if v.Len() != w.Len() {
			return v.Len() < w.Len()
		}
		vkeys, wkeys := v.SortedKeys(), w.SortedKeys()
		for i := range vkeys {
			if vkeys[i] != wkeys[i] {
				return vkeys[i] < wkeys[i]
			} else {
				var viFile, wiFile reflow.File
				viFile, _ = v.Lookup(vkeys[i])
				wiFile, _ = w.Lookup(wkeys[i])
				if Less(viFile, wiFile) {
					return true
				}
				// TODO(swami/pboyapalli): Why don't we return false if Less() returns false ?
			}
		}
		return false
	case List:
		w := w.(List)
		if len(v) != len(w) {
			return len(v) < len(w)
		}
		for i := range v {
			if Less(v[i], w[i]) {
				return true
			}
		}
		return false
	case *Map:
		w := w.(*Map)
		if n, m := v.Len(), w.Len(); n != m {
			return n < m
		}
		var (
			ventries = make([]*mapEntry, 0, v.Len())
			wentries = make([]*mapEntry, 0, w.Len())
		)
		for _, entryp := range v.tab {
			for entry := *entryp; entry != nil; entry = entry.Next {
				ventries = append(ventries, entry)
			}
		}
		for _, entryp := range w.tab {
			for entry := *entryp; entry != nil; entry = entry.Next {
				wentries = append(wentries, entry)
			}
		}
		sort.Slice(ventries, func(i, j int) bool { return Less(ventries[i].Key, ventries[j].Key) })
		sort.Slice(wentries, func(i, j int) bool { return Less(wentries[i].Key, wentries[j].Key) })
		for i := range ventries {
			ventry, wentry := ventries[i], wentries[i]
			if !Equal(ventry.Key, wentry.Key) {
				return Less(ventry.Key, wentry.Key)
			}
			if !Equal(ventry.Value, wentry.Value) {
				return Less(ventry.Value, wentry.Value)
			}
		}
		return false
	case Tuple:
		w := w.(Tuple)
		for i := range v {
			if !Equal(v[i], w[i]) {
				return Less(v[i], w[i])
			}
		}
		return false
	case Struct:
		w := w.(Struct)
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !Equal(v[k], w[k]) {
				return Less(v[k], w[k])
			}
		}
		return false
	case Module:
		w := w.(Module)
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !Equal(v[k], w[k]) {
				return Less(v[k], w[k])
			}
		}
		return false
	default:
		panic("attempted to compare incomparable values")
	}
}

// Location stores source code position and identifiers.
type Location struct {
	Ident    string
	Position string
}

// Func is the type of function value.
type Func interface {
	// Apply invokes this function with an argument list.
	// The supplied location may be used by system functions
	// for debugging and decorating flows.
	Apply(loc Location, args []T) (T, error)

	// Digest returns the digest of this function.
	Digest() digest.Digest
}

type shorter interface {
	Short() string
}

type stringer interface {
	String() string
}

// Sprint returns a pretty-printed version of value v
// with type t.
func Sprint(v T, t *types.T) string {
	switch arg := v.(type) {
	case shorter:
		return arg.Short()
	case stringer:
		return arg.String()
	case digester:
		return fmt.Sprintf("delayed(%v)", arg.Digest())
	}

	switch t.Kind {
	case types.ErrorKind, types.BottomKind:
		panic("illegal type")
	case types.IntKind:
		return v.(*big.Int).String()
	case types.FloatKind:
		return v.(*big.Float).String()
	case types.StringKind:
		return fmt.Sprintf("%q", v.(string))
	case types.BoolKind:
		if v.(bool) {
			return "true"
		}
		return "false"
	case types.FileKind:
		file := v.(reflow.File)
		if file.IsRef() {
			return fmt.Sprintf("file(source=%s, etag=%s)", file.Source, file.ETag)
		}
		return fmt.Sprintf("file(sha256=%s, size=%d)", file.ID, file.Size)
	case types.DirKind:
		dir := v.(Dir)
		entries := make([]string, 0, dir.Len())
		for scan := dir.Scan(); scan.Scan(); {
			entries = append(entries, fmt.Sprintf("%q: %s", scan.Path(), Sprint(scan.File(), types.File)))
		}
		return fmt.Sprintf("dir(%s)", strings.Join(entries, ", "))
	case types.FilesetKind:
		// We can't access the FileSet struct here because it would introduce
		// a circular dependency between reflow/ and reflow/values. We could
		// move the fileset definition elsewhere, but since this is anyway just a
		// backwards compatibility issue, we'll keep it opaque for now.
		d := v.(digester)
		return fmt.Sprintf("fileset(%s)", d.Digest().Short())
	case types.UnitKind:
		return "()"
	case types.ListKind:
		list := v.(List)
		elems := make([]string, len(list))
		for i, e := range list {
			elems[i] = Sprint(e, t.Elem)
		}
		return fmt.Sprintf("[%v]", strings.Join(elems, ", "))
	case types.MapKind:
		var keys, values []string
		for _, entryp := range v.(*Map).tab {
			for entry := *entryp; entry != nil; entry = entry.Next {
				keys = append(keys, Sprint(entry.Key, t.Index))
				values = append(values, Sprint(entry.Value, t.Elem))
			}
		}
		elems := make([]string, len(keys))
		for i := range keys {
			elems[i] = fmt.Sprintf("%s: %s", keys[i], values[i])
		}
		return fmt.Sprintf("[%s]", strings.Join(elems, ", "))
	case types.TupleKind:
		tuple := v.(Tuple)
		elems := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			elems[i] = Sprint(tuple[i], f.T)
		}
		return fmt.Sprintf("(%s)", strings.Join(elems, ", "))
	case types.StructKind:
		s := v.(Struct)
		elems := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			elems[i] = fmt.Sprintf("%s: %s", f.Name, Sprint(s[f.Name], f.T))
		}
		return fmt.Sprintf("{%s}", strings.Join(elems, ", "))
	case types.ModuleKind:
		s := v.(Module)
		elems := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			elems[i] = fmt.Sprintf("val %s = %s", f.Name, Sprint(s[f.Name], f.T))
		}
		return fmt.Sprintf("module{%s}", strings.Join(elems, "; "))
	case types.SumKind:
		variant := v.(*Variant)
		variantTyp := t.VariantMap()[variant.Tag]
		if variantTyp == nil {
			return fmt.Sprintf("#%s", variant.Tag)
		}
		return fmt.Sprintf("#%s(%s)", variant.Tag, Sprint(variant.Elem, variantTyp))
	case types.FuncKind:
		return fmt.Sprintf("func(?)")
	default:
		panic("unknown type " + t.String())
	}
}

func must(n int, err error) {
	if err != nil {
		panic(err)
	}
}

// Digest computes the digest for value v, given type t.
func Digest(v T, t *types.T) digest.Digest {
	w := Digester.NewWriter()
	WriteDigest(w, v, t)
	return w.Digest()
}

var (
	falseByte = []byte{0}
	trueByte  = []byte{1}
)

func writeLength(w io.Writer, n int) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(n))
	w.Write(b[:])
}

// WriteDigest writes digest material for value v (given type t)
// into the writer w.
func WriteDigest(w io.Writer, v T, t *types.T) {
	if d, ok := v.(digester); ok {
		digest.WriteDigest(w, d.Digest())
		return
	}

	w.Write([]byte{t.Kind.ID()})
	switch t.Kind {
	case types.ErrorKind, types.BottomKind, types.RefKind:
		panic("illegal type")
	case types.IntKind:
		vi := v.(*big.Int)
		// Bytes returns the normalized big-endian (i.e., free of a zero
		// prefix) representation of the absolute value of the integer.
		p := vi.Bytes()
		if len(p) == 0 {
			// This is the representation of "0"
			return
		}
		if p[0] == 0 {
			panic("big.Int byte representation is not normalized")
		}
		if vi.Sign() < 0 {
			w.Write([]byte{0})
		}
		w.Write(p)
	case types.FloatKind:
		w.Write([]byte(v.(*big.Float).Text('e', 10)))
	case types.StringKind:
		io.WriteString(w, v.(string))
	case types.BoolKind:
		if v.(bool) {
			w.Write(trueByte)
		} else {
			w.Write(falseByte)
		}
	case types.FileKind:
		digest.WriteDigest(w, v.(reflow.File).Digest())
	case types.DirKind:
		dir := v.(Dir)
		for scan := dir.Scan(); scan.Scan(); {
			io.WriteString(w, scan.Path())
			digest.WriteDigest(w, scan.File().Digest())
		}
	// Filesets are digesters, so they don't need to be handled here.
	case types.UnitKind:
	case types.ListKind:
		writeLength(w, len(v.(List)))
		for _, e := range v.(List) {
			WriteDigest(w, e, t.Elem)
		}
	case types.MapKind:
		m := v.(*Map)
		writeLength(w, m.Len())
		type kd struct {
			k T
			d digest.Digest
		}
		keys := make([]kd, 0, m.Len())
		for _, entryp := range m.tab {
			for entry := *entryp; entry != nil; entry = entry.Next {
				keys = append(keys, kd{entry.Key, Digest(entry.Key, t.Index)})
			}
		}
		// Sort the map so that it produces a consistent digest. We sort
		// its keys by their digest because the values may not yet be
		// evaluated.
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].d.Less(keys[j].d)
		})
		for _, k := range keys {
			WriteDigest(w, k.k, t.Index)
			WriteDigest(w, m.Lookup(k.d, k.k), t.Elem)
		}
	case types.TupleKind:
		writeLength(w, len(t.Fields))
		tuple := v.(Tuple)
		for i, f := range t.Fields {
			WriteDigest(w, tuple[i], f.T)
		}
	case types.StructKind:
		writeLength(w, len(t.Fields))
		s := v.(Struct)
		keys := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			keys[i] = f.Name
		}
		sort.Strings(keys)
		fm := t.FieldMap()
		for _, k := range keys {
			WriteDigest(w, s[k], fm[k])
		}
	case types.ModuleKind:
		writeLength(w, len(t.Fields))
		s := v.(Module)
		keys := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			keys[i] = f.Name
		}
		sort.Strings(keys)
		fm := t.FieldMap()
		for _, k := range keys {
			WriteDigest(w, s[k], fm[k])
		}
	case types.SumKind:
		variant := v.(*Variant)
		io.WriteString(w, variant.Tag)
		elemTyp, ok := t.VariantMap()[variant.Tag]
		if !ok {
			panic("unexpected variant tag: " + variant.Tag)
		}
		if elemTyp != nil {
			WriteDigest(w, variant.Elem, elemTyp)
		}
	case types.FuncKind:
		digest.WriteDigest(w, v.(Func).Digest())
	}
}
