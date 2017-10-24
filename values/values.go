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
	"crypto"
	// The SHA-256 implementation is required for this package's
	// Digester.
	_ "crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/big"
	"reflect"
	"sort"
	"strings"

	"github.com/grailbio/base/digest"
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

// Tuple is the type of tuple values.
type Tuple []T

// List is the type of list values.
type List []T

// Struct is the type of struct values.
type Struct map[string]T

// Module is the type of module values.
type Module map[string]T

// Map is the type of map values.
type Map map[T]T

// File is the type of file values.
type File struct {
	ID   digest.Digest
	Size int64
}

// Dir is the type of directory values.
type Dir map[string]File

// Unit is the unit value.
var Unit = struct{}{}

// Equal tells whether values v and w are structurally equal.
func Equal(v, w T) bool {
	return reflect.DeepEqual(v, w)
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
	case types.StringKind:
		return fmt.Sprintf("%q", v.(string))
	case types.BoolKind:
		if v.(bool) {
			return "true"
		}
		return "false"
	case types.FileKind:
		file := v.(File)
		return fmt.Sprintf("file(sha256=%s, size=%d)", file.ID, file.Size)
	case types.DirKind:
		dir := v.(Dir)
		var keys []string
		for k := range dir {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		entries := make([]string, len(keys))
		for i, k := range keys {
			entries[i] = fmt.Sprintf("%q: %s", k, Sprint(dir[k], types.File))
		}
		return fmt.Sprintf("dir(%s)", strings.Join(entries, ", "))
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
		for k, v := range v.(Map) {
			keys = append(keys, Sprint(k, t.Index))
			values = append(values, Sprint(v, t.Elem))
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
		return fmt.Sprintf("module{%s}", t.Kind, strings.Join(elems, "; "))
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

	w.Write([]byte{byte(t.Kind)})
	switch t.Kind {
	case types.ErrorKind, types.BottomKind, types.RefKind:
		panic("illegal type")
	case types.IntKind:
		w.Write(v.(*big.Int).Bytes())
	case types.StringKind:
		io.WriteString(w, v.(string))
	case types.BoolKind:
		if v.(bool) {
			w.Write(trueByte)
		} else {
			w.Write(falseByte)
		}
	case types.FileKind:
		digest.WriteDigest(w, v.(File).ID)
	case types.DirKind:
		dir := v.(Dir)
		var keys []string
		for k := range dir {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			io.WriteString(w, k)
			digest.WriteDigest(w, dir[k].ID)
		}
	case types.UnitKind:
	case types.ListKind:
		writeLength(w, len(v.(List)))
		for _, e := range v.(List) {
			WriteDigest(w, e, t.Elem)
		}
	case types.MapKind:
		m := v.(Map)
		writeLength(w, len(m))
		var keys []T
		for k := range m {
			keys = append(keys, k)
		}
		// Sort the map so that it produces a consistent digest. We sort
		// its keys by their digest because the values may not yet be
		// evaluated.
		sort.Slice(keys, func(i, j int) bool {
			var (
				di = Digest(keys[i], t.Index)
				dj = Digest(keys[j], t.Index)
			)
			return di.Less(dj)
		})
		for _, k := range keys {
			WriteDigest(w, k, t.Index)
			WriteDigest(w, m[k], t.Elem)
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
	case types.FuncKind:
		if v == nil {
			log.Println("type is", t)
			panic("wtf")
		}
		digest.WriteDigest(w, v.(Func).Digest())
	}

}
