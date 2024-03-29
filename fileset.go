// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
)

// File represents a File inside of Reflow. A file is said to be
// resolved if it contains the digest of the file's contents (ID).
// Otherwise, a File is said to be a reference, in which case it must
// contain a source and etag and may contain a ContentHash.
// Any type of File (resolved or reference) can contain Assertions.
// TODO(swami): Split into resolved/reference files explicitly.
type File struct {
	// The digest of the contents of the file.
	ID digest.Digest

	// The size of the file.
	Size int64

	// Source stores a URL for the file from which it may
	// be retrieved.
	Source string `json:",omitempty"`

	// ETag stores an optional entity tag for the Source file.
	ETag string `json:",omitempty"`

	// LastModified stores the file's last modified time.
	LastModified time.Time `json:",omitempty"`

	// ContentHash is the digest of the file contents and can be present
	// for unresolved (ie reference) files.
	// ContentHash is expected to equal ID once this file is resolved.
	ContentHash digest.Digest `json:",omitempty"`

	// Assertions are the set of assertions representing the state
	// of all the dependencies that went into producing this file.
	// Unlike Etag/Size etc which are properties of this File,
	// Assertions can include properties of other subjects that
	// contributed to producing this File.
	// In order to include Assertions when converting to/from JSON,
	// the custom Fileset.Write and Fileset.Read methods must be used.
	// The standard JSON library (and probably most third party ones) will ignore this field.
	Assertions *Assertions `json:"-"`
}

// Digest returns the file's digest: if the file is a reference and
// it's ContentHash is unset, the digest comprises the
// reference, source, etag and assertions.
// Reference files will return ContentHash if set
// (which is assumed to be the digest of the file's contents).
// Resolved files return ID which is the digest of the file's contents.
func (f File) Digest() digest.Digest {
	if !f.IsRef() {
		return f.ID
	}
	if !f.ContentHash.IsZero() {
		return f.ContentHash
	}
	w := Digester.NewWriterShort()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(f.Size))
	w.Write(b[:])
	io.WriteString(w, f.Source)
	io.WriteString(w, f.ETag)
	f.Assertions.writeDigest(w)
	return w.Digest()
}

// Equal returns whether files f and g represent the same content.
// Since equality is a property of the file's contents, assertions are ignored.
func (f File) Equal(g File) bool {
	if f.IsRef() != g.IsRef() {
		panic(fmt.Sprintf("cannot compare unresolved and resolved file: f(%v), g(%v)", f, g))
	}
	if f.IsRef() || g.IsRef() {
		// When we compare references, we require nonempty etags.
		equal := f.Size == g.Size && f.Source == g.Source && f.ETag != "" && f.ETag == g.ETag
		// We require equal ContentHash if present in both
		if !f.ContentHash.IsZero() && !g.ContentHash.IsZero() && f.ContentHash.Name() == g.ContentHash.Name() {
			return equal && f.ContentHash == g.ContentHash
		}
		return equal
	}
	return f.ID == g.ID
}

// IsRef returns whether this file is a file reference.
func (f File) IsRef() bool {
	return f.ID.IsZero()
}

func (f File) String() string {
	var b strings.Builder
	if !f.IsRef() {
		fmt.Fprintf(&b, "id: %s", f.ID)
	}
	maybeComma(&b)
	fmt.Fprintf(&b, "size: %d", f.Size)
	if f.Source != "" {
		maybeComma(&b)
		fmt.Fprintf(&b, "source: %s", f.Source)
	}
	if f.ETag != "" {
		maybeComma(&b)
		fmt.Fprintf(&b, "etag: %v", f.ETag)
	}
	if !f.ContentHash.IsZero() {
		maybeComma(&b)
		fmt.Fprintf(&b, "contenthash: %v", f.ContentHash.Short())
	}
	if !f.Assertions.IsEmpty() {
		maybeComma(&b)
		fmt.Fprintf(&b, "assertions: <%s>", f.Assertions.Short())
	}
	return b.String()
}

func (f File) Short() string {
	if f.IsRef() {
		return f.Source
	} else {
		return f.ID.Short()
	}
}

// Fileset is the result of an evaluated flow. Values may either be
// lists of values or Filesets.  Filesets are a map of paths to Files.
type Fileset struct {
	List []Fileset       `json:",omitempty"`
	Map  map[string]File `json:"Fileset,omitempty"`
}

// File returns the only file expected to be contained in this Fileset.
// Returns error if the fileset does not contain only one file.
func (v Fileset) File() (File, error) {
	if n := v.N(); n != 1 {
		return File{}, errors.E("Fileset.File", v.Digest(), errors.Precondition, fmt.Sprintf("contains %d files (!=1)", n))
	}
	file, ok := v.Map["."]
	if !ok {
		return File{}, errors.E("Fileset.File", v.Digest(), errors.Precondition, "missing Map[\".\"]")
	}
	return file, nil
}

// Equal reports whether v is equal to w.
func (v Fileset) Equal(w Fileset) bool {
	if len(v.List) != len(w.List) {
		return false
	}
	for i := range v.List {
		if !v.List[i].Equal(w.List[i]) {
			return false
		}
	}
	if len(v.Map) != len(w.Map) {
		return false
	}
	for key, f := range v.Map {
		g, ok := w.Map[key]
		if !ok || f.IsRef() != g.IsRef() || !f.Equal(g) {
			return false
		}
	}
	return true
}

// Empty tells whether this value is empty, that is, it contains
// no files.
func (v Fileset) Empty() bool {
	for _, fs := range v.List {
		if !fs.Empty() {
			return false
		}
	}
	return len(v.Map) == 0
}

// AnyEmpty tells whether this value, or any of its constituent
// values contain no files.
func (v Fileset) AnyEmpty() bool {
	for _, fs := range v.List {
		if fs.AnyEmpty() {
			return true
		}
	}
	return len(v.List) == 0 && len(v.Map) == 0
}

// Assertions returns all the assertions across all the Files in this Fileset.
func (v Fileset) Assertions() *Assertions {
	files := v.Files()
	fas := make([]*Assertions, len(files))
	for i, f := range files {
		fas[i] = f.Assertions
	}
	a, err := MergeAssertions(fas...)
	if err != nil {
		// We don't expect assertions within a fileset to ever be inconsistent with each other.
		panic(fmt.Sprintf("inconsistent assertions in fileset %s: %v", v.Short(), err))
	}
	return a
}

// AddAssertions adds the given assertions to all files in this Fileset.
func (v *Fileset) AddAssertions(a *Assertions) error {
	if a.size() == 0 {
		return nil
	}
	for _, fs := range v.List {
		if err := fs.AddAssertions(a); err != nil {
			return err
		}
	}
	// m maps the digest of an Assertions object to itself and is used to avoid duplicate in-memory objects.
	m := make(map[digest.Digest]*Assertions)
	for k := range v.Map {
		f := v.Map[k]
		if f.Assertions == nil {
			f.Assertions, _ = dedupFrom(m, a)
		} else {
			merged, err := MergeAssertions(f.Assertions, a)
			if err != nil {
				return err
			}
			f.Assertions, _ = dedupFrom(m, merged)
		}
		v.Map[k] = f
	}
	return nil
}

// Flatten is a convenience function to flatten (shallowly) the value
// v, returning a list of Values. If the value is a list value, the
// list is returned; otherwise a unary list of the value v is
// returned.
func (v Fileset) Flatten() []Fileset {
	switch {
	case v.List != nil:
		return v.List
	default:
		return []Fileset{v}
	}
}

// Files returns the set of Files that comprise the value.
func (v Fileset) Files() []File {
	fs := map[digest.Digest]File{}
	v.files(fs)
	files := make([]File, len(fs))
	i := 0
	for _, f := range fs {
		files[i] = f
		i++
	}
	return files
}

// N returns the number of files (not necessarily unique) in this value.
func (v Fileset) N() int {
	var n int
	for _, v := range v.List {
		n += v.N()
	}
	n += len(v.Map)
	return n
}

// Size returns the total size of this value.
func (v Fileset) Size() int64 {
	var s int64
	for _, v := range v.List {
		s += v.Size()
	}
	for _, f := range v.Map {
		s += f.Size
	}
	return s
}

// MapAssertionsByFile maps the assertions from the given set of files
// to the corresponding same file (based on file.Digest()), if any, in this fileset.
func (v *Fileset) MapAssertionsByFile(files []File) {
	byDigest := make(map[digest.Digest]*Assertions)
	for _, f := range files {
		byDigest[f.Digest()] = f.Assertions
	}
	v.mapAssertionsByFileDigest(byDigest)
}

func (v *Fileset) mapAssertionsByFileDigest(byDigest map[digest.Digest]*Assertions) {
	if v.List != nil {
		for i := range v.List {
			v.List[i].mapAssertionsByFileDigest(byDigest)
		}
	}
	if v.Map != nil {
		for path, file := range v.Map {
			if a, ok := byDigest[file.Digest()]; ok {
				file.Assertions = a
				v.Map[path] = file
			}
		}
	}
}

// Subst the files in fileset using the provided mapping of File object digests to Files.
// Subst returns whether the fileset is fully resolved after substitution.
// That is, any unresolved file f in this fileset tree, will be substituted by sub[f.Digest()].
func (v Fileset) Subst(sub map[digest.Digest]File) (out Fileset, resolved bool) {
	resolved = true
	if v.List != nil {
		out.List = make([]Fileset, len(v.List))
		for i := range out.List {
			var ok bool
			out.List[i], ok = v.List[i].Subst(sub)
			if !ok {
				resolved = false
			}
		}
	}
	if v.Map != nil {
		out.Map = make(map[string]File, len(v.Map))
		for path, file := range v.Map {
			if f, ok := sub[file.Digest()]; ok {
				out.Map[path] = f
			} else {
				out.Map[path] = file
				if file.IsRef() {
					resolved = false
				}
			}
		}
	}
	return
}

func (v Fileset) files(fs map[digest.Digest]File) {
	for i := range v.List {
		v.List[i].files(fs)
	}
	if v.Map != nil {
		for _, f := range v.Map {
			fs[f.Digest()] = f
		}
	}
}

// Short returns a short, human-readable string representing the
// value. Its intended use is for pretty-printed output. In
// particular, hashes are abbreviated, and lists display only the
// first member, followed by ellipsis. For example, a list of values
// is printed as:
//	list<val<sample.fastq.gz=f2c59c40>, ...50MB>
func (v Fileset) Short() string {
	switch {
	case v.List != nil:
		s := "list<"
		if len(v.List) != 0 {
			s += v.List[0].Short()
			if len(v.List) > 1 {
				s += ", ..."
			} else {
				s += " "
			}
		}
		s += data.Size(v.Size()).String() + ">"
		return s
	case len(v.Map) == 0:
		return "val<>"
	default:
		paths := make([]string, len(v.Map))
		i := 0
		for path := range v.Map {
			paths[i] = path
			i++
		}
		sort.Strings(paths)
		path := paths[0]
		file := v.Map[path]
		s := fmt.Sprintf("val<%s=%s", path, file.Short())
		if len(paths) > 1 {
			s += ", ..."
		} else {
			s += " "
		}
		s += data.Size(v.Size()).String() + ">"
		return s
	}
}

// String returns a full, human-readable string representing the value v.
// Unlike Short, string is fully descriptive: it contains the full digest and
// lists are complete. For example:
//	list<sample.fastq.gz=sha256:f2c59c40a1d71c0c2af12d38a2276d9df49073c08360d72320847efebc820160>,
//	  sample2.fastq.gz=sha256:59eb82c49448e349486b29540ad71f4ddd7f53e5a204d50997f054d05c939adb>>
func (v Fileset) String() string {
	switch {
	case v.List != nil:
		vals := make([]string, len(v.List))
		for i := range v.List {
			vals[i] = v.List[i].String()
		}
		return fmt.Sprintf("list<%s>", strings.Join(vals, ", "))
	case len(v.Map) == 0:
		return "void"
	default:
		// TODO(marius): should we include the bindings here?
		paths := make([]string, len(v.Map))
		i := 0
		for path := range v.Map {
			paths[i] = path
			i++
		}
		sort.Strings(paths)
		binds := make([]string, len(paths))
		for i, path := range paths {
			binds[i] = fmt.Sprintf("%s=<%s>", path, v.Map[path])
		}
		return fmt.Sprintf("obj<%s>", strings.Join(binds, ", "))
	}
}

// Digest returns a digest representing the value. Digests preserve
// semantics: two values with the same digest are considered to be
// equivalent.
func (v Fileset) Digest() digest.Digest {
	w := Digester.NewWriterShort()
	v.WriteDigest(w)
	return w.Digest()
}

// WriteDigest writes the digestible material for v to w. The
// io.Writer is assumed to be produced by a Digester, and hence
// infallible. Errors are not checked.
func (v Fileset) WriteDigest(w io.Writer) {
	switch {
	case v.List != nil:
		for i := range v.List {
			v.List[i].WriteDigest(w)
		}
	default:
		paths := make([]string, len(v.Map))
		i := 0
		for path := range v.Map {
			paths[i] = path
			i++
		}
		sort.Strings(paths)
		for _, path := range paths {
			io.WriteString(w, path)
			digest.WriteDigest(w, v.Map[path].Digest())
		}
	}
}

// Pullup merges this value (tree) into a single toplevel fileset.
func (v Fileset) Pullup() Fileset {
	if v.List == nil {
		return v
	}
	p := Fileset{Map: map[string]File{}}
	v.pullup(p.Map)
	return p
}

func (v Fileset) pullup(m map[string]File) {
	for k, f := range v.Map {
		m[k] = f
	}
	for _, v := range v.List {
		v.pullup(m)
	}
}

// Version bytes for proto header. Two characters by convention.
var (
	filesetV2VersionBytes = []byte{'v', '2', '0', '1'}
)

// Write serializes and writes (marshals) this Fileset in the specified format to the given writer.
// If includeFileRefFields is true, the proto marshalled format will include reference file fields
// such as source and etag.
// If includeAssertions is true, the proto marshalled format will include assertions on files.
func (v *Fileset) Write(w io.Writer, kind assoc.Kind, includeFileRefFields, includeAssertions bool) error {
	switch kind {
	case assoc.Fileset:
		return v.marshalJSON(w)
	case assoc.FilesetV2:
		return v.marshalProto(w, includeFileRefFields, includeAssertions)
	}
	panic(fmt.Sprintf("unknown fileset marshal kind %d", kind))
}

// marshalProto is more efficient than marshalJSON and uses a custom format with
// the following design:
// - A ten byte header. The first two bytes identify the version of the fileset, the next eight
//   bytes contains the length of the root list.
// - The components of fileset (files in Fileset.Map, individual assertions and the linkage between those
//   assertions and certain files) are converted into smaller "part" structs.
// - Those parts are marshalled to disk as the protobuf formats defined in `fileset.proto`.
// If includeFileRefFields is true, the marshalled format will include reference file fields.
// If includeAssertions is true, the marshalled format will include assertions on files.
func (v *Fileset) marshalProto(w io.Writer, includeFileRefFields, includeAssertions bool) error {
	files := make(map[digest.Digest]int32, 0)
	assertions := make(map[string]int32, 0)
	assertionsGroups := make(map[*Assertions]int32, 0)
	b := proto.NewBuffer([]byte{})

	var a *Assertions

	files[digest.Digest{}] = 0
	assertions[""] = 0
	assertionsGroups[a] = 0

	// write version bytes
	if n, err := w.Write(filesetV2VersionBytes); err != nil {
		return err
	} else if n != len(filesetV2VersionBytes) {
		return errors.E(fmt.Sprintf("wanted to write %d version bytes while marshalling "+
			"fileset but only wrote %d", len(filesetV2VersionBytes), n))
	}
	// encode the length of the root list
	rootLen := int64(len(v.List))
	if err := binary.Write(w, binary.LittleEndian, rootLen); err != nil {
		return err
	}

	if err := v.encodeFilesetParts(0, -1, w, b, files, assertions, assertionsGroups, includeFileRefFields, includeAssertions); err != nil {
		return err
	}

	for i, fs := range v.List {
		if len(fs.List) != 0 {
			return fmt.Errorf(fmt.Sprintf("invalid fileset encountered of depth > 1 (%d)", len(fs.List)))
		}
		if err := fs.encodeFilesetParts(1, int32(i), w, b, files, assertions, assertionsGroups, includeFileRefFields, includeAssertions); err != nil {
			return err
		}
	}

	return nil
}

// marshalProtoWithBuffer writes a proto Message to the given Writer. First,
// the message is written to an intermediate buffer. The length of that serialized
// Message is then written to the output, followed by the serialized message itself.
// We must write the length of the message as a header so we can decode multiple
// fileset parts later when we are decoding the fileset as a stream.
// See the documentation below for more explanation on streaming multiple protobufs.
// https://developers.google.com/protocol-buffers/docs/techniques#streaming
func marshalProtoWithBuffer(pb proto.Message, b *proto.Buffer, w io.Writer) error {
	defer b.Reset()
	if err := b.Marshal(pb); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int64(len(b.Bytes()))); err != nil {
		return err
	}
	if n, err := w.Write(b.Bytes()); err != nil {
		return err
	} else if n != len(b.Bytes()) {
		return errors.New(fmt.Sprintf("expected to write %d bytes but only "+
			"wrote %d bytes while marshalling fileset part", len(b.Bytes()), n))
	}
	return nil
}

// unmarshalProtoWithBuffer reads the length of the next FilesetPart in the stream,
// copies that number of bytes to an intermediate buffer and finally unmarshals the
// FilesetPart encoded by that buffer. See the documentation for marshalProtoWithBuffer
// for more explanation of the stream format.
func unmarshalProtoWithBuffer(b *bytes.Buffer, r io.Reader) (FilesetPart, error) {
	defer b.Reset()
	var partLen int64
	if err := binary.Read(r, binary.LittleEndian, &partLen); err != nil {
		return FilesetPart{}, err
	}

	// After this point, all errors must be wrapped as Invalid, including io.EOF errors
	// (which are unexpected once a non-zero part size is read).
	if n, err := io.CopyN(b, r, partLen); err != nil {
		return FilesetPart{}, errors.E(errors.Invalid, fmt.Sprintf("failed to copy %d bytes for part", partLen), err)
	} else if n != partLen {
		return FilesetPart{}, errors.E(errors.Invalid, fmt.Sprintf("failed to read file part, "+
			"expected to read %d bytes but read %d bytes to the buffer", partLen, n))
	}
	var part FilesetPart
	if err := proto.Unmarshal(b.Bytes(), &part); err != nil {
		return FilesetPart{}, errors.E(errors.Invalid, fmt.Sprintf("failed to unmarshal %d byte proto part", partLen), err)
	} else {
		return part, nil
	}
}

func (v *Fileset) encodeFilesetParts(depth, index int32, w io.Writer, b *proto.Buffer,
	files map[digest.Digest]int32, assertionsPartsIdsByKey map[string]int32,
	assertionsGroupsPartsByPtr map[*Assertions]int32,
	includeFileRefFields, includeAssertions bool) error {
	if (depth == 0 && index != -1) || (depth == 1 && index < 0) || (depth > 1) {
		panic(fmt.Sprintf("attempting to encode fileset with unexpected shape [depth = %d, index = %d]",
			depth, index))
	}

	var err error
	for path, file := range v.Map {
		fileDigest := file.Digest()
		if _, ok := files[fileDigest]; !ok {
			var assertionsGroupId int32
			if includeAssertions {
				if assertionsGroupId, err = encodeAssertionsParts(
					file.Assertions,
					w, b,
					assertionsPartsIdsByKey,
					assertionsGroupsPartsByPtr); err != nil {
					return err
				}
			}
			var id string
			if !file.ID.IsZero() {
				id = file.ID.String()
			}
			fP := &FileP{
				Id:   id,
				Size: file.Size,
			}
			if includeFileRefFields {
				var contentHash string
				if !file.ContentHash.IsZero() {
					contentHash = file.ContentHash.String()
				}
				fP.Source = file.Source
				fP.Etag = file.ETag
				fP.ContentHash = contentHash
				fP.LastModified = &Timestamp{
					Seconds: file.LastModified.Unix(),
					Nanos:   int64(file.LastModified.Nanosecond()),
				}
			}

			if err := marshalProtoWithBuffer(&FilesetPart{
				Part: &FilesetPart_Fp{
					Fp: &FilePart{
						Id:                int32(len(files)),
						File:              fP,
						AssertionsGroupId: assertionsGroupId,
					},
				},
			}, b, w); err != nil {
				return err
			}
			files[fileDigest] = int32(len(files))
		}

		fid := files[fileDigest]
		if err := marshalProtoWithBuffer(&FilesetPart{
			Part: &FilesetPart_Fmp{
				Fmp: &FileMappingPart{
					Depth:  depth,
					Index:  index,
					Key:    path,
					FileId: fid,
				},
			},
		}, b, w); err != nil {
			return err
		}
	}
	return nil
}

// encodeAssertionsParts encodes and yields an ID.
func encodeAssertionsParts(
	assertions *Assertions,
	w io.Writer,
	b *proto.Buffer,
	assertionsPartsIdsByKey map[string]int32,
	assertionsGroupsPartsByPtr map[*Assertions]int32,
) (int32, error) {
	if assertions == nil {
		return 0, nil
	}
	var groupId int32
	var ok bool
	if groupId, ok = assertionsGroupsPartsByPtr[assertions]; ok {
		return groupId, nil
	}

	var i int
	assertionIds := make([]int32, len(assertions.m))
	for key, as := range assertions.m {
		if _, ok := assertionsPartsIdsByKey[key.Subject]; !ok {
			idx := int32(len(assertionsPartsIdsByKey))
			bp := &BlobProperties{}
			if v, ok := as.objects[BlobAssertionPropertyETag]; ok {
				bp.Etag = v
			}
			if v, ok := as.objects[BlobAssertionPropertyLastModified]; ok {
				bp.LastModified = v
			}
			if v, ok := as.objects[BlobAssertionPropertySize]; ok {
				bp.Size = v
			}
			keyPart := &AssertionsKeyPart{
				Id:      idx,
				Subject: key.Subject,
				Properties: &AssertionsKeyPart_Bp{
					Bp: bp,
				},
			}
			if err := marshalProtoWithBuffer(&FilesetPart{
				Part: &FilesetPart_Akp{
					Akp: keyPart,
				},
			}, b, w); err != nil {
				return 0, err
			}
			assertionsPartsIdsByKey[key.Subject] = idx
		}
		assertionIds[i] = assertionsPartsIdsByKey[key.Subject]
		i += 1
	}

	groupId = int32(len(assertionsGroupsPartsByPtr))

	if err := marshalProtoWithBuffer(&FilesetPart{
		Part: &FilesetPart_Agp{
			Agp: &AssertionsGroupPart{
				Id:     groupId,
				KeyIds: assertionIds,
			},
		},
	}, b, w); err != nil {
		return 0, err
	}

	assertionsGroupsPartsByPtr[assertions] = groupId
	return groupId, nil
}

// marshalJSON is more efficient than `json.Marshal` and more specifically it includes
// the Assertions of every File within this Fileset.
func (v *Fileset) marshalJSON(w io.Writer) error {
	var (
		commaB       = []byte(",")
		objOpenB     = []byte("{")
		objCloseB    = []byte("}")
		arrCloseB    = []byte("]")
		listOpenB    = []byte("\"List\":[")
		filesetOpenB = []byte("\"Fileset\":{")
		assertionsB  = []byte(",\"Assertions\":")
	)
	var err error
	if _, err = w.Write(objOpenB); err != nil {
		return err
	}
	if len(v.List) > 0 {
		if _, err = w.Write(listOpenB); err != nil {
			return err
		}
		for i, fs := range v.List {
			if i > 0 {
				if _, err = w.Write(commaB); err != nil {
					return err
				}
			}
			if err = fs.marshalJSON(w); err != nil {
				return err
			}
		}
		if _, err = w.Write(arrCloseB); err != nil {
			return err
		}
	}
	if len(v.Map) > 0 {
		if len(v.List) > 0 {
			if _, err = w.Write(commaB); err != nil {
				return err
			}
		}
		if _, err = w.Write(filesetOpenB); err != nil {
			return err
		}
		fns := make([]string, 0, len(v.Map))
		for fn := range v.Map {
			fns = append(fns, fn)
		}
		sort.Strings(fns)
		var bb bytes.Buffer
		enc := json.NewEncoder(&bb)
		for i, fn := range fns {
			file := v.Map[fn]
			if i > 0 {
				if _, err = w.Write(commaB); err != nil {
					return err
				}
			}
			if _, err = fmt.Fprintf(w, "\"%s\":", fn); err != nil {
				return err
			}
			if err = enc.Encode(file); err != nil {
				return err
			}
			if file.Assertions.size() == 0 {
				if _, err = w.Write(bytes.TrimSuffix(bb.Bytes(), []byte("\n"))); err != nil {
					return err
				}
				bb.Reset()
			} else {
				if _, err = w.Write(bytes.TrimSuffix(bb.Bytes(), []byte("}\n"))); err != nil {
					return err
				}
				bb.Reset()
				if _, err = w.Write(assertionsB); err != nil {
					return err
				}
				if err = file.Assertions.marshal(w); err != nil {
					return err
				}
				if _, err = w.Write(objCloseB); err != nil {
					return err
				}
			}
		}
		if _, err = w.Write(objCloseB); err != nil {
			return err
		}
	}
	if _, err = w.Write(objCloseB); err != nil {
		return err
	}
	return nil
}

// Read reads (unmarshals) a serialized fileset from the given Reader into
// this Fileset.
func (v *Fileset) Read(r io.Reader, kind assoc.Kind) error {
	switch kind {
	case assoc.Fileset:
		return v.unmarshalJSON(json.NewDecoder(r))
	case assoc.FilesetV2:
		return v.unmarshalProto(r)
	}
	panic(fmt.Sprintf("unknown fileset marshal kind %d", kind))
}

func (v *Fileset) unmarshalProto(r io.Reader) error {
	// file id -> file and assertion fields
	files := make(map[int32]*FilePart, 0)
	// assertion key part id -> set of blob properties
	assertions := make(map[int32]*AssertionsKeyPart)
	// assertion group id -> slice of assertion key parts
	assertionsGroupPartById := make(map[int32]*AssertionsGroupPart)
	// assertion group id -> complete assertion groups constructed
	// of assertion key parts
	assertionsGroupById := make(map[int32]*Assertions)

	// read version header
	versionHeader := make([]byte, len(filesetV2VersionBytes))
	if n, err := r.Read(versionHeader); err != nil {
		return err
	} else if n != len(filesetV2VersionBytes) {
		return errors.E(fmt.Sprintf("wanted to read %d version "+
			"bytes while marshalling fileset but only read %d",
			len(filesetV2VersionBytes), n))
	}

	if bytes.Compare(versionHeader, filesetV2VersionBytes) != 0 {
		return errors.E(fmt.Sprintf("unexpected version header "+
			"%v found while unmarshalling fileset", versionHeader))
	}

	// attempt to unmarshal the length of the root list
	var rootLen int64
	if err := binary.Read(r, binary.LittleEndian, &rootLen); err != nil {
		return err
	}
	if rootLen > 0 {
		v.List = make([]Fileset, rootLen)
	}

	buf := new(bytes.Buffer)
	for {
		var p FilesetPart
		var err error
		if p, err = unmarshalProtoWithBuffer(buf, r); err != nil {
			if err == io.EOF {
				return nil
			} else {
				return err
			}
		}
		if fid := p.GetFp(); fid != nil {
			files[fid.Id] = fid
		} else if as := p.GetAkp(); as != nil {
			assertions[as.Id] = as
		} else if g := p.GetAgp(); g != nil {
			assertionsGroupPartById[g.Id] = g
		} else if m := p.GetFmp(); m != nil {
			switch m.Depth {
			case 0:
				v.insertFile(m.Key, m.FileId, files,
					assertionsGroupPartById, assertionsGroupById,
					assertions)
			case 1:
				if int(m.Index) >= len(v.List) {
					l := make([]Fileset, m.Index+1)
					for i, fs := range v.List {
						l[i] = fs
					}
					v.List = l
				}
				v.List[m.Index].insertFile(m.Key, m.FileId, files,
					assertionsGroupPartById, assertionsGroupById,
					assertions)
			default:
				panic(fmt.Sprintf("unexpected depth fileset %d", m.Depth))
			}
		} else {
			panic("unexpected null part received")
		}
	}
}

func (v *Fileset) insertFile(key string, fId int32,
	filesById map[int32]*FilePart,
	assertionsGroupPartById map[int32]*AssertionsGroupPart,
	assertionsGroupById map[int32]*Assertions,
	assertionsById map[int32]*AssertionsKeyPart) {

	var f *FilePart
	var ok bool
	if f, ok = filesById[fId]; !ok {
		panic(fmt.Sprintf("failed to find expected file with id %d", fId))
	}

	id, err := digest.Parse(f.File.Id)
	if err != nil {
		panic(fmt.Sprintf("failed to parse digest %s during fileset unmarshalling", f.File.Id))
	}

	var ts time.Time
	if f.File.LastModified != nil {
		ts = time.Unix(f.File.LastModified.Seconds, f.File.LastModified.Nanos)
	}

	contentHash, err := digest.Parse(f.File.ContentHash)
	if err != nil {
		panic(fmt.Sprintf("failed to parse content hash %s during fileset unmarshalling", f.File.ContentHash))
	}

	file := File{
		ID:           id,
		Size:         f.File.Size,
		Source:       f.File.Source,
		ETag:         f.File.Etag,
		LastModified: ts,
		ContentHash:  contentHash,
	}

	if f.AssertionsGroupId != 0 {
		if _, ok = assertionsGroupById[f.AssertionsGroupId]; !ok {
			assertions := NewAssertions()
			assertionsGroupPart := assertionsGroupPartById[f.AssertionsGroupId]
			// only reconstruct the group once, afterwards use cached value
			delete(assertionsGroupPartById, f.AssertionsGroupId)
			for _, assertionPartId := range assertionsGroupPart.KeyIds {
				var as *AssertionsKeyPart
				if as, ok = assertionsById[assertionPartId]; !ok {
					panic(fmt.Sprintf("failed to find expected assertion with id %d", assertionPartId))
				}
				if as.GetBp() == nil {
					panic(fmt.Sprintf("expected blob properties on assertion("+
						"%s) during unmarshalling but found none", as.Subject))
				}
				fields := make(map[string]string, 3)
				if as.GetBp().Etag != "" {
					fields[BlobAssertionPropertyETag] = as.GetBp().Etag
				}
				if as.GetBp().LastModified != "" {
					fields[BlobAssertionPropertyLastModified] = as.GetBp().LastModified
				}
				if as.GetBp().Size != "" {
					fields[BlobAssertionPropertySize] = as.GetBp().Size
				}
				assertions.m[AssertionKey{Namespace: "blob", Subject: as.Subject}] = newAssertion(fields)
			}
			assertionsGroupById[f.AssertionsGroupId] = assertions
		}
		file.Assertions = assertionsGroupById[f.AssertionsGroupId]
	}
	if v.Map == nil {
		v.Map = make(map[string]File)
	}
	v.Map[key] = file
}

func (v *Fileset) unmarshalJSON(dec *json.Decoder) error {
	// m maps the digest of an Assertions object to itself and is used to avoid duplicate in-memory objects.
	m := make(map[digest.Digest]*Assertions)
	const debugMsg = "fileset.Read"
	if err := expectDelim(dec, objOpen, debugMsg+" (top)"); err != nil {
		return err
	}
	for {
		t, err := dec.Token()
		if err != nil {
			return errors.E(debugMsg+" (reading top-level token)", errors.Invalid, err)
		}
		switch t {
		case "List":
			if err = expectDelim(dec, arrOpen, debugMsg+" (List start)"); err != nil {
				return err
			}
			for dec.More() {
				var fs Fileset
				if err = fs.unmarshalJSON(dec); err != nil {
					return err
				}
				v.List = append(v.List, fs)
			}
			if err = expectDelim(dec, arrClose, debugMsg+" (List end)"); err != nil {
				return err
			}
		case "Fileset":
			if err = expectDelim(dec, objOpen, debugMsg+" (Fileset Map start)"); err != nil {
				return err
			}
			v.Map = make(map[string]File)
			for dec.More() {
				var f File
				t, err = dec.Token()
				if err != nil {
					return err
				}
				name, ok := t.(string)
				if !ok {
					return errors.E(debugMsg, errors.Precondition, errors.Errorf("unexpected token type: %T (want string)", t))
				}
				// Now we parse File
				if err = expectDelim(dec, objOpen, debugMsg+" (File start)"); err != nil {
					return err
				}
				for dec.More() {
					t, err = dec.Token()
					if err != nil {
						return err
					}
					var v interface{}
					s, ok := t.(string)
					if !ok {
						return errors.E(debugMsg+" (File attribute name)", errors.Precondition, errors.Errorf("unexpected token type: %T (want string)", t))
					}
					switch s {
					case "ID":
						v = &f.ID
					case "Size":
						v = &f.Size
					case "ETag":
						v = &f.ETag
					case "Source":
						v = &f.Source
					case "LastModified":
						v = &f.LastModified
					case "ContentHash":
						v = &f.ContentHash
					case "Assertions":
						a := new(Assertions)
						if err = a.unmarshal(dec); err != nil {
							return err
						}
						f.Assertions, _ = dedupFrom(m, a)
						continue
					default:
						return errors.E(debugMsg, errors.Precondition, errors.Errorf("unexpected field for reflow.File: %v", t))
					}
					if err = dec.Decode(v); err != nil {
						return err
					}
				}
				if err = expectDelim(dec, objClose, debugMsg+" (File end)"); err != nil {
					return err
				}
				v.Map[name] = f
			}
			if err = expectDelim(dec, objClose, debugMsg+" (Fileset Map end)"); err != nil {
				return err
			}
		case objClose:
			return nil
		default:
			return errors.E(debugMsg, errors.Precondition, errors.Errorf("unexpected token: %s (want 'List' or 'Fileset')", t))
		}
	}
}

// Diff deep-compares the values two filesets assuming they have the same structure
// and returns a pretty-diff of the differences (if any) and a boolean if they are different.
func (v Fileset) Diff(w Fileset) (string, bool) {
	return diffdepth(v, w, "", 0)
}

func diffdepth(a, b Fileset, prefix string, depth int) (string, bool) {
	var diffs []string
	set := make(map[string]struct{})
	for k := range a.Map {
		set[k] = struct{}{}
	}
	for k := range b.Map {
		set[k] = struct{}{}
	}
	list := make([]string, len(set))
	i := 0
	for k := range set {
		list[i] = k
		i++
	}
	sort.Strings(list)
	prefix = strings.Repeat("  ", depth) + prefix
	for _, k := range list {
		av, aok := a.Map[k]
		bv, bok := b.Map[k]
		switch {
		case aok && bok && av.Digest() != bv.Digest():
			diffs = append(diffs, fmt.Sprintf("%s\"%s\" = %s -> %s", prefix, k, av.Short(), bv.Short()))
		case aok && !bok:
			diffs = append(diffs, fmt.Sprintf("%s\"%s\" = %s -> %s", prefix, k, av.Short(), "void"))
		case !aok && bok:
			diffs = append(diffs, fmt.Sprintf("%s\"%s\" = %s -> %s", prefix, k, "void", bv.Short()))
		}
	}
	n := len(a.List)
	if n < len(b.List) {
		n = len(b.List)
	}
	for i := 0; i < n; i++ {
		var fsa, fsb Fileset
		if i < len(a.List) {
			fsa = a.List[i]
		}
		if i < len(b.List) {
			fsb = b.List[i]
		}
		switch {
		case fsa.Empty() && !fsb.Empty():
			diffs = append(diffs, fmt.Sprintf("[%d]:empty -> %s", i, fsb.Short()))
		case !fsa.Empty() && fsb.Empty():
			diffs = append(diffs, fmt.Sprintf("[%d]:%s -> empty", i, fsa.Short()))
		case !fsa.Empty() && !fsb.Empty():
			if d, ok := diffdepth(fsa, fsb, fmt.Sprintf("[%d]:", i), depth+1); ok {
				diffs = append(diffs, d)
			}
		}
	}
	return strings.Join(diffs, "\n"), len(diffs) > 0
}

func maybeComma(b *strings.Builder) {
	if b.Len() > 0 {
		b.WriteString(", ")
	}
}

var (
	objOpen  = json.Delim('{')
	objClose = json.Delim('}')
	arrOpen  = json.Delim('[')
	arrClose = json.Delim(']')
)

func expectDelim(dec *json.Decoder, d json.Delim, msg string) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != d {
		return errors.E(msg, errors.Precondition, errors.Errorf("unexpected token: %v (want %s)", t, d))
	}
	return nil
}
