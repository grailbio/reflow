// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/unsafe"
)

// AssertionKey represents a subject within a namespace whose properties can be asserted.
// - Subject represents the unique entity within the Namespace to which this Assertion applies.
//   (eg: full path to blob object, a Docker Image, etc)
// - Namespace represents the namespace to which the subject of this Assertion belongs.
//   (eg: "blob" for blob objects, "docker" for docker images, etc)
type AssertionKey struct {
	Subject, Namespace string
}

// Less returns whether the given AssertionKey is lexicographically smaller than this one.
func (a AssertionKey) Less(b AssertionKey) bool {
	if a.Namespace == b.Namespace {
		return a.Subject < b.Subject
	}
	return a.Namespace < b.Namespace
}

// assertion represents the properties of a subject within a namespace (ie, an AssertionKey)
// captured as a mapping of property names to their values.
// For example, a specific blob object within the the "blob" namespace can have properties
// such as "etag" or "size" or a specific docker image (in the "docker" namespace)
// can have "sha256" or "version" as property names.
type assertion struct {
	objects map[string]string
	digest  digest.Digest
}

// newAssertion creates an assertion for a given key and object-value mappings.
func newAssertion(objects map[string]string) *assertion {
	a := assertion{objects: objects}
	w := Digester.NewWriter()
	for _, k := range sortedKeys(objects) {
		_, _ = io.WriteString(w, k)
		_, _ = io.WriteString(w, objects[k])
	}
	a.digest = w.Digest()
	return &a
}

// equal returns whether the given assertion is equal to this one.
func (a *assertion) equal(b *assertion) bool {
	return a.digest == b.digest
}

// prettyDiff returns a pretty-printable string representing
// the differences between this and the given assertion.
func (a *assertion) prettyDiff(b *assertion) string {
	if a.equal(b) {
		return ""
	}
	const empty = "<nil>"
	var sb strings.Builder
	for k, av := range a.objects {
		bv, ok := b.objects[k]
		if !ok {
			bv = empty
		}
		if av != bv {
			maybeComma(&sb)
			fmt.Fprintf(&sb, "%s (%s -> %s)", k, av, bv)
		}
	}
	for k, bv := range b.objects {
		if _, ok := a.objects[k]; !ok {
			maybeComma(&sb)
			fmt.Fprintf(&sb, "%s (%s -> %s)", k, empty, bv)
		}
	}
	return sb.String()
}

func (a *assertion) String() string {
	return strings.Join(a.stringParts(), ",")
}

func (a *assertion) stringParts() []string {
	s := make([]string, len(a.objects))
	for i, k := range sortedKeys(a.objects) {
		s[i] = fmt.Sprintf("%s=%s", k, a.objects[k])
	}
	return s
}

// sortedKeys returns a sorted slice of the keys in the given map.
func sortedKeys(m map[string]string) []string {
	i, keys := 0, make([]string, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// Assertions represent a collection of AssertionKeys with specific values for various properties of theirs.
// Assertions are immutable and constructed in one of the following ways:
//  NewAssertions: creates an empty Assertions and is typically used when subsequent operations are to AddFrom.
//  AssertionsFromEntry: creates Assertions from a single entry mapping an AssertionKey
//  to various properties (within the key's Namespace) of the named Subject in the key.
//  AssertionsFromMap: creates Assertions from a mapping of AssertionKey to properties.
//  MergeAssertions: merges a list of Assertions into a single Assertions.
type Assertions struct {
	m map[AssertionKey]*assertion

	digestOnce sync.Once
	digest     digest.Digest
}

// NewAssertions creates a new (empty) Assertions object.
func NewAssertions() *Assertions {
	return &Assertions{m: make(map[AssertionKey]*assertion)}
}

// AssertionsFromMap creates an Assertions from a given mapping of AssertionKey
// to a map representing its property names and corresponding values.
func AssertionsFromMap(m map[AssertionKey]map[string]string) *Assertions {
	a := &Assertions{m: make(map[AssertionKey]*assertion, len(m))}
	for k, v := range m {
		a.m[k] = newAssertion(v)
	}
	return a
}

// AssertionsFromEntry creates an Assertions from a single entry.
// It is similar to AssertionsFromMap and exists for convenience.
func AssertionsFromEntry(k AssertionKey, v map[string]string) *Assertions {
	return AssertionsFromMap(map[AssertionKey]map[string]string{k: v})
}

// MergeAssertions merges a list of Assertions into a single Assertions.
// Returns an error if the same key maps to a conflicting value as a result of the merge.
func MergeAssertions(list ...*Assertions) (*Assertions, error) {
	toMerge, l := DistinctAssertions(list...)
	if len(toMerge) == 1 {
		// If there's only one, Avoid unnecessarily populating and returning a new Assertions object.
		return toMerge[0], nil
	}
	merged := &Assertions{m: make(map[AssertionKey]*assertion, l)}
	for _, a := range toMerge {
		for k, v := range a.m {
			av, ok := merged.m[k]
			if !ok {
				merged.m[k] = v
				continue
			}
			if !av.equal(v) {
				return nil, fmt.Errorf("conflict for %s: %s", k, av.prettyDiff(v))
			}
		}
	}
	return merged, nil
}

// DistinctAssertions returns the distinct list of non-empty Assertions from the given list and a total size.
func DistinctAssertions(list ...*Assertions) ([]*Assertions, int) {
	// m maps the digest of an Assertions object to itself and is used to avoid duplicate in-memory objects.
	m := make(map[digest.Digest]*Assertions)
	deduped, l := list[:0], 0
	for _, a := range list {
		if a.IsEmpty() {
			continue
		}
		if v, dup := dedupFrom(m, a); !dup {
			l += v.size()
			deduped = append(deduped, v)
		}
	}
	deduped = deduped[:len(deduped):len(deduped)] // reduce capacity
	return deduped, l
}

// PrettyDiff returns a pretty-printable string representing the differences between
// the set of Assertions in lefts and rights.
// Specifically only these differences are relevant:
// - any key present in any of the rights but not in lefts.
// - any entry (in any of the rights) with a mismatching assertion (in any of the lefts).
// TODO(swami):  Add unit tests.
func PrettyDiff(lefts, rights []*Assertions) string {
	rights, rSz := DistinctAssertions(rights...)
	if rSz == 0 {
		return ""
	}
	var diffs []string
	lefts, lSz := DistinctAssertions(lefts...)
	if lSz == 0 {
		a := NewAssertions()
		for _, r := range rights {
			diffs = append(diffs, a.PrettyDiff(r))
		}
		sort.Strings(diffs)
		return strings.Join(diffs, "\n")
	}
	for _, r := range rights {
		for k, rv := range r.m {
			found := false
			for _, l := range lefts {
				if lv, ok := l.m[k]; ok {
					found = true
					if diff := lv.prettyDiff(rv); diff != "" {
						diffs = append(diffs, fmt.Sprintf("conflict %s: %s", k, diff))
					}
				}
				if found {
					break
				}
			}
			if !found {
				diffs = append(diffs, fmt.Sprintf("extra: %s: %s", k, rv))
			}
		}
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "\n")
}

// Equal returns whether the given Assertions is equal to this one.
func (s *Assertions) Equal(t *Assertions) bool {
	// Check sizes.
	if s.size() != t.size() {
		return false
	}
	if s.IsEmpty() {
		return true
	}
	// Check everything in s exists in t and has the same value.
	for k, sv := range s.m {
		if tv, ok := t.m[k]; !ok || !tv.equal(sv) {
			return false
		}
	}
	return true
}

// IsEmpty returns whether this is empty, which it is if its a nil reference or has no entries.
func (s *Assertions) IsEmpty() bool {
	return s.size() == 0
}

// size returns the size of this assertions.
func (s *Assertions) size() int {
	if s == nil || s.m == nil {
		return 0
	}
	return len(s.m)
}

// PrettyDiff returns a pretty-printable string representing
// the differences in the given Assertions that conflict with this one.
// Specifically only these differences are relevant:
// - any key present in t but not in s.
// - any entry with a mismatching assertion in t and s.
func (s *Assertions) PrettyDiff(t *Assertions) string {
	if t.IsEmpty() {
		return ""
	}
	var diffs []string
	if s.IsEmpty() {
		for k, tv := range t.m {
			diffs = append(diffs, fmt.Sprintf("extra: %s: %s", k, tv))
		}
	} else {
		for k, tv := range t.m {
			if sv, ok := s.m[k]; !ok {
				diffs = append(diffs, fmt.Sprintf("extra: %s: %s", k, tv))
			} else if diff := sv.prettyDiff(tv); diff != "" {
				diffs = append(diffs, fmt.Sprintf("conflict %s: %s", k, diff))
			}
		}
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "\n")
}

// Short returns a short, string representation of assertions.
func (s *Assertions) Short() string {
	if s.IsEmpty() {
		return "empty"
	}
	return fmt.Sprintf("#%d", s.size())
}

// String returns a full, human-readable string representing the assertions.
func (s *Assertions) String() string {
	if s.IsEmpty() {
		return "empty"
	}
	m := make(map[AssertionKey][]string)
	var keys []AssertionKey
	for k, v := range s.m {
		keys = append(keys, k)
		m[k] = v.stringParts()
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
	var ss []string
	for _, k := range keys {
		for _, p := range m[k] {
			ss = append(ss, fmt.Sprintf("%s %s %s", k.Namespace, k.Subject, p))
		}
	}
	return strings.Join(ss, ", ")
}

// Digest returns the assertions' digest.
func (s *Assertions) Digest() digest.Digest {
	if s != nil {
		s.digestOnce.Do(func() {
			s.digest = s.computeDigest()
		})
		return s.digest
	}
	return s.computeDigest()
}

// Digest returns the assertions' digest.
func (s *Assertions) computeDigest() digest.Digest {
	w := Digester.NewWriter()
	s.writeDigest(w)
	return w.Digest()
}

// WriteDigest writes the digestible material for a to w. The
// io.Writer is assumed to be produced by a Digester, and hence
// infallible. Errors are not checked.
func (s *Assertions) writeDigest(w io.Writer) {
	if s.IsEmpty() {
		return
	}
	// Convert the representation into the legacy format so that digests don't change
	var keys []assertionKey
	m := make(map[assertionKey]string)
	for k, v := range s.m {
		if v == nil {
			continue
		}
		for o, ov := range v.objects {
			key := assertionKey{k.Namespace, k.Subject, o}
			keys = append(keys, key)
			m[key] = ov
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].less(keys[j]) })
	for _, key := range keys {
		v := m[key]
		io.WriteString(w, key.Subject)
		io.WriteString(w, key.Namespace)
		io.WriteString(w, key.Object)
		io.WriteString(w, v)
	}
}

// RWAssertions are a mutable representation of Assertions.
type RWAssertions struct {
	a  *Assertions
	mu sync.Mutex
}

// NewRWAssertions creates a new RWAssertions with the given Assertions.
func NewRWAssertions(a *Assertions) *RWAssertions {
	return &RWAssertions{a: a}
}

// AddFrom adds to this RWAssertions from the given list of Assertions.
// Returns an error if the same key maps to a conflicting value as a result of the adding.
// AddFrom panics if s is nil.
func (s *RWAssertions) AddFrom(list ...*Assertions) error {
	toAdd, _ := DistinctAssertions(list...)
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	for _, a := range toAdd {
		for k, v := range a.m {
			av, ok := s.a.m[k]
			if !ok {
				s.a.m[k] = v
			} else if !av.equal(v) {
				err = fmt.Errorf("conflict for %s: %s", k, av.prettyDiff(v))
				break
			}
		}
		if err != nil {
			break
		}
	}
	return err
}

// Filter returns new Assertions mapping keys from t with values from s (panics if s is nil)
// and a list of AssertionKeys that exist in t but are missing in s.
func (s *RWAssertions) Filter(t *Assertions) (*Assertions, []AssertionKey) {
	if t == nil {
		return nil, nil
	}
	a := &Assertions{m: make(map[AssertionKey]*assertion, t.size())}
	var missing []AssertionKey
	s.mu.Lock()
	for k := range t.m {
		if sv, ok := s.a.m[k]; !ok {
			missing = append(missing, k)
		} else {
			a.m[k] = sv
		}
	}
	s.mu.Unlock()
	sort.Slice(missing, func(i, j int) bool { return missing[i].Less(missing[j]) })
	return a, missing
}

// dedupFrom de-duplicates the given Assertions object by returning the reference
// to an existing copy (if any) in the given map, or to itself.
// If the given assertion a is a duplicate, dedupFrom returns true.
func dedupFrom(m map[digest.Digest]*Assertions, a *Assertions) (*Assertions, bool) {
	d := a.Digest()
	if v, ok := m[d]; ok {
		return v, true
	}
	m[d] = a
	return a, false
}

// assertionKey is the legacy representation of AssertionKey and exists for the purpose of
// computing consistent digests (which, if changed, will invalidate all the cache keys)
// and for marshaling into and unmarshaling from legacy cached fileset results (and to continue
// to support the use of old and new binaries concurrently)
//
// assertionKey uniquely identifies an Assertion which consists of:
// - Namespace representing the namespace to which the subject of this Assertion belongs.
//   (Eg: "blob" for blob objects, "docker" for docker images, etc)
// - Subject representing the unique entity within the Namespace to which this Assertion applies.
//   (eg: full path to blob object, a Docker Image, etc)
// - Object representing the unique name of a property of the Subject within the Namespace.
//   (eg: "etag"/"size" for blob objects, "sha256" for docker images, etc)
type assertionKey struct {
	Namespace string `json:",omitempty"`
	Subject   string `json:",omitempty"`
	Object    string `json:",omitempty"`
}

// Less returns whether the given assertionKey is lexicographically smaller than this one.
func (a assertionKey) less(b assertionKey) bool {
	if a.Namespace == b.Namespace {
		if a.Subject == b.Subject {
			return a.Object < b.Object
		}
		return a.Subject < b.Subject
	}
	return a.Namespace < b.Namespace
}

// jsonEntry represents the json equivalent of each Assertions entry.
// This is used only to marshal/unmarshal Assertions into json.
type jsonEntry struct {
	Key   assertionKey `json:",omitempty"`
	Value string       `json:",omitempty"`
}

// marshal defines a custom marshal for converting Assertions to JSON into the given io.Writer.
func (s *Assertions) marshal(w io.Writer) error {
	var (
		commaB   = []byte(",")
		arrOpenB = []byte("[")
	)
	if _, err := w.Write(arrOpenB); err != nil {
		return err
	}
	keys := make([]AssertionKey, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })

	enc := json.NewEncoder(w)
	var okeys []string
	for i, k := range keys {
		if i > 0 {
			if _, err := w.Write(commaB); err != nil {
				return err
			}
		}
		objs := s.m[k].objects
		okeys = okeys[:0]
		for objk := range objs {
			okeys = append(okeys, objk)
		}
		sort.Strings(okeys)
		for j, ok := range okeys {
			if j > 0 {
				if _, err := w.Write(commaB); err != nil {
					return err
				}
			}
			ov := objs[ok]
			entry := jsonEntry{assertionKey{k.Namespace, k.Subject, ok}, ov}
			if err := enc.Encode(entry); err != nil {
				return err
			}
		}
	}
	if _, err := w.Write(unsafe.StringToBytes("]")); err != nil {
		return err
	}
	return nil
}

// unmarshal defines a custom unmarshal for Assertions using a json.Decoder.
func (s *Assertions) unmarshal(dec *json.Decoder) error {
	const debugMsg = "Assertions.unmarshal"
	if err := expectDelim(dec, arrOpen, debugMsg); err != nil {
		return err
	}
	m := make(map[AssertionKey]*assertion)
	for dec.More() {
		var entry jsonEntry
		if err := dec.Decode(&entry); err != nil {
			return err
		}
		k := AssertionKey{entry.Key.Subject, entry.Key.Namespace}
		v, ok := m[k]
		if !ok {
			v = new(assertion)
			v.objects = make(map[string]string)
		}
		existing, ok := v.objects[entry.Key.Object]
		if ok && existing != entry.Value {
			return fmt.Errorf("unmarshal conflict for %s: %s -> %s", entry.Key, existing, entry.Value)
		}
		if !ok {
			v.objects[entry.Key.Object] = entry.Value
			m[k] = v
		}
	}
	if err := expectDelim(dec, arrClose, debugMsg); err != nil {
		return err
	}
	var keys []string
	for _, v := range m {
		keys = keys[:0]
		for k := range v.objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		w := Digester.NewWriter()
		for _, k := range keys {
			_, _ = w.Write(unsafe.StringToBytes(k))
			_, _ = w.Write(unsafe.StringToBytes(v.objects[k]))
		}
		v.digest = w.Digest()
	}
	s.m = m
	return nil
}

// AssertionGenerator generates assertions based on a AssertionKey.
// Implementations are specific to a namespace and generate assertions for a given subject.
type AssertionGenerator interface {
	// Generate computes assertions for a given AssertionKey.
	Generate(ctx context.Context, key AssertionKey) (*Assertions, error)
}

// GeneratorMux multiplexes a number of AssertionGenerator implementations based on the namespace.
type AssertionGeneratorMux map[string]AssertionGenerator

// Generate implements the AssertionGenerator interface for AttributerMux.
func (am AssertionGeneratorMux) Generate(ctx context.Context, key AssertionKey) (*Assertions, error) {
	e, ok := am[key.Namespace]
	if !ok {
		return nil, fmt.Errorf("no assertion generator for namespace %v", key.Namespace)
	}
	return e.Generate(ctx, key)
}

// Assert asserts whether the target set of assertions are compatible with the src set.
// Compatibility is directional and this strictly determines if the target
// is compatible with src and Assert(target, src) may not yield the same result.
type Assert func(ctx context.Context, source, target []*Assertions) bool

// AssertNever implements Assert for an always match (ie, never assert).
func AssertNever(_ context.Context, _, _ []*Assertions) bool {
	return true
}

// AssertExact implements Assert for an exact match.
// That is, for each key in target, the value should match exactly what's in src
// and target can't contain keys missing in src.
func AssertExact(_ context.Context, source, target []*Assertions) bool {
	tgts, tSz := DistinctAssertions(target...)
	if tSz == 0 {
		return true
	}
	srcs, sSz := DistinctAssertions(source...)
	if sSz == 0 {
		return false
	}
	match := true
	for _, tgt := range tgts {
		for k, tv := range tgt.m {
			found := false
			for _, src := range srcs {
				if sv, ok := src.m[k]; ok {
					found = true
					match = match && sv.equal(tv)
				}
				if found {
					break
				}
			}
			match = match && found
			if !match {
				break
			}
		}
		if !match {
			break
		}
	}
	return match
}
