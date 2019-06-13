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
)

// AssertionKey uniquely identifies an Assertion which consists of:
// - Namespace representing the namespace to which the subject of this Assertion belongs.
//   (Eg: "blob" for blob objects, "docker" for docker images, etc)
// - Subject representing the unique entity within the Namespace to which this Assertion applies.
//   (eg: full path to blob object, a Docker Image, etc)
// - Object representing the unique name of a property of the Subject within the Namespace.
//   (eg: "etag"/"size" for blob objects, "sha256" for docker images, etc)
type AssertionKey struct {
	Namespace string `json:",omitempty"`
	Subject   string `json:",omitempty"`
	Object    string `json:",omitempty"`
}

// Less returns whether the given AssertionKey is lexicographically smaller than this one.
func (a AssertionKey) Less(b AssertionKey) bool {
	if a.Namespace == b.Namespace {
		if a.Subject == b.Subject {
			return a.Object < b.Object
		}
		return a.Subject < b.Subject
	}
	return a.Namespace < b.Namespace
}

// Assertions are a map of AssertionKey to value.
// Conceptually, they represent a collection of subjects with specific values for some property of theirs.
// Unlike a regular sync.Map, adding an entry which maps a key to a different value results in an error.
type Assertions sync.Map // map[AssertionKey]string

// AssertionsFromMap creates an Assertions from a given map.
func AssertionsFromMap(t map[AssertionKey]string) *Assertions {
	a := new(Assertions)
	for k, tv := range t {
		a.maybeStore(k, tv)
	}
	return a
}

// rangeOver is a type-safe version of (sync.Map).Range
func (s *Assertions) rangeOver(ranger func(AssertionKey, string) bool) {
	if s == nil {
		return
	}
	(*sync.Map)(s).Range(func(k, v interface{}) bool {
		return ranger(k.(AssertionKey), v.(string))
	})
}

// load is a type-safe version of (sync.Map).Load
func (s *Assertions) load(k AssertionKey) (string, bool) {
	if s == nil {
		return "", false
	}
	v, ok := (*sync.Map)(s).Load(k)
	if v == nil {
		v = ""
	}
	return v.(string), ok
}

// maybeStore stores a key value entry (if new) into this assertions
// returning false and the given value.  If the key already exists
// and maps to a different value, then this will return true and the existing value.
func (s *Assertions) maybeStore(key AssertionKey, value string) (bool, string) {
	existing, loaded := (*sync.Map)(s).LoadOrStore(key, value)
	return loaded && existing != value, existing.(string)
}

// AddFrom adds all key-value pairs from t into s (panics if s is nil)
// If any key in t, if already existing in s, maps to a different value, an error is returned.
func (s *Assertions) AddFrom(t *Assertions) error {
	var err error
	t.rangeOver(func(k AssertionKey, tv string) bool {
		if conflict, existing := s.maybeStore(k, tv); conflict {
			err = fmt.Errorf("conflict for %s: %s -> %s", k, existing, tv)
			return false
		}
		return true
	})
	return err
}

// Digest returns the assertions' digest.
func (s *Assertions) Digest() digest.Digest {
	w := Digester.NewWriter()
	s.WriteDigest(w)
	return w.Digest()
}

// Equal returns whether the given Assertions is equal to this one.
// Two assertions are equal when the contain exactly the same keys and values.
// Since assertions are a sync.Map, the result of equality is impacted
// in the same way a call to sync.Map.Range is impacted by concurrent modifications.
func (s *Assertions) Equal(t *Assertions) bool {
	// Check sizes.
	if s.size() != t.size() {
		return false
	}
	equal := true
	// Check everything in s exists in t and has the same value.
	s.rangeOver(func(k AssertionKey, sv string) bool {
		tv, ok := t.load(k)
		equal = ok && tv == sv
		return equal
	})
	return equal
}

// Filter returns new Assertions mapping keys from t with values from s (panics if s is nil)
// and a list of AssertionKeys that exist in t but are missing in s.
func (s *Assertions) Filter(t *Assertions) (*Assertions, []AssertionKey) {
	m := make(map[AssertionKey]string)
	var missing []AssertionKey
	t.rangeOver(func(k AssertionKey, _ string) bool {
		if sv, ok := s.load(k); !ok {
			missing = append(missing, k)
		} else {
			m[k] = sv
		}
		return true
	})
	sort.Slice(missing, func(i, j int) bool { return missing[i].Less(missing[j]) })
	return AssertionsFromMap(m), missing
}

// IsEmpty returns whether this is empty, which it is if
// its a nil reference or has no entries.
func (s *Assertions) IsEmpty() bool {
	return s.size() == 0
}

// PrettyDiff returns a pretty-printable string representing
// the differences in the given Assertions that conflict with this one.
// Specifically only these differences are relevant:
// - any key present in t but not in s.
// - any entry with a mismatch in t and s.
func (s *Assertions) PrettyDiff(t *Assertions) string {
	if t.IsEmpty() {
		return ""
	}
	var diffs []string
	if s.IsEmpty() {
		t.rangeOver(func(k AssertionKey, tv string) bool {
			diffs = append(diffs, fmt.Sprintf("extra: %s=%s", k, tv))
			return true
		})
	} else {
		t.rangeOver(func(k AssertionKey, tv string) bool {
			diff := ""
			if sv, ok := s.load(k); !ok {
				diff = fmt.Sprintf("extra: %s=%s", k, tv)
			} else if sv != tv {
				diff = fmt.Sprintf("conflict %s: %s -> %s", k, sv, tv)
			}
			if diff != "" {
				diffs = append(diffs, diff)
			}
			return true
		})
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "\n")
}

// size returns the size of the map.
func (s *Assertions) size() int {
	l := 0
	s.rangeOver(func(_ AssertionKey, _ string) bool {
		l++
		return true
	})
	return l
}

// Short returns a short, string representation of assertions.
func (s *Assertions) Short() string {
	if s.IsEmpty() {
		return "empty"
	}
	return fmt.Sprintf("%d (%s)", s.size(), s.Digest().Short())
}

// String returns a full, human-readable string representing the assertions.
func (s *Assertions) String() string {
	if s.IsEmpty() {
		return "empty"
	}
	var keys []AssertionKey
	s.rangeOver(func(k AssertionKey, _ string) bool {
		keys = append(keys, k)
		return true
	})
	sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
	ss := make([]string, len(keys))
	for i, key := range keys {
		if v, ok := s.load(key); ok {
			ss[i] = fmt.Sprintf("%s %s %s=%s", key.Namespace, key.Subject, key.Object, v)
		}
	}
	return strings.Join(ss, ", ")
}

// WriteDigest writes the digestible material for a to w. The
// io.Writer is assumed to be produced by a Digester, and hence
// infallible. Errors are not checked.
func (s *Assertions) WriteDigest(w io.Writer) {
	var keys []AssertionKey
	s.rangeOver(func(k AssertionKey, _ string) bool {
		keys = append(keys, k)
		return true
	})
	sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
	for _, key := range keys {
		v, _ := s.load(key)
		io.WriteString(w, key.Subject)
		io.WriteString(w, key.Namespace)
		io.WriteString(w, key.Object)
		io.WriteString(w, v)
	}
}

// jsonEntry represents the json equivalent of each Assertions entry.
// This is used only to marshal/unmarshal Assertions into json.
type jsonEntry struct {
	Key   AssertionKey `json:",omitempty"`
	Value string       `json:",omitempty"`
}

// MarshalJSON defines a custom marshal method for converting Assertions to JSON.
func (s *Assertions) MarshalJSON() ([]byte, error) {
	entries := make([]jsonEntry, 0)
	s.rangeOver(func(k AssertionKey, v string) bool {
		entries = append(entries, jsonEntry{k, v})
		return true
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key.Less(entries[j].Key) })
	return json.Marshal(entries)
}

// UnmarshalJSON defines a custom unmarshal method for Assertions.
func (s *Assertions) UnmarshalJSON(b []byte) error {
	entries := make([]jsonEntry, 0)
	if err := json.Unmarshal(b, &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if conflict, existing := s.maybeStore(entry.Key, entry.Value); conflict {
			return fmt.Errorf("unmarshal conflict for %s: %s -> %s", entry.Key, existing, entry.Value)
		}
	}
	return nil
}

// GeneratorKey is the unique combination for which Assertions can be generated.
// For a given Subject and Namespace, the set of possible Object names should be consistent.
// generated
type GeneratorKey struct {
	Subject, Namespace string
}

// AssertionGenerator generates assertions based on a GeneratorKey.
// Implementations are specific to a namespace and generate assertions for a given subject.
type AssertionGenerator interface {
	// Generate computes assertions for a given GeneratorKey.
	Generate(ctx context.Context, key GeneratorKey) (*Assertions, error)
}

// GeneratorMux multiplexes a number of AssertionGenerator implementations based on the namespace.
type AssertionGeneratorMux map[string]AssertionGenerator

// Generate implements the AssertionGenerator interface for AttributerMux.
func (am AssertionGeneratorMux) Generate(ctx context.Context, key GeneratorKey) (*Assertions, error) {
	e, ok := am[key.Namespace]
	if !ok {
		return nil, fmt.Errorf("no assertion generator for namespace %v", key.Namespace)
	}
	return e.Generate(ctx, key)
}

// Assert asserts whether the target assertions is compatible with the src.
// Compatibility is directional and this strictly determines if the target
// is compatible with src and Asserter(target, src) may not yield the same result.
type Assert func(ctx context.Context, src, target *Assertions) bool

// AssertNever implements Assert for an always match (ie, never assert).
func AssertNever(_ context.Context, _, _ *Assertions) bool {
	return true
}

// AssertExact implements Assert for an exact match.
// That is, for each key in target, the value should match exactly what's in src
// and target can't contain keys missing in src.
func AssertExact(_ context.Context, src, target *Assertions) bool {
	if target.IsEmpty() {
		return true
	}
	if src.IsEmpty() {
		return false
	}
	match := true
	target.rangeOver(func(k AssertionKey, tv string) bool {
		if sv, ok := src.load(k); !ok || sv != tv {
			match = false
		}
		return match
	})
	return match
}
