// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/grailbio/base/digest"
)

// This is for exporting otherwise unexported members for testing purposes.

type LegacyAssertionKey = assertionKey

func (a LegacyAssertionKey) Less(b LegacyAssertionKey) bool {
	return a.less(b)
}

// Marshal defines a custom marshal for converting Assertions to JSON into the given io.Writer.
func (s *Assertions) Marshal(w io.Writer) error {
	return s.marshal(w)
}

// Unmarshal defines a custom unmarshal for Assertions using a json.Decoder.
func (s *Assertions) Unmarshal(dec *json.Decoder) error {
	return s.unmarshal(dec)
}

// MarshalJSON defines a custom marshal method for converting Assertions to JSON.
func (s *Assertions) MarshalJSON() ([]byte, error) {
	l := 0
	for _, v := range s.m {
		l += len(v.objects)
	}
	entries := make([]jsonEntry, l)
	i := 0
	for k, v := range s.m {
		for o, ov := range v.objects {
			entries[i] = jsonEntry{assertionKey{k.Namespace, k.Subject, o}, ov}
			i++
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key.less(entries[j].Key) })
	return json.Marshal(entries)
}

// UnmarshalJSON defines a custom unmarshal method for Assertions.
func (s *Assertions) UnmarshalJSON(b []byte) error {
	entries := make([]jsonEntry, 0)
	if err := json.Unmarshal(b, &entries); err != nil {
		return err
	}
	m := make(map[AssertionKey]*assertion)
	for _, entry := range entries {
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
	for _, v := range m {
		v.digest = digestMap(v.objects)
	}
	s.m = m
	return nil
}

func (s *RWAssertions) Assertions() *Assertions {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.a == nil {
		return nil
	}
	a := &Assertions{m: make(map[AssertionKey]*assertion, len(s.a.m))}
	for k, v := range s.a.m {
		a.m[k] = &assertion{objects: v.objects, digest: v.digest}
	}
	return a
}

func (s *RWAssertions) PrettyDiff(t *Assertions) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.a.PrettyDiff(t)
}

func (s *RWAssertions) Equal(t *Assertions) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.a.Equal(t)
}

// digestMap computes the digest of the given map.
func digestMap(m map[string]string) digest.Digest {
	w := Digester.NewWriter()
	for _, k := range sortedKeys(m) {
		_, _ = io.WriteString(w, k)
		_, _ = io.WriteString(w, m[k])
	}
	return w.Digest()
}

func WriteDigestOld(s *Assertions, w io.Writer) {
	s.writeDigestOld(w)
}

// WriteDigest writes the digestible material for a to w. The
// io.Writer is assumed to be produced by a Digester, and hence
// infallible. Errors are not checked.
func (s *Assertions) writeDigestOld(w io.Writer) {
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
		_, _ = io.WriteString(w, key.Subject)
		_, _ = io.WriteString(w, key.Namespace)
		_, _ = io.WriteString(w, key.Object)
		_, _ = io.WriteString(w, v)
	}
}

func WriteDigest(s *Assertions, w io.Writer) {
	s.writeDigest(w)
}
