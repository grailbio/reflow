// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"errors"
	"strings"
)

var errInvalidTemplate = errors.New("invalid template")

// template implements a templating language: it parses identifiers
// enclosed by '{{' and '}}'; these identifiers are placed (in order)
// in Idents.
type template struct {
	Idents    []string
	Fragments []string
}

// newTemplate creates a new template from string s,
// returning errInvalidTemplate on error.
func newTemplate(s string) (*template, error) {
	s = strings.Replace(s, "%", "%%", -1)
	t := &template{}
	for {
		beg := strings.Index(s, `{{`)
		if beg < 0 {
			break
		}
		t.Fragments = append(t.Fragments, s[:beg])
		if len(s) < 4 {
			return nil, errInvalidTemplate
		}
		s = s[beg+2:]
		end := strings.Index(s, `}}`)
		if end < 0 {
			return nil, errInvalidTemplate
		}
		t.Idents = append(t.Idents, s[:end])
		s = s[end+2:]
	}
	t.Fragments = append(t.Fragments, s)
	return t, nil
}

func (t *template) Format(args ...string) string {
	s := ""
	for i := range t.Fragments {
		s += t.Fragments[i]
		if i >= len(t.Idents) {
			continue
		}
		s += args[i]
	}
	return s
}
