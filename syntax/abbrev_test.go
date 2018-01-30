// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"testing"
)

func TestAbbrev(t *testing.T) {
	for _, c := range []struct{ s, abbrev string }{
		{`"hello world"`, ""},
		{"123456", ""},
		{"[1, 2, 3]", ""},
		{"1 + 2", ""},
		{"((1) + 2)", "1 + 2"},
		{"1 * 2 + 3", "1 * 2 + 3"},
		{"1 * (2 + (3))", "1 * (2 + 3)"},
		{"1*3 + (4 * 4) + 1", "1 * 3 + 4 * 4 + 1"},
		{"1/(3 * 3)", "1 / (3 * 3)"},
		{"1 * (1 + (2 * (3 + 4 * (5 + 6))))", "1 * (1 + 2 * (3 + 4 * (5 + 6)))"},
		{`file("s3://grail-marius/xxx")`, ``},
		{"len(   [ 1, 2,     35] )", "len([1, 2, 35])"},
	} {
		p := Parser{Mode: ParseExpr, Body: bytes.NewReader([]byte(c.s))}
		if err := p.Parse(); err != nil {
			t.Errorf("parse error: %v", err)
			continue
		}
		if c.abbrev == "" {
			c.abbrev = c.s
		}
		if got, want := p.Expr.Abbrev(), c.abbrev; got != want {
			t.Errorf("for %s, got %v, want %v", p.Expr, got, want)
		}
	}
}
