// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"bytes"
	"testing"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		body string
		toks []int
	}{
		{`"hello world" intern 
			// nothing should appear here
			someident`, []int{_EXPR, _INTERN, ';', _IDENT}},
		{`foo(x) = img {
			Some Command here
			intern
			# whatever here
		}
		`, []int{_IDENT, '(', _IDENT, ')', '=', _IDENT, _TEMPLATE, ';'}},
		{`let i = image("blah") in i { 
			blah 
		}`, []int{_LET, _IDENT, '=', _IMAGE, '(', _EXPR, ')', _IN, _IDENT, _TEMPLATE, ';'}},
	}
	for i, test := range tests {
		lx := &Lexer{Body: bytes.NewReader([]byte(test.body)), Mode: LexerTop}
		lx.Init()
		var yy yySymType
		if got, want := lx.Lex(&yy), _STARTTOP; got != want {
			t.Fatalf("case %d: got %v, want %v", i, got, want)
		}
		for pos, tok := range test.toks {
			if got, want := lx.Lex(&yy), tok; got != want {
				t.Fatalf("case %d: got %v, want %v at position %d", i, got, want, pos)
			}
		}
		if got, want := lx.Lex(&yy), _EOF; got != want {
			t.Fatalf("case %d: expected EOF, got %v", i, got)
		}
	}
}
