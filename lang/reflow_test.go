// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"testing"

	"github.com/grailbio/reflow"
)

func TestType(t *testing.T) {
	tab := []struct {
		e string
		t Type
	}{
		{`"hello"`, typeString},
		{`image("hello")`, typeImage},
		{`intern("s3://foo/bar")`, typeFlow},
		{`let x = intern("s3://foo/bar") in collect(x, "blah")`, typeFlow},
		{`let f = intern("s3://foo/bar") in collect(x, "blah")`, typeError},
		{`let i = image("blah") in i { blah }`, typeFlow},
		{`let foo = "hello" in image("blah") { echo hello {{foo}} }`, typeFlow},
		{`groupby(intern(""), "(.*)")`, typeFlowList},
		{`let foo = "hello" in image("blah") { echo hello {{bar}} }`, typeError},
		{`let foo = "hello" in concat(foo, "bar")`, typeString},
		{`param("blah", "help for blah")`, typeString},
		{`let foo(p) = image("blah") { {{p}} } 
		  in foo(intern("blah"))`, typeFlow},
		{`let foo(p, q) = image("blah") { {{p}} } 
		  in foo(intern("blah"))`, typeError},
		{`let id(p) = p in id`, typeFunc(1, typeFlow)},
		{`let id(p) = p in map(groupby(intern(""), "(.*)"), id)`, typeFlowList},
		{`let id(p) = p in map(intern(""), p)`, typeError},
	}
	for i, test := range tab {
		var buf bytes.Buffer
		error := Error{W: &buf}
		lx := &Lexer{error: &error, File: fmt.Sprintf("<case%d>", i), Body: bytes.NewReader([]byte(test.e)), Mode: LexerExpr}
		lx.Init()
		yyParse(lx)
		if error.N > 0 {
			t.Fatalf("case %d: parse error: %s", i, buf.String())
		}
		env := TypeEnv{Error: &error}
		if got, want := lx.Expr.Type(env), test.t; got != want {
			t.Errorf("case %d: got %v, want %v; %s errors %s", i, got, want, lx.Expr, buf.String())
		}
	}
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

func evalOrError(t *testing.T, e string) Val {
	var buf bytes.Buffer
	error := Error{W: &buf}
	lx := &Lexer{error: &error, File: "<test>", Body: bytes.NewReader([]byte(e)), Mode: LexerExpr}
	lx.Init()
	yyParse(lx)
	if error.N > 0 {
		t.Fatalf("parse error: %s", buf.String())
	}
	tenv := TypeEnv{Error: &error}
	if lx.Expr.Type(tenv) == typeError {
		t.Errorf("error type checking %s: %s", lx.Expr, buf.String())
	}
	return lx.Expr.Eval(EvalEnv{})
}

func valEqual(v, w Val) bool {
	if v.typ != w.typ || v.num != w.num || v.str != w.str {
		return false
	}
	if len(v.list) != len(w.list) {
		return false
	}
	for i := range v.list {
		if !valEqual(v.list[i], w.list[i]) {
			return false
		}
	}
	if (v.fn == nil) != (w.fn == nil) {
		return false
	}
	if v.fn != nil {
		panic("can't compare values")
	}
	if (v.flow == nil) != (w.flow == nil) {
		return false
	}
	if v.flow != nil && v.flow.Digest() != w.flow.Digest() {
		return false
	}
	return true
}

func TestEval(t *testing.T) {
	tab := []struct {
		e string
		v Val
	}{
		{`"hello world"`, Val{typ: typeString, str: "hello world"}},
		{`let a = "hello" in let b = "world" in concat(a, b)`, Val{typ: typeString, str: "helloworld"}},
		{`intern("s3://blah")`, Val{typ: typeFlow, flow: &reflow.Flow{Op: reflow.OpIntern, URL: mustURL("s3://blah")}}},
		{
			`groupby(intern("s3://blah"), "(.*)")`,
			Val{
				typ: typeFlowList,
				flow: &reflow.Flow{
					Op:   reflow.OpGroupby,
					Re:   regexp.MustCompile("(.*)"),
					Deps: []*reflow.Flow{{Op: reflow.OpIntern, URL: mustURL("s3://blah")}},
				},
			},
		},
	}
	for i, test := range tab {
		if got, want := evalOrError(t, test.e), test.v; !valEqual(got, want) {
			t.Errorf("case %d: got %v, want %v", i, got, want)
		}
	}
}
