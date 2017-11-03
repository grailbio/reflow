// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"errors"
	"io"
	"math/big"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func init() {
	// This is basically always useful.
	yyErrorVerbose = true
}

// ParserMode determines the lexer's entry behavior.
type ParserMode int

const (
	// ParseModule parses a module.
	ParseModule ParserMode = iota
	// ParseDecls parses a set of declarations.
	ParseDecls
	// ParseExpr parses an expression.
	ParseExpr
	// ParseType parses a type.
	ParseType
)

// Parser is a Reflow lexer. It composes an (internal) scanner to
// produce tokens for the YACC grammar. Parser inserts semicolons
// following the rules outlined in the package docs.
// Lex implements the (internal) yyParser.
type Parser struct {
	// File is prefixed to parser error locations.
	File string
	// Body is the io.Reader that is parsed.
	Body io.Reader

	// Mode governs how the parser is started. See documentation above.
	// The fields Module, Decls, Expr, and Type are set depending on the
	// parser mode.
	Mode ParserMode

	// Module contains the parsed module (LexerModule).
	Module *ModuleImpl
	// Decls contains the parsed declarations (LexerDecls).
	Decls []*Decl
	// Expr contains the parsed expression (LexerExpr).
	Expr *Expr
	// Type contains the pased type (LexerType).
	Type *types.T

	el errlist

	scanner scanner.Scanner
	first   int

	cur, next, last struct {
		prev, tok rune
		text      string
		pos       scanner.Position
	}

	needUnscan bool
}

func isIdentRune(ch rune, i int) bool {
	return unicode.IsLetter(ch) || (unicode.IsDigit(ch) || ch == '_') && i > 0
}

func isValidIdent(s string) bool {
	for i, width := 0, 0; len(s) > 0; s, i = s[width:], i+1 {
		r := rune(s[0])
		width = 1
		if r >= utf8.RuneSelf {
			r, width = utf8.DecodeRuneInString(s)
		}
		if !isIdentRune(r, i) {
			return false
		}
	}
	return true
}

// Init initializes the lexer. It must be called before Lex.
func (x *Parser) init() {
	switch x.Mode {
	case ParseModule:
		x.first = tokStartModule
	case ParseDecls:
		x.first = tokStartDecls
	case ParseExpr:
		x.first = tokStartExpr
	case ParseType:
		x.first = tokStartType
	}
	x.scanner.Error = func(_ *scanner.Scanner, msg string) { x.Error(msg) }
	x.scanner.Filename = x.File
	x.scanner.Init(x.Body)
	x.scanner.Whitespace &= ^uint64(1 << '\n')
	x.scanner.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars |
		scanner.ScanStrings | scanner.ScanRawStrings | scanner.ScanComments
	x.scanner.IsIdentRune = isIdentRune
}

// Parse parses the parser's body and reports any parsing error.
// The parse result is deposited in x.Module, x.Decls, or x.Expr,
// depending on the parser's mode.
func (x *Parser) Parse() error {
	x.init()
	if yyParse(x) == 0 {
		return nil
	}
	err := x.el.Make()
	if err == nil {
		x.el = x.el.Errorf(x.scanner.Pos(), "unknown parse error")
		err = x.el.Make()
	}
	return err
}

// Error reports an error to the lexer.
func (x *Parser) Error(s string) {
	x.el = x.el.Error(x.scanner.Pos(), errors.New(s))
}

var identTokens = map[string]int{
	"val": tokVal,

	"file": tokFile,
	"dir":  tokDir,

	"struct": tokStruct,
	"module": tokModule,

	"exec": tokExec,

	"func":   tokFunc,
	"int":    tokInt,
	"float":  tokFloat,
	"string": tokString,
	"bool":   tokBool,

	"keyspace": tokKeyspace,
	"param":    tokParam,

	"if":   tokIf,
	"else": tokElse,

	"make": tokMake,

	"len":   tokLen,
	"zip":   tokZip,
	"unzip": tokUnzip,

	"flatten": tokFlatten,

	"map":  tokMap,
	"list": tokList,

	"panic": tokPanic,

	"requires": tokRequires,

	"type": tokType,

	// Reserved identifiers:
	"force":   tokReserved,
	"switch":  tokReserved,
	"case":    tokReserved,
	"import":  tokReserved,
	"include": tokReserved,
}

var tokens = map[rune]int{
	scanner.Arrow:         tokArrow,
	scanner.LeftArrow:     tokLeftArrow,
	scanner.Assign:        tokAssign,
	scanner.OrOr:          tokOrOr,
	scanner.AndAnd:        tokAndAnd,
	scanner.LE:            tokLE,
	scanner.GE:            tokGE,
	scanner.NE:            tokNE,
	scanner.EqEq:          tokEqEq,
	scanner.Lsh:           tokLSH,
	scanner.Rsh:           tokRSH,
	scanner.Ellipsis:      tokEllipsis,
	scanner.SquiggleArrow: tokSquiggleArrow,
	'@': tokAt,
}

// insertionToks defines the sets of tokens after which
// a semicolon is inserted.
var insertionToks = map[rune]bool{
	scanner.Ident:     true,
	scanner.String:    true,
	scanner.RawString: true,
	scanner.Int:       true,
	scanner.Float:     true,
	scanner.Char:      true,
	scanner.Template:  true,
	')':               true,
	'}':               true,
	']':               true,
}

func (x *Parser) scan() (prev, tok rune, text string, pos scanner.Position) {
	x.last = x.cur
	if x.next.tok != 0 {
		x.cur = x.next
		x.next.tok = 0
	} else {
		x.cur.prev, x.cur.tok, x.cur.text, x.cur.pos =
			x.cur.tok, x.scanner.Scan(), x.scanner.TokenText(), x.scanner.Pos()
	}
	return x.cur.prev, x.cur.tok, x.cur.text, x.cur.pos
}

func (x *Parser) unscan() (prev, tok rune, text string, pos scanner.Position) {
	x.next = x.cur
	return x.last.prev, x.last.tok, x.last.text, x.last.pos
}

// Lex returns the next token.
func (x *Parser) Lex(yy *yySymType) (xx int) {
	var comment string
	if tok := x.first; tok != 0 {
		x.first = 0
		return tok
	}

	prev, tok, text, pos := x.scan()
Scan:
	if tok, ok := tokens[tok]; ok {
		yy.pos.Position = pos
		yy.pos.comment = comment
		return tok
	}
	switch tok {
	case scanner.EOF:
		if (x.Mode == ParseDecls || x.Mode == ParseModule) && (prev != ';' && prev != '\n') {
			x.cur.tok = ';'
			return ';'
		}
		return tokEOF
	case scanner.Ident:
		if tok, ok := identTokens[text]; ok {
			yy.pos.Position = pos
			yy.pos.comment = comment
			return tok
		}
		switch text {
		case "true", "false":
			yy.expr = &Expr{
				Position: x.scanner.Pos(),
				Kind:     ExprConst,
				Type:     types.Bool,
				Val:      values.T(text == "true"),
			}
			return tokExpr
		}
		yy.expr = &Expr{Position: pos, Comment: comment, Kind: ExprIdent, Ident: text}
		return tokIdent
	case scanner.Int:
		yy.expr = &Expr{
			Position: pos,
			Kind:     ExprConst,
			Type:     types.Int,
		}
		i := new(big.Int)
		_, ok := i.SetString(text, 10)
		if !ok {
			x.Error("failed to parse integer \"" + text + "\"")
			return tokError
		}
		yy.expr.Val = values.T(i)
		return tokExpr
	case scanner.Float:
		yy.expr = &Expr{
			Position: pos,
			Kind:     ExprConst,
			Type:     types.Float,
		}
		f := new(big.Float)
		_, _, err := f.Parse(text, 10)
		if err != nil {
			x.Error("failed to parse float \"" + text + "\": " + err.Error())
			return tokError
		}
		yy.expr.Val = values.T(f)
		return tokExpr

	case scanner.String, scanner.RawString:
		yy.expr = &Expr{
			Position: pos,
			Kind:     ExprConst,
			Type:     types.String,
			Val:      values.T(text[1 : len(text)-1]),
		}
		return tokExpr
	case scanner.Template:
		s := text[2 : len(text)-2]
		yy.template = x.scanTemplate(s)
		if yy.template == nil {
			return tokError
		}
		return tokTemplate
	case scanner.Comment:
		prev1 := prev
		for {
			switch tok {
			case scanner.Comment:
				if strings.HasPrefix(text, "//") {
					comment += strings.TrimSpace(text[2:])
				} else {
					comment += strings.TrimSpace(text[2 : len(text)-2])
				}
			case '\n':
				comment += text
			default:
				if prev1 == '\n' {
					_, tok, text, pos = x.unscan()
				}
				goto Scan
			}
			prev1, tok, text, pos = x.scan()
		}
	case '\n':
		// Roughly follow Go's rules for semicolon insertion.
		if x.Mode != ParseExpr && insertionToks[prev] {
			return ';'
		}
		prev, tok, text, pos = x.scan()
		goto Scan
	}
	yy.pos.Position = pos
	yy.pos.comment = comment
	return int(tok)
}

func (x *Parser) scanTemplate(s string) *Template {
	t := &Template{Text: s}
	for {
		beg := strings.Index(s, "{{")
		if beg < 0 {
			break
		}
		t.Frags = append(t.Frags, s[:beg])
		end := strings.Index(s, "}}")
		if end < 0 {
			x.Error("unterminated interpolation")
			return nil
		}
		lx := &Parser{
			Mode: ParseExpr,
			Body: bytes.NewReader([]byte(s[beg+2 : end])),
		}
		if err := lx.Parse(); err != nil {
			for _, e := range err.(posErrors) {
				// Adjust positions to be relative to parent lexer.
				e.Filename = x.scanner.Pos().Filename
				e.Position.Line += x.scanner.Pos().Line
				e.Position.Offset += x.scanner.Pos().Offset
				if e.Position.Line == x.scanner.Pos().Line {
					e.Position.Column += x.scanner.Pos().Column
				}
				x.el = x.el.Append(e)
			}
			return nil
		}
		t.Args = append(t.Args, lx.Expr)
		s = s[end+2:]
	}
	t.Frags = append(t.Frags, s)
	return t
}
