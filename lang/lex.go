package lang

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/scanner"
	"unicode"
)

// LexerMode determines the lexer's entry behavior.
type LexerMode int

const (
	// LexerTop begins lexing of a top-level program--i.e., one
	// containing a number of statements.
	LexerTop LexerMode = iota
	// LexerExpr begins lexing of an expression.
	LexerExpr
	// LexerInclude begins lexing of an included file.
	LexerInclude
)

// Lexer is a lexer for reflow. Its tokens are defined in the reflow
// grammar. The lexer composes Go's text/scanner: it knows how to
// tokenize special identifiers, and performs semicolon insertion in
// the style of Go.
//
// The lexer also manages include directives, which are implemented
// by recursively instantiating a lexer for the included file.
// (If we want to support dynamic inclusion, this mechanism would
// need to be moved to the evaluator.)
type Lexer struct {
	// File is the filename reported by the lexer's position.
	File string
	// Body contains the text to be lexed.
	Body io.Reader

	// Mode specifies the Lexer mode.
	Mode LexerMode

	// HashVersion is the hash version string, if any.
	HashVersion string

	error *Error

	Expr  *Expr
	Stmts []*Stmt

	include       *Lexer
	includeCloser io.Closer

	scanner scanner.Scanner
	first   int
	tok     rune
	text    string
	nl      bool
}

// Init initializes the lexer.
func (lx *Lexer) Init() {
	switch lx.Mode {
	case LexerTop:
		lx.first = _STARTTOP
	case LexerExpr:
		lx.first = _STARTEXPR
	case LexerInclude:
	}
	lx.scanner.Error = func(_ *scanner.Scanner, msg string) { lx.Error(msg) }
	lx.scanner.Filename = lx.File
	lx.scanner.Init(lx.Body)
	lx.scanner.Whitespace &= ^uint64(1 << '\n')
}

var tokens = map[string]int{
	"intern":  _INTERN,
	"extern":  _EXTERN,
	"param":   _PARAM,
	"image":   _IMAGE,
	"groupby": _GROUPBY,
	"map":     _MAP,
	"collect": _COLLECT,
	"concat":  _CONCAT,
	"let":     _LET,
	"in":      _IN,
	"pullup":  _PULLUP,
}

// insertionToks defines the sets of tokens after which
// a semicolon may be inserted.
var insertionToks = map[rune]bool{
	scanner.Ident:     true,
	scanner.String:    true,
	scanner.RawString: true,
	scanner.Int:       true,
	scanner.Float:     true,
	scanner.Char:      true,
	')':               true,
	'}':               true,
	']':               true,
}

// Lex returns the next token.
func (lx *Lexer) Lex(yy *yySymType) (x int) {
	if lx.include != nil {
		tok := lx.include.Lex(yy)
		switch {
		case tok < 0:
			lx.include = nil
			lx.includeCloser.Close()
			return tok
		case tok == _EOF:
			lx.include = nil
			lx.includeCloser.Close()
			tok = lx.Lex(yy)
			if tok == ';' {
				return lx.Lex(yy)
			}
			return tok
		default:
			return tok
		}
	}
	if tok := lx.first; tok != 0 {
		lx.first = 0
		return tok
	}
	if lx.Mode != LexerExpr && lx.nl {
		// TODO(marius): fix pos here.
		lx.nl = false
		return ';'
	}
	last := lx.tok
	lx.tok, lx.text = lx.scanner.Scan(), lx.scanner.TokenText()
	switch lx.tok {
	case scanner.EOF:
		return _EOF
	case scanner.Ident:
		switch lx.text {
		case "include":
			if tok := lx.scanner.Scan(); tok != '(' {
				lx.Errorf("expected '(', got %v", tok)
				return -1
			}
			if tok := lx.scanner.Scan(); tok != scanner.String {
				lx.Errorf("expected string, got %v", tok)
				return -1
			}
			file := lx.scanner.TokenText()
			if tok := lx.scanner.Scan(); tok != ')' {
				lx.Errorf("expected ')', got %v", tok)
				return -1
			}
			file = file[1 : len(file)-1]
			dir := filepath.Dir(lx.File)
			file = filepath.Clean(filepath.Join(dir, file))
			f, err := os.Open(file)
			if err != nil {
				lx.Errorf("error opening include file %q: %v", file, err)
				return -1
			}
			lx.include = &Lexer{File: file, error: lx.error, Body: f, Mode: LexerInclude}
			lx.includeCloser = f
			lx.include.Init()
			return lx.Lex(yy)
		case "hash":
			if tok := lx.scanner.Scan(); tok != '(' {
				lx.Errorf("expected '(', got %v", tok)
				return -1
			}
			if tok := lx.scanner.Scan(); tok != scanner.String {
				lx.Errorf("expected string, got %v", tok)
				return -1
			}
			lx.HashVersion = lx.scanner.TokenText()
			lx.HashVersion = lx.HashVersion[1 : len(lx.HashVersion)-1]
			if tok := lx.scanner.Scan(); tok != ')' {
				lx.Errorf("expected ')', got %v", tok)
				return -1
			}
			return lx.Lex(yy)
		default:
			if tok, ok := tokens[lx.text]; ok {
				yy.pos = lx.scanner.Pos()
				return tok
			}
			yy.expr = &Expr{Position: lx.scanner.Pos(), op: opIdent, ident: lx.text}
			return _IDENT

		}
	case scanner.String, scanner.RawString:
		typ := typeString
		text := lx.text[1 : len(lx.text)-1]
		yy.expr = &Expr{Position: lx.scanner.Pos(), op: opConst, val: &Val{typ: typ, str: text}}
		return _EXPR
	case scanner.Int, scanner.Float, scanner.Char:
		lx.Error("integer, float, and char literals are currently unsupported")
		return 0
	case scanner.Comment:
		panic("comments should be ignored")
	case '\n':
		// Roughly follow Go's rules for semicolon insertion.
		if lx.Mode != LexerExpr && insertionToks[last] {
			return ';'
		}
		return lx.Lex(yy)
	case '{':
		// Scan a command. In order to support the use of '}' inside of a
		// command, we require that the '}' that terminates the command
		// itself is the last token on its line. This is an ugly hack, but
		// gets the job done. We'll replace this with something better
		// soon.
		//
		// TODO(marius): fix this.
		var s string
	Scan:
		for {
			tok := lx.scanner.Next()
			switch tok {
			case '}':
				buf := string(tok)
				for {
					tok := lx.scanner.Next()
					buf += string(tok)
					if tok == '\n' || tok == scanner.EOF {
						break Scan
					}
					if !unicode.IsSpace(tok) {
						s += buf
						break
					}
				}
			default:
				s += string(tok)
			case scanner.EOF:
				// Unexpected EOF.
				return _EOF
			}
		}
		yy.expr = &Expr{Position: lx.scanner.Pos(), op: opConst, val: &Val{typ: typeTemplate, str: s}}
		lx.nl = true
		return _TEMPLATE
	default:
		yy.pos = lx.scanner.Pos()
		return int(lx.tok)
	}
}

// Error reports an error to the lexer.
func (lx *Lexer) Error(s string) {
	lx.error.Errorf(lx.scanner.Pos(), "%s", s)
}

// Errorf formats and then reports an error to the lexer.
func (lx *Lexer) Errorf(format string, args ...interface{}) {
	lx.Error(fmt.Sprintf(format, args...))
}
