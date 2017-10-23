package syntax

import (
	"bytes"
	"fmt"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
	"grail.com/reflow/syntax/scanner"
)

// DeclKind is the type of declaration.
type DeclKind int

const (
	// DeclError is an illegal declaration.
	DeclError DeclKind = iota
	// DeclAssign is an assignment declaration (rvalue expression).
	DeclAssign
	// DeclDeclare is a "pure" declaration (type only).
	DeclDeclare
	// DeclType declares a type alias
	DeclType
)

// A Decl is a Reflow declaration.
type Decl struct {
	// Position contains the source position of the node.
	// It is set by the parser.
	scanner.Position
	// Comment stores an optional comment attached to the declaration.
	Comment string
	// Kind is the Decl's op; see above.
	Kind DeclKind
	// Ident is the identifier in a DeclDeclare and DeclType.
	Ident string
	// Pat stores the pattern for this declaration.
	Pat *Pat
	// Expr stores the rvalue expression for this declaration.
	Expr *Expr
	// Type stores the type for DeclDeclare and DeclType.
	Type *types.T
}

// ID returns the best identifier for this declaration, or else id.
func (d *Decl) ID(id string) string {
	switch d.Kind {
	case DeclDeclare, DeclType:
		if id != "" {
			return id + "." + d.Ident
		}
		return d.Ident
	case DeclAssign:
		if d.Pat.Kind == PatIdent {
			if id != "" {
				return id + "." + d.Ident + d.Pat.Ident
			}
			return d.Ident + d.Pat.Ident
		}
	}
	return id
}

// Init type checks this declaration. Type errors are returned.
func (d *Decl) Init(sess *Session, env *types.Env) error {
	d.init(sess, env)
	if d.Type == nil {
		panic("d type is nil")
	}
	d.Type = expand(d.Type, env)
	if d.Expr != nil {
		return d.Expr.err()
	}
	return nil
}

func (d *Decl) init(sess *Session, env *types.Env) {
	switch d.Kind {
	case DeclError:
		d.Type = typeError
		return
	case DeclDeclare, DeclType:
		return
	case DeclAssign:
		d.Expr.init(sess, env)
		d.Type = d.Expr.Type.Assign(nil)
	}
}

// Eval evaluates this declaration with the given environment and session.
func (d *Decl) Eval(sess *Session, env *values.Env, ident string) (values.T, error) {
	return d.Expr.eval(sess, env, d.ID(ident))
}

// Equal tests whether Decl d is equivalent to Decl e.
func (d *Decl) Equal(e *Decl) bool {
	if d.Kind == DeclError {
		return false
	}
	if d.Kind != e.Kind {
		return false
	}
	if !d.Pat.Equal(e.Pat) {
		return false
	}
	switch d.Kind {
	default:
		panic("error")
	case DeclAssign:
		return d.Expr.Equal(e.Expr)
	case DeclDeclare, DeclType:
		return d.Type.Equal(e.Type)
	}
}

// String renders a tree-formatted version of d.
func (d *Decl) String() string {
	b := new(bytes.Buffer)
	if d.Type != nil {
		b.WriteString("<" + d.Type.String() + ">")
	}
	switch d.Kind {
	default:
		panic("error")
	case DeclError:
		b.WriteString("error")
	case DeclAssign:
		fmt.Fprintf(b, "assign(%s, %s)", d.Pat, d.Expr)
	case DeclDeclare:
		fmt.Fprintf(b, "declare(%s, %s)", d.Ident, d.Type)
	case DeclType:
		fmt.Fprintf(b, "type(%s %s)", d.Ident, d.Type)
	}
	return b.String()
}
