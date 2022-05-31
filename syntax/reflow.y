%{
package syntax 

import (
	"fmt"

	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
)

type posIdents struct {
	pos scanner.Position
	idents []string
	comments []string
}

type typearg struct {
	t1, t2 *types.T
}

%}

%union {
	pos struct {
		 scanner.Position
		 comment string
	}
	expr *Expr
	exprlist []*Expr
	exprfield *FieldExpr
	exprfields []*FieldExpr
	exprmap map[*Expr]*Expr
	
	comprclauses []*ComprClause
	comprclause *ComprClause
	
	typ *types.T
	typlist []*types.T
	typfield *types.Field
	typfields []*types.Field
	decl *Decl
	decllist []*Decl
	pat *Pat
	patlist []*Pat
	caseclause *CaseClause
	caseclauses []*CaseClause
	tok int
	template *Template
	
	structpat struct{
		field string
		pat *Pat
	}
	
	structpats []struct{
		field string
		pat *Pat
	}

	listpats struct{
		list []*Pat
		tail *Pat
	}
	
	typearg typearg
	typeargs []typearg

	variant *types.Variant
	variants []*types.Variant
	
	module *ModuleImpl
	
	str string
	
	idents []string
	posidents posIdents
}

%token	<expr>	tokIdent tokExpr tokInt tokString tokBool tokFloat
%token	<template>	tokTemplate
%token	<pos>	tokFile tokDir tokStruct tokModule tokExec tokAs  tokAt
%token	<pos>	tokVal tokFunc tokAssign tokArrow tokLeftArrow tokIf tokElse 
%token	<pos>	tokSwitch tokCase
%token	<pos>	tokMake
%token	<pos>	tokStartModule tokStartDecls tokStartExpr tokStartType tokStartPat
%token	<pos>	tokKeyspace tokParam  tokEllipsis  tokReserved  tokRequires
%token	<pos>	tokType
%token	<pos>	'{' '(' '['
%token	<pos>	tokOrOr tokAndAnd tokLE tokGE  tokNE tokEqEq tokLSH tokRSH
%token	<pos>	tokSquiggleArrow
%token	<pos>	'<' '>' '+' '-' '|' '^'  '*' '/' '%' '&' '_' '!' '#'
%token	tokEOF tokError 

%type	<decllist>		defs defs1 commadefs  paramdef paramdefs 
%type	<decl>		val valdef typedef def  commadef
%type	<expr>		expr  term  keyspace exprblock ifelseblock elseifexpr
%type	<expr>		switchexpr caseexpr caseexprblock
%type	<caseclauses>	caseclauses
%type	<caseclause>	caseclause
%type	<exprlist>	 listargs  listappendargs
%type	<exprmap>	mapargs
%type	<comprclauses>	comprclauses
%type	<comprclause>	comprclause
%type	<idents>	identSelector
%type	<typ>		type
%type	<typearg>		typearg
%type	<typeargs>	typearglist
%type	<typfields>	typeargs
%type	<typfields>	typefields  typefield   funcargs
%type	<variant> variant
%type	<variants> variants
%type	<idents>		typefieldidents
%type	<posidents>	idents
%type	<exprfield>	structfieldarg
%type	<exprfields>	structfieldargs applyargs tupleargs
%type	<module>	module
%type	<decllist>	params param 
%type	<pat>		pat listpattail
%type	<structpat>	structpat
%type 	<structpats>	structpatargs
%type	<patlist>		tuplepatargs patlist
%type	<listpats>	listpatargs

// Precedence as in Go.

%nonassoc first
%nonassoc tokElse
%left tokSquiggleArrow
%left tokOrOr
%left tokAndAnd
%left '<' '>' tokLE tokGE tokNE tokEqEq
%left '+' '-' '|' '^'
%left '*' '/' '%' '&' tokLSH tokRSH
%left unary
%right '.' '[' ']' '(' ')' '{' '}'
%nonassoc apply
%left deref

%%

start:
	tokStartModule module tokEOF
	{
		yylex.(*Parser).Module = $2
		return 0
	}
|	tokStartDecls defs tokEOF
	{
		yylex.(*Parser).Decls = $2
		return 0
	}
|	tokStartExpr expr tokEOF
	{
		yylex.(*Parser).Expr = $2
		return 0
	}
|	tokStartType type tokEOF
	{
		yylex.(*Parser).Type = $2
		return 0
	}
|	tokStartPat pat tokEOF
	{
		yylex.(*Parser).Pat = $2
		return 0
	}

// Types.

// identSelector admits only static field selectors, so that we may
// refer to types from other modules.
identSelector:
	tokIdent
	{$$ = []string{$1.Ident}}
|	identSelector '.' tokIdent
	{$$ = append($1, $3.Ident)}

type:
	tokInt	{$$ =  types.Int}
|	tokFloat	{$$ = types.Float}
|	tokString	{$$ = types.String}
|	tokBool	{$$ = types.Bool}
|	tokFile	{$$ = types.File}
|	tokDir	{$$ = types.Dir}
|	identSelector	{$$ = types.Ref($1...)}
| 	'[' type ']'	{$$ = types.List($2)}
| 	'[' type ':' type ']'
	{$$ = types.Map($2, $4)}
|	 '{' typefields '}'
	{$$ = types.Struct($2...)}
|	tokModule '{' typefields '}'
	{$$ = types.Module($3, nil)}
|	'(' typeargs ')'
	{
		switch len($2) {
		// "()" is unit
		case 0: $$ = types.Unit
		// "(type)" and "(name type)" get collapsed with
		// (optional) label
		case 1: $$ = types.Labeled($2[0].Name, $2[0].T)
		// a regular tuple must have at least two members
		default: $$ = types.Tuple($2...)
		}
	}
|	tokFunc '(' typeargs ')' type
	{$$ = types.Func($5, $3...)}
|	variants
	{$$ = types.Sum($1...)}

variants:
	variant
	{$$ = []*types.Variant{$1}}
|	variants '|' variant
	{$$ = append($1, $3)}

variant:
	'#' tokIdent '(' type ')'
	{$$ = &types.Variant{Tag: $2.Ident, Elem: $4}}
|	'#' tokIdent %prec first
	{$$ = &types.Variant{Tag: $2.Ident}}

typefieldidents:
	tokIdent
	{$$ = []string{$1.Ident}}
|	typefieldidents ',' tokIdent
	{$$ = append($1, $3.Ident)}

typefield:
	typefieldidents type
	{
		for _, name := range $1 {
			$$ = append($$, &types.Field{Name: name, T: $2})
		}
	}

typefields:
	typefield
	{$$ = $1}
|	typefields ',' typefield
	{$$ = append($1, $3...)}

typearg:
	type
	{$$ = typearg{$1, nil}}
|	type type
	{$$ = typearg{$1, $2}}

typearglist:
	typearg
	{$$ = []typearg{$1}}
|	typearglist ',' typearg
	{$$ = append($1, $3)}

// We can't parse typeargs directly because yacc can't disambiguate
// between reducing an anonymous type list (which can consist of
// identifiers) and an identifier list. So we do this bit of parsing
// manually here. However, since the scope is always limited (the
// list itself), we parse it manually here and disambiguate between
// these cases.
typeargs:	typearglist
	{
		var (
			fields []*types.Field
			group []*types.T
		)
		for _, arg := range $1 {
			group = append(group, arg.t1)
			if arg.t2 != nil {		// x, y, z t2
				// We have a group: check that they are all 
				// idents, and convert them accordingly.
				for _, id := range group {
					if id.Kind != types.RefKind {
						yylex.Error(fmt.Sprintf("expected identifier, found %s", id))
						goto Fail
					}
					if len(id.Path) != 1 {
						yylex.Error(fmt.Sprintf("non-simple argument name"))
						goto Fail
					}
					fields = append(fields, &types.Field{Name: id.Path[0], T: arg.t2})
				}
				group = nil
			}
		}
		if len(group) > 0 {
			if len(fields) > 0 {
				yylex.Error("cannot mix named and unnamed arguments")
				goto Fail
			}
			// Only unnamed arguments: they are all types.
			for _, t := range group {
				fields = append(fields, &types.Field{T: t})
			}
		}
		$$ = fields
	Fail:
	}

// Patterns.

pat:
	tokIdent
	{$$ = &Pat{Position: $1.Position, Kind: PatIdent, Ident: $1.Ident}}
|	'_'
	{$$ = &Pat{Position: $1.Position, Kind: PatIgnore}}
|	'(' tuplepatargs ')'
	{$$ = &Pat{Position: $1.Position, Kind: PatTuple, List: $2}}
|	'[' listpatargs ']'
	{$$ = &Pat{Position: $1.Position, Kind: PatList, List: $2.list, Tail: $2.tail}}
|	'{' structpatargs '}'
	{
		$$ = &Pat{Position: $1.Position, Kind: PatStruct, Fields: make([]PatField, len($2))}
		for i, p := range $2 {
			$$.Fields[i] = PatField{p.field, p.pat}
		}
	}
|	'#' tokIdent %prec first
	{$$ = &Pat{Position: $1.Position, Kind: PatVariant, Tag: $2.Ident}}
|	'#' tokIdent '(' pat ')'
	{$$ = &Pat{Position: $1.Position, Kind: PatVariant, Tag: $2.Ident, Elem: $4}}

listpatargs:
	patlist
	{$$ = struct{
		list []*Pat
		tail *Pat
	}{
		list: $1,
	}}
|	patlist ',' listpattail
	{$$ = struct{
		list []*Pat
		tail *Pat
	}{
		list: $1,
		tail: $3,
	}}

listpattail:
	tokEllipsis
	{$$ = &Pat{Position: $1.Position, Kind: PatIgnore}}
|	tokEllipsis pat
	{$$ = $2}

tuplepatargs:
	patlist

patlist:
	pat
	{$$ = []*Pat{$1}}
|	patlist ',' pat
	{$$ = append($1, $3)}

structpatargs:
	structpat
	{$$ = []struct{
		field string
		pat *Pat
	}{$1}}
|	structpatargs ',' structpat
	{$$ = append($1, $3)}

structpat:
	tokIdent
	{$$ = struct{
		field string
		pat *Pat
	}{$1.Ident, &Pat{Kind: PatIdent, Ident: $1.Ident}}}
|	tokIdent ':' pat
	{$$ = struct{
		field string
		pat *Pat
	}{$1.Ident, $3}}

// Declarations.

defs:
	{$$ = nil}
|	defs def ';'
	{$$ = append($1, $2)}
	
defs1:
	def ';'
	{$$ = []*Decl{$1}}
|	defs1 def  ';'
	{$$ = append($1, $2)}

commadefs:
	{$$ = nil}
|	commadef
	{$$ = []*Decl{$1}}
|	commadefs ',' commadef
	{$$ = append($1, $3)}

commadef: def
|	tokIdent
	{
		$$ = &Decl{
			Position: $1.Position, 
			Comment: $1.Comment, 
			Pat: &Pat{Position: $1.Position, Kind: PatIdent, Ident: $1.Ident}, 
			Kind: DeclAssign, 
			Expr: &Expr{Kind: ExprIdent, Ident: $1.Ident},
		}
	}

paramdefs:
	{$$ = nil}
|	paramdefs paramdef ';'
	{$$ = append($1, $2...)}

def: valdef | typedef
valdef:
	tokAt tokRequires '(' commadefs ')' semiOk valdef
	{
		$7.Expr = &Expr{Position: $7.Expr.Position, Kind: ExprRequires, Left: $7.Expr, Decls: $4}
		$7.Comment = $1.comment
		$$ = $7
	}
|	tokVal val
	{
		$$ = $2
		$$.Comment = $1.comment
	}
|	tokIdent tokAssign expr
	{$$ = &Decl{Position: $1.Position, Comment: $1.Comment, Pat: &Pat{Position: $1.Position, Kind: PatIdent, Ident: $1.Ident}, Kind: DeclAssign, Expr: $3}}
|	tokFunc tokIdent '(' funcargs ')' '=' expr
	{$$ = &Decl{Position: $1.Position, Comment: $1.comment, Pat: &Pat{Position: $1.Position, Kind: PatIdent, Ident: $2.Ident}, Kind: DeclAssign, Expr: &Expr{
		Kind: ExprFunc,
		Args: $4,
		Left: $7}}}
|	tokFunc tokIdent '(' funcargs ')' type '=' expr
	{$$ = &Decl{Position: $1.Position, Comment: $1.comment, Pat: &Pat{Position: $1.Position, Kind: PatIdent, Ident: $2.Ident}, Kind: DeclAssign, Expr: &Expr{
		Position: $1.Position,
		Kind: ExprAscribe,
		Type: types.Func($6, $4...),
		Left: &Expr{Kind: ExprFunc, Args: $4, Left: $8}}}}

typedef:
	tokType tokIdent type
	{$$ = &Decl{Position: $1.Position, Comment: $1.comment, Kind: DeclType, Ident: $2.Ident, Type: $3}}

val:
	pat '=' expr
	{$$ = &Decl{Position: $3.Position, Pat: $1, Kind: DeclAssign, Expr: $3}}
|	pat type '=' expr
	{$$ = &Decl{
		Position: $4.Position, 
		Pat: $1, 
		Kind: DeclAssign,
		Expr: &Expr{
			Position: $4.Position,
			Kind: ExprAscribe, 
			Type: $2,
			Left: $4,
		},
	}}

// At the moment, we only permit val defs in params.
// We do not permit func sugar. No patterns are permitted.
paramdef:
	idents type 
	{
		$$ = nil
		for i := range $1.idents {
			$$ = append($$, &Decl{
				Position: $1.pos, 
				Comment: $1.comments[i],
				Ident: $1.idents[i], 
				Kind: DeclDeclare, 
				Type: $2,
			})
		}
	}
|	idents '=' expr 
	{
		if len($1.idents) != 1 {
			$$ = []*Decl{{Kind: DeclError}}
		} else {
			$$ = []*Decl{{Position: $1.pos, Comment: $1.comments[0], Pat: &Pat{Position: $1.pos, Kind: PatIdent, Ident: $1.idents[0]}, Kind: DeclAssign, Expr: $3}}
		}
	}
|	idents type '=' expr 
	{
		if len($1.idents) != 1 {
			$$ = []*Decl{{Kind: DeclError}}
		} else {
			$$ = []*Decl{{
				Position: $1.pos, 
				Comment: $1.comments[0],
				Pat: &Pat{Position: $1.pos, Kind: PatIdent, Ident: $1.idents[0]}, 
				Kind: DeclAssign, 
				Expr: &Expr{Kind: ExprAscribe, Position: $1.pos, Type: $2, Left: $4},
			}}
		}
	}

idents:
	tokIdent 
	{$$ = posIdents{$1.Position, []string{$1.Ident}, []string{$1.Comment}}}
|	idents ',' tokIdent 
	{$$ = posIdents{$1.pos,  append($1.idents, $3.Ident), append($1.comments, $3.Comment)}}

// Expressions.

expr: term
|	expr tokOrOr expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "||", Left: $1, Right: $3}}
|	expr tokAndAnd expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "&&", Left: $1, Right: $3}}
|	expr '<' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "<", Left: $1, Right: $3}}
|	expr '>' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: ">", Left: $1, Right: $3}}
|	expr tokLE expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "<=", Left: $1, Right: $3}}
|	expr tokGE expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: ">=", Left: $1, Right: $3}}
|	expr tokNE expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "!=", Left: $1, Right: $3}}
|	expr tokEqEq expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "==", Left: $1, Right: $3}}
|	expr '+' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "+", Left: $1, Right: $3}}
|	expr '-' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "-", Left: $1, Right: $3}}
|	expr '*' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "*", Left: $1, Right: $3}}
|	expr '/' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "/", Left: $1, Right: $3}}
|	expr '%' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "%", Left: $1, Right: $3}}
|	expr '&' expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "&", Left: $1, Right: $3}}
|	expr tokLSH expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "<<", Left: $1, Right: $3}}
|	expr tokRSH expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: ">>", Left: $1, Right: $3}}
|	expr tokSquiggleArrow expr
	{$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "~>", Left: $1, Right: $3}}
|	tokIf expr ifelseblock elseifexpr
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprCond, Cond: $2, Left: $3, Right: $4}}
|	switchexpr
|	expr '[' expr ']'
	{$$ = &Expr{Position: $1.Position, Kind: ExprIndex, Left: $1, Right: $3}}
|	expr '(' applyargs commaOk ')'  
	{$$ = &Expr{Position: $1.Position, Kind: ExprApply, Left: $1, Fields: $3}}
|	expr '.' tokIdent   %prec deref
	{$$ = &Expr{Position: $1.Position, Kind: ExprDeref, Left: $1, Ident: $3.Ident}}
|	'!' expr %prec unary
	{$$ = &Expr{Position: $1.Position, Kind: ExprUnop, Op: "!", Left: $2}}
|	'-' expr %prec unary
	{$$ = &Expr{Position: $1.Position, Kind: ExprUnop, Op: "-", Left: $2}}

elseifexpr:
      tokElse ifelseblock
      {$$ = &Expr{Position: $1.Position, Kind: ExprBlock, Left: $2}}
|     tokElse tokIf expr ifelseblock elseifexpr
      {$$ = &Expr{Position: $1.Position, Kind: ExprCond, Cond: $3, Left: $4, Right: $5}}

term:
	tokExpr
|	tokIdent
	// Promote file and dir to idents in the expression grammar.
|	tokFile
	{$$ = &Expr{Position: $1.Position, Kind: ExprIdent, Ident: "file"}}
|	tokDir
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprIdent, Ident: "dir"}}
|	tokFunc '(' funcargs ')' tokArrow expr  %prec first
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprFunc, Args: $3, Left: $6}}
|	tokFunc '(' funcargs ')' type tokArrow expr %prec first
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprAscribe, Type: $5, Left: &Expr{
		Position: $7.Position, Kind: ExprFunc, Args: $3, Left: $7}}}
|	tokExec '(' commadefs ')' type tokTemplate
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprExec, Decls: $3, Type: $5, Template: $6}}
|	tokMake '(' tokExpr  ')'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprMake, Left: $3}}
|	tokMake '(' tokExpr ',' commadefs commaOk ')'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprMake, Left: $3, Decls: $5}}
|	'(' expr ',' tupleargs commaOk ')'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: $2}}, $4...)}}
|	 '{' structfieldargs commaOk '}'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprStruct, Fields: $2}}
|	'[' listargs commaOk ']'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprList, List: $2}}
|	'[' listargs commaOk listappendargs commaOk ']'
	{
		$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprList, List: $2}
		for _, list := range $4 {
			$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "+", Left: $$, Right: list}
		}
	}
|	'[' ':' ']'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprMap}}
|	'[' mapargs commaOk ']'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprMap, Map: $2}}
|	'[' mapargs commaOk listappendargs commaOk ']'
	{
		$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprMap, Map: $2}
		for _, list := range $4 {
			$$ = &Expr{Position: $1.Position, Kind: ExprBinop, Op: "+", Left: list, Right: $$}
		}
	}
|	'[' expr '|' comprclauses ']'
	{
		$$ = &Expr{
			Position: $1.Position, 
			Comment: $1.comment, 
			Kind: ExprCompr, 
			ComprExpr: $2, 
			ComprClauses: $4,
		}
	}
|	'#' tokIdent %prec first
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprVariant, Ident: $2.Ident}}
|	'#' tokIdent '(' expr ')'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprVariant, Ident: $2.Ident, Left: $4}}
|	'(' expr ')'
	{$$ = $2}
|	exprblock
|       tokInt '(' expr ')'
	{$$ = &Expr{Position: $1.Position, Comment: $1.Comment, Kind: ExprBuiltin, Op: "int", Fields: []*FieldExpr{{Expr:$3}}}}
|       tokFloat '(' expr ')'
	{$$ = &Expr{Position: $1.Position, Comment: $1.Comment, Kind: ExprBuiltin, Op: "float", Fields: []*FieldExpr{{Expr:$3}}}}

exprblock:
	'{' defs1 expr maybeColon'}'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment,  Kind: ExprBlock, Decls: $2, Left: $3}}

ifelseblock:
	'{' defs expr maybeColon '}'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment,  Kind: ExprBlock, Decls: $2, Left: $3}}

switchexpr:
	tokSwitch expr '{' caseclauses '}'
	{$$ = &Expr{Position: $1.Position, Comment: $1.comment, Kind: ExprSwitch, Left: $2, CaseClauses: $4}}

caseclauses:
	{$$ = nil}
|	caseclauses caseclause
	{$$ = append($1, $2)}

caseclause:
	tokCase pat ':' caseexpr maybeColon
	{$$ = &CaseClause{Position: $1.Position, Comment: $1.comment, Pat: $2, Expr: $4}}

caseexpr: expr | caseexprblock

caseexprblock:
	defs1 expr
	{$$ = &Expr{Kind: ExprBlock, Decls: $1, Left: $2}}

comprclauses:
	comprclause
	{$$ = []*ComprClause{$1}}
|	comprclauses ',' comprclause
	{$$ = append($1, $3)}

comprclause:
	pat tokLeftArrow expr
	{$$ = &ComprClause{Kind: ComprEnum, Pat: $1, Expr: $3}}
|	tokIf expr
	{$$ = &ComprClause{Kind: ComprFilter, Expr: $2}}

maybeColon:
|	';' 

structfieldargs:
	structfieldarg
	{$$ = []*FieldExpr{$1}}
|	structfieldargs ',' structfieldarg
	{$$ = append($1, $3)}

structfieldarg:
	tokIdent
	{$$ = &FieldExpr{Name: $1.Ident, Expr: &Expr{Position: $1.Position, Kind: ExprIdent, Ident: $1.Ident}}}
|	tokIdent ':' expr
	{$$ = &FieldExpr{Name: $1.Ident, Expr: $3}}

listargs:
	{$$ = nil}	// empty
|	expr
	{$$ = []*Expr{$1}}
|	listargs ',' expr
	{$$ = append($1, $3)}

listappendargs:
	tokEllipsis expr semiOk
	{$$ = []*Expr{$2}}
|	listappendargs tokEllipsis expr semiOk
	{$$ = append($1, $3)}

tupleargs:
	expr
	{$$ = []*FieldExpr{{Expr: $1}}}
|	tupleargs ',' expr
	{$$ = append($1, &FieldExpr{Expr: $3})}

applyargs:
	expr
	{$$ = []*FieldExpr{{Expr: $1}}}
|	applyargs ',' expr
	{$$ = append($1, &FieldExpr{Expr: $3})}

mapargs:
	expr ':' expr
	{$$ = map[*Expr]*Expr{$1: $3}}
|	mapargs ',' expr ':' expr
	{
		$$ = $1
		$$[$3] = $5
	}

funcargs: typefields

module:
	keyspace
	params
	defs
	{$$ = &ModuleImpl{Keyspace: $1, ParamDecls: $2, Decls: $3}}

keyspace:
	{$$ = nil}
|	tokKeyspace tokExpr
	{$$ = $2}

params:
	{$$ = nil}
|	params param ';'
	{$$ = append($1, $2...)}

param:
	{$$ = nil}
|	tokParam paramdef
	{
		$$ = $2
		for _, d := range $$ {
			if d.Comment == "" {
				d.Comment = $1.comment
			}
		}
	}
| 	tokParam '(' paramdefs ')'
	{$$ = $3}

commaOk:
|	','

semiOk:
|	';'
