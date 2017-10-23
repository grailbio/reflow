%{
package lang

import "text/scanner"
%}

%union {
	pos scanner.Position
	expr *Expr
	exprlist []*Expr
	stmt *Stmt
	stmtlist []*Stmt
	tok int
	op Op
}

%left _SHIFT
%left		'!'
%right	_IN

// TODO(marius): rename to tokXXX.

%token	<expr>	_IDENT _EXPR _BLOCK _TEMPLATE
%token	<pos>	_IN _LET _INTERN  _EXTERN _PARAM _IMAGE _GROUPBY _MAP _COLLECT _CONCAT _PULLUP '=' '(' '[' '{'
%token	<pos>	_STARTTOP _STARTEXPR

%type	<stmtlist>	flow
%type	<stmt>	stmt
%type	<expr>	expr
%type	<exprlist>	exprlist identlist _EXPRlist

%token _EOF

%%

top:
	_STARTTOP flow _EOF
	{
		yylex.(*Lexer).Stmts = $2
		return 0
	}
|	_STARTEXPR expr _EOF
	{
		yylex.(*Lexer).Expr = $2
		return 0
	}

flow:
	{$$ = nil}
|	';'
	{$$ = nil}
|	flow stmt ';'
	{$$ = append($1, $2)}

stmt:
	_IDENT '=' expr
	{$$ = &Stmt{Position: $1.Position, left: $1, op: opDef, right: $3}}
|	_IDENT '(' identlist ')' '=' expr
	// This is syntactic sugar for ident = { (...) => .. }
	{$$ = &Stmt{Position: $1.Position, left: $1, op: opDef, right: &Expr{Position: $1.Position, list: $3, op: opFunc, right: $6 }}}

	// TODO(marius)_: lex '...' as a single token.
|	_IDENT '(' _IDENT '.' '.' '.' ')' '=' expr
	{$$ = &Stmt{Position: $1.Position, left: $1, op: opDef, right: &Expr{Position: $1.Position, list: []*Expr{$3}, op: opVarfunc, right: $9}}}
|	_EXTERN '(' expr ',' expr ')'
	{$$ = &Stmt{Position: $1, op: opExtern, list: []*Expr{$3, $5}}}

expr:
	'(' expr ')'
	{$$ = $2}
|	_LET _IDENT '=' expr _IN expr
	{$$ = &Expr{Position: $1, op: opLet, left: $2, list: []*Expr{$4}, right: $6}}
|	_LET _IDENT '(' identlist ')' '=' expr _IN expr
	// This is syntactic sugar for let ident = { (...) => .. } in
	{$$ = &Expr{Position: $1, op: opLet, left: $2, list: []*Expr{{Position: $2.Position, list: $4, op: opFunc, right: $7}}, right: $9}}
|	'(' identlist ')' '=' '>' expr
	{$$ = &Expr{Position: $1, op: opFunc, list: $2, right: $6}}
|	expr '(' exprlist ')'
	{$$ = &Expr{Position: $1.Position, op: opApply, left: $1, list: $3}}
|	_IMAGE '(' expr ')'
	{$$ = &Expr{Position: $1, op: opImage, left: $3}}
|	_INTERN '(' expr ')'
	{$$ = &Expr{Position: $1, op: opIntern, left: $3}}
|	_PARAM '(' _EXPR ',' _EXPR ')'
	{$$ = &Expr{Position: $1, op: opParam, list: []*Expr{$3, $5}}}
|	_GROUPBY '(' expr ',' expr ')'
	{$$ = &Expr{Position: $1, op: opGroupby, list: []*Expr{$3, $5}}}
|	_CONCAT '(' exprlist ')'
	{$$ = &Expr{Position: $1, op: opConcat, list: $3}}
|	_PULLUP '(' exprlist ')'
	{$$ = &Expr{Position: $1, op: opPullup, list: $3}}
|	_MAP '(' expr ',' expr ')'
	{$$ = &Expr{Position: $1, op: opMap, list: []*Expr{$3, $5}}}
|	_COLLECT '(' expr ',' expr ')'
	{$$ = &Expr{Position: $1, op: opCollect, list: []*Expr{$3, $5}}}
|	_COLLECT '(' expr ',' expr  ',' expr')'
	{$$ = &Expr{Position: $1, op: opCollect, list: []*Expr{$3, $5, $7}}}
|	expr '[' _EXPRlist ']' _TEMPLATE  
	{$$ = &Expr{Position: $1.Position, op: opExec, left: $1, list: $3, right: $5}}
|	expr _TEMPLATE  
	{$$ = &Expr{Position: $1.Position, op: opExec, left: $1, right: $2}}
|	_IDENT
|	_EXPR

identlist:
	_IDENT
	{$$ = []*Expr{$1}}
|	identlist ',' _IDENT
	{$$ = append($1, $3)}

_EXPRlist:
	_EXPR
	{$$ = []*Expr{$1}}
|	_EXPRlist ',' _EXPR
	{$$ = append($1, $3)}

exprlist:
	expr
	{$$ = []*Expr{$1}}
|	exprlist ',' expr
	{$$ = append($1, $3)}

