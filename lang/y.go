// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

//line reflow.y:2
package lang

import __yyfmt__ "fmt"

//line reflow.y:2
import "text/scanner"

//line reflow.y:7
type yySymType struct {
	yys      int
	pos      scanner.Position
	expr     *Expr
	exprlist []*Expr
	stmt     *Stmt
	stmtlist []*Stmt
	tok      int
	op       Op
}

const _SHIFT = 57346
const _IN = 57347
const _IDENT = 57348
const _EXPR = 57349
const _BLOCK = 57350
const _TEMPLATE = 57351
const _LET = 57352
const _INTERN = 57353
const _EXTERN = 57354
const _PARAM = 57355
const _IMAGE = 57356
const _GROUPBY = 57357
const _MAP = 57358
const _COLLECT = 57359
const _CONCAT = 57360
const _PULLUP = 57361
const _STARTTOP = 57362
const _STARTEXPR = 57363
const _EOF = 57364

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"_SHIFT",
	"'!'",
	"_IN",
	"_IDENT",
	"_EXPR",
	"_BLOCK",
	"_TEMPLATE",
	"_LET",
	"_INTERN",
	"_EXTERN",
	"_PARAM",
	"_IMAGE",
	"_GROUPBY",
	"_MAP",
	"_COLLECT",
	"_CONCAT",
	"_PULLUP",
	"'='",
	"'('",
	"'['",
	"'{'",
	"_STARTTOP",
	"_STARTEXPR",
	"_EOF",
	"';'",
	"')'",
	"'.'",
	"','",
	"'>'",
	"']'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 29,
	31, 28,
	-2, 26,
}

const yyNprod = 34
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 180

var yyAct = [...]int{

	44, 26, 28, 26, 6, 26, 87, 75, 27, 43,
	67, 106, 66, 24, 25, 24, 25, 24, 25, 89,
	103, 49, 104, 81, 83, 49, 80, 78, 77, 65,
	65, 95, 52, 53, 82, 55, 26, 110, 58, 59,
	26, 60, 100, 63, 61, 56, 57, 39, 24, 25,
	5, 70, 24, 25, 71, 42, 64, 79, 65, 38,
	48, 76, 49, 17, 18, 37, 84, 8, 10, 36,
	11, 9, 12, 15, 16, 13, 14, 91, 7, 26,
	92, 93, 2, 3, 96, 50, 51, 35, 97, 98,
	26, 24, 25, 34, 26, 105, 40, 41, 112, 33,
	108, 32, 24, 25, 31, 109, 24, 25, 113, 107,
	99, 94, 114, 102, 115, 29, 18, 68, 90, 8,
	10, 26, 11, 9, 12, 15, 16, 13, 14, 26,
	7, 26, 85, 24, 25, 86, 54, 1, 46, 26,
	101, 24, 25, 24, 25, 72, 69, 26, 74, 21,
	73, 24, 25, 62, 30, 22, 45, 111, 47, 24,
	25, 26, 88, 26, 23, 20, 26, 4, 0, 19,
	0, 0, 0, 24, 25, 24, 25, 0, 24, 25,
}
var yyPact = [...]int{

	57, -1000, 22, 56, 142, -1000, 137, 108, 147, 82,
	79, 77, 71, 65, 47, 43, 37, -1000, -1000, -1000,
	19, 75, 33, -1000, 56, 130, -1000, 129, 31, -1000,
	64, 56, 56, 128, 56, 56, 56, 56, 56, -1000,
	56, 146, 56, 27, 153, -21, -1000, -1000, 96, 139,
	56, 138, 121, 119, -24, 30, -1, -2, 26, -5,
	153, -6, 4, -7, -1000, 56, 122, 127, -26, -1000,
	156, -10, -1000, -1000, -1000, 110, 56, -1000, -1000, 56,
	56, 90, 1, 56, 153, -1000, -1000, 56, 56, 89,
	13, 111, 84, -9, 56, -19, 80, 153, 153, 56,
	-1000, -1000, -1000, -1000, 56, 153, 8, -1000, 151, 69,
	87, 56, -1000, 56, 153, 153,
}
var yyPgo = [...]int{

	0, 167, 165, 0, 9, 2, 156, 137,
}
var yyR1 = [...]int{

	0, 7, 7, 1, 1, 1, 2, 2, 2, 2,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 5, 5,
	6, 6, 4, 4,
}
var yyR2 = [...]int{

	0, 3, 3, 0, 1, 3, 3, 6, 9, 6,
	3, 6, 9, 6, 4, 4, 4, 6, 6, 4,
	4, 6, 6, 8, 5, 2, 1, 1, 1, 3,
	1, 3, 1, 3,
}
var yyChk = [...]int{

	-1000, -7, 25, 26, -1, 28, -3, 22, 11, 15,
	12, 14, 16, 19, 20, 17, 18, 7, 8, 27,
	-2, 7, 13, 27, 22, 23, 10, -3, -5, 7,
	7, 22, 22, 22, 22, 22, 22, 22, 22, 28,
	21, 22, 22, -4, -3, -6, 8, 29, 29, 31,
	21, 22, -3, -3, 8, -3, -4, -4, -3, -3,
	-3, -5, 7, -3, 29, 31, 33, 31, 21, 7,
	-3, -5, 7, 29, 29, 31, 31, 29, 29, 31,
	31, 29, 30, 31, -3, 10, 8, 32, 6, 29,
	8, -3, -3, -3, 21, 30, -3, -3, -3, 21,
	29, 29, 29, 29, 31, -3, 30, 29, -3, -3,
	29, 6, 29, 21, -3, -3,
}
var yyDef = [...]int{

	0, -2, 3, 0, 0, 4, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 26, 27, 1,
	0, 0, 0, 2, 0, 0, 25, 0, 0, -2,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 5,
	0, 0, 0, 0, 32, 0, 30, 10, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	6, 0, 28, 0, 14, 0, 0, 0, 0, 29,
	0, 0, 28, 15, 16, 0, 0, 19, 20, 0,
	0, 0, 0, 0, 33, 24, 31, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 13, 11, 0,
	17, 18, 21, 22, 0, 7, 0, 9, 0, 0,
	0, 0, 23, 0, 12, 8,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 5, 3, 3, 3, 3, 3, 3,
	22, 29, 3, 3, 31, 3, 30, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 28,
	3, 21, 32, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 23, 3, 33, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 24,
}
var yyTok2 = [...]int{

	2, 3, 4, 6, 7, 8, 9, 10, 11, 12,
	13, 14, 15, 16, 17, 18, 19, 20, 25, 26,
	27,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:38
		{
			yylex.(*Lexer).Stmts = yyDollar[2].stmtlist
			return 0
		}
	case 2:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:43
		{
			yylex.(*Lexer).Expr = yyDollar[2].expr
			return 0
		}
	case 3:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:49
		{
			yyVAL.stmtlist = nil
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:51
		{
			yyVAL.stmtlist = nil
		}
	case 5:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:53
		{
			yyVAL.stmtlist = append(yyDollar[1].stmtlist, yyDollar[2].stmt)
		}
	case 6:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:57
		{
			yyVAL.stmt = &Stmt{Position: yyDollar[1].expr.Position, left: yyDollar[1].expr, op: opDef, right: yyDollar[3].expr}
		}
	case 7:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:60
		{
			yyVAL.stmt = &Stmt{Position: yyDollar[1].expr.Position, left: yyDollar[1].expr, op: opDef, right: &Expr{Position: yyDollar[1].expr.Position, list: yyDollar[3].exprlist, op: opFunc, right: yyDollar[6].expr}}
		}
	case 8:
		yyDollar = yyS[yypt-9 : yypt+1]
		//line reflow.y:64
		{
			yyVAL.stmt = &Stmt{Position: yyDollar[1].expr.Position, left: yyDollar[1].expr, op: opDef, right: &Expr{Position: yyDollar[1].expr.Position, list: []*Expr{yyDollar[3].expr}, op: opVarfunc, right: yyDollar[9].expr}}
		}
	case 9:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:66
		{
			yyVAL.stmt = &Stmt{Position: yyDollar[1].pos, op: opExtern, list: []*Expr{yyDollar[3].expr, yyDollar[5].expr}}
		}
	case 10:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:70
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 11:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:72
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opLet, left: yyDollar[2].expr, list: []*Expr{yyDollar[4].expr}, right: yyDollar[6].expr}
		}
	case 12:
		yyDollar = yyS[yypt-9 : yypt+1]
		//line reflow.y:75
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opLet, left: yyDollar[2].expr, list: []*Expr{{Position: yyDollar[2].expr.Position, list: yyDollar[4].exprlist, op: opFunc, right: yyDollar[7].expr}}, right: yyDollar[9].expr}
		}
	case 13:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:77
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opFunc, list: yyDollar[2].exprlist, right: yyDollar[6].expr}
		}
	case 14:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:79
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, op: opApply, left: yyDollar[1].expr, list: yyDollar[3].exprlist}
		}
	case 15:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:81
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opImage, left: yyDollar[3].expr}
		}
	case 16:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:83
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opIntern, left: yyDollar[3].expr}
		}
	case 17:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:85
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opParam, list: []*Expr{yyDollar[3].expr, yyDollar[5].expr}}
		}
	case 18:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:87
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opGroupby, list: []*Expr{yyDollar[3].expr, yyDollar[5].expr}}
		}
	case 19:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:89
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opConcat, list: yyDollar[3].exprlist}
		}
	case 20:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:91
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opPullup, list: yyDollar[3].exprlist}
		}
	case 21:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:93
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opMap, list: []*Expr{yyDollar[3].expr, yyDollar[5].expr}}
		}
	case 22:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:95
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opCollect, list: []*Expr{yyDollar[3].expr, yyDollar[5].expr}}
		}
	case 23:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:97
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos, op: opCollect, list: []*Expr{yyDollar[3].expr, yyDollar[5].expr, yyDollar[7].expr}}
		}
	case 24:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:99
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, op: opExec, left: yyDollar[1].expr, list: yyDollar[3].exprlist, right: yyDollar[5].expr}
		}
	case 25:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:101
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, op: opExec, left: yyDollar[1].expr, right: yyDollar[2].expr}
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:107
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 29:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:109
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:113
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:115
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:119
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:121
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	}
	goto yystack /* stack new state and value */
}
