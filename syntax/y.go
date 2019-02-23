//line reflow.y:2
package syntax

import __yyfmt__ "fmt"

//line reflow.y:2
import (
	"fmt"

	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
)

type posIdents struct {
	pos      scanner.Position
	idents   []string
	comments []string
}

type typearg struct {
	t1, t2 *types.T
}

//line reflow.y:23
type yySymType struct {
	yys int
	pos struct {
		scanner.Position
		comment string
	}
	expr       *Expr
	exprlist   []*Expr
	exprfield  *FieldExpr
	exprfields []*FieldExpr
	exprmap    map[*Expr]*Expr

	comprclauses []*ComprClause
	comprclause  *ComprClause

	typ         *types.T
	typlist     []*types.T
	typfield    *types.Field
	typfields   []*types.Field
	decl        *Decl
	decllist    []*Decl
	pat         *Pat
	patlist     []*Pat
	caseclause  *CaseClause
	caseclauses []*CaseClause
	tok         int
	template    *Template

	structpat struct {
		field string
		pat   *Pat
	}

	structpats []struct {
		field string
		pat   *Pat
	}

	listpats struct {
		list []*Pat
		tail *Pat
	}

	typearg  typearg
	typeargs []typearg

	module *ModuleImpl

	str string

	idents    []string
	posidents posIdents
}

const tokIdent = 57346
const tokExpr = 57347
const tokInt = 57348
const tokString = 57349
const tokBool = 57350
const tokFloat = 57351
const tokTemplate = 57352
const tokFile = 57353
const tokDir = 57354
const tokStruct = 57355
const tokModule = 57356
const tokExec = 57357
const tokAs = 57358
const tokAt = 57359
const tokVal = 57360
const tokFunc = 57361
const tokAssign = 57362
const tokArrow = 57363
const tokLeftArrow = 57364
const tokIf = 57365
const tokElse = 57366
const tokSwitch = 57367
const tokCase = 57368
const tokMake = 57369
const tokStartModule = 57370
const tokStartDecls = 57371
const tokStartExpr = 57372
const tokStartType = 57373
const tokStartPat = 57374
const tokKeyspace = 57375
const tokParam = 57376
const tokEllipsis = 57377
const tokReserved = 57378
const tokRequires = 57379
const tokType = 57380
const tokOrOr = 57381
const tokAndAnd = 57382
const tokLE = 57383
const tokGE = 57384
const tokNE = 57385
const tokEqEq = 57386
const tokLSH = 57387
const tokRSH = 57388
const tokSquiggleArrow = 57389
const tokEOF = 57390
const tokError = 57391
const first = 57392
const unary = 57393
const apply = 57394
const deref = 57395

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"tokIdent",
	"tokExpr",
	"tokInt",
	"tokString",
	"tokBool",
	"tokFloat",
	"tokTemplate",
	"tokFile",
	"tokDir",
	"tokStruct",
	"tokModule",
	"tokExec",
	"tokAs",
	"tokAt",
	"tokVal",
	"tokFunc",
	"tokAssign",
	"tokArrow",
	"tokLeftArrow",
	"tokIf",
	"tokElse",
	"tokSwitch",
	"tokCase",
	"tokMake",
	"tokStartModule",
	"tokStartDecls",
	"tokStartExpr",
	"tokStartType",
	"tokStartPat",
	"tokKeyspace",
	"tokParam",
	"tokEllipsis",
	"tokReserved",
	"tokRequires",
	"tokType",
	"'{'",
	"'('",
	"'['",
	"tokOrOr",
	"tokAndAnd",
	"tokLE",
	"tokGE",
	"tokNE",
	"tokEqEq",
	"tokLSH",
	"tokRSH",
	"tokSquiggleArrow",
	"'<'",
	"'>'",
	"'+'",
	"'-'",
	"'|'",
	"'^'",
	"'*'",
	"'/'",
	"'%'",
	"'&'",
	"'_'",
	"'!'",
	"tokEOF",
	"tokError",
	"first",
	"unary",
	"'.'",
	"']'",
	"')'",
	"apply",
	"deref",
	"':'",
	"'}'",
	"','",
	"';'",
	"'='",
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
	-1, 52,
	75, 157,
	-2, 47,
}

const yyPrivate = 57344

const yyLast = 1081

var yyAct = [...]int{

	11, 92, 162, 218, 232, 56, 114, 317, 161, 156,
	243, 158, 31, 55, 84, 167, 85, 86, 205, 123,
	113, 236, 160, 107, 93, 90, 111, 99, 94, 337,
	300, 102, 10, 267, 233, 318, 119, 324, 231, 303,
	204, 288, 173, 45, 222, 159, 105, 289, 283, 253,
	189, 224, 223, 201, 202, 311, 225, 222, 188, 189,
	330, 128, 280, 189, 294, 135, 136, 137, 138, 139,
	140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
	150, 151, 152, 154, 217, 125, 106, 284, 200, 198,
	82, 81, 194, 170, 179, 169, 131, 176, 78, 79,
	168, 182, 183, 72, 73, 281, 228, 74, 75, 76,
	77, 292, 175, 132, 178, 191, 186, 83, 334, 174,
	187, 195, 44, 203, 32, 34, 35, 33, 55, 36,
	37, 208, 41, 212, 332, 82, 81, 43, 211, 313,
	305, 214, 196, 78, 79, 269, 255, 221, 237, 197,
	237, 193, 74, 75, 76, 77, 192, 40, 42, 39,
	82, 81, 83, 133, 319, 174, 227, 315, 295, 216,
	199, 177, 104, 116, 103, 234, 118, 238, 213, 51,
	241, 239, 246, 235, 208, 272, 209, 83, 115, 101,
	220, 100, 190, 229, 250, 89, 299, 88, 87, 87,
	240, 157, 110, 129, 2, 3, 4, 5, 6, 127,
	9, 266, 219, 251, 262, 254, 290, 244, 270, 309,
	206, 273, 259, 55, 132, 268, 121, 310, 275, 271,
	277, 276, 265, 164, 53, 256, 302, 256, 285, 109,
	260, 124, 279, 252, 230, 60, 291, 185, 278, 155,
	46, 134, 282, 133, 1, 120, 286, 60, 58, 59,
	61, 46, 296, 117, 301, 122, 257, 126, 304, 245,
	58, 59, 61, 306, 52, 308, 7, 226, 307, 62,
	298, 314, 153, 91, 316, 50, 48, 49, 320, 207,
	108, 322, 258, 112, 312, 297, 50, 48, 49, 38,
	321, 325, 242, 98, 54, 46, 96, 47, 293, 249,
	331, 326, 328, 341, 339, 14, 329, 27, 47, 8,
	276, 12, 57, 130, 333, 261, 244, 0, 336, 0,
	0, 323, 335, 338, 0, 340, 342, 94, 343, 0,
	50, 48, 49, 345, 344, 169, 82, 81, 64, 65,
	68, 69, 70, 71, 78, 79, 80, 66, 67, 72,
	73, 0, 47, 74, 75, 76, 77, 0, 0, 0,
	0, 0, 0, 83, 0, 0, 0, 0, 0, 0,
	0, 318, 82, 81, 64, 65, 68, 69, 70, 71,
	78, 79, 80, 66, 67, 72, 73, 0, 0, 74,
	75, 76, 77, 0, 0, 0, 0, 0, 0, 83,
	0, 0, 0, 0, 0, 0, 0, 233, 82, 81,
	64, 65, 68, 69, 70, 71, 78, 79, 80, 66,
	67, 72, 73, 0, 0, 74, 75, 76, 77, 0,
	0, 0, 0, 0, 0, 83, 0, 166, 0, 0,
	0, 0, 165, 82, 81, 64, 65, 68, 69, 70,
	71, 78, 79, 80, 66, 67, 72, 73, 180, 0,
	74, 75, 76, 77, 0, 0, 0, 0, 0, 0,
	83, 0, 0, 0, 0, 181, 82, 81, 64, 65,
	68, 69, 70, 71, 78, 79, 80, 66, 67, 72,
	73, 0, 0, 74, 75, 76, 77, 44, 0, 32,
	34, 35, 33, 83, 36, 37, 0, 41, 287, 0,
	0, 0, 43, 0, 0, 0, 163, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 58,
	59, 61, 40, 42, 39, 82, 81, 64, 65, 68,
	69, 70, 71, 78, 79, 80, 66, 67, 72, 73,
	62, 0, 74, 75, 76, 77, 0, 0, 0, 0,
	0, 0, 83, 0, 248, 0, 0, 264, 0, 263,
	82, 81, 64, 65, 68, 69, 70, 71, 78, 79,
	80, 66, 67, 72, 73, 0, 0, 74, 75, 76,
	77, 0, 0, 0, 0, 0, 60, 83, 44, 247,
	32, 34, 35, 33, 0, 36, 37, 0, 41, 58,
	59, 61, 0, 43, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	62, 0, 0, 40, 42, 39, 82, 81, 64, 65,
	68, 69, 70, 71, 78, 79, 80, 66, 67, 72,
	73, 0, 95, 74, 75, 76, 77, 0, 0, 0,
	0, 0, 0, 83, 215, 58, 59, 61, 0, 44,
	327, 32, 34, 35, 33, 0, 36, 37, 0, 41,
	18, 17, 28, 0, 43, 29, 62, 19, 20, 0,
	0, 22, 0, 0, 0, 21, 0, 0, 0, 13,
	0, 30, 0, 23, 40, 42, 39, 0, 0, 0,
	0, 0, 0, 0, 0, 25, 24, 26, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	16, 0, 0, 0, 0, 0, 0, 0, 15, 0,
	0, 210, 0, 0, 0, 0, 0, 0, 97, 157,
	82, 81, 64, 65, 68, 69, 70, 71, 78, 79,
	80, 66, 67, 72, 73, 0, 0, 74, 75, 76,
	77, 0, 0, 0, 0, 0, 0, 83, 184, 82,
	81, 64, 65, 68, 69, 70, 71, 78, 79, 80,
	66, 67, 72, 73, 0, 0, 74, 75, 76, 77,
	0, 0, 0, 0, 0, 0, 83, 82, 81, 64,
	65, 68, 69, 70, 71, 78, 79, 80, 66, 67,
	72, 73, 0, 0, 74, 75, 76, 77, 0, 0,
	63, 0, 0, 0, 83, 82, 81, 64, 65, 68,
	69, 70, 71, 78, 79, 80, 66, 67, 72, 73,
	0, 0, 74, 75, 76, 77, 0, 0, 0, 0,
	0, 0, 83, 82, 81, 64, 65, 68, 69, 70,
	71, 78, 79, 0, 66, 67, 72, 73, 0, 0,
	74, 75, 76, 77, 0, 0, 0, 0, 82, 81,
	83, 65, 68, 69, 70, 71, 78, 79, 0, 66,
	67, 72, 73, 0, 0, 74, 75, 76, 77, 0,
	0, 0, 82, 81, 0, 83, 68, 69, 70, 71,
	78, 79, 0, 66, 67, 72, 73, 0, 0, 74,
	75, 76, 77, 171, 17, 28, 0, 0, 29, 83,
	19, 20, 0, 0, 22, 0, 58, 59, 172, 0,
	0, 0, 13, 0, 30, 0, 23, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 62, 25, 24,
	26, 18, 17, 28, 0, 0, 29, 0, 19, 20,
	0, 0, 22, 16, 0, 0, 21, 0, 0, 0,
	13, 15, 30, 0, 23, 44, 0, 32, 34, 35,
	33, 0, 36, 37, 0, 41, 25, 24, 26, 0,
	43, 0, 274, 0, 0, 0, 0, 0, 0, 0,
	0, 16, 0, 0, 0, 0, 0, 0, 0, 15,
	40, 42, 39, 44, 0, 32, 34, 35, 33, 0,
	36, 37, 0, 41, 0, 0, 0, 0, 43, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 40, 42,
	39,
}
var yyPact = [...]int{

	176, -1000, 177, -1000, 977, 1039, 301, 116, -1000, 229,
	241, 777, -1000, 977, -1000, 977, 977, -1000, -1000, -1000,
	-1000, 158, 157, 155, 977, 658, 686, -1000, 151, 149,
	977, 111, -1000, -1000, -1000, -1000, -1000, -1000, 105, 1039,
	235, 163, 1039, 148, -1000, 110, -1000, -1000, 301, 301,
	237, -1000, 175, -1000, -1000, -14, -1000, -1000, 166, 301,
	204, 249, 247, -1000, 977, 977, 977, 977, 977, 977,
	977, 977, 977, 977, 977, 977, 977, 977, 977, 977,
	977, 977, 977, 245, 720, 120, 120, 235, 522, 228,
	378, 26, 939, -1000, -33, 93, 23, 103, 20, 413,
	977, 977, 749, -1000, 243, 48, -15, -1000, 118, -1000,
	235, 82, 18, -1000, 1039, 1039, -1000, 80, 15, -1000,
	102, 14, -20, -1000, 51, 602, -35, 180, -1000, 146,
	-1000, 675, 977, 138, 1039, 858, 882, 50, 50, 50,
	50, 50, 50, 95, 95, 120, 120, 120, 120, 120,
	120, 833, 606, 10, 805, -1000, 188, -1000, 78, -11,
	-17, -1000, -1000, 204, -18, 977, -1000, 33, 240, -37,
	342, 204, 159, -1000, 977, 115, 977, -1000, 113, 977,
	246, 977, 540, 505, -1000, -1000, -1000, 1039, -1000, 235,
	239, -1000, -24, -1000, 1039, -1000, 77, -1000, 301, -1000,
	257, -1000, 237, 301, -1000, -1000, -1000, 503, -1000, 522,
	977, -43, 805, 235, -1000, -1000, 76, 977, -1000, 162,
	939, 1001, 522, 1039, -1000, 522, -12, 805, -1000, -1000,
	47, -1000, 32, -1000, 805, -1000, 13, 977, 805, -1000,
	13, 446, -27, -1000, 194, 977, 805, -1000, -1000, 38,
	100, -1000, -1000, -1000, -1000, 1039, -1000, -1000, 301, -1000,
	-1000, 127, -46, 977, 232, -30, 805, 977, 71, -1000,
	805, -1000, 977, 342, 977, 198, -1000, 217, -19, 70,
	977, -1000, 99, 977, -1000, 306, 96, 977, -1000, 246,
	977, 805, -1000, -1000, 301, -1000, -1000, -1000, -38, -1000,
	977, 805, -1000, -40, 805, 604, 720, -13, 805, 977,
	-1000, 522, 65, -1000, 805, -1000, 306, -1000, -1000, -1000,
	805, -1000, 805, 46, -1000, 805, 253, 977, -47, 188,
	-1000, 805, -1000, -1000, 939, -1000, 805, 977, -1000, -41,
	805, -1000, 939, 805, -1000, 805,
}
var yyPgo = [...]int{

	0, 32, 1, 22, 18, 325, 323, 5, 322, 2,
	8, 0, 321, 319, 317, 9, 3, 315, 314, 313,
	309, 308, 306, 21, 303, 302, 10, 299, 6, 20,
	293, 26, 45, 23, 11, 290, 289, 24, 283, 282,
	277, 276, 274, 267, 36, 266, 19, 265, 263, 176,
	255, 254, 7, 15, 4,
}
var yyR1 = [...]int{

	0, 51, 51, 51, 51, 51, 27, 27, 28, 28,
	28, 28, 28, 28, 28, 28, 28, 28, 28, 28,
	28, 35, 35, 33, 32, 32, 29, 29, 30, 30,
	31, 44, 44, 44, 44, 44, 50, 50, 45, 45,
	48, 49, 49, 47, 47, 46, 46, 1, 1, 2,
	2, 3, 3, 3, 10, 10, 5, 5, 9, 9,
	7, 7, 7, 7, 7, 8, 6, 6, 4, 4,
	4, 36, 36, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 16, 16,
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 14, 15, 17, 20, 20, 21, 18, 18, 19,
	25, 25, 26, 26, 54, 54, 38, 38, 37, 37,
	22, 22, 22, 23, 23, 40, 40, 39, 39, 24,
	24, 34, 41, 13, 13, 42, 42, 43, 43, 43,
	53, 53, 52, 52,
}
var yyR2 = [...]int{

	0, 3, 3, 3, 3, 3, 1, 3, 1, 1,
	1, 1, 1, 1, 1, 3, 5, 3, 4, 3,
	5, 1, 3, 2, 1, 3, 1, 2, 1, 3,
	1, 1, 1, 3, 3, 3, 1, 3, 1, 2,
	1, 1, 3, 1, 3, 1, 3, 0, 3, 2,
	3, 0, 1, 3, 1, 1, 0, 3, 1, 1,
	7, 2, 3, 7, 8, 3, 3, 4, 2, 3,
	4, 1, 3, 1, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 4, 1, 4, 5, 3, 2, 2, 2, 5,
	1, 1, 1, 1, 6, 7, 6, 4, 7, 6,
	4, 4, 6, 3, 4, 6, 5, 3, 1, 4,
	4, 5, 5, 5, 0, 2, 5, 1, 1, 2,
	1, 3, 3, 2, 0, 1, 1, 3, 1, 3,
	0, 1, 3, 3, 4, 1, 3, 1, 3, 3,
	5, 1, 3, 0, 2, 0, 3, 0, 2, 4,
	0, 1, 0, 1,
}
var yyChk = [...]int{

	-1000, -51, 28, 29, 30, 31, 32, -41, -13, 33,
	-1, -11, -12, 23, -17, 62, 54, 5, 4, 11,
	12, 19, 15, 27, 40, 39, 41, -14, 6, 9,
	25, -28, 6, 9, 7, 8, 11, 12, -27, 41,
	39, 14, 40, 19, 4, -44, 4, 61, 40, 41,
	39, 63, -42, 5, 63, -9, -7, -8, 17, 18,
	4, 19, 38, 63, 42, 43, 51, 52, 44, 45,
	46, 47, 53, 54, 57, 58, 59, 60, 48, 49,
	50, 41, 40, 67, -11, -11, -11, 40, 40, 40,
	-11, -38, -2, -37, -9, 4, -22, 72, -24, -11,
	40, 40, -11, 63, 67, -28, -32, -33, -35, 4,
	39, -31, -30, -29, -28, 40, 63, -48, -49, -44,
	-50, -49, -47, -46, 4, -1, -43, 34, 75, 37,
	-6, -44, 20, 4, 4, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -39, -11, 4, -15, 39, -34, -32,
	-3, -10, -9, 4, 5, 74, 69, -53, 74, -9,
	-11, 4, 19, 75, 72, -53, 74, 68, -53, 74,
	55, 72, -11, -11, 39, 4, 68, 72, 73, 74,
	74, -28, -32, 69, 74, -28, -31, 69, 74, 68,
	74, 73, 74, 72, 75, -4, 40, -36, 4, 40,
	76, -28, -11, 40, -28, 68, -53, 74, -16, 24,
	-1, 69, 74, 69, 69, 74, -40, -11, 73, -37,
	4, 75, -54, 75, -11, 68, -23, 35, -11, 68,
	-23, -11, -25, -26, -44, 23, -11, 69, 69, -20,
	-28, -33, 4, 73, -29, 69, -44, -45, 35, -46,
	-44, -5, -28, 76, 74, -3, -11, 76, -34, 69,
	-11, -15, 23, -11, 21, -28, -10, -28, -3, -53,
	74, 73, -53, 35, 74, -11, -53, 72, 68, 74,
	22, -11, 73, -21, 26, 68, -28, -44, -4, 69,
	76, -11, 4, 69, -11, 69, -11, -54, -11, 21,
	10, 74, -53, 69, -11, 68, -11, -52, 75, 68,
	-11, -26, -11, -44, 75, -11, -52, 76, -28, -15,
	73, -11, 69, -52, 72, -7, -11, 76, -16, -18,
	-11, -19, -2, -11, -54, -11,
}
var yyDef = [...]int{

	0, -2, 153, 47, 0, 0, 0, 0, 155, 0,
	0, 0, 73, 0, 92, 0, 0, 100, 101, 102,
	103, 0, 0, 0, 0, 0, 140, 118, 0, 0,
	0, 0, 8, 9, 10, 11, 12, 13, 14, 0,
	0, 0, 0, 0, 6, 0, 31, 32, 0, 0,
	0, 1, -2, 154, 2, 0, 58, 59, 0, 0,
	0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 96, 97, 0, 51, 0,
	0, 160, 0, 136, 0, 138, 160, 0, 160, 141,
	0, 0, 0, 4, 0, 0, 0, 24, 0, 21,
	0, 0, 30, 28, 26, 0, 5, 0, 40, 41,
	0, 36, 0, 43, 45, 152, 0, 0, 48, 0,
	61, 0, 0, 0, 0, 74, 75, 76, 77, 78,
	79, 80, 81, 82, 83, 84, 85, 86, 87, 88,
	89, 90, 0, 160, 147, 95, 0, 47, 0, 151,
	0, 52, 54, 55, 0, 0, 117, 0, 161, 0,
	134, 101, 0, 49, 0, 0, 161, 113, 0, 161,
	0, 0, 0, 0, 124, 7, 15, 0, 17, 0,
	0, 23, 0, 19, 0, 27, 0, 33, 0, 34,
	0, 35, 0, 0, 156, 158, 56, 0, 71, 51,
	0, 0, 62, 0, 65, 93, 0, 161, 91, 0,
	0, 0, 0, 0, 107, 51, 160, 145, 110, 137,
	138, 50, 0, 135, 139, 111, 160, 0, 142, 114,
	160, 0, 0, 130, 0, 0, 149, 119, 120, 0,
	0, 25, 22, 18, 29, 0, 42, 37, 38, 44,
	46, 0, 68, 0, 0, 0, 66, 0, 0, 94,
	148, 98, 0, 134, 0, 0, 53, 0, 160, 0,
	161, 121, 0, 0, 161, 162, 0, 0, 116, 0,
	0, 133, 123, 125, 0, 16, 20, 39, 0, 159,
	0, 69, 72, 162, 67, 0, 0, 0, 104, 0,
	106, 161, 0, 109, 146, 112, 162, 143, 163, 115,
	150, 131, 132, 0, 57, 70, 0, 0, 0, 0,
	122, 105, 108, 144, 0, 60, 63, 0, 99, 134,
	127, 128, 0, 64, 126, 129,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 62, 3, 3, 3, 59, 60, 3,
	40, 69, 57, 53, 74, 54, 67, 58, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 72, 75,
	51, 76, 52, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 41, 3, 68, 56, 61, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 39, 55, 73,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 42, 43, 44,
	45, 46, 47, 48, 49, 50, 63, 64, 65, 66,
	70, 71,
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
		//line reflow.y:138
		{
			yylex.(*Parser).Module = yyDollar[2].module
			return 0
		}
	case 2:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:143
		{
			yylex.(*Parser).Decls = yyDollar[2].decllist
			return 0
		}
	case 3:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:148
		{
			yylex.(*Parser).Expr = yyDollar[2].expr
			return 0
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:153
		{
			yylex.(*Parser).Type = yyDollar[2].typ
			return 0
		}
	case 5:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:158
		{
			yylex.(*Parser).Pat = yyDollar[2].pat
			return 0
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:169
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 7:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:171
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:174
		{
			yyVAL.typ = types.Int
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:175
		{
			yyVAL.typ = types.Float
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:176
		{
			yyVAL.typ = types.String
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:177
		{
			yyVAL.typ = types.Bool
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:178
		{
			yyVAL.typ = types.File
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:179
		{
			yyVAL.typ = types.Dir
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:180
		{
			yyVAL.typ = types.Ref(yyDollar[1].idents...)
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:181
		{
			yyVAL.typ = types.List(yyDollar[2].typ)
		}
	case 16:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:183
		{
			yyVAL.typ = types.Map(yyDollar[2].typ, yyDollar[4].typ)
		}
	case 17:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:185
		{
			yyVAL.typ = types.Struct(yyDollar[2].typfields...)
		}
	case 18:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:187
		{
			yyVAL.typ = types.Module(yyDollar[3].typfields, nil)
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:189
		{
			switch len(yyDollar[2].typfields) {
			// "()" is unit
			case 0:
				yyVAL.typ = types.Unit
				// "(type)" and "(name type)" get collapsed with
			// (optional) label
			case 1:
				yyVAL.typ = types.Labeled(yyDollar[2].typfields[0].Name, yyDollar[2].typfields[0].T)
				// a regular tuple must have at least two members
			default:
				yyVAL.typ = types.Tuple(yyDollar[2].typfields...)
			}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:201
		{
			yyVAL.typ = types.Func(yyDollar[5].typ, yyDollar[3].typfields...)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:205
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 22:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:207
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 23:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:211
		{
			for _, name := range yyDollar[1].idents {
				yyVAL.typfields = append(yyVAL.typfields, &types.Field{Name: name, T: yyDollar[2].typ})
			}
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:219
		{
			yyVAL.typfields = yyDollar[1].typfields
		}
	case 25:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:221
		{
			yyVAL.typfields = append(yyDollar[1].typfields, yyDollar[3].typfields...)
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:225
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, nil}
		}
	case 27:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:227
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, yyDollar[2].typ}
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:231
		{
			yyVAL.typeargs = []typearg{yyDollar[1].typearg}
		}
	case 29:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:233
		{
			yyVAL.typeargs = append(yyDollar[1].typeargs, yyDollar[3].typearg)
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:242
		{
			var (
				fields []*types.Field
				group  []*types.T
			)
			for _, arg := range yyDollar[1].typeargs {
				group = append(group, arg.t1)
				if arg.t2 != nil { // x, y, z t2
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
			yyVAL.typfields = fields
		Fail:
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:284
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:286
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatIgnore}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:288
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatTuple, List: yyDollar[2].patlist}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:290
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatList, List: yyDollar[2].listpats.list, Tail: yyDollar[2].listpats.tail}
		}
	case 35:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:292
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatStruct, Fields: make([]PatField, len(yyDollar[2].structpats))}
			for i, p := range yyDollar[2].structpats {
				yyVAL.pat.Fields[i] = PatField{p.field, p.pat}
			}
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:301
		{
			yyVAL.listpats = struct {
				list []*Pat
				tail *Pat
			}{
				list: yyDollar[1].patlist,
			}
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:308
		{
			yyVAL.listpats = struct {
				list []*Pat
				tail *Pat
			}{
				list: yyDollar[1].patlist,
				tail: yyDollar[3].pat,
			}
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:318
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatIgnore}
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:320
		{
			yyVAL.pat = yyDollar[2].pat
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:327
		{
			yyVAL.patlist = []*Pat{yyDollar[1].pat}
		}
	case 42:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:329
		{
			yyVAL.patlist = append(yyDollar[1].patlist, yyDollar[3].pat)
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:333
		{
			yyVAL.structpats = []struct {
				field string
				pat   *Pat
			}{yyDollar[1].structpat}
		}
	case 44:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:338
		{
			yyVAL.structpats = append(yyDollar[1].structpats, yyDollar[3].structpat)
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:342
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:347
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, yyDollar[3].pat}
		}
	case 47:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:355
		{
			yyVAL.decllist = nil
		}
	case 48:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:357
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:361
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:363
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 51:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:366
		{
			yyVAL.decllist = nil
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:368
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 53:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:370
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[3].decl)
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:374
		{
			yyVAL.decl = &Decl{
				Position: yyDollar[1].expr.Position,
				Comment:  yyDollar[1].expr.Comment,
				Pat:      &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident},
				Kind:     DeclAssign,
				Expr:     &Expr{Kind: ExprIdent, Ident: yyDollar[1].expr.Ident},
			}
		}
	case 56:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:385
		{
			yyVAL.decllist = nil
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:387
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 60:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:392
		{
			yyDollar[7].decl.Expr = &Expr{Position: yyDollar[7].decl.Expr.Position, Kind: ExprRequires, Left: yyDollar[7].decl.Expr, Decls: yyDollar[4].decllist}
			yyDollar[7].decl.Comment = yyDollar[1].pos.comment
			yyVAL.decl = yyDollar[7].decl
		}
	case 61:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:398
		{
			yyVAL.decl = yyDollar[2].decl
			yyVAL.decl.Comment = yyDollar[1].pos.comment
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:403
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Pat: &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 63:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:405
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Position: yyDollar[1].pos.Position, Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Kind: ExprFunc,
				Args: yyDollar[4].typfields,
				Left: yyDollar[7].expr}}
		}
	case 64:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:410
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Position: yyDollar[1].pos.Position, Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Position: yyDollar[1].pos.Position,
				Kind:     ExprAscribe,
				Type:     types.Func(yyDollar[6].typ, yyDollar[4].typfields...),
				Left:     &Expr{Kind: ExprFunc, Args: yyDollar[4].typfields, Left: yyDollar[8].expr}}}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:418
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: DeclType, Ident: yyDollar[2].expr.Ident, Type: yyDollar[3].typ}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:422
		{
			yyVAL.decl = &Decl{Position: yyDollar[3].expr.Position, Pat: yyDollar[1].pat, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 67:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:424
		{
			yyVAL.decl = &Decl{
				Position: yyDollar[4].expr.Position,
				Pat:      yyDollar[1].pat,
				Kind:     DeclAssign,
				Expr: &Expr{
					Position: yyDollar[4].expr.Position,
					Kind:     ExprAscribe,
					Type:     yyDollar[2].typ,
					Left:     yyDollar[4].expr,
				},
			}
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:440
		{
			yyVAL.decllist = nil
			for i := range yyDollar[1].posidents.idents {
				yyVAL.decllist = append(yyVAL.decllist, &Decl{
					Position: yyDollar[1].posidents.pos,
					Comment:  yyDollar[1].posidents.comments[i],
					Ident:    yyDollar[1].posidents.idents[i],
					Kind:     DeclDeclare,
					Type:     yyDollar[2].typ,
				})
			}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:453
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{Position: yyDollar[1].posidents.pos, Comment: yyDollar[1].posidents.comments[0], Pat: &Pat{Position: yyDollar[1].posidents.pos, Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]}, Kind: DeclAssign, Expr: yyDollar[3].expr}}
			}
		}
	case 70:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:461
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{
					Position: yyDollar[1].posidents.pos,
					Comment:  yyDollar[1].posidents.comments[0],
					Pat:      &Pat{Position: yyDollar[1].posidents.pos, Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]},
					Kind:     DeclAssign,
					Expr:     &Expr{Kind: ExprAscribe, Position: yyDollar[1].posidents.pos, Type: yyDollar[2].typ, Left: yyDollar[4].expr},
				}}
			}
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:477
		{
			yyVAL.posidents = posIdents{yyDollar[1].expr.Position, []string{yyDollar[1].expr.Ident}, []string{yyDollar[1].expr.Comment}}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:479
		{
			yyVAL.posidents = posIdents{yyDollar[1].posidents.pos, append(yyDollar[1].posidents.idents, yyDollar[3].expr.Ident), append(yyDollar[1].posidents.comments, yyDollar[3].expr.Comment)}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:485
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "||", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:487
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:489
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:491
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:493
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:495
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:497
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "!=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:499
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "==", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:501
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "+", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:503
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "-", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:505
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "*", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:507
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "/", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:509
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "%", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:511
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:513
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:515
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:517
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "~>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 91:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:519
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[4].expr}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:522
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIndex, Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:524
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprApply, Left: yyDollar[1].expr, Fields: yyDollar[3].exprfields}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:526
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprDeref, Left: yyDollar[1].expr, Ident: yyDollar[3].expr.Ident}
		}
	case 96:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:528
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "!", Left: yyDollar[2].expr}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:530
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "-", Left: yyDollar[2].expr}
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:534
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBlock, Left: yyDollar[2].expr}
		}
	case 99:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:536
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprCond, Cond: yyDollar[3].expr, Left: yyDollar[4].expr, Right: yyDollar[5].expr}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:543
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:545
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 104:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:547
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 105:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:549
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 106:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:552
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 107:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:554
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 108:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:556
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 109:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:558
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 110:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:560
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:562
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 112:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:564
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:571
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 114:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:573
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 115:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:575
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 116:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:582
		{
			yyVAL.expr = &Expr{
				Position:     yyDollar[1].pos.Position,
				Comment:      yyDollar[1].pos.comment,
				Kind:         ExprCompr,
				ComprExpr:    yyDollar[2].expr,
				ComprClauses: yyDollar[4].comprclauses,
			}
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:592
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:595
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "int", Fields: []*FieldExpr{{Expr: yyDollar[3].expr}}}
		}
	case 120:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:597
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "float", Fields: []*FieldExpr{{Expr: yyDollar[3].expr}}}
		}
	case 121:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:601
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 122:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:605
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 123:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:609
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprSwitch, Left: yyDollar[2].expr, CaseClauses: yyDollar[4].caseclauses}
		}
	case 124:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:612
		{
			yyVAL.caseclauses = nil
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:614
		{
			yyVAL.caseclauses = append(yyDollar[1].caseclauses, yyDollar[2].caseclause)
		}
	case 126:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:618
		{
			yyVAL.caseclause = &CaseClause{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: yyDollar[2].pat, Expr: yyDollar[4].expr}
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:624
		{
			yyVAL.expr = &Expr{Kind: ExprBlock, Decls: yyDollar[1].decllist, Left: yyDollar[2].expr}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:628
		{
			yyVAL.comprclauses = []*ComprClause{yyDollar[1].comprclause}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:630
		{
			yyVAL.comprclauses = append(yyDollar[1].comprclauses, yyDollar[3].comprclause)
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:634
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprEnum, Pat: yyDollar[1].pat, Expr: yyDollar[3].expr}
		}
	case 133:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:636
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprFilter, Expr: yyDollar[2].expr}
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:643
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:645
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:649
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:651
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 140:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:654
		{
			yyVAL.exprlist = nil
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:656
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:658
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:662
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 144:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:664
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:668
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:670
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:674
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:676
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:680
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 150:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:682
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:693
		{
			yyVAL.module = &ModuleImpl{Keyspace: yyDollar[1].expr, ParamDecls: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 153:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:696
		{
			yyVAL.expr = nil
		}
	case 154:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:698
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 155:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:701
		{
			yyVAL.decllist = nil
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:703
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 157:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:706
		{
			yyVAL.decllist = nil
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:708
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 159:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:710
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
