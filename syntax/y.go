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

	typ       *types.T
	typlist   []*types.T
	typfield  *types.Field
	typfields []*types.Field
	decl      *Decl
	decllist  []*Decl
	pat       *Pat
	patlist   []*Pat
	tok       int
	template  *Template

	structpat struct {
		field string
		pat   *Pat
	}

	structpats []struct {
		field string
		pat   *Pat
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
const tokMake = 57367
const tokLen = 57368
const tokPanic = 57369
const tokDelay = 57370
const tokTrace = 57371
const tokMap = 57372
const tokList = 57373
const tokZip = 57374
const tokUnzip = 57375
const tokFlatten = 57376
const tokStartModule = 57377
const tokStartDecls = 57378
const tokStartExpr = 57379
const tokStartType = 57380
const tokKeyspace = 57381
const tokParam = 57382
const tokEllipsis = 57383
const tokReserved = 57384
const tokRequires = 57385
const tokRange = 57386
const tokType = 57387
const tokOrOr = 57388
const tokAndAnd = 57389
const tokLE = 57390
const tokGE = 57391
const tokNE = 57392
const tokEqEq = 57393
const tokLSH = 57394
const tokRSH = 57395
const tokSquiggleArrow = 57396
const tokEOF = 57397
const tokError = 57398
const first = 57399
const unary = 57400
const apply = 57401
const deref = 57402

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
	"tokMake",
	"tokLen",
	"tokPanic",
	"tokDelay",
	"tokTrace",
	"tokMap",
	"tokList",
	"tokZip",
	"tokUnzip",
	"tokFlatten",
	"tokStartModule",
	"tokStartDecls",
	"tokStartExpr",
	"tokStartType",
	"tokKeyspace",
	"tokParam",
	"tokEllipsis",
	"tokReserved",
	"tokRequires",
	"tokRange",
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
	-1, 53,
	82, 152,
	-2, 42,
}

const yyNprod = 159
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1384

var yyAct = [...]int{

	10, 340, 57, 221, 124, 257, 246, 167, 225, 213,
	38, 166, 162, 85, 86, 87, 164, 173, 123, 117,
	168, 250, 94, 91, 220, 100, 121, 359, 321, 285,
	56, 9, 341, 348, 324, 311, 289, 290, 165, 236,
	334, 312, 276, 204, 95, 51, 245, 39, 41, 42,
	40, 115, 43, 44, 286, 48, 306, 238, 237, 287,
	50, 212, 239, 236, 132, 179, 141, 142, 143, 144,
	145, 146, 147, 148, 149, 150, 151, 152, 153, 154,
	155, 156, 157, 158, 160, 126, 116, 47, 49, 46,
	203, 204, 353, 129, 176, 303, 307, 287, 204, 232,
	209, 185, 188, 189, 190, 191, 192, 193, 194, 195,
	196, 197, 198, 199, 175, 181, 182, 184, 174, 304,
	242, 201, 282, 206, 281, 202, 138, 180, 291, 210,
	355, 336, 329, 216, 293, 278, 235, 219, 208, 227,
	251, 342, 251, 338, 317, 229, 288, 56, 183, 114,
	113, 52, 211, 139, 228, 83, 82, 217, 216, 207,
	51, 223, 39, 41, 42, 40, 125, 43, 44, 112,
	48, 111, 241, 133, 253, 50, 249, 231, 110, 133,
	109, 248, 84, 252, 108, 180, 255, 107, 260, 106,
	258, 83, 82, 105, 104, 234, 88, 243, 259, 79,
	80, 214, 47, 49, 46, 320, 254, 273, 75, 76,
	77, 78, 61, 103, 102, 137, 135, 136, 84, 284,
	280, 137, 135, 136, 274, 59, 60, 62, 277, 283,
	101, 90, 89, 294, 169, 296, 88, 134, 163, 351,
	298, 120, 300, 134, 299, 292, 295, 59, 60, 62,
	130, 301, 308, 63, 128, 56, 8, 233, 302, 313,
	314, 2, 3, 4, 5, 315, 332, 138, 305, 316,
	333, 170, 309, 54, 226, 63, 323, 119, 55, 61,
	275, 244, 322, 318, 200, 161, 325, 96, 140, 319,
	139, 326, 59, 60, 62, 328, 1, 222, 331, 327,
	59, 60, 62, 330, 337, 224, 127, 339, 53, 6,
	61, 343, 240, 159, 345, 92, 258, 215, 344, 335,
	63, 118, 349, 59, 60, 62, 350, 122, 63, 45,
	256, 99, 97, 354, 352, 25, 7, 11, 58, 131,
	279, 356, 299, 16, 15, 27, 93, 0, 28, 0,
	17, 18, 358, 357, 20, 0, 0, 0, 19, 0,
	360, 0, 12, 0, 21, 26, 36, 35, 37, 32,
	34, 29, 30, 31, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 33, 0, 23, 22, 24, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 51,
	14, 39, 41, 42, 40, 0, 43, 44, 13, 48,
	0, 0, 0, 0, 50, 0, 0, 0, 98, 83,
	82, 65, 66, 69, 70, 71, 72, 79, 80, 81,
	67, 68, 73, 74, 0, 0, 75, 76, 77, 78,
	0, 47, 49, 46, 0, 0, 84, 0, 0, 0,
	0, 0, 0, 0, 341, 83, 82, 65, 66, 69,
	70, 71, 72, 79, 80, 81, 67, 68, 73, 74,
	0, 0, 75, 76, 77, 78, 0, 0, 218, 0,
	0, 0, 84, 0, 0, 0, 0, 0, 0, 0,
	247, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	172, 0, 0, 0, 0, 171, 83, 82, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 0, 84, 0, 0, 0, 0, 0, 0,
	268, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	0, 0, 0, 0, 0, 264, 83, 82, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 186, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 0, 84, 0, 0, 0, 0, 187, 83,
	82, 65, 66, 69, 70, 71, 72, 79, 80, 81,
	67, 68, 73, 74, 0, 0, 75, 76, 77, 78,
	0, 0, 0, 0, 0, 0, 84, 0, 0, 0,
	0, 310, 83, 82, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 0, 0, 0, 84,
	0, 347, 83, 82, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 177, 15, 27, 0, 84,
	28, 346, 17, 18, 0, 0, 20, 0, 59, 60,
	178, 0, 0, 0, 12, 0, 21, 26, 36, 35,
	37, 32, 34, 29, 30, 31, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 33, 63, 23, 22, 24,
	51, 0, 39, 41, 42, 40, 0, 43, 44, 0,
	48, 0, 14, 0, 0, 50, 0, 0, 0, 0,
	13, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 47, 49, 46, 0, 0, 0, 84, 0,
	272, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 205, 84, 0,
	271, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	270, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	269, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	267, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	266, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	265, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	263, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	262, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	261, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 230,
	163, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 83,
	82, 65, 66, 69, 70, 71, 72, 79, 80, 81,
	67, 68, 73, 74, 0, 0, 75, 76, 77, 78,
	0, 0, 64, 0, 0, 0, 84, 83, 82, 65,
	66, 69, 70, 71, 72, 79, 80, 81, 67, 68,
	73, 74, 0, 0, 75, 76, 77, 78, 16, 15,
	27, 0, 0, 28, 84, 17, 18, 0, 0, 20,
	0, 0, 0, 19, 0, 0, 0, 12, 0, 21,
	26, 36, 35, 37, 32, 34, 29, 30, 31, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 33, 0,
	23, 22, 24, 51, 0, 39, 41, 42, 40, 0,
	43, 44, 0, 48, 0, 14, 0, 0, 50, 0,
	297, 0, 0, 13, 83, 82, 65, 66, 69, 70,
	71, 72, 79, 80, 0, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 47, 49, 46, 0, 83,
	82, 84, 66, 69, 70, 71, 72, 79, 80, 0,
	67, 68, 73, 74, 0, 0, 75, 76, 77, 78,
	0, 0, 0, 83, 82, 0, 84, 69, 70, 71,
	72, 79, 80, 0, 67, 68, 73, 74, 83, 82,
	75, 76, 77, 78, 0, 0, 79, 80, 0, 0,
	84, 73, 74, 0, 0, 75, 76, 77, 78, 51,
	0, 39, 41, 42, 40, 84, 43, 44, 0, 48,
	0, 0, 0, 0, 50, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 47, 49, 46,
}
var yyPact = [...]int{

	226, -1000, 217, -1000, 1184, 1335, 81, -1000, 268, 208,
	1092, -1000, 1184, 1184, 1184, -1000, -1000, -1000, -1000, 189,
	185, 184, 1184, 283, 339, -1000, 183, 167, 166, 147,
	146, 142, 140, 137, 133, 131, 124, 122, 80, -1000,
	-1000, -1000, -1000, -1000, -1000, 75, 1335, 273, 195, 1335,
	119, -1000, -1000, 214, -1000, -1000, 11, -1000, -1000, 207,
	169, 247, 286, 284, -1000, 1184, 1184, 1184, 1184, 1184,
	1184, 1184, 1184, 1184, 1184, 1184, 1184, 1184, 1184, 1184,
	1184, 1184, 1184, 1184, 281, 1064, 108, 108, 273, 230,
	266, 444, 37, 711, -1000, -17, 106, 35, 73, 20,
	549, 1184, 1184, 1184, 1184, 1184, 1184, 1184, 1184, 1184,
	1184, 1184, 1184, -1000, 280, 46, 10, -1000, 756, -1000,
	273, 62, 19, -1000, 1335, 1335, 275, -21, 154, -1000,
	110, -1000, 395, -1000, -1000, 169, 169, 270, 1184, 107,
	1335, 1232, 1256, 1271, 1271, 1271, 1271, 1271, 1271, 144,
	144, 108, 108, 108, 108, 108, 108, 1207, 1034, 18,
	1120, -1000, 233, -1000, 60, 17, -18, -1000, -1000, 247,
	-19, 1184, -1000, 40, 277, -36, 408, 247, 149, -1000,
	1184, 101, 1184, -1000, 99, 1184, 175, 1184, 1004, 974,
	944, 514, 914, 884, 854, 479, 824, 794, 764, 734,
	-1000, -1000, 1335, -1000, 273, 276, -1000, -38, -1000, 1335,
	-1000, 59, -1000, -1000, -1000, 41, -1000, 230, 1184, -54,
	-22, -1000, 71, 16, -44, -1000, 49, 1120, 273, -1000,
	-1000, 58, 1184, 192, 711, 1229, 230, 1335, -1000, 230,
	14, 1120, -1000, -1000, 48, -1000, 39, -1000, 1120, -1000,
	15, 1184, 1120, -1000, 15, 582, -40, -1000, 237, 1184,
	1120, -1000, -1000, -1000, 1184, -1000, -1000, -1000, 1184, -1000,
	-1000, -1000, -1000, 69, -1000, -1000, -1000, -1000, 1335, 129,
	-55, 1184, 272, -42, 1120, 1184, -1000, 169, -1000, -1000,
	270, 169, 56, -1000, 1120, -1000, 408, 1184, 245, -1000,
	260, -41, 55, 1184, -1000, 68, 1184, -1000, 372, 66,
	1184, -1000, 175, 1184, 1120, 645, 615, -1000, -1000, -49,
	-1000, 1184, 1120, -1000, -50, 1120, -1000, -1000, -1000, 156,
	12, 1120, 1184, -1000, 230, 54, -1000, 1120, -1000, 372,
	-1000, -1000, -1000, 1120, -1000, 1120, -1000, -1000, -1000, 1120,
	306, 1184, -56, -1000, 1120, -1000, -1000, -1000, 1120, 1184,
	1120,
}
var yyPgo = [...]int{

	0, 31, 346, 11, 9, 340, 339, 2, 338, 20,
	7, 0, 337, 336, 335, 12, 332, 21, 331, 330,
	5, 329, 4, 18, 327, 26, 38, 19, 16, 321,
	317, 22, 315, 313, 312, 309, 308, 306, 3, 8,
	305, 24, 297, 296, 1, 17, 6,
}
var yyR1 = [...]int{

	0, 43, 43, 43, 43, 21, 21, 22, 22, 22,
	22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
	29, 29, 27, 26, 26, 23, 23, 24, 24, 25,
	38, 38, 38, 38, 38, 42, 41, 41, 40, 40,
	39, 39, 1, 1, 2, 2, 3, 3, 3, 10,
	10, 5, 5, 9, 9, 7, 7, 7, 7, 7,
	8, 6, 6, 4, 4, 4, 30, 30, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 14, 15, 19, 19, 20, 20, 46,
	46, 32, 32, 31, 31, 16, 16, 16, 17, 17,
	34, 34, 33, 33, 18, 18, 28, 35, 13, 13,
	36, 36, 37, 37, 37, 45, 45, 44, 44,
}
var yyR2 = [...]int{

	0, 3, 3, 3, 3, 1, 3, 1, 1, 1,
	1, 1, 1, 1, 3, 5, 3, 4, 3, 5,
	1, 3, 2, 1, 3, 1, 2, 1, 3, 1,
	1, 1, 3, 3, 3, 1, 1, 3, 1, 3,
	1, 3, 0, 3, 2, 3, 0, 1, 3, 1,
	1, 0, 3, 1, 1, 7, 2, 3, 7, 8,
	3, 3, 4, 2, 3, 4, 1, 3, 1, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 5, 4, 5, 3,
	2, 2, 1, 1, 1, 1, 6, 7, 6, 4,
	7, 6, 4, 4, 6, 3, 4, 6, 5, 3,
	1, 4, 4, 4, 6, 4, 4, 4, 6, 4,
	4, 4, 4, 5, 5, 1, 3, 3, 2, 0,
	1, 1, 3, 1, 3, 0, 1, 3, 3, 4,
	1, 3, 1, 3, 3, 5, 1, 3, 0, 2,
	0, 3, 0, 2, 4, 0, 1, 0, 1,
}
var yyChk = [...]int{

	-1000, -43, 35, 36, 37, 38, -35, -13, 39, -1,
	-11, -12, 23, 69, 61, 5, 4, 11, 12, 19,
	15, 25, 47, 46, 48, -14, 26, 6, 9, 32,
	33, 34, 30, 44, 31, 28, 27, 29, -22, 6,
	9, 7, 8, 11, 12, -21, 48, 46, 14, 47,
	19, 4, 70, -36, 5, 70, -9, -7, -8, 17,
	18, 4, 19, 45, 70, 49, 50, 58, 59, 51,
	52, 53, 54, 60, 61, 64, 65, 66, 67, 55,
	56, 57, 48, 47, 74, -11, -11, -11, 47, 47,
	47, -11, -32, -2, -31, -9, 4, -16, 79, -18,
	-11, 47, 47, 47, 47, 47, 47, 47, 47, 47,
	47, 47, 47, 70, 74, -22, -26, -27, -29, 4,
	46, -25, -24, -23, -22, 47, -1, -37, 40, 82,
	43, -6, -38, 4, 68, 47, 48, 46, 20, 4,
	4, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -33,
	-11, 4, -15, 46, -28, -26, -3, -10, -9, 4,
	5, 81, 76, -45, 81, -9, -11, 4, 19, 82,
	79, -45, 81, 75, -45, 81, 62, 79, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	4, 75, 79, 80, 81, 81, -22, -26, 76, 81,
	-22, -25, 82, -4, 47, -30, 4, 47, 83, -22,
	-41, -38, -42, -41, -40, -39, 4, -11, 47, -22,
	75, -45, 81, 24, -1, 76, 81, 76, 76, 81,
	-34, -11, 80, -31, 4, 82, -46, 82, -11, 75,
	-17, 41, -11, 75, -17, -11, -19, -20, -38, 23,
	-11, 76, 76, 76, 81, 76, 76, 76, 81, 76,
	76, 76, 76, -22, -27, 4, 80, -23, 76, -5,
	-22, 83, 81, -3, -11, 83, 76, 81, 75, 80,
	81, 79, -28, 76, -11, -15, -11, 21, -22, -10,
	-22, -3, -45, 81, 80, -45, 41, 81, -11, -45,
	79, 75, 81, 22, -11, -11, -11, 75, -22, -4,
	76, 83, -11, 4, 76, -11, -38, -39, -38, 76,
	-46, -11, 21, 10, 81, -45, 76, -11, 75, -11,
	-44, 82, 75, -11, -20, -11, 76, 76, 82, -11,
	-44, 83, -22, 80, -11, 76, -44, -7, -11, 83,
	-11,
}
var yyDef = [...]int{

	0, -2, 148, 42, 0, 0, 0, 150, 0, 0,
	0, 68, 0, 0, 0, 92, 93, 94, 95, 0,
	0, 0, 0, 0, 135, 110, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
	8, 9, 10, 11, 12, 13, 0, 0, 0, 0,
	0, 5, 1, -2, 149, 2, 0, 53, 54, 0,
	0, 0, 0, 0, 3, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 90, 91, 0, 46,
	0, 0, 155, 0, 131, 0, 133, 155, 0, 155,
	136, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 4, 0, 0, 0, 23, 0, 20,
	0, 0, 29, 27, 25, 0, 147, 0, 0, 43,
	0, 56, 0, 30, 31, 0, 0, 0, 0, 0,
	0, 69, 70, 71, 72, 73, 74, 75, 76, 77,
	78, 79, 80, 81, 82, 83, 84, 85, 0, 155,
	142, 89, 0, 42, 0, 146, 0, 47, 49, 50,
	0, 0, 109, 0, 156, 0, 129, 93, 0, 44,
	0, 0, 156, 105, 0, 156, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	6, 14, 0, 16, 0, 0, 22, 0, 18, 0,
	26, 0, 151, 153, 51, 0, 66, 46, 0, 0,
	0, 36, 0, 35, 0, 38, 40, 57, 0, 60,
	87, 0, 156, 0, 0, 0, 0, 0, 99, 46,
	155, 140, 102, 132, 133, 45, 0, 130, 134, 103,
	155, 0, 137, 106, 155, 0, 0, 125, 0, 0,
	144, 111, 112, 113, 0, 115, 116, 117, 0, 119,
	120, 121, 122, 0, 24, 21, 17, 28, 0, 0,
	63, 0, 0, 0, 61, 0, 32, 0, 33, 34,
	0, 0, 0, 88, 143, 86, 129, 0, 0, 48,
	0, 155, 0, 156, 123, 0, 0, 156, 157, 0,
	0, 108, 0, 0, 128, 0, 0, 15, 19, 0,
	154, 0, 64, 67, 157, 62, 37, 39, 41, 0,
	0, 96, 0, 98, 156, 0, 101, 141, 104, 157,
	138, 158, 107, 145, 126, 127, 114, 118, 52, 65,
	0, 0, 0, 124, 97, 100, 139, 55, 58, 0,
	59,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 69, 3, 3, 3, 66, 67, 3,
	47, 76, 64, 60, 81, 61, 74, 65, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 79, 82,
	58, 83, 59, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 48, 3, 75, 63, 68, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 46, 62, 80,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 70, 71, 72, 73, 77, 78,
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
		//line reflow.y:126
		{
			yylex.(*Parser).Module = yyDollar[2].module
			return 0
		}
	case 2:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:131
		{
			yylex.(*Parser).Decls = yyDollar[2].decllist
			return 0
		}
	case 3:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:136
		{
			yylex.(*Parser).Expr = yyDollar[2].expr
			return 0
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:141
		{
			yylex.(*Parser).Type = yyDollar[2].typ
			return 0
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:152
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 6:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:154
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:157
		{
			yyVAL.typ = types.Int
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:158
		{
			yyVAL.typ = types.Float
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:159
		{
			yyVAL.typ = types.String
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:160
		{
			yyVAL.typ = types.Bool
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:161
		{
			yyVAL.typ = types.File
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:162
		{
			yyVAL.typ = types.Dir
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:163
		{
			yyVAL.typ = types.Ref(yyDollar[1].idents...)
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:164
		{
			yyVAL.typ = types.List(yyDollar[2].typ)
		}
	case 15:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:166
		{
			yyVAL.typ = types.Map(yyDollar[2].typ, yyDollar[4].typ)
		}
	case 16:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:168
		{
			yyVAL.typ = types.Struct(yyDollar[2].typfields...)
		}
	case 17:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:170
		{
			yyVAL.typ = types.Module(yyDollar[3].typfields, nil)
		}
	case 18:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:172
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
	case 19:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:184
		{
			yyVAL.typ = types.Func(yyDollar[5].typ, yyDollar[3].typfields...)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:188
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:190
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 22:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:194
		{
			for _, name := range yyDollar[1].idents {
				yyVAL.typfields = append(yyVAL.typfields, &types.Field{Name: name, T: yyDollar[2].typ})
			}
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:202
		{
			yyVAL.typfields = yyDollar[1].typfields
		}
	case 24:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:204
		{
			yyVAL.typfields = append(yyDollar[1].typfields, yyDollar[3].typfields...)
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:208
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, nil}
		}
	case 26:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:210
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, yyDollar[2].typ}
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:214
		{
			yyVAL.typeargs = []typearg{yyDollar[1].typearg}
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:216
		{
			yyVAL.typeargs = append(yyDollar[1].typeargs, yyDollar[3].typearg)
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:225
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
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:267
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:269
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatIgnore}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:271
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatTuple, List: yyDollar[2].patlist}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:273
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatList, List: yyDollar[2].patlist}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:275
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatStruct, Map: make(map[string]*Pat)}
			for _, p := range yyDollar[2].structpats {
				yyVAL.pat.Map[p.field] = p.pat
			}
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:287
		{
			yyVAL.patlist = []*Pat{yyDollar[1].pat}
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:289
		{
			yyVAL.patlist = append(yyDollar[1].patlist, yyDollar[3].pat)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:293
		{
			yyVAL.structpats = []struct {
				field string
				pat   *Pat
			}{yyDollar[1].structpat}
		}
	case 39:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:298
		{
			yyVAL.structpats = append(yyDollar[1].structpats, yyDollar[3].structpat)
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:302
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:307
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, yyDollar[3].pat}
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:315
		{
			yyVAL.decllist = nil
		}
	case 43:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:317
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 44:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:321
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:323
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 46:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:326
		{
			yyVAL.decllist = nil
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:328
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 48:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:330
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[3].decl)
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:334
		{
			yyVAL.decl = &Decl{
				Position: yyDollar[1].expr.Position,
				Comment:  yyDollar[1].expr.Comment,
				Pat:      &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident},
				Kind:     DeclAssign,
				Expr:     &Expr{Kind: ExprIdent, Ident: yyDollar[1].expr.Ident},
			}
		}
	case 51:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:345
		{
			yyVAL.decllist = nil
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:347
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 55:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:352
		{
			yyDollar[7].decl.Expr = &Expr{Position: yyDollar[7].decl.Expr.Position, Kind: ExprRequires, Left: yyDollar[7].decl.Expr, Decls: yyDollar[4].decllist}
			yyDollar[7].decl.Comment = yyDollar[1].pos.comment
			yyVAL.decl = yyDollar[7].decl
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:358
		{
			yyVAL.decl = yyDollar[2].decl
			yyVAL.decl.Comment = yyDollar[1].pos.comment
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:363
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 58:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:365
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Kind: ExprFunc,
				Args: yyDollar[4].typfields,
				Left: yyDollar[7].expr}}
		}
	case 59:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:370
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Position: yyDollar[1].pos.Position,
				Kind:     ExprAscribe,
				Type:     types.Func(yyDollar[6].typ, yyDollar[4].typfields...),
				Left:     &Expr{Kind: ExprFunc, Args: yyDollar[4].typfields, Left: yyDollar[8].expr}}}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:378
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: DeclType, Ident: yyDollar[2].expr.Ident, Type: yyDollar[3].typ}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:382
		{
			yyVAL.decl = &Decl{Position: yyDollar[3].expr.Position, Pat: yyDollar[1].pat, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 62:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:384
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
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:400
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
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:413
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{Position: yyDollar[1].posidents.pos, Comment: yyDollar[1].posidents.comments[0], Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]}, Kind: DeclAssign, Expr: yyDollar[3].expr}}
			}
		}
	case 65:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:421
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{
					Position: yyDollar[1].posidents.pos,
					Comment:  yyDollar[1].posidents.comments[0],
					Pat:      &Pat{Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]},
					Kind:     DeclAssign,
					Expr:     &Expr{Kind: ExprAscribe, Position: yyDollar[1].posidents.pos, Type: yyDollar[2].typ, Left: yyDollar[4].expr},
				}}
			}
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:437
		{
			yyVAL.posidents = posIdents{yyDollar[1].expr.Position, []string{yyDollar[1].expr.Ident}, []string{yyDollar[1].expr.Comment}}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:439
		{
			yyVAL.posidents = posIdents{yyDollar[1].posidents.pos, append(yyDollar[1].posidents.idents, yyDollar[3].expr.Ident), append(yyDollar[1].posidents.comments, yyDollar[3].expr.Comment)}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:445
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "||", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:447
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:449
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:451
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:453
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:455
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:457
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "!=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:459
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "==", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:461
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "+", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:463
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "-", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:465
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "*", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:467
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "/", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:469
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "%", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:471
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:473
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:475
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:477
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "~>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:479
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 87:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:481
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIndex, Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:483
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprApply, Left: yyDollar[1].expr, Fields: yyDollar[3].exprfields}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:485
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprDeref, Left: yyDollar[1].expr, Ident: yyDollar[3].expr.Ident}
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:487
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "!", Left: yyDollar[2].expr}
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:489
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "-", Left: yyDollar[2].expr}
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:496
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:498
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 96:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:500
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 97:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:502
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 98:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:505
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 99:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:507
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 100:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:509
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:511
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:513
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 103:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:515
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 104:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:517
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:524
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:526
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 107:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:528
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 108:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:535
		{
			yyVAL.expr = &Expr{
				Position:     yyDollar[1].pos.Position,
				Comment:      yyDollar[1].pos.comment,
				Kind:         ExprCompr,
				ComprExpr:    yyDollar[2].expr,
				ComprClauses: yyDollar[4].comprclauses,
			}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:545
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:548
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "len", Left: yyDollar[3].expr}
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:550
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "int", Left: yyDollar[3].expr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:552
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "float", Left: yyDollar[3].expr}
		}
	case 114:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:554
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "zip", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:556
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "unzip", Left: yyDollar[3].expr}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:558
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "flatten", Left: yyDollar[3].expr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:560
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "map", Left: yyDollar[3].expr}
		}
	case 118:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:562
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "range", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:564
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "list", Left: yyDollar[3].expr}
		}
	case 120:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:566
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "delay", Left: yyDollar[3].expr}
		}
	case 121:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:568
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "panic", Left: yyDollar[3].expr}
		}
	case 122:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:570
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "trace", Left: yyDollar[3].expr}
		}
	case 123:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:574
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 124:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:578
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:582
		{
			yyVAL.comprclauses = []*ComprClause{yyDollar[1].comprclause}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:584
		{
			yyVAL.comprclauses = append(yyDollar[1].comprclauses, yyDollar[3].comprclause)
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:588
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprEnum, Pat: yyDollar[1].pat, Expr: yyDollar[3].expr}
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:590
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprFilter, Expr: yyDollar[2].expr}
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:597
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:599
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:603
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:605
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 135:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:608
		{
			yyVAL.exprlist = nil
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:610
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:612
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:616
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 139:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:618
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:622
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:624
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:628
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:630
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:634
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 145:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:636
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:647
		{
			yyVAL.module = &ModuleImpl{Keyspace: yyDollar[1].expr, ParamDecls: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 148:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:650
		{
			yyVAL.expr = nil
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:652
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 150:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:655
		{
			yyVAL.decllist = nil
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:657
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 152:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:660
		{
			yyVAL.decllist = nil
		}
	case 153:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:662
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 154:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:664
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
