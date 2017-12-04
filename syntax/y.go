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
const tokMap = 57371
const tokList = 57372
const tokZip = 57373
const tokUnzip = 57374
const tokFlatten = 57375
const tokStartModule = 57376
const tokStartDecls = 57377
const tokStartExpr = 57378
const tokStartType = 57379
const tokKeyspace = 57380
const tokParam = 57381
const tokEllipsis = 57382
const tokReserved = 57383
const tokRequires = 57384
const tokType = 57385
const tokOrOr = 57386
const tokAndAnd = 57387
const tokLE = 57388
const tokGE = 57389
const tokNE = 57390
const tokEqEq = 57391
const tokLSH = 57392
const tokRSH = 57393
const tokSquiggleArrow = 57394
const tokEOF = 57395
const tokError = 57396
const first = 57397
const apply = 57398

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
	"'.'",
	"apply",
	"']'",
	"':'",
	"'}'",
	"')'",
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
	78, 147,
	-2, 42,
}

const yyNprod = 154
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1343

var yyAct = [...]int{

	10, 326, 56, 241, 121, 220, 163, 164, 208, 159,
	37, 120, 161, 84, 170, 245, 93, 114, 215, 165,
	344, 118, 307, 275, 90, 216, 99, 327, 332, 55,
	9, 310, 231, 279, 240, 280, 276, 277, 266, 162,
	199, 233, 234, 320, 94, 232, 231, 211, 207, 50,
	112, 38, 40, 41, 39, 296, 42, 43, 198, 47,
	199, 337, 176, 126, 49, 139, 140, 141, 142, 143,
	144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
	154, 155, 156, 123, 158, 129, 113, 293, 277, 46,
	48, 45, 297, 173, 199, 226, 204, 182, 179, 171,
	339, 185, 186, 187, 188, 189, 190, 191, 192, 193,
	194, 178, 172, 181, 322, 315, 283, 268, 230, 306,
	201, 294, 272, 203, 271, 50, 205, 38, 40, 41,
	39, 237, 42, 43, 214, 47, 222, 196, 197, 135,
	49, 177, 224, 55, 206, 281, 246, 328, 324, 246,
	303, 278, 218, 225, 180, 64, 83, 202, 64, 83,
	111, 110, 51, 79, 80, 46, 48, 45, 223, 236,
	130, 212, 75, 76, 77, 78, 122, 136, 243, 248,
	247, 82, 244, 250, 82, 252, 109, 211, 238, 108,
	166, 229, 107, 177, 106, 105, 104, 249, 103, 102,
	335, 101, 263, 58, 59, 61, 100, 60, 89, 251,
	134, 132, 133, 88, 274, 270, 267, 264, 87, 273,
	58, 59, 61, 87, 160, 117, 127, 284, 209, 62,
	286, 125, 131, 8, 228, 288, 282, 290, 285, 289,
	301, 291, 2, 3, 4, 5, 62, 298, 318, 55,
	292, 50, 135, 38, 40, 41, 39, 302, 42, 43,
	295, 47, 319, 167, 299, 53, 49, 60, 221, 309,
	60, 54, 308, 304, 116, 265, 311, 239, 305, 195,
	58, 59, 61, 58, 59, 61, 313, 157, 317, 11,
	316, 46, 48, 45, 323, 137, 136, 325, 1, 217,
	95, 329, 330, 312, 85, 86, 321, 314, 333, 62,
	219, 124, 334, 58, 59, 61, 52, 6, 235, 338,
	336, 138, 91, 210, 115, 119, 213, 340, 289, 44,
	98, 96, 26, 7, 13, 57, 343, 342, 128, 62,
	269, 92, 0, 0, 0, 345, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 82, 0, 0, 0, 0, 0, 0, 327,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 0, 0,
	0, 0, 0, 242, 64, 83, 65, 66, 69, 70,
	71, 72, 79, 80, 81, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 0, 0, 0, 0, 0,
	82, 0, 0, 0, 0, 169, 168, 64, 83, 65,
	66, 69, 70, 71, 72, 79, 80, 81, 67, 68,
	73, 74, 0, 0, 75, 76, 77, 78, 0, 17,
	16, 28, 0, 82, 29, 0, 18, 19, 0, 256,
	21, 0, 0, 0, 20, 0, 0, 0, 12, 0,
	22, 27, 36, 35, 33, 34, 30, 31, 32, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 24,
	23, 25, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 50, 15, 38, 40, 41, 39, 0,
	42, 43, 14, 47, 0, 0, 0, 0, 49, 97,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 46, 48, 45, 82, 0, 0, 0,
	0, 331, 64, 83, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 200, 0, 82, 0,
	0, 0, 0, 262, 64, 83, 65, 66, 69, 70,
	71, 72, 79, 80, 81, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 0, 0, 0, 0, 0,
	82, 0, 0, 0, 0, 261, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 82, 0, 0, 0, 0, 260, 64, 83,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	0, 0, 0, 0, 82, 0, 0, 0, 0, 259,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 0, 0,
	0, 258, 64, 83, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 0, 0, 82, 0,
	0, 0, 0, 257, 64, 83, 65, 66, 69, 70,
	71, 72, 79, 80, 81, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 0, 0, 0, 0, 0,
	82, 0, 0, 0, 0, 255, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 82, 0, 0, 0, 0, 254, 64, 83,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	0, 0, 0, 0, 82, 0, 0, 0, 0, 253,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 183, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 0, 184,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 0, 300,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 341, 64,
	83, 65, 66, 69, 70, 71, 72, 79, 80, 81,
	67, 68, 73, 74, 0, 0, 75, 76, 77, 78,
	0, 174, 16, 28, 0, 82, 29, 227, 18, 19,
	0, 0, 21, 0, 58, 59, 175, 0, 0, 0,
	12, 0, 22, 27, 36, 35, 33, 34, 30, 31,
	32, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	62, 24, 23, 25, 0, 0, 0, 50, 0, 38,
	40, 41, 39, 0, 42, 43, 15, 47, 0, 0,
	0, 0, 49, 0, 14, 160, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 46, 48, 45,
	0, 0, 82, 64, 83, 65, 66, 69, 70, 71,
	72, 79, 80, 81, 67, 68, 73, 74, 0, 0,
	75, 76, 77, 78, 0, 0, 63, 0, 0, 82,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 17, 16, 28, 0, 82, 29, 0, 18,
	19, 0, 0, 21, 0, 0, 0, 20, 0, 0,
	0, 12, 0, 22, 27, 36, 35, 33, 34, 30,
	31, 32, 64, 83, 0, 0, 0, 0, 0, 0,
	79, 80, 24, 23, 25, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 0, 15, 82, 0,
	0, 0, 0, 0, 0, 14, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 0, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	64, 83, 82, 66, 69, 70, 71, 72, 79, 80,
	0, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 17, 16, 28, 0, 82, 29, 0, 18,
	19, 0, 0, 21, 0, 0, 0, 20, 0, 0,
	0, 0, 0, 22, 27, 36, 35, 33, 34, 30,
	31, 32, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 24, 23, 25, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 64, 83, 15, 0, 69,
	70, 71, 72, 79, 80, 14, 67, 68, 73, 74,
	0, 0, 75, 76, 77, 78, 0, 0, 0, 0,
	50, 82, 38, 40, 41, 39, 0, 42, 43, 0,
	47, 0, 0, 0, 0, 49, 0, 287, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	46, 48, 45,
}
var yyPact = [...]int{

	208, -1000, 195, -1000, 1108, 1013, 94, -1000, 260, 203,
	1018, -1000, 1108, -1000, 1218, 1218, -1000, -1000, -1000, -1000,
	178, 168, 163, 1108, 296, 465, -1000, 161, 156, 154,
	153, 151, 150, 149, 147, 144, 141, 93, -1000, -1000,
	-1000, -1000, -1000, -1000, 89, 1013, 270, 181, 1013, 131,
	-1000, -1000, 192, -1000, -1000, -15, -1000, -1000, 184, 166,
	232, 292, 291, -1000, 1108, 1108, 1108, 1108, 1108, 1108,
	1108, 1108, 1108, 1108, 1108, 1108, 1108, 1108, 1108, 1108,
	1108, 1108, 283, 1108, 991, -1000, -1000, 270, 186, 258,
	369, 22, 967, -1000, -16, 119, 21, 81, 20, 815,
	1108, 1108, 1108, 1108, 1108, 1108, 1108, 1108, 1108, 1108,
	-1000, 275, 64, -17, -1000, 519, -1000, 270, 47, 19,
	-1000, 1013, 1013, 266, -30, 183, -1000, 126, -1000, 247,
	-1000, -1000, 166, 166, 264, 1108, 123, 1013, 18, 1045,
	1155, 1230, 1097, 1097, 1097, 1097, 1097, 1097, 110, 110,
	113, 113, 113, 113, 113, 113, 1131, -1000, 904, 210,
	-1000, 42, 17, -31, -1000, -1000, 232, -35, 1108, -1000,
	56, 273, -44, 335, 232, 173, -1000, 1108, 109, 1108,
	-1000, 106, 1108, 166, 1108, 783, 751, 719, 402, 687,
	655, 623, 591, 559, 527, -1000, -1000, 1013, -1000, 270,
	271, -1000, -37, -1000, 1013, -1000, 41, -1000, -1000, -1000,
	45, -1000, 186, 1108, -56, -40, -1000, 78, 11, -42,
	-1000, 71, 1045, 270, -1000, 40, 1108, -1000, 180, 967,
	1296, 186, 1013, -1000, 186, 10, 1045, -1000, -1000, 67,
	-1000, 46, -1000, 1045, -1000, 15, 1108, 1045, -1000, 15,
	845, 218, 1045, -1000, -1000, -1000, 1108, -1000, -1000, -1000,
	-1000, -1000, -1000, 77, -1000, -1000, -1000, -1000, 1013, 43,
	-57, 1108, 265, -45, 1045, 1108, -1000, 166, -1000, -1000,
	264, 166, 39, -1000, 1045, -1000, 335, 1108, 227, -1000,
	252, -34, 38, 1108, -1000, 75, 1108, -1000, 301, 74,
	1108, 1108, 495, -1000, -1000, -50, -1000, 1108, 1045, -1000,
	-51, 1045, -1000, -1000, -1000, 121, -14, 1045, 1108, -1000,
	186, 24, -1000, 1045, -1000, 301, -1000, -1000, -1000, 1045,
	875, -1000, -1000, 1045, 263, 1108, -59, -1000, 1045, -1000,
	-1000, -1000, -1000, 1045, 1108, 1045,
}
var yyPgo = [...]int{

	0, 30, 341, 6, 8, 340, 338, 2, 335, 19,
	7, 0, 289, 334, 333, 332, 9, 331, 15, 330,
	329, 4, 11, 325, 21, 39, 17, 12, 324, 323,
	16, 322, 321, 318, 317, 316, 311, 25, 5, 310,
	18, 299, 298, 1, 14, 3,
}
var yyR1 = [...]int{

	0, 42, 42, 42, 42, 20, 20, 21, 21, 21,
	21, 21, 21, 21, 21, 21, 21, 21, 21, 21,
	28, 28, 26, 25, 25, 22, 22, 23, 23, 24,
	37, 37, 37, 37, 37, 41, 40, 40, 39, 39,
	38, 38, 1, 1, 2, 2, 3, 3, 3, 10,
	10, 5, 5, 9, 9, 7, 7, 7, 7, 7,
	8, 6, 6, 4, 4, 4, 29, 29, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	12, 12, 12, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 15, 16, 45, 45, 31, 31, 30, 30,
	17, 17, 17, 18, 18, 33, 33, 32, 32, 19,
	19, 27, 34, 14, 14, 35, 35, 36, 36, 36,
	44, 44, 43, 43,
}
var yyR2 = [...]int{

	0, 3, 3, 3, 3, 1, 3, 1, 1, 1,
	1, 1, 1, 1, 3, 5, 3, 4, 3, 5,
	1, 3, 2, 1, 3, 1, 2, 1, 3, 1,
	1, 1, 3, 3, 3, 1, 1, 3, 1, 3,
	1, 3, 0, 3, 2, 3, 0, 1, 3, 1,
	1, 0, 3, 1, 1, 7, 2, 3, 7, 8,
	3, 3, 4, 2, 3, 4, 1, 3, 1, 5,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 5, 3, 4,
	1, 2, 2, 1, 1, 1, 1, 6, 7, 6,
	4, 7, 6, 4, 4, 6, 3, 4, 6, 7,
	3, 1, 4, 4, 4, 6, 4, 4, 4, 4,
	4, 4, 5, 5, 0, 1, 1, 3, 1, 3,
	0, 1, 3, 3, 4, 1, 3, 1, 3, 3,
	5, 1, 3, 0, 2, 0, 3, 0, 2, 4,
	0, 1, 0, 1,
}
var yyChk = [...]int{

	-1000, -42, 34, 35, 36, 37, -34, -14, 38, -1,
	-11, -12, 23, -13, 67, 59, 5, 4, 11, 12,
	19, 15, 25, 45, 44, 46, -15, 26, 6, 9,
	31, 32, 33, 29, 30, 28, 27, -21, 6, 9,
	7, 8, 11, 12, -20, 46, 44, 14, 45, 19,
	4, 68, -35, 5, 68, -9, -7, -8, 17, 18,
	4, 19, 43, 68, 45, 47, 48, 56, 57, 49,
	50, 51, 52, 58, 59, 62, 63, 64, 65, 53,
	54, 55, 71, 46, -11, -12, -12, 45, 45, 45,
	-11, -31, -2, -30, -9, 4, -17, 74, -19, -11,
	45, 45, 45, 45, 45, 45, 45, 45, 45, 45,
	68, 71, -21, -25, -26, -28, 4, 44, -24, -23,
	-22, -21, 45, -1, -36, 39, 78, 42, -6, -37,
	4, 66, 45, 46, 44, 20, 4, 4, -32, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, 4, -11, -16,
	44, -27, -25, -3, -10, -9, 4, 5, 77, 76,
	-44, 77, -9, -11, 4, 19, 78, 74, -44, 77,
	73, -44, 77, 60, 74, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, 4, 73, 74, 75, 77,
	77, -21, -25, 76, 77, -21, -24, 78, -4, 45,
	-29, 4, 45, 79, -21, -40, -37, -41, -40, -39,
	-38, 4, -11, 45, -21, -44, 77, 73, 24, -1,
	76, 77, 76, 76, 77, -33, -11, 75, -30, 4,
	78, -45, 78, -11, 73, -18, 40, -11, 73, -18,
	-11, -37, -11, 76, 76, 76, 77, 76, 76, 76,
	76, 76, 76, -21, -26, 4, 75, -22, 76, -5,
	-21, 79, 77, -3, -11, 79, 76, 77, 73, 75,
	77, 74, -27, 76, -11, -16, -11, 21, -21, -10,
	-21, -3, -44, 77, 75, -44, 40, 77, -11, -44,
	74, 22, -11, 73, -21, -4, 76, 79, -11, 4,
	76, -11, -37, -38, -37, 76, -45, -11, 21, 10,
	77, -44, 76, -11, 73, -11, -43, 78, 73, -11,
	-11, 76, 78, -11, -43, 79, -21, 75, -11, 76,
	-43, 73, -7, -11, 79, -11,
}
var yyDef = [...]int{

	0, -2, 143, 42, 0, 0, 0, 145, 0, 0,
	0, 68, 0, 90, 0, 0, 93, 94, 95, 96,
	0, 0, 0, 0, 0, 130, 111, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 7, 8,
	9, 10, 11, 12, 13, 0, 0, 0, 0, 0,
	5, 1, -2, 144, 2, 0, 53, 54, 0, 0,
	0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 91, 92, 0, 46, 0,
	0, 150, 0, 126, 0, 128, 150, 0, 150, 131,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	4, 0, 0, 0, 23, 0, 20, 0, 0, 29,
	27, 25, 0, 142, 0, 0, 43, 0, 56, 0,
	30, 31, 0, 0, 0, 0, 0, 0, 150, 137,
	70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
	80, 81, 82, 83, 84, 85, 86, 88, 0, 0,
	42, 0, 141, 0, 47, 49, 50, 0, 0, 110,
	0, 151, 0, 124, 94, 0, 44, 0, 0, 151,
	106, 0, 151, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 6, 14, 0, 16, 0,
	0, 22, 0, 18, 0, 26, 0, 146, 148, 51,
	0, 66, 46, 0, 0, 0, 36, 0, 35, 0,
	38, 40, 57, 0, 60, 0, 151, 89, 0, 0,
	0, 0, 0, 100, 46, 150, 135, 103, 127, 128,
	45, 0, 125, 129, 104, 150, 0, 132, 107, 150,
	0, 0, 139, 112, 113, 114, 0, 116, 117, 118,
	119, 120, 121, 0, 24, 21, 17, 28, 0, 0,
	63, 0, 0, 0, 61, 0, 32, 0, 33, 34,
	0, 0, 0, 69, 138, 87, 124, 0, 0, 48,
	0, 150, 0, 151, 122, 0, 0, 151, 152, 0,
	0, 0, 0, 15, 19, 0, 149, 0, 64, 67,
	152, 62, 37, 39, 41, 0, 0, 97, 0, 99,
	151, 0, 102, 136, 105, 152, 133, 153, 108, 140,
	0, 115, 52, 65, 0, 0, 0, 123, 98, 101,
	134, 109, 55, 58, 0, 59,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 67, 3, 3, 3, 64, 65, 3,
	45, 76, 62, 58, 77, 59, 71, 63, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 74, 78,
	56, 79, 57, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 46, 3, 73, 61, 66, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 44, 60, 75,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 47, 48, 49, 50, 51, 52, 53, 54,
	55, 68, 69, 70, 72,
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
		//line reflow.y:120
		{
			yylex.(*Parser).Module = yyDollar[2].module
			return 0
		}
	case 2:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:125
		{
			yylex.(*Parser).Decls = yyDollar[2].decllist
			return 0
		}
	case 3:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:130
		{
			yylex.(*Parser).Expr = yyDollar[2].expr
			return 0
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:135
		{
			yylex.(*Parser).Type = yyDollar[2].typ
			return 0
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:146
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 6:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:148
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:151
		{
			yyVAL.typ = types.Int
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:152
		{
			yyVAL.typ = types.Float
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:153
		{
			yyVAL.typ = types.String
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:154
		{
			yyVAL.typ = types.Bool
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:155
		{
			yyVAL.typ = types.File
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:156
		{
			yyVAL.typ = types.Dir
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:157
		{
			yyVAL.typ = types.Ref(yyDollar[1].idents...)
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:158
		{
			yyVAL.typ = types.List(yyDollar[2].typ)
		}
	case 15:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:160
		{
			yyVAL.typ = types.Map(yyDollar[2].typ, yyDollar[4].typ)
		}
	case 16:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:162
		{
			yyVAL.typ = types.Struct(yyDollar[2].typfields...)
		}
	case 17:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:164
		{
			yyVAL.typ = types.Module(yyDollar[3].typfields, nil)
		}
	case 18:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:166
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
		//line reflow.y:178
		{
			yyVAL.typ = types.Func(yyDollar[5].typ, yyDollar[3].typfields...)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:182
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:184
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 22:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:188
		{
			for _, name := range yyDollar[1].idents {
				yyVAL.typfields = append(yyVAL.typfields, &types.Field{Name: name, T: yyDollar[2].typ})
			}
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:196
		{
			yyVAL.typfields = yyDollar[1].typfields
		}
	case 24:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:198
		{
			yyVAL.typfields = append(yyDollar[1].typfields, yyDollar[3].typfields...)
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:202
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, nil}
		}
	case 26:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:204
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, yyDollar[2].typ}
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:208
		{
			yyVAL.typeargs = []typearg{yyDollar[1].typearg}
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:210
		{
			yyVAL.typeargs = append(yyDollar[1].typeargs, yyDollar[3].typearg)
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:219
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
		//line reflow.y:261
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:263
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatIgnore}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:265
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatTuple, List: yyDollar[2].patlist}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:267
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatList, List: yyDollar[2].patlist}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:269
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatStruct, Map: make(map[string]*Pat)}
			for _, p := range yyDollar[2].structpats {
				yyVAL.pat.Map[p.field] = p.pat
			}
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:281
		{
			yyVAL.patlist = []*Pat{yyDollar[1].pat}
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:283
		{
			yyVAL.patlist = append(yyDollar[1].patlist, yyDollar[3].pat)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:287
		{
			yyVAL.structpats = []struct {
				field string
				pat   *Pat
			}{yyDollar[1].structpat}
		}
	case 39:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:292
		{
			yyVAL.structpats = append(yyDollar[1].structpats, yyDollar[3].structpat)
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:296
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:301
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, yyDollar[3].pat}
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:309
		{
			yyVAL.decllist = nil
		}
	case 43:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:311
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 44:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:315
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:317
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 46:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:320
		{
			yyVAL.decllist = nil
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:322
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 48:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:324
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[3].decl)
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:328
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
		//line reflow.y:339
		{
			yyVAL.decllist = nil
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:341
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 55:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:346
		{
			yyDollar[7].decl.Expr = &Expr{Position: yyDollar[7].decl.Expr.Position, Kind: ExprRequires, Left: yyDollar[7].decl.Expr, Decls: yyDollar[4].decllist}
			yyDollar[7].decl.Comment = yyDollar[1].pos.comment
			yyVAL.decl = yyDollar[7].decl
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:352
		{
			yyVAL.decl = yyDollar[2].decl
			yyVAL.decl.Comment = yyDollar[1].pos.comment
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:357
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 58:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:359
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Kind: ExprFunc,
				Args: yyDollar[4].typfields,
				Left: yyDollar[7].expr}}
		}
	case 59:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:364
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Position: yyDollar[1].pos.Position,
				Kind:     ExprAscribe,
				Type:     types.Func(yyDollar[6].typ, yyDollar[4].typfields...),
				Left:     &Expr{Kind: ExprFunc, Args: yyDollar[4].typfields, Left: yyDollar[8].expr}}}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:372
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: DeclType, Ident: yyDollar[2].expr.Ident, Type: yyDollar[3].typ}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:376
		{
			yyVAL.decl = &Decl{Position: yyDollar[3].expr.Position, Pat: yyDollar[1].pat, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 62:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:378
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
		//line reflow.y:394
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
		//line reflow.y:407
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{Position: yyDollar[1].posidents.pos, Comment: yyDollar[1].posidents.comments[0], Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]}, Kind: DeclAssign, Expr: yyDollar[3].expr}}
			}
		}
	case 65:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:415
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
		//line reflow.y:431
		{
			yyVAL.posidents = posIdents{yyDollar[1].expr.Position, []string{yyDollar[1].expr.Ident}, []string{yyDollar[1].expr.Comment}}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:433
		{
			yyVAL.posidents = posIdents{yyDollar[1].posidents.pos, append(yyDollar[1].posidents.idents, yyDollar[3].expr.Ident), append(yyDollar[1].posidents.comments, yyDollar[3].expr.Comment)}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:439
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprApply, Left: yyDollar[1].expr, Fields: yyDollar[3].exprfields}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:441
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "||", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:443
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:445
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:447
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:449
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:451
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:453
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "!=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:455
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "==", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:457
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "+", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:459
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "-", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:461
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "*", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:463
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "/", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:465
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "%", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:467
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:469
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:471
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:473
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "~>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:475
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:477
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprDeref, Left: yyDollar[1].expr, Ident: yyDollar[3].expr.Ident}
		}
	case 89:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:479
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIndex, Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:483
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "!", Left: yyDollar[2].expr}
		}
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:485
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "-", Left: yyDollar[2].expr}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:492
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:494
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:496
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 98:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:498
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 99:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:501
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 100:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:503
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 101:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:505
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 102:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:507
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 103:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:509
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:511
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 105:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:513
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:520
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 107:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:522
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:524
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 109:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:531
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCompr, Left: yyDollar[6].expr, Pat: yyDollar[4].pat, ComprExpr: yyDollar[2].expr}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:533
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:536
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "len", Left: yyDollar[3].expr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:538
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "int", Left: yyDollar[3].expr}
		}
	case 114:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:540
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "float", Left: yyDollar[3].expr}
		}
	case 115:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:542
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "zip", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:544
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "unzip", Left: yyDollar[3].expr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:546
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "flatten", Left: yyDollar[3].expr}
		}
	case 118:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:548
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "map", Left: yyDollar[3].expr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:550
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "list", Left: yyDollar[3].expr}
		}
	case 120:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:552
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "delay", Left: yyDollar[3].expr}
		}
	case 121:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:554
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "panic", Left: yyDollar[3].expr}
		}
	case 122:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:558
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 123:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:562
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:569
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:571
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:575
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:577
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 130:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:580
		{
			yyVAL.exprlist = nil
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:582
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:584
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:588
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 134:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:590
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:594
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:596
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:600
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:602
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:606
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 140:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:608
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:619
		{
			yyVAL.module = &ModuleImpl{Keyspace: yyDollar[1].expr, ParamDecls: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 143:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:622
		{
			yyVAL.expr = nil
		}
	case 144:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:624
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 145:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:627
		{
			yyVAL.decllist = nil
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:629
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 147:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:632
		{
			yyVAL.decllist = nil
		}
	case 148:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:634
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 149:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:636
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
