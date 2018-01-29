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
const tokType = 57386
const tokOrOr = 57387
const tokAndAnd = 57388
const tokLE = 57389
const tokGE = 57390
const tokNE = 57391
const tokEqEq = 57392
const tokLSH = 57393
const tokRSH = 57394
const tokSquiggleArrow = 57395
const tokEOF = 57396
const tokError = 57397
const first = 57398
const apply = 57399

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
	-1, 53,
	79, 152,
	-2, 42,
}

const yyNprod = 159
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1377

var yyAct = [...]int{

	10, 336, 57, 219, 123, 255, 244, 166, 223, 211,
	38, 165, 161, 85, 163, 172, 122, 116, 248, 94,
	167, 218, 120, 354, 91, 317, 100, 282, 337, 343,
	56, 308, 9, 320, 234, 309, 286, 243, 287, 164,
	283, 284, 273, 210, 202, 95, 178, 303, 236, 237,
	51, 114, 39, 41, 42, 40, 128, 43, 44, 330,
	48, 235, 234, 300, 131, 50, 141, 142, 143, 144,
	145, 146, 147, 148, 149, 150, 151, 152, 153, 154,
	155, 156, 157, 158, 304, 160, 125, 115, 201, 214,
	202, 47, 49, 46, 175, 284, 202, 229, 207, 184,
	181, 173, 187, 188, 189, 190, 191, 192, 193, 194,
	195, 196, 197, 180, 174, 183, 350, 332, 325, 290,
	275, 233, 204, 348, 279, 206, 278, 51, 208, 39,
	41, 42, 40, 301, 43, 44, 217, 48, 225, 240,
	199, 200, 50, 179, 227, 137, 56, 209, 288, 338,
	249, 249, 334, 313, 285, 182, 228, 221, 113, 205,
	65, 84, 316, 112, 52, 226, 215, 124, 47, 49,
	46, 239, 111, 138, 214, 110, 109, 108, 107, 106,
	246, 105, 250, 251, 247, 253, 83, 258, 104, 256,
	103, 102, 101, 241, 90, 232, 89, 88, 162, 119,
	179, 129, 252, 346, 51, 270, 39, 41, 42, 40,
	8, 43, 44, 127, 48, 88, 212, 281, 277, 50,
	271, 231, 61, 310, 274, 328, 137, 280, 132, 329,
	291, 169, 54, 293, 224, 59, 60, 62, 295, 319,
	297, 289, 296, 118, 292, 47, 49, 46, 272, 298,
	305, 242, 61, 56, 299, 198, 132, 11, 311, 159,
	139, 138, 63, 312, 302, 59, 60, 62, 306, 136,
	134, 135, 86, 87, 1, 257, 220, 222, 203, 318,
	314, 126, 53, 321, 6, 238, 315, 55, 322, 140,
	92, 133, 324, 213, 117, 327, 323, 136, 134, 135,
	326, 333, 121, 45, 335, 254, 99, 97, 339, 26,
	168, 341, 7, 256, 331, 340, 13, 58, 344, 133,
	130, 276, 345, 59, 60, 62, 65, 84, 93, 349,
	347, 0, 0, 0, 80, 81, 0, 351, 296, 74,
	75, 0, 0, 76, 77, 78, 79, 353, 352, 0,
	63, 0, 83, 0, 0, 355, 65, 84, 66, 67,
	70, 71, 72, 73, 80, 81, 82, 68, 69, 74,
	75, 61, 0, 76, 77, 78, 79, 2, 3, 4,
	5, 0, 83, 0, 59, 60, 62, 0, 0, 337,
	65, 84, 66, 67, 70, 71, 72, 73, 80, 81,
	82, 68, 69, 74, 75, 96, 0, 76, 77, 78,
	79, 63, 0, 0, 0, 0, 83, 0, 59, 60,
	62, 0, 0, 245, 65, 84, 66, 67, 70, 71,
	72, 73, 80, 81, 82, 68, 69, 74, 75, 0,
	0, 76, 77, 78, 79, 63, 17, 16, 28, 0,
	83, 29, 0, 18, 19, 171, 170, 21, 0, 0,
	0, 20, 0, 0, 0, 12, 0, 22, 27, 36,
	35, 37, 33, 34, 30, 31, 32, 65, 84, 0,
	0, 0, 0, 0, 0, 80, 81, 24, 23, 25,
	0, 0, 0, 0, 76, 77, 78, 79, 0, 0,
	0, 0, 15, 83, 0, 0, 0, 0, 0, 0,
	14, 0, 0, 0, 0, 0, 0, 98, 65, 84,
	66, 67, 70, 71, 72, 73, 80, 81, 82, 68,
	69, 74, 75, 0, 0, 76, 77, 78, 79, 0,
	0, 0, 0, 0, 83, 0, 0, 0, 0, 0,
	262, 65, 84, 66, 67, 70, 71, 72, 73, 80,
	81, 82, 68, 69, 74, 75, 0, 0, 76, 77,
	78, 79, 0, 0, 0, 0, 0, 83, 0, 0,
	0, 0, 342, 65, 84, 66, 67, 70, 71, 72,
	73, 80, 81, 82, 68, 69, 74, 75, 0, 0,
	76, 77, 78, 79, 0, 0, 0, 0, 0, 83,
	0, 0, 0, 0, 269, 65, 84, 66, 67, 70,
	71, 72, 73, 80, 81, 82, 68, 69, 74, 75,
	0, 0, 76, 77, 78, 79, 0, 0, 0, 0,
	0, 83, 0, 0, 0, 0, 268, 65, 84, 66,
	67, 70, 71, 72, 73, 80, 81, 82, 68, 69,
	74, 75, 0, 0, 76, 77, 78, 79, 0, 0,
	0, 0, 0, 83, 0, 0, 0, 0, 267, 65,
	84, 66, 67, 70, 71, 72, 73, 80, 81, 82,
	68, 69, 74, 75, 0, 0, 76, 77, 78, 79,
	0, 0, 0, 0, 0, 83, 0, 0, 0, 0,
	266, 65, 84, 66, 67, 70, 71, 72, 73, 80,
	81, 82, 68, 69, 74, 75, 0, 0, 76, 77,
	78, 79, 0, 0, 0, 0, 0, 83, 0, 0,
	0, 0, 265, 65, 84, 66, 67, 70, 71, 72,
	73, 80, 81, 82, 68, 69, 74, 75, 0, 0,
	76, 77, 78, 79, 0, 0, 0, 0, 0, 83,
	0, 0, 0, 0, 264, 65, 84, 66, 67, 70,
	71, 72, 73, 80, 81, 82, 68, 69, 74, 75,
	0, 0, 76, 77, 78, 79, 0, 0, 0, 0,
	0, 83, 0, 0, 0, 0, 263, 65, 84, 66,
	67, 70, 71, 72, 73, 80, 81, 82, 68, 69,
	74, 75, 0, 0, 76, 77, 78, 79, 0, 0,
	0, 0, 0, 83, 0, 0, 0, 0, 261, 65,
	84, 66, 67, 70, 71, 72, 73, 80, 81, 82,
	68, 69, 74, 75, 0, 0, 76, 77, 78, 79,
	0, 0, 0, 0, 0, 83, 0, 0, 0, 0,
	260, 65, 84, 66, 67, 70, 71, 72, 73, 80,
	81, 82, 68, 69, 74, 75, 0, 0, 76, 77,
	78, 79, 0, 0, 0, 0, 0, 83, 0, 0,
	0, 0, 259, 65, 84, 66, 67, 70, 71, 72,
	73, 80, 81, 82, 68, 69, 74, 75, 185, 0,
	76, 77, 78, 79, 0, 0, 0, 0, 0, 83,
	0, 0, 186, 65, 84, 66, 67, 70, 71, 72,
	73, 80, 81, 82, 68, 69, 74, 75, 0, 0,
	76, 77, 78, 79, 0, 0, 176, 16, 28, 83,
	0, 29, 307, 18, 19, 0, 0, 21, 0, 59,
	60, 177, 0, 0, 0, 12, 0, 22, 27, 36,
	35, 37, 33, 34, 30, 31, 32, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 63, 24, 23, 25,
	0, 51, 0, 39, 41, 42, 40, 0, 43, 44,
	0, 48, 15, 0, 0, 0, 50, 0, 0, 0,
	14, 65, 84, 66, 67, 70, 71, 72, 73, 80,
	81, 82, 68, 69, 74, 75, 0, 0, 76, 77,
	78, 79, 47, 49, 46, 0, 0, 83, 0, 230,
	0, 162, 65, 84, 66, 67, 70, 71, 72, 73,
	80, 81, 82, 68, 69, 74, 75, 0, 0, 76,
	77, 78, 79, 0, 0, 0, 0, 216, 83, 65,
	84, 66, 67, 70, 71, 72, 73, 80, 81, 82,
	68, 69, 74, 75, 0, 0, 76, 77, 78, 79,
	0, 0, 64, 0, 0, 83, 65, 84, 66, 67,
	70, 71, 72, 73, 80, 81, 82, 68, 69, 74,
	75, 0, 0, 76, 77, 78, 79, 0, 17, 16,
	28, 0, 83, 29, 0, 18, 19, 0, 0, 21,
	0, 0, 0, 20, 0, 0, 0, 12, 0, 22,
	27, 36, 35, 37, 33, 34, 30, 31, 32, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 24,
	23, 25, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 15, 0, 0, 0, 0, 0,
	0, 0, 14, 65, 84, 66, 67, 70, 71, 72,
	73, 80, 81, 0, 68, 69, 74, 75, 0, 0,
	76, 77, 78, 79, 0, 17, 16, 28, 0, 83,
	29, 0, 18, 19, 0, 0, 21, 0, 0, 0,
	20, 0, 0, 0, 0, 0, 22, 27, 36, 35,
	37, 33, 34, 30, 31, 32, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 24, 23, 25, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 65,
	84, 15, 67, 70, 71, 72, 73, 80, 81, 14,
	68, 69, 74, 75, 0, 0, 76, 77, 78, 79,
	0, 0, 65, 84, 0, 83, 70, 71, 72, 73,
	80, 81, 0, 68, 69, 74, 75, 0, 0, 76,
	77, 78, 79, 0, 0, 0, 0, 51, 83, 39,
	41, 42, 40, 0, 43, 44, 0, 48, 0, 0,
	0, 0, 50, 51, 294, 39, 41, 42, 40, 0,
	43, 44, 0, 48, 0, 0, 0, 0, 50, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 47, 49,
	46, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 47, 49, 46,
}
var yyPact = [...]int{

	342, -1000, 171, -1000, 1124, 1329, 95, -1000, 227, 218,
	1033, -1000, 1124, -1000, 1211, 1211, -1000, -1000, -1000, -1000,
	151, 150, 148, 1124, 401, 442, -1000, 146, 145, 144,
	142, 135, 133, 132, 131, 130, 129, 126, 94, -1000,
	-1000, -1000, -1000, -1000, -1000, 86, 1329, 239, 154, 1329,
	121, -1000, -1000, 173, -1000, -1000, -23, -1000, -1000, 158,
	224, 206, 257, 256, -1000, 1124, 1124, 1124, 1124, 1124,
	1124, 1124, 1124, 1124, 1124, 1124, 1124, 1124, 1124, 1124,
	1124, 1124, 1124, 255, 1124, 1006, -1000, -1000, 239, 306,
	226, 378, 23, 952, -1000, -33, 125, 22, 81, 21,
	857, 1124, 1124, 1124, 1124, 1124, 1124, 1124, 1124, 1124,
	1124, 1124, -1000, 251, 66, 12, -1000, 200, -1000, 239,
	48, 20, -1000, 1329, 1329, 367, -36, 170, -1000, 120,
	-1000, 997, -1000, -1000, 224, 224, 230, 1124, 119, 1329,
	19, 1060, 1223, 1246, 280, 280, 280, 280, 280, 280,
	431, 431, 114, 114, 114, 114, 114, 114, 1147, -1000,
	975, 197, -1000, 44, 18, -16, -1000, -1000, 206, -29,
	1124, -1000, 63, 247, -42, 344, 206, 169, -1000, 1124,
	110, 1124, -1000, 109, 1124, 252, 1124, 825, 793, 761,
	472, 729, 697, 665, 633, 601, 569, 537, -1000, -1000,
	1329, -1000, 239, 244, -1000, -34, -1000, 1329, -1000, 43,
	-1000, -1000, -1000, 46, -1000, 306, 1124, -53, -37, -1000,
	80, 17, -40, -1000, 73, 1060, 239, -1000, 42, 1124,
	-1000, 153, 952, 1313, 306, 1329, -1000, 306, -15, 1060,
	-1000, -1000, 68, -1000, 57, -1000, 1060, -1000, 6, 1124,
	1060, -1000, 6, 887, -43, -1000, 201, 1124, 1060, -1000,
	-1000, -1000, 1124, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	79, -1000, -1000, -1000, -1000, 1329, 85, -55, 1124, 235,
	-44, 1060, 1124, -1000, 224, -1000, -1000, 230, 224, 41,
	-1000, 1060, -1000, 344, 1124, 204, -1000, 219, -19, 40,
	1124, -1000, 78, 1124, -1000, 310, 75, 1124, -1000, 252,
	1124, 1060, 505, -1000, -1000, -50, -1000, 1124, 1060, -1000,
	-51, 1060, -1000, -1000, -1000, 123, 47, 1060, 1124, -1000,
	306, 39, -1000, 1060, -1000, 310, -1000, -1000, -1000, 1060,
	-1000, 1060, -1000, -1000, 1060, 248, 1124, -57, -1000, 1060,
	-1000, -1000, -1000, 1060, 1124, 1060,
}
var yyPgo = [...]int{

	0, 32, 328, 11, 9, 321, 320, 2, 317, 20,
	7, 0, 257, 316, 312, 309, 12, 307, 18, 306,
	305, 5, 303, 4, 16, 302, 22, 39, 17, 14,
	294, 293, 19, 290, 289, 285, 284, 282, 281, 3,
	8, 277, 21, 276, 274, 1, 15, 6,
}
var yyR1 = [...]int{

	0, 44, 44, 44, 44, 22, 22, 23, 23, 23,
	23, 23, 23, 23, 23, 23, 23, 23, 23, 23,
	30, 30, 28, 27, 27, 24, 24, 25, 25, 26,
	39, 39, 39, 39, 39, 43, 42, 42, 41, 41,
	40, 40, 1, 1, 2, 2, 3, 3, 3, 10,
	10, 5, 5, 9, 9, 7, 7, 7, 7, 7,
	8, 6, 6, 4, 4, 4, 31, 31, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	12, 12, 12, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 15, 16, 20, 20, 21, 21, 47,
	47, 33, 33, 32, 32, 17, 17, 17, 18, 18,
	35, 35, 34, 34, 19, 19, 29, 36, 14, 14,
	37, 37, 38, 38, 38, 46, 46, 45, 45,
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
	4, 7, 6, 4, 4, 6, 3, 4, 6, 5,
	3, 1, 4, 4, 4, 6, 4, 4, 4, 4,
	4, 4, 4, 5, 5, 1, 3, 3, 2, 0,
	1, 1, 3, 1, 3, 0, 1, 3, 3, 4,
	1, 3, 1, 3, 3, 5, 1, 3, 0, 2,
	0, 3, 0, 2, 4, 0, 1, 0, 1,
}
var yyChk = [...]int{

	-1000, -44, 35, 36, 37, 38, -36, -14, 39, -1,
	-11, -12, 23, -13, 68, 60, 5, 4, 11, 12,
	19, 15, 25, 46, 45, 47, -15, 26, 6, 9,
	32, 33, 34, 30, 31, 28, 27, 29, -23, 6,
	9, 7, 8, 11, 12, -22, 47, 45, 14, 46,
	19, 4, 69, -37, 5, 69, -9, -7, -8, 17,
	18, 4, 19, 44, 69, 46, 48, 49, 57, 58,
	50, 51, 52, 53, 59, 60, 63, 64, 65, 66,
	54, 55, 56, 72, 47, -11, -12, -12, 46, 46,
	46, -11, -33, -2, -32, -9, 4, -17, 75, -19,
	-11, 46, 46, 46, 46, 46, 46, 46, 46, 46,
	46, 46, 69, 72, -23, -27, -28, -30, 4, 45,
	-26, -25, -24, -23, 46, -1, -38, 40, 79, 43,
	-6, -39, 4, 67, 46, 47, 45, 20, 4, 4,
	-34, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, 4,
	-11, -16, 45, -29, -27, -3, -10, -9, 4, 5,
	78, 77, -46, 78, -9, -11, 4, 19, 79, 75,
	-46, 78, 74, -46, 78, 61, 75, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, 4, 74,
	75, 76, 78, 78, -23, -27, 77, 78, -23, -26,
	79, -4, 46, -31, 4, 46, 80, -23, -42, -39,
	-43, -42, -41, -40, 4, -11, 46, -23, -46, 78,
	74, 24, -1, 77, 78, 77, 77, 78, -35, -11,
	76, -32, 4, 79, -47, 79, -11, 74, -18, 41,
	-11, 74, -18, -11, -20, -21, -39, 23, -11, 77,
	77, 77, 78, 77, 77, 77, 77, 77, 77, 77,
	-23, -28, 4, 76, -24, 77, -5, -23, 80, 78,
	-3, -11, 80, 77, 78, 74, 76, 78, 75, -29,
	77, -11, -16, -11, 21, -23, -10, -23, -3, -46,
	78, 76, -46, 41, 78, -11, -46, 75, 74, 78,
	22, -11, -11, 74, -23, -4, 77, 80, -11, 4,
	77, -11, -39, -40, -39, 77, -47, -11, 21, 10,
	78, -46, 77, -11, 74, -11, -45, 79, 74, -11,
	-21, -11, 77, 79, -11, -45, 80, -23, 76, -11,
	77, -45, -7, -11, 80, -11,
}
var yyDef = [...]int{

	0, -2, 148, 42, 0, 0, 0, 150, 0, 0,
	0, 68, 0, 90, 0, 0, 93, 94, 95, 96,
	0, 0, 0, 0, 0, 135, 111, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
	8, 9, 10, 11, 12, 13, 0, 0, 0, 0,
	0, 5, 1, -2, 149, 2, 0, 53, 54, 0,
	0, 0, 0, 0, 3, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 91, 92, 0, 46,
	0, 0, 155, 0, 131, 0, 133, 155, 0, 155,
	136, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 4, 0, 0, 0, 23, 0, 20, 0,
	0, 29, 27, 25, 0, 147, 0, 0, 43, 0,
	56, 0, 30, 31, 0, 0, 0, 0, 0, 0,
	155, 142, 70, 71, 72, 73, 74, 75, 76, 77,
	78, 79, 80, 81, 82, 83, 84, 85, 86, 88,
	0, 0, 42, 0, 146, 0, 47, 49, 50, 0,
	0, 110, 0, 156, 0, 129, 94, 0, 44, 0,
	0, 156, 106, 0, 156, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 6, 14,
	0, 16, 0, 0, 22, 0, 18, 0, 26, 0,
	151, 153, 51, 0, 66, 46, 0, 0, 0, 36,
	0, 35, 0, 38, 40, 57, 0, 60, 0, 156,
	89, 0, 0, 0, 0, 0, 100, 46, 155, 140,
	103, 132, 133, 45, 0, 130, 134, 104, 155, 0,
	137, 107, 155, 0, 0, 125, 0, 0, 144, 112,
	113, 114, 0, 116, 117, 118, 119, 120, 121, 122,
	0, 24, 21, 17, 28, 0, 0, 63, 0, 0,
	0, 61, 0, 32, 0, 33, 34, 0, 0, 0,
	69, 143, 87, 129, 0, 0, 48, 0, 155, 0,
	156, 123, 0, 0, 156, 157, 0, 0, 109, 0,
	0, 128, 0, 15, 19, 0, 154, 0, 64, 67,
	157, 62, 37, 39, 41, 0, 0, 97, 0, 99,
	156, 0, 102, 141, 105, 157, 138, 158, 108, 145,
	126, 127, 115, 52, 65, 0, 0, 0, 124, 98,
	101, 139, 55, 58, 0, 59,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 68, 3, 3, 3, 65, 66, 3,
	46, 77, 63, 59, 78, 60, 72, 64, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 75, 79,
	57, 80, 58, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 47, 3, 74, 62, 67, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 45, 61, 76,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 69, 70, 71, 73,
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
		//line reflow.y:125
		{
			yylex.(*Parser).Module = yyDollar[2].module
			return 0
		}
	case 2:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:130
		{
			yylex.(*Parser).Decls = yyDollar[2].decllist
			return 0
		}
	case 3:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:135
		{
			yylex.(*Parser).Expr = yyDollar[2].expr
			return 0
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:140
		{
			yylex.(*Parser).Type = yyDollar[2].typ
			return 0
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:151
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 6:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:153
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:156
		{
			yyVAL.typ = types.Int
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:157
		{
			yyVAL.typ = types.Float
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:158
		{
			yyVAL.typ = types.String
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:159
		{
			yyVAL.typ = types.Bool
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:160
		{
			yyVAL.typ = types.File
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:161
		{
			yyVAL.typ = types.Dir
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:162
		{
			yyVAL.typ = types.Ref(yyDollar[1].idents...)
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:163
		{
			yyVAL.typ = types.List(yyDollar[2].typ)
		}
	case 15:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:165
		{
			yyVAL.typ = types.Map(yyDollar[2].typ, yyDollar[4].typ)
		}
	case 16:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:167
		{
			yyVAL.typ = types.Struct(yyDollar[2].typfields...)
		}
	case 17:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:169
		{
			yyVAL.typ = types.Module(yyDollar[3].typfields, nil)
		}
	case 18:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:171
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
		//line reflow.y:183
		{
			yyVAL.typ = types.Func(yyDollar[5].typ, yyDollar[3].typfields...)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:187
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:189
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 22:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:193
		{
			for _, name := range yyDollar[1].idents {
				yyVAL.typfields = append(yyVAL.typfields, &types.Field{Name: name, T: yyDollar[2].typ})
			}
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:201
		{
			yyVAL.typfields = yyDollar[1].typfields
		}
	case 24:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:203
		{
			yyVAL.typfields = append(yyDollar[1].typfields, yyDollar[3].typfields...)
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:207
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, nil}
		}
	case 26:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:209
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, yyDollar[2].typ}
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:213
		{
			yyVAL.typeargs = []typearg{yyDollar[1].typearg}
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:215
		{
			yyVAL.typeargs = append(yyDollar[1].typeargs, yyDollar[3].typearg)
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:224
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
		//line reflow.y:266
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:268
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatIgnore}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:270
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatTuple, List: yyDollar[2].patlist}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:272
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatList, List: yyDollar[2].patlist}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:274
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatStruct, Map: make(map[string]*Pat)}
			for _, p := range yyDollar[2].structpats {
				yyVAL.pat.Map[p.field] = p.pat
			}
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:286
		{
			yyVAL.patlist = []*Pat{yyDollar[1].pat}
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:288
		{
			yyVAL.patlist = append(yyDollar[1].patlist, yyDollar[3].pat)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:292
		{
			yyVAL.structpats = []struct {
				field string
				pat   *Pat
			}{yyDollar[1].structpat}
		}
	case 39:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:297
		{
			yyVAL.structpats = append(yyDollar[1].structpats, yyDollar[3].structpat)
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:301
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:306
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, yyDollar[3].pat}
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:314
		{
			yyVAL.decllist = nil
		}
	case 43:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:316
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 44:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:320
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:322
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 46:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:325
		{
			yyVAL.decllist = nil
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:327
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 48:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:329
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[3].decl)
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:333
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
		//line reflow.y:344
		{
			yyVAL.decllist = nil
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:346
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 55:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:351
		{
			yyDollar[7].decl.Expr = &Expr{Position: yyDollar[7].decl.Expr.Position, Kind: ExprRequires, Left: yyDollar[7].decl.Expr, Decls: yyDollar[4].decllist}
			yyDollar[7].decl.Comment = yyDollar[1].pos.comment
			yyVAL.decl = yyDollar[7].decl
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:357
		{
			yyVAL.decl = yyDollar[2].decl
			yyVAL.decl.Comment = yyDollar[1].pos.comment
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:362
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 58:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:364
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Kind: ExprFunc,
				Args: yyDollar[4].typfields,
				Left: yyDollar[7].expr}}
		}
	case 59:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:369
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Position: yyDollar[1].pos.Position,
				Kind:     ExprAscribe,
				Type:     types.Func(yyDollar[6].typ, yyDollar[4].typfields...),
				Left:     &Expr{Kind: ExprFunc, Args: yyDollar[4].typfields, Left: yyDollar[8].expr}}}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:377
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: DeclType, Ident: yyDollar[2].expr.Ident, Type: yyDollar[3].typ}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:381
		{
			yyVAL.decl = &Decl{Position: yyDollar[3].expr.Position, Pat: yyDollar[1].pat, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 62:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:383
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
		//line reflow.y:399
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
		//line reflow.y:412
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{Position: yyDollar[1].posidents.pos, Comment: yyDollar[1].posidents.comments[0], Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]}, Kind: DeclAssign, Expr: yyDollar[3].expr}}
			}
		}
	case 65:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:420
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
		//line reflow.y:436
		{
			yyVAL.posidents = posIdents{yyDollar[1].expr.Position, []string{yyDollar[1].expr.Ident}, []string{yyDollar[1].expr.Comment}}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:438
		{
			yyVAL.posidents = posIdents{yyDollar[1].posidents.pos, append(yyDollar[1].posidents.idents, yyDollar[3].expr.Ident), append(yyDollar[1].posidents.comments, yyDollar[3].expr.Comment)}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:444
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprApply, Left: yyDollar[1].expr, Fields: yyDollar[3].exprfields}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:446
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "||", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:448
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:450
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:452
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:454
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:456
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:458
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "!=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:460
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "==", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:462
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "+", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:464
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "-", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:466
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "*", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:468
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "/", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:470
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "%", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:472
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:474
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:476
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:478
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "~>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:480
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:482
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprDeref, Left: yyDollar[1].expr, Ident: yyDollar[3].expr.Ident}
		}
	case 89:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:484
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIndex, Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:488
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "!", Left: yyDollar[2].expr}
		}
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:490
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "-", Left: yyDollar[2].expr}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:497
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:499
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:501
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 98:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:503
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 99:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:506
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 100:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:508
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 101:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:510
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 102:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:512
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 103:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:514
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:516
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 105:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:518
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:525
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 107:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:527
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:529
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:536
		{
			yyVAL.expr = &Expr{
				Position:     yyDollar[1].pos.Position,
				Comment:      yyDollar[1].pos.comment,
				Kind:         ExprCompr,
				ComprExpr:    yyDollar[2].expr,
				ComprClauses: yyDollar[4].comprclauses,
			}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:546
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:549
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "len", Left: yyDollar[3].expr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:551
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "int", Left: yyDollar[3].expr}
		}
	case 114:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:553
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "float", Left: yyDollar[3].expr}
		}
	case 115:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:555
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "zip", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:557
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "unzip", Left: yyDollar[3].expr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:559
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "flatten", Left: yyDollar[3].expr}
		}
	case 118:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:561
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "map", Left: yyDollar[3].expr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:563
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "list", Left: yyDollar[3].expr}
		}
	case 120:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:565
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "delay", Left: yyDollar[3].expr}
		}
	case 121:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:567
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "panic", Left: yyDollar[3].expr}
		}
	case 122:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:569
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "trace", Left: yyDollar[3].expr}
		}
	case 123:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:573
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 124:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:577
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:581
		{
			yyVAL.comprclauses = []*ComprClause{yyDollar[1].comprclause}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:583
		{
			yyVAL.comprclauses = append(yyDollar[1].comprclauses, yyDollar[3].comprclause)
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:587
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprEnum, Pat: yyDollar[1].pat, Expr: yyDollar[3].expr}
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:589
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprFilter, Expr: yyDollar[2].expr}
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:596
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:598
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:602
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:604
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 135:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:607
		{
			yyVAL.exprlist = nil
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:609
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:611
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:615
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 139:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:617
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:621
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:623
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:627
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:629
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:633
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 145:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:635
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:646
		{
			yyVAL.module = &ModuleImpl{Keyspace: yyDollar[1].expr, ParamDecls: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 148:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:649
		{
			yyVAL.expr = nil
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:651
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 150:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:654
		{
			yyVAL.decllist = nil
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:656
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 152:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:659
		{
			yyVAL.decllist = nil
		}
	case 153:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:661
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 154:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:663
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
