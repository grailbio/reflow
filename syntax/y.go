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
	82, 154,
	-2, 42,
}

const yyNprod = 161
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1393

var yyAct = [...]int{

	10, 233, 57, 343, 221, 124, 162, 167, 258, 247,
	225, 38, 123, 85, 86, 87, 173, 164, 213, 166,
	117, 168, 251, 91, 94, 100, 220, 121, 363, 323,
	286, 56, 9, 344, 351, 326, 313, 165, 290, 291,
	237, 337, 314, 277, 204, 95, 51, 308, 39, 41,
	42, 40, 115, 43, 44, 287, 48, 246, 239, 238,
	288, 50, 212, 240, 237, 132, 141, 142, 143, 144,
	145, 146, 147, 148, 149, 150, 151, 152, 153, 154,
	155, 156, 157, 158, 160, 116, 126, 309, 47, 49,
	46, 203, 204, 305, 176, 179, 129, 288, 204, 232,
	209, 185, 188, 189, 190, 191, 192, 193, 194, 195,
	196, 197, 198, 199, 181, 175, 184, 182, 174, 357,
	306, 243, 201, 283, 206, 282, 202, 180, 138, 292,
	210, 359, 339, 216, 331, 294, 279, 252, 219, 227,
	252, 236, 83, 82, 208, 345, 229, 341, 56, 319,
	79, 80, 289, 211, 183, 73, 74, 114, 207, 75,
	76, 77, 78, 223, 113, 61, 139, 83, 82, 84,
	133, 254, 242, 52, 250, 216, 231, 297, 59, 60,
	62, 249, 228, 253, 83, 82, 256, 180, 261, 260,
	217, 259, 79, 80, 84, 125, 235, 112, 111, 244,
	163, 75, 76, 77, 78, 322, 63, 255, 274, 88,
	110, 84, 137, 135, 136, 169, 109, 108, 214, 285,
	133, 281, 278, 107, 106, 275, 105, 104, 59, 60,
	62, 55, 103, 295, 134, 61, 298, 284, 102, 101,
	90, 296, 300, 89, 302, 301, 293, 88, 59, 60,
	62, 120, 130, 310, 128, 315, 63, 56, 304, 8,
	303, 316, 137, 135, 136, 234, 317, 335, 307, 138,
	318, 336, 311, 170, 54, 226, 63, 96, 2, 3,
	4, 5, 325, 324, 134, 320, 119, 327, 276, 245,
	59, 60, 62, 328, 200, 161, 140, 330, 332, 321,
	334, 139, 329, 1, 222, 224, 340, 127, 333, 342,
	53, 6, 61, 346, 241, 159, 348, 92, 63, 259,
	338, 215, 118, 347, 352, 59, 60, 62, 122, 45,
	353, 257, 99, 97, 25, 7, 358, 355, 11, 356,
	58, 131, 280, 93, 0, 301, 360, 0, 0, 0,
	0, 0, 16, 15, 27, 362, 361, 28, 364, 17,
	18, 0, 0, 20, 365, 0, 0, 19, 0, 0,
	0, 12, 0, 21, 26, 36, 35, 37, 32, 34,
	29, 30, 31, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 33, 0, 23, 22, 24, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 51, 14,
	39, 41, 42, 40, 0, 43, 44, 13, 48, 0,
	0, 0, 0, 50, 0, 0, 0, 98, 83, 82,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	47, 49, 46, 0, 0, 84, 0, 0, 0, 0,
	0, 0, 0, 344, 83, 82, 65, 66, 69, 70,
	71, 72, 79, 80, 81, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 0, 0, 354, 0, 0,
	0, 84, 0, 0, 0, 0, 0, 0, 0, 248,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 172,
	0, 0, 0, 0, 171, 83, 82, 65, 66, 69,
	70, 71, 72, 79, 80, 81, 67, 68, 73, 74,
	0, 0, 75, 76, 77, 78, 0, 0, 0, 0,
	0, 0, 84, 0, 0, 0, 0, 0, 0, 269,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 0,
	0, 0, 0, 0, 265, 83, 82, 65, 66, 69,
	70, 71, 72, 79, 80, 81, 67, 68, 73, 74,
	186, 0, 75, 76, 77, 78, 0, 0, 0, 0,
	0, 0, 84, 0, 0, 0, 0, 187, 83, 82,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	0, 0, 0, 0, 0, 84, 0, 0, 0, 0,
	312, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 0, 0, 0, 0, 84, 0,
	350, 83, 82, 65, 66, 69, 70, 71, 72, 79,
	80, 81, 67, 68, 73, 74, 0, 0, 75, 76,
	77, 78, 0, 0, 177, 15, 27, 0, 84, 28,
	349, 17, 18, 0, 0, 20, 0, 59, 60, 178,
	0, 0, 0, 12, 0, 21, 26, 36, 35, 37,
	32, 34, 29, 30, 31, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 33, 63, 23, 22, 24, 51,
	0, 39, 41, 42, 40, 0, 43, 44, 0, 48,
	0, 14, 0, 0, 50, 0, 0, 0, 0, 13,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 47, 49, 46, 0, 0, 0, 84, 0, 273,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 218, 272,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 271,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 270,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 268,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 267,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 266,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 264,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 263,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 0, 262,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 230, 163,
	83, 82, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 0, 84, 83, 82,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	0, 64, 0, 0, 0, 84, 83, 82, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 16, 15, 27,
	0, 0, 28, 84, 17, 18, 0, 0, 20, 0,
	0, 0, 19, 0, 0, 0, 12, 0, 21, 26,
	36, 35, 37, 32, 34, 29, 30, 31, 51, 0,
	39, 41, 42, 40, 0, 43, 44, 33, 48, 23,
	22, 24, 0, 50, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 51, 14, 39, 41, 42, 40, 0,
	43, 44, 13, 48, 0, 0, 0, 0, 50, 0,
	47, 49, 46, 83, 82, 65, 66, 69, 70, 71,
	72, 79, 80, 0, 67, 68, 73, 74, 0, 0,
	75, 76, 77, 78, 0, 47, 49, 46, 0, 0,
	84, 0, 0, 83, 82, 205, 66, 69, 70, 71,
	72, 79, 80, 0, 67, 68, 73, 74, 0, 0,
	75, 76, 77, 78, 0, 0, 0, 83, 82, 0,
	84, 69, 70, 71, 72, 79, 80, 0, 67, 68,
	73, 74, 0, 0, 75, 76, 77, 78, 51, 0,
	39, 41, 42, 40, 84, 43, 44, 0, 48, 0,
	0, 0, 0, 50, 0, 299, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	47, 49, 46,
}
var yyPact = [...]int{

	243, -1000, 220, -1000, 1193, 1249, 103, -1000, 269, 161,
	1101, -1000, 1193, 1193, 1193, -1000, -1000, -1000, -1000, 200,
	196, 193, 1193, 273, 348, -1000, 192, 191, 185, 180,
	179, 177, 176, 170, 169, 163, 151, 150, 94, -1000,
	-1000, -1000, -1000, -1000, -1000, 83, 1249, 282, 205, 1249,
	148, -1000, -1000, 214, -1000, -1000, 14, -1000, -1000, 209,
	216, 249, 297, 292, -1000, 1193, 1193, 1193, 1193, 1193,
	1193, 1193, 1193, 1193, 1193, 1193, 1193, 1193, 1193, 1193,
	1193, 1193, 1193, 1193, 291, 1073, 120, 120, 282, 211,
	268, 453, 37, 720, -1000, 13, 108, 36, 79, 20,
	558, 1193, 1193, 1193, 1193, 1193, 1193, 1193, 1193, 1193,
	1193, 1193, 1193, -1000, 290, 47, 11, -1000, 1224, -1000,
	282, 68, 19, -1000, 1249, 1249, 231, -20, 171, -1000,
	143, -1000, 765, -1000, -1000, 216, 216, 271, 1193, 135,
	1249, 1256, 1280, 95, 95, 95, 95, 95, 95, 137,
	137, 120, 120, 120, 120, 120, 120, 1226, 1043, 18,
	1129, -1000, 241, -1000, 65, 17, -17, -1000, -1000, 249,
	-18, 1193, -1000, 41, 285, -25, 417, 249, 162, -1000,
	1193, 99, 1193, -1000, 96, 1193, 166, 1193, 1013, 983,
	953, 523, 923, 893, 863, 488, 833, 803, 773, 743,
	-1000, -1000, 1249, -1000, 282, 284, -1000, -37, -1000, 1249,
	-1000, 60, -1000, -1000, -1000, 42, -1000, 211, 1193, -53,
	-21, -1000, 77, 16, -42, -1000, 50, 1129, 282, -1000,
	-1000, 59, 1193, -1000, 154, 720, 1344, 211, 1249, -1000,
	211, 12, 1129, -1000, -1000, 48, -1000, 40, -1000, 1129,
	-1000, 6, 1193, 1129, -1000, 6, 591, -39, -1000, 233,
	1193, 1129, -1000, -1000, -1000, 1193, -1000, -1000, -1000, 1193,
	-1000, -1000, -1000, -1000, 74, -1000, -1000, -1000, -1000, 1249,
	129, -54, 1193, 278, -41, 1129, 1193, -1000, 216, -1000,
	-1000, 271, 216, 58, -1000, 1129, -1000, 1193, 417, 1193,
	246, -1000, 261, -40, 56, 1193, -1000, 72, 1193, -1000,
	381, 70, 1193, -1000, 166, 1193, 1129, 654, 624, -1000,
	-1000, -48, -1000, 1193, 1129, -1000, -49, 1129, -1000, -1000,
	-1000, 404, 1073, 39, 1129, 1193, -1000, 211, 55, -1000,
	1129, -1000, 381, -1000, -1000, -1000, 1129, -1000, 1129, -1000,
	-1000, -1000, 1129, 308, 1193, -55, 241, -1000, 1129, -1000,
	-1000, -1000, 1129, 1193, -1000, 1129,
}
var yyPgo = [...]int{

	0, 32, 343, 19, 18, 342, 341, 2, 340, 21,
	7, 0, 338, 335, 334, 6, 1, 333, 22, 332,
	331, 8, 329, 5, 12, 328, 27, 37, 20, 17,
	322, 321, 24, 317, 315, 314, 311, 310, 307, 4,
	10, 305, 26, 304, 303, 3, 16, 9,
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
	11, 11, 16, 16, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
	12, 12, 12, 12, 12, 14, 15, 20, 20, 21,
	21, 47, 47, 33, 33, 32, 32, 17, 17, 17,
	18, 18, 35, 35, 34, 34, 19, 19, 29, 36,
	13, 13, 37, 37, 38, 38, 38, 46, 46, 45,
	45,
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
	3, 3, 3, 3, 3, 3, 4, 4, 5, 3,
	2, 2, 2, 5, 1, 1, 1, 1, 6, 7,
	6, 4, 7, 6, 4, 4, 6, 3, 4, 6,
	5, 3, 1, 4, 4, 4, 6, 4, 4, 4,
	6, 4, 4, 4, 4, 5, 5, 1, 3, 3,
	2, 0, 1, 1, 3, 1, 3, 0, 1, 3,
	3, 4, 1, 3, 1, 3, 3, 5, 1, 3,
	0, 2, 0, 3, 0, 2, 4, 0, 1, 0,
	1,
}
var yyChk = [...]int{

	-1000, -44, 35, 36, 37, 38, -36, -13, 39, -1,
	-11, -12, 23, 69, 61, 5, 4, 11, 12, 19,
	15, 25, 47, 46, 48, -14, 26, 6, 9, 32,
	33, 34, 30, 44, 31, 28, 27, 29, -23, 6,
	9, 7, 8, 11, 12, -22, 48, 46, 14, 47,
	19, 4, 70, -37, 5, 70, -9, -7, -8, 17,
	18, 4, 19, 45, 70, 49, 50, 58, 59, 51,
	52, 53, 54, 60, 61, 64, 65, 66, 67, 55,
	56, 57, 48, 47, 74, -11, -11, -11, 47, 47,
	47, -11, -33, -2, -32, -9, 4, -17, 79, -19,
	-11, 47, 47, 47, 47, 47, 47, 47, 47, 47,
	47, 47, 47, 70, 74, -23, -27, -28, -30, 4,
	46, -26, -25, -24, -23, 47, -1, -38, 40, 82,
	43, -6, -39, 4, 68, 47, 48, 46, 20, 4,
	4, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -34,
	-11, 4, -15, 46, -29, -27, -3, -10, -9, 4,
	5, 81, 76, -46, 81, -9, -11, 4, 19, 82,
	79, -46, 81, 75, -46, 81, 62, 79, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	4, 75, 79, 80, 81, 81, -23, -27, 76, 81,
	-23, -26, 82, -4, 47, -31, 4, 47, 83, -23,
	-42, -39, -43, -42, -41, -40, 4, -11, 47, -23,
	75, -46, 81, -16, 24, -1, 76, 81, 76, 76,
	81, -35, -11, 80, -32, 4, 82, -47, 82, -11,
	75, -18, 41, -11, 75, -18, -11, -20, -21, -39,
	23, -11, 76, 76, 76, 81, 76, 76, 76, 81,
	76, 76, 76, 76, -23, -28, 4, 80, -24, 76,
	-5, -23, 83, 81, -3, -11, 83, 76, 81, 75,
	80, 81, 79, -29, 76, -11, -15, 23, -11, 21,
	-23, -10, -23, -3, -46, 81, 80, -46, 41, 81,
	-11, -46, 79, 75, 81, 22, -11, -11, -11, 75,
	-23, -4, 76, 83, -11, 4, 76, -11, -39, -40,
	-39, 76, -11, -47, -11, 21, 10, 81, -46, 76,
	-11, 75, -11, -45, 82, 75, -11, -21, -11, 76,
	76, 82, -11, -45, 83, -23, -15, 80, -11, 76,
	-45, -7, -11, 83, -16, -11,
}
var yyDef = [...]int{

	0, -2, 150, 42, 0, 0, 0, 152, 0, 0,
	0, 68, 0, 0, 0, 94, 95, 96, 97, 0,
	0, 0, 0, 0, 137, 112, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
	8, 9, 10, 11, 12, 13, 0, 0, 0, 0,
	0, 5, 1, -2, 151, 2, 0, 53, 54, 0,
	0, 0, 0, 0, 3, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 90, 91, 0, 46,
	0, 0, 157, 0, 133, 0, 135, 157, 0, 157,
	138, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 4, 0, 0, 0, 23, 0, 20,
	0, 0, 29, 27, 25, 0, 149, 0, 0, 43,
	0, 56, 0, 30, 31, 0, 0, 0, 0, 0,
	0, 69, 70, 71, 72, 73, 74, 75, 76, 77,
	78, 79, 80, 81, 82, 83, 84, 85, 0, 157,
	144, 89, 0, 42, 0, 148, 0, 47, 49, 50,
	0, 0, 111, 0, 158, 0, 131, 95, 0, 44,
	0, 0, 158, 107, 0, 158, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	6, 14, 0, 16, 0, 0, 22, 0, 18, 0,
	26, 0, 153, 155, 51, 0, 66, 46, 0, 0,
	0, 36, 0, 35, 0, 38, 40, 57, 0, 60,
	87, 0, 158, 86, 0, 0, 0, 0, 0, 101,
	46, 157, 142, 104, 134, 135, 45, 0, 132, 136,
	105, 157, 0, 139, 108, 157, 0, 0, 127, 0,
	0, 146, 113, 114, 115, 0, 117, 118, 119, 0,
	121, 122, 123, 124, 0, 24, 21, 17, 28, 0,
	0, 63, 0, 0, 0, 61, 0, 32, 0, 33,
	34, 0, 0, 0, 88, 145, 92, 0, 131, 0,
	0, 48, 0, 157, 0, 158, 125, 0, 0, 158,
	159, 0, 0, 110, 0, 0, 130, 0, 0, 15,
	19, 0, 156, 0, 64, 67, 159, 62, 37, 39,
	41, 0, 0, 0, 98, 0, 100, 158, 0, 103,
	143, 106, 159, 140, 160, 109, 147, 128, 129, 116,
	120, 52, 65, 0, 0, 0, 0, 126, 99, 102,
	141, 55, 58, 0, 93, 59,
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
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatStruct, Fields: make([]PatField, len(yyDollar[2].structpats))}
			for i, p := range yyDollar[2].structpats {
				yyVAL.pat.Fields[i] = PatField{p.field, p.pat}
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
				Pat:      &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident},
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
			yyVAL.decl = &Decl{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Pat: &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 58:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:365
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Position: yyDollar[1].pos.Position, Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Kind: ExprFunc,
				Args: yyDollar[4].typfields,
				Left: yyDollar[7].expr}}
		}
	case 59:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:370
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Position: yyDollar[1].pos.Position, Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
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
				yyVAL.decllist = []*Decl{{Position: yyDollar[1].posidents.pos, Comment: yyDollar[1].posidents.comments[0], Pat: &Pat{Position: yyDollar[1].posidents.pos, Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]}, Kind: DeclAssign, Expr: yyDollar[3].expr}}
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
					Pat:      &Pat{Position: yyDollar[1].posidents.pos, Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]},
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
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:479
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[4].expr}
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
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:493
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBlock, Left: yyDollar[2].expr}
		}
	case 93:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:495
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprCond, Cond: yyDollar[3].expr, Left: yyDollar[4].expr, Right: yyDollar[5].expr}
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:502
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:504
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 98:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:506
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 99:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:508
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 100:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:511
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 101:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:513
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 102:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:515
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 103:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:517
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:519
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 105:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:521
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 106:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:523
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:530
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:532
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 109:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:534
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 110:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:541
		{
			yyVAL.expr = &Expr{
				Position:     yyDollar[1].pos.Position,
				Comment:      yyDollar[1].pos.comment,
				Kind:         ExprCompr,
				ComprExpr:    yyDollar[2].expr,
				ComprClauses: yyDollar[4].comprclauses,
			}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:551
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:554
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "len", Left: yyDollar[3].expr}
		}
	case 114:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:556
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "int", Left: yyDollar[3].expr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:558
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "float", Left: yyDollar[3].expr}
		}
	case 116:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:560
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "zip", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:562
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "unzip", Left: yyDollar[3].expr}
		}
	case 118:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:564
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "flatten", Left: yyDollar[3].expr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:566
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "map", Left: yyDollar[3].expr}
		}
	case 120:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:568
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "range", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 121:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:570
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "list", Left: yyDollar[3].expr}
		}
	case 122:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:572
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "delay", Left: yyDollar[3].expr}
		}
	case 123:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:574
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "panic", Left: yyDollar[3].expr}
		}
	case 124:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:576
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "trace", Left: yyDollar[3].expr}
		}
	case 125:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:580
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 126:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:584
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:588
		{
			yyVAL.comprclauses = []*ComprClause{yyDollar[1].comprclause}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:590
		{
			yyVAL.comprclauses = append(yyDollar[1].comprclauses, yyDollar[3].comprclause)
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:594
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprEnum, Pat: yyDollar[1].pat, Expr: yyDollar[3].expr}
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:596
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprFilter, Expr: yyDollar[2].expr}
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:603
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:605
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:609
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:611
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 137:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:614
		{
			yyVAL.exprlist = nil
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:616
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:618
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:622
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 141:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:624
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
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
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:634
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:636
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:640
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 147:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:642
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:653
		{
			yyVAL.module = &ModuleImpl{Keyspace: yyDollar[1].expr, ParamDecls: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 150:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:656
		{
			yyVAL.expr = nil
		}
	case 151:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:658
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 152:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:661
		{
			yyVAL.decllist = nil
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:663
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 154:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:666
		{
			yyVAL.decllist = nil
		}
	case 155:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:668
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 156:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:670
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
