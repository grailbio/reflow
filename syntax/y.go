// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

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
	78, 151,
	-2, 42,
}

const yyNprod = 158
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1320

var yyAct = [...]int{

	10, 332, 56, 216, 121, 252, 241, 164, 220, 208,
	37, 163, 159, 84, 161, 170, 120, 114, 245, 93,
	165, 215, 118, 350, 90, 313, 99, 278, 333, 339,
	55, 304, 9, 316, 231, 305, 282, 240, 283, 162,
	279, 280, 269, 207, 199, 94, 299, 233, 234, 50,
	112, 38, 40, 41, 39, 176, 42, 43, 126, 47,
	232, 231, 326, 129, 49, 139, 140, 141, 142, 143,
	144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
	154, 155, 156, 300, 158, 123, 113, 296, 280, 46,
	48, 45, 198, 173, 199, 211, 199, 226, 204, 182,
	179, 185, 186, 187, 188, 189, 190, 191, 192, 193,
	194, 171, 178, 172, 181, 346, 328, 321, 286, 271,
	201, 344, 275, 230, 274, 203, 205, 297, 237, 196,
	197, 177, 135, 284, 214, 246, 222, 246, 334, 330,
	309, 281, 224, 180, 55, 206, 64, 83, 111, 110,
	51, 223, 212, 136, 225, 218, 50, 202, 38, 40,
	41, 39, 60, 42, 43, 122, 47, 312, 248, 236,
	244, 49, 82, 109, 108, 58, 59, 61, 243, 211,
	247, 107, 106, 250, 105, 255, 177, 253, 104, 103,
	102, 238, 130, 229, 87, 101, 46, 48, 45, 100,
	249, 62, 266, 50, 89, 38, 40, 41, 39, 88,
	42, 43, 87, 47, 277, 273, 160, 267, 49, 127,
	209, 270, 117, 125, 276, 8, 54, 287, 228, 306,
	289, 342, 134, 132, 133, 291, 324, 293, 285, 292,
	135, 288, 325, 46, 48, 45, 294, 301, 167, 60,
	55, 295, 11, 130, 131, 307, 2, 3, 4, 5,
	308, 298, 58, 59, 61, 302, 53, 85, 86, 221,
	315, 116, 254, 268, 239, 314, 310, 195, 213, 317,
	157, 1, 311, 137, 318, 136, 217, 219, 320, 124,
	52, 323, 319, 134, 132, 133, 322, 329, 6, 235,
	331, 138, 91, 210, 335, 115, 166, 337, 119, 253,
	327, 336, 44, 251, 340, 131, 98, 96, 341, 58,
	59, 61, 64, 83, 26, 345, 343, 7, 13, 57,
	79, 80, 128, 347, 292, 73, 74, 272, 92, 75,
	76, 77, 78, 349, 348, 62, 0, 0, 82, 0,
	0, 351, 64, 83, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 60, 75,
	76, 77, 78, 0, 0, 0, 0, 0, 82, 0,
	0, 58, 59, 61, 0, 333, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 95, 75, 76, 77, 78, 62, 0, 0,
	0, 0, 82, 0, 0, 58, 59, 61, 0, 242,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 62, 0, 0, 0, 0, 82, 0, 0, 0,
	0, 169, 168, 64, 83, 65, 66, 69, 70, 71,
	72, 79, 80, 81, 67, 68, 73, 74, 0, 0,
	75, 76, 77, 78, 0, 17, 16, 28, 0, 82,
	29, 0, 18, 19, 0, 259, 21, 0, 0, 0,
	20, 0, 0, 0, 12, 0, 22, 27, 36, 35,
	33, 34, 30, 31, 32, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 24, 23, 25, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 50,
	15, 38, 40, 41, 39, 0, 42, 43, 14, 47,
	0, 0, 0, 0, 49, 97, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 46,
	48, 45, 82, 0, 0, 0, 0, 338, 64, 83,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	0, 0, 200, 0, 82, 0, 0, 0, 0, 265,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 0, 0,
	0, 264, 64, 83, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 0, 0, 82, 0,
	0, 0, 0, 263, 64, 83, 65, 66, 69, 70,
	71, 72, 79, 80, 81, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 0, 0, 0, 0, 0,
	82, 0, 0, 0, 0, 262, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 82, 0, 0, 0, 0, 261, 64, 83,
	65, 66, 69, 70, 71, 72, 79, 80, 81, 67,
	68, 73, 74, 0, 0, 75, 76, 77, 78, 0,
	0, 0, 0, 0, 82, 0, 0, 0, 0, 260,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 0, 0, 0, 82, 0, 0, 0,
	0, 258, 64, 83, 65, 66, 69, 70, 71, 72,
	79, 80, 81, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 0, 0, 82, 0,
	0, 0, 0, 257, 64, 83, 65, 66, 69, 70,
	71, 72, 79, 80, 81, 67, 68, 73, 74, 0,
	0, 75, 76, 77, 78, 0, 0, 0, 0, 0,
	82, 0, 0, 0, 0, 256, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 183, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 82, 0, 0, 184, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 0, 0,
	0, 0, 82, 0, 0, 303, 64, 83, 65, 66,
	69, 70, 71, 72, 79, 80, 81, 67, 68, 73,
	74, 0, 0, 75, 76, 77, 78, 0, 174, 16,
	28, 0, 82, 29, 227, 18, 19, 0, 0, 21,
	0, 58, 59, 175, 0, 0, 0, 12, 0, 22,
	27, 36, 35, 33, 34, 30, 31, 32, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 62, 24, 23,
	25, 0, 0, 0, 50, 0, 38, 40, 41, 39,
	0, 42, 43, 15, 47, 0, 0, 0, 0, 49,
	0, 14, 160, 64, 83, 65, 66, 69, 70, 71,
	72, 79, 80, 81, 67, 68, 73, 74, 0, 0,
	75, 76, 77, 78, 46, 48, 45, 0, 0, 82,
	64, 83, 65, 66, 69, 70, 71, 72, 79, 80,
	81, 67, 68, 73, 74, 0, 0, 75, 76, 77,
	78, 0, 0, 63, 0, 0, 82, 64, 83, 65,
	66, 69, 70, 71, 72, 79, 80, 81, 67, 68,
	73, 74, 0, 0, 75, 76, 77, 78, 0, 17,
	16, 28, 0, 82, 29, 0, 18, 19, 0, 0,
	21, 0, 0, 0, 20, 0, 0, 0, 12, 0,
	22, 27, 36, 35, 33, 34, 30, 31, 32, 64,
	83, 0, 0, 0, 0, 0, 0, 79, 80, 24,
	23, 25, 0, 0, 0, 0, 75, 76, 77, 78,
	0, 0, 0, 0, 15, 82, 0, 0, 0, 0,
	0, 0, 14, 64, 83, 65, 66, 69, 70, 71,
	72, 79, 80, 0, 67, 68, 73, 74, 0, 0,
	75, 76, 77, 78, 0, 0, 0, 64, 83, 82,
	66, 69, 70, 71, 72, 79, 80, 0, 67, 68,
	73, 74, 0, 0, 75, 76, 77, 78, 0, 17,
	16, 28, 0, 82, 29, 0, 18, 19, 0, 0,
	21, 0, 0, 0, 20, 0, 0, 0, 0, 0,
	22, 27, 36, 35, 33, 34, 30, 31, 32, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 24,
	23, 25, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 64, 83, 15, 0, 69, 70, 71, 72,
	79, 80, 14, 67, 68, 73, 74, 0, 0, 75,
	76, 77, 78, 0, 0, 0, 0, 50, 82, 38,
	40, 41, 39, 0, 42, 43, 0, 47, 0, 0,
	0, 0, 49, 0, 290, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 46, 48, 45,
}
var yyPact = [...]int{

	222, -1000, 187, -1000, 1085, 990, 82, -1000, 261, 158,
	995, -1000, 1085, -1000, 1195, 1195, -1000, -1000, -1000, -1000,
	167, 164, 159, 1085, 398, 471, -1000, 154, 150, 145,
	144, 143, 139, 137, 136, 129, 128, 81, -1000, -1000,
	-1000, -1000, -1000, -1000, 77, 990, 267, 178, 990, 120,
	-1000, -1000, 184, -1000, -1000, -20, -1000, -1000, 177, 188,
	220, 281, 279, -1000, 1085, 1085, 1085, 1085, 1085, 1085,
	1085, 1085, 1085, 1085, 1085, 1085, 1085, 1085, 1085, 1085,
	1085, 1085, 276, 1085, 968, -1000, -1000, 267, 302, 243,
	375, 34, 944, -1000, -23, 112, 23, 70, 22, 821,
	1085, 1085, 1085, 1085, 1085, 1085, 1085, 1085, 1085, 1085,
	-1000, 273, 56, 17, -1000, 525, -1000, 267, 49, 21,
	-1000, 990, 990, 364, -35, 175, -1000, 107, -1000, 199,
	-1000, -1000, 188, 188, 265, 1085, 106, 990, 20, 1022,
	1132, 1207, 277, 277, 277, 277, 277, 277, 1074, 1074,
	101, 101, 101, 101, 101, 101, 1108, -1000, 881, 204,
	-1000, 47, 19, -16, -1000, -1000, 220, -29, 1085, -1000,
	53, 270, -41, 341, 220, 149, -1000, 1085, 97, 1085,
	-1000, 95, 1085, 249, 1085, 789, 757, 725, 408, 693,
	661, 629, 597, 565, 533, -1000, -1000, 990, -1000, 267,
	269, -1000, -33, -1000, 990, -1000, 43, -1000, -1000, -1000,
	45, -1000, 302, 1085, -52, -36, -1000, 68, 11, -39,
	-1000, 59, 1022, 267, -1000, 42, 1085, -1000, 172, 944,
	1273, 302, 990, -1000, 302, 10, 1022, -1000, -1000, 57,
	-1000, 52, -1000, 1022, -1000, 6, 1085, 1022, -1000, 6,
	851, -42, -1000, 207, 1085, 1022, -1000, -1000, -1000, 1085,
	-1000, -1000, -1000, -1000, -1000, -1000, 67, -1000, -1000, -1000,
	-1000, 990, 91, -54, 1085, 266, -43, 1022, 1085, -1000,
	188, -1000, -1000, 265, 188, 41, -1000, 1022, -1000, 341,
	1085, 215, -1000, 232, -15, 40, 1085, -1000, 66, 1085,
	-1000, 307, 65, 1085, -1000, 249, 1085, 1022, 501, -1000,
	-1000, -49, -1000, 1085, 1022, -1000, -50, 1022, -1000, -1000,
	-1000, 152, 46, 1022, 1085, -1000, 302, 39, -1000, 1022,
	-1000, 307, -1000, -1000, -1000, 1022, -1000, 1022, -1000, -1000,
	1022, 245, 1085, -56, -1000, 1022, -1000, -1000, -1000, 1022,
	1085, 1022,
}
var yyPgo = [...]int{

	0, 32, 338, 11, 9, 337, 332, 2, 329, 20,
	7, 0, 252, 328, 327, 324, 12, 317, 18, 316,
	313, 5, 312, 4, 16, 308, 22, 39, 17, 14,
	305, 303, 19, 302, 301, 299, 298, 290, 289, 3,
	8, 287, 21, 286, 281, 1, 15, 6,
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
	13, 13, 15, 16, 20, 20, 21, 21, 47, 47,
	33, 33, 32, 32, 17, 17, 17, 18, 18, 35,
	35, 34, 34, 19, 19, 29, 36, 14, 14, 37,
	37, 38, 38, 38, 46, 46, 45, 45,
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
	4, 4, 5, 5, 1, 3, 3, 2, 0, 1,
	1, 3, 1, 3, 0, 1, 3, 3, 4, 1,
	3, 1, 3, 3, 5, 1, 3, 0, 2, 0,
	3, 0, 2, 4, 0, 1, 0, 1,
}
var yyChk = [...]int{

	-1000, -44, 34, 35, 36, 37, -36, -14, 38, -1,
	-11, -12, 23, -13, 67, 59, 5, 4, 11, 12,
	19, 15, 25, 45, 44, 46, -15, 26, 6, 9,
	31, 32, 33, 29, 30, 28, 27, -23, 6, 9,
	7, 8, 11, 12, -22, 46, 44, 14, 45, 19,
	4, 68, -37, 5, 68, -9, -7, -8, 17, 18,
	4, 19, 43, 68, 45, 47, 48, 56, 57, 49,
	50, 51, 52, 58, 59, 62, 63, 64, 65, 53,
	54, 55, 71, 46, -11, -12, -12, 45, 45, 45,
	-11, -33, -2, -32, -9, 4, -17, 74, -19, -11,
	45, 45, 45, 45, 45, 45, 45, 45, 45, 45,
	68, 71, -23, -27, -28, -30, 4, 44, -26, -25,
	-24, -23, 45, -1, -38, 39, 78, 42, -6, -39,
	4, 66, 45, 46, 44, 20, 4, 4, -34, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, 4, -11, -16,
	44, -29, -27, -3, -10, -9, 4, 5, 77, 76,
	-46, 77, -9, -11, 4, 19, 78, 74, -46, 77,
	73, -46, 77, 60, 74, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, 4, 73, 74, 75, 77,
	77, -23, -27, 76, 77, -23, -26, 78, -4, 45,
	-31, 4, 45, 79, -23, -42, -39, -43, -42, -41,
	-40, 4, -11, 45, -23, -46, 77, 73, 24, -1,
	76, 77, 76, 76, 77, -35, -11, 75, -32, 4,
	78, -47, 78, -11, 73, -18, 40, -11, 73, -18,
	-11, -20, -21, -39, 23, -11, 76, 76, 76, 77,
	76, 76, 76, 76, 76, 76, -23, -28, 4, 75,
	-24, 76, -5, -23, 79, 77, -3, -11, 79, 76,
	77, 73, 75, 77, 74, -29, 76, -11, -16, -11,
	21, -23, -10, -23, -3, -46, 77, 75, -46, 40,
	77, -11, -46, 74, 73, 77, 22, -11, -11, 73,
	-23, -4, 76, 79, -11, 4, 76, -11, -39, -40,
	-39, 76, -47, -11, 21, 10, 77, -46, 76, -11,
	73, -11, -45, 78, 73, -11, -21, -11, 76, 78,
	-11, -45, 79, -23, 75, -11, 76, -45, -7, -11,
	79, -11,
}
var yyDef = [...]int{

	0, -2, 147, 42, 0, 0, 0, 149, 0, 0,
	0, 68, 0, 90, 0, 0, 93, 94, 95, 96,
	0, 0, 0, 0, 0, 134, 111, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 7, 8,
	9, 10, 11, 12, 13, 0, 0, 0, 0, 0,
	5, 1, -2, 148, 2, 0, 53, 54, 0, 0,
	0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 91, 92, 0, 46, 0,
	0, 154, 0, 130, 0, 132, 154, 0, 154, 135,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	4, 0, 0, 0, 23, 0, 20, 0, 0, 29,
	27, 25, 0, 146, 0, 0, 43, 0, 56, 0,
	30, 31, 0, 0, 0, 0, 0, 0, 154, 141,
	70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
	80, 81, 82, 83, 84, 85, 86, 88, 0, 0,
	42, 0, 145, 0, 47, 49, 50, 0, 0, 110,
	0, 155, 0, 128, 94, 0, 44, 0, 0, 155,
	106, 0, 155, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 6, 14, 0, 16, 0,
	0, 22, 0, 18, 0, 26, 0, 150, 152, 51,
	0, 66, 46, 0, 0, 0, 36, 0, 35, 0,
	38, 40, 57, 0, 60, 0, 155, 89, 0, 0,
	0, 0, 0, 100, 46, 154, 139, 103, 131, 132,
	45, 0, 129, 133, 104, 154, 0, 136, 107, 154,
	0, 0, 124, 0, 0, 143, 112, 113, 114, 0,
	116, 117, 118, 119, 120, 121, 0, 24, 21, 17,
	28, 0, 0, 63, 0, 0, 0, 61, 0, 32,
	0, 33, 34, 0, 0, 0, 69, 142, 87, 128,
	0, 0, 48, 0, 154, 0, 155, 122, 0, 0,
	155, 156, 0, 0, 109, 0, 0, 127, 0, 15,
	19, 0, 153, 0, 64, 67, 156, 62, 37, 39,
	41, 0, 0, 97, 0, 99, 155, 0, 102, 140,
	105, 156, 137, 157, 108, 144, 125, 126, 115, 52,
	65, 0, 0, 0, 123, 98, 101, 138, 55, 58,
	0, 59,
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
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:571
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 123:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:575
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:579
		{
			yyVAL.comprclauses = []*ComprClause{yyDollar[1].comprclause}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:581
		{
			yyVAL.comprclauses = append(yyDollar[1].comprclauses, yyDollar[3].comprclause)
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:585
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprEnum, Pat: yyDollar[1].pat, Expr: yyDollar[3].expr}
		}
	case 127:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:587
		{
			yyVAL.comprclause = &ComprClause{Kind: ComprFilter, Expr: yyDollar[2].expr}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:594
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:596
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:600
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:602
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 134:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:605
		{
			yyVAL.exprlist = nil
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:607
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:609
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:613
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 138:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:615
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:619
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:621
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:625
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:627
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:631
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 144:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:633
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:644
		{
			yyVAL.module = &ModuleImpl{Keyspace: yyDollar[1].expr, ParamDecls: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 147:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:647
		{
			yyVAL.expr = nil
		}
	case 148:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:649
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 149:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:652
		{
			yyVAL.decllist = nil
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:654
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 151:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:657
		{
			yyVAL.decllist = nil
		}
	case 152:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:659
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 153:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:661
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
