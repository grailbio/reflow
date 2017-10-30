// Copyright 2017 GRAIL, Inc. All rights reserved.
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

	module *Module

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
const tokMap = 57370
const tokList = 57371
const tokZip = 57372
const tokUnzip = 57373
const tokFlatten = 57374
const tokStartModule = 57375
const tokStartDecls = 57376
const tokStartExpr = 57377
const tokStartType = 57378
const tokKeyspace = 57379
const tokParam = 57380
const tokEllipsis = 57381
const tokReserved = 57382
const tokRequires = 57383
const tokType = 57384
const tokOrOr = 57385
const tokAndAnd = 57386
const tokLE = 57387
const tokGE = 57388
const tokNE = 57389
const tokEqEq = 57390
const tokLSH = 57391
const tokRSH = 57392
const tokSquiggleArrow = 57393
const tokEOF = 57394
const tokError = 57395
const first = 57396
const apply = 57397

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
	-1, 51,
	77, 145,
	-2, 42,
}

const yyPrivate = 57344

const yyLast = 1318

var yyAct = [...]int{

	10, 320, 55, 236, 118, 215, 159, 160, 203, 155,
	36, 117, 157, 82, 166, 240, 91, 111, 210, 161,
	338, 115, 211, 301, 88, 269, 97, 321, 326, 54,
	235, 9, 304, 226, 273, 260, 274, 194, 158, 270,
	271, 228, 229, 314, 92, 227, 226, 287, 49, 109,
	37, 39, 40, 38, 202, 41, 42, 290, 46, 193,
	172, 194, 271, 48, 136, 137, 138, 139, 140, 141,
	142, 143, 144, 145, 146, 147, 148, 149, 150, 151,
	152, 126, 154, 120, 110, 123, 194, 45, 47, 44,
	206, 169, 221, 199, 291, 178, 175, 167, 333, 181,
	182, 183, 184, 185, 186, 187, 188, 189, 316, 174,
	168, 177, 309, 277, 262, 225, 198, 196, 63, 81,
	266, 331, 265, 200, 288, 232, 77, 78, 191, 192,
	132, 209, 173, 217, 275, 73, 74, 75, 76, 219,
	54, 201, 241, 322, 80, 241, 127, 318, 297, 213,
	220, 63, 81, 197, 65, 68, 69, 70, 71, 77,
	78, 300, 66, 67, 72, 231, 272, 176, 73, 74,
	75, 76, 63, 81, 238, 243, 242, 80, 239, 245,
	107, 247, 108, 173, 233, 131, 129, 130, 224, 50,
	218, 207, 119, 244, 106, 133, 105, 257, 80, 104,
	206, 103, 246, 102, 101, 59, 100, 128, 99, 268,
	264, 261, 258, 98, 267, 87, 86, 85, 57, 58,
	60, 156, 278, 114, 162, 280, 124, 122, 223, 8,
	282, 276, 284, 279, 283, 85, 285, 57, 58, 60,
	204, 295, 292, 61, 54, 286, 49, 312, 37, 39,
	40, 38, 296, 41, 42, 289, 46, 132, 313, 293,
	59, 48, 61, 59, 1, 163, 302, 298, 53, 52,
	305, 216, 299, 57, 58, 60, 57, 58, 60, 303,
	307, 113, 311, 259, 310, 45, 47, 44, 317, 234,
	93, 319, 190, 153, 306, 323, 324, 134, 308, 133,
	315, 61, 327, 57, 58, 60, 328, 2, 3, 4,
	5, 11, 212, 332, 330, 214, 121, 51, 6, 230,
	329, 334, 283, 135, 89, 205, 83, 84, 61, 112,
	337, 336, 116, 43, 96, 94, 26, 7, 13, 339,
	63, 81, 64, 65, 68, 69, 70, 71, 77, 78,
	79, 66, 67, 72, 56, 125, 263, 73, 74, 75,
	76, 90, 0, 0, 0, 0, 80, 0, 0, 0,
	0, 0, 0, 321, 63, 81, 64, 65, 68, 69,
	70, 71, 77, 78, 79, 66, 67, 72, 0, 0,
	0, 73, 74, 75, 76, 0, 0, 0, 0, 0,
	80, 0, 0, 0, 0, 0, 0, 237, 63, 81,
	64, 65, 68, 69, 70, 71, 77, 78, 79, 66,
	67, 72, 0, 0, 0, 73, 74, 75, 76, 0,
	0, 0, 0, 0, 80, 0, 0, 0, 0, 165,
	164, 63, 81, 64, 65, 68, 69, 70, 71, 77,
	78, 79, 66, 67, 72, 0, 0, 0, 73, 74,
	75, 76, 0, 17, 16, 28, 0, 80, 29, 0,
	18, 19, 0, 251, 21, 0, 0, 0, 20, 0,
	0, 0, 12, 0, 22, 27, 35, 33, 34, 30,
	31, 32, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 24, 23, 25, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 49, 15, 37, 39,
	40, 38, 0, 41, 42, 14, 46, 0, 0, 0,
	0, 48, 95, 63, 81, 64, 65, 68, 69, 70,
	71, 77, 78, 79, 66, 67, 72, 0, 0, 0,
	73, 74, 75, 76, 0, 45, 47, 44, 0, 80,
	0, 0, 0, 0, 325, 63, 81, 64, 65, 68,
	69, 70, 71, 77, 78, 79, 66, 67, 72, 0,
	0, 0, 73, 74, 75, 76, 0, 0, 0, 0,
	208, 80, 0, 0, 0, 0, 256, 63, 81, 64,
	65, 68, 69, 70, 71, 77, 78, 79, 66, 67,
	72, 0, 0, 0, 73, 74, 75, 76, 0, 0,
	0, 0, 0, 80, 0, 0, 0, 0, 255, 63,
	81, 64, 65, 68, 69, 70, 71, 77, 78, 79,
	66, 67, 72, 0, 0, 0, 73, 74, 75, 76,
	0, 0, 0, 0, 0, 80, 0, 0, 0, 0,
	254, 63, 81, 64, 65, 68, 69, 70, 71, 77,
	78, 79, 66, 67, 72, 0, 0, 0, 73, 74,
	75, 76, 0, 0, 0, 0, 0, 80, 0, 0,
	0, 0, 253, 63, 81, 64, 65, 68, 69, 70,
	71, 77, 78, 79, 66, 67, 72, 0, 0, 0,
	73, 74, 75, 76, 0, 0, 0, 0, 0, 80,
	0, 0, 0, 0, 252, 63, 81, 64, 65, 68,
	69, 70, 71, 77, 78, 79, 66, 67, 72, 0,
	0, 0, 73, 74, 75, 76, 0, 0, 0, 0,
	0, 80, 0, 0, 0, 0, 250, 63, 81, 64,
	65, 68, 69, 70, 71, 77, 78, 79, 66, 67,
	72, 0, 0, 0, 73, 74, 75, 76, 0, 0,
	0, 0, 0, 80, 0, 0, 0, 0, 249, 63,
	81, 64, 65, 68, 69, 70, 71, 77, 78, 79,
	66, 67, 72, 0, 0, 0, 73, 74, 75, 76,
	0, 0, 0, 0, 0, 80, 0, 0, 0, 0,
	248, 63, 81, 64, 65, 68, 69, 70, 71, 77,
	78, 79, 66, 67, 72, 0, 179, 0, 73, 74,
	75, 76, 0, 0, 0, 0, 0, 80, 0, 0,
	180, 63, 81, 64, 65, 68, 69, 70, 71, 77,
	78, 79, 66, 67, 72, 0, 0, 0, 73, 74,
	75, 76, 0, 0, 0, 0, 0, 80, 0, 0,
	294, 63, 81, 64, 65, 68, 69, 70, 71, 77,
	78, 79, 66, 67, 72, 0, 0, 0, 73, 74,
	75, 76, 0, 0, 0, 0, 0, 80, 0, 335,
	63, 81, 64, 65, 68, 69, 70, 71, 77, 78,
	79, 66, 67, 72, 0, 0, 0, 73, 74, 75,
	76, 0, 170, 16, 28, 0, 80, 29, 222, 18,
	19, 0, 0, 21, 0, 57, 58, 171, 0, 0,
	0, 12, 0, 22, 27, 35, 33, 34, 30, 31,
	32, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	61, 24, 23, 25, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 15, 0, 0, 0,
	0, 0, 0, 0, 14, 156, 63, 81, 64, 65,
	68, 69, 70, 71, 77, 78, 79, 66, 67, 72,
	0, 0, 0, 73, 74, 75, 76, 0, 0, 0,
	0, 0, 80, 63, 81, 64, 65, 68, 69, 70,
	71, 77, 78, 79, 66, 67, 72, 0, 0, 0,
	73, 74, 75, 76, 0, 0, 62, 0, 0, 80,
	63, 81, 64, 65, 68, 69, 70, 71, 77, 78,
	79, 66, 67, 72, 0, 0, 0, 73, 74, 75,
	76, 0, 0, 0, 0, 49, 80, 37, 39, 40,
	38, 0, 41, 42, 0, 46, 0, 0, 0, 0,
	48, 0, 0, 0, 0, 0, 0, 0, 17, 16,
	28, 0, 0, 29, 0, 18, 19, 0, 0, 21,
	0, 0, 0, 20, 45, 47, 44, 12, 0, 22,
	27, 35, 33, 34, 30, 31, 32, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 24, 23, 25,
	0, 0, 0, 0, 0, 0, 0, 195, 0, 0,
	0, 0, 15, 0, 0, 0, 0, 0, 0, 0,
	14, 63, 81, 64, 65, 68, 69, 70, 71, 77,
	78, 0, 66, 67, 72, 0, 0, 0, 73, 74,
	75, 76, 0, 17, 16, 28, 0, 80, 29, 0,
	18, 19, 0, 0, 21, 0, 0, 0, 20, 0,
	0, 0, 0, 0, 22, 27, 35, 33, 34, 30,
	31, 32, 63, 81, 0, 0, 0, 0, 0, 0,
	77, 78, 24, 23, 25, 72, 0, 0, 0, 73,
	74, 75, 76, 0, 0, 63, 81, 15, 80, 68,
	69, 70, 71, 77, 78, 14, 66, 67, 72, 0,
	0, 0, 73, 74, 75, 76, 0, 0, 0, 0,
	49, 80, 37, 39, 40, 38, 0, 41, 42, 0,
	46, 0, 0, 0, 0, 48, 49, 281, 37, 39,
	40, 38, 0, 41, 42, 0, 46, 0, 0, 0,
	0, 48, 0, 0, 0, 0, 0, 0, 0, 45,
	47, 44, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 45, 47, 44,
}
var yyPact = [...]int{

	274, -1000, 192, -1000, 1094, 1272, 122, -1000, 264, 201,
	979, -1000, 1094, -1000, 1179, 1179, -1000, -1000, -1000, -1000,
	173, 172, 171, 1094, 286, 459, -1000, 169, 164, 162,
	160, 159, 157, 155, 152, 150, 113, -1000, -1000, -1000,
	-1000, -1000, -1000, 112, 1272, 277, 180, 1272, 148, -1000,
	-1000, 189, -1000, -1000, 8, -1000, -1000, 185, 142, 237,
	295, 293, -1000, 1094, 1094, 1094, 1094, 1094, 1094, 1094,
	1094, 1094, 1094, 1094, 1094, 1094, 1094, 1094, 1094, 1094,
	289, 1094, 952, -1000, -1000, 277, 220, 260, 364, 21,
	928, -1000, -17, 110, 20, 95, 19, 777, 1094, 1094,
	1094, 1094, 1094, 1094, 1094, 1094, 1094, -1000, 288, 56,
	-15, -1000, 1071, -1000, 277, 41, 17, -1000, 1272, 1272,
	259, -23, 196, -1000, 147, -1000, 512, -1000, -1000, 142,
	142, 267, 1094, 146, 1272, 16, 1006, 107, 1191, 1168,
	1168, 1168, 1168, 1168, 1168, 74, 128, 128, 128, 128,
	128, 128, 1117, -1000, 866, 204, -1000, 40, 10, -30,
	-1000, -1000, 237, -34, 1094, -1000, 51, 285, -47, 330,
	237, 191, -1000, 1094, 106, 1094, -1000, 103, 1094, 142,
	1094, 745, 713, 681, 397, 649, 617, 585, 553, 521,
	-1000, -1000, 1272, -1000, 277, 279, -1000, -39, -1000, 1272,
	-1000, 39, -1000, -1000, -1000, 44, -1000, 220, 1094, -53,
	-36, -1000, 94, -14, -40, -1000, 61, 1006, 277, -1000,
	38, 1094, -1000, 178, 928, 1256, 220, 1272, -1000, 220,
	-29, 1006, -1000, -1000, 59, -1000, 50, -1000, 1006, -1000,
	18, 1094, 1006, -1000, 18, 807, 219, 1006, -1000, -1000,
	-1000, 1094, -1000, -1000, -1000, -1000, -1000, 76, -1000, -1000,
	-1000, -1000, 1272, 86, -55, 1094, 275, -43, 1006, 1094,
	-1000, 142, -1000, -1000, 267, 142, 37, -1000, 1006, -1000,
	330, 1094, 226, -1000, 248, -33, 33, 1094, -1000, 75,
	1094, -1000, 296, 71, 1094, 1094, 489, -1000, -1000, -49,
	-1000, 1094, 1006, -1000, -50, 1006, -1000, -1000, -1000, 242,
	47, 1006, 1094, -1000, 220, 23, -1000, 1006, -1000, 296,
	-1000, -1000, -1000, 1006, 837, -1000, -1000, 1006, 256, 1094,
	-58, -1000, 1006, -1000, -1000, -1000, -1000, 1006, 1094, 1006,
}
var yyPgo = [...]int{

	0, 31, 361, 6, 8, 356, 355, 2, 354, 19,
	7, 0, 311, 338, 337, 336, 9, 335, 15, 334,
	333, 4, 11, 332, 21, 38, 17, 12, 329, 325,
	16, 324, 323, 319, 318, 317, 316, 22, 5, 315,
	18, 312, 264, 1, 14, 3,
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
	11, 11, 11, 11, 11, 11, 11, 11, 11, 12,
	12, 12, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	15, 16, 45, 45, 31, 31, 30, 30, 17, 17,
	17, 18, 18, 33, 33, 32, 32, 19, 19, 27,
	34, 14, 14, 35, 35, 36, 36, 36, 44, 44,
	43, 43,
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
	3, 3, 3, 3, 3, 3, 5, 3, 4, 1,
	2, 2, 1, 1, 1, 1, 6, 7, 6, 4,
	7, 6, 4, 4, 6, 3, 4, 6, 7, 3,
	1, 4, 4, 4, 6, 4, 4, 4, 4, 4,
	5, 5, 0, 1, 1, 3, 1, 3, 0, 1,
	3, 3, 4, 1, 3, 1, 3, 3, 5, 1,
	3, 0, 2, 0, 3, 0, 2, 4, 0, 1,
	0, 1,
}
var yyChk = [...]int{

	-1000, -42, 33, 34, 35, 36, -34, -14, 37, -1,
	-11, -12, 23, -13, 66, 58, 5, 4, 11, 12,
	19, 15, 25, 44, 43, 45, -15, 26, 6, 9,
	30, 31, 32, 28, 29, 27, -21, 6, 9, 7,
	8, 11, 12, -20, 45, 43, 14, 44, 19, 4,
	67, -35, 5, 67, -9, -7, -8, 17, 18, 4,
	19, 42, 67, 44, 46, 47, 55, 56, 48, 49,
	50, 51, 57, 61, 62, 63, 64, 52, 53, 54,
	70, 45, -11, -12, -12, 44, 44, 44, -11, -31,
	-2, -30, -9, 4, -17, 73, -19, -11, 44, 44,
	44, 44, 44, 44, 44, 44, 44, 67, 70, -21,
	-25, -26, -28, 4, 43, -24, -23, -22, -21, 44,
	-1, -36, 38, 77, 41, -6, -37, 4, 65, 44,
	45, 43, 20, 4, 4, -32, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, 4, -11, -16, 43, -27, -25, -3,
	-10, -9, 4, 5, 76, 75, -44, 76, -9, -11,
	4, 19, 77, 73, -44, 76, 72, -44, 76, 59,
	73, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	4, 72, 73, 74, 76, 76, -21, -25, 75, 76,
	-21, -24, 77, -4, 44, -29, 4, 44, 78, -21,
	-40, -37, -41, -40, -39, -38, 4, -11, 44, -21,
	-44, 76, 72, 24, -1, 75, 76, 75, 75, 76,
	-33, -11, 74, -30, 4, 77, -45, 77, -11, 72,
	-18, 39, -11, 72, -18, -11, -37, -11, 75, 75,
	75, 76, 75, 75, 75, 75, 75, -21, -26, 4,
	74, -22, 75, -5, -21, 78, 76, -3, -11, 78,
	75, 76, 72, 74, 76, 73, -27, 75, -11, -16,
	-11, 21, -21, -10, -21, -3, -44, 76, 74, -44,
	39, 76, -11, -44, 73, 22, -11, 72, -21, -4,
	75, 78, -11, 4, 75, -11, -37, -38, -37, 75,
	-45, -11, 21, 10, 76, -44, 75, -11, 72, -11,
	-43, 77, 72, -11, -11, 75, 77, -11, -43, 78,
	-21, 74, -11, 75, -43, 72, -7, -11, 78, -11,
}
var yyDef = [...]int{

	0, -2, 141, 42, 0, 0, 0, 143, 0, 0,
	0, 68, 0, 89, 0, 0, 92, 93, 94, 95,
	0, 0, 0, 0, 0, 128, 110, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 7, 8, 9,
	10, 11, 12, 13, 0, 0, 0, 0, 0, 5,
	1, -2, 142, 2, 0, 53, 54, 0, 0, 0,
	0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 90, 91, 0, 46, 0, 0, 148,
	0, 124, 0, 126, 148, 0, 148, 129, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 4, 0, 0,
	0, 23, 0, 20, 0, 0, 29, 27, 25, 0,
	140, 0, 0, 43, 0, 56, 0, 30, 31, 0,
	0, 0, 0, 0, 0, 148, 135, 70, 71, 72,
	73, 74, 75, 76, 77, 78, 79, 80, 81, 82,
	83, 84, 85, 87, 0, 0, 42, 0, 139, 0,
	47, 49, 50, 0, 0, 109, 0, 149, 0, 122,
	93, 0, 44, 0, 0, 149, 105, 0, 149, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	6, 14, 0, 16, 0, 0, 22, 0, 18, 0,
	26, 0, 144, 146, 51, 0, 66, 46, 0, 0,
	0, 36, 0, 35, 0, 38, 40, 57, 0, 60,
	0, 149, 88, 0, 0, 0, 0, 0, 99, 46,
	148, 133, 102, 125, 126, 45, 0, 123, 127, 103,
	148, 0, 130, 106, 148, 0, 0, 137, 111, 112,
	113, 0, 115, 116, 117, 118, 119, 0, 24, 21,
	17, 28, 0, 0, 63, 0, 0, 0, 61, 0,
	32, 0, 33, 34, 0, 0, 0, 69, 136, 86,
	122, 0, 0, 48, 0, 148, 0, 149, 120, 0,
	0, 149, 150, 0, 0, 0, 0, 15, 19, 0,
	147, 0, 64, 67, 150, 62, 37, 39, 41, 0,
	0, 96, 0, 98, 149, 0, 101, 134, 104, 150,
	131, 151, 107, 138, 0, 114, 52, 65, 0, 0,
	0, 121, 97, 100, 132, 108, 55, 58, 0, 59,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 66, 3, 3, 3, 63, 64, 3,
	44, 75, 61, 57, 76, 58, 70, 62, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 73, 77,
	55, 78, 56, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 45, 3, 72, 60, 65, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 43, 59, 74,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 46, 47, 48, 49, 50, 51, 52, 53, 54,
	67, 68, 69, 71,
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
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "*", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:461
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "/", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:463
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "%", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:465
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:467
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:469
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:471
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "~>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:473
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:475
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprDeref, Left: yyDollar[1].expr, Ident: yyDollar[3].expr.Ident}
		}
	case 88:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:477
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIndex, Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:481
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "!", Left: yyDollar[2].expr}
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:483
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "-", Left: yyDollar[2].expr}
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:490
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:492
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 96:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:494
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 97:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:496
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 98:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:499
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 99:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:501
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 100:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:503
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:505
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:507
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 103:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:509
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 104:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:511
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:518
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:520
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 107:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:522
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 108:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:529
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCompr, Left: yyDollar[6].expr, Pat: yyDollar[4].pat, ComprExpr: yyDollar[2].expr}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:531
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:534
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "len", Left: yyDollar[3].expr}
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:536
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "int", Left: yyDollar[3].expr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:538
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Kind: ExprBuiltin, Op: "float", Left: yyDollar[3].expr}
		}
	case 114:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:540
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "zip", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:542
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "unzip", Left: yyDollar[3].expr}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:544
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "flatten", Left: yyDollar[3].expr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:546
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "map", Left: yyDollar[3].expr}
		}
	case 118:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:548
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "list", Left: yyDollar[3].expr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:550
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "panic", Left: yyDollar[3].expr}
		}
	case 120:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:554
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 121:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:558
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:565
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:567
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:571
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:573
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 128:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:576
		{
			yyVAL.exprlist = nil
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:578
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:580
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:584
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 132:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:586
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:590
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:592
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:596
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:598
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:602
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 138:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:604
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:615
		{
			yyVAL.module = &Module{Keyspace: yyDollar[1].expr, Params: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 141:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:618
		{
			yyVAL.expr = nil
		}
	case 142:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:620
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 143:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:623
		{
			yyVAL.decllist = nil
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:625
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 145:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:628
		{
			yyVAL.decllist = nil
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:630
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 147:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:632
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
