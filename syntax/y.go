// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

//line reflow.y:2
package syntax

import __yyfmt__ "fmt"

//line reflow.y:2
import (
	"fmt"

	"github.com/grailbio/reflow/types"
	"grail.com/reflow/syntax/scanner"
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
const tokTemplate = 57351
const tokFile = 57352
const tokDir = 57353
const tokStruct = 57354
const tokModule = 57355
const tokExec = 57356
const tokAs = 57357
const tokAt = 57358
const tokVal = 57359
const tokFunc = 57360
const tokAssign = 57361
const tokArrow = 57362
const tokLeftArrow = 57363
const tokIf = 57364
const tokElse = 57365
const tokMake = 57366
const tokLen = 57367
const tokPanic = 57368
const tokMap = 57369
const tokList = 57370
const tokZip = 57371
const tokUnzip = 57372
const tokFlatten = 57373
const tokStartModule = 57374
const tokStartDecls = 57375
const tokStartExpr = 57376
const tokStartType = 57377
const tokKeyspace = 57378
const tokParam = 57379
const tokEllipsis = 57380
const tokReserved = 57381
const tokRequires = 57382
const tokType = 57383
const tokOrOr = 57384
const tokAndAnd = 57385
const tokLE = 57386
const tokGE = 57387
const tokNE = 57388
const tokEqEq = 57389
const tokLSH = 57390
const tokRSH = 57391
const tokSquiggleArrow = 57392
const tokEOF = 57393
const tokError = 57394
const first = 57395
const apply = 57396

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"tokIdent",
	"tokExpr",
	"tokInt",
	"tokString",
	"tokBool",
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
	-1, 48,
	76, 142,
	-2, 41,
}

const yyNprod = 149
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1192

var yyAct = [...]int{

	10, 113, 52, 308, 229, 208, 154, 34, 196, 150,
	155, 161, 152, 79, 88, 112, 106, 233, 203, 156,
	325, 110, 291, 260, 85, 309, 94, 314, 204, 51,
	228, 9, 60, 78, 195, 153, 304, 219, 294, 219,
	74, 75, 167, 104, 89, 69, 280, 276, 277, 70,
	71, 72, 73, 264, 118, 265, 261, 262, 77, 221,
	222, 131, 132, 133, 134, 135, 136, 137, 138, 139,
	140, 141, 142, 143, 144, 145, 146, 147, 105, 149,
	115, 220, 219, 281, 121, 251, 262, 187, 164, 213,
	214, 186, 199, 187, 187, 192, 176, 177, 178, 179,
	180, 181, 182, 169, 173, 172, 170, 163, 162, 189,
	299, 253, 218, 191, 319, 193, 278, 225, 184, 185,
	127, 168, 266, 202, 310, 306, 287, 263, 210, 171,
	103, 212, 102, 47, 234, 51, 194, 234, 211, 128,
	200, 114, 119, 101, 206, 190, 60, 78, 61, 62,
	65, 66, 67, 68, 74, 75, 76, 63, 64, 69,
	224, 100, 290, 70, 71, 72, 73, 236, 99, 231,
	232, 235, 77, 168, 238, 199, 240, 226, 82, 309,
	60, 78, 157, 217, 98, 97, 96, 248, 95, 84,
	237, 83, 82, 151, 54, 55, 57, 56, 109, 117,
	255, 8, 259, 239, 249, 216, 77, 258, 252, 54,
	55, 57, 285, 127, 197, 268, 302, 11, 270, 58,
	272, 303, 274, 158, 267, 49, 269, 122, 209, 275,
	273, 293, 80, 81, 58, 282, 108, 51, 2, 3,
	4, 5, 250, 286, 46, 279, 35, 36, 37, 283,
	38, 39, 227, 43, 183, 288, 56, 292, 45, 50,
	148, 295, 129, 289, 128, 126, 124, 125, 54, 55,
	57, 297, 301, 1, 205, 300, 207, 116, 305, 48,
	6, 307, 42, 44, 41, 311, 312, 123, 60, 78,
	223, 296, 315, 58, 130, 298, 74, 75, 316, 86,
	198, 318, 107, 320, 111, 70, 71, 72, 73, 56,
	40, 321, 93, 91, 77, 257, 26, 256, 324, 323,
	7, 54, 55, 57, 13, 53, 326, 60, 78, 61,
	62, 65, 66, 67, 68, 74, 75, 76, 63, 64,
	69, 120, 254, 87, 70, 71, 72, 73, 0, 0,
	0, 0, 0, 77, 0, 0, 0, 0, 0, 0,
	230, 60, 78, 61, 62, 65, 66, 67, 68, 74,
	75, 76, 63, 64, 69, 0, 0, 0, 70, 71,
	72, 73, 0, 0, 0, 0, 0, 77, 0, 0,
	0, 0, 160, 159, 60, 78, 61, 62, 65, 66,
	67, 68, 74, 75, 76, 63, 64, 69, 0, 0,
	0, 70, 71, 72, 73, 0, 0, 0, 0, 0,
	77, 0, 0, 0, 0, 0, 242, 60, 78, 61,
	62, 65, 66, 67, 68, 74, 75, 76, 63, 64,
	69, 0, 0, 90, 70, 71, 72, 73, 0, 0,
	0, 0, 0, 77, 0, 54, 55, 57, 313, 60,
	78, 61, 62, 65, 66, 67, 68, 74, 75, 76,
	63, 64, 69, 0, 0, 0, 70, 71, 72, 73,
	58, 0, 0, 0, 0, 77, 0, 0, 0, 0,
	247, 60, 78, 61, 62, 65, 66, 67, 68, 74,
	75, 76, 63, 64, 69, 0, 0, 0, 70, 71,
	72, 73, 0, 0, 0, 0, 0, 77, 0, 0,
	0, 0, 246, 60, 78, 61, 62, 65, 66, 67,
	68, 74, 75, 76, 63, 64, 69, 0, 0, 0,
	70, 71, 72, 73, 0, 0, 0, 0, 0, 77,
	0, 0, 0, 0, 245, 60, 78, 61, 62, 65,
	66, 67, 68, 74, 75, 76, 63, 64, 69, 0,
	0, 0, 70, 71, 72, 73, 0, 0, 0, 0,
	0, 77, 0, 0, 0, 0, 244, 60, 78, 61,
	62, 65, 66, 67, 68, 74, 75, 76, 63, 64,
	69, 0, 0, 0, 70, 71, 72, 73, 0, 0,
	0, 0, 0, 77, 0, 0, 0, 0, 243, 60,
	78, 61, 62, 65, 66, 67, 68, 74, 75, 76,
	63, 64, 69, 0, 0, 0, 70, 71, 72, 73,
	0, 0, 0, 0, 0, 77, 0, 0, 0, 0,
	241, 60, 78, 61, 62, 65, 66, 67, 68, 74,
	75, 76, 63, 64, 69, 0, 174, 0, 70, 71,
	72, 73, 17, 16, 0, 0, 0, 77, 18, 19,
	175, 0, 21, 0, 0, 0, 20, 0, 0, 0,
	12, 0, 22, 27, 33, 31, 32, 28, 29, 30,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	24, 23, 25, 0, 0, 0, 0, 46, 0, 35,
	36, 37, 0, 38, 39, 15, 43, 0, 0, 0,
	0, 45, 0, 14, 0, 0, 0, 0, 0, 0,
	92, 60, 78, 61, 62, 65, 66, 67, 68, 74,
	75, 76, 63, 64, 69, 42, 44, 41, 70, 71,
	72, 73, 0, 0, 0, 0, 0, 77, 0, 0,
	284, 60, 78, 61, 62, 65, 66, 67, 68, 74,
	75, 76, 63, 64, 69, 0, 0, 0, 70, 71,
	72, 73, 0, 0, 0, 0, 0, 77, 0, 322,
	60, 78, 61, 62, 65, 66, 67, 68, 74, 75,
	76, 63, 64, 69, 0, 0, 0, 70, 71, 72,
	73, 0, 0, 0, 0, 0, 77, 46, 215, 35,
	36, 37, 0, 38, 39, 0, 43, 0, 0, 0,
	46, 45, 35, 36, 37, 0, 38, 39, 0, 43,
	0, 0, 0, 0, 45, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 42, 44, 41, 46, 0,
	35, 36, 37, 0, 38, 39, 0, 43, 42, 44,
	41, 0, 45, 151, 60, 78, 61, 62, 65, 66,
	67, 68, 74, 75, 76, 63, 64, 69, 0, 0,
	317, 70, 71, 72, 73, 0, 42, 44, 41, 0,
	77, 0, 0, 201, 60, 78, 61, 62, 65, 66,
	67, 68, 74, 75, 76, 63, 64, 69, 0, 0,
	0, 70, 71, 72, 73, 0, 0, 59, 0, 188,
	77, 60, 78, 61, 62, 65, 66, 67, 68, 74,
	75, 76, 63, 64, 69, 0, 165, 16, 70, 71,
	72, 73, 18, 19, 0, 0, 21, 77, 54, 55,
	166, 0, 0, 0, 12, 0, 22, 27, 33, 31,
	32, 28, 29, 30, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 58, 24, 23, 25, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 15,
	0, 0, 0, 0, 0, 0, 0, 14, 60, 78,
	61, 62, 65, 66, 67, 68, 74, 75, 0, 63,
	64, 69, 0, 0, 0, 70, 71, 72, 73, 0,
	0, 0, 60, 78, 77, 62, 65, 66, 67, 68,
	74, 75, 0, 63, 64, 69, 0, 17, 16, 70,
	71, 72, 73, 18, 19, 0, 0, 21, 77, 0,
	0, 20, 0, 0, 0, 12, 0, 22, 27, 33,
	31, 32, 28, 29, 30, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 24, 23, 25, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 60, 78,
	15, 0, 65, 66, 67, 68, 74, 75, 14, 63,
	64, 69, 0, 17, 16, 70, 71, 72, 73, 18,
	19, 0, 0, 21, 77, 0, 0, 20, 0, 0,
	0, 0, 0, 22, 27, 33, 31, 32, 28, 29,
	30, 46, 0, 35, 36, 37, 0, 38, 39, 0,
	43, 24, 23, 25, 0, 45, 0, 271, 0, 0,
	0, 0, 0, 0, 0, 0, 15, 0, 0, 0,
	0, 0, 0, 0, 14, 0, 0, 0, 0, 42,
	44, 41,
}
var yyPact = [...]int{

	206, -1000, 165, -1000, 1053, 713, 67, -1000, 220, 193,
	871, -1000, 1053, -1000, 1119, 1119, -1000, -1000, -1000, -1000,
	149, 148, 146, 1053, 439, 668, -1000, 145, 143, 142,
	141, 125, 118, 100, 66, -1000, -1000, -1000, -1000, -1000,
	61, 713, 232, 156, 713, 98, -1000, -1000, 162, -1000,
	-1000, -22, -1000, -1000, 102, 223, 194, 260, 258, -1000,
	1053, 1053, 1053, 1053, 1053, 1053, 1053, 1053, 1053, 1053,
	1053, 1053, 1053, 1053, 1053, 1053, 1053, 256, 1053, 841,
	-1000, -1000, 232, 178, 218, 318, 33, 952, -1000, -34,
	101, 31, 58, 29, 608, 1053, 1053, 1053, 1053, 1053,
	1053, 1053, -1000, 250, 47, 18, -1000, 864, -1000, 232,
	39, 20, -1000, 713, 713, 252, -42, 171, -1000, 97,
	-1000, 836, -1000, -1000, 223, 223, 224, 1053, 95, 713,
	15, 898, 999, 1065, -11, -11, -11, -11, -11, -11,
	245, 137, 137, 137, 137, 137, 137, 975, -1000, 757,
	182, -1000, 38, 19, 7, -1000, -1000, 194, -15, 1053,
	-1000, 44, 248, -46, 284, 194, 135, -1000, 1053, 99,
	1053, -1000, 96, 1053, 223, 1053, 576, 351, 544, 512,
	480, 448, 416, -1000, -1000, 713, -1000, 232, 238, -1000,
	12, -1000, 713, -1000, 37, -1000, -1000, -1000, 240, -1000,
	178, 1053, -54, -18, -1000, 56, 11, -20, -1000, 50,
	898, 232, -1000, -1000, 1053, -1000, 151, 952, 1147, 178,
	713, -1000, 178, -27, 898, -1000, -1000, 49, -1000, 43,
	-1000, 898, -1000, 8, 1053, 898, -1000, 8, 698, 191,
	898, -1000, 1053, -1000, -1000, -1000, -1000, -1000, 55, -1000,
	-1000, -1000, -1000, 713, 88, -55, 1053, 227, -36, 898,
	1053, -1000, 223, -1000, -1000, 224, 223, 36, 898, -1000,
	284, 1053, 196, -1000, 212, -38, -1000, 1053, -1000, 54,
	1053, -1000, 103, 53, 1053, 1053, 384, -1000, -1000, -49,
	-1000, 1053, 898, -1000, -51, 898, -1000, -1000, -1000, 823,
	41, 898, 1053, -1000, -1000, 898, -1000, 103, -1000, -1000,
	-1000, 898, 728, -1000, -1000, 898, 305, 1053, -57, -1000,
	898, -1000, -1000, -1000, 898, 1053, 898,
}
var yyPgo = [...]int{

	0, 31, 343, 6, 8, 342, 341, 2, 325, 19,
	10, 0, 217, 324, 320, 316, 9, 313, 17, 312,
	310, 1, 15, 304, 21, 35, 16, 12, 302, 300,
	14, 299, 294, 290, 280, 279, 277, 28, 5, 276,
	18, 274, 273, 3, 11, 4,
}
var yyR1 = [...]int{

	0, 42, 42, 42, 42, 20, 20, 21, 21, 21,
	21, 21, 21, 21, 21, 21, 21, 21, 21, 28,
	28, 26, 25, 25, 22, 22, 23, 23, 24, 37,
	37, 37, 37, 37, 41, 40, 40, 39, 39, 38,
	38, 1, 1, 2, 2, 3, 3, 3, 10, 10,
	5, 5, 9, 9, 7, 7, 7, 7, 7, 8,
	6, 6, 4, 4, 4, 29, 29, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 12, 12,
	12, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
	13, 13, 13, 13, 13, 13, 13, 15, 16, 45,
	45, 31, 31, 30, 30, 17, 17, 17, 18, 18,
	33, 33, 32, 32, 19, 19, 27, 34, 14, 14,
	35, 35, 36, 36, 36, 44, 44, 43, 43,
}
var yyR2 = [...]int{

	0, 3, 3, 3, 3, 1, 3, 1, 1, 1,
	1, 1, 1, 3, 5, 3, 4, 3, 5, 1,
	3, 2, 1, 3, 1, 2, 1, 3, 1, 1,
	1, 3, 3, 3, 1, 1, 3, 1, 3, 1,
	3, 0, 3, 2, 3, 0, 1, 3, 1, 1,
	0, 3, 1, 1, 7, 2, 3, 7, 8, 3,
	3, 4, 2, 3, 4, 1, 3, 1, 4, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 5, 3, 4, 1, 2,
	2, 1, 1, 1, 1, 6, 7, 6, 4, 6,
	5, 4, 4, 6, 3, 4, 6, 7, 3, 1,
	4, 6, 4, 4, 4, 4, 4, 5, 5, 0,
	1, 1, 3, 1, 3, 0, 1, 3, 3, 4,
	1, 3, 1, 3, 3, 5, 1, 3, 0, 2,
	0, 3, 0, 2, 4, 0, 1, 0, 1,
}
var yyChk = [...]int{

	-1000, -42, 32, 33, 34, 35, -34, -14, 36, -1,
	-11, -12, 22, -13, 65, 57, 5, 4, 10, 11,
	18, 14, 24, 43, 42, 44, -15, 25, 29, 30,
	31, 27, 28, 26, -21, 6, 7, 8, 10, 11,
	-20, 44, 42, 13, 43, 18, 4, 66, -35, 5,
	66, -9, -7, -8, 16, 17, 4, 18, 41, 66,
	43, 45, 46, 54, 55, 47, 48, 49, 50, 56,
	60, 61, 62, 63, 51, 52, 53, 69, 44, -11,
	-12, -12, 43, 43, 43, -11, -31, -2, -30, -9,
	4, -17, 72, -19, -11, 43, 43, 43, 43, 43,
	43, 43, 66, 69, -21, -25, -26, -28, 4, 42,
	-24, -23, -22, -21, 43, -1, -36, 37, 76, 40,
	-6, -37, 4, 64, 43, 44, 42, 19, 4, 4,
	-32, -11, -11, -11, -11, -11, -11, -11, -11, -11,
	-11, -11, -11, -11, -11, -11, -11, -11, 4, -11,
	-16, 42, -27, -25, -3, -10, -9, 4, 5, 75,
	74, -44, 75, -9, -11, 4, 18, 76, 72, -44,
	75, 71, -44, 75, 58, 72, -11, -11, -11, -11,
	-11, -11, -11, 4, 71, 72, 73, 75, 75, -21,
	-25, 74, 75, -21, -24, 76, -4, 43, -29, 4,
	43, 77, -21, -40, -37, -41, -40, -39, -38, 4,
	-11, 43, -21, 74, 75, 71, 23, -1, 74, 75,
	74, 74, 75, -33, -11, 73, -30, 4, 76, -45,
	76, -11, 71, -18, 38, -11, 71, -18, -11, -37,
	-11, 74, 75, 74, 74, 74, 74, 74, -21, -26,
	4, 73, -22, 74, -5, -21, 77, 75, -3, -11,
	77, 74, 75, 71, 73, 75, 72, -27, -11, -16,
	-11, 20, -21, -10, -21, -3, 74, 75, 73, -44,
	38, 75, -11, -44, 72, 21, -11, 71, -21, -4,
	74, 77, -11, 4, 74, -11, -37, -38, -37, 74,
	-45, -11, 20, 9, 74, -11, 71, -11, -43, 76,
	71, -11, -11, 74, 76, -11, -43, 77, -21, 73,
	-11, -43, 71, -7, -11, 77, -11,
}
var yyDef = [...]int{

	0, -2, 138, 41, 0, 0, 0, 140, 0, 0,
	0, 67, 0, 88, 0, 0, 91, 92, 93, 94,
	0, 0, 0, 0, 0, 125, 109, 0, 0, 0,
	0, 0, 0, 0, 0, 7, 8, 9, 10, 11,
	12, 0, 0, 0, 0, 0, 5, 1, -2, 139,
	2, 0, 52, 53, 0, 0, 0, 0, 0, 3,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	89, 90, 0, 45, 0, 0, 145, 0, 121, 0,
	123, 145, 0, 145, 126, 0, 0, 0, 0, 0,
	0, 0, 4, 0, 0, 0, 22, 0, 19, 0,
	0, 28, 26, 24, 0, 137, 0, 0, 42, 0,
	55, 0, 29, 30, 0, 0, 0, 0, 0, 0,
	0, 132, 69, 70, 71, 72, 73, 74, 75, 76,
	77, 78, 79, 80, 81, 82, 83, 84, 86, 0,
	0, 41, 0, 136, 0, 46, 48, 49, 0, 0,
	108, 0, 146, 0, 119, 92, 0, 43, 0, 0,
	146, 104, 0, 146, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 6, 13, 0, 15, 0, 0, 21,
	0, 17, 0, 25, 0, 141, 143, 50, 0, 65,
	45, 0, 0, 0, 35, 0, 34, 0, 37, 39,
	56, 0, 59, 68, 0, 87, 0, 0, 0, 0,
	0, 98, 45, 0, 130, 101, 122, 123, 44, 0,
	120, 124, 102, 145, 0, 127, 105, 145, 0, 0,
	134, 110, 0, 112, 113, 114, 115, 116, 0, 23,
	20, 16, 27, 0, 0, 62, 0, 0, 0, 60,
	0, 31, 0, 32, 33, 0, 0, 0, 133, 85,
	119, 0, 0, 47, 0, 0, 100, 0, 117, 0,
	0, 146, 147, 0, 0, 0, 0, 14, 18, 0,
	144, 0, 63, 66, 147, 61, 36, 38, 40, 0,
	0, 95, 0, 97, 99, 131, 103, 147, 128, 148,
	106, 135, 0, 111, 51, 64, 0, 0, 0, 118,
	96, 129, 107, 54, 57, 0, 58,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 65, 3, 3, 3, 62, 63, 3,
	43, 74, 60, 56, 75, 57, 69, 61, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 72, 76,
	54, 77, 55, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 44, 3, 71, 59, 64, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 42, 58, 73,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	45, 46, 47, 48, 49, 50, 51, 52, 53, 66,
	67, 68, 70,
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
			yyVAL.typ = types.String
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:153
		{
			yyVAL.typ = types.Bool
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:154
		{
			yyVAL.typ = types.File
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:155
		{
			yyVAL.typ = types.Dir
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:156
		{
			yyVAL.typ = types.Ref(yyDollar[1].idents...)
		}
	case 13:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:157
		{
			yyVAL.typ = types.List(yyDollar[2].typ)
		}
	case 14:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:159
		{
			yyVAL.typ = types.Map(yyDollar[2].typ, yyDollar[4].typ)
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:161
		{
			yyVAL.typ = types.Struct(yyDollar[2].typfields...)
		}
	case 16:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:163
		{
			yyVAL.typ = types.Module(yyDollar[3].typfields, nil)
		}
	case 17:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:165
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
	case 18:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:177
		{
			yyVAL.typ = types.Func(yyDollar[5].typ, yyDollar[3].typfields...)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:181
		{
			yyVAL.idents = []string{yyDollar[1].expr.Ident}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:183
		{
			yyVAL.idents = append(yyDollar[1].idents, yyDollar[3].expr.Ident)
		}
	case 21:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:187
		{
			for _, name := range yyDollar[1].idents {
				yyVAL.typfields = append(yyVAL.typfields, &types.Field{Name: name, T: yyDollar[2].typ})
			}
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:195
		{
			yyVAL.typfields = yyDollar[1].typfields
		}
	case 23:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:197
		{
			yyVAL.typfields = append(yyDollar[1].typfields, yyDollar[3].typfields...)
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:201
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, nil}
		}
	case 25:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:203
		{
			yyVAL.typearg = typearg{yyDollar[1].typ, yyDollar[2].typ}
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:207
		{
			yyVAL.typeargs = []typearg{yyDollar[1].typearg}
		}
	case 27:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:209
		{
			yyVAL.typeargs = append(yyDollar[1].typeargs, yyDollar[3].typearg)
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:218
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
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:260
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].expr.Position, Kind: PatIdent, Ident: yyDollar[1].expr.Ident}
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:262
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatIgnore}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:264
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatTuple, List: yyDollar[2].patlist}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:266
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatList, List: yyDollar[2].patlist}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:268
		{
			yyVAL.pat = &Pat{Position: yyDollar[1].pos.Position, Kind: PatStruct, Map: make(map[string]*Pat)}
			for _, p := range yyDollar[2].structpats {
				yyVAL.pat.Map[p.field] = p.pat
			}
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:280
		{
			yyVAL.patlist = []*Pat{yyDollar[1].pat}
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:282
		{
			yyVAL.patlist = append(yyDollar[1].patlist, yyDollar[3].pat)
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:286
		{
			yyVAL.structpats = []struct {
				field string
				pat   *Pat
			}{yyDollar[1].structpat}
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:291
		{
			yyVAL.structpats = append(yyDollar[1].structpats, yyDollar[3].structpat)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:295
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 40:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:300
		{
			yyVAL.structpat = struct {
				field string
				pat   *Pat
			}{yyDollar[1].expr.Ident, yyDollar[3].pat}
		}
	case 41:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:308
		{
			yyVAL.decllist = nil
		}
	case 42:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:310
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:314
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 44:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:316
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decl)
		}
	case 45:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:319
		{
			yyVAL.decllist = nil
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:321
		{
			yyVAL.decllist = []*Decl{yyDollar[1].decl}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:323
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[3].decl)
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:327
		{
			yyVAL.decl = &Decl{
				Position: yyDollar[1].expr.Position,
				Comment:  yyDollar[1].expr.Comment,
				Pat:      &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident},
				Kind:     DeclAssign,
				Expr:     &Expr{Kind: ExprIdent, Ident: yyDollar[1].expr.Ident},
			}
		}
	case 50:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:338
		{
			yyVAL.decllist = nil
		}
	case 51:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:340
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 54:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:345
		{
			yyDollar[7].decl.Expr = &Expr{Position: yyDollar[7].decl.Expr.Position, Kind: ExprRequires, Left: yyDollar[7].decl.Expr, Decls: yyDollar[4].decllist}
			yyDollar[7].decl.Comment = yyDollar[1].pos.comment
			yyVAL.decl = yyDollar[7].decl
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:351
		{
			yyVAL.decl = yyDollar[2].decl
			yyVAL.decl.Comment = yyDollar[1].pos.comment
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:356
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].expr.Position, Comment: yyDollar[1].expr.Comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].expr.Ident}, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 57:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:358
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Kind: ExprFunc,
				Args: yyDollar[4].typfields,
				Left: yyDollar[7].expr}}
		}
	case 58:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line reflow.y:363
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Pat: &Pat{Kind: PatIdent, Ident: yyDollar[2].expr.Ident}, Kind: DeclAssign, Expr: &Expr{
				Position: yyDollar[1].pos.Position,
				Kind:     ExprAscribe,
				Type:     types.Func(yyDollar[6].typ, yyDollar[4].typfields...),
				Left:     &Expr{Kind: ExprFunc, Args: yyDollar[4].typfields, Left: yyDollar[8].expr}}}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:371
		{
			yyVAL.decl = &Decl{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: DeclType, Ident: yyDollar[2].expr.Ident, Type: yyDollar[3].typ}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:375
		{
			yyVAL.decl = &Decl{Position: yyDollar[3].expr.Position, Pat: yyDollar[1].pat, Kind: DeclAssign, Expr: yyDollar[3].expr}
		}
	case 61:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:377
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
	case 62:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:393
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
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:406
		{
			if len(yyDollar[1].posidents.idents) != 1 {
				yyVAL.decllist = []*Decl{{Kind: DeclError}}
			} else {
				yyVAL.decllist = []*Decl{{Position: yyDollar[1].posidents.pos, Comment: yyDollar[1].posidents.comments[0], Pat: &Pat{Kind: PatIdent, Ident: yyDollar[1].posidents.idents[0]}, Kind: DeclAssign, Expr: yyDollar[3].expr}}
			}
		}
	case 64:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:414
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
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:430
		{
			yyVAL.posidents = posIdents{yyDollar[1].expr.Position, []string{yyDollar[1].expr.Ident}, []string{yyDollar[1].expr.Comment}}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:432
		{
			yyVAL.posidents = posIdents{yyDollar[1].posidents.pos, append(yyDollar[1].posidents.idents, yyDollar[3].expr.Ident), append(yyDollar[1].posidents.comments, yyDollar[3].expr.Comment)}
		}
	case 68:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:438
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprApply, Left: yyDollar[1].expr, Fields: yyDollar[3].exprfields}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:440
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "||", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:442
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:444
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:446
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:448
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:450
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:452
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "!=", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:454
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "==", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:456
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "+", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:458
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "*", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:460
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "/", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:462
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "%", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:464
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "&", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:466
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "<<", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:468
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: ">>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:470
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprBinop, Op: "~>", Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:472
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCond, Cond: yyDollar[2].expr, Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:474
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprDeref, Left: yyDollar[1].expr, Ident: yyDollar[3].expr.Ident}
		}
	case 87:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:476
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIndex, Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:480
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "!", Left: yyDollar[2].expr}
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:482
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprUnop, Op: "-", Left: yyDollar[2].expr}
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:489
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprIdent, Ident: "file"}
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:491
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprIdent, Ident: "dir"}
		}
	case 95:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:493
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[6].expr}
		}
	case 96:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:495
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprAscribe, Type: yyDollar[5].typ, Left: &Expr{
				Position: yyDollar[7].expr.Position, Kind: ExprFunc, Args: yyDollar[3].typfields, Left: yyDollar[7].expr}}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:498
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprExec, Decls: yyDollar[3].decllist, Type: yyDollar[5].typ, Template: yyDollar[6].template}
		}
	case 98:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:500
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr}
		}
	case 99:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:502
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMake, Left: yyDollar[3].expr, Decls: yyDollar[5].decllist}
		}
	case 100:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:504
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprTuple, Fields: append([]*FieldExpr{{Expr: yyDollar[2].expr}}, yyDollar[4].exprfields...)}
		}
	case 101:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:506
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprStruct, Fields: yyDollar[2].exprfields}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:508
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
		}
	case 103:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:510
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprList, List: yyDollar[2].exprlist}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: yyVAL.expr, Right: list}
			}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:517
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap}
		}
	case 105:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:519
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
		}
	case 106:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:521
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprMap, Map: yyDollar[2].exprmap}
			for _, list := range yyDollar[4].exprlist {
				yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Kind: ExprBinop, Op: "+", Left: list, Right: yyVAL.expr}
			}
		}
	case 107:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line reflow.y:528
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprCompr, Left: yyDollar[6].expr, Pat: yyDollar[4].pat, ComprExpr: yyDollar[2].expr}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:530
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 110:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:533
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "len", Left: yyDollar[3].expr}
		}
	case 111:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line reflow.y:535
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "zip", Left: yyDollar[3].expr, Right: yyDollar[5].expr}
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:537
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "unzip", Left: yyDollar[3].expr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:539
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "flatten", Left: yyDollar[3].expr}
		}
	case 114:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:541
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "map", Left: yyDollar[3].expr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:543
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "list", Left: yyDollar[3].expr}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:545
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBuiltin, Op: "panic", Left: yyDollar[3].expr}
		}
	case 117:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:549
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 118:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:553
		{
			yyVAL.expr = &Expr{Position: yyDollar[1].pos.Position, Comment: yyDollar[1].pos.comment, Kind: ExprBlock, Decls: yyDollar[2].decllist, Left: yyDollar[3].expr}
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:560
		{
			yyVAL.exprfields = []*FieldExpr{yyDollar[1].exprfield}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:562
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, yyDollar[3].exprfield)
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:566
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: &Expr{Position: yyDollar[1].expr.Position, Kind: ExprIdent, Ident: yyDollar[1].expr.Ident}}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:568
		{
			yyVAL.exprfield = &FieldExpr{Name: yyDollar[1].expr.Ident, Expr: yyDollar[3].expr}
		}
	case 125:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:571
		{
			yyVAL.exprlist = nil
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:573
		{
			yyVAL.exprlist = []*Expr{yyDollar[1].expr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:575
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:579
		{
			yyVAL.exprlist = []*Expr{yyDollar[2].expr}
		}
	case 129:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:581
		{
			yyVAL.exprlist = append(yyDollar[1].exprlist, yyDollar[3].expr)
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:585
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:587
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line reflow.y:591
		{
			yyVAL.exprfields = []*FieldExpr{{Expr: yyDollar[1].expr}}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:593
		{
			yyVAL.exprfields = append(yyDollar[1].exprfields, &FieldExpr{Expr: yyDollar[3].expr})
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:597
		{
			yyVAL.exprmap = map[*Expr]*Expr{yyDollar[1].expr: yyDollar[3].expr}
		}
	case 135:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line reflow.y:599
		{
			yyVAL.exprmap = yyDollar[1].exprmap
			yyVAL.exprmap[yyDollar[3].expr] = yyDollar[5].expr
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:610
		{
			yyVAL.module = &Module{Keyspace: yyDollar[1].expr, Params: yyDollar[2].decllist, Decls: yyDollar[3].decllist}
		}
	case 138:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:613
		{
			yyVAL.expr = nil
		}
	case 139:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:615
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 140:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:618
		{
			yyVAL.decllist = nil
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line reflow.y:620
		{
			yyVAL.decllist = append(yyDollar[1].decllist, yyDollar[2].decllist...)
		}
	case 142:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line reflow.y:623
		{
			yyVAL.decllist = nil
		}
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line reflow.y:625
		{
			yyVAL.decllist = yyDollar[2].decllist
		}
	case 144:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line reflow.y:627
		{
			yyVAL.decllist = yyDollar[3].decllist
		}
	}
	goto yystack /* stack new state and value */
}
