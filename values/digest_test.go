// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"math/big"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/types"
)

func TestDigest(t *testing.T) {
	for _, c := range []struct {
		typ *types.T
		// val is either a T or a func() T to accommodate values that cannot be
		// constructed in a single expression.
		val          interface{}
		digestString string
	}{
		{
			types.Int,
			big.NewInt(12345),
			"sha256:499fb000867f99f88d5539466aa2489fbc5847f70b6b7171024583a3cfff454b",
		},
		{
			types.Float,
			big.NewFloat(1.2345),
			"sha256:694a41b0b9ad43b3bc07880247f0fb01ff0fc9dadb251a10eacb6950d39c2e4d",
		},
		{
			types.String,
			"hello",
			"sha256:0b4d354d56ea9a985571a56b1829f33d072e7902c1afaf981381089b9eb00ffe",
		},
		{
			types.Bool,
			true,
			"sha256:38b8bc5c86db41a80615b2f4694fc754cccffb95e8933d5b376021feab83cea3",
		},
		{
			types.Unit,
			Unit,
			"sha256:ca358758f6d27e6cf45272937977a748fd88391db679ceda7dc7bf1f005ee879",
		},
		{
			types.List(types.String),
			List{"a", "man", "a", "plan", "a", "canal", "panama"},
			"sha256:bc0a302ebd71d0d1b0b0338a7c5c2ea83964b7364504516dd1f450e4b77430ef",
		},
		{
			types.Tuple(&types.Field{T: types.Int}, &types.Field{T: types.String}),
			Tuple{big.NewInt(1), "hello"},
			"sha256:aa29288ba688ca5e503e47c4e82003e97780ca64606baed753f58ebb74f38e44",
		},
		{
			types.Map(
				types.String,
				types.Struct(
					&types.Field{Name: "field1", T: types.Int},
					&types.Field{Name: "field2", T: types.String})),
			func() T {
				m := new(Map)
				m.Insert(Digest("hello", types.String), "hello", Struct{
					"field1": NewInt(123),
					"field2": T("hello world"),
				})
				m.Insert(Digest("world", types.String), "world", Struct{
					"field1": NewInt(321),
					"field2": T("foo bar"),
				})
				return m
			},
			"sha256:c1c3e68de6ccf619538b5810a4feaeac5049505b7719ad67321f62d0c63f52a9",
		},
		{
			types.Struct(
				&types.Field{Name: "field1", T: types.Int},
				&types.Field{Name: "field2", T: types.String},
				// "field4" is deliberately out of order to test behavior when
				// field order does not match lexicographic name order.
				&types.Field{Name: "field4", T: types.Bool},
				&types.Field{Name: "field3", T: types.Int},
			),
			Struct{
				"field1": big.NewInt(1),
				"field2": "hello",
				"field3": big.NewInt(2),
				"field4": false,
			},
			"sha256:ea6d8ba6d3dbf27f61eb0da431b5247082afc31e189fd57af77f86e5f889b7b0",
		},
		{
			types.Struct(
				&types.Field{Name: "field1", T: types.Int},
				&types.Field{Name: "field2", T: types.String},
				&types.Field{Name: "field3", T: types.Int},
			),
			Struct{
				"field1": big.NewInt(1),
				"field2": "hello",
				"field3": big.NewInt(2),
				"field4": false,
			},
			"sha256:0f600c515b7e3467ad33b74e3a11b199db950c80a45a0272c198ac039b17b3fc",
		},
		{
			types.Module(
				[]*types.Field{
					&types.Field{Name: "field1", T: types.Int},
					&types.Field{Name: "field2", T: types.String},
					// "field4" is deliberately out of order to test behavior
					// when field order does not match lexicographic name order.
					&types.Field{Name: "field4", T: types.Bool},
					&types.Field{Name: "field3", T: types.Int},
				},
				nil,
			),
			Module{
				"field1": big.NewInt(1),
				"field2": "hello",
				"field3": big.NewInt(2),
				"field4": false,
			},
			"sha256:f869ee25f349dea1ad75f9b01000075fdbc10d720e088972f1c523c21d5d490d",
		},
		{
			types.Module(
				[]*types.Field{
					&types.Field{Name: "field1", T: types.Int},
					&types.Field{Name: "field2", T: types.String},
					&types.Field{Name: "field3", T: types.Int},
				},
				nil,
			),
			Module{
				"field1": big.NewInt(1),
				"field2": "hello",
				"field3": big.NewInt(2),
				"field4": false,
			},
			"sha256:c117c8823c75892a495a703ee83b41257f9b5174f4fea59dc3e0c2ee2520af95",
		},
		{
			types.File,
			reflow.File{
				ID:   reflow.Digester.FromString("contents"),
				Size: 54321,
			},
			"sha256:cf2f6c9da27487304271965d77439ea161575f87d0202867f1c099234fbbc219",
		},
		{
			types.File,
			reflow.File{
				ID:         reflow.Digester.FromString("contents"),
				Size:       54321,
				Assertions: reflow.AssertionsFromEntry(reflow.AssertionKey{Subject: "subject", Namespace: "namespace"}, map[string]string{"object": "value"}),
			},
			"sha256:cf2f6c9da27487304271965d77439ea161575f87d0202867f1c099234fbbc219",
		},
		{
			types.Fileset,
			reflow.Fileset{
				List: []reflow.Fileset{
					{
						Map: map[string]reflow.File{
							"a": reflow.File{
								ID:   reflow.Digester.FromString("a contents"),
								Size: 2,
							},
							"b": reflow.File{
								ID:   reflow.Digester.FromString("b contents"),
								Size: 4,
							},
							"d": reflow.File{
								ID:   reflow.Digester.FromString("d contents"),
								Size: 3,
							},
							"c": reflow.File{
								ID:   reflow.Digester.FromString("c contents"),
								Size: 1,
							},
						},
					},
					{
						List: []reflow.Fileset{
							{
								Map: map[string]reflow.File{
									"e": reflow.File{
										ID:   reflow.Digester.FromString("e contents"),
										Size: 5,
									},
								},
							},
						},
					},
				},
			},
			"sha256:35e66174fb0e8ed518dc0d6797566730558b6eb83fcf196b8d8476825035107c",
		},
		{
			types.Dir,
			func() T {
				var md MutableDir
				md.Set("a", reflow.File{
					ID:   reflow.Digester.FromString("a contents"),
					Size: 2,
				})
				md.Set("b", reflow.File{
					ID:   reflow.Digester.FromString("b contents"),
					Size: 4,
				})
				md.Set("d", reflow.File{
					ID:   reflow.Digester.FromString("d contents"),
					Size: 3,
				})
				md.Set("c", reflow.File{
					ID:   reflow.Digester.FromString("c contents"),
					Size: 1,
				})
				return md.Dir()
			},
			"sha256:06132bcc7b9dc5373e7bee046fd23eb1826e599424cff420346111bd5f2835ab",
		},
		{
			&types.T{
				Kind: types.SumKind,
				Variants: []*types.Variant{
					{Tag: "None"},
					{Tag: "Int", Elem: types.Int},
				},
			},
			func() T { return &Variant{Tag: "None"} },
			"sha256:727e511e8fb530780a628ad017419d933b7f7453f87bdef61f3339ec4f86f373",
		},
		{
			&types.T{
				Kind: types.SumKind,
				Variants: []*types.Variant{
					{Tag: "None"},
					{Tag: "Int", Elem: types.Int},
				},
			},
			func() T { return &Variant{Tag: "Int", Elem: big.NewInt(0)} },
			"sha256:cdffa9f65561562220ad62d08cc612245cb2d2486233e8fe547bc8afb80e5c03",
		},
	} {
		var v T
		if f, ok := c.val.(func() T); ok {
			v = f()
		} else {
			v = c.val
		}
		d, _ := Digester.Parse(c.digestString)
		if got, want := Digest(v, c.typ), d; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestNumericDigest(t *testing.T) {
	zeroDigest, _ := Digester.Parse("sha256:dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d986")
	if got, want := Digest(NewInt(0), types.Int), zeroDigest; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if pos, neg := Digest(NewInt(1), types.Int), Digest(NewInt(-1), types.Int); pos == neg {
		t.Error("hashes do not account for sign")
	}
	if pos, neg := Digest(NewFloat(1), types.Float), Digest(NewFloat(-1), types.Float); pos == neg {
		t.Error("hashes do not account for sign")
	}
}
