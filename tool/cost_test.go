package tool

import "testing"

func TestCost(t *testing.T) {
	var nilCost Cost
	for _, tt := range []struct {
		a, b, want Cost
		m          float64
	}{
		{NewCostExact(1.0), nilCost, NewCostExact(1.0), 1},
		{nilCost, NewCostExact(1.0), NewCostExact(1.0), 1},
		{NewCostExact(1.0), NewCostUB(0), NewCostUB(1.0), 1},
		{NewCostUB(0.0), NewCostExact(1.0), NewCostUB(1.0), 1},
		{NewCostExact(1.0), NewCostExact(2.5), NewCostExact(3.5), 1},
		{NewCostUB(1.0), NewCostExact(2.5), NewCostUB(3.5), 1},
		{NewCostExact(1.0), NewCostUB(2.5), NewCostUB(3.5), 1},
		{NewCostUB(1.0), NewCostUB(2.5), NewCostUB(3.5), 1},
		{NewCostExact(1.0), NewCostExact(2.5), NewCostExact(1.75), 0.5},
		{NewCostUB(1.0), NewCostExact(2.5), NewCostUB(1.75), 0.5},
		{NewCostExact(1.0), NewCostUB(2.5), NewCostUB(1.75), 0.5},
		{NewCostUB(1.0), NewCostUB(2.5), NewCostUB(1.75), 0.5},
	} {
		v := tt.a
		v.Add(tt.b)
		v.Mul(tt.m)
		if got, want := v, tt.want; got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
}
