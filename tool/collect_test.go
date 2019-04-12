package tool

import "testing"

func TestClauses(t *testing.T) {

	for _, test := range []struct {
		re     string
		labels []string
		match  bool
	}{
		{`label=alpha`, []string{`label=alpha`}, true},
		{`!label=alpha`, []string{`label=alpha`}, false},
		{`label=beta`, []string{`label=alpha`}, false},

		{`label=alpha label=beta`, []string{`label=alpha`}, true},
		{`label=alpha label=beta`, []string{`label=beta`}, true},
		{`label=alpha label=beta`, []string{`label=gamma`}, false},

		{`label=alpha,label=beta`, []string{`label=alpha`}, false},
		{`label=alpha,label=beta`, []string{`label=alpha`, `label=beta`}, true},
		{`label=alpha,label=beta`, []string{`label=alpha`, `label=beta`, `label=gamma`}, true},
		{`label=alpha,label=beta label=alpha`, []string{`label=alpha`}, true},
		{`label=alpha label=alpha,label=beta`, []string{`label=alpha`}, true},

		{`label=alpha !label=beta`, []string{`label=alpha`, `label=beta`}, true},
		{`label=alpha !label=beta`, []string{`label=alpha`, `label=gamma`}, true},
		{`label=alpha !label=beta`, []string{`label=gamma`, `label=phi`}, true},
		{`label=alpha !label=beta`, []string{`label=gamma`, `label=beta`}, false},

		{`label=alpha,!label=beta`, []string{`label=alpha`, `label=beta`}, false},
		{`label=alpha,!label=beta`, []string{`label=alpha`, `label=gamma`}, true},
		{`!label=alpha,!label=beta`, []string{`label=gamma`}, true},
		{`!label=alpha,!label=beta`, []string{`label=alpha`}, false},
		{`!label=alpha,!label=beta`, []string{`label=beta`}, false},
	} {
		filter, err := parseFilter(test.re)
		if err != nil {
			t.Fatal(err)
		}
		if got, expected := filter.Match(test.labels), test.match; got != expected {
			t.Errorf("filter:%v, labels:%v expected %v, got %v", test.re, test.labels, expected, got)
		}
	}
}
