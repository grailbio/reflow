package local

import (
	"math/rand"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	const (
		n    = 10
		name = "some_stat"
	)
	var (
		st    = make(stats)
		times = make([]time.Time, n)
	)
	for i := 0; i < n; i++ {
		times[i] = time.Now().Add(time.Duration(i) * time.Second)
		st.Observe(times[i], name, float64(i))
	}
	first, last := times[0], times[n-1]
	rand.Shuffle(len(times), func(i, j int) {
		times[i], times[j] = times[j], times[i]
	})
	if got, want := st.N(name), int64(n); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := st.First(name), first; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := st.Last(name), last; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := st.Max(name), float64(n-1); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
